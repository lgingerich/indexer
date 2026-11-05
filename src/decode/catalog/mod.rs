//! The immutable contract catalog: protocol manifests, loaded for one chain.
//!
//! A protocol is a directory holding a `protocol.toml` and the ABI files it names. The
//! protocols directory holds them at any depth, so versions can sit under their protocol;
//! a directory without a manifest only groups others:
//!
//! ```text
//! protocols/
//!   uniswap/
//!     v3/
//!       protocol.toml
//!       UniswapV3Factory.json
//!       UniswapV3Pool.json
//!     v4/
//!       protocol.toml
//!       PoolManager.json
//! ```
//!
//! The setting may name the whole tree, a group (`protocols/uniswap`), or one protocol
//! (`protocols/uniswap/v4`); every manifest under it loads. Where a manifest sits does
//! not change what it is called: its `protocol` value names it.
//!
//! ```toml
//! protocol = "uniswap_v3"
//!
//! [[contract]]
//! abi = ["UniswapV3Factory.json"]
//! addresses = { base = ["0x33128a8fC17869897dcE68Ed026d694621f6FDfD"] }
//!
//! [[contract]]
//! abi = ["UniswapV3Pool.json"]
//! created_by = [{ contract = "UniswapV3Factory", event = "PoolCreated", param = "pool" }]
//! ```
//!
//! A `[[contract]]` is one contract of the protocol, named after its first ABI file:
//! `UniswapV3Pool.json` is `UniswapV3Pool`. Name ABI files after the contract they
//! describe. The name is how a rule points at its parent and how a stored discovery finds
//! its ABI again after a restart, so it must stay stable: list a proxy's original ABI
//! first and append upgrades. The `abi` files merge into one event set, and a log decodes
//! against whichever event it was emitted as. `addresses` lists the deployed instances
//! per chain, decoded from the first block. `created_by` makes the contract discoverable:
//! when an instance of the parent contract emits `event`, its `param` argument is a new
//! instance of this one.
//!
//! Only one chain is loaded: a process indexes one chain, so the other chains' addresses
//! are parsed and then dropped. Event, parameter, and parent names are validated against
//! the ABIs at load, so a typo is a startup error rather than a rule that never fires. So
//! are the chains `addresses` names, against [`CHAINS`], so `bnb` for `bsc` is an error
//! rather than a chain that silently has no seeds.
//!
//! # Event tables
//!
//! Every event of every contract also gets a typed table — see
//! [`Schema`](crate::sink::table::Schema) — named `{protocol}_{contract}_{event}`: the
//! manifest's `protocol`, the contract's name in `snake_case`, and the event's name in
//! `snake_case`. The name does not depend on the directory, so pointing the setting at a
//! subtree writes to the same tables. A contract's optional `table` key replaces its part
//! of the name:
//!
//! ```toml
//! [[contract]]
//! abi = ["UniswapV3Pool.json"]
//! table = "pool"                  # uniswap_v3_pool_swap, not uniswap_v3_uniswap_v3_pool_swap
//! ```
//!
//! A name longer than `PostgreSQL`'s 63-byte identifier limit, which it would silently
//! truncate, is a startup error asking for a shorter `table`. So is a name produced twice:
//! two contracts given the same `table`, or one contract declaring two events by one name.
//!
//! A contract's optional `before_lake` key names events a backfill also fetches from
//! before the lake's first block, for the contract's listed `addresses`, when it has
//! `[backfill.hypersync]`: what describes something older than the lake, such as a pool's
//! tokens, while the rest of its history is left to the lake's range:
//!
//! ```toml
//! [[contract]]
//! abi = ["UniswapV3Factory.json"]
//! before_lake = ["PoolCreated"]
//! ```
//!
//! A contract's optional `indexes` key adds indexes to its event tables, by event name,
//! each index its columns in order, for the reads the block index does not serve:
//!
//! ```toml
//! [[contract]]
//! abi = ["UniswapV3Pool.json"]
//! indexes = { Swap = [["chain", "address", "block_number", "log_index"]] }
//! ```
//!
//! An event or a column the contract's tables do not have is a startup error. A store
//! builds a missing index when it starts, without `CONCURRENTLY`, which blocks writes to
//! the table until it is built: declare one before its table holds much, and build it by
//! hand first otherwise.

mod column;
mod error;

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use alloy_primitives::{Address, B256};
use serde::Deserialize;
use tracing::warn;

use super::abi::{Abi, AddressSlot, EventKey};
use super::derive::{DERIVATIONS, Derivation, Derivations};
use crate::sink::table::{MAX_IDENTIFIER, Schema, snake_case};
use crate::wire::envelope::ChainId;
use column::param;
pub use error::CatalogError;
use error::event_table_error;

/// The file each protocol directory must hold.
const MANIFEST: &str = "protocol.toml";

/// Every chain a manifest's `addresses` may name: the chains the indexer is run on. A
/// chain is free text everywhere else, so this is the one list a manifest is checked
/// against; add a chain here when first listing addresses on it.
const CHAINS: [&str; 4] = ["base", "bsc", "ethereum", "robinhood"];

/// One protocol's manifest, as written.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    protocol: String,
    #[serde(default)]
    contract: Vec<ContractSpec>,
}

/// One `[[contract]]`, as written.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContractSpec {
    abi: Vec<PathBuf>,
    /// This contract's part of its event tables' names; its name in `snake_case` when
    /// absent.
    table: Option<String>,
    /// The indexes of this contract's event tables, by event name, each its columns in
    /// order.
    #[serde(default)]
    indexes: BTreeMap<String, Vec<Vec<String>>>,
    #[serde(default)]
    addresses: BTreeMap<String, Vec<Address>>,
    #[serde(default)]
    created_by: Vec<CreatedBy>,
    /// The events a backfill also fetches from before the lake's first block.
    #[serde(default)]
    before_lake: Vec<String>,
}

/// One discovery rule, as written.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreatedBy {
    contract: String,
    event: String,
    param: String,
}

/// Index of an [`Entry`] in [`Catalog::entries`].
pub(crate) type EntryId = usize;

/// One `[[contract]]`, loaded: its protocol, name, merged ABI, and the contracts its
/// instances create.
#[derive(Debug)]
pub(crate) struct Entry {
    /// The protocol tag decoded records carry.
    pub(crate) protocol: String,
    /// The contract's name, from its first ABI file, for example `UniswapV3Pool`.
    pub(crate) name: String,
    /// Every event the contract can emit.
    pub(crate) abi: Abi,
    /// The contracts its instances create, keyed by the creating event.
    pub(crate) rules: HashMap<EventKey, Vec<Rule>>,
    /// The ids of the events a backfill also fetches from before the lake's first block.
    #[cfg_attr(
        not(feature = "delta"),
        expect(dead_code, reason = "only a backfill reads them, which needs the lake")
    )]
    before_lake: Vec<B256>,
}

/// A discovery rule, held by its parent's entry.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Rule {
    /// Where the `address` argument naming the child sits in the creation log, so the
    /// child is discovered from that argument alone.
    pub(crate) slot: AddressSlot,
    /// The child's entry.
    pub(crate) child: EntryId,
}

/// The loaded catalog for one chain: every contract entry, and the seed addresses on that
/// chain.
#[derive(Debug)]
pub struct Catalog {
    pub(crate) entries: Vec<Entry>,
    pub(crate) seeds: Vec<(Address, EntryId)>,
    /// The derivations whose events a manifest declares, bound to them; see
    /// [`super::derive`]. A [`DecodingSink`](super::DecodingSink) runs its own copy.
    pub(super) derivations: Derivations,
    schema: Schema,
}

impl Catalog {
    /// A catalog with no protocols: nothing decodes, and the schema is the dataset tables.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Datasets`] when a dataset table's declaration is
    /// inconsistent.
    pub fn empty() -> Result<Self, CatalogError> {
        Ok(Self {
            entries: Vec::new(),
            seeds: Vec::new(),
            derivations: Derivations::default(),
            schema: Schema::new().map_err(CatalogError::Datasets)?,
        })
    }

    /// Loads every protocol under `dir`, keeping `chain`'s addresses.
    ///
    /// # Errors
    ///
    /// Rejects an unreadable directory, a tree with no manifest in it, a malformed
    /// manifest or ABI, a contract without an ABI, duplicate protocol or contract
    /// names, unknown or ambiguous rule references, a rule parameter that is not an
    /// `address`, addresses listed under a chain the indexer does not know, and an address listed
    /// twice on `chain`.
    ///
    /// `dir` and every directory below it holding a `protocol.toml` is a protocol, so
    /// `dir` may be the whole catalog, a group such as `uniswap/`, or one protocol.
    pub fn load(dir: impl AsRef<Path>, chain: &ChainId) -> Result<Self, CatalogError> {
        let dir = dir.as_ref();
        let mut protocols = Vec::new();
        find_manifests(dir, &mut protocols)?;
        if protocols.is_empty() {
            return Err(CatalogError::NoManifests {
                path: dir.display().to_string(),
            });
        }
        // Sorted, so entry ids and error order do not depend on the filesystem's order.
        protocols.sort();
        let mut catalog = Self::empty()?;
        let mut names = HashMap::new();
        for protocol in protocols {
            let path = protocol.join(MANIFEST);
            let text = std::fs::read_to_string(&path).map_err(|source| CatalogError::File {
                path: path.display().to_string(),
                source,
            })?;
            let manifest: Manifest =
                toml::from_str(&text).map_err(|source| CatalogError::Manifest {
                    path: path.display().to_string(),
                    source,
                })?;
            if let Some(first) = names.insert(manifest.protocol.clone(), path) {
                return Err(CatalogError::DuplicateProtocol {
                    protocol: manifest.protocol,
                    first: first.display().to_string(),
                });
            }
            catalog.add(manifest, &protocol, chain)?;
        }
        catalog.add_derivations()?;
        let mut seen = HashMap::new();
        for &(address, entry) in &catalog.seeds {
            if let Some(first) = seen.insert(address, entry) {
                return Err(CatalogError::DuplicateAddress {
                    address,
                    first: catalog.label(first),
                    second: catalog.label(entry),
                });
            }
        }
        Ok(catalog)
    }

    /// Adds one manifest's contracts, `chain`'s seeds, and then its rules, which may name
    /// any contract in the manifest.
    fn add(&mut self, manifest: Manifest, dir: &Path, chain: &ChainId) -> Result<(), CatalogError> {
        let protocol = manifest.protocol;
        let mut by_name = HashMap::new();
        let mut rules = Vec::new();
        for spec in manifest.contract {
            let id = self.entries.len();
            let name = contract_name(&spec, &protocol)?;
            if by_name.insert(name.clone(), id).is_some() {
                return Err(CatalogError::DuplicateContract { protocol, name });
            }
            let abi = load_abi(dir, &spec.abi)?;
            let label = format!("{protocol}.{name}");
            let before_lake = before_lake_events(&abi, &spec.before_lake, &label)?;
            let prefix = spec.table.unwrap_or_else(|| snake_case(&name));
            self.add_event_tables(&protocol, &name, &prefix, &abi, &label, &spec.indexes)?;
            let seeds = chain_addresses(&spec.addresses, chain, || label)?;
            self.seeds
                .extend(seeds.iter().map(|&address| (address, id)));
            rules.extend(spec.created_by.into_iter().map(|rule| (id, rule)));
            self.entries.push(Entry {
                protocol: protocol.clone(),
                name,
                abi,
                rules: HashMap::new(),
                before_lake,
            });
        }
        for (child, rule) in rules {
            self.add_rule(&by_name, child, rule)?;
        }
        Ok(())
    }

    /// Declares the table of every event of `abi`, contract `name` of `protocol`, named
    /// with its `prefix` and indexed on `indexes`, by event name; `label` names the
    /// contract in errors.
    fn add_event_tables(
        &mut self,
        protocol: &str,
        name: &str,
        prefix: &str,
        abi: &Abi,
        label: &str,
        indexes: &BTreeMap<String, Vec<Vec<String>>>,
    ) -> Result<(), CatalogError> {
        if let Some(event) = indexes
            .keys()
            .find(|event| abi.events_named(event).is_empty())
        {
            return Err(CatalogError::IndexEvent {
                contract: label.to_owned(),
                event: event.clone(),
            });
        }
        for event in abi.events() {
            let table = format!("{protocol}_{prefix}_{}", snake_case(event.name));
            if table.len() > MAX_IDENTIFIER {
                return Err(CatalogError::TableName {
                    table,
                    contract: label.to_owned(),
                });
            }
            let params = event.inputs.iter().map(param).collect();
            self.schema
                .add_event(
                    (protocol, name, event.id),
                    table.clone(),
                    params,
                    indexes.get(event.name).map_or(&[], Vec::as_slice),
                )
                .map_err(|source| event_table_error(source, table, label.to_owned()))?;
        }
        Ok(())
    }

    /// Adds the discovery `rule` of entry `child` to its parent, which `by_name` finds
    /// among the manifest's contracts.
    fn add_rule(
        &mut self,
        by_name: &HashMap<String, EntryId>,
        child: EntryId,
        rule: CreatedBy,
    ) -> Result<(), CatalogError> {
        let label = || self.label(child);
        let parent = *by_name
            .get(&rule.contract)
            .ok_or_else(|| CatalogError::UnknownParent {
                contract: label(),
                parent: rule.contract.clone(),
            })?;
        let abi = &self.entries[parent].abi;
        let events = abi.events_named(&rule.event);
        let [event] = events[..] else {
            return Err(CatalogError::Event {
                contract: label(),
                event: rule.event,
                found: events.len(),
            });
        };
        let Some(slot) = abi.address_input(event, &rule.param) else {
            return Err(CatalogError::Param {
                contract: label(),
                event: rule.event,
                param: rule.param,
            });
        };
        self.entries[parent]
            .rules
            .entry(event)
            .or_default()
            .push(Rule { slot, child });
        Ok(())
    }

    /// The dataset tables and every decoded event's typed table.
    #[must_use]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// The entry for contract `name` of `protocol`, if the catalog has it.
    pub(crate) fn entry(&self, protocol: &str, name: &str) -> Option<EntryId> {
        self.entries
            .iter()
            .position(|entry| entry.protocol == protocol && entry.name == name)
    }

    /// Declares the table of every derivation whose events a manifest declares, and binds
    /// those derivations to their contract's entry and events. One whose contract is not
    /// loaded, as when the setting names a subtree without its protocol, or is declared
    /// without one of its events, because the contract's ABI changed under it, is skipped
    /// with a warning. Neither is an error here: a store that lists the skipped table fails
    /// at startup for want of it.
    fn add_derivations(&mut self) -> Result<(), CatalogError> {
        'derivations: for derivation in DERIVATIONS {
            let Derivation {
                protocol,
                contract,
                signatures,
                table,
                columns,
                indexes,
                ..
            } = derivation;
            let Some(entry) = self.entry(protocol, contract) else {
                warn!(
                    table,
                    protocol, contract, "the contract this table derives from is not loaded"
                );
                continue;
            };
            let mut events = Vec::with_capacity(signatures.len());
            for (index, &signature) in signatures.iter().enumerate() {
                let keys = self.entries[entry].abi.keys(signature);
                if keys.is_empty() {
                    warn!(
                        table,
                        protocol,
                        contract,
                        signature,
                        "the manifest does not declare an event this table derives from"
                    );
                    continue 'derivations;
                }
                events.extend(keys.into_iter().map(|key| (index, key)));
            }
            self.schema
                .add_derived(table, columns, indexes)
                .map_err(|source| CatalogError::Table {
                    contract: self.label(entry),
                    source,
                })?;
            self.derivations.bind(*derivation, entry, events);
        }
        Ok(())
    }

    /// Whether any contract is discovered from its parent's creation event rather than
    /// only listed.
    #[cfg(feature = "delta")]
    pub(crate) fn discovers(&self) -> bool {
        self.entries.iter().any(|entry| !entry.rules.is_empty())
    }

    /// Each listed contract with events a manifest asks a backfill to fetch from before
    /// the lake's first block, and those events' ids.
    #[cfg(feature = "delta")]
    pub(crate) fn before_lake(&self) -> Vec<(Address, Vec<B256>)> {
        self.seeds
            .iter()
            .map(|&(address, entry)| (address, self.entries[entry].before_lake.clone()))
            .filter(|(_, events)| !events.is_empty())
            .collect()
    }

    /// Whether any derived table is fed from the declared events.
    #[cfg(feature = "delta")]
    pub(crate) fn derives(&self) -> bool {
        !self.derivations.is_empty()
    }

    /// `protocol.name`, for messages.
    pub(crate) fn label(&self, entry: EntryId) -> String {
        format!(
            "{}.{}",
            self.entries[entry].protocol, self.entries[entry].name
        )
    }
}

/// A contract's name: its first ABI file's, without the extension.
fn contract_name(spec: &ContractSpec, protocol: &str) -> Result<String, CatalogError> {
    spec.abi
        .first()
        .and_then(|file| file.file_stem())
        .map(|stem| stem.to_string_lossy().into_owned())
        .ok_or_else(|| CatalogError::NoAbi {
            protocol: protocol.to_owned(),
        })
}

/// One contract's ABI: `files`, under `dir`, merged into one event set.
fn load_abi(dir: &Path, files: &[PathBuf]) -> Result<Abi, CatalogError> {
    let mut abi = Abi::default();
    for file in files {
        let path = dir.join(file);
        let json = std::fs::read_to_string(&path).map_err(|source| CatalogError::File {
            path: path.display().to_string(),
            source,
        })?;
        Abi::from_json(&json)
            .and_then(|part| abi.merge(part))
            .map_err(|source| CatalogError::Abi {
                path: path.display().to_string(),
                source,
            })?;
    }
    Ok(abi)
}

/// The ids of `events`, a contract's `before_lake` as written; `label` names it in errors.
fn before_lake_events(
    abi: &Abi,
    events: &[String],
    label: &str,
) -> Result<Vec<B256>, CatalogError> {
    let mut ids = Vec::new();
    for event in events {
        let keys = abi.events_named(event);
        if keys.is_empty() {
            return Err(CatalogError::BeforeLakeEvent {
                contract: label.to_owned(),
                event: event.clone(),
            });
        }
        ids.extend(keys.into_iter().map(|(id, _)| id));
    }
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

/// The addresses a contract, `contract` for messages, lists on `chain`, after checking
/// that every chain it lists addresses on is one of [`CHAINS`].
fn chain_addresses<'a>(
    addresses: &'a BTreeMap<String, Vec<Address>>,
    chain: &ChainId,
    contract: impl FnOnce() -> String,
) -> Result<&'a [Address], CatalogError> {
    if let Some(unknown) = addresses
        .keys()
        .find(|listed| !CHAINS.contains(&listed.as_str()))
    {
        return Err(CatalogError::UnknownChain {
            contract: contract(),
            chain: unknown.clone(),
        });
    }
    Ok(addresses.get(chain.as_str()).map_or(&[], Vec::as_slice))
}

/// Adds `dir` to `protocols` when it holds a manifest, then searches its subdirectories.
/// Symlinks are not followed.
fn find_manifests(dir: &Path, protocols: &mut Vec<PathBuf>) -> Result<(), CatalogError> {
    let read = |source| CatalogError::Directory {
        path: dir.display().to_string(),
        source,
    };
    if dir.join(MANIFEST).is_file() {
        protocols.push(dir.to_path_buf());
    }
    for entry in std::fs::read_dir(dir).map_err(read)? {
        let entry = entry.map_err(read)?;
        if entry.file_type().map_err(read)?.is_dir() {
            find_manifests(&entry.path(), protocols)?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use super::{AddressSlot, Catalog, CatalogError};
    use crate::decode::fixtures::{protocol, protocols, shipped};
    use crate::sink::table::{ColumnType, TableError, TableId};

    /// One protocol holding `manifest` and the shipped Uniswap V3 ABIs.
    fn single(manifest: &str) -> Result<Catalog, CatalogError> {
        let abi = |file: &str| {
            std::fs::read_to_string(protocols().join("uniswap/v3").join(file)).expect("read ABI")
        };
        protocol(&[
            ("UniswapV3Factory.json", &abi("UniswapV3Factory.json")),
            ("UniswapV3Pool.json", &abi("UniswapV3Pool.json")),
            ("protocol.toml", manifest),
        ])
    }

    const FACTORY: &str = r#"
protocol = "uniswap_v3"
[[contract]]
abi = ["UniswapV3Factory.json"]
addresses = { base = ["0x33128a8fC17869897dcE68Ed026d694621f6FDfD"], ethereum = ["0x1F98431c8aD98523631AE4a59f267346ea31F984"] }
"#;

    // The pool's default table names pass the limit, as the shipped manifest's do.
    const POOL: &str = "[[contract]]\nabi = [\"UniswapV3Pool.json\"]\ntable = \"pool\"\n";

    /// ERC-20's and ERC-721's `Transfer`: one name and selector, a different topic count.
    const TRANSFERS: &str = r#"[
        {"type":"event","name":"Transfer","anonymous":false,"inputs":[
            {"name":"from","type":"address","indexed":true},
            {"name":"to","type":"address","indexed":true},
            {"name":"value","type":"uint256","indexed":false}]},
        {"type":"event","name":"Transfer","anonymous":false,"inputs":[
            {"name":"from","type":"address","indexed":true},
            {"name":"to","type":"address","indexed":true},
            {"name":"tokenId","type":"uint256","indexed":true}]}
    ]"#;

    /// The event tables' names: every table after the dataset tables.
    fn table_names(catalog: &Catalog) -> Vec<String> {
        catalog
            .schema()
            .tables()
            .iter()
            .filter(|table| matches!(table.id, TableId::Event(_)))
            .map(|table| table.name.clone())
            .collect()
    }

    /// Every event of every shipped contract has a table within the name limit, named
    /// `{protocol}_{contract}_{event}`, with the `table` key replacing the contract.
    #[test]
    fn the_shipped_protocols_have_event_tables() {
        let catalog = shipped("base");
        let names = table_names(&catalog);
        assert!(names.iter().all(|name| name.len() <= super::MAX_IDENTIFIER));
        assert!(names.contains(&"uniswap_v3_uniswap_v3_factory_pool_created".to_owned()));
        assert!(names.contains(&"uniswap_v4_pool_manager_swap".to_owned()));
        let swap = catalog
            .schema()
            .tables()
            .iter()
            .find(|table| table.name == "uniswap_v3_pool_swap")
            .expect("the pool's swap table");
        let columns: Vec<(&str, ColumnType)> = swap
            .columns
            .iter()
            .map(|column| (column.name.as_ref(), column.kind))
            .collect();
        assert_eq!(
            columns,
            [
                ("address", ColumnType::Text),
                ("transaction_hash", ColumnType::Text),
                ("transaction_index", ColumnType::Uint),
                ("log_index", ColumnType::Uint),
                ("sender", ColumnType::Text),
                ("recipient", ColumnType::Text),
                ("amount0", ColumnType::BigInt),
                ("amount1", ColumnType::BigInt),
                ("sqrt_price_x96", ColumnType::BigInt),
                ("liquidity", ColumnType::BigInt),
                ("tick", ColumnType::Int),
                ("block_number", ColumnType::Uint),
                ("block_hash", ColumnType::Text),
                ("block_timestamp", ColumnType::Timestamp),
                ("chain", ColumnType::Text),
                ("dedupe_key", ColumnType::Text),
            ]
        );
    }

    /// What says what an older Uniswap pool is comes from before the lake: V3's factory's
    /// `PoolCreated` and V4's manager's `Initialize`, each from its listed contract alone.
    #[cfg(feature = "delta")]
    #[test]
    fn the_shipped_uniswap_contracts_fetch_what_describes_a_pool_from_before_the_lake() {
        let catalog = shipped("base");
        let ids: std::collections::BTreeSet<super::B256> = catalog
            .before_lake()
            .into_iter()
            .flat_map(|(_, events)| events)
            .collect();
        let expected: std::collections::BTreeSet<super::B256> = [
            "PoolCreated(address,address,uint24,int24,address)",
            "Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)",
        ]
        .into_iter()
        .map(alloy_primitives::keccak256)
        .collect();
        assert_eq!(ids, expected);
    }

    /// A `before_lake` naming an event its contract does not declare is a startup error.
    #[test]
    fn a_before_lake_naming_nothing_is_a_startup_error() {
        let abi = super::Abi::from_json(
            r#"[{"type":"event","name":"Ping","anonymous":false,"inputs":[]}]"#,
        )
        .expect("an ABI");
        assert_eq!(
            super::before_lake_events(&abi, &["Ping".to_owned()], "p.c").expect("declared"),
            [alloy_primitives::keccak256("Ping()")]
        );
        assert!(matches!(
            super::before_lake_events(&abi, &["Pong".to_owned()], "p.c"),
            Err(CatalogError::BeforeLakeEvent { contract, event })
                if contract == "p.c" && event == "Pong"
        ));
    }

    /// The shipped Uniswap tables are indexed for reading one pool, by its address in V3
    /// and its id in V4, where every pool has one address.
    #[test]
    fn the_shipped_uniswap_tables_are_indexed_by_pool() {
        let catalog = shipped("base");
        let columns = |table: &str| -> Vec<Vec<String>> {
            catalog
                .schema()
                .tables()
                .iter()
                .find(|def| def.name == table)
                .unwrap_or_else(|| panic!("{table} exists"))
                .indexes
                .iter()
                .map(|index| index.columns.iter().map(ToString::to_string).collect())
                .collect()
        };
        let pool = |column: &'static str| vec!["chain", column, "block_number", "log_index"];
        for (table, expected) in [
            ("uniswap_v3_pool_swap", pool("address")),
            ("uniswap_v3_pool_mint", pool("address")),
            ("uniswap_v3_pool_burn", pool("address")),
            ("uniswap_v4_pool_manager_swap", pool("id")),
            ("uniswap_v4_pool_manager_modify_liquidity", pool("id")),
            ("uniswap_v4_pool_manager_initialize", vec!["chain", "id"]),
        ] {
            assert!(
                columns(table).iter().any(|index| index == &expected),
                "{table}: {:?}",
                columns(table)
            );
        }
    }

    /// An index on an event or a column the contract's tables do not have is a startup
    /// error rather than an index that is never built.
    #[test]
    fn an_index_naming_nothing_is_a_startup_error() {
        let abi = r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[]}]"#;
        let manifest = |indexes: &str| {
            format!("protocol = \"p\"\n[[contract]]\nabi = [\"Token.json\"]\nindexes = {indexes}\n")
        };
        match protocol(&[
            ("Token.json", abi),
            ("protocol.toml", &manifest(r#"{ Mint = [["chain"]] }"#)),
        ]) {
            Err(CatalogError::IndexEvent { contract, event }) => {
                assert_eq!((contract.as_str(), event.as_str()), ("p.Token", "Mint"));
            }
            other => panic!("an unknown event: {other:?}"),
        }
        match protocol(&[
            ("Token.json", abi),
            ("protocol.toml", &manifest(r#"{ Transfer = [["nope"]] }"#)),
        ]) {
            Err(CatalogError::Table { contract, source }) => {
                assert_eq!(contract, "p.Token");
                assert!(
                    matches!(source, TableError::UnknownColumn { column, .. } if column == "nope")
                );
            }
            other => panic!("an unknown column: {other:?}"),
        }
    }

    /// A name past the limit, two contracts sharing a `table`, and two events by one name
    /// in one contract are each a startup error rather than a table that collides.
    #[test]
    fn long_or_repeated_table_names_are_startup_errors() {
        let abi = ("Token.json", TRANSFERS);
        let manifest = |extra: &str| {
            format!("protocol = \"p\"\n[[contract]]\nabi = [\"Token.json\"]\n{extra}")
        };
        let long = "t".repeat(60);
        match protocol(&[
            abi,
            ("protocol.toml", &manifest(&format!("table = \"{long}\"\n"))),
        ]) {
            Err(CatalogError::TableName { table, contract }) => assert_eq!(
                (table, contract.as_str()),
                (format!("p_{long}_transfer"), "p.Token")
            ),
            other => panic!("a long name: {other:?}"),
        }
        let one = r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[]}]"#;
        for (case, files, (repeated, by)) in [
            (
                "ERC-20's and ERC-721's Transfer share a name",
                vec![
                    ("Token.json", TRANSFERS.to_owned()),
                    ("protocol.toml", manifest("")),
                ],
                ("p_token_transfer", "p.Token"),
            ),
            (
                "two contracts share a table",
                vec![
                    ("Token.json", one.to_owned()),
                    ("Other.json", one.to_owned()),
                    (
                        "protocol.toml",
                        manifest(
                            "table = \"t\"\n[[contract]]\nabi = [\"Other.json\"]\ntable = \"t\"\n",
                        ),
                    ),
                ],
                ("p_t_transfer", "p.Other"),
            ),
        ] {
            let files: Vec<_> = files
                .iter()
                .map(|(name, contents)| (*name, contents.as_str()))
                .collect();
            match protocol(&files) {
                Err(CatalogError::DuplicateTable { table, contract }) => {
                    assert_eq!(
                        (table.as_str(), contract.as_str()),
                        (repeated, by),
                        "{case}"
                    );
                }
                other => panic!("{case}: {other:?}"),
            }
        }
        assert_eq!(
            table_names(
                &protocol(&[
                    ("Token.json", one),
                    ("protocol.toml", &manifest("").replace("\"p\"", "\"proto\""))
                ])
                .expect("loads")
            ),
            ["proto_token_transfer"],
            "named for the manifest's protocol, not its folder"
        );
    }

    /// The shipped Uniswap V3 manifest wires as written: the factory's `PoolCreated`
    /// discovers pools from its `pool` argument, and the pool is seeded with exactly its
    /// Base addresses, so a short run decodes swaps out of the box.
    #[test]
    fn the_shipped_pools_are_discovered_and_seeded() {
        let catalog = shipped("base");
        let factory = catalog
            .entry("uniswap_v3", "UniswapV3Factory")
            .expect("factory");
        let pool = catalog.entry("uniswap_v3", "UniswapV3Pool").expect("pool");
        let rules: Vec<_> = catalog.entries[factory]
            .rules
            .values()
            .flatten()
            .map(|rule| (rule.slot, rule.child))
            .collect();
        assert_eq!(
            rules,
            [(AddressSlot::Word(32), pool)],
            "PoolCreated's fifth input, the second word of its data, names the pool"
        );
        let mut seeded: Vec<_> = catalog
            .seeds
            .iter()
            .filter(|&&(_, entry)| entry == pool)
            .map(|&(address, _)| address)
            .collect();
        seeded.sort_unstable();
        let mut listed: Vec<alloy_primitives::Address> = [
            "0x2df380544b88adb3ad0a94100dcc45fd705aae2d",
            "0xd0b53D9277642d899DF5C87A3966A349A798F224",
        ]
        .map(|address| address.parse().expect("address"))
        .to_vec();
        listed.sort_unstable();
        assert_eq!(seeded, listed);
    }

    /// A chain name in `addresses` that no chain has is a startup error, not a chain with
    /// no seeds.
    #[test]
    fn an_unknown_chain_in_addresses_is_a_startup_error() {
        match single(&FACTORY.replace("ethereum = ", "bnb = ")) {
            Err(CatalogError::UnknownChain { contract, chain }) => assert_eq!(
                (contract.as_str(), chain.as_str()),
                ("uniswap_v3.UniswapV3Factory", "bnb")
            ),
            other => panic!("expected an unknown chain, got {other:?}"),
        }
    }

    /// A contract is named after its first ABI file, and a rule resolves where its
    /// parameter sits from the parent's ABI.
    #[test]
    fn contracts_are_named_by_their_abi_file_and_rules_resolve_against_the_parent() {
        let catalog = single(&format!(
            "{FACTORY}{POOL}created_by = [{{ contract = \"UniswapV3Factory\", event = \"PoolCreated\", param = \"pool\" }}]\n"
        ))
        .expect("loads");
        let factory = catalog
            .entry("uniswap_v3", "UniswapV3Factory")
            .expect("factory");
        let pool = catalog.entry("uniswap_v3", "UniswapV3Pool").expect("pool");
        let rules: Vec<_> = catalog.entries[factory].rules.values().flatten().collect();
        // `pool` is PoolCreated's fifth input, after three indexed ones and tickSpacing,
        // the first word of the data.
        assert_eq!(
            rules
                .iter()
                .map(|rule| (rule.slot, rule.child))
                .collect::<Vec<_>>(),
            [(AddressSlot::Word(32), pool)]
        );
        assert_eq!(catalog.seeds.len(), 1, "only the loaded chain's addresses");
    }
}
