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
//! the ABIs at load, so a typo is a startup error rather than a rule that never fires.
//!
//! # Event tables
//!
//! Every event of every contract also gets a typed table — see
//! [`Schema`](crate::wire::row::Schema) — named `{protocol}_{contract}_{event}`: the
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
//! A name longer than [`MAX_TABLE_NAME`] bytes, which `PostgreSQL` would silently
//! truncate, is a startup error asking for a shorter `table`. So is a name produced twice:
//! two contracts given the same `table`, or one contract declaring two events by one name.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use alloy_dyn_abi::DynSolType;
use alloy_primitives::Address;
use serde::Deserialize;

use super::abi::{Abi, AbiError, EventKey, InputSpec};
use crate::wire::envelope::ChainId;
use crate::wire::row::{Column, ColumnType, Schema, snake_case};

/// The longest event table name: `PostgreSQL`'s identifier limit, past which it would
/// silently truncate the name and two tables could collide.
pub const MAX_TABLE_NAME: usize = 63;

/// The file each protocol directory must hold.
const MANIFEST: &str = "protocol.toml";

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
    #[serde(default)]
    addresses: BTreeMap<String, Vec<Address>>,
    #[serde(default)]
    created_by: Vec<CreatedBy>,
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
}

/// A discovery rule, held by its parent's entry.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Rule {
    /// The position of the `address` argument naming the child.
    pub(crate) position: usize,
    /// The child's entry.
    pub(crate) child: EntryId,
}

/// The loaded catalog for one chain: every contract entry, and the seed addresses on that
/// chain.
#[derive(Debug, Default)]
pub struct Catalog {
    pub(crate) entries: Vec<Entry>,
    pub(crate) seeds: Vec<(Address, EntryId)>,
    schema: Schema,
}

impl Catalog {
    /// Loads every protocol under `dir`, keeping `chain`'s addresses.
    ///
    /// # Errors
    ///
    /// Rejects an unreadable directory, a tree with no manifest in it, a malformed
    /// manifest or ABI, a contract without an ABI, duplicate protocol or contract
    /// names, unknown or ambiguous rule references, a rule parameter that is not an
    /// `address`, and an address listed twice on `chain`.
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
        let mut catalog = Self::default();
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
        let mut by_name = HashMap::new();
        let mut rules = Vec::new();
        for spec in manifest.contract {
            let id = self.entries.len();
            let Some(name) = spec
                .abi
                .first()
                .and_then(|file| file.file_stem())
                .map(|stem| stem.to_string_lossy().into_owned())
            else {
                return Err(CatalogError::NoAbi {
                    protocol: manifest.protocol,
                });
            };
            if by_name.insert(name.clone(), id).is_some() {
                return Err(CatalogError::DuplicateContract {
                    protocol: manifest.protocol,
                    name,
                });
            }
            let mut abi = Abi::default();
            for file in &spec.abi {
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
            let prefix = spec.table.clone().unwrap_or_else(|| snake_case(&name));
            for event in abi.events() {
                let table = format!("{}_{prefix}_{}", manifest.protocol, snake_case(event.name));
                let contract = || format!("{}.{name}", manifest.protocol);
                if table.len() > MAX_TABLE_NAME {
                    return Err(CatalogError::TableName {
                        table,
                        contract: contract(),
                    });
                }
                let params = event.inputs.iter().map(param).collect();
                let key = (manifest.protocol.as_str(), name.as_str(), event.id);
                if !self.schema.add_event(key, table.clone(), params) {
                    return Err(CatalogError::DuplicateTable {
                        table,
                        contract: contract(),
                    });
                }
            }
            if let Some(addresses) = spec.addresses.get(chain.as_str()) {
                self.seeds
                    .extend(addresses.iter().map(|&address| (address, id)));
            }
            rules.extend(spec.created_by.into_iter().map(|rule| (id, rule)));
            self.entries.push(Entry {
                protocol: manifest.protocol.clone(),
                name,
                abi,
                rules: HashMap::new(),
            });
        }
        for (child, rule) in rules {
            let label = || self.label(child);
            let parent =
                *by_name
                    .get(&rule.contract)
                    .ok_or_else(|| CatalogError::UnknownParent {
                        contract: label(),
                        parent: rule.contract.clone(),
                    })?;
            let events = self.entries[parent].abi.events_named(&rule.event);
            let [event] = events[..] else {
                return Err(CatalogError::Event {
                    contract: label(),
                    event: rule.event,
                    found: events.len(),
                });
            };
            let Some(position) = self.entries[parent].abi.address_input(event, &rule.param) else {
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
                .push(Rule { position, child });
        }
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

    /// Whether any contract is discovered from a creation event.
    #[must_use]
    pub fn discovers(&self) -> bool {
        self.entries.iter().any(|entry| !entry.rules.is_empty())
    }

    /// Drops every discovery rule, leaving seeds and stored contracts to decode as listed.
    pub fn disable_discovery(&mut self) {
        for entry in &mut self.entries {
            entry.rules.clear();
        }
    }

    /// `protocol.name`, for messages.
    pub(crate) fn label(&self, entry: EntryId) -> String {
        format!(
            "{}.{}",
            self.entries[entry].protocol, self.entries[entry].name
        )
    }
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

/// How one input is stored in its event's table: a column under its ABI name.
fn param(input: &InputSpec<'_>) -> Column {
    let (kind, required) = match input.ty {
        // An indexed string, bytes, array, or tuple is only its topic hash.
        DynSolType::String
        | DynSolType::Bytes
        | DynSolType::Array(_)
        | DynSolType::FixedArray(..)
        | DynSolType::Tuple(_)
            if input.indexed =>
        {
            (ColumnType::Text, true)
        }
        DynSolType::Bool => (ColumnType::Bool, true),
        DynSolType::Uint(bits) if *bits <= 64 => (ColumnType::Uint, true),
        DynSolType::Int(bits) if *bits <= 64 => (ColumnType::Int, true),
        DynSolType::Uint(_) | DynSolType::Int(_) => (ColumnType::BigInt, true),
        DynSolType::Address
        | DynSolType::Function
        | DynSolType::FixedBytes(_)
        | DynSolType::Bytes => (ColumnType::Text, true),
        // Null when the bytes are not text.
        DynSolType::String => (ColumnType::Text, false),
        _ => (ColumnType::Document, true),
    };
    Column::named(input.name.to_owned(), kind, required)
}

/// Why the catalog could not be loaded.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    /// The protocols directory could not be read.
    #[error("read protocols directory {path}: {source}")]
    Directory {
        /// The directory.
        path: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// The protocols directory holds no `protocol.toml` anywhere in its tree, so the
    /// setting names the wrong directory.
    #[error("no protocol.toml under {path}")]
    NoManifests {
        /// The directory.
        path: String,
    },
    /// A manifest or ABI file could not be read.
    #[error("read {path}: {source}")]
    File {
        /// The file.
        path: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// A manifest did not parse, or a required value was absent.
    #[error("invalid manifest {path}: {source}")]
    Manifest {
        /// The manifest.
        path: String,
        /// The underlying TOML error.
        source: toml::de::Error,
    },
    /// An ABI file did not parse, or conflicts with another file of the same contract.
    #[error("invalid ABI at {path}: {source}")]
    Abi {
        /// The ABI file.
        path: String,
        /// The ABI failure.
        source: AbiError,
    },
    /// A contract lists no ABI file, so it has no name and nothing to decode with.
    #[error("{protocol} has a contract with no ABI file")]
    NoAbi {
        /// The protocol.
        protocol: String,
    },
    /// Two manifests name the same protocol.
    #[error("protocol {protocol:?} is declared twice; first in {first}")]
    DuplicateProtocol {
        /// The protocol.
        protocol: String,
        /// The manifest that declared it first.
        first: String,
    },
    /// One manifest has two contracts whose first ABI file has the same name.
    #[error("{protocol} declares contract {name:?} twice")]
    DuplicateContract {
        /// The protocol.
        protocol: String,
        /// The contract's name.
        name: String,
    },
    /// One address is listed under two contracts on the loaded chain.
    #[error("{address} is listed as both {first} and {second}")]
    DuplicateAddress {
        /// The address.
        address: Address,
        /// The first contract, as `protocol.name`.
        first: String,
        /// The second contract, as `protocol.name`.
        second: String,
    },
    /// A rule names a parent contract its protocol does not declare.
    #[error("{contract} is created_by unknown contract {parent:?}")]
    UnknownParent {
        /// The child contract, as `protocol.name`.
        contract: String,
        /// The parent as written.
        parent: String,
    },
    /// A rule's event is absent from the parent's ABI, or overloaded.
    #[error("{contract} is created_by event {event:?}, which the parent declares {found} times")]
    Event {
        /// The child contract, as `protocol.name`.
        contract: String,
        /// The event as written.
        event: String,
        /// How many events by that name the parent declares; exactly one is required.
        found: usize,
    },
    /// An event table's name is longer than [`MAX_TABLE_NAME`].
    #[error(
        "event table {table} is longer than {MAX_TABLE_NAME} bytes; give {contract} a shorter `table`"
    )]
    TableName {
        /// The table.
        table: String,
        /// The contract, as `protocol.name`.
        contract: String,
    },
    /// An event table's name is produced twice.
    #[error(
        "event table {table} already exists when adding {contract}: two contracts share a \
         `table`, or the contract declares two events by one name"
    )]
    DuplicateTable {
        /// The table.
        table: String,
        /// The contract that produced it again, as `protocol.name`.
        contract: String,
    },
    /// A rule's parameter is absent from its event, or not an `address`.
    #[error("{contract} is created_by {event}.{param}, which is not an address argument")]
    Param {
        /// The child contract, as `protocol.name`.
        contract: String,
        /// The event.
        event: String,
        /// The parameter as written.
        param: String,
    },
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{Catalog, CatalogError};
    use crate::wire::envelope::ChainId;
    use crate::wire::row::{ColumnType, TableId};

    fn repository() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("protocols")
    }

    /// A protocols directory in a temporary location, holding one manifest that shares
    /// the shipped Uniswap V3 ABIs.
    fn single(manifest: &str) -> Result<Catalog, CatalogError> {
        let root = std::env::temp_dir().join(format!(
            "indexer-protocols-{}-{:x}",
            std::process::id(),
            alloy_primitives::keccak256(manifest)
        ));
        let dir = root.join("uniswap_v3");
        std::fs::create_dir_all(&dir).expect("create protocol dir");
        for file in ["UniswapV3Factory.json", "UniswapV3Pool.json"] {
            std::fs::copy(repository().join("uniswap/v3").join(file), dir.join(file))
                .expect("copy ABI");
        }
        std::fs::write(dir.join("protocol.toml"), manifest).expect("write manifest");
        let catalog = Catalog::load(&root, &ChainId::new("base"));
        std::fs::remove_dir_all(&root).expect("clean up");
        catalog
    }

    const FACTORY: &str = r#"
protocol = "uniswap_v3"
[[contract]]
abi = ["UniswapV3Factory.json"]
addresses = { base = ["0x33128a8fC17869897dcE68Ed026d694621f6FDfD"], ethereum = ["0x1F98431c8aD98523631AE4a59f267346ea31F984"] }
"#;

    // The pool's default table names pass the limit, as the shipped manifest's do.
    const POOL: &str = "[[contract]]\nabi = [\"UniswapV3Pool.json\"]\ntable = \"pool\"\n";

    /// A protocol `p` holding `files`, one of which is its `protocol.toml`.
    fn protocol(files: &[(&str, &str)]) -> Result<Catalog, CatalogError> {
        let root = std::env::temp_dir().join(format!(
            "indexer-protocol-{}-{:x}",
            std::process::id(),
            alloy_primitives::keccak256(format!("{files:?}"))
        ));
        let dir = root.join("p");
        std::fs::create_dir_all(&dir).expect("create protocol dir");
        for (name, contents) in files {
            std::fs::write(dir.join(name), contents).expect("write file");
        }
        let catalog = Catalog::load(&root, &ChainId::new("base"));
        std::fs::remove_dir_all(&root).expect("clean up");
        catalog
    }

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
        let catalog = Catalog::load(repository(), &ChainId::new("base")).expect("loads");
        let names = table_names(&catalog);
        assert!(names.iter().all(|name| name.len() <= super::MAX_TABLE_NAME));
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
                ("block_timestamp", ColumnType::Uint),
                ("chain", ColumnType::Text),
                ("dedupe_key", ColumnType::Text),
            ]
        );
    }

    /// A name past the limit, two contracts sharing a `table`, and two events by one name
    /// in one contract are each a startup error rather than a table that collides.
    #[test]
    fn long_or_repeated_table_names_are_startup_errors() {
        let abi = ("Token.json", TRANSFERS);
        let manifest = |extra: &str| {
            format!("protocol = \"p\"\n[[contract]]\nabi = [\"Token.json\"]\n{extra}")
        };
        assert!(matches!(
            protocol(&[
                abi,
                (
                    "protocol.toml",
                    &manifest(&format!("table = \"{}\"\n", "t".repeat(60)))
                )
            ]),
            Err(CatalogError::TableName { .. })
        ));
        assert!(
            matches!(
                protocol(&[abi, ("protocol.toml", &manifest(""))]),
                Err(CatalogError::DuplicateTable { .. })
            ),
            "ERC-20's and ERC-721's Transfer share a name"
        );
        let one = r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[]}]"#;
        assert!(matches!(
            protocol(&[
                ("Token.json", one),
                ("Other.json", one),
                (
                    "protocol.toml",
                    &manifest(
                        "table = \"t\"\n[[contract]]\nabi = [\"Other.json\"]\ntable = \"t\"\n"
                    )
                ),
            ]),
            Err(CatalogError::DuplicateTable { .. })
        ));
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

    #[test]
    fn the_shipped_protocols_load() {
        let catalog = Catalog::load(repository(), &ChainId::new("base")).expect("loads");
        assert!(catalog.discovers());
        let pool = catalog.entry("uniswap_v3", "UniswapV3Pool").expect("pool");
        assert!(catalog.seeds.iter().any(|&(_, entry)| entry == pool));
        assert!(catalog.entry("uniswap_v4", "PoolManager").is_some());
        assert!(catalog.entry("metric_v1", "MetricOmmPool").is_some());
    }

    /// A contract is named after its first ABI file, and a rule resolves its parameter's
    /// position from the parent's ABI.
    #[test]
    fn contracts_are_named_by_their_abi_file_and_rules_resolve_against_the_parent() {
        let mut catalog = single(&format!(
            "{FACTORY}{POOL}created_by = [{{ contract = \"UniswapV3Factory\", event = \"PoolCreated\", param = \"pool\" }}]\n"
        ))
        .expect("loads");
        let factory = catalog
            .entry("uniswap_v3", "UniswapV3Factory")
            .expect("factory");
        let pool = catalog.entry("uniswap_v3", "UniswapV3Pool").expect("pool");
        let rules: Vec<_> = catalog.entries[factory].rules.values().flatten().collect();
        // `pool` is PoolCreated's fifth input, after token0, token1, fee, tickSpacing.
        assert_eq!(
            rules
                .iter()
                .map(|rule| (rule.position, rule.child))
                .collect::<Vec<_>>(),
            [(4, pool)]
        );
        assert_eq!(catalog.seeds.len(), 1, "only the loaded chain's addresses");

        catalog.disable_discovery();
        assert!(!catalog.discovers());
    }

    #[test]
    fn bad_rules_are_startup_errors() {
        for (rule, expected) in [
            (
                r#"{ contract = "Nope", event = "PoolCreated", param = "pool" }"#,
                "parent",
            ),
            (
                r#"{ contract = "UniswapV3Factory", event = "Nope", param = "pool" }"#,
                "event",
            ),
            (
                r#"{ contract = "UniswapV3Factory", event = "PoolCreated", param = "nope" }"#,
                "param",
            ),
            (
                r#"{ contract = "UniswapV3Factory", event = "PoolCreated", param = "fee" }"#,
                "param",
            ),
        ] {
            let error =
                single(&format!("{FACTORY}{POOL}created_by = [{rule}]\n")).expect_err("rejected");
            let matched = match expected {
                "parent" => matches!(error, CatalogError::UnknownParent { .. }),
                "event" => matches!(error, CatalogError::Event { found: 0, .. }),
                _ => matches!(error, CatalogError::Param { .. }),
            };
            assert!(matched, "{rule}: {error}");
        }
    }

    #[test]
    fn duplicates_and_malformed_contracts_are_startup_errors() {
        let second = FACTORY.replace("protocol = \"uniswap_v3\"", "");
        assert!(matches!(
            single(&format!("{FACTORY}{second}")),
            Err(CatalogError::DuplicateContract { .. })
        ));
        assert!(matches!(
            single(&format!(
                "{FACTORY}{POOL}addresses = {{ base = [\"0x33128a8fC17869897dcE68Ed026d694621f6FDfD\"] }}\n"
            )),
            Err(CatalogError::DuplicateAddress { .. })
        ));
        assert!(matches!(
            single(&format!("{FACTORY}[[contract]]\nabi = []\n")),
            Err(CatalogError::NoAbi { .. })
        ));
        for malformed in [
            FACTORY.replace("0x1F98431c8aD98523631AE4a59f267346ea31F984", "0x12"),
            format!("{FACTORY}kind = \"renamed\"\n"),
        ] {
            assert!(matches!(
                single(&malformed),
                Err(CatalogError::Manifest { .. })
            ));
        }
    }

    #[test]
    fn a_missing_directory_is_a_startup_error() {
        assert!(matches!(
            Catalog::load("/no/such/protocols", &ChainId::new("base")),
            Err(CatalogError::Directory { .. })
        ));
    }

    /// The setting may name the whole tree, a group, or one protocol, and the tables are
    /// named the same whichever it names. A directory with no manifest anywhere under it
    /// is a startup error rather than a run that decodes nothing.
    #[test]
    fn any_subtree_loads_and_names_tables_the_same() {
        let chain = ChainId::new("base");
        let names = |dir: PathBuf| table_names(&Catalog::load(dir, &chain).expect("loads"));
        let all = names(repository());
        let uniswap = names(repository().join("uniswap"));
        let v4 = names(repository().join("uniswap/v4"));
        assert!(v4.contains(&"uniswap_v4_pool_manager_swap".to_owned()));
        assert!(v4.iter().all(|name| name.starts_with("uniswap_v4_")));
        assert!(uniswap.iter().any(|name| name.starts_with("uniswap_v3_")));
        assert!(uniswap.iter().all(|name| all.contains(name)));
        assert!(v4.iter().all(|name| uniswap.contains(name)));

        let empty = std::env::temp_dir().join(format!("indexer-empty-{}", std::process::id()));
        std::fs::create_dir_all(empty.join("nested")).expect("create dirs");
        let error = Catalog::load(&empty, &chain).expect_err("no manifest");
        std::fs::remove_dir_all(&empty).expect("clean up");
        assert!(matches!(error, CatalogError::NoManifests { .. }), "{error}");
    }
}
