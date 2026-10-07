//! The immutable contract catalog: protocol manifests, loaded for one chain.
//!
//! A protocols directory holds one subdirectory per protocol, each with a
//! `protocol.toml` and the ABI files it names:
//!
//! ```text
//! protocols/
//!   uniswap_v3/
//!     protocol.toml
//!     UniswapV3Factory.json
//!     UniswapV3Pool.json
//! ```
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

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use alloy_primitives::Address;
use serde::Deserialize;

use super::abi::{Abi, AbiError, EventKey};
use crate::wire::envelope::ChainId;

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
}

impl Catalog {
    /// Loads every protocol under `dir`, keeping `chain`'s addresses.
    ///
    /// # Errors
    ///
    /// Rejects an unreadable directory, a protocol directory without a manifest, a
    /// malformed manifest or ABI, a contract without an ABI, duplicate protocol or contract
    /// names, unknown or ambiguous rule references, a rule parameter that is not an
    /// `address`, and an address listed twice on `chain`.
    pub fn load(dir: impl AsRef<Path>, chain: &ChainId) -> Result<Self, CatalogError> {
        let dir = dir.as_ref();
        let read = |source| CatalogError::Directory {
            path: dir.display().to_string(),
            source,
        };
        let mut protocols = Vec::new();
        for entry in std::fs::read_dir(dir).map_err(read)? {
            let entry = entry.map_err(read)?;
            if entry.file_type().map_err(read)?.is_dir() {
                protocols.push(entry.path());
            }
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
            std::fs::copy(repository().join("uniswap_v3").join(file), dir.join(file))
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

    const POOL: &str = "[[contract]]\nabi = [\"UniswapV3Pool.json\"]\n";

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
}
