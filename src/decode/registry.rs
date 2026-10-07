//! An immutable catalog of contract ABIs and block-ranged registrations.
//!
//! The `abis` directory is walked recursively and every `.json` file in it is loaded as
//! an ABI, shared through [`Arc`]. A file's path under that root, without the extension,
//! is its name — so `abis/uniswap/v3/pool.json` is `uniswap/v3/pool`, and a protocol's
//! versions nest as directories. Each `[[contract]]` identifies a chain, address, ABI
//! name, and explicit protocol tag. Registrations apply from `from_block` (inclusive,
//! default zero) to `to_block` (exclusive, omitted for no upper bound). Ranges for one
//! chain/address cannot overlap.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr as _;
use std::sync::Arc;

use alloy_primitives::{Address, AddressError};
use serde::Deserialize;

use super::abi::{Abi, AbiError};
use crate::wire::envelope::ChainId;

/// One address's ABI and protocol over a half-open block range.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractEntry {
    /// The chain the address is on.
    pub chain: String,
    /// The contract address.
    pub address: String,
    /// The ABI name it decodes with: a file's path under the `abis` root, without `.json`.
    pub abi: String,
    /// The protocol tag on decoded records, independent of the ABI name.
    pub protocol: String,
    /// The first block this registration applies to, inclusive.
    #[serde(default)]
    pub from_block: u64,
    /// The first block this registration no longer applies to, exclusive.
    pub to_block: Option<u64>,
}

/// One registered contract: its shared ABI and explicit protocol tag.
#[derive(Debug, Clone)]
pub struct Contract {
    /// The contract's ABI, shared with every registration that uses it.
    pub abi: Arc<Abi>,
    /// The protocol, for example `uniswap_v3`.
    pub protocol: String,
}

/// The loaded, immutable registry, with registrations sorted by their first block.
///
/// Nested maps let lookups borrow the chain without allocating a tuple key.
#[derive(Debug, Default)]
pub struct ContractRegistry {
    entries: HashMap<ChainId, HashMap<Address, Vec<Registration>>>,
}

#[derive(Debug)]
struct Registration {
    from_block: u64,
    to_block: Option<u64>,
    contract: Contract,
}

/// The ABI catalog and registrations held by a registry file.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryConfig {
    /// The directory walked for ABI files, relative to the registry file's directory.
    /// Omitted means no ABI directory, so only the empty registry loads.
    #[serde(default)]
    pub abis: Option<PathBuf>,
    /// The block-ranged contract registrations.
    #[serde(default)]
    pub contract: Vec<ContractEntry>,
}

impl ContractRegistry {
    /// Loads a registry, resolving ABI paths relative to the registry file's directory.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::RegistryRead`] when the file cannot be read,
    /// [`RegistryError::RegistryParse`] when it does not parse, or a load error.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, RegistryError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|source| RegistryError::RegistryRead {
            path: path.display().to_string(),
            source,
        })?;
        let config: RegistryConfig =
            toml::from_str(&text).map_err(|source| RegistryError::RegistryParse {
                path: path.display().to_string(),
                source,
            })?;
        Self::load(&config, path.parent().unwrap_or_else(|| Path::new(".")))
    }

    /// Builds an immutable registry, resolving the ABI root and relative paths against `base`.
    ///
    /// # Errors
    ///
    /// Rejects an unreadable ABI directory, unreadable or malformed ABIs, unknown ABI
    /// references, invalid addresses, empty or reversed ranges, and overlapping ranges on
    /// one address.
    pub fn load(config: &RegistryConfig, base: impl AsRef<Path>) -> Result<Self, RegistryError> {
        let base = base.as_ref();
        let mut by_name: HashMap<String, Arc<Abi>> = HashMap::new();
        if let Some(abis) = &config.abis {
            let root = base.join(abis);
            let mut files = Vec::new();
            collect_abis(&root, &mut files)?;
            files.sort();
            for path in files {
                // A file's path under the root, without `.json`, is its name: the
                // separators are deliberately kept, so a nested version is `v3/pool`.
                let name = path
                    .strip_prefix(&root)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .replace('\\', "/");
                let name = name.strip_suffix(".json").unwrap_or(&name).to_owned();
                let json =
                    std::fs::read_to_string(&path).map_err(|source| RegistryError::AbiFile {
                        path: path.display().to_string(),
                        source,
                    })?;
                let abi = Abi::from_json(&json).map_err(|source| RegistryError::Abi {
                    path: path.display().to_string(),
                    source,
                })?;
                by_name.insert(name, Arc::new(abi));
            }
        }

        let mut registry = Self::default();
        for entry in &config.contract {
            if entry.to_block.is_some_and(|end| end <= entry.from_block) {
                return Err(RegistryError::InvalidRange {
                    chain: entry.chain.clone(),
                    address: entry.address.clone(),
                    from_block: entry.from_block,
                    to_block: entry.to_block,
                });
            }
            let address = Address::from_str(&entry.address)
                .map_err(|source| RegistryError::Address(AddressError::Hex(source)))?;
            let abi = by_name
                .get(entry.abi.as_str())
                .ok_or_else(|| RegistryError::UnknownAbi {
                    entry: format!("{}.{}", entry.chain, entry.address),
                    name: entry.abi.clone(),
                })?;
            registry
                .entries
                .entry(ChainId::new(&entry.chain))
                .or_default()
                .entry(address)
                .or_default()
                .push(Registration {
                    from_block: entry.from_block,
                    to_block: entry.to_block,
                    contract: Contract {
                        abi: Arc::clone(abi),
                        protocol: entry.protocol.clone(),
                    },
                });
        }

        for (chain, addresses) in &mut registry.entries {
            for (address, registrations) in addresses {
                registrations.sort_unstable_by_key(|registration| registration.from_block);
                for pair in registrations.windows(2) {
                    if pair[0].to_block.is_none_or(|end| end > pair[1].from_block) {
                        return Err(RegistryError::Overlap {
                            chain: chain.to_string(),
                            address: address.to_string(),
                        });
                    }
                }
            }
        }
        Ok(registry)
    }

    /// Borrows the contract registered at `block`, or returns `None` for a miss or gap.
    ///
    /// Lookup performs no I/O or allocation. The upper bound of a range is exclusive.
    #[must_use]
    pub fn contract(&self, chain: &ChainId, address: Address, block: u64) -> Option<&Contract> {
        let registrations = self.entries.get(chain)?.get(&address)?;
        let index = registrations.partition_point(|entry| entry.from_block <= block);
        let registration = registrations.get(index.checked_sub(1)?)?;
        registration
            .to_block
            .is_none_or(|end| block < end)
            .then_some(&registration.contract)
    }

    /// How many distinct addresses are registered, across every chain.
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.entries.values().map(HashMap::len).sum()
    }

    /// Whether no address is registered at all.
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Collects every `.json` file under `dir`, recursing into subdirectories.
///
/// The walk is deterministic: entries are read in directory order and the caller sorts
/// the result, so discovery does not depend on the filesystem's ordering.
fn collect_abis(dir: &Path, files: &mut Vec<PathBuf>) -> Result<(), RegistryError> {
    let entries = std::fs::read_dir(dir).map_err(|source| RegistryError::AbiDirectory {
        path: dir.display().to_string(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| RegistryError::AbiDirectory {
            path: dir.display().to_string(),
            source,
        })?;
        let file_type = entry
            .file_type()
            .map_err(|source| RegistryError::AbiDirectory {
                path: dir.display().to_string(),
                source,
            })?;
        let path = entry.path();
        // Symlinks are neither followed nor treated as files, so a link loop cannot make
        // the walk recurse forever.
        if file_type.is_dir() {
            collect_abis(&path, files)?;
        } else if file_type.is_file() && path.extension().is_some_and(|ext| ext == "json") {
            files.push(path);
        }
    }
    Ok(())
}

/// Why the registry could not be loaded.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    /// The registry file could not be read.
    #[error("read registry file {path}: {source}")]
    RegistryRead {
        /// The path that failed.
        path: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// The registry file did not parse, or a required value was absent.
    #[error("invalid registry file {path}: {source}")]
    RegistryParse {
        /// The path that failed.
        path: String,
        /// The underlying TOML error.
        source: toml::de::Error,
    },
    /// An ABI file could not be read.
    #[error("read {path}: {source}")]
    AbiFile {
        /// The resolved ABI path.
        path: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// The ABI root could not be walked.
    #[error("read ABI directory {path}: {source}")]
    AbiDirectory {
        /// The directory that failed.
        path: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// A named ABI file could not be parsed.
    #[error("invalid ABI at {path}: {source}")]
    Abi {
        /// The resolved ABI path identifies the failed catalog input.
        path: String,
        /// Concrete startup ABI failure, distinct from per-log decoding errors.
        source: AbiError,
    },
    /// An entry's address did not parse.
    #[error(transparent)]
    Address(#[from] AddressError),
    /// A contract references an ABI name that no ABI in the walked directory declares.
    #[error("entry {entry:?} references ABI {name:?}, which no ABI file under `abis` declares")]
    UnknownAbi {
        /// The registration as written.
        entry: String,
        /// The ABI name it referenced.
        name: String,
    },
    /// A registration's exclusive end is not greater than its inclusive start.
    #[error("invalid block range for {chain}.{address}: [{from_block}, {to_block:?})")]
    InvalidRange {
        /// The chain as written.
        chain: String,
        /// The address as written.
        address: String,
        /// The inclusive start.
        from_block: u64,
        /// The exclusive end.
        to_block: Option<u64>,
    },
    /// Two registrations overlap on the same chain and address.
    #[error("overlapping block ranges for {chain}.{address}")]
    Overlap {
        /// The chain.
        chain: String,
        /// The address.
        address: String,
    },
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use super::{ContractEntry, ContractRegistry, RegistryConfig, RegistryError};
    use crate::wire::envelope::ChainId;

    const POOL: &str = "0xd0b53D9277642d899DF5C87A3966A349A798F224";

    /// A registry rooted at `abis` with one registration, sharing the shipped ABI tree.
    fn config(ranges: &[(u64, Option<u64>)]) -> RegistryConfig {
        RegistryConfig {
            abis: Some(PathBuf::from("abis")),
            contract: ranges
                .iter()
                .map(|&(from_block, to_block)| ContractEntry {
                    chain: "base".to_owned(),
                    address: POOL.to_owned(),
                    abi: "uniswap/v3/pool".to_owned(),
                    protocol: "uniswap_v3".to_owned(),
                    from_block,
                    to_block,
                })
                .collect(),
        }
    }

    fn load(config: &RegistryConfig) -> Result<ContractRegistry, RegistryError> {
        ContractRegistry::load(config, env!("CARGO_MANIFEST_DIR"))
    }

    #[test]
    fn ranges_are_sorted_half_open_and_share_an_abi() {
        let mut config = config(&[(20, Some(30)), (10, Some(20)), (40, None)]);
        config.contract[0].protocol = "upgraded_protocol".to_owned();
        let registry = load(&config).expect("valid adjacent ranges and gap");
        let chain = ChainId::new("base");
        let address = POOL.parse().expect("valid address");
        for block in [0, 9, 30, 39] {
            assert!(registry.contract(&chain, address, block).is_none());
        }
        let first = registry
            .contract(&chain, address, 10)
            .expect("inclusive start");
        let second = registry
            .contract(&chain, address, 20)
            .expect("adjacent range");
        assert_eq!(first.protocol, "uniswap_v3");
        assert_eq!(second.protocol, "upgraded_protocol");
        assert!(Arc::ptr_eq(&first.abi, &second.abi));
        for block in [19, 29, 40, u64::MAX] {
            assert!(registry.contract(&chain, address, block).is_some());
        }
        assert!(
            registry
                .contract(&ChainId::new("ethereum"), address, 10)
                .is_none()
        );
        assert!(
            registry
                .contract(&chain, alloy_primitives::Address::ZERO, 10)
                .is_none()
        );
        assert_eq!(registry.len(), 1);
        assert!(!registry.is_empty());
    }

    #[test]
    fn empty_reversed_and_overlapping_ranges_are_rejected() {
        for end in [9, 10] {
            assert!(matches!(
                load(&config(&[(10, Some(end))])),
                Err(RegistryError::InvalidRange { from_block: 10, to_block: Some(value), .. })
                    if value == end
            ));
        }
        for ranges in [
            vec![(10, Some(21)), (20, Some(30))],
            vec![(10, None), (20, Some(30))],
            vec![(10, Some(20)), (10, Some(20))],
            vec![(0, None), (u64::MAX, None)],
        ] {
            assert!(matches!(
                load(&config(&ranges)),
                Err(RegistryError::Overlap { .. })
            ));
        }
    }

    #[test]
    fn ranges_on_different_chains_or_addresses_do_not_conflict() {
        let mut config = config(&[(0, None), (0, None), (0, None)]);
        config.contract[1].chain = "ethereum".to_owned();
        config.contract[2].address = "0x1111111111111111111111111111111111111111".to_owned();
        assert_eq!(load(&config).expect("separate addresses").len(), 3);
    }

    #[test]
    fn range_defaults_and_protocol_are_explicit() {
        let entry: ContractEntry = toml::from_str(&format!(
            "chain = 'base'\naddress = '{POOL}'\nabi = 'pool_abi'\nprotocol = 'uniswap_v3'"
        ))
        .expect("default block range");
        assert_eq!(entry.from_block, 0);
        assert_eq!(entry.to_block, None);
        assert_eq!(entry.protocol, "uniswap_v3");
        assert!(
            toml::from_str::<ContractEntry>(&format!(
                "chain = 'base'\naddress = '{POOL}'\nabi = 'pool_abi'"
            ))
            .is_err()
        );
        assert!(toml::from_str::<RegistryConfig>("[[discovery]]\nchain = 'base'").is_err());
        assert!(ContractRegistry::default().is_empty());
    }

    /// A registration that references an ABI no file under the walked root declares is a
    /// startup error, so a typo cannot silently decode nothing.
    #[test]
    fn an_unknown_abi_name_is_rejected() {
        let mut unknown = config(&[(0, None)]);
        unknown.contract[0].abi = "missing".to_owned();
        assert!(matches!(
            load(&unknown),
            Err(RegistryError::UnknownAbi { .. })
        ));
    }

    /// The shipped tree is walked recursively and a nested file is addressable by its path
    /// under the root without the extension, separate from the flat names beside it.
    #[test]
    fn nested_abi_files_are_named_by_their_path_under_the_root() {
        let root =
            std::env::temp_dir().join(format!("indexer-abis-{}-{}", std::process::id(), line!()));
        let nested = root.join("uniswap/v3");
        std::fs::create_dir_all(&nested).expect("create nested ABI dir");
        std::fs::write(
            nested.join("pool.json"),
            include_str!("../../abis/uniswap/v3/pool.json"),
        )
        .expect("write nested ABI");
        // A file with another extension is not an ABI and must not be walked.
        std::fs::write(root.join("notes.md"), "not an ABI").expect("write ignored file");

        let config = RegistryConfig {
            abis: Some(PathBuf::from(".")),
            contract: vec![ContractEntry {
                chain: "base".to_owned(),
                address: POOL.to_owned(),
                abi: "uniswap/v3/pool".to_owned(),
                protocol: "uniswap_v3".to_owned(),
                from_block: 0,
                to_block: None,
            }],
        };
        let registry = ContractRegistry::load(&config, &root).expect("nested ABI loads");
        assert_eq!(registry.len(), 1);

        std::fs::remove_dir_all(&root).expect("clean up");
    }

    /// An ABI root that is not there is a startup error naming the directory, rather than a
    /// run that quietly decodes nothing.
    #[test]
    fn a_missing_abi_directory_is_rejected() {
        let config = RegistryConfig {
            abis: Some(PathBuf::from("no/such/abis")),
            contract: Vec::new(),
        };
        assert!(matches!(
            load(&config),
            Err(RegistryError::AbiDirectory { .. })
        ));
    }

    #[test]
    fn repository_registry_loads_relative_to_its_file() {
        let registry = ContractRegistry::from_file(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("registry.toml"),
        )
        .expect("repository registry loads");
        assert_eq!(registry.len(), 4);
    }
}
