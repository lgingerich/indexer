//! Which ABI applies where, and what discovery reveals.
//!
//! Three lists, all data, no code per protocol:
//!
//! ```toml
//! [[abi]]
//! name = "uniswap_v3_pool"
//! path = "abis/uniswap_v3_pool.json"
//!
//! [[contract]]
//! chain = "base"
//! address = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
//! abi = "uniswap_v3_factory"
//!
//! [[discovery]]
//! chain = "base"
//! address = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
//! event = "PoolCreated(address,address,uint24,int24,address)"
//! child = "pool"
//! abi = "uniswap_v3_pool"
//! ```
//!
//! # Why an ABI is named once
//!
//! A protocol like Uniswap V3 has thousands of pools across many chains with an
//! *identical* ABI, so naming an ABI file per address would repeat the same file
//! thousands of times. Here the ABI is a shared [`Arc`], loaded once, and an address is
//! a line.
//!
//! # Discovery
//!
//! A pool created by a factory is not in the registry file, because it did not exist when
//! that file was written, so it cannot be a `[[contract]]`. A `[[discovery]]` rule closes
//! that: when the factory's creation event decodes, the named argument holds the new
//! child's address, and the child is registered with the named ABI.
//!
//! Registration is **deterministic**: the child's ABI is already loaded, so no network
//! is involved and the registry never has to block. The only ordering fact relied on is
//! that a factory emits its creation event before the child emits anything — true by
//! construction, so a sequential pass over the stream registers the child before its
//! first log. The transform surfaces the effect and the sink applies it; see
//! [`super::transform`] and [`super::sink`].
//!
//! The ABI is keyed by `(chain, address)` only, so a protocol that does not put its
//! pools at an address — Uniswap V4's `PoolManager`, where a pool is a `bytes32` id — is
//! simply one `[[contract]]` with no `[[discovery]]`. Logical pools need no entry at all.
//!
//! # Known limitation
//!
//! A contract applies at every height. A proxy that upgrades changes its ABI at a
//! height, which this cannot express; [`AbiRegistry::contract`] already takes a `block`
//! so a block-ranged version can replace it without the decoder changing. A rule also
//! cannot resolve a proxy or fetch an unknown ABI — both are additive behind the same
//! rule shape.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alloy_primitives::{Address, B256};
use serde::Deserialize;

use super::abi::{Abi, DecodedEvent};
use crate::wire::envelope::ChainId;

/// One ABI file, named so a `[[contract]]` or a `[[discovery]]` can reference it.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AbiEntry {
    /// The name to reference it by, for example `uniswap_v3_pool`.
    pub name: String,
    /// The ABI file, relative to the settings file's directory.
    pub path: PathBuf,
}

/// One address that decodes with an ABI from the first block.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractEntry {
    /// The chain the address is on.
    pub chain: String,
    /// The contract address.
    pub address: String,
    /// The `[[abi]]` name it decodes with.
    pub abi: String,
}

/// One factory the registry learns children from.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryEntry {
    /// The chain the factory is on.
    pub chain: String,
    /// The factory address that emits the creation event.
    pub address: String,
    /// The creation event's signature, for example
    /// `PoolCreated(address,address,uint24,int24,address)`.
    pub event: String,
    /// The decoded argument holding the new child's address.
    pub child: String,
    /// The `[[abi]]` name the child decodes with.
    pub abi: String,
}

/// One contract as the registry knows it: its ABI and what it is.
///
/// The ABI is an `Arc` because one ABI serves every address that uses it: a registry
/// stores it once and clones a pointer, not the event map.
#[derive(Debug, Clone)]
pub struct Contract {
    /// The contract's ABI, shared with every address that uses it.
    pub abi: Arc<Abi>,
    /// What the contract is, for example `uniswap_v3_pool`.
    pub protocol: String,
}

/// One decoded log's discovery effect: which address is a child, and how to decode it.
#[derive(Debug)]
pub struct Discovery {
    /// The child contract the creating event revealed, to register.
    pub child: Address,
    /// The ABI the child decodes with.
    pub abi: Arc<Abi>,
    /// What the child is, for example `uniswap_v3_pool`.
    pub protocol: String,
}

/// What the decoder needs from a registry: the ABI for an address, and what a decoded
/// log revealed.
///
/// Kept a trait so the transform can be driven by a test double without a settings file,
/// and so a block-ranged or proxy-aware registry can replace the concrete one without
/// the decoder changing.
pub trait AbiRegistry {
    /// The contract for `address` on `chain` as of `block`, or `None` if unknown.
    ///
    /// Must not do I/O on the hot path: a miss returns `None`, and the caller drops the
    /// log rather than stalling the pipeline behind a lookup.
    fn contract(&self, chain: &ChainId, address: Address, block: u64) -> Option<Contract>;

    /// The discovery rule a just-decoded record triggered, if any.
    ///
    /// `address` is the contract that emitted the log, `selector` is the event that
    /// matched, and `event` is the decoded event. The default answers `None`, so a
    /// registry without discovery needs nothing.
    fn discovery(
        &self,
        _chain: &ChainId,
        _address: Address,
        _selector: B256,
        _event: &DecodedEvent,
    ) -> Option<Discovery> {
        None
    }
}

/// The loaded registry: one shared ABI per address, plus the discovery rules.
#[derive(Debug, Default)]
pub struct ContractRegistry {
    entries: HashMap<(ChainId, Address), Contract>,
    rules: HashMap<(ChainId, Address, B256), Rule>,
}

/// A discovery rule, resolved against its factory's ABI at load.
#[derive(Debug)]
struct Rule {
    /// The decoded argument holding the new child's address.
    child: String,
    /// The ABI the child decodes with.
    abi: Arc<Abi>,
    /// What the child is, for the decoded record it produces.
    protocol: String,
}

/// The three lists a registry file holds, on their own so the file is self-contained.
///
/// Parse this with [`toml::from_str`], or load it with [`ContractRegistry::from_file`].
/// The shape is documented in the module docs; the entries carry their own docs.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryConfig {
    /// The ABI files, named so a contract or rule can reference one.
    #[serde(default)]
    pub abi: Vec<AbiEntry>,
    /// The addresses that decode with an ABI from the first block.
    #[serde(default)]
    pub contract: Vec<ContractEntry>,
    /// The factories the registry learns child contracts from.
    #[serde(default)]
    pub discovery: Vec<DiscoveryEntry>,
}

impl ContractRegistry {
    /// Loads ABIs, static contracts, and discovery rules from a registry file.
    ///
    /// The file is [`RegistryConfig`]; ABI paths in it resolve relative to the file's own
    /// directory, so a moved file keeps working.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::RegistryRead`] when the file cannot be read,
    /// [`RegistryError::RegistryParse`] when it does not parse, and otherwise whatever
    /// [`Self::load`] returns.
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
        Self::load(
            &config.abi,
            &config.contract,
            &config.discovery,
            path.parent().unwrap_or_else(|| Path::new(".")),
        )
    }

    /// Loads the ABIs, the static contracts, and the discovery rules.
    ///
    /// Prefer [`Self::from_file`]; this is the in-memory form, for a caller that has the
    /// lists already. `base` is the directory relative ABI paths resolve against: a path
    /// in a registry file reads as relative to that file, not to the process's working
    /// directory.
    ///
    /// # Errors
    ///
    /// Returns an error when an ABI cannot be read or is not an ABI, when a name
    /// referenced by a contract or rule is not declared, when an address does not parse,
    /// when two contracts claim one `(chain, address)`, and when a discovery rule names
    /// an event its factory's ABI does not declare. Each is a startup error rather than a
    /// silent no-op: a registry that decodes less than it was told to is a wrong answer
    /// that looks like a quiet chain.
    pub fn load(
        abis: &[AbiEntry],
        contracts: &[ContractEntry],
        discoveries: &[DiscoveryEntry],
        base: impl AsRef<Path>,
    ) -> Result<Self, RegistryError> {
        let base = base.as_ref();
        let mut registry = Self::default();

        // A load-time name map, deliberately local: contracts and rules resolve their ABI
        // by name here, and once both loops finish nothing references a name again, so the
        // registry retains one `Arc<Abi>` per address and drops the map. A duplicate name
        // is refused rather than last-wins, because every other registry mistake is a
        // startup error and a name is a reference, not an address.
        let mut by_name: HashMap<String, Arc<Abi>> = HashMap::with_capacity(abis.len());
        for entry in abis {
            let path = base.join(&entry.path);
            let json = std::fs::read_to_string(&path).map_err(|source| RegistryError::AbiFile {
                path: path.display().to_string(),
                source,
            })?;
            if by_name
                .insert(entry.name.clone(), Arc::new(Abi::from_json(&json)?))
                .is_some()
            {
                return Err(RegistryError::DuplicateAbi {
                    name: entry.name.clone(),
                });
            }
        }
        let abi_named = |name: &str, entry: &str| -> Result<Arc<Abi>, RegistryError> {
            by_name
                .get(name)
                .cloned()
                .ok_or_else(|| RegistryError::UnknownAbi {
                    entry: entry.to_owned(),
                    name: name.to_owned(),
                })
        };

        for contract in contracts {
            let address = parse_address(&contract.chain, &contract.address)?;
            let origin = format!("{}.{}", contract.chain, contract.address);
            let abi = abi_named(&contract.abi, &origin)?;
            let key = (ChainId::new(&contract.chain), address);
            // The ABI's name is the protocol tag: an ABI is exactly the granularity a
            // protocol has, and the settings already name it.
            let entry = Contract {
                abi,
                protocol: contract.abi.clone(),
            };
            if registry.entries.insert(key, entry).is_some() {
                return Err(RegistryError::Duplicate {
                    chain: contract.chain.clone(),
                    address: contract.address.clone(),
                });
            }
        }

        for discovery in discoveries {
            let address = parse_address(&discovery.chain, &discovery.address)?;
            let origin = format!("{}.{}", discovery.chain, discovery.address);
            let abi = abi_named(&discovery.abi, &origin)?;
            // The factory must itself be registered to emit the event, so the rule is
            // validated against the factory's ABI.
            let selector = registry
                .entries
                .get(&(ChainId::new(&discovery.chain), address))
                .and_then(|factory| factory.abi.selector(&discovery.event))
                .ok_or_else(|| RegistryError::Discovery {
                    chain: discovery.chain.clone(),
                    address: discovery.address.clone(),
                    detail: format!(
                        "event {:?} is not declared by the factory's ABI",
                        discovery.event
                    ),
                })?;
            registry.rules.insert(
                (ChainId::new(&discovery.chain), address, selector),
                Rule {
                    child: discovery.child.clone(),
                    protocol: discovery.abi.clone(),
                    abi,
                },
            );
        }

        Ok(registry)
    }

    /// Registers a contract learned at runtime, from a [`Discovery`] the transform
    /// surfaced.
    ///
    /// A no-op when the address is already known, which is the normal case: a factory
    /// replaces a pool, or a replayed stream re-registers what a prior pass found.
    /// Discovery adds an address to the set that decodes; it never overrides one that is
    /// already there, because a static registration is a deliberate statement and a
    /// learned one is not.
    pub fn register_discovered(&mut self, chain: &ChainId, discovery: Discovery) {
        self.entries
            .entry((chain.clone(), discovery.child))
            .or_insert_with(|| Contract {
                abi: discovery.abi,
                protocol: discovery.protocol,
            });
    }

    /// Whether any address is registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// How many addresses are registered, for logging what was picked up.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

impl AbiRegistry for ContractRegistry {
    fn contract(&self, chain: &ChainId, address: Address, _block: u64) -> Option<Contract> {
        self.entries.get(&(chain.clone(), address)).cloned()
    }

    fn discovery(
        &self,
        chain: &ChainId,
        address: Address,
        selector: B256,
        event: &DecodedEvent,
    ) -> Option<Discovery> {
        // The rule is keyed by the factory that emitted the log, which is the log's
        // address — the child's address is in the decoded argument, not the map key.
        let rule = self.rules.get(&(chain.clone(), address, selector))?;
        Some(Discovery {
            child: child_address(event, &rule.child)?,
            abi: Arc::clone(&rule.abi),
            protocol: rule.protocol.clone(),
        })
    }
}

/// Parses an address, naming the entry it came from when it does not.
fn parse_address(chain: &str, address: &str) -> Result<Address, RegistryError> {
    address.parse().map_err(|_| RegistryError::Address {
        entry: format!("{chain}.{address}"),
    })
}

/// Reads a child address from the decoded event's named argument `name`.
fn child_address(event: &DecodedEvent, name: &str) -> Option<Address> {
    let arg = event
        .indexed
        .iter()
        .chain(&event.body)
        .find(|arg| arg.name == name)?;
    match &arg.value {
        crate::wire::typed::TypedValue::Address { value } => Some(*value),
        _ => None,
    }
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
    ///
    /// Separate from [`Self::RegistryRead`] so both causes keep their own type. They
    /// shared one `Box<dyn Error>` variant before, which erased them to a string and
    /// meant a caller that recovers from a missing file — falling back to an empty
    /// registry — could not tell that from a file that exists but is malformed, which is
    /// the case that must not be swallowed.
    #[error("invalid registry file {path}: {source}")]
    RegistryParse {
        /// The path that failed.
        path: String,
        /// `toml`'s error, which carries the line and column.
        source: toml::de::Error,
    },
    /// An ABI file could not be read.
    #[error("read {path}: {source}")]
    AbiFile {
        /// The path that failed, as resolved against the registry file's directory.
        path: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// An ABI could not be parsed, or the registry could not be read otherwise.
    #[error(transparent)]
    Abi(#[from] super::abi::DecodeError),
    /// An entry's address did not parse.
    #[error("registration {entry:?} has an invalid address")]
    Address {
        /// The entry as written.
        entry: String,
    },
    /// A `[[abi]]` entry references an ABI name that no ABI declares.
    #[error("entry {entry:?} references ABI {name:?}, which no [[abi]] declares")]
    UnknownAbi {
        /// The registration as written, for locating it.
        entry: String,
        /// The ABI name it referenced.
        name: String,
    },
    /// Two `[[abi]]` entries declare one name.
    #[error("two [[abi]] entries are named {name:?}; a name must label one ABI")]
    DuplicateAbi {
        /// The name both entries claimed.
        name: String,
    },
    /// Two registry entries claim one `(chain, address)`.
    #[error("both {chain}.{address} are registered; one address decodes with one ABI")]
    Duplicate {
        /// The chain, as written.
        chain: String,
        /// The address, as written.
        address: String,
    },
    /// A discovery rule's event is not declared by its factory's ABI.
    #[error("discovery rule for {chain}.{address}: {detail}")]
    Discovery {
        /// The chain, as written.
        chain: String,
        /// The factory address, as written.
        address: String,
        /// What was wrong with the rule.
        detail: String,
    },
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are allowed
// them per the repository test style, since a failed expectation there means the fixture
// or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use alloy_primitives::{Address, B256};

    use super::{AbiEntry, ContractEntry, ContractRegistry, Discovery, DiscoveryEntry};
    use crate::decode::AbiRegistry as _;
    use crate::decode::abi::Abi;
    use crate::wire::envelope::ChainId;

    const POOL: &str = "0xd0b53D9277642d899DF5C87A3966A349A798F224";
    const FACTORY: &str = "0x1F98431c8aD98523631AE4a59f267346ea31F984";
    const SIGNATURE: &str = "PoolCreated(address,address,uint24,int24,address)";

    /// The crate's ABI directory, so the fixture does not have to be copied.
    fn abi_dir() -> std::path::PathBuf {
        std::path::PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/abis"))
    }

    fn pool_abi() -> AbiEntry {
        AbiEntry {
            name: "uniswap_v3_pool".to_owned(),
            path: std::path::PathBuf::from("uniswap_v3_pool.json"),
        }
    }

    fn factory_abi() -> AbiEntry {
        AbiEntry {
            name: "uniswap_v3_factory".to_owned(),
            path: std::path::PathBuf::from("uniswap_v3_factory.json"),
        }
    }

    fn contract(chain: &str, address: &str, abi: &str) -> ContractEntry {
        ContractEntry {
            chain: chain.to_owned(),
            address: address.to_owned(),
            abi: abi.to_owned(),
        }
    }

    fn discovery(
        chain: &str,
        address: &str,
        event: &str,
        child: &str,
        abi: &str,
    ) -> DiscoveryEntry {
        DiscoveryEntry {
            chain: chain.to_owned(),
            address: address.to_owned(),
            event: event.to_owned(),
            child: child.to_owned(),
            abi: abi.to_owned(),
        }
    }

    /// One ABI shared by several addresses: the point of naming an ABI once, since a
    /// protocol like Uniswap V3 has thousands of pools with identical ABIs.
    #[test]
    fn one_abi_serves_many_addresses() {
        let registry = ContractRegistry::load(
            &[pool_abi()],
            &[
                contract("base", POOL, "uniswap_v3_pool"),
                contract(
                    "base",
                    "0x1111111111111111111111111111111111111111",
                    "uniswap_v3_pool",
                ),
                contract("ethereum", POOL, "uniswap_v3_pool"),
            ],
            &[],
            abi_dir(),
        )
        .expect("the registry loads");

        assert_eq!(registry.len(), 3);
        for (chain, address) in [
            ("base", POOL),
            ("base", "0x1111111111111111111111111111111111111111"),
            ("ethereum", POOL),
        ] {
            let chain = ChainId::new(chain);
            let address: Address = address.parse().expect("an address");
            assert!(registry.contract(&chain, address, 1).is_some());
        }
    }

    #[test]
    fn an_unknown_abi_name_is_refused() {
        let result = ContractRegistry::load(
            &[pool_abi()],
            &[contract("base", POOL, "nope")],
            &[],
            abi_dir(),
        );
        assert!(result.is_err(), "a dangling ABI reference must be caught");
    }

    /// A name is a reference, not an address, so two `[[abi]]` entries claiming one name
    /// is a malformed catalog — a startup error rather than a silent last-wins that leaves
    /// whichever contract referenced it decoding with the other's ABI.
    #[test]
    fn two_abis_with_one_name_are_refused() {
        let result = ContractRegistry::load(&[pool_abi(), pool_abi()], &[], &[], abi_dir());
        assert!(
            matches!(result, Err(super::RegistryError::DuplicateAbi { .. })),
            "a duplicate ABI name must be caught"
        );
    }

    #[test]
    fn a_duplicate_address_is_refused() {
        let result = ContractRegistry::load(
            &[pool_abi()],
            &[
                contract("base", POOL, "uniswap_v3_pool"),
                contract("base", POOL, "uniswap_v3_pool"),
            ],
            &[],
            abi_dir(),
        );
        assert!(result.is_err(), "one address must decode with one ABI");
    }

    #[test]
    fn a_missing_abi_is_an_error() {
        let mut bad = pool_abi();
        bad.path = std::path::PathBuf::from("nope.json");
        assert!(ContractRegistry::load(&[bad], &[], &[], abi_dir()).is_err());
    }

    #[test]
    fn a_bad_address_is_refused() {
        let result = ContractRegistry::load(
            &[pool_abi()],
            &[contract("base", "not-an-address", "uniswap_v3_pool")],
            &[],
            abi_dir(),
        );
        assert!(result.is_err());
    }

    /// A discovery rule that names an event its factory's ABI does not declare is a
    /// startup error, not a rule that silently never fires.
    #[test]
    fn a_rule_for_an_unknown_event_is_refused() {
        let registry = ContractRegistry::load(
            &[pool_abi()],
            &[contract("base", FACTORY, "uniswap_v3_pool")],
            &[discovery(
                "base",
                FACTORY,
                "Nope(uint256)",
                "pool",
                "uniswap_v3_pool",
            )],
            abi_dir(),
        );
        assert!(
            registry.is_err(),
            "a rule for a missing event must be caught"
        );
    }

    /// No entries is an empty registry, not an error: running without decoding is
    /// legitimate and the caller says so at startup.
    #[test]
    fn no_entries_is_an_empty_registry() {
        let registry = ContractRegistry::load(&[], &[], &[], abi_dir()).expect("empty is fine");
        assert!(registry.is_empty());
    }

    /// Discovery never overrides a static registration. A learned address is
    /// speculation; a registration in the file is a deliberate statement, so a rule that
    /// reveals an address already claimed must leave that contract's ABI and protocol
    /// alone rather than quietly replacing them.
    #[test]
    fn discovery_does_not_override_a_static_registration() {
        let mut registry = ContractRegistry::load(
            &[pool_abi(), factory_abi()],
            &[contract("base", POOL, "uniswap_v3_pool")],
            &[],
            abi_dir(),
        )
        .expect("the registry loads");
        let chain = ChainId::new("base");
        let child: Address = POOL.parse().expect("an address");

        registry.register_discovered(
            &chain,
            Discovery {
                child,
                abi: Arc::new(
                    Abi::from_json(include_str!("../../abis/uniswap_v3_factory.json"))
                        .expect("factory ABI"),
                ),
                protocol: "uniswap_v3_factory".to_owned(),
            },
        );

        // The static protocol (its ABI name) stands; the learned one did not win.
        assert_eq!(
            registry
                .contract(&chain, child, 1)
                .expect("registered")
                .protocol,
            "uniswap_v3_pool"
        );
        assert_eq!(registry.len(), 1, "no second entry was added");
    }

    /// Re-registering a known child is a no-op, which is the normal case: a factory
    /// replaces a pool, or a replayed stream re-discovers what a prior pass found. The
    /// count must not grow.
    #[test]
    fn re_registering_a_discovered_child_is_a_no_op() {
        let mut registry = ContractRegistry::default();
        let chain = ChainId::new("base");
        let child: Address = POOL.parse().expect("an address");
        // A second, *different* discovery of the same address must be inert: the first
        // wins, because discovery only adds addresses and never re-points one.
        let discovery = |protocol: &str| Discovery {
            child,
            abi: Arc::new(
                Abi::from_json(include_str!("../../abis/uniswap_v3_pool.json")).expect("pool ABI"),
            ),
            protocol: protocol.to_owned(),
        };

        registry.register_discovered(&chain, discovery("uniswap_v3_pool"));
        registry.register_discovered(&chain, discovery("uniswap_v3_factory"));

        assert_eq!(
            registry.len(),
            1,
            "a second discovery of the same child is inert"
        );
        assert_eq!(
            registry
                .contract(&chain, child, 1)
                .expect("registered")
                .protocol,
            "uniswap_v3_pool",
            "the first discovery stands; a later one does not re-point the address"
        );
    }

    /// A registry file loads through `from_file`, resolving its ABI paths against the
    /// file's own directory rather than the working directory.
    #[test]
    fn a_registry_file_loads_and_resolves_its_own_abi_dir() {
        let dir = std::env::temp_dir().join(format!("indexer-registry-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let registry_path = dir.join("registry.toml");
        std::fs::write(
            &registry_path,
            r#"
[[abi]]
name = "uniswap_v3_pool"
path = "uniswap_v3_pool.json"

[[contract]]
chain = "base"
address = "0xd0b53D9277642d899DF5C87A3966A349A798F224"
abi = "uniswap_v3_pool"
"#,
        )
        .expect("write registry file");
        std::fs::copy(
            abi_dir().join("uniswap_v3_pool.json"),
            dir.join("uniswap_v3_pool.json"),
        )
        .expect("copy the ABI beside it");

        let registry = ContractRegistry::from_file(&registry_path).expect("the file loads");
        assert_eq!(registry.len(), 1);
        std::fs::remove_dir_all(&dir).expect("clean up");
    }

    /// End to end: a real Uniswap V3 `PoolCreated` log decodes, the discovery rule reads
    /// the pool address out of it, and registering that address makes the pool decode.
    ///
    /// Checked against the factory's real event — topics carry `token0,token1,fee` and
    /// the pool address is the *second word of `data`*, not a topic — rather than a
    /// convenient shape.
    #[test]
    fn a_factory_event_reveals_and_registers_its_pool() {
        use alloy_primitives::{B256, TxHash};

        use crate::decode::abi::Abi;
        use crate::decode::transform::Transform;
        use crate::wire::datasets::evm::Log;
        use crate::wire::envelope::{Envelope, Event};

        let selector = Abi::from_json(include_str!("../../abis/uniswap_v3_factory.json"))
            .expect("factory ABI")
            .selector(SIGNATURE)
            .expect("declares PoolCreated");

        let mut registry = ContractRegistry::load(
            &[factory_abi(), pool_abi()],
            &[contract("base", FACTORY, "uniswap_v3_factory")],
            &[discovery(
                "base",
                FACTORY,
                SIGNATURE,
                "pool",
                "uniswap_v3_pool",
            )],
            abi_dir(),
        )
        .expect("the registry loads");

        let child: Address = POOL.parse().expect("a pool address");
        let mut data = Vec::new();
        data.extend_from_slice(word(60).as_slice()); // tickSpacing: int24
        data.extend_from_slice(address_word(child).as_slice()); // pool: address
        let log = Log {
            log_index: 1,
            transaction_hash: TxHash::from([0x01; 32]),
            address: FACTORY.parse().expect("a factory address"),
            topic0: Some(selector),
            topic1: Some(address_word(Address::from([0x11; 20]))),
            topic2: Some(address_word(Address::from([0x22; 20]))),
            topic3: Some(word(3_000)),
            data: data.into(),
            block_number: 100,
            block_hash: B256::from([0x02; 32]),
            ..Log::default()
        };

        let applied = Transform::apply(
            &registry,
            &Envelope::new(ChainId::new("base"), Event::Log(Box::new(log))),
        );
        assert!(
            applied.error.is_none(),
            "the factory log decodes: {:?}",
            applied.error
        );
        let discovery = applied.discovery.expect("a PoolCreated reveals a pool");
        assert_eq!(discovery.child, child);

        // The pool is unknown until the rule fires, and decodes with the pool ABI after.
        let chain = ChainId::new("base");
        assert!(registry.contract(&chain, child, 100).is_none());
        registry.register_discovered(&chain, discovery);
        assert_eq!(
            registry
                .contract(&chain, child, 100)
                .expect("registered")
                .protocol,
            "uniswap_v3_pool"
        );
    }

    /// A 32-byte big-endian word holding `value`, the form an ABI integer takes.
    fn word(value: u64) -> B256 {
        let mut word = [0u8; 32];
        word[24..].copy_from_slice(&value.to_be_bytes());
        B256::from(word)
    }

    /// A 32-byte word holding an address in its low 20 bytes.
    fn address_word(address: Address) -> B256 {
        let mut word = [0u8; 32];
        word[12..].copy_from_slice(address.as_slice());
        B256::from(word)
    }
}
