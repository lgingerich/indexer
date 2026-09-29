//! The contract registry: which ABI applies where, and what it decodes into.
//!
//! A contract is described once, and its addresses are listed under it:
//!
//! ```toml
//! [[protocol]]
//! name = "uniswap_v3"
//! abi = "abis/uniswap_v3_pool.json"
//!
//! [[protocol.deployment]]
//! chain = "base"
//! address = "0xd0b53D9277642d899DF5C87A3966A349A798F224"
//! ```
//!
//! # Why a list rather than filenames
//!
//! Encoding the tag in a filename works for one deployment and stops working at scale.
//! A protocol like Uniswap V3 has thousands of pools across many chains: naming each ABI
//! file for the protocol means repeating the protocol thousands of times, and the ABI is
//! *identical* across every one of them. Here the ABI is referenced once and the
//! addresses are lines.
//!
//! # What an entry carries that an ABI cannot
//!
//! **`name`** — what the contract is. An ABI is a list of signatures; nothing in it says
//! "this is a Uniswap V3 pool". That is a fact about a deployed contract and it travels
//! onto every [`Decoded`](crate::wire::envelope::Decoded) record.
//!
//! Deliberately *not* the dataset. Where an event's rows belong depends on context this
//! registry does not have — which token a pool trades, how many decimals it has — and on
//! modeling choices that change for reasons decoding should not care about. See
//! [`super`] for why the projection lives downstream.
//!
//! # Known limitation
//!
//! A deployment applies at every height. A proxy that upgrades changes its ABI at a
//! height, which this cannot express; the [`AbiRegistry`] seam is
//! what a block-ranged version replaces.

use std::collections::HashMap;

use crate::wire::envelope::ChainId;
use alloy_primitives::Address;
use serde::Deserialize;

use super::{Abi, AbiRegistry, RegistryError};

/// One protocol: its name, its ABI, and where it is deployed.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolEntry {
    /// What the contract is, for example `uniswap_v3`.
    ///
    /// Travels onto every decoded record, so a consumer can route on protocol rather
    /// than on addresses.
    pub name: String,
    /// The ABI file, relative to the settings file's directory.
    pub abi: std::path::PathBuf,
    /// Where this protocol is deployed.
    #[serde(default)]
    pub deployment: Vec<Deployment>,
}

/// One contract address running a protocol.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deployment {
    /// The chain the address is on.
    pub chain: String,
    /// The contract address.
    pub address: String,
}

/// The loaded registry: one ABI per address, with what it decodes into.
#[derive(Debug, Default)]
pub struct ContractRegistry {
    entries: HashMap<(ChainId, Address), Entry>,
}

/// What the registry knows about one address.
#[derive(Debug, Clone)]
struct Entry {
    abi: Abi,
    protocol: String,
}

impl ContractRegistry {
    /// Loads every protocol and its deployments.
    ///
    /// `base` is the directory relative ABI paths resolve against, which is the
    /// settings file's own directory: a path in the settings file reads as relative to
    /// it, not to the process's working directory.
    ///
    /// # Errors
    ///
    /// Returns an error when an ABI cannot be read or is not an ABI, when an address
    /// does not parse, and when two entries claim one `(chain, address)`. The last is an
    /// error rather than a last-wins: which ABI decoded a log would otherwise depend on
    /// file order.
    pub fn load(
        protocols: &[ProtocolEntry],
        base: impl AsRef<std::path::Path>,
    ) -> Result<Self, RegistryError> {
        let base = base.as_ref();
        let mut registry = Self::default();
        for protocol in protocols {
            let path = base.join(&protocol.abi);
            let json = std::fs::read_to_string(&path).map_err(|error| RegistryError::Abi {
                detail: format!("read {}: {error}", path.display()),
            })?;
            let abi = Abi::from_json(&json)?;

            for deployment in &protocol.deployment {
                let address: Address =
                    deployment
                        .address
                        .parse()
                        .map_err(|_| RegistryError::Address {
                            entry: format!("{}.{}", deployment.chain, deployment.address),
                        })?;
                let key = (ChainId::new(&deployment.chain), address);
                let entry = Entry {
                    abi: abi.clone(),
                    protocol: protocol.name.clone(),
                };
                if registry.entries.insert(key, entry).is_some() {
                    return Err(RegistryError::Duplicate {
                        chain: deployment.chain.clone(),
                        address: deployment.address.clone(),
                    });
                }
            }
        }
        Ok(registry)
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

    /// What the registry knows about an address, for the decode stage to stamp onto the
    /// record it produces.
    ///
    /// The trait method forwards here, so a caller holding a `dyn AbiRegistry` — which
    /// is what the transform holds — gets the same answer as one holding a
    /// [`ContractRegistry`] directly.
    #[must_use]
    fn describe_entry(&self, chain: &ChainId, address: Address) -> Option<&str> {
        self.entries
            .get(&(chain.clone(), address))
            .map(|entry| entry.protocol.as_str())
    }
}

impl AbiRegistry for ContractRegistry {
    fn abi(&self, chain: &ChainId, address: Address, _block: u64) -> Option<&Abi> {
        self.entries
            .get(&(chain.clone(), address))
            .map(|entry| &entry.abi)
    }

    fn describe(&self, chain: &ChainId, address: Address) -> Option<&str> {
        self.describe_entry(chain, address)
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use crate::wire::envelope::ChainId;
    use alloy_primitives::Address;

    use super::{ContractRegistry, ProtocolEntry};
    use crate::decode::AbiRegistry as _;

    const POOL: &str = "0xd0b53D9277642d899DF5C87A3966A349A798F224";

    /// A protocol entry pointing at the crate's own ABI.
    fn entry(name: &str, deployments: Vec<(&str, &str)>) -> ProtocolEntry {
        ProtocolEntry {
            name: name.to_owned(),
            abi: std::path::PathBuf::from("uniswap_v3_pool.json"),
            deployment: deployments
                .into_iter()
                .map(|(chain, address)| super::Deployment {
                    chain: chain.to_owned(),
                    address: address.to_owned(),
                })
                .collect(),
        }
    }

    /// The crate's ABI directory, so the fixture does not have to be copied.
    fn abi_dir() -> std::path::PathBuf {
        std::path::PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/abis"))
    }

    /// One ABI shared by several addresses: the point of a list over filename-per-address,
    /// since a protocol like Uniswap V3 has thousands of pools with identical ABIs.
    #[test]
    fn one_protocol_serves_many_addresses() {
        let registry = ContractRegistry::load(
            &[entry(
                "uniswap_v3",
                vec![
                    ("base", POOL),
                    ("base", "0x1111111111111111111111111111111111111111"),
                    ("ethereum", POOL),
                ],
            )],
            abi_dir(),
        )
        .expect("the registry loads");

        assert_eq!(registry.len(), 3);
        // Every deployment answers with the same ABI and the same identity.
        for (chain, address) in [
            ("base", POOL),
            ("base", "0x1111111111111111111111111111111111111111"),
            ("ethereum", POOL),
        ] {
            let chain = ChainId::new(chain);
            let address: Address = address.parse().expect("an address");
            assert!(registry.abi(&chain, address, 1).is_some());
            assert_eq!(registry.describe_entry(&chain, address), Some("uniswap_v3"));
        }
    }

    /// The protocol travels with the address, which is what an ABI JSON cannot supply:
    /// nothing in a list of signatures says what the contract is.
    #[test]
    fn a_registry_entry_says_what_a_contract_is() {
        let registry =
            ContractRegistry::load(&[entry("uniswap_v3", vec![("base", POOL)])], abi_dir())
                .expect("the registry loads");

        let address: Address = POOL.parse().expect("an address");
        assert_eq!(
            registry.describe_entry(&ChainId::new("base"), address),
            Some("uniswap_v3")
        );
        // A different chain is a miss, so one chain's registry does not leak into another.
        assert_eq!(
            registry.describe_entry(&ChainId::new("ethereum"), address),
            None
        );
    }

    /// Two entries claiming one address is an error rather than last-wins: which ABI
    /// decoded a log would otherwise depend on the order they were written.
    #[test]
    fn a_duplicate_address_is_refused() {
        let result = ContractRegistry::load(
            &[
                entry("uniswap_v3", vec![("base", POOL)]),
                entry("something_else", vec![("base", POOL)]),
            ],
            abi_dir(),
        );
        assert!(result.is_err(), "one address must decode with one ABI");
    }

    /// An ABI path that does not resolve is an error naming it, rather than a registry
    /// that silently decodes nothing.
    #[test]
    fn a_missing_abi_is_an_error() {
        let mut bad = entry("uniswap_v3", vec![("base", POOL)]);
        bad.abi = std::path::PathBuf::from("nope.json");
        assert!(ContractRegistry::load(&[bad], abi_dir()).is_err());
    }

    /// A malformed address is refused, so a typo does not become a registry entry that
    /// never matches a log.
    #[test]
    fn a_bad_address_is_refused() {
        let result = ContractRegistry::load(
            &[entry("uniswap_v3", vec![("base", "not-an-address")])],
            abi_dir(),
        );
        assert!(result.is_err());
    }

    /// No protocols is an empty registry, not an error: running without decoding is
    /// legitimate and the caller says so at startup.
    #[test]
    fn no_protocols_is_an_empty_registry() {
        let registry = ContractRegistry::load(&[], abi_dir()).expect("empty is fine");
        assert!(registry.is_empty());
    }
}
