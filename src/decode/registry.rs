//! Decoding one log against one contract's ABI.
//!
//! [`Abi`] owns a parsed [`JsonAbi`] and turns a log's topics and data into a
//! [`DecodedEvent`]: the event's name and its named, typed arguments. It knows
//! nothing about the wire — no chain, no block, no envelope — so the same decoder
//! drives the pipeline, a batch, or a test. The published record is assembled from a
//! `DecodedEvent` and the raw log it came from by
//! [`Transform`](crate::decode::Transform), which is the layer that owns identity.
//!
//! [`AbiRegistry`] answers which [`Contract`] — an ABI plus what the contract is —
//! applies to an address at a block. Lookup is keyed by `(chain, contract address,
//! block)` and not by address alone. A contract's ABI is valid over a *block range*:
//! a proxy upgrades, a new implementation appears, and the same address answers
//! different selectors at different heights. An implicit "latest" would silently
//! decode an old log with a new ABI, which is the failure mode that produces
//! plausible but wrong rows — so the height is part of the key and there is no
//! default for it.
//!
//! # Static and dynamic ABIs
//!
//! The two sources are complementary rather than alternatives:
//!
//! - A **compile-time** ABI is checked code. `sol!`-generated bindings, or a JSON
//!   ABI load, give a decoder that cannot silently mis-type a field.
//! - A **runtime** ABI is data. No compile-time set can cover an open-ended set of
//!   contracts deployed by third parties, and a proxy's ABI is not knowable at build
//!   time at all.
//!
//! [`Abi`] is the runtime path; a compile-time binding would be converted into one
//! for a uniform call site. [`AbiRegistry`] is the seam either way, so the transform
//! does not know which answered — and so a registry backed by a table of ABI
//! versions can be swapped in without touching the decoder.
//!
//! The production registry is [`ContractRegistry`](crate::decode::contracts),
//! which lists each protocol once and its deployments under it.
//!
//! # Known limitation
//!
//! One ABI per `(chain, address)`, applying at every height. A proxy that upgrades
//! changes its ABI at a height, which the current registries cannot express; the
//! [`AbiRegistry`] seam is what a table-backed registry — keyed by
//! `(chain, address, block_range)` — replaces without the decoder changing.

use std::collections::BTreeMap;

use crate::wire::envelope::{ChainId, DecodedArg};
use alloy_dyn_abi::{DynSolValue, EventExt as _};
use alloy_json_abi::{Event, JsonAbi};
use alloy_primitives::{Address, B256};

use crate::decode::convert::{self, ConversionError};

/// One decoded event: its name and its named, typed arguments.
///
/// The decoder's whole output. It carries no chain, block, or transaction — those
/// are the raw log's, and [`Transform`](crate::decode::Transform) stamps them on when
/// it assembles the published record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedEvent {
    /// The event name from the ABI, for example `Transfer`.
    pub name: String,
    /// The event selector, `keccak256` of its signature.
    pub selector: B256,
    /// The event's human-readable signature, for example
    /// `Transfer(address,address,uint256)`.
    pub signature: String,
    /// Whether the ABI declares this event anonymous.
    pub anonymous: bool,
    /// The indexed arguments, in ABI order, each carrying its name.
    pub indexed: Vec<DecodedArg>,
    /// The non-indexed arguments, in ABI order, each carrying its name.
    pub body: Vec<DecodedArg>,
}

/// One contract as the registry knows it: its ABI, and what it is.
///
/// Returned by [`AbiRegistry::contract`] as a borrowed pair so the lookup that found
/// the ABI also yields the protocol. An [`Abi`] is a list of signatures and says
/// nothing about which protocol an address implements, and a second lookup for the
/// protocol could disagree with the first — so the two travel together.
#[derive(Debug, Clone, Copy)]
pub struct Contract<'a> {
    /// The contract's ABI.
    pub abi: &'a Abi,
    /// What the contract is, for example `uniswap_v3`, or `""` if unknown.
    pub protocol: &'a str,
}

/// One loaded contract ABI, able to decode a log against its events.
///
/// Indexed by selector at load, because the lookup path must not do I/O and must not
/// scan: a registry that reads a file, or walks the events, per log is a registry that
/// stalls the pipeline under load.
#[derive(Debug, Clone, Default)]
pub struct Abi {
    /// Selector to event, built once so the per-log lookup is a map hit rather than
    /// a linear scan over the ABI's events.
    by_selector: BTreeMap<B256, Event>,
}

impl Abi {
    /// Loads an ABI from its JSON form.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::Abi`] if the JSON is not a valid ABI.
    pub fn from_json(json: &str) -> Result<Self, RegistryError> {
        let abi: JsonAbi = serde_json::from_str(json).map_err(|error| RegistryError::Abi {
            detail: error.to_string(),
        })?;
        Ok(Self::from_abi(&abi))
    }

    /// Indexes an already-parsed ABI by event selector.
    ///
    /// Takes a reference and clones only the events it keeps, because the caller may
    /// still want the parsed ABI; [`Self::from_json`] hands it a temporary.
    ///
    /// Anonymous events are excluded: they carry no selector in `topic0`, so they
    /// cannot be found by one, and pretending otherwise would match the wrong event
    /// on an unrelated log.
    #[must_use]
    pub fn from_abi(abi: &JsonAbi) -> Self {
        let by_selector = abi
            .events()
            .filter(|event| !event.anonymous)
            .map(|event| (event.selector(), event.clone()))
            .collect();
        Self { by_selector }
    }

    /// Decodes a log's topics and data into a [`DecodedEvent`], if this ABI declares
    /// an event with the log's selector.
    ///
    /// Returns `Ok(None)` when no event in the ABI has the log's selector. That is a
    /// normal miss, not an error: a contract emits events outside any ABI the
    /// consumer cares about, and the transform forwards the log undecoded.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::Decode`] when an event *did* match and its data does
    /// not decode against it. That is worth surfacing rather than skipping: a
    /// mismatch usually means the ABI is the wrong version for this height, and
    /// silently dropping the log would hide exactly that.
    pub fn decode_log(
        &self,
        topics: &[B256],
        data: &[u8],
    ) -> Result<Option<DecodedEvent>, RegistryError> {
        let Some(selector) = topics.first() else {
            return Ok(None);
        };
        let Some(event) = self.by_selector.get(selector) else {
            return Ok(None);
        };

        let decoded = event
            .decode_log_parts(topics.iter().copied(), data)
            .map_err(|error| RegistryError::Decode {
                selector: *selector,
                detail: error.to_string(),
            })?;

        // `decode_log_parts` splits the event's inputs into indexed and non-indexed
        // exactly as the ABI declares them, so the names come from the same split.
        let (indexed_params, body_params): (Vec<_>, Vec<_>) =
            event.inputs.iter().partition(|param| param.indexed);

        Ok(Some(DecodedEvent {
            name: event.name.clone(),
            selector: *selector,
            signature: event.signature(),
            anonymous: event.anonymous,
            indexed: typed_args(&indexed_params, &decoded.indexed)?,
            body: typed_args(&body_params, &decoded.body)?,
        }))
    }
}

/// Converts decoded values to named arguments, pairing each with its ABI parameter.
///
/// The decoder returns values in ABI order with no names, so the names come from the
/// event's own input list — the same split the decoder used. A length mismatch would
/// mean the decoder disagreed with the ABI about its own shape, which is a bug rather
/// than bad input, so it is an error rather than a truncation.
fn typed_args(
    params: &[&alloy_json_abi::EventParam],
    values: &[DynSolValue],
) -> Result<Vec<DecodedArg>, RegistryError> {
    if params.len() != values.len() {
        return Err(RegistryError::Shape {
            decoded: values.len(),
            declared: params.len(),
        });
    }

    params
        .iter()
        .zip(values)
        .map(|(param, value)| {
            Ok(DecodedArg {
                name: param.name.clone(),
                value: convert::value(value)?,
            })
        })
        .collect::<Result<Vec<_>, ConversionError>>()
        .map_err(RegistryError::Conversion)
}

/// Where an ABI comes from, so the transform does not know.
///
/// The height is part of the key because an ABI is valid over a block range, not
/// forever. Implement this over a table of `(address, block_range) -> ABI` for a
/// proxy-aware registry.
pub trait AbiRegistry {
    /// The contract for `address` on `chain` as of `block`, or `None` if unknown.
    ///
    /// Returns the ABI and what the contract is together, so the protocol cannot
    /// disagree with the ABI that decoded the log. A registry that knows only the ABI
    /// reports an empty protocol, which is honest: nothing knows what the contract is.
    ///
    /// Must not do I/O on the hot path: a miss returns `None`, and the caller
    /// forwards the log undecoded rather than stalling the pipeline behind a lookup.
    fn contract(&self, chain: &ChainId, address: Address, block: u64) -> Option<Contract<'_>>;
}

/// Why a log could not be decoded.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    /// The ABI could not be loaded.
    #[error("invalid ABI: {detail}")]
    Abi {
        /// What was wrong with it.
        detail: String,
    },
    /// An event matched the log's selector but the data did not decode against it.
    #[error("log {selector} does not decode against its ABI: {detail}")]
    Decode {
        /// The event selector that matched.
        selector: B256,
        /// The decoder's own explanation.
        detail: String,
    },
    /// The decoder returned a different number of values than the ABI declares.
    #[error("ABI declares {declared} arguments but {decoded} were decoded")]
    Shape {
        /// How many values the decoder produced.
        decoded: usize,
        /// How many the ABI declares.
        declared: usize,
    },
    /// A decoded value could not be published.
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    /// A registration's address did not parse.
    #[error("registration {entry:?} has an invalid address")]
    Address {
        /// The entry as written.
        entry: String,
    },
    /// Two registry entries claim one `(chain, address)`.
    #[error("both {chain}.{address} are registered; one address decodes with one ABI")]
    Duplicate {
        /// The chain, as written.
        chain: String,
        /// The address, as written.
        address: String,
    },
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{Address, B256, I256, U256};

    use super::{Abi, AbiRegistry, Contract, RegistryError};
    use crate::wire::envelope::{ChainId, DecodedArg};
    use crate::wire::typed::TypedValue;

    /// The canonical ERC-20 event, as an ABI JSON would spell it.
    const ERC20: &str = r#"[{
        "type": "event",
        "name": "Transfer",
        "anonymous": false,
        "inputs": [
            {"name": "from", "type": "address", "indexed": true},
            {"name": "to", "type": "address", "indexed": true},
            {"name": "value", "type": "uint256", "indexed": false}
        ]
    }]"#;

    fn address(byte: u8) -> Address {
        Address::from([byte; 20])
    }

    /// `keccak256("Transfer(address,address,uint256)")`. Pinned as a constant rather
    /// than computed, so a change in how the selector is derived shows up as a
    /// failure here rather than as a silent mismatch against real chain data.
    fn transfer_selector() -> B256 {
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
            .parse()
            .expect("selector parses")
    }

    /// A real `Transfer` log: two indexed addresses as topics, the amount as data.
    fn transfer_log() -> (Vec<B256>, Vec<u8>) {
        let mut from = [0u8; 32];
        from[12..].copy_from_slice(&[0x11; 20]);
        let mut to = [0u8; 32];
        to[12..].copy_from_slice(&[0x22; 20]);
        let value = U256::from(1_000_000_000_000_000_000u64).to_be_bytes::<32>();
        (
            vec![transfer_selector(), B256::from(from), B256::from(to)],
            value.to_vec(),
        )
    }

    /// The happy path: a real log decodes into named, typed arguments.
    #[test]
    fn a_transfer_log_decodes_into_typed_arguments() {
        let abi = Abi::from_json(ERC20).expect("ABI loads");
        let (topics, data) = transfer_log();
        let decoded = abi
            .decode_log(&topics, &data)
            .expect("log decodes")
            .expect("an event matched");

        assert_eq!(decoded.name, "Transfer");
        assert_eq!(decoded.selector, transfer_selector());
        assert_eq!(decoded.signature, "Transfer(address,address,uint256)");
        assert!(!decoded.anonymous);
        assert_eq!(decoded.indexed.len(), 2);
        assert_eq!(decoded.body.len(), 1);
        // Each argument carries the ABI's own name, so a store can address it rather
        // than count positions.
        assert_eq!(
            decoded.indexed,
            vec![
                DecodedArg {
                    name: "from".to_owned(),
                    value: TypedValue::Address {
                        value: address(0x11)
                    },
                },
                DecodedArg {
                    name: "to".to_owned(),
                    value: TypedValue::Address {
                        value: address(0x22)
                    },
                },
            ]
        );
        assert_eq!(
            decoded.body,
            vec![DecodedArg {
                name: "value".to_owned(),
                value: TypedValue::Uint {
                    value: U256::from(1_000_000_000_000_000_000u64),
                    bits: 256,
                },
            }]
        );
    }

    /// A log whose selector the ABI does not declare is a miss, not an error: the
    /// transform forwards it undecoded rather than failing the batch.
    #[test]
    fn an_unknown_selector_is_a_miss_not_an_error() {
        let abi = Abi::from_json(ERC20).expect("ABI loads");
        let topics = vec![B256::from([0x99; 32])];
        assert!(
            abi.decode_log(&topics, &[])
                .expect("a miss is not an error")
                .is_none()
        );
    }

    /// A log with no topics at all, which is legal for an anonymous event, is also a
    /// miss rather than a panic.
    #[test]
    fn a_log_with_no_topics_is_a_miss() {
        let abi = Abi::from_json(ERC20).expect("ABI loads");
        assert!(
            abi.decode_log(&[], &[])
                .expect("a miss is not an error")
                .is_none()
        );
    }

    /// A selector that matches but data that is too short to decode is an error,
    /// because the likely cause is an ABI from the wrong block range and hiding it
    /// would publish the wrong contract's values.
    #[test]
    fn a_selector_that_matches_with_undecodable_data_is_an_error() {
        let abi = Abi::from_json(ERC20).expect("ABI loads");
        let (topics, _) = transfer_log();
        // Half a word where the event declares a `uint256`.
        let short = [0u8; 16];
        assert!(matches!(
            abi.decode_log(&topics, &short),
            Err(RegistryError::Decode { .. })
        ));
    }

    /// An ABI that is not JSON fails loudly at load, not at first decode.
    #[test]
    fn malformed_abi_json_is_rejected_at_load() {
        assert!(matches!(
            Abi::from_json("not an abi"),
            Err(RegistryError::Abi { .. })
        ));
    }

    /// A regression test against a real Uniswap V3 `Swap` log captured from Base.
    ///
    /// Every other test here builds its own bytes, so they would all still pass if
    /// the decoder agreed with itself about a layout the chain does not use. This one
    /// pins the layout to the chain: the `int256` amounts are signed, the pool's
    /// `uint160` price is read at 160 bits, and the trailing `int24` tick is not
    /// silently widened.
    #[test]
    fn a_real_uniswap_v3_swap_log_decodes_with_signed_amounts() {
        let abi = Abi::from_json(include_str!("../../abis/uniswap_v3_pool.json"))
            .expect("the pool ABI loads");
        let topic0: B256 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
            .parse()
            .expect("selector parses");
        let topic1: B256 = "0x0000000000000000000000006ff5693b99212da76ad316178a184ab56d299b43"
            .parse()
            .expect("topic parses");
        let data = hex_bytes(
            "fffffffffffffffffffffffffffffffffffffffffffffffffff4b34627fb9302\
             0000000000000000000000000000000000000000000000000000000000830544\
             00000000000000000000000000000000000000000003678007a6bbf505d858fa\
             00000000000000000000000000000000000000000000000012fb062ae6731f9d\
             fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcfd3b",
        );
        let log_topics = vec![topic0, topic1, topic1];

        let decoded = abi
            .decode_log(&log_topics, &data)
            .expect("the log decodes")
            .expect("the ABI declares Swap");

        assert_eq!(decoded.name, "Swap");
        assert_eq!(
            decoded.signature,
            "Swap(address,address,int256,int256,uint160,uint128,int24)"
        );
        // Every argument is named from the ABI, which is what lets a store map
        // `amount0` to a column instead of counting positions.
        let names: Vec<&str> = decoded
            .indexed
            .iter()
            .chain(&decoded.body)
            .map(|arg| arg.name.as_str())
            .collect();
        assert_eq!(
            names,
            [
                "sender",
                "recipient",
                "amount0",
                "amount1",
                "sqrtPriceX96",
                "liquidity",
                "tick"
            ]
        );

        // `amount0` is negative: this swap sold token0, and reading the two's
        // complement word as unsigned would produce 1.15e77 instead.
        assert_eq!(
            decoded.body.first().map(|arg| &arg.value),
            Some(&TypedValue::Int {
                value: I256::try_from(-3_180_585_820_646_654_i64).expect("fits"),
                bits: 256,
            })
        );
        assert_eq!(
            decoded.body.get(1).map(|arg| &arg.value),
            Some(&TypedValue::Int {
                value: I256::try_from(8_586_564_i64).expect("fits"),
                bits: 256,
            })
        );
        // `sqrtPriceX96` is `uint160`, not `uint256`.
        assert_eq!(
            decoded.body.get(2).and_then(rebuild_type),
            Some(alloy_dyn_abi::DynSolType::Uint(160))
        );
        assert_eq!(
            decoded.body.get(4).and_then(rebuild_type),
            Some(alloy_dyn_abi::DynSolType::Int(24))
        );
    }

    /// The Solidity type a published argument declares, for asserting a width.
    fn rebuild_type(arg: &DecodedArg) -> Option<alloy_dyn_abi::DynSolType> {
        crate::decode::convert::dyn_type(&arg.value).ok()
    }

    /// Decodes a hex string with whitespace, so a long fixture stays readable.
    fn hex_bytes(hex: &str) -> Vec<u8> {
        let compact: String = hex.chars().filter(|c| !c.is_whitespace()).collect();
        alloy_primitives::hex::decode(compact).expect("fixture is valid hex")
    }

    /// The registry is keyed by chain as well as address, so the same address on two
    /// chains does not resolve to one ABI.
    #[test]
    fn the_registry_is_keyed_by_chain_and_address() {
        struct OneAbi {
            base: Abi,
        }

        impl AbiRegistry for OneAbi {
            fn contract(
                &self,
                chain: &ChainId,
                contract: Address,
                _block: u64,
            ) -> Option<Contract<'_>> {
                (chain.as_str() == "base" && contract == address(0xaa)).then_some(Contract {
                    abi: &self.base,
                    protocol: "erc20",
                })
            }
        }

        let registry = OneAbi {
            base: Abi::from_json(ERC20).expect("ABI loads"),
        };
        let base = ChainId::new("base");
        let other = ChainId::new("ethereum");

        let found = registry
            .contract(&base, address(0xaa), 100)
            .expect("base answers");
        assert_eq!(found.protocol, "erc20");
        assert!(registry.contract(&base, address(0xbb), 100).is_none());
        assert!(registry.contract(&other, address(0xaa), 100).is_none());
    }
}
