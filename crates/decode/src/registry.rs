//! The ABI registry: which contract's ABI applies to a log at a height.
//!
//! Lookup is keyed by `(chain, contract address, block)` and not by address alone.
//! A contract's ABI is valid over a *block range*: a proxy upgrades, a new
//! implementation appears, and the same address answers different selectors at
//! different heights. An implicit "latest" would silently decode an old log with a
//! new ABI, which is the failure mode that produces plausible but wrong rows — so
//! the height is part of the key and there is no default for it.
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

use std::collections::BTreeMap;

use alloy_dyn_abi::{DynSolValue, EventExt as _};
use alloy_json_abi::{Event, JsonAbi};
use alloy_primitives::{Address, B256, TxHash};
use wire::envelope::{ChainId, Decoded, DecodedArg};

use crate::convert::{self, ConversionError};

/// The parts of a raw log a decoder needs.
///
/// A borrowed view rather than the whole [`Log`](wire::datasets::evm::Log) record, so
/// the seal is explicit: a decoder reads a log and produces a record, and it has no
/// business seeing the rest of the envelope.
#[derive(Debug, Clone, Copy)]
pub struct RawLog<'a> {
    /// The chain the log came from.
    pub chain: &'a ChainId,
    /// The contract that emitted the log.
    pub address: Address,
    /// The log's topics, `topic0` first.
    pub topics: &'a [B256],
    /// The log's unindexed data.
    pub data: &'a [u8],
    /// The transaction that emitted the log.
    pub transaction_hash: TxHash,
    /// The log's position within its block.
    pub log_index: u64,
    /// Height of the block containing the log.
    pub block_number: u64,
    /// Hash of the block containing the log.
    pub block_hash: B256,
    /// Timestamp of the block containing the log.
    pub block_timestamp: u64,
}

/// One loaded contract ABI, able to decode a log against its events.
///
/// Owns a parsed [`JsonAbi`] rather than a source path, because the lookup path must
/// not do I/O. A registry that reads a file per log is a registry that stalls the
/// pipeline under load.
#[derive(Debug, Clone, Default)]
pub struct Abi {
    abi: JsonAbi,
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
        Ok(Self::from_abi(abi))
    }

    /// Indexes an already-parsed ABI by event selector.
    ///
    /// Anonymous events are excluded: they carry no selector in `topic0`, so they
    /// cannot be found by one, and pretending otherwise would match the wrong event
    /// on an unrelated log.
    #[must_use]
    pub fn from_abi(abi: JsonAbi) -> Self {
        let by_selector = abi
            .events()
            .filter(|event| !event.anonymous)
            .map(|event| (event.selector(), event.clone()))
            .collect();
        Self { abi, by_selector }
    }

    /// The parsed ABI, for a caller that needs more than event lookup.
    #[must_use]
    pub const fn json_abi(&self) -> &JsonAbi {
        &self.abi
    }

    /// Decodes one log, if this ABI declares an event with that topic.
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
    pub fn decode_log(&self, log: RawLog<'_>) -> Result<Option<Decoded>, RegistryError> {
        let Some(selector) = log.topics.first() else {
            return Ok(None);
        };
        let Some(event) = self.by_selector.get(selector) else {
            return Ok(None);
        };

        let decoded = event
            .decode_log_parts(log.topics.iter().copied(), log.data)
            .map_err(|error| RegistryError::Decode {
                selector: *selector,
                detail: error.to_string(),
            })?;

        // `decode_log_parts` splits the event's inputs into indexed and non-indexed
        // exactly as the ABI declares them, so the names come from the same split.
        let (indexed_params, body_params): (Vec<_>, Vec<_>) =
            event.inputs.iter().partition(|param| param.indexed);

        Ok(Some(Decoded {
            name: event.name.clone(),
            address: log.address,
            selector: *selector,
            signature: event.signature(),
            source: source_key(&log),
            indexed: typed_args(&indexed_params, &decoded.indexed)?,
            body: typed_args(&body_params, &decoded.body)?,
            block_number: log.block_number,
            block_hash: log.block_hash,
            block_timestamp: log.block_timestamp,
        }))
    }
}

/// The raw log's natural key, in the same shape
/// [`Log::dedupe_key`](wire::datasets::evm::Log::dedupe_key) uses, so a decoded
/// record links back to the exact record it came from.
fn source_key(log: &RawLog<'_>) -> String {
    format!(
        "{}:{}:{}",
        log.block_number, log.transaction_hash, log.log_index
    )
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
    /// The ABI for `address` on `chain` as of `block`, or `None` if unknown.
    ///
    /// Must not do I/O on the hot path: a miss returns `None`, and the caller
    /// forwards the log undecoded rather than stalling the pipeline behind a lookup.
    fn abi(&self, chain: &ChainId, address: Address, block: u64) -> Option<&Abi>;
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
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::sync::LazyLock;

    use alloy_primitives::{Address, B256, I256, TxHash, U256};

    use super::{Abi, AbiRegistry, RawLog, RegistryError};
    use wire::envelope::{ChainId, DecodedArg};
    use wire::typed::TypedValue;

    /// One chain for every fixture; a registry keyed by chain is tested below.
    static CHAIN: LazyLock<ChainId> = LazyLock::new(|| ChainId::new("base"));

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

    fn raw_log<'a>(topics: &'a [B256], data: &'a [u8]) -> RawLog<'a> {
        RawLog {
            chain: &CHAIN,
            address: address(0xaa),
            topics,
            data,
            transaction_hash: TxHash::from([0x01; 32]),
            log_index: 3,
            block_number: 100,
            block_hash: B256::from([0x02; 32]),
            block_timestamp: 1_700_000_000,
        }
    }

    /// The happy path: a real log decodes into named, typed arguments, and the
    /// record links back to the exact raw log it came from.
    #[test]
    fn a_transfer_log_decodes_into_typed_arguments() {
        let abi = Abi::from_json(ERC20).expect("ABI loads");
        let (topics, data) = transfer_log();
        let decoded = abi
            .decode_log(raw_log(&topics, &data))
            .expect("log decodes")
            .expect("an event matched");

        assert_eq!(decoded.name, "Transfer");
        assert_eq!(decoded.address, address(0xaa));
        assert_eq!(decoded.selector, transfer_selector());
        assert_eq!(decoded.signature, "Transfer(address,address,uint256)");
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
        // The source is the raw log's natural key, so a store can join back to it.
        assert_eq!(
            decoded.source,
            format!("100:{}:3", TxHash::from([0x01; 32]))
        );
        assert_eq!(decoded.block_number, 100);
        assert_eq!(decoded.block_timestamp, 1_700_000_000);
    }

    /// A log whose selector the ABI does not declare is a miss, not an error: the
    /// transform forwards it undecoded rather than failing the batch.
    #[test]
    fn an_unknown_selector_is_a_miss_not_an_error() {
        let abi = Abi::from_json(ERC20).expect("ABI loads");
        let topics = vec![B256::from([0x99; 32])];
        assert!(
            abi.decode_log(raw_log(&topics, &[]))
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
            abi.decode_log(raw_log(&[], &[]))
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
            abi.decode_log(raw_log(&topics, &short)),
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
        let abi = Abi::from_json(include_str!("../abi/uniswap_v3_pool.json"))
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
            .decode_log(RawLog {
                chain: &ChainId::new("base"),
                address: address(0xd0),
                topics: &log_topics,
                data: &data,
                transaction_hash: TxHash::from([0x2a; 32]),
                log_index: 767,
                block_number: 51_913_794,
                block_hash: B256::from([0xd4; 32]),
                block_timestamp: 1_700_000_000,
            })
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
        // The link back to the raw log is its natural key.
        assert_eq!(
            decoded.source,
            format!("51913794:{}:767", TxHash::from([0x2a; 32]))
        );
    }

    /// The Solidity type a published argument declares, for asserting a width.
    fn rebuild_type(arg: &DecodedArg) -> Option<alloy_dyn_abi::DynSolType> {
        crate::convert::dyn_type(&arg.value).ok()
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
            fn abi(&self, chain: &ChainId, contract: Address, _block: u64) -> Option<&Abi> {
                (chain.as_str() == "base" && contract == address(0xaa)).then_some(&self.base)
            }
        }

        let registry = OneAbi {
            base: Abi::from_json(ERC20).expect("ABI loads"),
        };
        let base = ChainId::new("base");
        let other = ChainId::new("ethereum");

        assert!(registry.abi(&base, address(0xaa), 100).is_some());
        assert!(registry.abi(&base, address(0xbb), 100).is_none());
        assert!(registry.abi(&other, address(0xaa), 100).is_none());
    }
}
