//! One contract's ABI, and decoding a log against it.
//!
//! [`Abi`] owns a parsed ABI indexed by event selector, and turns a log's topics and
//! data into a [`DecodedEvent`]: the event's name and its named, typed arguments. It
//! knows nothing about the wire — no chain, no block, no envelope — so the same decoder
//! drives the pipeline, a batch, or a test.
//!
//! # The value model boundary
//!
//! Decoding yields alloy's own value types, which cannot be published: they carry no
//! serde impls, and depending on them would tie the wire format to one decoder. So the
//! conversion to the wire's [`TypedValue`] is explicit and lives here, the only place in
//! the crate that knows alloy's dynamic value model. It is total for every variant a
//! decoded log can produce, and lossless in the direction that matters: re-encoding the
//! converted value reproduces the bytes that were decoded. A Solidity `function` is the
//! one type the wire shape does not carry, and it is a decode failure rather than a
//! silent mangling.

use std::collections::BTreeMap;

use alloy_dyn_abi::{DynSolValue, EventExt as _};
use alloy_json_abi::{Event, JsonAbi};
use alloy_primitives::{B256, Bytes};
use thiserror::Error;

use crate::wire::datasets::evm::Log;
use crate::wire::envelope::DecodedArg;
use crate::wire::typed::TypedValue;

/// One decoded event: its name and its named, typed arguments.
///
/// The decoder's whole output. It carries no chain, block, or transaction — those are
/// the raw log's, and [`Transform`](crate::decode::Transform) stamps them on when it
/// assembles the published record.
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

/// One loaded contract ABI, able to decode a log against its events.
///
/// Indexed by selector at load, because the lookup path must not do I/O and must not
/// scan: a registry that reads a file, or walks the events, per log is a registry that
/// stalls the pipeline under load.
#[derive(Debug, Clone, Default)]
pub struct Abi {
    /// Selector to event, built once so the per-log lookup is a map hit rather than a
    /// linear scan over the ABI's events.
    by_selector: BTreeMap<B256, Event>,
}

impl Abi {
    /// Loads an ABI from its JSON form.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::Abi`] if the JSON is not a valid ABI.
    pub fn from_json(json: &str) -> Result<Self, DecodeError> {
        let abi: JsonAbi = serde_json::from_str(json).map_err(|error| DecodeError::Abi {
            detail: error.to_string(),
        })?;
        Ok(Self::from_abi(&abi))
    }

    /// Indexes an already-parsed ABI by event selector.
    ///
    /// Anonymous events are excluded: they carry no selector in `topic0`, so they cannot
    /// be found by one, and pretending otherwise would match the wrong event on an
    /// unrelated log.
    fn from_abi(abi: &JsonAbi) -> Self {
        let by_selector = abi
            .events()
            .filter(|event| !event.anonymous)
            .map(|event| (event.selector(), event.clone()))
            .collect();
        Self { by_selector }
    }

    /// The selector of the event whose signature is `signature`, or `None` if this ABI
    /// declares no such event.
    ///
    /// A discovery rule names its creation event by signature — the form an ABI writes,
    /// for example `PoolCreated(address,address,uint24,int24,address)` — and this is how
    /// the rule is resolved to the selector the map is keyed by. The rule is checked
    /// against the factory's ABI at load, so a rule for an event the factory cannot emit
    /// is a startup error rather than a rule that silently never fires.
    #[must_use]
    pub fn selector(&self, signature: &str) -> Option<B256> {
        self.by_selector
            .values()
            .find(|event| event.signature() == signature)
            .map(Event::selector)
    }

    /// Decodes a log into a [`DecodedEvent`], if this ABI declares the log's event.
    ///
    /// Returns `Ok(None)` when no event in the ABI has the log's selector. That is a
    /// normal miss, not an error: a contract emits events outside any ABI the consumer
    /// cares about, and the caller drops the log.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::Decode`] when an event *did* match and its data does not
    /// decode against it. That is worth surfacing rather than skipping: a mismatch
    /// usually means the ABI is the wrong version for this height, and silently dropping
    /// the log would hide exactly that.
    pub fn decode_log(&self, log: &Log) -> Result<Option<DecodedEvent>, DecodeError> {
        // The wire flattens topics into `topic0..topic3`, so they are packed back into
        // the contiguous list the decoder expects. Topics are contiguous from `topic0`
        // by construction, so the first gap ends the list.
        let mut flattened = [log.topic0, log.topic1, log.topic2, log.topic3];
        let topics: Vec<B256> = flattened.iter_mut().map_while(Option::take).collect();
        let Some(selector) = topics.first().copied() else {
            return Ok(None);
        };
        let Some(event) = self.by_selector.get(&selector) else {
            return Ok(None);
        };

        let decoded = event
            .decode_log_parts(topics.iter().copied(), &log.data)
            .map_err(|error| DecodeError::Decode {
                selector,
                detail: error.to_string(),
            })?;

        // `decode_log_parts` splits the event's inputs into indexed and non-indexed
        // exactly as the ABI declares them, so the names come from the same split.
        let (indexed_params, body_params): (Vec<_>, Vec<_>) =
            event.inputs.iter().partition(|param| param.indexed);

        Ok(Some(DecodedEvent {
            name: event.name.clone(),
            selector,
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
/// event's own input list — the same split the decoder used. A length mismatch means the
/// decoder disagreed with the ABI about its own shape, which is a bug rather than bad
/// input, so it is an error rather than a truncation.
fn typed_args(
    params: &[&alloy_json_abi::EventParam],
    values: &[DynSolValue],
) -> Result<Vec<DecodedArg>, DecodeError> {
    if params.len() != values.len() {
        return Err(DecodeError::Shape {
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
                value: value_to_typed(value)?,
            })
        })
        .collect()
}

/// Converts one decoded value to its published form.
///
/// Re-encoding the result reproduces the bytes that were decoded, so the trim of a
/// `FixedBytes` to its declared size is lossless.
///
/// # Errors
///
/// Returns [`DecodeError::Unsupported`] for a value type the wire shape does not carry
/// (only a Solidity `function`), and [`DecodeError::Width`] if a declared width or
/// size exceeds a `u16`, which no real ABI can produce.
fn value_to_typed(decoded: &DynSolValue) -> Result<TypedValue, DecodeError> {
    let typed = match decoded {
        DynSolValue::Bool(value) => TypedValue::Bool { value: *value },
        DynSolValue::Int(value, bits) => TypedValue::Int {
            value: *value,
            bits: width("int bits", *bits)?,
        },
        DynSolValue::Uint(value, bits) => TypedValue::Uint {
            value: *value,
            bits: width("uint bits", *bits)?,
        },
        DynSolValue::FixedBytes(word, size) => TypedValue::FixedBytes {
            // The decoded word is right-padded, so the declared size is what says how
            // much of it is the value.
            value: Bytes::copy_from_slice(word.as_slice().get(..*size).unwrap_or(&[])),
            size: width("fixed bytes size", *size)?,
        },
        DynSolValue::Address(value) => TypedValue::Address { value: *value },
        DynSolValue::Bytes(value) => TypedValue::Bytes {
            value: Bytes::from(value.clone()),
        },
        DynSolValue::String(value) => TypedValue::String {
            value: value.clone(),
        },
        DynSolValue::Array(values) => TypedValue::Array {
            value: convert_all(values)?,
        },
        DynSolValue::FixedArray(values) => TypedValue::FixedArray {
            value: convert_all(values)?,
            size: width("fixed array size", values.len())?,
        },
        // A tuple is positional in an event, so there are no component names to carry
        // even when the ABI declares a struct.
        DynSolValue::Tuple(values) => TypedValue::Tuple {
            value: convert_all(values)?,
        },
        DynSolValue::Function(_) => return Err(DecodeError::Unsupported("function type")),
    };
    Ok(typed)
}

/// Converts a sequence of decoded values, so the recursive arms stay one line.
fn convert_all(values: &[DynSolValue]) -> Result<Vec<TypedValue>, DecodeError> {
    values.iter().map(value_to_typed).collect()
}

/// Narrows a decoder-supplied `usize` to the `u16` the wire shape carries.
fn width(kind: &'static str, value: usize) -> Result<u16, DecodeError> {
    u16::try_from(value).map_err(|_| DecodeError::Width { kind, value })
}

/// Why a log could not be decoded.
///
/// One type for the whole stage: loading an ABI, decoding a log against it, and
/// converting the result to the wire shape all fail for the same reason — the bytes and
/// the ABI disagree — so a caller that stops at any of them wants the same explanation.
#[derive(Debug, Error)]
pub enum DecodeError {
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
    /// A decoded value is a type the wire shape does not carry.
    ///
    /// Only a Solidity `function` type reaches this.
    #[error("unsupported ABI value: {0}")]
    Unsupported(&'static str),
    /// A declared width or size did not fit the wire field that carries it.
    #[error("{kind} {value} does not fit a u16")]
    Width {
        /// Which width or size, for the message.
        kind: &'static str,
        /// The offending value.
        value: usize,
    },
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are allowed
// them per the repository test style, since a failed expectation there means the fixture
// or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use alloy_dyn_abi::DynSolValue;
    use alloy_primitives::{Address, B256, I256, U256};

    use super::{Abi, DecodeError, value_to_typed};
    use crate::wire::datasets::evm::Log;
    use crate::wire::typed::TypedValue;

    /// A log whose fields the tests set, defaulting the rest.
    fn log(topic0: Option<B256>, indexed: [Option<B256>; 3], data: Vec<u8>) -> Log {
        let [topic1, topic2, topic3] = indexed;
        Log {
            log_index: 3,
            transaction_hash: alloy_primitives::TxHash::from([0x01; 32]),
            address: Address::from([0xaa; 20]),
            topic0,
            topic1,
            topic2,
            topic3,
            data: data.into(),
            block_number: 5,
            ..Log::default()
        }
    }

    /// A 32-byte word holding an address in its low 20 bytes.
    fn address_word(address: Address) -> B256 {
        let mut word = [0u8; 32];
        word[12..].copy_from_slice(address.as_slice());
        B256::from(word)
    }

    /// A 32-byte big-endian word holding `value`, the form an ABI integer takes.
    fn word(value: u64) -> B256 {
        let mut word = [0u8; 32];
        word[24..].copy_from_slice(&value.to_be_bytes());
        B256::from(word)
    }

    /// A fixed-bytes word as the decoder produces it: the first `size` bytes are the
    /// value and the rest are zero padding to a word.
    fn padded(first: &[u8]) -> B256 {
        let mut word = B256::ZERO;
        word.get_mut(..first.len())
            .expect("fixture fits in a word")
            .copy_from_slice(first);
        word
    }

    fn round_trip(decoded: &DynSolValue) -> TypedValue {
        value_to_typed(decoded).expect("value converts")
    }

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

    /// The happy path: a real log decodes into named, typed arguments.
    #[test]
    fn a_transfer_log_decodes_into_typed_arguments() {
        let abi = Abi::from_json(ERC20).expect("ABI loads");
        // keccak256("Transfer(address,address,uint256)")
        let selector: B256 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
            .parse()
            .expect("selector parses");
        let value = U256::from(1_000_000_000_000_000_000u64).to_be_bytes::<32>();
        let decoded = abi
            .decode_log(&log(
                Some(selector),
                [
                    Some(address_word(Address::from([0x11; 20]))),
                    Some(address_word(Address::from([0x22; 20]))),
                    None,
                ],
                value.to_vec(),
            ))
            .expect("log decodes")
            .expect("an event matched");

        assert_eq!(decoded.name, "Transfer");
        assert_eq!(decoded.signature, "Transfer(address,address,uint256)");
        assert_eq!(decoded.indexed.len(), 2);
        assert_eq!(decoded.body.len(), 1);
        // Each argument carries the ABI's own name, so a store can address it rather
        // than count positions.
        assert_eq!(
            decoded.indexed.first().map(|arg| arg.name.as_str()),
            Some("from")
        );
        assert_eq!(
            decoded.body.first().map(|arg| &arg.value),
            Some(&TypedValue::Uint {
                value: U256::from(1_000_000_000_000_000_000u64),
                bits: 256,
            })
        );
    }

    /// A log whose selector the ABI does not declare is a miss, not an error: the
    /// transform drops it rather than failing the batch.
    #[test]
    fn an_unknown_selector_is_a_miss_not_an_error() {
        let abi = Abi::from_json(ERC20).expect("ABI loads");
        assert!(
            abi.decode_log(&log(
                Some(B256::from([0x99; 32])),
                [None, None, None],
                Vec::new()
            ))
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
            abi.decode_log(&log(None, [None, None, None], Vec::new()))
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
        let selector: B256 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
            .parse()
            .expect("selector parses");
        // Half a word where the event declares a `uint256`.
        assert!(matches!(
            abi.decode_log(&log(Some(selector), [None, None, None], vec![0u8; 16])),
            Err(DecodeError::Decode { .. })
        ));
    }

    /// An ABI that is not JSON fails loudly at load, not at first decode.
    #[test]
    fn malformed_abi_json_is_rejected_at_load() {
        assert!(matches!(
            Abi::from_json("not an abi"),
            Err(DecodeError::Abi { .. })
        ));
    }

    /// A real Uniswap V3 `Swap` log: the `int256` amounts are signed, the pool's
    /// `uint160` price is read at 160 bits, and the trailing `int24` tick is not
    /// silently widened.
    ///
    /// Every other test here builds its own bytes, so they would all still pass if the
    /// decoder agreed with itself about a layout the chain does not use. This one pins
    /// the layout to the chain.
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
        let data = alloy_primitives::hex::decode(
            "fffffffffffffffffffffffffffffffffffffffffffffffffff4b34627fb9302\
             0000000000000000000000000000000000000000000000000000000000830544\
             00000000000000000000000000000000000000000003678007a6bbf505d858fa\
             00000000000000000000000000000000000000000000000012fb062ae6731f9d\
             fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcfd3b",
        )
        .expect("fixture is valid hex");

        let mut source = log(Some(topic0), [Some(topic1), Some(topic1), None], data);
        source.topic3 = None;
        let decoded = abi
            .decode_log(&source)
            .expect("the log decodes")
            .expect("the ABI declares Swap");

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
        // `amount0` is negative: this swap sold token0, and reading the two's complement
        // word as unsigned would produce 1.15e77 instead.
        assert_eq!(
            decoded.body.first().map(|arg| &arg.value),
            Some(&TypedValue::Int {
                value: I256::try_from(-3_180_585_820_646_654_i64).expect("fits"),
                bits: 256,
            })
        );
        // `sqrtPriceX96` is `uint160`, not `uint256`: a widened column would be wrong.
        assert!(matches!(
            decoded.body.get(2).map(|arg| &arg.value),
            Some(TypedValue::Uint { bits: 160, .. })
        ));
        // `tick` is `int24`, and the decoded value carries that width.
        assert!(matches!(
            decoded.body.get(4).map(|arg| &arg.value),
            Some(TypedValue::Int { bits: 24, .. })
        ));
    }

    /// The canonical Uniswap V4 `PoolManager` ABI declares `Initialize` and decodes its
    /// flat layout: topics carry `id`, `currency0`, `currency1`; the rest is `data`.
    ///
    /// V4 pools have no address — the pool is the `bytes32` `id`, a hash of the
    /// `PoolKey`. The selector is derived from the ABI rather than pinned, so a typo in
    /// the signature fails here instead of silently never matching a real log.
    #[test]
    fn the_uniswap_v4_pool_manager_abi_declares_and_decodes_initialize() {
        const SIGNATURE: &str =
            "Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)";
        let abi = Abi::from_json(include_str!("../../abis/uniswap_v4_pool_manager.json"))
            .expect("the PoolManager ABI loads");
        let selector = abi.selector(SIGNATURE).expect("declares Initialize");

        let mut data = Vec::new();
        data.extend_from_slice(word(3_000).as_slice()); // fee: uint24
        data.extend_from_slice(word(60).as_slice()); // tickSpacing: int24
        data.extend_from_slice(word(0).as_slice()); // hooks: address(0)
        data.extend_from_slice(word(1_000_000).as_slice()); // sqrtPriceX96: uint160
        data.extend_from_slice(word(0).as_slice()); // tick: int24
        let decoded = abi
            .decode_log(&log(
                Some(selector),
                [
                    Some(B256::from([0x22; 32])),
                    Some(B256::from([0x33; 32])),
                    Some(B256::from([0x44; 32])),
                ],
                data,
            ))
            .expect("the log decodes")
            .expect("the ABI declares Initialize");

        assert_eq!(decoded.name, "Initialize");
        let names: Vec<&str> = decoded
            .indexed
            .iter()
            .chain(&decoded.body)
            .map(|arg| arg.name.as_str())
            .collect();
        assert_eq!(
            names,
            [
                "id",
                "currency0",
                "currency1",
                "fee",
                "tickSpacing",
                "hooks",
                "sqrtPriceX96",
                "tick"
            ]
        );
    }

    /// A nested value keeps its width at every level, because a width is not recoverable
    /// from a value and losing it would collapse a `uint8` and a `uint256` into the same
    /// column.
    #[test]
    fn widths_survive_at_every_level() {
        let decoded = DynSolValue::Tuple(vec![
            DynSolValue::Uint(U256::from(1), 8),
            DynSolValue::Array(vec![
                DynSolValue::Int(I256::MINUS_ONE, 256),
                DynSolValue::Int(I256::ZERO, 32),
            ]),
            DynSolValue::FixedArray(vec![DynSolValue::Uint(U256::from(2), 64)]),
        ]);
        assert_eq!(
            round_trip(&decoded),
            TypedValue::Tuple {
                value: vec![
                    TypedValue::Uint {
                        value: U256::from(1),
                        bits: 8,
                    },
                    TypedValue::Array {
                        value: vec![
                            TypedValue::Int {
                                value: I256::MINUS_ONE,
                                bits: 256,
                            },
                            TypedValue::Int {
                                value: I256::ZERO,
                                bits: 32,
                            },
                        ],
                    },
                    TypedValue::FixedArray {
                        value: vec![TypedValue::Uint {
                            value: U256::from(2),
                            bits: 64,
                        }],
                        size: 1,
                    },
                ],
            }
        );
    }

    /// `FixedBytes` carries only its declared bytes, not the zero padding to a word, so
    /// a `bytes4` is four bytes on the wire and not thirty-two. Re-encoding pads it back,
    /// which is what keeps the value lossless despite the trim.
    #[test]
    fn fixed_bytes_carries_only_its_declared_size() {
        let typed = round_trip(&DynSolValue::FixedBytes(padded(&[0xab; 4]), 4));
        let TypedValue::FixedBytes { value, size } = typed else {
            panic!("a fixed-bytes value must convert to fixed bytes");
        };
        assert_eq!(size, 4);
        assert_eq!(value.as_ref(), &[0xab; 4]);
    }

    /// A Solidity `function` is the one type the wire shape does not carry, so it fails
    /// loudly rather than being coerced into something it is not.
    #[test]
    fn a_function_type_is_refused_rather_than_mangled() {
        assert!(matches!(
            value_to_typed(&DynSolValue::Function(alloy_primitives::Function::ZERO)),
            Err(DecodeError::Unsupported("function type"))
        ));
    }
}
