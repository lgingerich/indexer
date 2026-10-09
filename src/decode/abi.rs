//! Prepared event ABIs and schema-aware value conversion.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use alloy_dyn_abi::{DynSolEvent, DynSolType, DynSolValue, Specifier as _};
use alloy_json_abi::{Event, JsonAbi};
use alloy_primitives::{B256, Bytes, keccak256};
use thiserror::Error;

use crate::wire::datasets::evm::Log;
use crate::wire::envelope::DecodedArg;
use crate::wire::typed::{AbiType, TypedValue};

/// A decoded event without chain or transaction metadata.
#[derive(Debug)]
pub struct DecodedEvent {
    /// Identity of the event definition used: see [`Abi`].
    pub id: B256,
    /// ABI event name.
    pub name: String,
    /// Event signature hash.
    pub selector: B256,
    /// Canonical event signature.
    pub signature: String,
    /// Indexed arguments with their original positions and declared schemas.
    pub indexed: Vec<DecodedArg>,
    /// Non-indexed arguments with their original positions and declared schemas.
    pub body: Vec<DecodedArg>,
}

#[derive(Debug, Clone)]
struct Argument {
    name: String,
    position: usize,
    schema: AbiType,
    ty: DynSolType,
}

#[derive(Debug, Clone)]
struct PreparedEvent {
    id: B256,
    name: String,
    signature: String,
    layout: DynSolEvent,
    indexed: Vec<Argument>,
    body: Vec<Argument>,
}

/// An event's lookup key: its selector and how many topics it occupies, `topic0`
/// included.
///
/// The selector alone is not enough. ERC-20 and ERC-721 `Transfer` share a signature and
/// so a selector, but index a different number of arguments; the topic count is what
/// tells their layouts apart.
pub(crate) type EventKey = (B256, usize);

/// Immutable event ABI, validated and prepared once before processing logs.
///
/// Built from one or more ABI files: a contract upgraded behind a proxy lists every
/// version, and their events merge by selector and topic count. Each event carries its own
/// identity — a hash of its definition — so a decoded record names the one event it used
/// rather than the file it came from.
#[derive(Debug, Clone, Default)]
pub struct Abi {
    events: BTreeMap<EventKey, PreparedEvent>,
}

impl Abi {
    /// Parses and prepares all events. Anonymous events are explicitly unsupported.
    ///
    /// # Errors
    /// Returns a typed parse/layout error or rejects anonymous and conflicting events.
    pub fn from_json(json: &str) -> Result<Self, AbiError> {
        let abi: JsonAbi = serde_json::from_str(json)?;
        let mut events = BTreeMap::new();
        for (event_index, event) in abi.events().enumerate() {
            if event.anonymous {
                return Err(AbiError::Anonymous {
                    event: event.name.clone(),
                });
            }
            let key = (
                event.selector(),
                1 + event.inputs.iter().filter(|input| input.indexed).count(),
            );
            // The definition's identity: name, types, indexed flags, and parameter and
            // component names. Not the JSON, whose compiler-specific `internalType`s differ
            // between builds of an unchanged event.
            let id = keccak256(event.full_signature());
            let prepared = PreparedEvent::new(event, id).map_err(|source| AbiError::Layout {
                event_index,
                source,
            })?;
            if events.insert(key, prepared).is_some() {
                return Err(AbiError::Conflict { selector: key.0 });
            }
        }
        Ok(Self { events })
    }

    /// Adds `other`'s events. An event both declare identically is kept once.
    ///
    /// # Errors
    /// Returns [`AbiError::Conflict`] when the two define one selector and topic count
    /// differently, since a log could then decode either way.
    pub fn merge(&mut self, other: Self) -> Result<(), AbiError> {
        for (key, event) in other.events {
            match self.events.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert(event);
                }
                Entry::Occupied(entry) if entry.get().id == event.id => {}
                Entry::Occupied(_) => return Err(AbiError::Conflict { selector: key.0 }),
            }
        }
        Ok(())
    }

    /// Every event, with its inputs in declaration order: what an event table is
    /// generated from.
    pub(crate) fn events(&self) -> impl Iterator<Item = EventSpec<'_>> {
        self.events.values().map(|event| {
            let mut inputs: Vec<InputSpec<'_>> = event
                .indexed
                .iter()
                .map(|argument| (argument, true))
                .chain(event.body.iter().map(|argument| (argument, false)))
                .map(|(argument, indexed)| InputSpec {
                    name: &argument.name,
                    position: argument.position,
                    ty: &argument.ty,
                    indexed,
                })
                .collect();
            inputs.sort_by_key(|input| input.position);
            EventSpec {
                id: event.id,
                name: &event.name,
                inputs,
            }
        })
    }

    /// The keys of every event declared under `name`.
    pub(crate) fn events_named(&self, name: &str) -> Vec<EventKey> {
        self.events
            .iter()
            .filter(|(_, event)| event.name == name)
            .map(|(key, _)| *key)
            .collect()
    }

    /// The position of `event`'s `address` input called `param`.
    pub(crate) fn address_input(&self, event: EventKey, param: &str) -> Option<usize> {
        let event = self.events.get(&event)?;
        event
            .indexed
            .iter()
            .chain(&event.body)
            .find(|argument| argument.name == param && argument.ty == DynSolType::Address)
            .map(|argument| argument.position)
    }

    /// The identity of the event a log would decode as, for diagnostics.
    pub(crate) fn event_id(&self, log: &Log) -> Option<B256> {
        let key = (log.topic0?, topic_count(log));
        self.events.get(&key).map(|event| event.id)
    }

    /// Decodes a raw log; unknown selectors are ordinary misses.
    ///
    /// # Errors
    /// Reports malformed topics, layout mismatches, and invalid decoded values.
    pub fn decode_log(&self, log: &Log) -> Result<Option<DecodedEvent>, DecodeError> {
        let flattened = [log.topic0, log.topic1, log.topic2, log.topic3];
        let mut topics = Vec::with_capacity(4);
        let mut gap = false;
        for topic in flattened {
            match topic {
                Some(topic) if !gap => topics.push(topic),
                Some(_) => return Err(DecodeError::TopicGap),
                None => gap = true,
            }
        }
        let Some(selector) = topics.first() else {
            return Ok(None);
        };
        let Some(event) = self.events.get(&(*selector, topics.len())) else {
            return Ok(None);
        };
        let decoded = event
            .layout
            .decode_log_parts(topics.iter().copied(), &log.data)
            .map_err(DecodeError::Log)?;
        Ok(Some(DecodedEvent {
            id: event.id,
            name: event.name.clone(),
            selector: *selector,
            signature: event.signature.clone(),
            indexed: arguments(&event.indexed, &decoded.indexed, true)?,
            body: arguments(&event.body, &decoded.body, false)?,
        }))
    }
}

/// How many topics a log carries, counting up to the first absent one.
fn topic_count(log: &Log) -> usize {
    [log.topic0, log.topic1, log.topic2, log.topic3]
        .iter()
        .take_while(|topic| topic.is_some())
        .count()
}

/// One event of an [`Abi`], as the catalog turns it into a table.
#[derive(Debug)]
pub(crate) struct EventSpec<'a> {
    /// The definition's identity; see [`Abi`].
    pub(crate) id: B256,
    /// The event name.
    pub(crate) name: &'a str,
    /// Every input, in declaration order.
    pub(crate) inputs: Vec<InputSpec<'a>>,
}

/// One input of an [`EventSpec`].
#[derive(Debug)]
pub(crate) struct InputSpec<'a> {
    /// The input name; may be empty.
    pub(crate) name: &'a str,
    /// The position among all inputs.
    pub(crate) position: usize,
    /// The resolved type.
    pub(crate) ty: &'a DynSolType,
    /// Whether it is a topic.
    pub(crate) indexed: bool,
}

impl PreparedEvent {
    fn new(event: &Event, id: B256) -> Result<Self, alloy_dyn_abi::Error> {
        let resolved = event.resolve()?;
        let mut indexed = Vec::new();
        let mut body = Vec::new();
        for (position, param) in event.inputs.iter().enumerate() {
            let ty = param.resolve()?;
            let argument = Argument {
                name: param.name.clone(),
                position,
                schema: AbiType::from(param),
                ty,
            };
            if param.indexed {
                indexed.push(argument);
            } else {
                body.push(argument);
            }
        }
        let layout = DynSolEvent::new(
            resolved.topic_0(),
            resolved.indexed().iter().map(decoding_type).collect(),
            DynSolType::Tuple(resolved.body().iter().map(decoding_type).collect()),
        )
        .ok_or_else(|| alloy_dyn_abi::Error::custom("prepared event layout is invalid"))?;
        Ok(Self {
            id,
            name: event.name.clone(),
            signature: event.signature(),
            layout,
            indexed,
            body,
        })
    }
}

// Solidity string and bytes have the same ABI layout. Decode strings as bytes so Alloy's
// lossy UTF-8 conversion cannot alter data; the original schema drives conversion below.
fn decoding_type(ty: &DynSolType) -> DynSolType {
    match ty {
        DynSolType::String => DynSolType::Bytes,
        // Decode words without coercing malformed booleans or truncating addresses.
        DynSolType::Bool | DynSolType::Address => DynSolType::Uint(256),
        DynSolType::Array(inner) => DynSolType::Array(Box::new(decoding_type(inner))),
        DynSolType::FixedArray(inner, size) => {
            DynSolType::FixedArray(Box::new(decoding_type(inner)), *size)
        }
        DynSolType::Tuple(types) => DynSolType::Tuple(types.iter().map(decoding_type).collect()),
        _ => ty.clone(),
    }
}

fn hashed(ty: &DynSolType) -> bool {
    matches!(
        ty,
        DynSolType::String
            | DynSolType::Bytes
            | DynSolType::Array(_)
            | DynSolType::FixedArray(_, _)
            | DynSolType::Tuple(_)
    )
}

fn arguments(
    params: &[Argument],
    values: &[DynSolValue],
    indexed: bool,
) -> Result<Vec<DecodedArg>, DecodeError> {
    if params.len() != values.len() {
        return Err(DecodeError::Shape);
    }
    params
        .iter()
        .zip(values)
        .map(|(param, value)| {
            let value = if indexed && hashed(&param.ty) {
                let DynSolValue::FixedBytes(value, 32) = value else {
                    return Err(DecodeError::Shape);
                };
                TypedValue::IndexedHash { value: *value }
            } else {
                convert(&param.ty, value)?
            };
            Ok(DecodedArg {
                name: param.name.clone(),
                position: param.position,
                abi_type: param.schema.clone(),
                value,
            })
        })
        .collect()
}

fn convert(ty: &DynSolType, value: &DynSolValue) -> Result<TypedValue, DecodeError> {
    Ok(match (ty, value) {
        (DynSolType::Bool, DynSolValue::Uint(value, _)) => {
            if *value > alloy_primitives::U256::from(1) {
                return Err(DecodeError::Value {
                    kind: "boolean must be zero or one",
                });
            }
            TypedValue::Bool {
                value: !value.is_zero(),
            }
        }
        (DynSolType::Address, DynSolValue::Uint(value, _)) => {
            if value.bit_len() > 160 {
                return Err(DecodeError::Value {
                    kind: "address exceeds 160 bits",
                });
            }
            TypedValue::Address {
                value: alloy_primitives::Address::from_word(B256::from(value.to_be_bytes::<32>())),
            }
        }
        (DynSolType::Function, DynSolValue::Function(value)) => {
            TypedValue::Function { value: *value }
        }
        (DynSolType::Uint(bits), DynSolValue::Uint(value, _)) if valid_bits(*bits) => {
            if value.bit_len() > *bits {
                return Err(DecodeError::Value {
                    kind: "unsigned integer exceeds declared width",
                });
            }
            TypedValue::Uint {
                value: *value,
                bits: width(*bits)?,
            }
        }
        (DynSolType::Int(bits), DynSolValue::Int(value, _)) if valid_bits(*bits) => {
            if *bits < 256 {
                let shifted = value.asr(*bits - 1);
                if shifted != alloy_primitives::I256::ZERO
                    && shifted != alloy_primitives::I256::MINUS_ONE
                {
                    return Err(DecodeError::Value {
                        kind: "signed integer exceeds declared width",
                    });
                }
            }
            TypedValue::Int {
                value: *value,
                bits: width(*bits)?,
            }
        }
        (DynSolType::FixedBytes(size), DynSolValue::FixedBytes(value, _)) => {
            TypedValue::FixedBytes {
                value: Bytes::copy_from_slice(
                    value.as_slice().get(..*size).ok_or(DecodeError::Shape)?,
                ),
                size: width(*size)?,
            }
        }
        (DynSolType::Bytes, DynSolValue::Bytes(value)) => TypedValue::Bytes {
            value: Bytes::copy_from_slice(value),
        },
        (DynSolType::String, DynSolValue::Bytes(value)) => TypedValue::String {
            value: Bytes::copy_from_slice(value),
            // `PostgreSQL` rejects NUL in `text` and `jsonb`.
            text: std::str::from_utf8(value)
                .ok()
                .filter(|text| !text.contains('\0'))
                .map(str::to_owned),
        },
        (DynSolType::Array(inner), DynSolValue::Array(values)) => TypedValue::Array {
            value: values
                .iter()
                .map(|value| convert(inner, value))
                .collect::<Result<_, _>>()?,
        },
        (DynSolType::FixedArray(inner, size), DynSolValue::FixedArray(values))
            if *size == values.len() =>
        {
            TypedValue::FixedArray {
                value: values
                    .iter()
                    .map(|value| convert(inner, value))
                    .collect::<Result<_, _>>()?,
                size: *size,
            }
        }
        (DynSolType::Tuple(types), DynSolValue::Tuple(values)) if types.len() == values.len() => {
            TypedValue::Tuple {
                value: types
                    .iter()
                    .zip(values)
                    .map(|(ty, value)| convert(ty, value))
                    .collect::<Result<_, _>>()?,
            }
        }
        _ => return Err(DecodeError::Shape),
    })
}

fn valid_bits(bits: usize) -> bool {
    (8..=256).contains(&bits) && bits.is_multiple_of(8)
}

fn width(value: usize) -> Result<u16, DecodeError> {
    u16::try_from(value).map_err(|_| DecodeError::Shape)
}

/// Failures while loading and preparing an ABI, before any logs are processed.
#[derive(Debug, Error)]
pub enum AbiError {
    /// ABI JSON parse or serialization error.
    #[error("invalid ABI JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// Event type preparation failed.
    #[error("invalid ABI event at index {event_index}: {source}")]
    Layout {
        /// Zero-based event index in the parsed ABI's event iteration order.
        event_index: usize,
        /// Concrete type resolution error.
        source: alloy_dyn_abi::Error,
    },
    /// Anonymous events cannot be selected unambiguously.
    #[error("anonymous event {event} is unsupported")]
    Anonymous {
        /// Unsupported event name.
        event: String,
    },
    /// Two different definitions claim the same selector and topic count.
    #[error("conflicting event definitions for selector {selector}")]
    Conflict {
        /// Ambiguous selector.
        selector: B256,
    },
}

/// Failures while decoding a log against an already prepared ABI.
/// Log and registration context belongs to the caller, not the reusable codec error.
#[derive(Debug, Error)]
pub enum DecodeError {
    /// Raw log does not match the prepared layout.
    #[error("log does not match its event layout: {0}")]
    Log(#[from] alloy_dyn_abi::Error),
    /// Topics must be contiguous.
    #[error("log topics contain a gap")]
    TopicGap,
    /// Decoder result disagrees with the prepared schema: an internal invariant failure.
    #[error("decoded value shape disagrees with prepared ABI schema")]
    Shape,
    /// A decoded value violates its declared type.
    #[error("invalid ABI value: {kind}")]
    Value {
        /// Violated constraint.
        kind: &'static str,
    },
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use super::*;
    use alloy_primitives::U256;

    fn decode(abi: &str, values: &[DynSolValue], indexed: Option<B256>) -> DecodedEvent {
        let abi = Abi::from_json(abi).expect("valid ABI");
        let (selector, _) = *abi.events.keys().next().expect("event");
        abi.decode_log(&Log {
            topic0: Some(selector),
            topic1: indexed,
            data: DynSolValue::Tuple(values.to_vec())
                .abi_encode_params()
                .into(),
            ..Log::default()
        })
        .expect("decode")
        .expect("match")
    }

    #[test]
    fn invalid_utf8_nul_and_nested_strings_remain_bytes() {
        let result = decode(
            r#"[{"type":"event","name":"Text","anonymous":false,"inputs":[{"name":"s","type":"string[]","indexed":false}]}]"#,
            &[DynSolValue::Array(vec![
                DynSolValue::Bytes(vec![0xff]),
                DynSolValue::Bytes(b"hello".to_vec()),
                DynSolValue::Bytes(b"a\0b".to_vec()),
            ])],
            None,
        );
        let TypedValue::Array { value } = &result.body[0].value else {
            panic!("array");
        };
        assert_eq!(
            value[0],
            TypedValue::String {
                value: Bytes::from_static(&[0xff]),
                text: None
            }
        );
        assert_eq!(
            value[1],
            TypedValue::String {
                value: Bytes::from_static(b"hello"),
                text: Some("hello".into())
            }
        );
        assert_eq!(
            value[2],
            TypedValue::String {
                value: Bytes::from_static(b"a\0b"),
                text: None
            }
        );
    }

    #[test]
    fn schema_preserves_empty_arrays_tuple_names_and_original_positions() {
        let result = decode(
            r#"[{"type":"event","name":"Data","anonymous":false,"inputs":[{"name":"id","type":"bytes32","indexed":true},{"name":"entries","type":"tuple[]","indexed":false,"components":[{"name":"owner","type":"address"}]}]}]"#,
            &[DynSolValue::Array(vec![])],
            Some(B256::ZERO),
        );
        assert_eq!(result.body[0].position, 1);
        assert_eq!(result.body[0].abi_type.kind, "tuple[]");
        assert_eq!(result.body[0].abi_type.components[0].name, "owner");
        assert_eq!(
            result.body[0].abi_type.components[0].abi_type.kind,
            "address"
        );
        assert!(matches!(
            result.indexed[0].value,
            TypedValue::FixedBytes { .. }
        ));
    }

    #[test]
    fn indexed_strings_are_hashes_not_values() {
        let hash = keccak256("text");
        let result = decode(
            r#"[{"type":"event","name":"Text","anonymous":false,"inputs":[{"name":"s","type":"string","indexed":true}]}]"#,
            &[],
            Some(hash),
        );
        assert_eq!(
            result.indexed[0].value,
            TypedValue::IndexedHash { value: hash }
        );
        assert_eq!(result.indexed[0].abi_type.kind, "string");
    }

    #[test]
    fn functions_decode_and_invalid_integer_widths_fail() {
        let result = decode(
            r#"[{"type":"event","name":"Callback","anonymous":false,"inputs":[{"name":"f","type":"function","indexed":false}]}]"#,
            &[DynSolValue::Function(alloy_primitives::Function::ZERO)],
            None,
        );
        assert!(matches!(result.body[0].value, TypedValue::Function { .. }));
        assert!(matches!(
            convert(&DynSolType::Uint(8), &DynSolValue::Uint(U256::from(256), 8)),
            Err(DecodeError::Value { .. })
        ));
    }

    #[test]
    fn signed_widths_and_scalar_constraints_are_checked() {
        use alloy_primitives::{I256, U256};
        assert!(convert(&DynSolType::Int(8), &DynSolValue::Int(I256::MINUS_ONE, 8)).is_ok());
        assert!(
            convert(
                &DynSolType::Int(8),
                &DynSolValue::Int(I256::try_from(128).expect("integer"), 8)
            )
            .is_err()
        );
        assert!(
            convert(
                &DynSolType::Int(8),
                &DynSolValue::Int(I256::try_from(-129).expect("integer"), 8)
            )
            .is_err()
        );
        assert!(convert(&DynSolType::Bool, &DynSolValue::Uint(U256::from(2), 256)).is_err());
        assert!(convert(&DynSolType::Address, &DynSolValue::Uint(U256::MAX, 256)).is_err());
    }

    const ERC20_TRANSFER: &str = r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[{"name":"from","type":"address","indexed":true},{"name":"to","type":"address","indexed":true},{"name":"value","type":"uint256","indexed":false}]}]"#;
    const ERC721_TRANSFER: &str = r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[{"name":"from","type":"address","indexed":true},{"name":"to","type":"address","indexed":true},{"name":"tokenId","type":"uint256","indexed":true}]}]"#;

    /// One selector, two layouts: the topic count picks the one a log was emitted with,
    /// and each carries its own identity.
    #[test]
    fn a_shared_selector_decodes_by_topic_count() {
        let mut abi = Abi::from_json(ERC20_TRANSFER).expect("erc20");
        abi.merge(Abi::from_json(ERC721_TRANSFER).expect("erc721"))
            .expect("different topic counts do not conflict");
        let (selector, _) = *abi.events.keys().next().expect("event");
        let fungible = Log {
            topic0: Some(selector),
            topic1: Some(B256::ZERO),
            topic2: Some(B256::ZERO),
            data: DynSolValue::Uint(U256::from(5), 256).abi_encode().into(),
            ..Log::default()
        };
        let token = Log {
            topic3: Some(B256::with_last_byte(5)),
            data: Bytes::new(),
            ..fungible
        };
        let fungible = abi.decode_log(&fungible).expect("decode").expect("erc20");
        let token = abi.decode_log(&token).expect("decode").expect("erc721");
        assert_eq!(fungible.body[0].name, "value");
        assert_eq!(token.indexed[2].name, "tokenId");
        assert_ne!(fungible.id, token.id);
    }

    /// Merging another version keeps an identical event once and refuses a different
    /// definition under the same key, since a log could then decode either way.
    #[test]
    fn merging_dedupes_identical_events_and_rejects_conflicts() {
        let mut abi = Abi::from_json(ERC20_TRANSFER).expect("erc20");
        abi.merge(Abi::from_json(ERC20_TRANSFER).expect("erc20"))
            .expect("identical events merge");
        assert_eq!(abi.events.len(), 1);
        let renamed = ERC20_TRANSFER.replace("\"value\"", "\"amount\"");
        assert!(matches!(
            abi.merge(Abi::from_json(&renamed).expect("renamed")),
            Err(AbiError::Conflict { .. })
        ));
    }

    /// An event's identity is its own definition: an unrelated event added to the file
    /// does not move it.
    #[test]
    fn an_event_id_ignores_the_rest_of_the_file() {
        let alone = Abi::from_json(ERC20_TRANSFER).expect("alone");
        let with_more = Abi::from_json(&ERC20_TRANSFER.replace(
            "]}]",
            r#"]},{"type":"event","name":"Other","anonymous":false,"inputs":[]}]"#,
        ))
        .expect("with another event");
        let id = |abi: &Abi| {
            abi.events
                .values()
                .find(|e| e.name == "Transfer")
                .expect("transfer")
                .id
        };
        assert_eq!(id(&alone), id(&with_more));
    }

    #[test]
    fn actual_swap_fixture_decodes() {
        let abi = Abi::from_json(include_str!(
            "../../protocols/uniswap/v3/UniswapV3Pool.json"
        ))
        .expect("pool ABI");
        let source: crate::wire::envelope::Envelope = serde_json::from_str(
            include_str!("../../examples/fixtures/uniswap_v3_swaps.ndjson")
                .lines()
                .next()
                .expect("fixture"),
        )
        .expect("envelope");
        let crate::wire::envelope::Event::Log(log) = source.event else {
            panic!("log");
        };
        let decoded = abi.decode_log(&log).expect("decode").expect("match");
        assert_eq!(decoded.name, "Swap");
        assert!(matches!(
            decoded.body[0].value,
            TypedValue::Int { bits: 256, .. }
        ));
    }
}
