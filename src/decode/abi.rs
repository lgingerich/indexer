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
    /// Parses and prepares all events. Anonymous events are explicitly unsupported. An
    /// event the file lists twice identically is kept once, as across files.
    ///
    /// # Errors
    /// Returns a typed parse/layout error or rejects anonymous and conflicting events.
    pub fn from_json(json: &str) -> Result<Self, AbiError> {
        let abi: JsonAbi = serde_json::from_str(json)?;
        let mut parsed = Self::default();
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
            parsed.add(key, prepared)?;
        }
        Ok(parsed)
    }

    /// Adds `other`'s events. An event both declare identically is kept once.
    ///
    /// # Errors
    /// Returns [`AbiError::Conflict`] when the two define one selector and topic count
    /// differently, since a log could then decode either way.
    pub fn merge(&mut self, other: Self) -> Result<(), AbiError> {
        for (key, event) in other.events {
            self.add(key, event)?;
        }
        Ok(())
    }

    /// Adds `event` under `key`, keeping an identical one already there once.
    fn add(&mut self, key: EventKey, event: PreparedEvent) -> Result<(), AbiError> {
        match self.events.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(event);
            }
            Entry::Occupied(entry) if entry.get().id == event.id => {}
            Entry::Occupied(_) => return Err(AbiError::Conflict { selector: key.0 }),
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

    /// Whether an event has the canonical `signature`, for example
    /// `Swap(address,address,uint256)`.
    pub(crate) fn declares(&self, signature: &str) -> bool {
        self.events
            .values()
            .any(|event| event.signature == signature)
    }

    /// The keys of every event declared under `name`.
    pub(crate) fn events_named(&self, name: &str) -> Vec<EventKey> {
        self.events
            .iter()
            .filter(|(_, event)| event.name == name)
            .map(|(key, _)| *key)
            .collect()
    }

    /// Where `event`'s `address` input called `param` sits in a log.
    pub(crate) fn address_input(&self, event: EventKey, param: &str) -> Option<AddressSlot> {
        let event = self.events.get(&event)?;
        let named =
            |argument: &&Argument| argument.name == param && argument.ty == DynSolType::Address;
        if let Some(index) = event.indexed.iter().position(|argument| named(&argument)) {
            return Some(AddressSlot::Topic(index + 1));
        }
        let index = event.body.iter().position(|argument| named(&argument))?;
        let offset = event.body[..index]
            .iter()
            .map(|argument| head_words(&argument.ty) * 32)
            .sum();
        Some(AddressSlot::Word(offset))
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

/// Where an `address` argument sits in a log of its event, so it can be read without
/// decoding the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AddressSlot {
    /// Topic `n`, `topic0` being the selector.
    Topic(usize),
    /// The word at this byte offset of the data. An address is static, so its word is in
    /// the head, after the head words of the body arguments before it.
    Word(usize),
}

impl AddressSlot {
    /// The address in `log`'s slot, or `None` when the log is too short for it or its
    /// word holds more than 160 bits, which the decoder refuses as well.
    pub(crate) fn read(self, log: &Log) -> Option<alloy_primitives::Address> {
        let word = match self {
            Self::Topic(index) => [log.topic1, log.topic2, log.topic3]
                .get(index.checked_sub(1)?)
                .copied()
                .flatten()?,
            Self::Word(offset) => B256::from_slice(log.data.get(offset..offset.checked_add(32)?)?),
        };
        word[..12]
            .iter()
            .all(|&byte| byte == 0)
            .then(|| alloy_primitives::Address::from_word(word))
    }
}

/// A log's lookup key, or `None` when it has no selector or a gap in its topics.
pub(crate) fn event_key(log: &Log) -> Option<EventKey> {
    let count = topic_count(log);
    let present = [log.topic0, log.topic1, log.topic2, log.topic3]
        .iter()
        .filter(|topic| topic.is_some())
        .count();
    Some((log.topic0?, count)).filter(|_| present == count)
}

/// How many head words a body argument of type `ty` takes: one for a dynamic type, which
/// the head holds as an offset, and its whole encoding for a static one.
fn head_words(ty: &DynSolType) -> usize {
    if ty.is_dynamic() {
        return 1;
    }
    match ty {
        DynSolType::Tuple(types) => types.iter().map(head_words).sum(),
        DynSolType::FixedArray(inner, size) => head_words(inner) * size,
        _ => 1,
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
            let ty = plain(param.resolve()?);
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
            indexed
                .iter()
                .map(|argument| decoding_type(&argument.ty))
                .collect(),
            DynSolType::Tuple(
                body.iter()
                    .map(|argument| decoding_type(&argument.ty))
                    .collect(),
            ),
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

/// `ty` with every struct a plain tuple.
///
/// With alloy's `eip712` feature on, which any crate in the build can enable, a tuple
/// argument whose `internalType` names a struct resolves to `CustomStruct` rather than
/// `Tuple`. Both encode alike, so the decoder treats them alike rather than meeting a
/// variant it does not match. `as_custom_struct` exists with or without the feature,
/// which is what lets this compile either way.
fn plain(ty: DynSolType) -> DynSolType {
    if let Some((_, _, tuple)) = ty.as_custom_struct() {
        return DynSolType::Tuple(tuple.iter().cloned().map(plain).collect());
    }
    match ty {
        DynSolType::Array(inner) => DynSolType::Array(Box::new(plain(*inner))),
        DynSolType::FixedArray(inner, size) => {
            DynSolType::FixedArray(Box::new(plain(*inner)), size)
        }
        DynSolType::Tuple(types) => DynSolType::Tuple(types.into_iter().map(plain).collect()),
        ty => ty,
    }
}

// Solidity string and bytes have the same ABI layout. Decode strings as bytes so Alloy's
// lossy UTF-8 conversion cannot alter data; the original schema drives conversion below.
fn decoding_type(ty: &DynSolType) -> DynSolType {
    match ty {
        DynSolType::String => DynSolType::Bytes,
        // Decode words without coercing malformed booleans or truncating addresses.
        DynSolType::Bool | DynSolType::Address => DynSolType::Uint(256),
        // A function is 24 bytes of its word; decode all 32 so its padding is checked.
        DynSolType::Function => DynSolType::FixedBytes(32),
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
        (DynSolType::Function, DynSolValue::FixedBytes(word, _)) => TypedValue::Function {
            value: alloy_primitives::Function::from_slice(padded(
                word,
                24,
                "function has nonzero padding",
            )?),
        },
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
        (DynSolType::FixedBytes(size), DynSolValue::FixedBytes(word, _)) => {
            TypedValue::FixedBytes {
                value: Bytes::copy_from_slice(padded(word, *size, "bytesN has nonzero padding")?),
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

/// The first `size` bytes of `word`, refused for `kind` when any byte after them is not
/// zero, as a word holding more than its integer type is.
fn padded<'a>(word: &'a B256, size: usize, kind: &'static str) -> Result<&'a [u8], DecodeError> {
    let (value, padding) = word
        .as_slice()
        .split_at_checked(size)
        .ok_or(DecodeError::Shape)?;
    if padding.iter().any(|&byte| byte != 0) {
        return Err(DecodeError::Value { kind });
    }
    Ok(value)
}

impl DecodeError {
    /// Whether this is an internal invariant failure rather than a bad log: the decoder
    /// disagreeing with its own prepared schema, after which continuing could publish
    /// wrong data. A caller skips any other failure and keeps the raw log.
    #[must_use]
    pub const fn is_invariant(&self) -> bool {
        matches!(self, Self::Shape)
    }
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
        assert_eq!(
            result.body[0].value,
            TypedValue::Array {
                value: vec![
                    TypedValue::String {
                        value: Bytes::from_static(&[0xff]),
                        text: None
                    },
                    TypedValue::String {
                        value: Bytes::from_static(b"hello"),
                        text: Some("hello".into())
                    },
                    TypedValue::String {
                        value: Bytes::from_static(b"a\0b"),
                        text: None
                    },
                ]
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
        assert_eq!(
            result.indexed[0].value,
            TypedValue::FixedBytes {
                value: Bytes::copy_from_slice(B256::ZERO.as_slice()),
                size: 32
            }
        );
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

    /// A tuple argument whose `internalType` names a struct decodes as a tuple, in the
    /// body, indexed, and in an array. With alloy's `eip712` feature on, such an argument
    /// resolves to a `CustomStruct`, which must not reach the decoder as a shape it does
    /// not match.
    #[test]
    fn struct_arguments_decode_as_tuples() {
        let key = |name: &str, kind: &str, indexed: bool| {
            format!(
                r#"{{"name":"{name}","type":"tuple{kind}","internalType":"struct Pool.Key{kind}",
                    "indexed":{indexed},"components":[
                    {{"name":"owner","type":"address","internalType":"address"}}]}}"#
            )
        };
        let abi = format!(
            r#"[{{"type":"event","name":"Keyed","anonymous":false,"inputs":[{},{},{}]}}]"#,
            key("hashed", "", true),
            key("key", "", false),
            key("keys", "[]", false),
        );
        let owner = DynSolValue::Tuple(vec![DynSolValue::Address(alloy_primitives::Address::ZERO)]);
        let result = decode(
            &abi,
            &[owner.clone(), DynSolValue::Array(vec![owner])],
            Some(B256::repeat_byte(1)),
        );
        let tuple = TypedValue::Tuple {
            value: vec![TypedValue::Address {
                value: alloy_primitives::Address::ZERO,
            }],
        };
        assert_eq!(
            result.indexed[0].value,
            TypedValue::IndexedHash {
                value: B256::repeat_byte(1)
            }
        );
        assert_eq!(result.body[0].value, tuple);
        assert_eq!(
            result.body[1].value,
            TypedValue::Array { value: vec![tuple] }
        );
    }

    #[test]
    fn functions_decode() {
        let result = decode(
            r#"[{"type":"event","name":"Callback","anonymous":false,"inputs":[{"name":"f","type":"function","indexed":false}]}]"#,
            &[DynSolValue::Function(alloy_primitives::Function::ZERO)],
            None,
        );
        assert_eq!(
            result.body[0].value,
            TypedValue::Function {
                value: alloy_primitives::Function::ZERO
            }
        );
    }

    /// An integer converts up to its declared width's extremes and is refused one past
    /// them, and a boolean or address word holding more than its type is refused, each
    /// naming why.
    #[test]
    fn integer_widths_and_scalar_constraints_are_checked() {
        use alloy_primitives::I256;
        let int = |value: i64| I256::try_from(value).expect("integer");
        for (case, ty, value, expected) in [
            (
                "int8 -128",
                DynSolType::Int(8),
                DynSolValue::Int(int(-128), 8),
                Ok(TypedValue::Int {
                    value: int(-128),
                    bits: 8,
                }),
            ),
            (
                "int8 127",
                DynSolType::Int(8),
                DynSolValue::Int(int(127), 8),
                Ok(TypedValue::Int {
                    value: int(127),
                    bits: 8,
                }),
            ),
            (
                "int8 128",
                DynSolType::Int(8),
                DynSolValue::Int(int(128), 8),
                Err("signed integer exceeds declared width"),
            ),
            (
                "int8 -129",
                DynSolType::Int(8),
                DynSolValue::Int(int(-129), 8),
                Err("signed integer exceeds declared width"),
            ),
            (
                "uint8 255",
                DynSolType::Uint(8),
                DynSolValue::Uint(U256::from(255), 8),
                Ok(TypedValue::Uint {
                    value: U256::from(255),
                    bits: 8,
                }),
            ),
            (
                "uint8 256",
                DynSolType::Uint(8),
                DynSolValue::Uint(U256::from(256), 8),
                Err("unsigned integer exceeds declared width"),
            ),
            (
                "bool 2",
                DynSolType::Bool,
                DynSolValue::Uint(U256::from(2), 256),
                Err("boolean must be zero or one"),
            ),
            (
                "address of 256 bits",
                DynSolType::Address,
                DynSolValue::Uint(U256::MAX, 256),
                Err("address exceeds 160 bits"),
            ),
        ] {
            let converted = convert(&ty, &value).map_err(|error| match error {
                DecodeError::Value { kind } => kind,
                other => panic!("{case}: {other:?}"),
            });
            assert_eq!(converted, expected, "{case}");
        }
    }

    /// A `bytesN` or `function` word with a nonzero byte past its value is refused, as a
    /// word holding more than its integer type is; with clean padding it decodes.
    #[test]
    fn short_bytes_and_functions_with_dirty_padding_are_refused() {
        let mut dirty = [0_u8; 32];
        dirty[31] = 1;
        let dirty = DynSolValue::FixedBytes(B256::from(dirty), 32);
        let clean = DynSolValue::FixedBytes(B256::ZERO, 32);
        for (ty, kind) in [
            (DynSolType::FixedBytes(4), "bytesN has nonzero padding"),
            (DynSolType::Function, "function has nonzero padding"),
        ] {
            assert!(convert(&ty, &clean).is_ok(), "{kind}");
            assert!(
                matches!(convert(&ty, &dirty), Err(DecodeError::Value { kind: found }) if found == kind),
                "{kind}"
            );
        }
        assert!(
            convert(&DynSolType::FixedBytes(32), &dirty).is_ok(),
            "bytes32 has no padding"
        );
    }

    /// An address argument is found where the encoder put it, past a dynamic argument's
    /// one head word and a static tuple's and fixed array's full encodings, or in its
    /// topic, and read from there without decoding the rest.
    #[test]
    fn an_address_argument_is_read_from_its_slot() {
        let abi = Abi::from_json(
            r#"[{"type":"event","name":"Made","anonymous":false,"inputs":[
                {"name":"owner","type":"address","indexed":true},
                {"name":"note","type":"bytes","indexed":false},
                {"name":"pair","type":"tuple","indexed":false,"components":[
                    {"name":"a","type":"uint256"},{"name":"b","type":"uint256"}]},
                {"name":"two","type":"uint8[2]","indexed":false},
                {"name":"child","type":"address","indexed":false}]}]"#,
        )
        .expect("valid ABI");
        let (key, _) = abi.events.first_key_value().expect("event");
        let child = alloy_primitives::Address::repeat_byte(0xc0);
        let owner = alloy_primitives::Address::repeat_byte(0x0e);
        let log = Log {
            topic0: Some(key.0),
            topic1: Some(owner.into_word()),
            data: DynSolValue::Tuple(vec![
                DynSolValue::Bytes(vec![1, 2, 3]),
                DynSolValue::Tuple(vec![
                    DynSolValue::Uint(U256::ZERO, 256),
                    DynSolValue::Uint(U256::ZERO, 256),
                ]),
                DynSolValue::FixedArray(vec![
                    DynSolValue::Uint(U256::ZERO, 8),
                    DynSolValue::Uint(U256::ZERO, 8),
                ]),
                DynSolValue::Address(child),
            ])
            .abi_encode_params()
            .into(),
            ..Log::default()
        };
        let slot = abi.address_input(*key, "child").expect("an address input");
        assert_eq!(slot, AddressSlot::Word(5 * 32));
        assert_eq!(slot.read(&log), Some(child));
        let slot = abi.address_input(*key, "owner").expect("an address input");
        assert_eq!(slot, AddressSlot::Topic(1));
        assert_eq!(slot.read(&log), Some(owner));
        assert_eq!(abi.address_input(*key, "note"), None, "not an address");
    }

    /// Garbled topics and data for an event with indexed string and tuple arguments and
    /// dynamic body ones are ordinary decode failures or decodes, never the invariant
    /// failure that stops the run: a hostile log must not be able to halt ingest.
    #[test]
    fn garbled_logs_never_fail_as_an_invariant() {
        let abi = Abi::from_json(
            r#"[{"type":"event","name":"Odd","anonymous":false,"inputs":[
                {"name":"label","type":"string","indexed":true},
                {"name":"key","type":"tuple","indexed":true,"components":[
                    {"name":"a","type":"address"},{"name":"b","type":"bool"}]},
                {"name":"note","type":"string","indexed":false},
                {"name":"pairs","type":"tuple[]","indexed":false,"components":[
                    {"name":"a","type":"bytes4"},{"name":"b","type":"int8"}]}]}]"#,
        )
        .expect("valid ABI");
        let (selector, _) = *abi.events.keys().next().expect("event");
        let words = [
            B256::ZERO,
            B256::repeat_byte(0xff),
            B256::with_last_byte(0x20),
        ];
        let mut checked = 0;
        for &topic1 in &words {
            for &topic2 in &words {
                for length in (0..=320).step_by(8) {
                    for fill in [0x00_u8, 0x01, 0x20, 0xff] {
                        let mut data = vec![fill; length];
                        // Offsets that point inside the data, at its end, and past it.
                        if let Some(word) = data.get_mut(..32) {
                            word.fill(0);
                            word[31] = u8::try_from(length % 256).expect("a byte");
                        }
                        let log = Log {
                            topic0: Some(selector),
                            topic1: Some(topic1),
                            topic2: Some(topic2),
                            data: data.into(),
                            ..Log::default()
                        };
                        if let Err(error) = abi.decode_log(&log) {
                            assert!(
                                !error.is_invariant(),
                                "{length} bytes of {fill:#x}: {error}"
                            );
                        }
                        checked += 1;
                    }
                }
            }
        }
        assert!(checked > 1000);
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
        match abi.merge(Abi::from_json(&renamed).expect("renamed")) {
            Err(AbiError::Conflict { selector }) => {
                assert_eq!(selector, keccak256("Transfer(address,address,uint256)"));
            }
            other => panic!("expected a conflict, got {other:?}"),
        }
    }

    /// One file listing an event twice identically keeps it once, as two files do, and
    /// still refuses two definitions under one key.
    #[test]
    fn a_file_may_list_an_identical_event_twice() {
        let event = ERC20_TRANSFER.trim_start_matches('[').trim_end_matches(']');
        let twice = Abi::from_json(&format!("[{event},{event}]")).expect("identical events");
        assert_eq!(twice.events.len(), 1);
        let renamed = event.replace("\"value\"", "\"amount\"");
        assert!(matches!(
            Abi::from_json(&format!("[{event},{renamed}]")),
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
        let names = |args: &[DecodedArg]| -> Vec<String> {
            args.iter().map(|arg| arg.name.clone()).collect()
        };
        assert_eq!(names(&decoded.indexed), ["sender", "recipient"]);
        assert_eq!(
            names(&decoded.body),
            ["amount0", "amount1", "sqrtPriceX96", "liquidity", "tick"]
        );
        assert!(matches!(
            decoded.body[0].value,
            TypedValue::Int { bits: 256, .. }
        ));
    }
}
