//! Prepared event ABIs and schema-aware value conversion.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use alloy_dyn_abi::{DynSolEvent, DynSolType, DynSolValue, Specifier as _};
use alloy_json_abi::{Event, JsonAbi};
use alloy_primitives::{B256, Bytes, keccak256};
use thiserror::Error;

use crate::wire::datasets::evm::{DecodedArg, Log};
use crate::wire::typed::{AbiType, TypedValue};

/// A decoded event without chain or transaction metadata.
#[derive(Debug)]
pub(crate) struct DecodedEvent {
    /// Identity of the event definition used: see [`Abi`].
    pub(crate) id: B256,
    /// ABI event name.
    pub(crate) name: String,
    /// The event's key in its [`Abi`]: its selector and topic count.
    pub(crate) key: EventKey,
    /// Canonical event signature.
    pub(crate) signature: String,
    /// Indexed arguments with their original positions and declared schemas.
    pub(crate) indexed: Vec<DecodedArg>,
    /// Non-indexed arguments with their original positions and declared schemas.
    pub(crate) body: Vec<DecodedArg>,
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
pub(crate) struct Abi {
    events: BTreeMap<EventKey, PreparedEvent>,
}

impl Abi {
    /// Parses and prepares all events. Anonymous events are explicitly unsupported. An
    /// event the file lists twice identically is kept once, as across files.
    ///
    /// # Errors
    /// Returns a typed parse/layout error or rejects anonymous and conflicting events.
    pub(crate) fn from_json(json: &str) -> Result<Self, AbiError> {
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
    pub(crate) fn merge(&mut self, other: Self) -> Result<(), AbiError> {
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
        self.events.values().map(|event| EventSpec {
            id: event.id,
            name: &event.name,
            inputs: event
                .inputs()
                .map(|(argument, indexed)| InputSpec {
                    name: &argument.name,
                    ty: &argument.ty,
                    indexed,
                })
                .collect(),
        })
    }

    /// The keys of every event with the canonical `signature`, for example
    /// `Swap(address,address,uint256)`: more than one when the signature is declared with
    /// different arguments indexed.
    pub(crate) fn keys(&self, signature: &str) -> Vec<EventKey> {
        self.events
            .iter()
            .filter(|(_, event)| event.signature == signature)
            .map(|(key, _)| *key)
            .collect()
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
        let (found, indexed) = event
            .inputs()
            .find(|(argument, _)| argument.name == param && argument.ty == DynSolType::Address)?;
        let before = |argument: &&Argument| argument.position < found.position;
        if indexed {
            let topic = 1 + event.indexed.iter().filter(before).count();
            return Some(AddressSlot::Topic(topic));
        }
        let offset = event
            .body
            .iter()
            .filter(before)
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
    pub(crate) fn decode_log(&self, log: &Log) -> Result<Option<DecodedEvent>, DecodeError> {
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
        let key = (*selector, topics.len());
        let Some(event) = self.events.get(&key) else {
            return Ok(None);
        };
        let decoded = event
            .layout
            .decode_log_parts(topics.iter().copied(), &log.data)
            .map_err(DecodeError::Log)?;
        Ok(Some(DecodedEvent {
            id: event.id,
            name: event.name.clone(),
            key,
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

    /// Input `position` among all the event's inputs, and whether it is indexed.
    fn argument(&self, position: usize) -> Option<(&Argument, bool)> {
        let at = |argument: &&Argument| argument.position == position;
        self.indexed
            .iter()
            .find(at)
            .map(|argument| (argument, true))
            .or_else(|| self.body.iter().find(at).map(|argument| (argument, false)))
    }

    /// Every input in declaration order, and whether it is indexed.
    fn inputs(&self) -> impl Iterator<Item = (&Argument, bool)> {
        (0..self.indexed.len() + self.body.len()).filter_map(|position| self.argument(position))
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

/// Whether an indexed argument of type `ty` is only its topic hash: a string, bytes, array,
/// or tuple, whose value an event cannot fit in one topic. Decoding and the event table's
/// column follow this one rule.
pub(crate) const fn topic_hashed(ty: &DynSolType) -> bool {
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
            let value = if indexed && topic_hashed(&param.ty) {
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
mod tests;
