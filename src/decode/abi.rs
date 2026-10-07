//! Prepared event ABIs and schema-aware value conversion.

use std::collections::BTreeMap;

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
    name: String,
    signature: String,
    layout: DynSolEvent,
    indexed: Vec<Argument>,
    body: Vec<Argument>,
}

/// Immutable ABI, validated and prepared once before processing logs.
#[derive(Debug, Clone)]
pub struct Abi {
    id: B256,
    events: BTreeMap<B256, PreparedEvent>,
}

impl Abi {
    /// Parses and prepares all events. Anonymous events are explicitly unsupported.
    ///
    /// # Errors
    /// Returns a typed parse/layout error or rejects anonymous and ambiguous events.
    pub fn from_json(json: &str) -> Result<Self, AbiError> {
        let abi: JsonAbi = serde_json::from_str(json)?;
        let mut events = BTreeMap::new();
        for (event_index, event) in abi.events().enumerate() {
            if event.anonymous {
                return Err(AbiError::Anonymous {
                    event: event.name.clone(),
                });
            }
            let selector = event.selector();
            let prepared = PreparedEvent::new(event).map_err(|source| AbiError::Layout {
                event_index,
                source,
            })?;
            if events.insert(selector, prepared).is_some() {
                return Err(AbiError::DuplicateSelector { selector });
            }
        }
        // Content identity intentionally includes the complete parsed ABI, not its file name.
        let id = keccak256(serde_json::to_vec(&abi)?);
        Ok(Self { id, events })
    }

    /// Content identity of the parsed ABI.
    #[must_use]
    pub const fn id(&self) -> B256 {
        self.id
    }

    /// Finds a declared event by canonical signature.
    #[must_use]
    pub fn selector(&self, signature: &str) -> Option<B256> {
        self.events
            .iter()
            .find_map(|(selector, event)| (event.signature == signature).then_some(*selector))
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
        let Some(event) = self.events.get(selector) else {
            return Ok(None);
        };
        let decoded = event
            .layout
            .decode_log_parts(topics.iter().copied(), &log.data)
            .map_err(DecodeError::Log)?;
        Ok(Some(DecodedEvent {
            name: event.name.clone(),
            selector: *selector,
            signature: event.signature.clone(),
            indexed: arguments(&event.indexed, &decoded.indexed, true)?,
            body: arguments(&event.body, &decoded.body, false)?,
        }))
    }
}

impl PreparedEvent {
    fn new(event: &Event) -> Result<Self, alloy_dyn_abi::Error> {
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
            text: std::str::from_utf8(value).ok().map(str::to_owned),
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
    /// Two layouts claim the same event selector.
    #[error("multiple event layouts for selector {selector}")]
    DuplicateSelector {
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
        let selector = *abi.events.keys().next().expect("event");
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
    fn invalid_utf8_and_nested_strings_remain_bytes() {
        let result = decode(
            r#"[{"type":"event","name":"Text","anonymous":false,"inputs":[{"name":"s","type":"string[]","indexed":false}]}]"#,
            &[DynSolValue::Array(vec![
                DynSolValue::Bytes(vec![0xff]),
                DynSolValue::Bytes(b"hello".to_vec()),
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
    fn startup_rejects_anonymous_and_impossible_topic_layouts() {
        assert!(matches!(
            Abi::from_json(r#"[{"type":"event","name":"Hidden","anonymous":true,"inputs":[]}]"#),
            Err(AbiError::Anonymous { .. })
        ));
        assert!(matches!(
            Abi::from_json(
                r#"[{"type":"event","name":"TooMany","anonymous":false,"inputs":[{"name":"a","type":"uint256","indexed":true},{"name":"b","type":"uint256","indexed":true},{"name":"c","type":"uint256","indexed":true},{"name":"d","type":"uint256","indexed":true}]}]"#
            ),
            Err(AbiError::Layout { .. })
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

    #[test]
    fn actual_swap_fixture_decodes() {
        let abi =
            Abi::from_json(include_str!("../../abis/uniswap/v3/pool.json")).expect("pool ABI");
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
