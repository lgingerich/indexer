//! A decoded ABI value: a Solidity value with its type kept.
//!
//! Decoding a log is only useful if the result is typed, so [`TypedValue`] is the
//! published value of one decoded argument. [`AbiType`] separately preserves the
//! declared type, even for empty arrays and opaque indexed hashes. The published
//! shape uses plain serde types rather than a decoder's dynamic value model, so it
//! does not move when the decoder changes.
//!
//! # Why widths are carried
//!
//! A `uint8` and a `uint256` hold the same number but they are different columns in
//! a typed table, so [`TypedValue::Uint`] carries its bit width and
//! [`TypedValue::FixedBytes`] its byte size. Dropping either would be irreversible:
//! a width is not recoverable from a value alone.
//!
//! # Wire form
//!
//! Every value is one object tagged by `type`, uniformly, so a consumer never has to
//! guess a shape from context:
//!
//! ```json
//! {"type":"uint","value":"0xde0b6b3a7640000","bits":256}
//! {"type":"address","value":"0x0000000000000000000000000000000000000001"}
//! {"type":"tuple","value":[{"type":"bool","value":true}]}
//! ```
//!
//! `bits` and `size` are plain JSON numbers rather than quantity-encoded, because
//! they are our own metadata and not a chain field — the same treatment
//! [`SCHEMA_VERSION`](crate::wire::envelope::SCHEMA_VERSION) gets.

use alloy_json_abi::{EventParam, Param};
use alloy_primitives::{Address, B256, Bytes, Function, I256, U256};
use serde::{Deserialize, Serialize};

/// A declared ABI type, independent of whether its decoded value has elements.
///
/// Tuple arrays retain their array suffixes in `kind` and describe their tuple
/// components recursively, including component names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbiType {
    /// Canonical Solidity spelling from the ABI, for example `uint256[]` or `tuple[2][]`.
    pub kind: String,
    /// Tuple components in ABI order; empty for non-tuple types.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub components: Vec<AbiComponent>,
}

/// One named component of an ABI tuple.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbiComponent {
    /// The ABI component name, which may be empty.
    pub name: String,
    /// The component's declared type, including any nested tuple components.
    pub abi_type: AbiType,
}

impl From<&EventParam> for AbiType {
    fn from(param: &EventParam) -> Self {
        Self {
            kind: param.ty.clone(),
            components: param.components.iter().map(AbiComponent::from).collect(),
        }
    }
}

impl From<&Param> for AbiType {
    fn from(param: &Param) -> Self {
        Self {
            kind: param.ty.clone(),
            components: param.components.iter().map(AbiComponent::from).collect(),
        }
    }
}

impl From<&Param> for AbiComponent {
    fn from(param: &Param) -> Self {
        Self {
            name: param.name.clone(),
            abi_type: AbiType::from(param),
        }
    }
}

/// One decoded ABI value, tagged by its Solidity type.
///
/// A value is always an object with a `type` tag and a `value`, plus the width or
/// size where the type has one. The variants mirror Solidity's type system rather
/// than the ABI's encoding. The argument's [`AbiType`] supplies array element types
/// and tuple component names that a value alone cannot preserve.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TypedValue {
    /// A `bool`.
    Bool {
        /// The value.
        value: bool,
    },
    /// A signed integer of `bits` width, for example `int256`.
    Int {
        /// The value, as a two's-complement signed integer of `bits` width.
        value: I256,
        /// The declared width in bits, `8..=256` and a multiple of 8.
        bits: u16,
    },
    /// An unsigned integer of `bits` width, for example `uint256`.
    Uint {
        /// The value.
        value: U256,
        /// The declared width in bits, `8..=256` and a multiple of 8.
        bits: u16,
    },
    /// Fixed-length bytes of `size` bytes, for example `bytes4`.
    FixedBytes {
        /// The value, exactly `size` bytes — not padded to a word.
        value: Bytes,
        /// The declared size in bytes, `1..=32`.
        size: u16,
    },
    /// An `address`.
    Address {
        /// The value.
        value: Address,
    },
    /// Dynamically-sized bytes.
    Bytes {
        /// The value.
        value: Bytes,
    },
    /// A Solidity string, whose bytes need not be valid UTF-8.
    String {
        /// The original bytes, preserved losslessly.
        value: Bytes,
        /// The UTF-8 text when the original bytes are valid UTF-8; otherwise `None`.
        text: Option<String>,
    },
    /// An indexed dynamic or compound argument's opaque topic hash, not its value.
    IndexedHash {
        /// The hash carried in the log's topic.
        value: B256,
    },
    /// An external function pointer: a 20-byte address followed by a 4-byte selector.
    Function {
        /// The 24-byte function pointer.
        value: Function,
    },
    /// A dynamically-sized array; its declared element type lives in [`AbiType`].
    Array {
        /// The elements, in order.
        value: Vec<Self>,
    },
    /// A fixed-length array of `size` elements.
    FixedArray {
        /// The elements, in order.
        value: Vec<Self>,
        /// The declared length, which is always the length of `value`.
        size: usize,
    },
    /// A tuple, or a struct, which is a tuple with named components.
    ///
    /// Component names and declared types live in the argument's [`AbiType`];
    /// this value carries the components in the same positional order.
    Tuple {
        /// The components, in order.
        value: Vec<Self>,
    },
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use alloy_json_abi::EventParam;
    use alloy_primitives::{Address, B256, Bytes, Function, I256, U256};

    use crate::wire::datasets::evm::DecodedArg;

    use super::{AbiType, TypedValue};

    fn address(byte: u8) -> Address {
        Address::from([byte; 20])
    }

    #[test]
    fn an_empty_array_preserves_its_recursive_declared_schema() {
        let param: EventParam = serde_json::from_str(
            r#"{"name":"items","type":"tuple[2][]","indexed":false,"components":[
                {"name":"children","type":"tuple[]","components":[
                    {"name":"amount","type":"uint256"}
                ]}
            ]}"#,
        )
        .expect("tuple array ABI parses");
        let argument = DecodedArg {
            position: 3,
            name: param.name.clone(),
            abi_type: AbiType::from(&param),
            value: TypedValue::Array { value: Vec::new() },
        };
        let encoded = serde_json::to_value(&argument).expect("argument serializes");
        assert_eq!(encoded["position"], 3);
        assert_eq!(encoded["abi_type"]["kind"], "tuple[2][]");
        let child = &encoded["abi_type"]["components"][0];
        assert_eq!(child["name"], "children");
        assert_eq!(child["abi_type"]["kind"], "tuple[]");
        let amount = &child["abi_type"]["components"][0];
        assert_eq!(amount["name"], "amount");
        assert_eq!(amount["abi_type"], serde_json::json!({"kind": "uint256"}));
        assert_eq!(encoded["value"]["value"], serde_json::json!([]));
        assert_eq!(
            serde_json::from_value::<DecodedArg>(encoded).expect("argument deserializes"),
            argument
        );
    }

    #[test]
    fn an_indexed_hash_is_distinct_from_decoded_fixed_bytes() {
        let value = TypedValue::IndexedHash {
            value: B256::repeat_byte(0x11),
        };
        assert_eq!(
            serde_json::to_value(&value).expect("hash serializes"),
            serde_json::json!({"type": "indexed_hash", "value": format!("0x{}", "11".repeat(32))})
        );
    }

    #[test]
    fn invalid_utf8_string_bytes_round_trip_without_replacement() {
        let value = TypedValue::String {
            value: Bytes::from_static(&[0xff, 0x00, 0x80]),
            text: None,
        };
        let encoded = serde_json::to_string(&value).expect("string serializes");
        assert_eq!(
            encoded,
            r#"{"type":"string","value":"0xff0080","text":null}"#
        );
        assert_eq!(
            serde_json::from_str::<TypedValue>(&encoded).expect("string deserializes"),
            value
        );
    }

    #[test]
    fn a_function_pointer_keeps_all_twenty_four_bytes() {
        let value = TypedValue::Function {
            value: Function::from([0x22; 24]),
        };
        let encoded = serde_json::to_value(&value).expect("function serializes");
        assert_eq!(
            encoded,
            serde_json::json!({"type": "function", "value": format!("0x{}", "22".repeat(24))})
        );
        assert_eq!(
            serde_json::from_value::<TypedValue>(encoded).expect("function deserializes"),
            value
        );
    }

    /// The round trip a value takes through a sink and back. The derive makes this
    /// pass by construction, so it guards only against someone replacing the derive
    /// with a hand-written `Serialize`/`Deserialize`; the tagging is pinned by the two
    /// format tests below.
    #[test]
    fn every_variant_round_trips_through_json() {
        let variants = [
            TypedValue::Bool { value: true },
            TypedValue::Int {
                value: I256::MINUS_ONE,
                bits: 256,
            },
            TypedValue::Uint {
                value: U256::MAX,
                bits: 8,
            },
            TypedValue::FixedBytes {
                value: Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]),
                size: 4,
            },
            TypedValue::Address {
                value: address(0x11),
            },
            TypedValue::Bytes {
                value: Bytes::from_static(&[0x01, 0x02]),
            },
            TypedValue::String {
                value: Bytes::from_static(b"hello"),
                text: Some("hello".to_owned()),
            },
            TypedValue::IndexedHash {
                value: B256::repeat_byte(0x11),
            },
            TypedValue::Function {
                value: Function::from([0x22; 24]),
            },
            TypedValue::Array {
                value: vec![TypedValue::Bool { value: false }],
            },
            TypedValue::FixedArray {
                value: vec![TypedValue::Uint {
                    value: U256::from(1),
                    bits: 256,
                }],
                size: 1,
            },
            TypedValue::Tuple {
                value: vec![
                    TypedValue::Address {
                        value: address(0x22),
                    },
                    TypedValue::Array {
                        value: vec![TypedValue::String {
                            value: Bytes::from_static(b"nested"),
                            text: Some("nested".to_owned()),
                        }],
                    },
                ],
            },
        ];

        for value in variants {
            let encoded = serde_json::to_string(&value).expect("value serializes");
            let decoded: TypedValue = serde_json::from_str(&encoded)
                .unwrap_or_else(|error| panic!("{encoded} does not round-trip: {error}"));
            assert_eq!(decoded, value, "{encoded} changed across the round trip");
        }
    }

    /// The wire form is one object tagged by type, with the width a column needs and
    /// the value in the encoding the rest of the stream already uses: `0x` hex.
    #[test]
    fn a_value_is_one_tagged_object_with_its_width() {
        let value = TypedValue::Uint {
            value: U256::from(1_000_000_000_000_000_000u64),
            bits: 256,
        };
        assert_eq!(
            serde_json::to_string(&value).expect("value serializes"),
            r#"{"type":"uint","value":"0xde0b6b3a7640000","bits":256}"#
        );
    }

    /// A nested value keeps its own tag, so a consumer reads a shape from the value
    /// rather than inferring it from position.
    #[test]
    fn a_nested_value_carries_its_own_tag() {
        let value = TypedValue::Tuple {
            value: vec![
                TypedValue::Address {
                    value: address(0x01),
                },
                TypedValue::Bool { value: true },
            ],
        };
        let encoded = serde_json::to_string(&value).expect("value serializes");
        assert_eq!(
            encoded,
            concat!(
                r#"{"type":"tuple","value":[{"type":"address","#,
                r#""value":"0x0101010101010101010101010101010101010101"},"#,
                r#"{"type":"bool","value":true}]}"#
            )
        );
    }
}
