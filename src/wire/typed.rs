//! A decoded ABI value: a Solidity value with its type kept.
//!
//! Decoding a log is only useful if the result is typed, so [`TypedValue`] is the
//! published form of one decoded argument: the value plus the ABI type that gives it
//! meaning. It is structurally the same information a decoder's own value carries,
//! but expressed here in plain serde terms rather than depending on that decoder's
//! type — so the published shape does not move when the decoder changes.
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
//! [`Envelope::sequence`](crate::wire::envelope::Envelope::sequence) gets.

use alloy_primitives::{Address, Bytes, I256, U256};
use serde::{Deserialize, Serialize};

/// One decoded ABI value, tagged by its Solidity type.
///
/// A value is always an object with a `type` tag and a `value`, plus the width or
/// size where the type has one. The variants mirror Solidity's type system rather
/// than the ABI's encoding, so a consumer can rebuild a typed column from a value
/// without reading the ABI itself.
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
    /// A UTF-8 string. Solidity does not enforce the encoding, so a value that is
    /// not valid UTF-8 does not survive decoding as one.
    String {
        /// The value.
        value: String,
    },
    /// A dynamically-sized array.
    Array {
        /// The elements, in order.
        value: Vec<Self>,
    },
    /// A fixed-length array of `size` elements.
    FixedArray {
        /// The elements, in order.
        value: Vec<Self>,
        /// The declared length, which is always the length of `value`.
        size: u16,
    },
    /// A tuple, or a struct, which is a tuple with named components.
    ///
    /// Component names are not carried: an ABI event's tuple components are
    /// positional, and the named form only arises from EIP-712 typed data, which
    /// this pipeline does not decode.
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
    use alloy_primitives::{Address, Bytes, I256, U256};

    use super::TypedValue;

    fn address(byte: u8) -> Address {
        Address::from([byte; 20])
    }

    /// Every variant must survive the round trip an event takes: serialized, then
    /// read back by a consumer.
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
                value: "hello".to_owned(),
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
                            value: "nested".to_owned(),
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
