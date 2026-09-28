//! The dynamic-ABI conversions: alloy's decode output to the wire's typed value.
//!
//! Decoding a log yields alloy's own value types, which cannot be published
//! directly: they carry no serde impls, and depending on them would tie the wire
//! format to one decoder. So the conversion is explicit, and it is the only place
//! in the crate that knows about alloy's value model.
//!
//! # Fidelity
//!
//! The conversion is total in both directions for every variant a decoded log can
//! produce, and it is lossless in the direction that matters: re-encoding the
//! converted value reproduces the bytes that were decoded. `alloy-dyn-abi`'s
//! `Function` variant is deliberately unsupported — it is a 24-byte address plus
//! selector, vanishingly rare in an event, and widening [`TypedValue`] for it would
//! put a shape on the wire that no consumer would ever read. It is reported as a
//! decode failure instead of being silently mangled.
//!
//! [`TypedValue`]: crate::wire::typed::TypedValue

use crate::wire::typed::TypedValue;
use alloy_dyn_abi::{DynSolType, DynSolValue};
use alloy_primitives::Bytes;
use thiserror::Error;

/// Why a decoded value could not be published.
#[derive(Debug, Error)]
pub enum ConversionError {
    /// The value is a type the wire shape does not carry.
    ///
    /// Only a Solidity `function` type reaches this today; see the module docs.
    #[error("unsupported ABI value: {0}")]
    Unsupported(&'static str),
    /// A declared width or size did not fit the wire field that carries it.
    #[error("{kind} {value} does not fit a u16")]
    WidthOverflow {
        /// Which width or size, for the message.
        kind: &'static str,
        /// The offending value.
        value: usize,
    },
}

/// Converts one decoded value to its published form.
///
/// # Errors
///
/// Returns [`ConversionError::Unsupported`] for a value type the wire shape does not
/// carry, and [`ConversionError::WidthOverflow`] if a declared width or size exceeds
/// a `u16`, which no real ABI can produce.
pub fn value(decoded: &DynSolValue) -> Result<TypedValue, ConversionError> {
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
            // The decoded word is right-padded, so the declared size is what says
            // how much of it is the value.
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
        // A tuple is positional in an event, so there are no component names to
        // carry even when the ABI declares a struct.
        DynSolValue::Tuple(values) => TypedValue::Tuple {
            value: convert_all(values)?,
        },
        DynSolValue::Function(_) => return Err(ConversionError::Unsupported("function type")),
    };
    Ok(typed)
}

/// Converts a sequence of decoded values, so the recursive arms stay one line.
fn convert_all(values: &[DynSolValue]) -> Result<Vec<TypedValue>, ConversionError> {
    values.iter().map(value).collect()
}

/// Narrows a decoder-supplied `usize` to the `u16` the wire shape carries.
fn width(kind: &'static str, value: usize) -> Result<u16, ConversionError> {
    u16::try_from(value).map_err(|_| ConversionError::WidthOverflow { kind, value })
}

/// Converts a decoded value back to alloy's form, which is what lets a round trip be
/// checked against the decoder rather than against a copy of its logic.
///
/// # Errors
///
/// Returns [`ConversionError::Unsupported`] when the wire value's declared width is
/// not one alloy can represent, which cannot happen for a value this crate produced.
pub fn dyn_value(typed: &TypedValue) -> Result<DynSolValue, ConversionError> {
    let converted = match typed {
        TypedValue::Bool { value } => DynSolValue::Bool(*value),
        TypedValue::Int { value, bits } => DynSolValue::Int(*value, usize::from(*bits)),
        TypedValue::Uint { value, bits } => DynSolValue::Uint(*value, usize::from(*bits)),
        TypedValue::FixedBytes { value, size } => {
            let mut word = alloy_primitives::B256::ZERO;
            let size = usize::from(*size);
            if value.len() != size || size > word.len() {
                return Err(ConversionError::Unsupported("fixed bytes size"));
            }
            word.get_mut(..size)
                .ok_or(ConversionError::Unsupported("fixed bytes size"))?
                .copy_from_slice(value);
            DynSolValue::FixedBytes(word, size)
        }
        TypedValue::Address { value } => DynSolValue::Address(*value),
        TypedValue::Bytes { value } => DynSolValue::Bytes(value.to_vec()),
        TypedValue::String { value } => DynSolValue::String(value.clone()),
        TypedValue::Array { value } => DynSolValue::Array(convert_dyn_all(value)?),
        TypedValue::FixedArray { value, .. } => DynSolValue::FixedArray(convert_dyn_all(value)?),
        TypedValue::Tuple { value } => DynSolValue::Tuple(convert_dyn_all(value)?),
    };
    Ok(converted)
}

/// Converts a sequence of published values back to alloy's form, so the recursive
/// arms stay one line.
fn convert_dyn_all(values: &[TypedValue]) -> Result<Vec<DynSolValue>, ConversionError> {
    values.iter().map(dyn_value).collect()
}

/// Rebuilds the Solidity type of a published value, so a converted value can be
/// matched against the ABI type it is supposed to have.
///
/// # Errors
///
/// Returns [`ConversionError::Unsupported`] when the declared width or size is not
/// one the type can hold.
pub fn dyn_type(typed: &TypedValue) -> Result<DynSolType, ConversionError> {
    let converted = match typed {
        TypedValue::Bool { .. } => DynSolType::Bool,
        TypedValue::Int { bits, .. } => DynSolType::Int(usize::from(*bits)),
        TypedValue::Uint { bits, .. } => DynSolType::Uint(usize::from(*bits)),
        TypedValue::FixedBytes { size, .. } => DynSolType::FixedBytes(usize::from(*size)),
        TypedValue::Address { .. } => DynSolType::Address,
        TypedValue::Bytes { .. } => DynSolType::Bytes,
        TypedValue::String { .. } => DynSolType::String,
        // An empty sequence has no element type to read, and the ABI cannot express
        // one either, so the placeholder is never observable.
        TypedValue::Array { value } => DynSolType::Array(Box::new(
            value.first().map_or(Ok(DynSolType::Bool), dyn_type)?,
        )),
        TypedValue::FixedArray { value, size } => {
            if value.len() != usize::from(*size) {
                return Err(ConversionError::Unsupported("fixed array length"));
            }
            DynSolType::FixedArray(
                Box::new(value.first().map_or(Ok(DynSolType::Bool), dyn_type)?),
                usize::from(*size),
            )
        }
        TypedValue::Tuple { value } => {
            DynSolType::Tuple(value.iter().map(dyn_type).collect::<Result<Vec<_>, _>>()?)
        }
    };
    Ok(converted)
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use alloy_dyn_abi::{DynSolType, DynSolValue};
    use alloy_primitives::{Address, B256, I256, U256};

    use super::{ConversionError, dyn_type, dyn_value, value};
    use crate::wire::typed::TypedValue;

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
        let typed = value(decoded).expect("value converts");
        // The conversion must be faithful: alloy can rebuild the same value from it.
        assert_eq!(dyn_value(&typed).expect("value converts back"), *decoded);
        typed
    }

    /// A nested value keeps its width at every level, because a width is not
    /// recoverable from a value and losing it would collapse a `uint8` and a
    /// `uint256` into the same column.
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

        let typed = round_trip(&decoded);

        assert_eq!(
            typed,
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

    /// The rebuilt type is the type the value was decoded as, so a converted value
    /// still matches its ABI type. This is what lets a store trust the width.
    #[test]
    fn a_converted_value_still_matches_its_declared_type() {
        let cases = [
            (DynSolType::Bool, DynSolValue::Bool(true)),
            (DynSolType::Uint(8), DynSolValue::Uint(U256::from(7), 8)),
            (
                DynSolType::FixedBytes(4),
                DynSolValue::FixedBytes(padded(&[0xaa; 4]), 4),
            ),
            (
                DynSolType::Address,
                DynSolValue::Address(Address::from([0x11; 20])),
            ),
            (DynSolType::String, DynSolValue::String("hi".to_owned())),
            (
                DynSolType::Tuple(vec![DynSolType::Bool, DynSolType::Uint(256)]),
                DynSolValue::Tuple(vec![
                    DynSolValue::Bool(false),
                    DynSolValue::Uint(U256::MAX, 256),
                ]),
            ),
            (
                DynSolType::Array(Box::new(DynSolType::Uint(16))),
                DynSolValue::Array(vec![
                    DynSolValue::Uint(U256::from(1), 16),
                    DynSolValue::Uint(U256::from(2), 16),
                ]),
            ),
        ];

        for (dyn_type_expected, decoded) in cases {
            let typed = round_trip(&decoded);
            let rebuilt = dyn_type(&typed).expect("type rebuilds");
            assert_eq!(
                rebuilt.to_string(),
                dyn_type_expected.to_string(),
                "{typed:?} rebuilt as the wrong type"
            );
            assert!(
                dyn_type_expected.matches(&decoded),
                "{decoded:?} does not match {dyn_type_expected}"
            );
        }
    }

    /// `FixedBytes` carries only its declared bytes, not the zero padding to a word,
    /// so a `bytes4` is four bytes on the wire and not thirty-two. The value round-trips
    /// anyway, because re-encoding pads it back.
    #[test]
    fn fixed_bytes_carries_only_its_declared_size() {
        let decoded = DynSolValue::FixedBytes(padded(&[0xab; 4]), 4);
        let typed = round_trip(&decoded);
        let TypedValue::FixedBytes { value, size } = typed else {
            panic!("a fixed-bytes value must convert to fixed bytes");
        };
        assert_eq!(size, 4);
        assert_eq!(value.as_ref(), &[0xab; 4]);
    }

    /// A Solidity `function` is the one type the wire shape does not carry, so it
    /// fails loudly rather than being coerced into something it is not.
    #[test]
    fn a_function_type_is_refused_rather_than_mangled() {
        let decoded = DynSolValue::Function(alloy_primitives::Function::ZERO);
        assert!(matches!(
            value(&decoded),
            Err(ConversionError::Unsupported("function type"))
        ));
    }
}
