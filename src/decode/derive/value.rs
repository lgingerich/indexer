//! Reading a record's arguments and the words they pack, and typing what a row stores.
//!
//! An argument is read by position and name and refused as [`DeriveError::Argument`]
//! when it is missing, misnamed, or of another type. A packed word is read in bit fields,
//! the least significant bit being bit 0, as Solidity's shifts and masks number them.

use alloy_primitives::{Address, I256, U256};

use crate::wire::envelope::{Decoded, TypedValue};

use super::DeriveError;

/// Argument `position` of `decoded`, among all its inputs, when it is named `name`.
fn argument<'a>(decoded: &'a Decoded, position: usize, name: &str) -> Option<&'a TypedValue> {
    decoded
        .argument(position)
        .filter(|argument| argument.name == name)
        .map(|argument| &argument.value)
}

/// Argument `position` of `decoded`, an `address` named `name`.
pub(super) fn address(
    decoded: &Decoded,
    position: usize,
    name: &'static str,
) -> Result<Address, DeriveError> {
    match argument(decoded, position, name) {
        Some(TypedValue::Address { value }) => Ok(*value),
        _ => Err(element(position, name, "an address")),
    }
}

/// Argument `position` of `decoded`, an `int{bits}` named `name`.
pub(super) fn int(
    decoded: &Decoded,
    position: usize,
    name: &'static str,
    bits: u16,
) -> Result<I256, DeriveError> {
    match argument(decoded, position, name) {
        Some(TypedValue::Int { value, bits: found }) if *found == bits => Ok(*value),
        _ => Err(element(
            position,
            name,
            "a signed integer of its declared width",
        )),
    }
}

/// Argument `position` of `decoded`, a `uint{bits}` named `name`.
pub(super) fn uint(
    decoded: &Decoded,
    position: usize,
    name: &'static str,
    bits: u16,
) -> Result<U256, DeriveError> {
    match argument(decoded, position, name) {
        Some(TypedValue::Uint { value, bits: found }) if *found == bits => Ok(*value),
        _ => Err(element(
            position,
            name,
            "an unsigned integer of its declared width",
        )),
    }
}

/// Argument `position` of `decoded`, a dynamic array named `name`.
pub(super) fn array<'a>(
    decoded: &'a Decoded,
    position: usize,
    name: &'static str,
) -> Result<&'a [TypedValue], DeriveError> {
    match argument(decoded, position, name) {
        Some(TypedValue::Array { value }) => Ok(value),
        _ => Err(element(position, name, "an array")),
    }
}

/// The error for argument `position`, `name`, not holding what the derivation reads.
pub(super) const fn element(
    position: usize,
    name: &'static str,
    expected: &'static str,
) -> DeriveError {
    DeriveError::Argument {
        position,
        name,
        expected,
    }
}

/// A signed value `bits` wide.
pub(super) const fn signed(value: I256, bits: u16) -> TypedValue {
    TypedValue::Int { value, bits }
}

/// An unsigned value `bits` wide.
pub(super) const fn unsigned(value: U256, bits: u16) -> TypedValue {
    TypedValue::Uint { value, bits }
}

/// `len` bits of `word` from bit `low`.
pub(super) fn bits(word: U256, low: usize, len: usize) -> U256 {
    (word >> low) & (U256::MAX >> (256 - len))
}

/// `len` bits of `word` from bit `low`, read as a two's-complement signed integer.
pub(super) fn signed_bits(word: U256, low: usize, len: usize) -> I256 {
    let value = bits(word, low, len);
    if value.bit(len - 1) {
        I256::from_raw(value | (U256::MAX << len))
    } else {
        I256::from_raw(value)
    }
}
