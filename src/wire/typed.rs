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
