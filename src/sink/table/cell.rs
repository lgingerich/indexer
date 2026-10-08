//! A row's values, and how each Rust field type becomes one.

use alloy_primitives::{Address, Bloom, Bytes, FixedBytes, U256};
use alloy_rpc_types_eth::AccessList;

use super::ColumnType;

/// One value in a row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// Absent: the chain has no such field for this record. Never a stand-in for zero or
    /// empty, which are values the chain does have.
    Null,
    /// A 64-bit unsigned integer.
    Uint(u64),
    /// A 64-bit signed integer.
    Int(i64),
    /// An integer up to 256 bits, signed or not, held exactly.
    BigInt {
        /// Whether the value is below zero.
        negative: bool,
        /// The absolute value.
        magnitude: U256,
    },
    /// Text: lowercase `0x` hex for hashes, addresses, and bytes.
    Text(String),
    /// A boolean.
    Bool(bool),
    /// Seconds since the Unix epoch, UTC.
    Timestamp(u64),
    /// A list of scalars, each in its column's element type.
    List(Vec<Self>),
    /// JSON.
    Document(String),
}

impl Value {
    /// An unsigned integer of any width up to 256 bits.
    #[must_use]
    pub const fn unsigned(magnitude: U256) -> Self {
        Self::BigInt {
            negative: false,
            magnitude,
        }
    }

    /// Any serializable value as a JSON document.
    ///
    /// # Errors
    ///
    /// Returns `serde_json`'s error when `value` does not serialize, rather than a
    /// fallback document that would claim a value the row does not have.
    pub fn json<T: serde::Serialize + ?Sized>(value: &T) -> Result<Self, serde_json::Error> {
        serde_json::to_string(value).map(Self::Document)
    }
}

/// A Rust field type a table column can hold: its column type, whether it can be absent,
/// and its value.
///
/// Implemented once per field type, so a table declared from fields cannot type a column
/// one way and fill it another, or let a field that can be absent into a `NOT NULL`
/// column.
pub trait Cell {
    /// The column type.
    const TYPE: ColumnType;
    /// Whether the value can be absent.
    const NULLABLE: bool = false;
    /// The value.
    ///
    /// # Errors
    ///
    /// Returns `serde_json`'s error when a document does not render; every other value
    /// cannot fail.
    fn value(&self) -> Result<Value, serde_json::Error>;
}

impl Cell for u64 {
    const TYPE: ColumnType = ColumnType::Uint;
    fn value(&self) -> Result<Value, serde_json::Error> {
        Ok(Value::Uint(*self))
    }
}

impl Cell for u8 {
    const TYPE: ColumnType = ColumnType::Uint;
    fn value(&self) -> Result<Value, serde_json::Error> {
        Ok(Value::Uint(u64::from(*self)))
    }
}

impl Cell for bool {
    const TYPE: ColumnType = ColumnType::Bool;
    fn value(&self) -> Result<Value, serde_json::Error> {
        Ok(Value::Bool(*self))
    }
}

/// A wei amount or price: 128 bits, wider than a `u64` column holds. `u64::MAX` wei is
/// 18.4 ETH, which a gas price spike can pass.
impl Cell for u128 {
    const TYPE: ColumnType = ColumnType::BigInt;
    fn value(&self) -> Result<Value, serde_json::Error> {
        Ok(Value::unsigned(U256::from(*self)))
    }
}

impl Cell for U256 {
    const TYPE: ColumnType = ColumnType::BigInt;
    fn value(&self) -> Result<Value, serde_json::Error> {
        Ok(Value::unsigned(*self))
    }
}

impl<const N: usize> Cell for FixedBytes<N> {
    const TYPE: ColumnType = ColumnType::Text;
    fn value(&self) -> Result<Value, serde_json::Error> {
        Ok(Value::Text(format!("{self:#x}")))
    }
}

impl Cell for Address {
    const TYPE: ColumnType = ColumnType::Text;
    fn value(&self) -> Result<Value, serde_json::Error> {
        Ok(Value::Text(format!("{self:#x}")))
    }
}

impl Cell for Bloom {
    const TYPE: ColumnType = ColumnType::Text;
    fn value(&self) -> Result<Value, serde_json::Error> {
        Ok(Value::Text(format!("{self:#x}")))
    }
}

impl Cell for Bytes {
    const TYPE: ColumnType = ColumnType::Text;
    fn value(&self) -> Result<Value, serde_json::Error> {
        Ok(Value::Text(format!("{self:#x}")))
    }
}

impl Cell for String {
    const TYPE: ColumnType = ColumnType::Text;
    fn value(&self) -> Result<Value, serde_json::Error> {
        Ok(Value::Text(self.clone()))
    }
}

/// A list of hashes or records, as a JSON array.
impl<T: serde::Serialize> Cell for Vec<T> {
    const TYPE: ColumnType = ColumnType::Document;
    fn value(&self) -> Result<Value, serde_json::Error> {
        Value::json(self)
    }
}

impl Cell for AccessList {
    const TYPE: ColumnType = ColumnType::Document;
    fn value(&self) -> Result<Value, serde_json::Error> {
        Value::json(self)
    }
}

impl<T: Cell> Cell for Option<T> {
    const TYPE: ColumnType = T::TYPE;
    const NULLABLE: bool = true;
    fn value(&self) -> Result<Value, serde_json::Error> {
        self.as_ref().map_or(Ok(Value::Null), Cell::value)
    }
}

/// Seconds since the Unix epoch, stored as a timestamp rather than a number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timestamp(pub u64);

impl Cell for Timestamp {
    const TYPE: ColumnType = ColumnType::Timestamp;
    fn value(&self) -> Result<Value, serde_json::Error> {
        Ok(Value::Timestamp(self.0))
    }
}
