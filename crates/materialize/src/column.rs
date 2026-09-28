//! A typed column value, ready to bind into a database row.
//!
//! The decode stage publishes [`TypedValue`], which keeps every argument's Solidity
//! type. A database column needs something narrower: a number a `BIGINT` can hold, a
//! string, or an exact decimal for the widths no integer column can carry.
//!
//! # Why the widths matter here
//!
//! An ABI integer can be 256 bits, and no common database integer column holds that.
//! A `uint160` price and a `uint256` amount both overflow a signed 64-bit column, so
//! the choice is not stylistic:
//!
//! - A value that fits `i64` becomes [`Value::Integer`], which sorts and aggregates
//!   as a number.
//! - Anything wider becomes [`Value::Decimal`], an exact base-10 string. Not a
//!   float: a `uint256` wei amount has 78 digits, and rounding it would silently
//!   corrupt a balance. This is the same reason Allium exposes `_str` variants of its
//!   numeric columns "to retain precision".
//!
//! A `bytes` or `address` becomes text in its canonical `0x` form, since that is what
//! a consumer compares and joins on.

use alloy_primitives::{Address, I256, U256};
use wire::typed::TypedValue;

/// One decoded argument, ready to bind into a column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// The ABI parameter name, for example `amount0`.
    pub name: String,
    /// The value, narrowed to something a column can hold.
    pub value: Value,
}

/// A column value, in the three shapes a store actually has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// A value that fits a signed 64-bit integer column.
    Integer(i64),
    /// A value too wide for an integer column, as exact base-10 text.
    ///
    /// Only ever produced for a value that does not fit [`Value::Integer`], so a
    /// reader can treat the two as one numeric column with a fallback rather than
    /// guessing which widths arrive which way.
    Decimal(String),
    /// A boolean.
    Bool(bool),
    /// Text: an address or `bytes` in canonical `0x` form, or a Solidity `string`.
    Text(String),
}

impl Column {
    /// Converts one decoded argument to a column.
    ///
    /// A composite value — an array or a tuple — becomes its JSON form as
    /// [`Value::Text`], because a column cannot hold a shape. It is the one lossy
    /// case: the type is still recoverable from the JSON, but a consumer cannot index
    /// into it. A first-class column for a nested shape would need a store that
    /// supports one.
    #[must_use]
    pub fn from_arg(name: &str, value: &TypedValue) -> Self {
        Self {
            name: name.to_owned(),
            value: Value::from_typed(value),
        }
    }
}

impl Value {
    /// Narrows one typed value to a column value.
    #[must_use]
    pub fn from_typed(value: &TypedValue) -> Self {
        match value {
            TypedValue::Bool { value } => Self::Bool(*value),
            TypedValue::Uint { value, .. } => Self::number(value),
            TypedValue::Int { value, .. } => Self::signed(value),
            TypedValue::Address { value } => Self::Text(value.to_string()),
            TypedValue::FixedBytes { value, .. } | TypedValue::Bytes { value } => {
                Self::Text(format!("0x{}", alloy_primitives::hex::encode(value)))
            }
            TypedValue::String { value } => Self::Text(value.clone()),
            // A composite has no column shape; its JSON keeps the types and the
            // nesting, so nothing is invented and nothing is dropped.
            TypedValue::Array { .. } | TypedValue::FixedArray { .. } | TypedValue::Tuple { .. } => {
                Self::Text(serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned()))
            }
        }
    }

    /// A `uint` as an integer when it fits one, and exact decimal text when it does
    /// not.
    fn number(value: &U256) -> Self {
        if let Ok(narrowed) = u64::try_from(*value) {
            // `u64` does not fit `i64` at the top end, so the widest `uint64` still
            // lands in text rather than wrapping to a negative number.
            if let Ok(signed) = i64::try_from(narrowed) {
                return Self::Integer(signed);
            }
        }
        Self::Decimal(value.to_string())
    }

    /// An `int` as an integer when it fits one, and exact decimal text when it does
    /// not. A `int256` that fits `i64` stays a number, sign intact.
    fn signed(value: &I256) -> Self {
        match i64::try_from(*value) {
            Ok(narrowed) => Self::Integer(narrowed),
            Err(_) => Self::Decimal(value.to_string()),
        }
    }
}

/// The addresses a decoded record is attributed to, resolved from the raw log.
///
/// A decoded event knows its contract and its arguments, but not who sent the
/// transaction. A store that wants to answer "which swaps did this address make"
/// needs the sender, and reaching it otherwise means joining back to the raw
/// transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Attribution {
    /// The contract that emitted the log.
    pub contract: Address,
    /// The transaction's sender.
    pub transaction_from: Address,
    /// The transaction's recipient.
    pub transaction_to: Option<Address>,
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{Address, B256, I256, U256};

    use super::{Column, Value};
    use wire::typed::TypedValue;

    /// The whole point of the narrowing: a value that fits an integer column is one,
    /// and a value that does not is exact text rather than a rounded float.
    #[test]
    fn a_number_is_an_integer_only_when_it_fits_one() {
        let fits = TypedValue::Uint {
            value: U256::from(8_586_564),
            bits: 256,
        };
        assert_eq!(Value::from_typed(&fits), Value::Integer(8_586_564));

        // A `uint160` price: 2.4e47, which no 64-bit column holds. Built from a
        // decimal string because the literal does not fit a `u128` either.
        let wide = TypedValue::Uint {
            value: "246000000000000000000000000000000000000000000000"
                .parse::<U256>()
                .expect("a valid uint160"),
            bits: 160,
        };
        let Value::Decimal(text) = Value::from_typed(&wide) else {
            panic!("a uint160 must not be narrowed into an integer column");
        };
        assert_eq!(text, "246000000000000000000000000000000000000000000000");
    }

    /// The widest `uint64` does not fit a signed column, so it must not wrap into a
    /// negative number.
    #[test]
    fn a_uint64_at_the_top_of_its_range_does_not_wrap_negative() {
        let value = TypedValue::Uint {
            value: U256::from(u64::MAX),
            bits: 64,
        };
        let Value::Decimal(text) = Value::from_typed(&value) else {
            panic!("u64::MAX exceeds i64 and must be exact text");
        };
        assert_eq!(text, u64::MAX.to_string());
    }

    /// A signed value keeps its sign, which is the difference between a swap that
    /// sold token0 and one that bought it.
    #[test]
    fn a_signed_value_keeps_its_sign() {
        let value = TypedValue::Int {
            value: I256::try_from(-3_180_585_820_646_654_i64).expect("fits"),
            bits: 256,
        };
        assert_eq!(
            Value::from_typed(&value),
            Value::Integer(-3_180_585_820_646_654)
        );

        // Beyond `i64`, still negative, still exact.
        let wide = TypedValue::Int {
            value: I256::MIN,
            bits: 256,
        };
        assert_eq!(
            Value::from_typed(&wide),
            Value::Decimal(
                "-57896044618658097711785492504343953926634992332820282019728792003956564819968"
                    .to_owned()
            )
        );
    }

    /// An address and `bytes` become canonical `0x` text, and a `bytes4` is four
    /// bytes rather than the whole padded word.
    #[test]
    fn byte_values_are_canonical_hex_text() {
        let address = TypedValue::Address {
            value: Address::from([0x11; 20]),
        };
        assert_eq!(
            Value::from_typed(&address),
            Value::Text("0x1111111111111111111111111111111111111111".to_owned())
        );

        let selector = TypedValue::FixedBytes {
            value: alloy_primitives::Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]),
            size: 4,
        };
        assert_eq!(
            Value::from_typed(&selector),
            Value::Text("0xdeadbeef".to_owned())
        );
    }

    /// A composite keeps its types in JSON rather than being flattened into
    /// something it is not.
    #[test]
    fn a_composite_becomes_json_text() {
        let tuple = TypedValue::Tuple {
            value: vec![TypedValue::Bool { value: true }],
        };
        let Value::Text(json) = Value::from_typed(&tuple) else {
            panic!("a tuple has no column shape");
        };
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("json parses");
        assert_eq!(parsed["type"], "tuple");
    }

    /// A column carries the ABI name, which is what makes it addressable.
    #[test]
    fn a_column_keeps_its_name() {
        let value = TypedValue::Uint {
            value: U256::from(1),
            bits: 256,
        };
        let column = Column::from_arg("amount0", &value);
        assert_eq!(column.name, "amount0");
        assert_eq!(column.value, Value::Integer(1));
    }

    /// The identity fields a consumer joins on survive as text.
    #[test]
    fn identity_fields_are_text() {
        let hash = TypedValue::FixedBytes {
            value: alloy_primitives::Bytes::from(B256::from([0xab; 32]).to_vec()),
            size: 32,
        };
        assert_eq!(
            Value::from_typed(&hash),
            Value::Text(format!("0x{}", "ab".repeat(32)))
        );
    }
}
