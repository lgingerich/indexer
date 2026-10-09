//! A table's Delta schema, and its rows as an Arrow batch.
//!
//! The Delta schema is derived from the [`TableDef`] every store shares, and the Arrow
//! schema from the Delta one, so a batch is built against exactly the types the table was
//! created with: a list's element field is named the way Delta names it, and a timestamp
//! carries the zone Delta gives it.
//!
//! | [`ColumnType`] | Delta | Why |
//! | --- | --- | --- |
//! | `Uint` | `decimal(20,0)` | Delta has no unsigned integers, and a decoded `uint64` can pass `i64::MAX`. Exact, as `NUMERIC(20,0)` is in Postgres. |
//! | `Int` | `long` | |
//! | `BigInt` | `string`, decimal text | Delta decimals stop at 38 digits, and a `uint256` needs 78. Exact, and every reader can cast it. |
//! | `Text`, `Document` | `string` | `0x` hex and JSON, as the SQL stores hold them. |
//! | `Bool` | `boolean` | |
//! | `Timestamp` | `timestamp` | Microseconds in UTC, Delta's only zoned timestamp. |
//! | `List` | `array` | Of the element's type. |
//!
//! A table whose rows belong to a block also gets a [`PARTITION`] column: the block
//! height divided by [`PARTITION_BLOCKS`]. It is what a reorg's delete is bounded by.

use std::sync::Arc;

use alloy_primitives::U256;
use deltalake::arrow::array::{
    ArrayRef, BooleanArray, Decimal128Array, Int64Array, ListArray, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use deltalake::arrow::buffer::{NullBuffer, OffsetBuffer};
use deltalake::arrow::datatypes::{DataType as ArrowType, Field, Schema as ArrowSchema};
use deltalake::arrow::error::ArrowError;
use deltalake::kernel::engine::arrow_conversion::TryIntoArrow as _;
use deltalake::kernel::{ArrayType, DataType, StructField, StructType};

use crate::sink::table::{ColumnType, Row, TableDef, Value};

/// The partition column of every table whose rows belong to a block.
pub(super) const PARTITION: &str = "block_range";

/// How many block heights one partition covers: about 2.3 days of Base, two weeks of
/// Ethereum. A constant, not a setting, because rows stay in the partition they were
/// written to: a table whose width changed could no longer bound a delete by it.
pub(super) const PARTITION_BLOCKS: u64 = 100_000;

/// Digits of a `Uint` column: `u64::MAX` has 20.
pub(super) const UINT_PRECISION: u8 = 20;

/// The Delta type a column of `kind` is stored as.
fn delta_type(kind: ColumnType) -> Result<DataType, ArrowError> {
    Ok(match kind {
        ColumnType::Uint => DataType::decimal(UINT_PRECISION, 0).map_err(schema_error)?,
        ColumnType::Int => DataType::LONG,
        ColumnType::BigInt | ColumnType::Text | ColumnType::Document => DataType::STRING,
        ColumnType::Bool => DataType::BOOLEAN,
        ColumnType::Timestamp => DataType::TIMESTAMP,
        ColumnType::List(element) => ArrayType::new(delta_type(*element)?, true).into(),
    })
}

/// The Delta schema of `def`, with the [`PARTITION`] column last when its rows belong to
/// a block.
pub(super) fn delta_schema(def: &TableDef) -> Result<StructType, ArrowError> {
    let mut fields = def
        .columns
        .iter()
        .map(|column| {
            let kind = delta_type(column.kind)?;
            Ok(StructField::new(
                column.name.as_ref(),
                kind,
                column.nullable,
            ))
        })
        .collect::<Result<Vec<_>, ArrowError>>()?;
    if partitioned(def).is_some() {
        fields.push(StructField::new(PARTITION, DataType::LONG, false));
    }
    StructType::try_new(fields).map_err(schema_error)
}

fn schema_error(error: impl std::fmt::Display) -> ArrowError {
    ArrowError::SchemaError(error.to_string())
}

/// The Arrow schema a batch of `def` is built against.
pub(super) fn arrow_schema(def: &TableDef) -> Result<Arc<ArrowSchema>, ArrowError> {
    let schema: ArrowSchema = (&delta_schema(def)?).try_into_arrow()?;
    Ok(Arc::new(schema))
}

/// The block height column `def`'s [`PARTITION`] is derived from, when its rows belong
/// to a block.
pub(super) fn partitioned(def: &TableDef) -> Option<&str> {
    def.block_number.as_deref()
}

/// `rows` of one table as a batch of `schema`, each row in the partition its block height
/// falls in.
///
/// # Errors
///
/// Returns [`ArrowError::InvalidArgumentError`] when a value is not of its column's type,
/// which a row built from its own table never is.
pub(super) fn record_batch(
    schema: &Arc<ArrowSchema>,
    def: &TableDef,
    rows: &[&Row],
) -> Result<RecordBatch, ArrowError> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (position, column) in def.columns.iter().enumerate() {
        let values: Vec<&Value> = rows.iter().map(|row| &row.values()[position]).collect();
        columns.push(array(
            &def.name,
            column.kind,
            schema.field(position),
            &values,
        )?);
    }
    if let Some(height) = partitioned(def) {
        let ranges = rows
            .iter()
            .map(|row| {
                row.block_number()
                    .map(partition_of)
                    .ok_or_else(|| invalid(&def.name, height))
            })
            .collect::<Result<Int64Array, _>>()?;
        columns.push(Arc::new(ranges));
    }
    RecordBatch::try_new(Arc::clone(schema), columns)
}

/// The partition a block at `height` falls in.
pub(super) fn partition_of(height: u64) -> i64 {
    // `u64::MAX` divided by the width is far inside `i64`, so this never saturates.
    i64::try_from(height / PARTITION_BLOCKS).unwrap_or(i64::MAX)
}

/// One column's array: `values` as `kind`, typed as `field` says.
fn array(
    table: &str,
    kind: ColumnType,
    field: &Field,
    values: &[&Value],
) -> Result<ArrayRef, ArrowError> {
    let wrong = || invalid(table, field.name());
    Ok(match kind {
        ColumnType::Uint => Arc::new(
            column::<Decimal128Array, _>(values, wrong, |value| match value {
                Value::Uint(number) => Some(i128::from(*number)),
                _ => None,
            })?
            .with_precision_and_scale(UINT_PRECISION, 0)?,
        ),
        ColumnType::Int => Arc::new(column::<Int64Array, _>(
            values,
            wrong,
            |value| match value {
                Value::Int(number) => Some(*number),
                _ => None,
            },
        )?),
        ColumnType::BigInt => Arc::new(column::<StringArray, _>(
            values,
            wrong,
            |value| match value {
                Value::BigInt {
                    negative,
                    magnitude,
                } => Some(decimal_text(*negative, *magnitude)),
                _ => None,
            },
        )?),
        ColumnType::Text | ColumnType::Document => Arc::new(column::<StringArray, _>(
            values,
            wrong,
            |value| match value {
                Value::Text(text) | Value::Document(text) => Some(text.as_str()),
                _ => None,
            },
        )?),
        ColumnType::Bool => Arc::new(column::<BooleanArray, _>(
            values,
            wrong,
            |value| match value {
                Value::Bool(flag) => Some(*flag),
                _ => None,
            },
        )?),
        ColumnType::Timestamp => Arc::new(
            column::<TimestampMicrosecondArray, _>(values, wrong, |value| match value {
                Value::Timestamp(seconds) => i64::try_from(*seconds).ok()?.checked_mul(1_000_000),
                _ => None,
            })?
            .with_data_type(field.data_type().clone()),
        ),
        ColumnType::List(element) => {
            let ArrowType::List(element_field) = field.data_type() else {
                return Err(wrong());
            };
            let mut lengths = Vec::with_capacity(values.len());
            let mut present = Vec::with_capacity(values.len());
            let mut elements = Vec::new();
            for value in values {
                match value {
                    Value::Null => {
                        lengths.push(0);
                        present.push(false);
                    }
                    Value::List(items) => {
                        lengths.push(items.len());
                        present.push(true);
                        elements.extend(items);
                    }
                    _ => return Err(wrong()),
                }
            }
            Arc::new(ListArray::try_new(
                Arc::clone(element_field),
                OffsetBuffer::from_lengths(lengths),
                array(table, *element, element_field, &elements)?,
                Some(NullBuffer::from(present)),
            )?)
        }
    })
}

/// A column of scalars: `Null` as null, and every other value as `item` reads it, which
/// is `None` for a value of another type.
fn column<'a, A, T>(
    values: &[&'a Value],
    wrong: impl Fn() -> ArrowError,
    item: impl Fn(&'a Value) -> Option<T>,
) -> Result<A, ArrowError>
where
    A: FromIterator<Option<T>>,
{
    values
        .iter()
        .map(|value| match value {
            Value::Null => Ok(None),
            value => item(value).map(Some).ok_or_else(&wrong),
        })
        .collect()
}

/// An integer as signed decimal text: what a `BigInt` column holds.
pub(super) fn decimal_text(negative: bool, magnitude: U256) -> String {
    if negative && !magnitude.is_zero() {
        format!("-{magnitude}")
    } else {
        magnitude.to_string()
    }
}

fn invalid(table: &str, column: &str) -> ArrowError {
    ArrowError::InvalidArgumentError(format!("{table}.{column} holds a value of another type"))
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::U256;

    use super::decimal_text;

    /// A `uint256` and an `int256` at their extremes survive as exact decimal text, which
    /// is the only exact form a 78-digit integer has in Delta.
    #[test]
    fn wide_integers_are_exact_decimal_text() {
        assert_eq!(
            decimal_text(false, U256::MAX),
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        );
        let int256_min = U256::from(1) << 255;
        assert_eq!(
            decimal_text(true, int256_min),
            "-57896044618658097711785492504343953926634992332820282019728792003956564819968"
        );
        assert_eq!(decimal_text(true, U256::ZERO), "0");
        assert_eq!(
            U256::from_str_radix(&decimal_text(false, U256::MAX), 10).expect("parses"),
            U256::MAX
        );
    }
}
