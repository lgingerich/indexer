//! The `DataFusion` expressions the lake's reads and deletes filter by, and the columns
//! of the batches a read hands back.

use deltalake::arrow::array::{AsArray as _, RecordBatch, StringArray, UInt64Array};
use deltalake::arrow::datatypes::{DataType as ArrowType, UInt64Type};
use deltalake::datafusion::prelude::{Expr, cast, ident, lit};
use deltalake::datafusion::scalar::ScalarValue;

use super::arrow::{self, PARTITION, UINT_PRECISION};
use crate::sink::store_error::InvalidStoredValue;

/// `column >= height`, with the partition column bounding it too, `width` heights to a
/// partition, so a scan or delete skips every older partition without opening a file.
pub(super) fn at_or_above(column: &str, height: u64, width: u64) -> Expr {
    ident(PARTITION)
        .gt_eq(lit(arrow::partition_of(height, width)))
        .and(ident(column).gt_eq(uint(height)))
}

/// The rows of the blocks `hashes`, by a table's `(hash, height)` columns, none of them
/// below `floor` when there is one, `width` heights to a partition.
pub(super) fn of_blocks(
    (hash, height): (&str, &str),
    hashes: &[String],
    floor: Option<u64>,
    width: u64,
) -> Expr {
    let blocks = in_texts(hash, hashes);
    match floor {
        Some(floor) => at_or_above(height, floor, width).and(blocks),
        None => blocks,
    }
}

/// A `Uint` value, such as a block height, as the lake stores it.
pub(super) fn uint(value: u64) -> Expr {
    lit(ScalarValue::Decimal128(
        Some(i128::from(value)),
        UINT_PRECISION,
        0,
    ))
}

/// `column IN (values)`.
pub(super) fn in_texts(column: &str, values: &[String]) -> Expr {
    ident(column).in_list(
        values.iter().map(|value| lit(value.as_str())).collect(),
        false,
    )
}

/// `columns`, each cast to plain text: a scan may return a string view instead.
pub(super) fn texts_of(columns: &[&str]) -> Vec<Expr> {
    columns
        .iter()
        .map(|column| cast(ident(*column), ArrowType::Utf8).alias(*column))
        .collect()
}

pub(super) fn parse<T: std::str::FromStr>(
    column: &'static str,
    value: &str,
) -> Result<T, InvalidStoredValue> {
    value
        .parse()
        .map_err(|_| InvalidStoredValue::new(column, value))
}

/// Column `index` of a batch read back, which the read cast to text.
pub(super) fn texts<'a>(
    batch: &'a RecordBatch,
    index: usize,
    column: &'static str,
) -> Result<&'a StringArray, InvalidStoredValue> {
    batch
        .column(index)
        .as_string_opt::<i32>()
        .ok_or_else(|| InvalidStoredValue::new(column, "not text"))
}

/// Column `index` of a batch read back, which the read cast to unsigned integers.
pub(super) fn uints<'a>(
    batch: &'a RecordBatch,
    index: usize,
    column: &'static str,
) -> Result<&'a UInt64Array, InvalidStoredValue> {
    batch
        .column(index)
        .as_primitive_opt::<UInt64Type>()
        .ok_or_else(|| InvalidStoredValue::new(column, "not an unsigned integer"))
}
