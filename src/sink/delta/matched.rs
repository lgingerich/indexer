//! The pools the lake records as matched to Metric pools' pairs (see
//! [`Matched`]), read back when it opens so the decode stage keeps them.

use deltalake::arrow::array::{Array as _, RecordBatch, StringArray, UInt64Array};
use deltalake::arrow::datatypes::DataType as ArrowType;
use deltalake::datafusion::prelude::{Expr, cast, ident};

use super::expr::{seconds, texts, uints};
use super::lake::Lake;
use crate::sink::store_error::{InvalidStoredValue, StoreError};
use crate::sink::table::MATCHED_COLUMNS;
use crate::wire::envelope::Matched;

/// The columns read as whole seconds; every other is text or an unsigned integer.
const TIMESTAMPS: [&str; 2] = ["from_timestamp", "block_timestamp"];
/// The columns read as unsigned integers.
const NUMBERS: [&str; 3] = ["fee", "created_block", "block_number"];

/// The rows of `lake`, a `matched_pools` table, in key order.
pub(super) async fn matched(lake: &Lake) -> Result<Vec<Matched>, StoreError> {
    let columns: Vec<Expr> = MATCHED_COLUMNS
        .iter()
        .map(|&column| {
            if TIMESTAMPS.contains(&column) {
                seconds(column)
            } else if NUMBERS.contains(&column) {
                cast(ident(column), ArrowType::UInt64).alias(column)
            } else {
                cast(ident(column), ArrowType::Utf8).alias(column)
            }
        })
        .collect();
    let batches = lake
        .read(|frame| {
            frame
                .sort(vec![ident("dedupe_key").sort(true, false)])?
                .select(columns)
        })
        .await?;
    let mut matched = Vec::new();
    for batch in &batches {
        let read = Columns(batch);
        for row in 0..batch.num_rows() {
            let text = |column| read.text(column).map(|texts| texts.value(row));
            let number = |column| read.number(column).map(|numbers| numbers.value(row));
            let parse = |column| -> Result<_, InvalidStoredValue> { Ok((column, text(column)?)) };
            matched.push(Matched {
                protocol: text("protocol")?.to_owned(),
                contract: typed(parse("contract")?)?,
                pool_id: read.optional("pool_id", row)?,
                token0: typed(parse("token0")?)?,
                token1: typed(parse("token1")?)?,
                fee: number("fee")?,
                hooks: read.optional("hooks", row)?,
                created_block: number("created_block")?,
                from_timestamp: number("from_timestamp")?,
                filled: typed(parse("filled")?)?,
                block_number: number("block_number")?,
                block_hash: typed(parse("block_hash")?)?,
                block_timestamp: number("block_timestamp")?,
            });
        }
    }
    Ok(matched)
}

/// A column's text, `(column, text)`, parsed as what it holds.
fn typed<T: std::str::FromStr>(
    (column, text): (&'static str, &str),
) -> Result<T, InvalidStoredValue> {
    InvalidStoredValue::parse(column, text)
}

/// A batch read in [`MATCHED_COLUMNS`]' order, its columns found by name.
struct Columns<'a>(&'a RecordBatch);

impl Columns<'_> {
    /// Where `column` is in the batch: an error for a name the read did not select.
    fn position(column: &'static str) -> Result<usize, InvalidStoredValue> {
        MATCHED_COLUMNS
            .iter()
            .position(|named| *named == column)
            .ok_or_else(|| InvalidStoredValue::new(column, "not read"))
    }

    fn text(&self, column: &'static str) -> Result<&StringArray, InvalidStoredValue> {
        texts(self.0, Self::position(column)?, column)
    }

    fn number(&self, column: &'static str) -> Result<&UInt64Array, InvalidStoredValue> {
        uints(self.0, Self::position(column)?, column)
    }

    /// Row `row` of nullable text `column`, parsed, or `None` for none.
    fn optional<T: std::str::FromStr>(
        &self,
        column: &'static str,
        row: usize,
    ) -> Result<Option<T>, InvalidStoredValue> {
        let texts = self.text(column)?;
        (!texts.is_null(row))
            .then(|| InvalidStoredValue::parse(column, texts.value(row)))
            .transpose()
    }
}
