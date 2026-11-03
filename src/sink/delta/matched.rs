//! The pools the lake records as matched to Metric pools' pairs (see
//! [`Matched`]), read back when it opens so the decode stage keeps them.

use deltalake::arrow::array::Array as _;
use deltalake::arrow::datatypes::DataType as ArrowType;
use deltalake::datafusion::prelude::{cast, ident};

use super::expr::{seconds, texts, texts_of, uints};
use super::lake::Lake;
use crate::sink::store_error::{InvalidStoredValue, StoreError};
use crate::sink::table::MATCHED_COLUMNS;
use crate::wire::envelope::Matched;

/// The rows of `lake`, a `matched_pools` table, in key order.
pub(super) async fn matched(lake: &Lake) -> Result<Vec<Matched>, StoreError> {
    let [
        protocol,
        contract,
        pool_id,
        token0,
        token1,
        fee,
        hooks,
        created_block,
        from_timestamp,
        filled,
        block_number,
        block_hash,
        block_timestamp,
    ] = MATCHED_COLUMNS;
    let batches = lake
        .read(|frame| {
            let mut columns = texts_of(&[
                protocol, contract, pool_id, token0, token1, hooks, filled, block_hash,
            ]);
            for number in [fee, created_block, block_number] {
                columns.push(cast(ident(number), ArrowType::UInt64).alias(number));
            }
            columns.push(seconds(from_timestamp));
            columns.push(seconds(block_timestamp));
            frame
                .sort(vec![ident("dedupe_key").sort(true, false)])?
                .select(columns)
        })
        .await?;
    let mut matched = Vec::new();
    for batch in &batches {
        let text = |index, column| texts(batch, index, column);
        let (protocols, contracts, ids) = (
            text(0, "matched_pools.protocol")?,
            text(1, "matched_pools.contract")?,
            text(2, "matched_pools.pool_id")?,
        );
        let (firsts, seconds_tokens, hook_texts) = (
            text(3, "matched_pools.token0")?,
            text(4, "matched_pools.token1")?,
            text(5, "matched_pools.hooks")?,
        );
        let (filleds, hashes) = (
            text(6, "matched_pools.filled")?,
            text(7, "matched_pools.block_hash")?,
        );
        let fees = uints(batch, 8, "matched_pools.fee")?;
        let createds = uints(batch, 9, "matched_pools.created_block")?;
        let numbers = uints(batch, 10, "matched_pools.block_number")?;
        let froms = uints(batch, 11, "matched_pools.from_timestamp")?;
        let timestamps = uints(batch, 12, "matched_pools.block_timestamp")?;
        for row in 0..batch.num_rows() {
            matched.push(Matched {
                protocol: protocols.value(row).to_owned(),
                contract: InvalidStoredValue::parse(
                    "matched_pools.contract",
                    contracts.value(row),
                )?,
                pool_id: (!ids.is_null(row))
                    .then(|| InvalidStoredValue::parse("matched_pools.pool_id", ids.value(row)))
                    .transpose()?,
                token0: InvalidStoredValue::parse("matched_pools.token0", firsts.value(row))?,
                token1: InvalidStoredValue::parse(
                    "matched_pools.token1",
                    seconds_tokens.value(row),
                )?,
                fee: fees.value(row),
                hooks: (!hook_texts.is_null(row))
                    .then(|| {
                        InvalidStoredValue::parse("matched_pools.hooks", hook_texts.value(row))
                    })
                    .transpose()?,
                created_block: createds.value(row),
                from_timestamp: froms.value(row),
                filled: InvalidStoredValue::parse("matched_pools.filled", filleds.value(row))?,
                block_number: numbers.value(row),
                block_hash: InvalidStoredValue::parse(
                    "matched_pools.block_hash",
                    hashes.value(row),
                )?,
                block_timestamp: timestamps.value(row),
            });
        }
    }
    Ok(matched)
}
