//! Splitting a response into a block per height, checked, and projecting its rows.

use alloy_primitives::B256;

use crate::ingest::source::assemble::{
    BlockRows, check_height, check_log_order, check_logs_present, check_no_leftovers,
};
use crate::ingest::source::{FetchedBlock, SourceError, inconsistent, malformed};
use crate::wire::envelope::BlockMeta;

use super::wire::{Quantity, RawBlock, RawLog, RawTransaction, Response, required};
use super::{CONTEXT, Plan};

/// Splits one response into a block per height, from `from` to its `next_block`.
///
/// Rows come back sorted by block, then by index; each block takes the next run of rows
/// carrying its number, and a row left over at the end, or out of order, is an error.
/// Events are ordered as every source orders them; see [`BlockRows::into_events`].
pub(super) fn assemble(
    from: u64,
    to: u64,
    response: Response,
    plan: Plan,
) -> Result<Vec<FetchedBlock>, SourceError> {
    let Response { data, next_block } = response;
    if next_block <= from {
        return Err(inconsistent(
            CONTEXT,
            format!("made no progress past {from}; its archive may not reach that height yet"),
        ));
    }
    let last = next_block - 1;
    if last > to {
        return Err(malformed(
            CONTEXT,
            format!("answered through {last} for heights {from}..={to}"),
        ));
    }
    let mut blocks = Vec::new();
    let mut transactions = Vec::new();
    let mut logs = Vec::new();
    for batch in data {
        blocks.extend(batch.blocks);
        transactions.extend(batch.transactions);
        logs.extend(batch.logs);
    }
    let expected = usize::try_from(next_block - from).unwrap_or(usize::MAX);
    if blocks.len() != expected {
        return Err(malformed(
            CONTEXT,
            format!("{} blocks for heights {from}..={last}", blocks.len()),
        ));
    }

    let mut transactions = transactions.into_iter().peekable();
    let mut logs = logs.into_iter().peekable();
    let mut fetched = Vec::with_capacity(blocks.len());
    for (height, block) in (from..=last).zip(blocks) {
        let meta = block.meta()?;
        check_height(CONTEXT, height, meta.height)?;
        let own_transactions = take_block(&mut transactions, meta, |row| {
            (row.block_number, row.block_hash)
        })?;
        let own_logs = take_block(&mut logs, meta, |row| (row.block_number, row.block_hash))?;
        let events = project(block, meta, own_transactions, own_logs, plan)?.into_events();
        fetched.push(FetchedBlock { meta, events });
    }
    let leftover = transactions.next().is_some() || logs.next().is_some();
    check_no_leftovers(CONTEXT, from, last, leftover)?;
    Ok(fetched)
}

/// Takes the next run of `rows` at `meta`'s height, each checked to name its block.
fn take_block<T>(
    rows: &mut std::iter::Peekable<std::vec::IntoIter<T>>,
    meta: BlockMeta,
    identity: impl Fn(&T) -> (Quantity, B256),
) -> Result<Vec<T>, SourceError> {
    let mut own = Vec::new();
    while let Some(row) = rows.peek() {
        let (number, hash) = identity(row);
        if number.to_u64("block_number")? != meta.height {
            break;
        }
        if hash != meta.hash {
            return Err(inconsistent(
                CONTEXT,
                format!(
                    "a row at {} belongs to block {hash}, not {}",
                    meta.height, meta.hash
                ),
            ));
        }
        own.extend(rows.next());
    }
    Ok(own)
}

/// A log row with its two indexes read.
struct PlacedLog {
    log_index: u64,
    transaction_index: u64,
    row: RawLog,
}

/// Reads a block's logs' indexes, refusing logs out of `log_index` order.
fn place_logs(logs: Vec<RawLog>, meta: BlockMeta) -> Result<Vec<PlacedLog>, SourceError> {
    let placed = logs
        .into_iter()
        .map(|row| {
            Ok(PlacedLog {
                log_index: row.log_index.to_u64("log_index")?,
                transaction_index: row.transaction_index.to_u64("transaction_index")?,
                row,
            })
        })
        .collect::<Result<Vec<_>, SourceError>>()?;
    check_log_order(CONTEXT, meta.height, placed.iter().map(|log| log.log_index))?;
    Ok(placed)
}

/// Checks a block's rows against each other and its header: logs present when the bloom
/// is set, transactions in index order with no gap, the last one's cumulative gas the
/// block's, and every log naming a transaction the block has.
fn check_block(
    block: &RawBlock,
    meta: BlockMeta,
    transactions: &[RawTransaction],
    placed: &[PlacedLog],
    plan: Plan,
) -> Result<(), SourceError> {
    if plan.reads_logs {
        check_logs_present(CONTEXT, meta.height, !placed.is_empty(), &block.logs_bloom)?;
    }
    for (position, transaction) in transactions.iter().enumerate() {
        if transaction.transaction_index.to_u64("transaction_index")? != position as u64 {
            return Err(malformed(
                CONTEXT,
                format!(
                    "transaction {position} of block {} is missing or out of order",
                    meta.height
                ),
            ));
        }
    }
    if plan.reads_transactions {
        let cumulative = match transactions.last() {
            Some(last) => required(last.cumulative_gas_used, "cumulative_gas_used")?
                .to_u64("cumulative_gas_used")?,
            None => 0,
        };
        let gas_used = block.gas_used.to_u64("gas_used")?;
        if cumulative != gas_used {
            return Err(inconsistent(
                CONTEXT,
                format!(
                    "block {} used {gas_used} gas, but its receipts add to {cumulative}",
                    meta.height
                ),
            ));
        }
    }
    if plan.reads_transactions
        && let Some(log) = placed
            .iter()
            .find(|log| log.transaction_index >= transactions.len() as u64)
    {
        return Err(malformed(
            CONTEXT,
            format!(
                "a log of block {} names transaction {}, which it does not have",
                meta.height, log.transaction_index
            ),
        ));
    }
    Ok(())
}

/// One block's rows, from its header and the rows that carry its number.
fn project(
    block: RawBlock,
    meta: BlockMeta,
    transactions: Vec<RawTransaction>,
    logs: Vec<RawLog>,
    plan: Plan,
) -> Result<BlockRows, SourceError> {
    let placed = place_logs(logs, meta)?;
    check_block(&block, meta, &transactions, &placed, plan)?;
    let datasets = plan.datasets;

    let mut rows = BlockRows::default();
    if datasets.blocks {
        let hashes = transactions
            .iter()
            .map(|transaction| transaction.hash)
            .collect();
        rows.block = Some(block.into_block(meta, hashes)?);
    }
    if datasets.transactions {
        let mut placed = placed.into_iter().peekable();
        for (position, transaction) in transactions.into_iter().enumerate() {
            let index = position as u64;
            let mut count = 0;
            let mut own = Vec::new();
            while let Some(log) = placed.next_if(|log| log.transaction_index == index) {
                count += 1;
                if datasets.logs {
                    own.push(log.row.into_log(meta, index, log.log_index)?);
                }
            }
            rows.transactions
                .push((transaction.into_transaction(meta, index, count)?, own));
        }
        if placed.next().is_some() {
            return Err(malformed(
                CONTEXT,
                format!(
                    "logs of block {} are not grouped by transaction",
                    meta.height
                ),
            ));
        }
    } else if datasets.logs {
        for log in placed {
            rows.logs.push(
                log.row
                    .into_log(meta, log.transaction_index, log.log_index)?,
            );
        }
    }
    Ok(rows)
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::ingest::source::hypersync::TRANSACTION_POSITION_FIELDS;

    use crate::ingest::source::Datasets;
    use crate::wire::envelope::Event;

    /// A block's identity row: `number`'s hash is `0x..number`, and its parent the one
    /// below. `bloom` sets the logs bloom, and `gas` its gas used.
    fn block(number: u64, bloom: bool, gas: u64) -> serde_json::Value {
        serde_json::json!({
            "number": number,
            "hash": hash(number),
            "parent_hash": hash(number - 1),
            "timestamp": "0x064",
            "logs_bloom": format!("0x{}{}", if bloom { "01" } else { "00" }, "00".repeat(255)),
            "gas_used": format!("0x{gas:x}"),
        })
    }

    fn hash(number: u64) -> String {
        format!("0x{number:064x}")
    }

    fn log(number: u64, transaction_index: u64, log_index: u64) -> serde_json::Value {
        serde_json::json!({
            "block_hash": hash(number),
            "block_number": number,
            "transaction_index": transaction_index,
            "log_index": log_index,
            "transaction_hash": hash(1000 + transaction_index),
            "address": "0x00000000000000000000000000000000000000aa",
            "data": "0x",
            "topic0": hash(7),
        })
    }

    fn logs_only() -> Datasets {
        Datasets {
            blocks: false,
            transactions: false,
            logs: true,
        }
    }

    fn respond(
        next_block: u64,
        blocks: &[serde_json::Value],
        logs: &[serde_json::Value],
    ) -> Response {
        serde_json::from_value(serde_json::json!({
            "data": [{ "blocks": blocks, "logs": logs }],
            "next_block": next_block,
        }))
        .expect("response")
    }

    /// Every height in the response becomes a block, empty ones included, each with the
    /// logs that name it, in order.
    #[test]
    fn a_response_splits_into_a_block_per_height() {
        let response = respond(
            13,
            &[block(10, true, 0), block(11, false, 0), block(12, true, 0)],
            &[log(10, 0, 0), log(10, 1, 1), log(12, 0, 0)],
        );
        let blocks = assemble(10, 20, response, Plan::new(logs_only())).expect("assembles");
        let shape: Vec<_> = blocks
            .iter()
            .map(|block| (block.meta.height, block.events.len()))
            .collect();
        assert_eq!(shape, [(10, 2), (11, 0), (12, 1)]);
        let Event::Log(second) = &blocks[0].events[1] else {
            panic!("a log");
        };
        assert_eq!(
            (
                second.log_index,
                second.transaction_index,
                second.block_timestamp
            ),
            (1, 1, 100)
        );
    }

    /// A response that skips a height, answers a log for another block, leaves logs out
    /// of a block whose bloom says it has some, misorders them, runs past the range, or
    /// makes no progress, is refused: as inconsistent when asking again can help, and as
    /// malformed when it cannot.
    #[test]
    fn a_short_or_mismatched_response_is_refused() {
        let mut foreign = log(10, 0, 0);
        foreign["block_hash"] = hash(99).into();
        let empty: Response = serde_json::from_value(serde_json::json!({
            "data": [], "next_block": 10, "archive_height": 9
        }))
        .expect("response");
        for (case, to, response, expected) in [
            (
                "a skipped height",
                20,
                respond(12, &[block(10, false, 0)], &[]),
                "malformed data from hypersync: 1 blocks for heights 10..=11".to_owned(),
            ),
            (
                "a log of another block",
                20,
                respond(11, &[block(10, true, 0)], &[foreign]),
                format!(
                    "inconsistent data from hypersync: a row at 10 belongs to block {}, not {}",
                    hash(99),
                    hash(10)
                ),
            ),
            (
                "no logs under a set bloom",
                20,
                respond(11, &[block(10, true, 0)], &[]),
                "inconsistent data from hypersync: no logs returned for block 10, whose logsBloom is set"
                    .to_owned(),
            ),
            (
                "misordered logs",
                20,
                respond(11, &[block(10, true, 0)], &[log(10, 0, 1), log(10, 0, 0)]),
                "malformed data from hypersync: logs of block 10 are not in ascending logIndex order"
                    .to_owned(),
            ),
            (
                "past the end",
                10,
                respond(12, &[block(10, false, 0), block(11, false, 0)], &[]),
                "malformed data from hypersync: answered through 11 for heights 10..=10".to_owned(),
            ),
            (
                "no progress",
                20,
                empty,
                "inconsistent data from hypersync: made no progress past 10; its archive may not reach that height yet"
                    .to_owned(),
            ),
        ] {
            let error = assemble(10, to, response, Plan::new(logs_only()))
                .expect_err(case);
            assert_eq!(error.to_string(), expected, "{case}");
        }
    }

    /// With transactions read, the last receipt's cumulative gas must be the block's: for
    /// the `transactions` dataset, and for `blocks` alone, whose row lists the hashes.
    #[test]
    fn a_dropped_transaction_fails_the_gas_check() {
        let transaction = |index: u64, cumulative: u64| {
            serde_json::json!({
                "block_hash": hash(10),
                "block_number": 10,
                "transaction_index": index,
                "hash": hash(1000 + index),
                "nonce": "0x0", "from": "0x00000000000000000000000000000000000000bb",
                "value": "0x0", "gas": "0x5208", "input": "0x", "type": 0,
                "status": "0x1", "gas_used": "0x5208",
                "cumulative_gas_used": format!("0x{cumulative:x}"),
                "effective_gas_price": "0x1",
                "logs_bloom": format!("0x{}", "00".repeat(256)),
            })
        };
        let transactions = Datasets {
            blocks: false,
            transactions: true,
            logs: false,
        };
        let response = |rows: &[serde_json::Value]| -> Response {
            serde_json::from_value(serde_json::json!({
                "data": [{ "blocks": [block(10, false, 42_000)], "transactions": rows }],
                "next_block": 11,
            }))
            .expect("response")
        };
        let whole = [transaction(0, 21_000), transaction(1, 42_000)];
        let blocks =
            assemble(10, 10, response(&whole), Plan::new(transactions)).expect("assembles");
        let kinds: Vec<&str> = blocks[0].events.iter().map(Event::kind).collect();
        assert_eq!(kinds, ["transaction", "transaction"]);
        let dropped = "inconsistent data from hypersync: block 10 used 42000 gas, but its receipts add to 21000";
        let error = assemble(10, 10, response(&whole[..1]), Plan::new(transactions))
            .expect_err("transactions");
        assert_eq!(error.to_string(), dropped);

        // `blocks` alone reads only the transactions' positions, and checks them the same.
        let positions: Vec<_> = whole
            .iter()
            .map(|row| {
                let mut row = row.clone();
                row.as_object_mut()
                    .expect("an object")
                    .retain(|field, _| TRANSACTION_POSITION_FIELDS.contains(&field.as_str()));
                row
            })
            .collect();
        let blocks_only = Datasets {
            blocks: true,
            transactions: false,
            logs: false,
        };
        let error = assemble(10, 10, response(&positions[..1]), Plan::new(blocks_only))
            .expect_err("blocks");
        assert_eq!(error.to_string(), dropped);
    }
}
