//! What a store must hold, computed from the chain, and what it does hold, read back.
//!
//! A store's rows are a function of its ledger. A block's rows commit with its ledger
//! entry, so whatever the process was doing when it went, a store holds exactly the rows
//! of the blocks its ledger names, each as the chain made it. The ledger itself must be
//! one branch of the chain: linked, starting at or above the run's start height. The
//! oracle walks that branch back from the store's tip to the run's start, since what
//! ingest decoded from a block depends on the blocks before it, and compares every
//! table the store writes, row for row.

use std::collections::BTreeMap;
use std::path::Path;

use alloy_primitives::B256;

use super::chain::{SimBlock, World};
use super::{CHAIN, Plan, StorePlan};

/// Rows as text, by table, each table's rows sorted.
pub(super) type Stored = BTreeMap<&'static str, Vec<Vec<String>>>;

/// The tables the oracle compares, how it reads each back as text, and how many
/// columns that reads.
const TABLES: &[(&str, &str, usize)] = &[
    (
        "accepted_blocks",
        "SELECT height::VARCHAR, hash, parent_hash FROM sim.accepted_blocks",
        3,
    ),
    (
        "blocks",
        "SELECT number::VARCHAR, hash, parent_hash, epoch(timestamp)::BIGINT::VARCHAR, \
         logs_bloom, transaction_count::VARCHAR, extra_data FROM sim.blocks",
        7,
    ),
    (
        "logs",
        "SELECT block_hash, log_index::VARCHAR, transaction_hash, \
         transaction_index::VARCHAR, address, coalesce(topic0, '-'), \
         coalesce(topic1, '-'), coalesce(topic2, '-'), coalesce(topic3, '-'), data, \
         block_number::VARCHAR, epoch(block_timestamp)::BIGINT::VARCHAR, \
         removed::VARCHAR FROM sim.logs",
        13,
    ),
];

/// The rows `store` must hold given its ledger's `tip` and `first` block heights: the
/// branch ending at `tip`, from `first`.
pub(super) fn expected(
    world: &World,
    plan: &Plan,
    store: &StorePlan,
    tip: B256,
    first: u64,
) -> Result<Stored, String> {
    let mut branch: Vec<&SimBlock> = Vec::new();
    let mut hash = tip;
    loop {
        let block = world
            .block(&hash)
            .ok_or_else(|| format!("the tip {hash} is not a block the chain made"))?;
        if block.number() < plan.start {
            break;
        }
        branch.push(block);
        if block.number() == plan.start {
            break;
        }
        hash = block.header.inner.parent_hash;
    }
    branch.reverse();
    let mut stored = Stored::new();
    for block in branch.iter().filter(|block| block.number() >= first) {
        let mut add = |table: &'static str, rows: Vec<Vec<String>>| {
            stored.entry(table).or_default().extend(rows);
        };
        add("accepted_blocks", vec![ledger_row(block)]);
        if plan.datasets.blocks && store.raw {
            add("blocks", vec![block_row(block)]);
        }
        if plan.datasets.logs && store.raw {
            add("logs", log_rows(block));
        }
    }
    for rows in stored.values_mut() {
        rows.sort();
    }
    stored.retain(|_, rows| !rows.is_empty());
    Ok(stored)
}

/// Why `stored`, which `store` holds, is not what its ledger says it must be.
pub(super) fn check(world: &World, plan: &Plan, store: &StorePlan, stored: &Stored) -> Vec<String> {
    let name = store.backend;
    let Some((tip, first)) = ends(stored) else {
        return stored
            .iter()
            .filter(|(_, rows)| !rows.is_empty())
            .map(|(table, rows)| format!("{name} has no ledger but {} {table} rows", rows.len()))
            .collect();
    };
    let mut problems = Vec::new();
    if first < plan.start {
        problems.push(format!(
            "{name}'s ledger starts at {first}, below the start {}",
            plan.start
        ));
    }
    let expected = match expected(world, plan, store, tip, first) {
        Ok(expected) => expected,
        Err(problem) => return vec![format!("{name}: {problem}")],
    };
    for table in stored.keys().chain(expected.keys()) {
        let empty = Vec::new();
        let have = stored.get(table).unwrap_or(&empty);
        let want = expected.get(table).unwrap_or(&empty);
        if have == want {
            continue;
        }
        let extra: Vec<_> = have
            .iter()
            .filter(|row| !want.contains(row))
            .take(3)
            .collect();
        let missing: Vec<_> = want
            .iter()
            .filter(|row| !have.contains(row))
            .take(3)
            .collect();
        problems.push(format!(
            "{name}'s {table} differ from its ledger's branch: {} rows, {} expected; \
             extra {extra:?}, missing {missing:?}",
            have.len(),
            want.len()
        ));
    }
    problems.dedup();
    problems
}

/// The hash of the ledger's newest block and the height of its oldest, if it has any.
pub(super) fn ends(stored: &Stored) -> Option<(B256, u64)> {
    let ledger = stored.get("accepted_blocks")?;
    let height = |row: &Vec<String>| row[0].parse::<u64>().unwrap_or(u64::MAX);
    let tip = ledger.iter().max_by_key(|row| height(row))?;
    let first = ledger.iter().map(height).min()?;
    Some((tip[1].parse().ok()?, first))
}

fn ledger_row(block: &SimBlock) -> Vec<String> {
    vec![
        block.number().to_string(),
        format!("{:#x}", block.hash()),
        format!("{:#x}", block.header.inner.parent_hash),
    ]
}

fn block_row(block: &SimBlock) -> Vec<String> {
    let header = &block.header.inner;
    vec![
        header.number.to_string(),
        format!("{:#x}", block.hash()),
        format!("{:#x}", header.parent_hash),
        header.timestamp.to_string(),
        format!("{:#x}", header.logs_bloom),
        block.transactions().len().to_string(),
        header.extra_data.to_string(),
    ]
}

fn log_rows(block: &SimBlock) -> Vec<Vec<String>> {
    let transactions = block.transactions();
    block
        .logs
        .iter()
        .zip(0_u64..)
        .map(|(log, index)| {
            let topic = |i: usize| {
                log.topics
                    .get(i)
                    .map_or_else(|| "-".to_owned(), |t| format!("{t:#x}"))
            };
            let transaction_index = transactions
                .iter()
                .position(|hash| *hash == log.transaction_hash)
                .unwrap_or_default();
            vec![
                format!("{:#x}", block.hash()),
                index.to_string(),
                format!("{:#x}", log.transaction_hash),
                transaction_index.to_string(),
                format!("{:#x}", log.address),
                topic(0),
                topic(1),
                topic(2),
                topic(3),
                log.data.to_string(),
                block.number().to_string(),
                block.header.inner.timestamp.to_string(),
                "false".to_owned(),
            ]
        })
        .collect()
}

/// The rows the oracle compares, read from the database file without writing to it.
/// A table the store does not write is absent, and reads as no rows.
pub(super) fn read(path: &Path) -> Stored {
    let mut stored = Stored::new();
    if !path.exists() {
        return stored;
    }
    let config = duckdb::Config::default()
        .access_mode(duckdb::AccessMode::ReadOnly)
        .expect("read-only mode");
    let connection = duckdb::Connection::open_with_flags(path, config).expect("open store to read");
    for &(table, sql, width) in TABLES {
        let exists: u64 = connection
            .query_row(
                "SELECT count(*) FROM information_schema.tables \
                 WHERE table_schema = ? AND table_name = ?",
                [CHAIN, table],
                |row| row.get(0),
            )
            .expect("list tables");
        if exists == 0 {
            continue;
        }
        let mut statement = connection.prepare(sql).expect("prepare");
        let mut rows: Vec<Vec<String>> = statement
            .query_map([], |row| {
                (0..width).map(|i| row.get::<_, String>(i)).collect()
            })
            .expect("query")
            .collect::<Result<_, _>>()
            .expect("rows");
        if rows.is_empty() {
            continue;
        }
        rows.sort();
        stored.insert(table, rows);
    }
    stored
}
