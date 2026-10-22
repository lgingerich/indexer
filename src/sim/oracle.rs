//! What a store must hold, computed from the chain, and what it does hold, read back.
//!
//! A store's rows are a function of its ledger. A block's rows commit with its ledger
//! entry, so whatever the process was doing when it went, a store holds exactly the rows
//! of the blocks its ledger names, each as the chain made it. The ledger itself must be
//! one branch of the chain: linked, starting at or above the run's start height. The
//! oracle walks that branch back from the store's tip to the run's start, since what
//! ingest decoded from a block depends on the blocks before it, and compares every
//! table the store writes, row for row.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use alloy_primitives::{Address, B256};

use super::chain::{SimBlock, World};
use super::protocol::{self, PROTOCOL};
use super::{CHAIN, Plan, StorePlan};

/// Rows as text, by table, each table's rows sorted.
pub(super) type Stored = BTreeMap<&'static str, Vec<Vec<String>>>;

/// The tables the oracle compares, how it reads each back as text, and how many
/// columns that reads.
const TABLES: &[(&str, &str, usize)] = &[
    (
        "accepted_blocks",
        "SELECT height::VARCHAR, hash, parent_hash FROM accepted_blocks",
        3,
    ),
    (
        "blocks",
        "SELECT number::VARCHAR, hash, parent_hash, epoch(timestamp)::BIGINT::VARCHAR, \
         logs_bloom, transaction_count::VARCHAR, extra_data FROM blocks",
        7,
    ),
    (
        "logs",
        "SELECT block_hash, log_index::VARCHAR, transaction_hash, \
         transaction_index::VARCHAR, address, coalesce(topic0, '-'), \
         coalesce(topic1, '-'), coalesce(topic2, '-'), coalesce(topic3, '-'), data, \
         block_number::VARCHAR, epoch(block_timestamp)::BIGINT::VARCHAR, \
         removed::VARCHAR FROM logs",
        13,
    ),
    (
        "transactions",
        "SELECT block_hash, hash, transaction_index::VARCHAR, from_address, to_address, \
         nonce::VARCHAR, input, receipt_status::VARCHAR, log_count::VARCHAR, \
         block_number::VARCHAR FROM transactions",
        10,
    ),
    (
        "decoded_logs",
        "SELECT block_hash, log_index::VARCHAR, address, transaction_hash, protocol, \
         contract, name FROM decoded_logs",
        7,
    ),
    (
        "contracts",
        "SELECT block_hash, log_index::VARCHAR, address, transaction_hash, protocol, \
         name, factory_address FROM contracts",
        7,
    ),
    (
        "metric_v1_factory_pool_created",
        "SELECT block_hash, log_index::VARCHAR, address, transaction_hash, pool_address \
         FROM metric_v1_factory_pool_created",
        5,
    ),
    (
        "metric_v1_pool_swap",
        "SELECT block_hash, log_index::VARCHAR, address, transaction_hash, sender, \
         recipient FROM metric_v1_pool_swap",
        6,
    ),
    (
        "metric_v1_swaps",
        "SELECT block_hash, log_index::VARCHAR, address, transaction_hash, sender, \
         recipient, coalesce(transaction_from_address, '-'), \
         coalesce(transaction_to_address, '-') FROM metric_v1_swaps",
        8,
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
    // The pools discovered on the branch so far: decoding depends on every block from
    // the start, though a store holds rows only from its first.
    let mut pools: BTreeSet<Address> = BTreeSet::new();
    for block in branch {
        let decoded = decode(block, &mut pools, plan.datasets.transactions);
        if block.number() < first {
            continue;
        }
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
        if plan.datasets.transactions && store.raw {
            add("transactions", transaction_rows(block));
        }
        for (table, row) in decoded {
            if table == "contracts" || store.decoded {
                add(table, vec![row]);
            }
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

/// The rows decode makes of `block`'s logs, given the `pools` discovered before it,
/// which it adds the block's creations to. A creation publishes a contract once per
/// pool and block, whether or not an earlier block created the pool too, since a reorg
/// of either must retract it. A pool decodes from the log after the one
/// that created it, in the same block too, and a swap from an address not yet created
/// decodes nothing.
///
/// A derived row carries its transaction's sender and recipient when transactions are
/// fetched, and nothing in their place when they are not.
fn decode(
    block: &SimBlock,
    pools: &mut BTreeSet<Address>,
    transactions: bool,
) -> Vec<(&'static str, Vec<String>)> {
    let mut rows = Vec::new();
    // The pools this block has already created: a second creation of one in the same
    // block is decoded, but records nothing new, so it publishes no contract.
    let mut created = BTreeSet::new();
    let text = |address: &Address| format!("{address:#x}");
    for (log, index) in block.logs.iter().zip(0_u64..) {
        let hash = |table: &'static str, mut own: Vec<String>| {
            let mut row = vec![
                format!("{:#x}", block.hash()),
                index.to_string(),
                text(&log.address),
                format!("{:#x}", log.transaction_hash),
            ];
            row.append(&mut own);
            (table, row)
        };
        let topic = |i: usize| log.topics.get(i).map(|topic| Address::from_word(*topic));
        if log.address == protocol::FACTORY && log.topics.first() == Some(&protocol::created()) {
            let Some(pool) = topic(1) else { continue };
            let own = |name: &str| vec![PROTOCOL.to_owned(), name.to_owned()];
            rows.push(hash(
                "decoded_logs",
                [own("Factory"), vec!["PoolCreated".to_owned()]].concat(),
            ));
            rows.push(hash("metric_v1_factory_pool_created", vec![text(&pool)]));
            // A contract's row names the pool it created, and the factory after.
            let (table, mut row) = hash(
                "contracts",
                [own("MetricOmmPool"), vec![text(&log.address)]].concat(),
            );
            row[2] = text(&pool);
            if created.insert(pool) {
                rows.push((table, row));
            }
            pools.insert(pool);
        } else if pools.contains(&log.address) && log.topics.first() == Some(&protocol::swap()) {
            let (Some(sender), Some(recipient)) = (topic(1), topic(2)) else {
                continue;
            };
            let own = vec![
                PROTOCOL.to_owned(),
                "MetricOmmPool".to_owned(),
                "Swap".to_owned(),
            ];
            rows.push(hash("decoded_logs", own));
            rows.push(hash(
                "metric_v1_pool_swap",
                vec![text(&sender), text(&recipient)],
            ));
            let sent = block
                .transactions
                .iter()
                .find(|transaction| transaction.hash() == log.transaction_hash)
                .filter(|_| transactions);
            let (from, to) = sent.map_or_else(
                || ("-".to_owned(), "-".to_owned()),
                |sent| {
                    (
                        text(&sent.from),
                        sent.to().map_or_else(|| "-".to_owned(), |to| text(&to)),
                    )
                },
            );
            rows.push(hash(
                "metric_v1_swaps",
                vec![text(&sender), text(&recipient), from, to],
            ));
        }
    }
    rows
}

fn transaction_rows(block: &SimBlock) -> Vec<Vec<String>> {
    block
        .transactions
        .iter()
        .enumerate()
        .map(|(index, transaction)| {
            vec![
                format!("{:#x}", block.hash()),
                format!("{:#x}", transaction.hash()),
                index.to_string(),
                format!("{:#x}", transaction.from),
                transaction
                    .to()
                    .map_or_else(String::new, |to| format!("{to:#x}")),
                transaction.nonce().to_string(),
                transaction.input().to_string(),
                "true".to_owned(),
                block.logs_of(index).count().to_string(),
                block.number().to_string(),
            ]
        })
        .collect()
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
    let mut schema_exists = connection
        .prepare("SELECT count(*) FROM information_schema.schemata WHERE schema_name = ?")
        .expect("prepare");
    let schemas: u64 = schema_exists
        .query_row([CHAIN], |row| row.get(0))
        .expect("list schemas");
    if schemas == 0 {
        return stored;
    }
    // The chain's tables live in the database schema named for it.
    connection
        .execute_batch(&format!("USE {CHAIN}"))
        .expect("use the chain's schema");
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
