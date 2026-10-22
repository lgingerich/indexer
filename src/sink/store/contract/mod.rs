//! What every SQL store must do, written once and run against each engine.
//!
//! Each test here is generic over a [`Harness`]: how an engine opens a store in a
//! database schema of its own, and a second connection of the test's to read that schema
//! back. An engine's own tests run them all with [`contract_tests!`], so the two engines
//! cannot drift apart: a test added here runs on both, and one left out of the macro's
//! list is dead code, which the lints refuse.
//!
//! The SQL a test reads back with is what both engines accept — `CAST(… AS VARCHAR)`,
//! `information_schema` — and the values come back as the store's own [`Value`]s, a
//! `UBIGINT` from one engine and a `NUMERIC` from the other both read with [`uint`].
//!
//! This module holds the harness, the macro, and the fixtures and readers every test
//! shares; the tests are in `rows`, for what a store writes and what a flush keeps, and
//! `reorg`, for retracting, rewinding, and reading back.

// The crate denies `expect`/`unwrap` to keep production paths honest; tests are allowed
// them per the repository test style, since a failed expectation there means the fixture
// or setup is wrong.
#![expect(clippy::expect_used)]

mod reorg;
mod rows;

pub(crate) use reorg::*;
pub(crate) use rows::*;

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use alloy_primitives::{Address, B256, TxHash, U256};

use crate::sink::sql::{self, Dialect};
use crate::sink::store::{Engine, SqlStore};
use crate::sink::table::{ColumnType, Schema, Table, TableId, Value};
use crate::sink::{
    EnvelopeSink as _, Operation, SinkError, Store as _, StoreError, StoredContract,
};
use crate::window::LEDGER_WINDOW;
use crate::wire::datasets::evm::{Block, Contract, Log, Transaction};
use crate::wire::envelope::{BlockMeta, ChainId, Envelope, Event, Reorg};

/// How an engine's tests open a store and look inside it.
pub(crate) trait Harness: Sized {
    /// The engine the store runs on.
    type Engine: Engine + 'static;

    /// Opens a store of `schema`'s tables in a database schema no other test uses, and
    /// this harness's own connection to it, with that schema as its default.
    async fn open(schema: Arc<Schema>) -> (SqlStore<Self::Engine>, Self);

    /// The chain the store was opened for: the one it restores and rewinds.
    fn chain(&self) -> ChainId;

    /// The rows `sql` selects, read on the harness's connection.
    async fn query(&mut self, sql: &str) -> Vec<Vec<Value>>;

    /// Runs `sql` on the harness's connection.
    async fn execute(&mut self, sql: &str);

    /// Drops what [`open`](Self::open) created.
    async fn remove(self);
}

/// The SQL the harness's engine speaks.
type Sql<H> = <<H as Harness>::Engine as Engine>::Dialect;

/// Runs every contract test against the engine `$harness` opens, each a `#[tokio::test]`
/// with the attributes given after it.
macro_rules! contract_tests {
    ($harness:ty $(, #[$attr:meta])*) => {
        $crate::sink::store::contract::contract_tests!(@each $harness, [$(#[$attr])*],
            a_wei_price_above_u64_max_is_not_truncated,
            a_wide_block_row_writes_every_column,
            an_absent_optional_column_is_null,
            nothing_is_durable_until_the_flush,
            a_late_table_failure_rolls_back_and_a_retry_writes_each_row_once,
            a_deferred_constraint_failure_keeps_the_batch_and_rolls_back,
            a_replay_updates_the_row_and_a_reorg_replaces_the_branch,
            a_reorg_deletes_a_committed_block_from_every_table,
            a_reorg_drops_an_orphaned_block_from_the_same_batch,
            a_block_that_returns_after_a_reorg_is_stored,
            a_failed_reorg_commit_keeps_its_deletes_for_the_retry,
            a_rewind_deletes_every_block_above_the_height,
            a_rewind_deletes_blocks_whose_ledger_rows_were_pruned,
            the_ledger_reads_back_the_newest_canonical_window,
            a_flush_prunes_the_ledger_below_the_window,
            stored_contracts_read_back_and_orphaned_ones_are_deleted,
            a_decoded_record_lands_in_its_typed_event_table,
            a_store_writes_only_the_tables_routed_to_it,
        );
    };
    (@each $harness:ty, $attrs:tt, $($name:ident),* $(,)?) => {
        $($crate::sink::store::contract::contract_tests!(@one $harness, $attrs, $name);)*
    };
    (@one $harness:ty, [$(#[$attr:meta])*], $name:ident) => {
        #[tokio::test]
        $(#[$attr])*
        async fn $name() {
            $crate::sink::store::contract::$name::<$harness>().await;
        }
    };
}
pub(crate) use contract_tests;

/// The dataset tables alone.
pub(crate) fn datasets() -> Arc<Schema> {
    Arc::new(Schema::new().expect("the dataset tables"))
}

fn hash(byte: u8) -> B256 {
    B256::from([byte; 32])
}

/// One envelope of every kind on `chain`, so a flush exercises every table at once.
///
/// Values are distinct per field, so a column-order or column-name skew shows up as a
/// wrong value rather than as a plausible-looking row.
pub(crate) fn every_kind(chain: &ChainId) -> Vec<Envelope> {
    [
        Event::Block(Box::new(Block {
            number: 100,
            hash: hash(0x01),
            parent_hash: hash(0x02),
            timestamp: 1_700_000_000,
            gas_limit: 30_000_000,
            gas_used: 21_000,
            transaction_count: 1,
            ..Block::default()
        })),
        Event::Transaction(Box::new(Transaction {
            hash: TxHash::from([0x11; 32]),
            transaction_index: 3,
            from: Address::from([0x22; 20]),
            value: U256::from(1_000),
            // A wei price above `u64::MAX`, which is ~18.4 ETH — so this also checks the
            // 128-bit columns are not truncated to 64.
            gas_price: Some(u128::from(u64::MAX) + 1),
            receipt_status: true,
            receipt_gas_used: 21_000,
            log_count: 1,
            block_number: 100,
            block_hash: hash(0x01),
            ..Transaction::default()
        })),
        Event::Log(Box::new(Log {
            log_index: 7,
            transaction_hash: TxHash::from([0x11; 32]),
            transaction_index: 3,
            address: Address::from([0x33; 20]),
            topic0: Some(hash(0x44)),
            block_number: 100,
            block_hash: hash(0x01),
            block_timestamp: 1_700_000_000,
            ..Log::default()
        })),
        Event::Contract(Box::new(contract())),
        Event::Reorg(Reorg {
            height: 100,
            new_head_hash: hash(0x55),
            orphaned_hashes: vec![hash(0x66)],
        }),
        Event::AcceptedBlock(BlockMeta {
            height: 100,
            hash: hash(0x01),
            parent_hash: hash(0x02),
            timestamp: 1_700_000_000,
        }),
    ]
    .map(|event| Envelope::new(chain.clone(), event))
    .into()
}

/// The contract `every_kind` discovers, in its block.
fn contract() -> Contract {
    Contract {
        protocol: "uniswap_v3".to_owned(),
        name: "UniswapV3Pool".to_owned(),
        address: Address::from([0x77; 20]),
        factory_address: Address::from([0x33; 20]),
        transaction_hash: TxHash::from([0x11; 32]),
        transaction_index: 3,
        log_index: 7,
        block_number: 100,
        block_hash: hash(0x01),
        block_timestamp: 1_700_000_000,
    }
}

/// A reorg on `chain` naming `orphaned` blocks, by their fixture hash bytes.
fn reorg(chain: &ChainId, orphaned: &[u8]) -> Envelope {
    Envelope::new(
        chain.clone(),
        Event::Reorg(Reorg {
            height: 100,
            // Not the fixture marker's head, so this marker is its own row.
            new_head_hash: hash(0x56),
            orphaned_hashes: orphaned.iter().map(|byte| hash(*byte)).collect(),
        }),
    )
}

fn accepted(height: u64) -> BlockMeta {
    BlockMeta {
        height,
        hash: hash(u8::try_from(height).expect("small heights")),
        parent_hash: hash(u8::try_from(height - 1).expect("small heights")),
        timestamp: 1_700_000_000 + height,
    }
}

/// Hands `envelopes` to the store without committing them.
async fn buffer<E: Engine + 'static>(
    sink: &mut SqlStore<E>,
    envelopes: impl IntoIterator<Item = Envelope>,
) {
    for envelope in envelopes {
        sink.publish(envelope).await.expect("row buffers");
    }
}

/// Hands `envelopes` to the store and commits them.
pub(crate) async fn commit<E: Engine + 'static>(
    sink: &mut SqlStore<E>,
    envelopes: impl IntoIterator<Item = Envelope>,
) {
    buffer(sink, envelopes).await;
    sink.flush().await.expect("batch flushes");
}

/// Asserts `flushed` failed in the engine while the store was doing `operation` to
/// `table`.
fn assert_failed(flushed: Result<(), SinkError>, operation: Operation, table: &str) {
    match flushed {
        Err(SinkError::Store(StoreError::Engine {
            operation: found,
            target,
            ..
        })) => assert_eq!((found, target.as_deref()), (operation, Some(table))),
        other => panic!("a {operation:?} failure on {table}, not {other:?}"),
    }
}

/// How many rows `every_kind` writes to `table`: one to each, as no fixture event is
/// decoded.
pub(crate) fn one_unless_decoded(table: Table) -> i64 {
    i64::from(table != Table::Decoded)
}

/// An unsigned integer read back: a `UBIGINT` from one engine, a `NUMERIC` from the other.
fn uint(value: &Value) -> u64 {
    match value {
        Value::Uint(number) => *number,
        Value::Int(number) => u64::try_from(*number).expect("not negative"),
        Value::BigInt {
            negative: false,
            magnitude,
        } => u64::try_from(*magnitude).expect("fits 64 bits"),
        other => panic!("an unsigned integer, not {other:?}"),
    }
}

fn text(value: &Value) -> String {
    match value {
        Value::Text(text) => text.clone(),
        other => panic!("text, not {other:?}"),
    }
}

/// The first column of every row `sql` selects, as text.
async fn texts<H: Harness>(harness: &mut H, sql: &str) -> Vec<String> {
    harness
        .query(sql)
        .await
        .iter()
        .map(|row| text(&row[0]))
        .collect()
}

/// The first column of every row `sql` selects, as unsigned integers.
async fn uints<H: Harness>(harness: &mut H, sql: &str) -> Vec<u64> {
    harness
        .query(sql)
        .await
        .iter()
        .map(|row| uint(&row[0]))
        .collect()
}

async fn row_count<H: Harness>(harness: &mut H, table: &str) -> i64 {
    match harness
        .query(&format!("SELECT count(*) FROM \"{table}\""))
        .await[0][0]
    {
        Value::Int(count) => count,
        ref other => panic!("a count, not {other:?}"),
    }
}

/// Every table but `reorgs`, with how many rows it holds.
async fn block_tables<H: Harness>(harness: &mut H) -> Vec<(Table, i64)> {
    let mut counts = Vec::new();
    for table in Table::ALL {
        if table != Table::Reorg {
            counts.push((table, row_count(harness, table.name()).await));
        }
    }
    counts
}

/// The `reorgs` table's DDL, to recreate it after a test drops it.
fn reorg_ddl<H: Harness>() -> String {
    sql::create_table::<Sql<H>>(datasets().dataset(Table::Reorg))
}

async fn log_timestamp<H: Harness>(harness: &mut H) -> u64 {
    match harness.query("SELECT block_timestamp FROM logs").await[0][0] {
        Value::Timestamp(seconds) => seconds,
        ref other => panic!("a timestamp, not {other:?}"),
    }
}
