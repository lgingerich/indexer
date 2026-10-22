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

// The crate denies `expect`/`unwrap` to keep production paths honest; tests are allowed
// them per the repository test style, since a failed expectation there means the fixture
// or setup is wrong.
#![expect(clippy::expect_used)]

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

/// A chain other than the store's, whose rows a test expects left alone.
fn other() -> ChainId {
    ChainId::new("ethereum")
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

/// A wei price above `u64::MAX` survives the round trip as an exact number, which is the
/// whole reason those columns are not 64-bit.
pub(crate) async fn a_wei_price_above_u64_max_is_not_truncated<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    commit(&mut sink, every_kind(&harness.chain())).await;

    let read = harness
        .query(
            "SELECT CAST(gas_price AS VARCHAR), CAST(gas_price + gas_price AS VARCHAR) \
             FROM transactions",
        )
        .await;
    let expected = u128::from(u64::MAX) + 1;
    assert_eq!(text(&read[0][0]), expected.to_string());
    assert_eq!(
        text(&read[0][1]),
        (expected * 2).to_string(),
        "it adds exactly"
    );
    harness.remove().await;
}

/// A block's wide row is written whole, in the order the generated DDL declares. The
/// header is the schema, so this checks the two agree end to end.
pub(crate) async fn a_wide_block_row_writes_every_column<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    commit(&mut sink, every_kind(&harness.chain())).await;

    let declared = texts(
        &mut harness,
        "SELECT CAST(column_name AS VARCHAR) FROM information_schema.columns \
         WHERE table_schema = current_schema() AND table_name = 'blocks' \
         ORDER BY ordinal_position",
    )
    .await;
    let header: Vec<String> = datasets()
        .dataset(Table::Block)
        .columns
        .iter()
        .map(|column| column.name.to_string())
        .collect();
    assert_eq!(declared, header, "the DDL and the header must agree");

    let row = harness
        .query(
            "SELECT number, hash, parent_hash, timestamp, gas_limit, gas_used, \
             transaction_count FROM blocks",
        )
        .await
        .remove(0);
    assert_eq!(uint(&row[0]), 100);
    assert_eq!(text(&row[1]), format!("{:#x}", hash(0x01)));
    assert_eq!(text(&row[2]), format!("{:#x}", hash(0x02)));
    assert_eq!(row[3], Value::Timestamp(1_700_000_000));
    assert_eq!(uint(&row[4]), 30_000_000);
    assert_eq!(uint(&row[5]), 21_000);
    assert_eq!(uint(&row[6]), 1);
    harness.remove().await;
}

/// An absent optional field is SQL `NULL`, not an empty string and not a zero.
/// `withdrawals_root` is `None` on a pre-merge block, so a consumer filtering
/// `WHERE withdrawals_root IS NOT NULL` has to get exactly those blocks. The fixture block
/// leaves every optional field absent, so every nullable column is checked.
pub(crate) async fn an_absent_optional_column_is_null<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    commit(&mut sink, every_kind(&harness.chain())).await;

    let schema = datasets();
    let nullable: Vec<&str> = schema
        .dataset(Table::Block)
        .columns
        .iter()
        .filter(|column| column.nullable)
        .map(|column| column.name.as_ref())
        .collect();
    assert!(nullable.contains(&"withdrawals_root"), "{nullable:?}");
    for name in nullable {
        let present = harness
            .query(&format!(
                "SELECT count(*) FROM blocks WHERE \"{name}\" IS NOT NULL"
            ))
            .await;
        assert_eq!(
            present[0][0],
            Value::Int(0),
            "the fixture block has no {name}"
        );
    }
    harness.remove().await;
}

/// Nothing is durable before the flush, in any table, and everything buffered is written
/// and gone from the buffer after it.
pub(crate) async fn nothing_is_durable_until_the_flush<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let envelopes = every_kind(&harness.chain());
    let buffered = envelopes.len();
    buffer(&mut sink, envelopes).await;
    assert_eq!(sink.buffered(), buffered, "buffered but not written");
    assert!(sink.buffered_bytes() > 0, "the drain sees what is buffered");
    for table in Table::ALL {
        assert_eq!(
            row_count(&mut harness, table.name()).await,
            0,
            "{table} before the flush"
        );
    }

    sink.flush().await.expect("batch flushes");
    assert_eq!(sink.buffered(), 0, "the buffers are cleared");
    assert_eq!(sink.buffered_bytes(), 0);
    for table in Table::ALL {
        assert_eq!(
            row_count(&mut harness, table.name()).await,
            one_unless_decoded(table),
            "{table} after the flush"
        );
    }
    harness.remove().await;
}

/// A table failing late in a flush rolls back the tables written before it and keeps the
/// batch, and the retry writes every row once.
pub(crate) async fn a_late_table_failure_rolls_back_and_a_retry_writes_each_row_once<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let envelopes = every_kind(&harness.chain());
    let buffered = envelopes.len();
    buffer(&mut sink, envelopes).await;
    harness.execute("DROP TABLE reorgs").await;

    assert_failed(sink.flush().await, Operation::Merge, "reorgs");
    assert_eq!(sink.buffered(), buffered, "failed batch stays buffered");
    for (table, rows) in block_tables(&mut harness).await {
        assert_eq!(rows, 0, "{table} rolls back");
    }

    harness.execute(&reorg_ddl::<H>()).await;
    sink.flush()
        .await
        .expect("retry commits the original batch");
    sink.flush().await.expect("empty flush is a no-op");
    assert_eq!(sink.buffered(), 0);
    for table in Table::ALL {
        assert_eq!(
            row_count(&mut harness, table.name()).await,
            one_unless_decoded(table),
            "{table} after the retry"
        );
    }
    harness.remove().await;
}

/// A constraint the engine checks only at the merge fails the flush there, keeps the
/// batch, and leaves the table empty; the retry writes the whole batch.
pub(crate) async fn a_deferred_constraint_failure_keeps_the_batch_and_rolls_back<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let height = format!("\"height\" {}", Sql::<H>::type_name(ColumnType::Uint));
    let constrained = reorg_ddl::<H>().replace(&height, &format!("{height} CHECK (height < 1)"));
    harness
        .execute(&format!("DROP TABLE reorgs; {constrained}"))
        .await;
    let chain = harness.chain();
    buffer(
        &mut sink,
        (0..2).map(|height| {
            Envelope::new(
                chain.clone(),
                Event::Reorg(Reorg {
                    height,
                    new_head_hash: hash(u8::try_from(height).expect("heights 0 and 1")),
                    orphaned_hashes: vec![],
                }),
            )
        }),
    )
    .await;

    assert_failed(sink.flush().await, Operation::Merge, "reorgs");
    assert_eq!(row_count(&mut harness, "reorgs").await, 0);
    assert_eq!(sink.buffered(), 2);

    harness
        .execute(&format!("DROP TABLE reorgs; {}", reorg_ddl::<H>()))
        .await;
    sink.flush().await.expect("retry the whole batch");
    assert_eq!(
        uints(&mut harness, "SELECT height FROM reorgs ORDER BY height").await,
        [0, 1]
    );
    assert_eq!(sink.buffered(), 0);
    harness.remove().await;
}

/// A batch that holds a key twice keeps its last copy, a later flush of the same key
/// updates the stored row rather than appending a second, and a reorg replaces the
/// orphaned branch's row with the replacement's.
pub(crate) async fn a_replay_updates_the_row_and_a_reorg_replaces_the_branch<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let chain = harness.chain();
    let orphaned = Log {
        log_index: 0,
        transaction_hash: TxHash::from([0x11; 32]),
        block_number: 100,
        block_hash: hash(0xaa),
        block_timestamp: 1,
        ..Log::default()
    };
    let log = |log: &Log, block_timestamp: u64| {
        let mut log = log.clone();
        log.block_timestamp = block_timestamp;
        Envelope::new(chain.clone(), Event::Log(Box::new(log)))
    };
    commit(&mut sink, [log(&orphaned, 1), log(&orphaned, 5)]).await;
    assert_eq!(
        row_count(&mut harness, "logs").await,
        1,
        "a repeated key in one batch is one row"
    );
    assert_eq!(
        log_timestamp(&mut harness).await,
        5,
        "the later copy in the batch wins"
    );

    // The same key in a later flush updates the stored row, not appends a second — the
    // merge's `DO UPDATE` branch, which a first flush into an empty table misses.
    commit(&mut sink, [log(&orphaned, 7)]).await;
    assert_eq!(
        row_count(&mut harness, "logs").await,
        1,
        "a replay never appends a second"
    );
    assert_eq!(
        log_timestamp(&mut harness).await,
        7,
        "the replay's value replaces the stored one"
    );

    // The reorg deletes the stored branch; the replacement is its own key.
    let mut replacement = orphaned;
    replacement.block_hash = hash(0xbb);
    commit(&mut sink, [reorg(&chain, &[0xaa]), log(&replacement, 9)]).await;
    assert_eq!(
        texts(&mut harness, "SELECT block_hash FROM logs").await,
        [format!("{:#x}", hash(0xbb))],
        "only the replacement remains"
    );
    assert_eq!(log_timestamp(&mut harness).await, 9);
    harness.remove().await;
}

/// A reorg deletes a committed block's rows from every table that belongs to a block, the
/// ledger included, and keeps itself as the record of the retraction.
pub(crate) async fn a_reorg_deletes_a_committed_block_from_every_table<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let chain = harness.chain();
    commit(&mut sink, every_kind(&chain)).await;
    commit(&mut sink, every_kind(&other())).await;

    commit(&mut sink, [reorg(&chain, &[0x01])]).await;

    for (table, rows) in block_tables(&mut harness).await {
        let chains = texts(&mut harness, &format!("SELECT chain FROM \"{table}\"")).await;
        let expected = if table == Table::Decoded {
            Vec::new()
        } else {
            vec![other().as_str().to_owned()]
        };
        assert_eq!(
            chains, expected,
            "{table} keeps nothing of the orphaned block, and the other chain's row"
        );
        assert_eq!(rows, one_unless_decoded(table), "{table}");
    }
    assert_eq!(
        texts(
            &mut harness,
            &format!(
                "SELECT new_head_hash FROM reorgs WHERE chain = '{chain}' ORDER BY new_head_hash"
            )
        )
        .await,
        [format!("{:#x}", hash(0x55)), format!("{:#x}", hash(0x56))],
        "the fixture marker and this one"
    );
    assert_eq!(
        row_count(&mut harness, "reorgs").await,
        3,
        "and the other chain's marker"
    );
    harness.remove().await;
}

/// An orphaned block still in the buffer is dropped before it is ever written.
pub(crate) async fn a_reorg_drops_an_orphaned_block_from_the_same_batch<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let chain = harness.chain();
    buffer(&mut sink, every_kind(&chain)).await;
    sink.publish(reorg(&chain, &[0x01]))
        .await
        .expect("reorg buffers");
    assert_eq!(
        sink.buffered(),
        2,
        "only the two reorg markers stay buffered"
    );
    sink.flush().await.expect("batch commits");

    for (table, rows) in block_tables(&mut harness).await {
        assert_eq!(rows, 0, "{table} never sees the orphaned block");
    }
    assert_eq!(row_count(&mut harness, "reorgs").await, 2);
    harness.remove().await;
}

/// A block orphaned and then canonical again is stored, whether its return arrives in a
/// later batch or the same one: the deletes run before the rows are written.
pub(crate) async fn a_block_that_returns_after_a_reorg_is_stored<H: Harness>() {
    let cases: [(&str, fn(&ChainId) -> Vec<Vec<Envelope>>); 2] = [
        ("across batches", |chain| {
            vec![
                vec![block_log(chain, 0xaa)],
                vec![reorg(chain, &[0xaa]), block_log(chain, 0xbb)],
                vec![reorg(chain, &[0xbb]), block_log(chain, 0xaa)],
            ]
        }),
        (
            "within one batch, with the block already committed",
            |chain| {
                vec![
                    vec![block_log(chain, 0xaa)],
                    vec![
                        reorg(chain, &[0xaa]),
                        block_log(chain, 0xbb),
                        reorg(chain, &[0xbb]),
                        block_log(chain, 0xaa),
                    ],
                ]
            },
        ),
    ];
    for (case, batches) in cases {
        let (mut sink, mut harness) = H::open(datasets()).await;
        for batch in batches(&harness.chain()) {
            commit(&mut sink, batch).await;
        }
        assert_eq!(
            texts(
                &mut harness,
                "SELECT block_hash FROM logs ORDER BY block_hash"
            )
            .await,
            [format!("{:#x}", hash(0xaa))],
            "{case}"
        );
        harness.remove().await;
    }
}

/// The one log of block `block` at height 100.
fn block_log(chain: &ChainId, block: u8) -> Envelope {
    Envelope::new(
        chain.clone(),
        Event::Log(Box::new(Log {
            log_index: 0,
            transaction_hash: TxHash::from([0x11; 32]),
            block_number: 100,
            block_hash: hash(block),
            ..Log::default()
        })),
    )
}

/// A failed commit keeps the reorg's deletions with its rows, and the retry applies them.
pub(crate) async fn a_failed_reorg_commit_keeps_its_deletes_for_the_retry<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let chain = harness.chain();
    commit(&mut sink, every_kind(&chain)).await;
    sink.publish(reorg(&chain, &[0x01]))
        .await
        .expect("reorg buffers");
    harness.execute("DROP TABLE reorgs").await;
    assert_failed(sink.flush().await, Operation::Merge, "reorgs");
    for (table, rows) in block_tables(&mut harness).await {
        assert_eq!(
            rows,
            one_unless_decoded(table),
            "{table}'s delete rolled back"
        );
    }

    harness.execute(&reorg_ddl::<H>()).await;
    sink.flush().await.expect("the retry commits");
    for (table, rows) in block_tables(&mut harness).await {
        assert_eq!(rows, 0, "{table} is retracted on the retry");
    }
    assert_eq!(
        texts(&mut harness, "SELECT new_head_hash FROM reorgs").await,
        [format!("{:#x}", hash(0x56))],
        "the retry records the retraction"
    );
    harness.remove().await;
}

/// A rewind deletes every block above the height from every table, the ledger included,
/// and leaves the blocks at or below it and other chains' rows alone.
pub(crate) async fn a_rewind_deletes_every_block_above_the_height<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    // A store rewinds the chain it was opened for.
    let chain = harness.chain();
    for name in [&chain, &other()] {
        for height in 1..=5 {
            let meta = accepted(height);
            for event in [
                Event::Block(Box::new(Block {
                    number: height,
                    hash: meta.hash,
                    ..Block::default()
                })),
                Event::AcceptedBlock(meta),
            ] {
                sink.publish(Envelope::new(name.clone(), event))
                    .await
                    .expect("buffer");
            }
        }
    }
    sink.flush().await.expect("flush");

    sink.rewind(3).await.expect("rewind");

    for (table, height) in [("blocks", "number"), ("accepted_blocks", "height")] {
        for (name, expected) in [(&chain, &[1, 2, 3][..]), (&other(), &[1, 2, 3, 4, 5])] {
            assert_eq!(
                uints(
                    &mut harness,
                    &format!(
                        "SELECT {height} FROM {table} WHERE chain = '{name}' ORDER BY {height}"
                    )
                )
                .await,
                expected,
                "{table} on {name}"
            );
        }
    }
    harness.remove().await;
}

/// A store more than the ledger's window ahead, the only kind that is rewound, has pruned
/// the ledger rows of the blocks just above the height; the rewind deletes those blocks'
/// rows all the same.
pub(crate) async fn a_rewind_deletes_blocks_whose_ledger_rows_were_pruned<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let chain = harness.chain();
    let tip = BlockMeta {
        height: LEDGER_WINDOW as u64 + 10,
        hash: B256::repeat_byte(0xee),
        parent_hash: B256::repeat_byte(0xed),
        timestamp: 1_700_000_000,
    };
    for meta in (1..=5).map(accepted).chain([tip]) {
        commit(
            &mut sink,
            [
                Event::Block(Box::new(Block {
                    number: meta.height,
                    hash: meta.hash,
                    ..Block::default()
                })),
                Event::AcceptedBlock(meta),
            ]
            .map(|event| Envelope::new(chain.clone(), event)),
        )
        .await;
    }
    assert_eq!(
        uints(&mut harness, "SELECT height FROM accepted_blocks").await,
        [tip.height],
        "the flush at the tip pruned the ledger below its window"
    );

    sink.rewind(3).await.expect("rewind");

    assert_eq!(
        uints(&mut harness, "SELECT number FROM blocks ORDER BY number").await,
        [1, 2, 3],
        "no block above the height is left"
    );
    harness.remove().await;
}

/// The ledger reads back the newest unorphaned accepted blocks of one chain, oldest first,
/// and deletes nothing.
pub(crate) async fn the_ledger_reads_back_the_newest_canonical_window<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let chain = harness.chain();
    assert!(sink.ledger(&chain, 3).await.expect("empty").is_empty());
    let mut envelopes: Vec<Envelope> = (1..=10)
        .map(|height| Envelope::new(chain.clone(), Event::AcceptedBlock(accepted(height))))
        .collect();
    envelopes.push(Envelope::new(other(), Event::AcceptedBlock(accepted(1))));
    // Block 10 was orphaned and deleted, so the tip falls back to 9.
    envelopes.push(Envelope::new(
        chain.clone(),
        Event::Reorg(Reorg {
            height: 10,
            new_head_hash: hash(0xee),
            orphaned_hashes: vec![hash(10)],
        }),
    ));
    commit(&mut sink, envelopes).await;

    let ledger = sink.ledger(&chain, 3).await.expect("read back");
    assert_eq!(ledger, [accepted(7), accepted(8), accepted(9)]);
    assert_eq!(
        uints(
            &mut harness,
            &format!("SELECT height FROM accepted_blocks WHERE chain = '{chain}' ORDER BY height")
        )
        .await,
        [1, 2, 3, 4, 5, 6, 7, 8, 9],
        "only the orphaned row was deleted"
    );
    assert_eq!(
        sink.ledger(&other(), 3).await.expect("read back"),
        [accepted(1)],
        "another chain's ledger is untouched"
    );
    harness.remove().await;
}

/// A flush deletes a chain's ledger rows more than the window below the newest block it
/// accepts there, and leaves the other chains' rows and the window itself.
pub(crate) async fn a_flush_prunes_the_ledger_below_the_window<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let chain = harness.chain();
    let block = |height: u64| BlockMeta {
        height,
        hash: B256::from(U256::from(height)),
        parent_hash: B256::from(U256::from(height - 1)),
        timestamp: 1_700_000_000 + height,
    };
    let mut envelopes: Vec<Envelope> = (1..=3)
        .map(|height| Envelope::new(chain.clone(), Event::AcceptedBlock(block(height))))
        .collect();
    envelopes.push(Envelope::new(other(), Event::AcceptedBlock(block(1))));
    commit(&mut sink, envelopes).await;
    let tip = LEDGER_WINDOW as u64 + 2;
    commit(
        &mut sink,
        [Envelope::new(
            chain.clone(),
            Event::AcceptedBlock(block(tip)),
        )],
    )
    .await;

    let heights = |chain: &ChainId| {
        format!("SELECT height FROM accepted_blocks WHERE chain = '{chain}' ORDER BY height")
    };
    assert_eq!(
        uints(&mut harness, &heights(&chain)).await,
        [2, 3, tip],
        "the row more than the window below the tip was deleted"
    );
    assert_eq!(
        uints(&mut harness, &heights(&other())).await,
        [1],
        "another chain's ledger is untouched"
    );
    harness.remove().await;
}

/// A stored contract reads back scoped to its chain, and is deleted once a reorg orphans
/// its creating block.
pub(crate) async fn stored_contracts_read_back_and_orphaned_ones_are_deleted<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let chain = harness.chain();
    let contract = contract();
    commit(
        &mut sink,
        [&chain, &other()]
            .map(|name| Envelope::new(name.clone(), Event::Contract(Box::new(contract.clone())))),
    )
    .await;
    assert_eq!(
        sink.contracts(&chain).await.expect("read back"),
        [StoredContract {
            protocol: contract.protocol,
            name: contract.name,
            address: contract.address,
            block_hash: contract.block_hash,
            block_number: contract.block_number,
            log_index: contract.log_index,
        }]
    );

    commit(&mut sink, [reorg(&chain, &[0x01])]).await;
    assert!(sink.contracts(&chain).await.expect("read back").is_empty());
    assert_eq!(
        row_count(&mut harness, "contracts").await,
        1,
        "the other chain's contract stays"
    );
    harness.remove().await;
}

/// A decoded record lands in `decoded_logs` and in its event's own table, where every
/// argument is a typed column: wide integers exact and signed, so they sum without a cast.
/// A reorg deletes the typed row with the rest of its block.
pub(crate) async fn a_decoded_record_lands_in_its_typed_event_table<H: Harness>() {
    let (mut sink, mut harness) = H::open(crate::sink::fixtures::schema()).await;
    let chain = harness.chain();
    let swap = crate::sink::fixtures::decoded_swap();
    let block = swap.block_hash;
    commit(
        &mut sink,
        [Envelope::new(chain.clone(), Event::Decoded(Box::new(swap)))],
    )
    .await;

    assert_eq!(row_count(&mut harness, "decoded_logs").await, 1);
    assert_eq!(row_count(&mut harness, "uniswap_v3_pool_swap").await, 1);
    let row = harness
        .query(
            "SELECT CAST(amount0 AS VARCHAR), CAST(amount1 AS VARCHAR), \
             CAST(sqrt_price_x96 AS VARCHAR), CAST(liquidity AS VARCHAR), tick, sender \
             FROM uniswap_v3_pool_swap",
        )
        .await
        .remove(0);
    assert_eq!(text(&row[0]), "-3180585820646654");
    assert_eq!(text(&row[1]), "8586564");
    assert_eq!(text(&row[2]), "4115542941155561242646778");
    assert_eq!(text(&row[3]), "1367693693161775005");
    assert_eq!(row[4], Value::Int(-197_317));
    assert_eq!(text(&row[5]), "0x6ff5693b99212da76ad316178a184ab56d299b43");
    assert_eq!(
        texts(
            &mut harness,
            "SELECT CAST(sum(sqrt_price_x96) + sum(liquidity) AS VARCHAR) \
             FROM uniswap_v3_pool_swap"
        )
        .await,
        ["4115544308849254404421783"],
        "exact, not a float"
    );

    commit(
        &mut sink,
        [Envelope::new(
            chain,
            Event::Reorg(Reorg {
                height: 1,
                new_head_hash: hash(0x56),
                orphaned_hashes: vec![block],
            }),
        )],
    )
    .await;
    assert_eq!(row_count(&mut harness, "uniswap_v3_pool_swap").await, 0);
    assert_eq!(row_count(&mut harness, "decoded_logs").await, 0);
    harness.remove().await;
}

/// A store routed only the event tables creates and fills those and the bookkeeping
/// tables: a decoded record's `decoded_logs` row, and a raw log, are not kept.
pub(crate) async fn a_store_writes_only_the_tables_routed_to_it<H: Harness>() {
    let schema = crate::sink::fixtures::schema();
    let (events, mut expected): (HashSet<TableId>, BTreeSet<String>) = schema
        .tables()
        .iter()
        .filter(|def| matches!(def.id, TableId::Event(_)))
        .map(|def| (def.id, def.name.clone()))
        .unzip();
    expected.extend(["accepted_blocks", "contracts", "reorgs"].map(str::to_owned));
    let (mut sink, mut harness) = H::open(Arc::new(schema.select(&events))).await;
    let chain = harness.chain();
    let swap = crate::sink::fixtures::decoded_swap();
    let log = Log {
        block_number: swap.block_number,
        block_hash: swap.block_hash,
        ..Log::default()
    };
    commit(
        &mut sink,
        [
            Envelope::new(chain.clone(), Event::Decoded(Box::new(swap))),
            Envelope::new(chain, Event::Log(Box::new(log))),
        ],
    )
    .await;

    let created: BTreeSet<String> = texts(
        &mut harness,
        "SELECT CAST(table_name AS VARCHAR) FROM information_schema.tables \
         WHERE table_schema = current_schema()",
    )
    .await
    .into_iter()
    .collect();
    assert_eq!(created, expected, "only the routed and bookkeeping tables");
    assert_eq!(row_count(&mut harness, "uniswap_v3_pool_swap").await, 1);
    for table in ["accepted_blocks", "reorgs", "contracts"] {
        assert_eq!(row_count(&mut harness, table).await, 0, "{table} is empty");
    }
    harness.remove().await;
}
