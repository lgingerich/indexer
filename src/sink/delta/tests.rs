//! The lake against a local directory: what it holds after commits, reorgs, and a
//! commit that stopped part-way.

// The crate denies `expect`/`unwrap` to keep production paths honest; tests are allowed
// them per the repository test style, since a failed expectation there means the fixture
// or setup is wrong.
#![expect(clippy::expect_used)]

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use alloy_primitives::{B256, U256};
use deltalake::arrow::util::display::{ArrayFormatter, FormatOptions};
use deltalake::datafusion::prelude::ident;
use deltalake::kernel::{DataType, StructField};
use deltalake::{DeltaTable, ensure_table_uri};

use super::{DeltaSettings, DeltaSink, in_texts, maintenance, texts, texts_of};
use crate::sink::table::{Schema, Table, TableId};
use crate::sink::{EnvelopeSink, SinkError, Store as _, StoreError, fixtures};
use crate::wire::envelope::{Block, BlockMeta, ChainId, Envelope, Event, Log, Reorg, Transaction};

const CHAIN: &str = "base";

/// A fresh directory for one test's lake, removed when the test's guard drops.
struct Dir(PathBuf);

impl Dir {
    fn new() -> Self {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let path = std::env::temp_dir().join(format!(
            "indexer-delta-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        Self(path)
    }

    /// Settings for a lake in this directory. The sink commits on every flush; how
    /// often that is, the drain decides. A test runs maintenance itself.
    fn settings(&self) -> DeltaSettings {
        DeltaSettings {
            tables: serde_json::from_str(r#"["contract_events"]"#).expect("tables"),
            uri: self.0.display().to_string(),
            commit_interval_secs: 30,
            max_buffer_bytes: usize::MAX,
            maintenance: false,
            vacuum_retention_hours: 168,
            storage: std::collections::BTreeMap::new(),
        }
    }

    /// Where the lake keeps `table` of the test chain.
    fn table(&self, table: &str) -> String {
        format!("{}/{CHAIN}/{table}", self.0.display())
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn open(dir: &Dir) -> DeltaSink {
    DeltaSink::open(
        &dir.settings(),
        Arc::new(Schema::new().expect("the dataset tables")),
        CHAIN,
    )
    .await
    .expect("the lake opens")
}

/// The block at `height` on `branch`: 0 is the canonical chain, anything else a fork.
fn hash(height: u64, branch: u8) -> B256 {
    let mut bytes = [0; 32];
    bytes[0] = branch;
    bytes[24..].copy_from_slice(&height.to_be_bytes());
    B256::from(bytes)
}

fn hex(hash: B256) -> String {
    format!("{hash:#x}")
}

/// A block's envelopes as the pipeline publishes them: the block, one transaction, its
/// log, and the block's ledger entry last.
fn block(height: u64, branch: u8) -> Vec<Envelope> {
    let block_hash = hash(height, branch);
    let transaction = hash(height, branch.wrapping_add(100));
    let events = [
        Event::Block(Box::new(Block {
            number: height,
            hash: block_hash,
            parent_hash: hash(height.saturating_sub(1), 0),
            timestamp: 1_700_000_000 + height,
            ..Block::default()
        })),
        Event::Transaction(Box::new(Transaction {
            hash: transaction,
            value: U256::MAX,
            gas_price: Some(u128::from(u64::MAX) + 1),
            block_number: height,
            block_hash,
            block_timestamp: 1_700_000_000 + height,
            ..Transaction::default()
        })),
        Event::Log(Box::new(Log {
            log_index: 0,
            transaction_hash: transaction,
            block_number: height,
            block_hash,
            block_timestamp: 1_700_000_000 + height,
            ..Log::default()
        })),
        Event::AcceptedBlock(BlockMeta {
            height,
            hash: block_hash,
            parent_hash: hash(height.saturating_sub(1), 0),
            timestamp: 1_700_000_000 + height,
        }),
    ];
    events
        .into_iter()
        .map(|event| Envelope::new(ChainId::new(CHAIN), event))
        .collect()
}

/// The ledger a restart resumes from, as the lake restores it.
async fn ledger(sink: &mut DeltaSink) -> Vec<BlockMeta> {
    sink.restore().await.expect("the lake restores").ledger
}

/// Hands `envelopes` to the lake without committing them.
async fn buffer(sink: &mut DeltaSink, envelopes: Vec<Envelope>) {
    for envelope in envelopes {
        sink.publish(envelope).await.expect("accepted");
    }
}

/// Hands `envelopes` to the lake and commits them, the ledger entries included: a
/// commit's ledger append runs on past its flush, so this waits for it as the end of a
/// stream does.
async fn publish(sink: &mut DeltaSink, envelopes: Vec<Envelope>) {
    buffer(sink, envelopes).await;
    sink.flush().await.expect("flushed");
    sink.finish().await.expect("the ledger appended");
}

/// The distinct values of `column` the lake holds in `table`.
async fn stored(sink: &DeltaSink, table: Table, column: &str) -> BTreeSet<String> {
    sink.lake(table)
        .read(|frame| frame.select(texts_of(&[column])))
        .await
        .expect("the table reads")
        .iter()
        .flat_map(|batch| {
            texts(batch, 0, "test")
                .expect("text")
                .iter()
                .flatten()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The block hashes every block table holds, which must agree.
async fn blocks(sink: &DeltaSink) -> BTreeSet<String> {
    let held = stored(sink, Table::Block, "hash").await;
    assert_eq!(stored(sink, Table::Transaction, "block_hash").await, held);
    assert_eq!(stored(sink, Table::Log, "block_hash").await, held);
    assert_eq!(stored(sink, Table::AcceptedBlock, "hash").await, held);
    held
}

fn expected(blocks: &[(u64, u8)]) -> BTreeSet<String> {
    blocks
        .iter()
        .map(|&(height, branch)| hex(hash(height, branch)))
        .collect()
}

/// Committed blocks survive a reopen, the ledger comes back oldest first, and wide
/// integers come back exactly.
#[tokio::test]
async fn committed_blocks_survive_a_reopen() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=12 {
        publish(&mut sink, block(height, 0)).await;
    }
    drop(sink);

    let mut sink = open(&dir).await;
    assert_eq!(
        ledger(&mut sink)
            .await
            .iter()
            .map(|block| block.height)
            .collect::<Vec<_>>(),
        (1..=12).collect::<Vec<_>>()
    );
    let heights: Vec<(u64, u8)> = (1..=12).map(|height| (height, 0)).collect();
    assert_eq!(blocks(&sink).await, expected(&heights));
    assert_eq!(
        stored(&sink, Table::Transaction, "value").await,
        BTreeSet::from([U256::MAX.to_string()])
    );
    assert_eq!(
        stored(&sink, Table::Transaction, "gas_price").await,
        BTreeSet::from([(u128::from(u64::MAX) + 1).to_string()])
    );
}

/// Blocks a reorg orphans while still buffered are dropped before they are written,
/// and the retraction is recorded.
#[tokio::test]
async fn a_reorg_of_buffered_blocks_never_reaches_the_lake() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=3 {
        buffer(&mut sink, block(height, 0)).await;
    }
    buffer(
        &mut sink,
        vec![Envelope::new(
            ChainId::new(CHAIN),
            Event::Reorg(Reorg {
                height: 3,
                new_head_hash: hash(3, 1),
                orphaned_hashes: vec![hash(3, 0)],
            }),
        )],
    )
    .await;
    publish(&mut sink, block(3, 1)).await;

    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 1)]));
    assert_eq!(stored(&sink, Table::Reorg, "new_head_hash").await.len(), 1);
}

/// Committed blocks a reorg orphans are deleted from every table, the ledger included,
/// and their replacements take their place.
#[tokio::test]
async fn a_reorg_of_committed_blocks_deletes_them_everywhere() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=12 {
        publish(&mut sink, block(height, 0)).await;
    }
    let mut replacements = vec![reorg(10..=12, 1)];
    for height in 10..=12 {
        replacements.extend(block(height, 1));
    }
    publish(&mut sink, replacements).await;

    let canonical: Vec<(u64, u8)> = (1..=9)
        .map(|height| (height, 0))
        .chain((10..=12).map(|height| (height, 1)))
        .collect();
    assert_eq!(blocks(&sink).await, expected(&canonical));
    drop(sink);

    let mut sink = open(&dir).await;
    let ledger = ledger(&mut sink).await;
    assert_eq!(ledger.last().map(|block| block.hash), Some(hash(12, 1)));
    assert_eq!(blocks(&sink).await, expected(&canonical));
    assert_eq!(retractions(&sink).await, expected(&[(12, 1)]));
}

/// The `reorg` the pipeline publishes when `heights` of the canonical chain are replaced
/// by `branch`: at the lowest orphan's height, naming the new head.
fn reorg(heights: std::ops::RangeInclusive<u64>, branch: u8) -> Envelope {
    Envelope::new(
        ChainId::new(CHAIN),
        Event::Reorg(Reorg {
            height: *heights.start(),
            new_head_hash: hash(*heights.end(), branch),
            orphaned_hashes: heights.rev().map(|height| hash(height, 0)).collect(),
        }),
    )
}

/// The new head of every retraction the lake records.
async fn retractions(sink: &DeltaSink) -> BTreeSet<String> {
    stored(sink, Table::Reorg, "new_head_hash").await
}

/// Appends `envelope`'s row straight to its table, as a commit that stopped part-way
/// would have.
async fn append_only(sink: &mut DeltaSink, envelope: &Envelope) {
    let schema = Schema::new().expect("the dataset tables");
    let row = schema
        .row(&ChainId::new(CHAIN), &envelope.event)
        .expect("a row");
    let position = sink.positions[&row.table().id];
    sink.append_some(position, &[&row]).await.expect("appended");
}

/// A commit that stopped after recording a retraction but before taking its orphans out
/// of the ledger leaves a record of a retraction that did not happen. Opening the lake
/// deletes it, and the restart, still on the orphaned branch, records it once.
#[tokio::test]
async fn opening_deletes_a_retraction_the_ledger_never_saw() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=3 {
        publish(&mut sink, block(height, 0)).await;
    }
    append_only(&mut sink, &reorg(3..=3, 1)).await;
    drop(sink);

    let mut sink = open(&dir).await;
    assert!(retractions(&sink).await.is_empty());
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 0)]));

    let mut replacement = vec![reorg(3..=3, 1)];
    replacement.extend(block(3, 1));
    publish(&mut sink, replacement).await;
    drop(sink);

    let sink = open(&dir).await;
    assert_eq!(retractions(&sink).await, expected(&[(3, 1)]));
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 1)]));
}

/// A commit that stopped after taking the orphans out of the ledger has already recorded
/// the retraction. Opening the lake keeps the record and deletes the orphans' rows, and
/// the restart, resuming below the fork, has nothing to retract again.
#[tokio::test]
async fn a_retraction_out_of_the_ledger_keeps_its_record() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=3 {
        publish(&mut sink, block(height, 0)).await;
    }
    append_only(&mut sink, &reorg(3..=3, 1)).await;
    let position = sink.positions[&TableId::Dataset(Table::AcceptedBlock)];
    let writer = sink.writer.clone();
    sink.lakes[position]
        .delete(&writer, in_texts("hash", &[hex(hash(3, 0))]))
        .await
        .expect("deleted");
    drop(sink);

    let mut sink = open(&dir).await;
    assert_eq!(retractions(&sink).await, expected(&[(3, 1)]));
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0)]));
    assert_eq!(ledger(&mut sink).await.len(), 2);
}

/// A commit that stopped after writing rows but before their ledger entry leaves rows the
/// ledger does not name — above its tip, or at a height it holds another block for.
/// Opening the lake deletes them, so a resumed run writes them once.
#[tokio::test]
async fn opening_repairs_rows_a_stopped_commit_left_behind() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=3 {
        publish(&mut sink, block(height, 0)).await;
    }
    // A stopped commit: rows of a fork at 3 and of 4, without their ledger entries.
    let schema = Schema::new().expect("the dataset tables");
    let chain = ChainId::new(CHAIN);
    let rows: Vec<_> = block(3, 1)
        .into_iter()
        .chain(block(4, 0))
        .filter(|envelope| !matches!(envelope.event, Event::AcceptedBlock(_)))
        .map(|envelope| schema.row(&chain, &envelope.event).expect("a row"))
        .collect();
    for table in [Table::Block, Table::Transaction, Table::Log] {
        let position = sink.positions[&TableId::Dataset(table)];
        let rows: Vec<_> = rows
            .iter()
            .filter(|row| row.table().id == TableId::Dataset(table))
            .collect();
        sink.append_some(position, &rows).await.expect("appended");
    }
    drop(sink);

    let mut sink = open(&dir).await;
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 0)]));
    assert_eq!(ledger(&mut sink).await.len(), 3);
}

/// With no ledger at all, no block row counts: the first commit stopped before its
/// ledger entry, and the run starts over.
#[tokio::test]
async fn opening_with_no_ledger_keeps_no_block_rows() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    let schema = Schema::new().expect("the dataset tables");
    let chain = ChainId::new(CHAIN);
    let position = sink.positions[&TableId::Dataset(Table::Block)];
    let row = schema
        .row(&chain, &block(1, 0)[0].event)
        .expect("a block row");
    sink.append_some(position, &[&row]).await.expect("appended");
    drop(sink);

    let mut sink = open(&dir).await;
    assert!(stored(&sink, Table::Block, "hash").await.is_empty());
    assert!(ledger(&mut sink).await.is_empty());
}

/// A commit that fails part-way refuses every later write rather than reporting the
/// lost batch as stored, and the next open repairs what it left behind.
#[tokio::test]
async fn a_failed_commit_refuses_every_later_write() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    publish(&mut sink, block(1, 0)).await;
    // A timestamp too large for the lake's microseconds fails the commit part-way.
    let mut broken = block(2, 0);
    for envelope in &mut broken {
        match &mut envelope.event {
            Event::Block(block) => block.timestamp = u64::MAX,
            Event::Transaction(transaction) => transaction.block_timestamp = u64::MAX,
            Event::Log(log) => log.block_timestamp = u64::MAX,
            _ => {}
        }
    }
    buffer(&mut sink, broken).await;
    assert!(sink.flush().await.is_err());

    assert!(matches!(
        sink.flush().await,
        Err(SinkError::Store(StoreError::Failed))
    ));
    assert!(matches!(
        sink.publish(block(2, 0).remove(0)).await,
        Err(SinkError::Store(StoreError::Failed))
    ));
    drop(sink);

    let mut sink = open(&dir).await;
    assert_eq!(blocks(&sink).await, expected(&[(1, 0)]));
    assert_eq!(ledger(&mut sink).await.len(), 1);
}

/// A batch that accepts a committed block again, with no reorg orphaning it first, is
/// refused before anything is written: the lake appends, so it would hold the block
/// twice.
#[tokio::test]
async fn a_replayed_block_is_refused() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=3 {
        publish(&mut sink, block(height, 0)).await;
    }
    buffer(&mut sink, block(3, 0)).await;
    assert!(matches!(
        sink.flush().await,
        Err(SinkError::Store(StoreError::Replayed { block })) if block == hex(hash(3, 0))
    ));
    let rows: usize = sink
        .lake(Table::Block)
        .read(Ok)
        .await
        .expect("the table reads")
        .iter()
        .map(deltalake::arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 3, "nothing of the replay was written");
}

/// A rewind deletes every block above its height from every table, keeps no record of
/// it, and lets the blocks it took be written again — on the same branch or another —
/// without counting as a replay.
#[tokio::test]
async fn a_rewind_takes_the_blocks_above_it_and_they_can_be_written_again() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=5 {
        publish(&mut sink, block(height, 0)).await;
    }
    sink.rewind(3).await.expect("rewound");
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 0)]));
    assert!(retractions(&sink).await.is_empty());

    publish(&mut sink, block(4, 0)).await;
    publish(&mut sink, block(5, 1)).await;
    drop(sink);
    let mut sink = open(&dir).await;
    assert_eq!(
        blocks(&sink).await,
        expected(&[(1, 0), (2, 0), (3, 0), (4, 0), (5, 1)])
    );
    assert_eq!(
        ledger(&mut sink)
            .await
            .iter()
            .map(|block| block.height)
            .collect::<Vec<_>>(),
        [1, 2, 3, 4, 5]
    );
}

/// The lake reports what it buffers, so the drain can commit by size: it grows with
/// every block, shrinks when a reorg drops buffered blocks, and is empty after a commit.
#[tokio::test]
async fn the_lake_reports_what_it_buffers() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    assert_eq!(sink.buffered_bytes(), 0);
    buffer(&mut sink, block(1, 0)).await;
    let one = sink.buffered_bytes();
    assert!(one > 0);
    buffer(&mut sink, block(2, 0)).await;
    assert!(sink.buffered_bytes() > one);
    buffer(&mut sink, vec![reorg(2..=2, 1)]).await;
    assert!(sink.buffered_bytes() < 2 * one);
    sink.flush().await.expect("commits");
    assert_eq!(sink.buffered_bytes(), 0);
}

/// A reorg that orphans committed blocks and buffered ones at once deletes the first and
/// drops the second, and only the replacements are stored.
#[tokio::test]
async fn a_reorg_across_committed_and_buffered_blocks_keeps_only_its_replacements() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=3 {
        publish(&mut sink, block(height, 0)).await;
    }
    for height in 4..=5 {
        buffer(&mut sink, block(height, 0)).await;
    }
    let mut replacements = vec![reorg(3..=5, 1)];
    for height in 3..=5 {
        replacements.extend(block(height, 1));
    }
    publish(&mut sink, replacements).await;

    let canonical = [(1, 0), (2, 0), (3, 1), (4, 1), (5, 1)];
    assert_eq!(blocks(&sink).await, expected(&canonical));
    drop(sink);
    let sink = open(&dir).await;
    assert_eq!(blocks(&sink).await, expected(&canonical));
}

/// A committed block orphaned and canonical again within one buffer ends up stored, and
/// a later reorg can still orphan it.
#[tokio::test]
async fn a_block_orphaned_and_canonical_again_in_one_buffer_is_stored() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=3 {
        publish(&mut sink, block(height, 0)).await;
    }
    let mut flips = vec![reorg(3..=3, 1)];
    flips.extend(block(3, 1));
    flips.push(Envelope::new(
        ChainId::new(CHAIN),
        Event::Reorg(Reorg {
            height: 3,
            new_head_hash: hash(3, 0),
            orphaned_hashes: vec![hash(3, 1)],
        }),
    ));
    flips.extend(block(3, 0));
    publish(&mut sink, flips).await;

    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 0)]));
    assert!(sink.committed.contains_key(&hex(hash(3, 0))));
}

/// A commit that stopped part-way through deleting a retraction's orphans — out of the
/// ledger and one table, not yet the others — is finished by the next open.
#[tokio::test]
async fn opening_finishes_a_retraction_stopped_between_tables() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=3 {
        publish(&mut sink, block(height, 0)).await;
    }
    append_only(&mut sink, &reorg(3..=3, 1)).await;
    for (table, column) in [(Table::AcceptedBlock, "hash"), (Table::Block, "hash")] {
        let position = sink.positions[&TableId::Dataset(table)];
        let writer = sink.writer.clone();
        sink.lakes[position]
            .delete(&writer, in_texts(column, &[hex(hash(3, 0))]))
            .await
            .expect("deleted");
    }
    drop(sink);

    let sink = open(&dir).await;
    assert_eq!(retractions(&sink).await, expected(&[(3, 1)]));
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0)]));
}

/// The ledger is read from its newest two partitions: the window across a partition
/// boundary comes back whole, and a block partitions below it is not read.
#[tokio::test]
async fn the_ledger_is_read_from_its_newest_partitions() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in [1, 199_999, 200_000, 200_001] {
        publish(&mut sink, block(height, 0)).await;
    }
    drop(sink);

    let mut sink = open(&dir).await;
    assert_eq!(
        ledger(&mut sink)
            .await
            .iter()
            .map(|block| block.height)
            .collect::<Vec<_>>(),
        [199_999, 200_000, 200_001]
    );
}

/// A decoded record is stored twice — its generic row and its event's typed row — with
/// an array as a typed list, a `uint256` element as exact decimal text, and a tuple as
/// one JSON object.
#[tokio::test]
async fn a_decoded_record_fills_its_event_table_with_typed_lists() {
    let dir = Dir::new();
    let schema = fixtures::schema();
    let mut sink = DeltaSink::open(&dir.settings(), Arc::clone(&schema), CHAIN)
        .await
        .expect("the lake opens with the event tables");
    let created = fixtures::decoded_pool_created();
    let swap = fixtures::decoded_swap();
    let event = schema
        .event_row(&ChainId::new(CHAIN), &created)
        .expect("a row")
        .expect("an event table");
    let name = event.table().name.clone();
    publish(
        &mut sink,
        [created, swap]
            .into_iter()
            .map(|decoded| Envelope::new(ChainId::new(CHAIN), Event::Decoded(Box::new(decoded))))
            .collect(),
    )
    .await;

    assert_eq!(stored(&sink, Table::Decoded, "dedupe_key").await.len(), 2);
    let lake = &sink.lakes[sink.positions[&event.table().id]];
    assert_eq!(lake.name(), name);
    let columns = [
        "extensions",
        "negative_bin_data_array",
        "extension_orders",
        "price_provider_timelock",
    ];
    let batches = lake
        .read(|frame| frame.select(columns.map(ident)))
        .await
        .expect("the event table reads");
    let values: Vec<String> = (0..columns.len())
        .map(|index| {
            ArrayFormatter::try_new(batches[0].column(index).as_ref(), &FormatOptions::default())
                .expect("formats")
                .value(0)
                .to_string()
        })
        .collect();
    assert_eq!(
        values[0],
        "[0xb1a246b1131ff328067c4aaf4f772ff351475244, \
         0xe4038fa09f0bb9963068afaf97be0c045155090d, \
         0xebc53e61078976118e384f110c262a263decb84b]"
    );
    assert!(
        values[1].starts_with("[2510840694154681225832395181564014445733957084757624461722000,"),
        "a packed word is its exact integer: {}",
        values[1]
    );
    assert_eq!(
        values[2],
        r#"{"beforeAddLiquidity":"0","afterAddLiquidity":"0","beforeRemoveLiquidity":"0","afterRemoveLiquidity":"0","beforeSwap":"3","afterSwap":"10"}"#
    );
    assert_eq!(values[3], U256::MAX.to_string());
}

/// A table already at the lake's location that does not match its definition stops the
/// open, rather than the first write.
#[tokio::test]
async fn opening_over_a_different_table_is_drift() {
    let dir = Dir::new();
    let schema = Schema::new().expect("the dataset tables");
    let name = schema.dataset(Table::Block).name.clone();
    let url = ensure_table_uri(dir.table(&name)).expect("a table url");
    DeltaTable::try_from_url(url)
        .await
        .expect("a location")
        .create()
        .with_columns([StructField::new("hash", DataType::STRING, false)])
        .await
        .expect("a stranger's table");

    let error = DeltaSink::open(&dir.settings(), Arc::new(schema), CHAIN)
        .await
        .expect_err("drift stops the open");
    assert!(
        matches!(&error, StoreError::Drift { table, .. } if *table == name),
        "{error}"
    );
}

/// The files `table` holds in each partition, by partition.
fn files_by_partition(sink: &DeltaSink, table: Table) -> std::collections::BTreeMap<String, usize> {
    let lake = sink.lake(table);
    let mut files = std::collections::BTreeMap::new();
    for file in lake
        .table
        .snapshot()
        .expect("a snapshot")
        .snapshot()
        .try_log_data()
        .expect("the file list")
        .iter()
    {
        let partition = file
            .partition_values_map()
            .remove(super::PARTITION)
            .flatten()
            .expect("a partition");
        *files.entry(partition).or_default() += 1;
    }
    files
}

/// A partition the tip has left behind by more than the reorg window is compacted into
/// one file, with every row kept; the partitions a reorg can still reach are not
/// touched. Vacuum then deletes the files compaction replaced.
#[tokio::test]
async fn maintenance_compacts_finished_partitions_and_vacuums_what_they_replaced() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in [1, 2, 3, 100_001, 100_002] {
        publish(&mut sink, block(height, 0)).await;
    }
    let before = files_by_partition(&sink, Table::Block);
    assert_eq!(before, [("0".to_owned(), 3), ("1".to_owned(), 2)].into());

    let position = sink.positions[&TableId::Dataset(Table::Block)];
    let mut table = sink.lakes[position].table.clone();
    let writer = sink.writer.clone();
    // Partition 0 ends at 99_999: one height short of the window past it, nothing moves.
    let short = 100_000 + crate::ingest::pipeline::MAX_UNFINALIZED_BLOCKS as u64 - 1;
    assert_eq!(
        maintenance::compact("blocks", &mut table, short, &writer)
            .await
            .expect("compacts"),
        0
    );
    let compacted = maintenance::compact("blocks", &mut table, short + 1, &writer)
        .await
        .expect("compacts");
    assert_eq!(compacted, 1);
    sink.lakes[position].table = table.clone();
    assert_eq!(
        files_by_partition(&sink, Table::Block),
        [("0".to_owned(), 1), ("1".to_owned(), 2)].into()
    );
    let held = [(1, 0), (2, 0), (3, 0), (100_001, 0), (100_002, 0)];
    assert_eq!(stored(&sink, Table::Block, "hash").await, expected(&held));

    let on_disk = |partition: &str| {
        std::fs::read_dir(dir.table(&format!("blocks/{}={partition}", super::PARTITION)))
            .expect("the partition directory")
            .filter(|entry| {
                entry
                    .as_ref()
                    .is_ok_and(|entry| entry.path().extension().is_some_and(|ext| ext == "parquet"))
            })
            .count()
    };
    assert_eq!(on_disk("0"), 4, "the replaced files stay until a vacuum");
    let vacuumed = maintenance::vacuum("blocks", &mut table, std::time::Duration::ZERO)
        .await
        .expect("vacuums");
    assert_eq!(vacuumed, 3);
    assert_eq!(on_disk("0"), 1);
    assert_eq!(on_disk("1"), 2);
}

/// Compaction is a second writer: the tip writer, holding a snapshot from before it,
/// still appends and deletes in the newest partition, and nothing is lost on either side.
#[tokio::test]
async fn the_writer_commits_past_a_compaction_it_did_not_see() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in [1, 2, 104_096, 104_097] {
        publish(&mut sink, block(height, 0)).await;
    }
    let position = sink.positions[&TableId::Dataset(Table::Block)];
    let mut behind = sink.lakes[position].table.clone();
    let writer = sink.writer.clone();
    maintenance::compact("blocks", &mut behind, 104_097, &writer)
        .await
        .expect("compacts");

    let mut replacement = vec![reorg(104_097..=104_097, 1)];
    replacement.extend(block(104_097, 1));
    replacement.extend(block(104_098, 0));
    publish(&mut sink, replacement).await;
    drop(sink);

    let sink = open(&dir).await;
    assert_eq!(
        blocks(&sink).await,
        expected(&[(1, 0), (2, 0), (104_096, 0), (104_097, 1), (104_098, 0)])
    );
    assert_eq!(files_by_partition(&sink, Table::Block)["0"], 1);
}

/// Log cleanup deletes the transaction log's entries a checkpoint replaces, and keeps
/// the checkpoint and every entry after it, so the table still opens with every row.
#[tokio::test]
async fn log_cleanup_keeps_what_a_checkpoint_does_not_replace() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in [1, 2, 3] {
        publish(&mut sink, block(height, 0)).await;
    }
    let position = sink.positions[&TableId::Dataset(Table::Block)];
    deltalake::checkpoints::create_checkpoint(&sink.lakes[position].table, None)
        .await
        .expect("a checkpoint");
    let checkpoint = sink.lakes[position].table.version().expect("a version");
    publish(&mut sink, block(4, 0)).await;

    let entries = || {
        let mut versions: Vec<u64> = std::fs::read_dir(dir.table("blocks/_delta_log"))
            .expect("the log directory")
            .filter_map(|entry| {
                let name = entry.ok()?.file_name().into_string().ok()?;
                name.strip_suffix(".json")?.parse().ok()
            })
            .collect();
        versions.sort_unstable();
        versions
    };
    assert_eq!(entries(), (0..=checkpoint + 1).collect::<Vec<_>>());

    let mut table = sink.lakes[position].table.clone();
    let cleaned = maintenance::clean_log("blocks", &mut table, std::time::Duration::ZERO)
        .await
        .expect("cleans");
    assert_eq!(
        cleaned,
        usize::try_from(checkpoint).expect("a small version")
    );
    assert_eq!(entries(), vec![checkpoint, checkpoint + 1]);
    drop(sink);

    let sink = open(&dir).await;
    assert_eq!(
        blocks(&sink).await,
        expected(&[(1, 0), (2, 0), (3, 0), (4, 0)])
    );
}

/// A table with more rows than one Arrow batch holds is built in several, on several
/// threads, and still commits every row once.
#[tokio::test]
async fn a_table_larger_than_one_batch_commits_every_row() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    let logs = super::ROWS_PER_BATCH * 2 + 7;
    let mut envelopes = block(1, 0);
    let ledger_entry = envelopes.pop().expect("the ledger entry");
    let transaction = hash(1, 100);
    for index in 1..logs {
        envelopes.push(Envelope::new(
            ChainId::new(CHAIN),
            Event::Log(Box::new(Log {
                log_index: u64::try_from(index).expect("an index"),
                transaction_hash: transaction,
                block_number: 1,
                block_hash: hash(1, 0),
                block_timestamp: 1_700_000_001,
                ..Log::default()
            })),
        ));
    }
    envelopes.push(ledger_entry);
    publish(&mut sink, envelopes).await;

    assert_eq!(stored(&sink, Table::Log, "dedupe_key").await.len(), logs);
    drop(sink);
    let mut sink = open(&dir).await;
    assert_eq!(stored(&sink, Table::Log, "dedupe_key").await.len(), logs);
    assert_eq!(ledger(&mut sink).await.len(), 1);
}

/// A commit's ledger append runs on past its flush, beside the next commit's appends,
/// and a restore waits for it: every block committed is in the ledger it reads.
#[tokio::test]
async fn a_ledger_append_overlaps_the_next_commit() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=3 {
        buffer(&mut sink, block(height, 0)).await;
        sink.flush().await.expect("flushed");
    }
    assert_eq!(
        ledger(&mut sink)
            .await
            .iter()
            .map(|block| block.height)
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 0)]));
}

/// The store checkpoints a table beside its writes once enough versions pass, rather
/// than inside the commit that reaches the interval.
#[tokio::test]
async fn a_checkpoint_is_written_beside_the_commits() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    let commits = super::CHECKPOINT_EVERY + 1;
    for height in 1..=commits {
        publish(&mut sink, block(height, 0)).await;
    }
    wait_for_checkpoint(&log_of(&dir, "accepted_blocks")).await;
}

/// A table committed to less often than the checkpoint interval in each run is still
/// checkpointed once the interval has passed across runs: the count starts at its
/// newest checkpoint, not at the version a run opened it at.
#[tokio::test]
async fn the_checkpoint_interval_counts_across_reopens() {
    let dir = Dir::new();
    let log = log_of(&dir, "accepted_blocks");
    let half = super::CHECKPOINT_EVERY / 2 + 1;
    let mut sink = open(&dir).await;
    for height in 1..=half {
        publish(&mut sink, block(height, 0)).await;
    }
    drop(sink);
    assert!(!log.join("_last_checkpoint").exists(), "checkpointed early");
    let mut sink = open(&dir).await;
    for height in half + 1..=super::CHECKPOINT_EVERY + 1 {
        publish(&mut sink, block(height, 0)).await;
    }
    wait_for_checkpoint(&log).await;
}

/// Opening starts the checkpoint a table is due, so one a stop interrupted, or a table
/// no commit reaches, is not left to read its whole log on every open.
#[tokio::test]
async fn opening_starts_a_checkpoint_that_is_due() {
    let dir = Dir::new();
    let log = log_of(&dir, "accepted_blocks");
    let mut sink = open(&dir).await;
    for height in 1..=super::CHECKPOINT_EVERY + 1 {
        publish(&mut sink, block(height, 0)).await;
    }
    wait_for_checkpoint(&log).await;
    drop(sink);
    // As if the process stopped before the checkpoint was written.
    for entry in std::fs::read_dir(&log).expect("the log").flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.contains(".checkpoint.") || name == "_last_checkpoint" {
            std::fs::remove_file(entry.path()).expect("removed");
        }
    }
    let _sink = open(&dir).await;
    wait_for_checkpoint(&log).await;
}

/// The transaction log of `table` in the lake at `dir`.
fn log_of(dir: &Dir, table: &str) -> PathBuf {
    PathBuf::from(dir.table(table)).join("_delta_log")
}

/// Waits for the log at `log` to name a checkpoint: `_last_checkpoint` is written once
/// the checkpoint itself is.
async fn wait_for_checkpoint(log: &std::path::Path) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !log.join("_last_checkpoint").exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "no checkpoint was written"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// A contract created in block `height` on `branch`.
fn contract(height: u64, branch: u8) -> Envelope {
    Envelope::new(
        ChainId::new(CHAIN),
        Event::Contract(Box::new(crate::wire::envelope::Contract {
            protocol: "uniswap_v3".to_owned(),
            name: "UniswapV3Pool".to_owned(),
            address: alloy_primitives::Address::from([branch.wrapping_add(0xd0); 20]),
            factory_address: alloy_primitives::Address::from([0xfa; 20]),
            transaction_hash: hash(height, branch.wrapping_add(100)),
            transaction_index: 0,
            log_index: 1,
            block_number: height,
            block_hash: hash(height, branch),
            block_timestamp: 1_700_000_000 + height,
        })),
    )
}

/// The first restore after opening hands on what opening read, and it matches what a
/// later read of the lake finds; once a commit lands, a restore reads it.
#[tokio::test]
async fn a_restore_after_opening_hands_on_what_opening_read() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    let mut first = block(1, 0);
    first.insert(1, contract(1, 0));
    publish(&mut sink, first).await;
    publish(&mut sink, block(2, 0)).await;
    drop(sink);

    let mut sink = open(&dir).await;
    let handed_on = sink.restore().await.expect("the lake restores");
    let read = sink.restore().await.expect("the lake restores again");
    assert_eq!(handed_on.ledger, read.ledger);
    assert_eq!(handed_on.contracts, read.contracts);
    assert_eq!(handed_on.contracts.len(), 1);
    assert_eq!(
        handed_on
            .ledger
            .iter()
            .map(|block| block.height)
            .collect::<Vec<_>>(),
        [1, 2]
    );

    publish(&mut sink, block(3, 0)).await;
    assert_eq!(
        ledger(&mut sink).await.len(),
        3,
        "a commit is read, not handed on"
    );
}

/// A contract a stopped commit left behind is repaired away before opening hands the
/// contracts on: they are read again when the repair deletes some of them.
#[tokio::test]
async fn opening_hands_on_contracts_as_repaired() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    let mut first = block(1, 0);
    first.insert(1, contract(1, 0));
    publish(&mut sink, first).await;
    // Block 2's contract, appended without its ledger entry, as a stopped commit would.
    append_only(&mut sink, &contract(2, 0)).await;
    drop(sink);

    let mut sink = open(&dir).await;
    let restored = sink.restore().await.expect("the lake restores");
    assert_eq!(restored.contracts.len(), 1, "the stray contract is gone");
    assert_eq!(restored.contracts[0].block_hash, hash(1, 0));
}
