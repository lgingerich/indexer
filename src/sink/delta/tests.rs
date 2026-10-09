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

use super::{DeltaSettings, DeltaSink, in_texts, texts, texts_of};
use crate::sink::EnvelopeSink;
use crate::sink::table::{Schema, Table, TableId};
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

    /// Settings that commit on every flush, unless `interval` says otherwise.
    fn settings(&self, interval: u64) -> DeltaSettings {
        DeltaSettings {
            uri: self.0.display().to_string(),
            commit_interval_secs: interval,
            max_buffer_bytes: usize::MAX,
            storage: std::collections::BTreeMap::new(),
        }
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn open(dir: &Dir, interval: u64) -> DeltaSink {
    DeltaSink::open(
        &dir.settings(interval),
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

async fn publish(sink: &mut DeltaSink, envelopes: Vec<Envelope>) {
    for envelope in envelopes {
        sink.publish(envelope).await.expect("accepted");
    }
    sink.flush().await.expect("flushed");
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
    let mut sink = open(&dir, 0).await;
    for height in 1..=12 {
        publish(&mut sink, block(height, 0)).await;
    }
    sink.close().await.expect("closes");
    drop(sink);

    let mut sink = open(&dir, 0).await;
    let ledger = sink.ledger(5);
    assert_eq!(
        ledger.iter().map(|block| block.height).collect::<Vec<_>>(),
        [8, 9, 10, 11, 12]
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
    let mut sink = open(&dir, 3600).await;
    for height in 1..=3 {
        publish(&mut sink, block(height, 0)).await;
    }
    publish(
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
    sink.close().await.expect("closes");

    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 1)]));
    assert_eq!(stored(&sink, Table::Reorg, "new_head_hash").await.len(), 1);
}

/// Committed blocks a reorg orphans are deleted from every table, the ledger included,
/// and their replacements take their place.
#[tokio::test]
async fn a_reorg_of_committed_blocks_deletes_them_everywhere() {
    let dir = Dir::new();
    let mut sink = open(&dir, 0).await;
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

    let mut sink = open(&dir, 0).await;
    let ledger = sink.ledger(usize::MAX);
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
    sink.append(position, &[&row]).await.expect("appended");
}

/// A commit that stopped after recording a retraction but before taking its orphans out
/// of the ledger leaves a record of a retraction that did not happen. Opening the lake
/// deletes it, and the restart, still on the orphaned branch, records it once.
#[tokio::test]
async fn opening_deletes_a_retraction_the_ledger_never_saw() {
    let dir = Dir::new();
    let mut sink = open(&dir, 0).await;
    for height in 1..=3 {
        publish(&mut sink, block(height, 0)).await;
    }
    append_only(&mut sink, &reorg(3..=3, 1)).await;
    drop(sink);

    let mut sink = open(&dir, 0).await;
    assert!(retractions(&sink).await.is_empty());
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 0)]));

    let mut replacement = vec![reorg(3..=3, 1)];
    replacement.extend(block(3, 1));
    publish(&mut sink, replacement).await;
    drop(sink);

    let sink = open(&dir, 0).await;
    assert_eq!(retractions(&sink).await, expected(&[(3, 1)]));
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 1)]));
}

/// A commit that stopped after taking the orphans out of the ledger has already recorded
/// the retraction. Opening the lake keeps the record and deletes the orphans' rows, and
/// the restart, resuming below the fork, has nothing to retract again.
#[tokio::test]
async fn a_retraction_out_of_the_ledger_keeps_its_record() {
    let dir = Dir::new();
    let mut sink = open(&dir, 0).await;
    for height in 1..=3 {
        publish(&mut sink, block(height, 0)).await;
    }
    append_only(&mut sink, &reorg(3..=3, 1)).await;
    let ledger = sink.positions[&TableId::Dataset(Table::AcceptedBlock)];
    sink.delete(ledger, in_texts("hash", &[hex(hash(3, 0))]))
        .await
        .expect("deleted");
    drop(sink);

    let mut sink = open(&dir, 0).await;
    assert_eq!(retractions(&sink).await, expected(&[(3, 1)]));
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0)]));
    assert_eq!(sink.ledger(usize::MAX).len(), 2);
}

/// A commit that stopped after writing rows but before their ledger entry leaves rows the
/// ledger does not name — above its tip, or at a height it holds another block for.
/// Opening the lake deletes them, so a resumed run writes them once.
#[tokio::test]
async fn opening_repairs_rows_a_stopped_commit_left_behind() {
    let dir = Dir::new();
    let mut sink = open(&dir, 0).await;
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
        sink.append(position, &rows).await.expect("appended");
    }
    drop(sink);

    let mut sink = open(&dir, 0).await;
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 0)]));
    assert_eq!(sink.ledger(usize::MAX).len(), 3);
}

/// With no ledger at all, no block row counts: the first commit stopped before its
/// ledger entry, and the run starts over.
#[tokio::test]
async fn opening_with_no_ledger_keeps_no_block_rows() {
    let dir = Dir::new();
    let mut sink = open(&dir, 0).await;
    let schema = Schema::new().expect("the dataset tables");
    let chain = ChainId::new(CHAIN);
    let position = sink.positions[&TableId::Dataset(Table::Block)];
    let row = schema
        .row(&chain, &block(1, 0)[0].event)
        .expect("a block row");
    sink.append(position, &[&row]).await.expect("appended");
    drop(sink);

    let mut sink = open(&dir, 0).await;
    assert!(stored(&sink, Table::Block, "hash").await.is_empty());
    assert!(sink.ledger(usize::MAX).is_empty());
}
