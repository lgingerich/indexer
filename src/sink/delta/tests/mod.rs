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
use deltalake::DeltaTable;

use super::expr::{in_texts, texts, texts_of};
use super::{DeltaSettings, DeltaSink};
use crate::sink::table::{Schema, Table, TableId};
use crate::sink::{EnvelopeSink, SinkError, Store as _, StoreError};
use crate::wire::datasets::evm::{Block, Log, Transaction};
use crate::wire::envelope::{BlockMeta, ChainId, Envelope, Event, Reorg};

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
            partition_blocks: 100_000,
            storage: std::collections::BTreeMap::new(),
        }
    }

    /// [`settings`](Self::settings), `width` heights a partition.
    fn settings_at(&self, width: u64) -> DeltaSettings {
        DeltaSettings {
            partition_blocks: width,
            ..self.settings()
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
    open_at(dir, 100_000).await.expect("the lake opens")
}

/// Opens the lake in `dir`, `width` heights a partition.
async fn open_at(dir: &Dir, width: u64) -> Result<DeltaSink, StoreError> {
    DeltaSink::open(
        &dir.settings_at(width),
        Arc::new(Schema::new().expect("the dataset tables")),
        CHAIN,
    )
    .await
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

/// The heights of the ledger a restart resumes from, oldest first.
async fn ledger_heights(sink: &mut DeltaSink) -> Vec<u64> {
    ledger(sink)
        .await
        .iter()
        .map(|block| block.height)
        .collect()
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

/// How many rows the lake holds in `table`, duplicates included.
async fn rows(sink: &DeltaSink, table: Table) -> usize {
    sink.lake(table)
        .read(Ok)
        .await
        .expect("the table reads")
        .iter()
        .map(deltalake::arrow::array::RecordBatch::num_rows)
        .sum()
}

fn expected(blocks: &[(u64, u8)]) -> BTreeSet<String> {
    blocks
        .iter()
        .map(|&(height, branch)| hex(hash(height, branch)))
        .collect()
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

/// Deletes the block `hash`'s rows straight from `table`, as a retraction that stopped
/// part-way would have.
async fn delete_only(sink: &mut DeltaSink, table: Table, column: &str, hash: B256) {
    let position = sink.positions[&TableId::Dataset(table)];
    let writer = sink.writer.clone();
    sink.lakes[position]
        .delete(&writer, in_texts(column, &[hex(hash)]))
        .await
        .expect("deleted");
}

mod commit;
mod maintenance;
mod repair;
