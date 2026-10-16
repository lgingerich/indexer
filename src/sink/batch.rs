//! What a store has been handed since its last commit, buffered the same way by every
//! store: rows rendered from envelopes, the blocks a `reorg` retracted, and the last
//! copy of each row.
//!
//! # Blocks a store already holds
//!
//! A restart resumes ingest from the slowest store's tip, so a faster store is handed
//! again blocks it committed before the stop. [`Batch::reconcile`] names them, and each
//! is settled when its `accepted_block` marker arrives, the last envelope of its block:
//!
//! - the same hash at that height: the store holds this block, so its rows are dropped
//!   from the buffer and nothing is written for it;
//! - another hash: the store's copy was orphaned while it was ahead, so the batch
//!   records a retraction of it — a `reorgs` row, and its rows deleted at the commit,
//!   before the canonical block's are written — as a reorg at the tip would.
//!
//! A store ahead on the canonical chain therefore deletes nothing on a restart, which
//! matters for a database serving an app: see `sink::resume`.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use alloy_primitives::B256;

use crate::sink::SinkError;
#[cfg(feature = "delta")]
use crate::sink::table::Value;
use crate::sink::table::{Row, Schema, TableId};
use crate::wire::envelope::{BlockMeta, Envelope, Event, Reorg};

/// What a store has been handed since its last commit: the rows to write, and the
/// blocks a buffered `reorg` retracted.
///
/// A decoded record is two rows: its generic `decoded_logs` row, and its event's typed
/// row, from the store's [`Schema`]. A row of a table the store does not write is not
/// kept, so a store routed only one of the two keeps only that one.
///
/// A store holds only the canonical chain. A `reorg` orphans blocks that are either
/// already committed or still in this buffer — blocks are published in order and the
/// storage channel is FIFO, so an orphaned block can never arrive after its `reorg`.
/// [`push`](Self::push) drops the buffered ones at once, and the store deletes the
/// committed ones when it commits [`rows`](Self::rows), before writing them.
#[derive(Debug)]
pub(super) struct Batch {
    /// Rows to write, in publish order.
    pub(super) rows: Vec<Row>,
    /// Orphaned block hashes to delete from the store, as `0x` hex, by chain, with the
    /// lowest buffered `reorg` height, which none of them is below.
    pub(super) orphaned: BTreeMap<String, (u64, BTreeSet<String>)>,
    /// Roughly how many bytes `rows` hold in memory.
    #[cfg(feature = "delta")]
    bytes: usize,
    /// Where the rows of the block being buffered start in `rows`: every row from here
    /// on came after the last `accepted_block` marker.
    block_start: usize,
    /// The blocks the store committed above where ingest resumed, by height, each
    /// forgotten once ingest hands that height again (see the module docs). Kept across
    /// commits.
    held: BTreeMap<u64, B256>,
    /// Every table the run writes, which renders each decoded record's typed row.
    schema: Arc<Schema>,
}

impl Batch {
    pub(super) fn new(schema: Arc<Schema>) -> Self {
        Self {
            rows: Vec::new(),
            orphaned: BTreeMap::new(),
            #[cfg(feature = "delta")]
            bytes: 0,
            block_start: 0,
            held: BTreeMap::new(),
            schema,
        }
    }

    /// Names the blocks the store already holds that ingest will hand it again: those
    /// above the block every store holds, where ingest resumes. Each is settled when its
    /// height arrives; see the module docs.
    pub(super) fn reconcile(&mut self, held: &[BlockMeta]) {
        self.held = held
            .iter()
            .map(|block| (block.height, block.hash))
            .collect();
    }

    /// Buffers one envelope's rows. A `reorg` first drops every buffered row of the
    /// blocks it orphans and records them for deletion; its own row is kept as the
    /// record of the retraction. A block the store already holds is settled at its
    /// `accepted_block` marker; see the module docs.
    ///
    /// # Errors
    ///
    /// Returns [`SinkError::UnknownEvent`] for a decoded record no event table holds,
    /// which means it was decoded against a different catalog than the store opened with,
    /// and [`SinkError::Table`] when a row cannot be built.
    pub(super) fn push(&mut self, envelope: &Envelope) -> Result<(), SinkError> {
        if let Event::AcceptedBlock(block) = &envelope.event
            && let Some(held) = self.held.remove(&block.height)
        {
            if held == block.hash {
                self.drop_block(block.hash);
                return Ok(());
            }
            // The store's copy of this height is on a branch that lost while it was
            // ahead: retract it as a reorg at the tip would, before this block lands.
            self.push(&Envelope::new(
                envelope.chain.clone(),
                Event::Reorg(Reorg {
                    height: block.height,
                    new_head_hash: block.hash,
                    orphaned_hashes: vec![held],
                }),
            ))?;
        }
        self.buffer(envelope)?;
        if matches!(envelope.event, Event::AcceptedBlock(_)) {
            self.block_start = self.rows.len();
        }
        Ok(())
    }

    /// Drops the rows of the block being buffered, `hash`, which the store already
    /// holds. Its rows all came after the last marker, so only those are looked at.
    fn drop_block(&mut self, hash: B256) {
        let hash = format!("{hash:#x}");
        let start = self.block_start.min(self.rows.len());
        let tail = self.rows.split_off(start);
        for row in tail {
            if row.block_hash() == Some(hash.as_str()) {
                #[cfg(feature = "delta")]
                {
                    self.bytes = self.bytes.saturating_sub(row_size(&row));
                }
            } else {
                self.rows.push(row);
            }
        }
        self.block_start = self.rows.len();
    }

    /// Buffers one envelope's rows, as [`push`](Self::push) describes, once any block the
    /// store holds is settled.
    fn buffer(&mut self, envelope: &Envelope) -> Result<(), SinkError> {
        if let Event::Reorg(reorg) = &envelope.event
            && !reorg.orphaned_hashes.is_empty()
        {
            let chain = envelope.chain.as_str();
            let hashes: BTreeSet<String> = reorg
                .orphaned_hashes
                .iter()
                .map(|hash| format!("{hash:#x}"))
                .collect();
            self.rows.retain(|row| {
                row.chain() != chain || !row.block_hash().is_some_and(|h| hashes.contains(h))
            });
            self.block_start = self.block_start.min(self.rows.len());
            #[cfg(feature = "delta")]
            {
                self.bytes = self.rows.iter().map(row_size).sum();
            }
            let (height, orphaned) = self
                .orphaned
                .entry(chain.to_owned())
                .or_insert((reorg.height, BTreeSet::new()));
            *height = (*height).min(reorg.height);
            orphaned.extend(hashes);
        }
        if let Event::Decoded(decoded) = &envelope.event {
            let row = self
                .schema
                .event_row(&envelope.chain, decoded)?
                .ok_or_else(|| SinkError::UnknownEvent {
                    protocol: decoded.protocol.clone(),
                    contract: decoded.contract.clone(),
                    event: decoded.name.clone(),
                })?;
            self.keep(row);
        }
        let row = self.schema.row(&envelope.chain, &envelope.event)?;
        self.keep(row);
        Ok(())
    }

    fn keep(&mut self, row: Row) {
        if !self.schema.writes(row.table().id) {
            return;
        }
        #[cfg(feature = "delta")]
        {
            self.bytes += row_size(&row);
        }
        self.rows.push(row);
    }

    /// The rows each table should write: the last buffered copy of each
    /// `(chain, dedupe_key)`, in publish order.
    ///
    /// A merge must not see a key twice — both SQL engines refuse to update one conflict
    /// row twice in a statement — and "last" means last published, which only the buffer
    /// knows; a staging table's physical order does not promise it.
    #[cfg(any(feature = "duckdb", feature = "postgres"))]
    pub(super) fn by_table(&self) -> HashMap<TableId, Vec<&Row>> {
        // Sized up front: growing a set of half a million keys rehashes it again and again.
        let mut seen = HashSet::with_capacity(self.rows.len());
        let mut tables: HashMap<TableId, Vec<&Row>> = HashMap::new();
        for row in self.rows.iter().rev() {
            let id = row.table().id;
            if seen.insert((id, row.chain(), row.dedupe_key())) {
                tables.entry(id).or_default().push(row);
            }
        }
        for rows in tables.values_mut() {
            rows.reverse();
        }
        tables
    }

    /// [`by_table`](Self::by_table)'s rows, owned: taken out of the batch, so whoever
    /// writes them can free each as soon as it is written rather than all at the end.
    #[cfg(feature = "delta")]
    pub(super) fn take_tables(&mut self) -> HashMap<TableId, Vec<Row>> {
        let mut keep = vec![false; self.rows.len()];
        let mut seen = HashSet::with_capacity(self.rows.len());
        for (position, row) in self.rows.iter().enumerate().rev() {
            keep[position] = seen.insert((row.table().id, row.chain(), row.dedupe_key()));
        }
        drop(seen);
        let mut tables: HashMap<TableId, Vec<Row>> = HashMap::new();
        for (row, keep) in std::mem::take(&mut self.rows).into_iter().zip(keep) {
            if keep {
                tables.entry(row.table().id).or_default().push(row);
            }
        }
        tables
    }

    /// Whether there is nothing to commit. A `reorg` always buffers its own row, so a
    /// batch with deletions is never empty.
    pub(super) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Roughly how many bytes the buffered rows hold in memory.
    #[cfg(feature = "delta")]
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    /// Forgets what was buffered, once a commit has made it durable. The blocks still
    /// to reconcile are kept: ingest has not reached them yet.
    pub(super) fn clear(&mut self) {
        self.rows.clear();
        self.orphaned.clear();
        self.block_start = 0;
        #[cfg(feature = "delta")]
        {
            self.bytes = 0;
        }
    }
}

/// Roughly how many bytes `row` holds in memory: the row with its own copies of the
/// chain and key, every value's slot, and what each value holds on the heap.
#[cfg(feature = "delta")]
fn row_size(row: &Row) -> usize {
    size_of::<Row>()
        + row.chain().len()
        + row.dedupe_key().len()
        + row.values().iter().map(value_size).sum::<usize>()
}

#[cfg(feature = "delta")]
fn value_size(value: &Value) -> usize {
    size_of::<Value>()
        + match value {
            Value::Text(text) | Value::Document(text) => text.capacity(),
            Value::List(items) => items.iter().map(value_size).sum(),
            Value::Null
            | Value::Uint(_)
            | Value::Int(_)
            | Value::BigInt { .. }
            | Value::Bool(_)
            | Value::Timestamp(_) => 0,
        }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::B256;

    use super::Batch;
    use crate::sink::table::{Table, TableId};
    use crate::wire::envelope::{Block, BlockMeta, ChainId, Envelope, Event};

    fn meta(height: u64, tag: u8) -> BlockMeta {
        BlockMeta {
            height,
            hash: B256::repeat_byte(tag),
            parent_hash: B256::ZERO,
            timestamp: height,
        }
    }

    /// A block's envelopes as ingest publishes them: its row, then its marker.
    fn block(batch: &mut Batch, meta: BlockMeta) {
        let chain = ChainId::new("base");
        let row = Event::Block(Box::new(Block {
            number: meta.height,
            hash: meta.hash,
            ..Block::default()
        }));
        for event in [row, Event::AcceptedBlock(meta)] {
            batch
                .push(&Envelope::new(chain.clone(), event))
                .expect("buffered");
        }
    }

    fn tables(batch: &Batch) -> Vec<TableId> {
        batch.rows.iter().map(|row| row.table().id).collect()
    }

    /// A block the store already holds, handed again with the same hash, leaves nothing
    /// to write, and the block after it is written as usual.
    #[test]
    fn a_held_block_handed_again_is_not_written() {
        let mut batch = Batch::new(crate::sink::fixtures::schema());
        batch.reconcile(&[meta(5, 0xa0)]);
        block(&mut batch, meta(5, 0xa0));
        assert!(batch.is_empty(), "{:?}", tables(&batch));
        assert!(batch.orphaned.is_empty());

        block(&mut batch, meta(6, 0xa1));
        assert_eq!(
            tables(&batch),
            [Table::Block.into(), Table::AcceptedBlock.into()]
        );
    }

    /// A held height handed with another hash retracts the store's copy: a `reorgs` row,
    /// and the old hash to delete at the commit, before the new block's rows.
    #[test]
    fn a_held_block_whose_hash_changed_is_retracted() {
        let mut batch = Batch::new(crate::sink::fixtures::schema());
        batch.reconcile(&[meta(5, 0xa0)]);
        block(&mut batch, meta(5, 0xb0));
        let (height, hashes) = &batch.orphaned["base"];
        assert_eq!(*height, 5);
        assert_eq!(
            hashes.iter().collect::<Vec<_>>(),
            [&format!("{:#x}", B256::repeat_byte(0xa0))]
        );
        assert_eq!(
            tables(&batch),
            [
                Table::Block.into(),
                Table::Reorg.into(),
                Table::AcceptedBlock.into()
            ]
        );
    }

    /// What is still to reconcile outlives a commit: the drain commits on its own
    /// schedule, long before ingest reaches every held height.
    #[test]
    fn held_blocks_outlive_a_commit() {
        let mut batch = Batch::new(crate::sink::fixtures::schema());
        batch.reconcile(&[meta(5, 0xa0), meta(6, 0xa1)]);
        block(&mut batch, meta(5, 0xa0));
        batch.clear();
        block(&mut batch, meta(6, 0xa1));
        assert!(batch.is_empty(), "{:?}", tables(&batch));
    }
}
