//! The Delta Lake store: every table as a Delta table on object storage.
//!
//! The cheap raw-data layer. Each table of the run's [`Schema`] is one Delta table at
//! `<uri>/<chain>/<table>`, written by this process alone, with no catalog: the
//! transaction log sits beside the data, and commits are made safe on S3 and
//! S3-compatible stores by conditional writes.
//!
//! Writes go through delta-rs, which takes rows as Arrow batches (see `arrow`) and runs
//! its write, delete, and read operations on `DataFusion`.
//!
//! # Writing at the tip
//!
//! Blocks are written as they are accepted, not held back until they are final. The
//! storage drain gathers them into one flush, and so one commit, until the oldest has
//! waited [`commit_interval_secs`](DeltaSettings::commit_interval_secs) or
//! [`max_buffer_bytes`](DeltaSettings::max_buffer_bytes) are buffered, so a commit
//! holds many blocks. Each commit writes one file per table and partition it touches,
//! which at the tip are small; once the tip has left a partition behind, `maintenance`
//! compacts it, and vacuums the files compaction and reorgs replaced.
//!
//! A `reorg` drops the orphaned blocks still in the buffer, which most are, since a reorg
//! is a few blocks deep and a commit holds tens of seconds of them. Orphaned blocks
//! already committed are deleted at the next commit. A delete is bounded by the block
//! height partition, so it rewrites only the newest few files.
//!
//! # The ledger
//!
//! Delta commits one table at a time, so a flush that touches several tables is several
//! commits, and a process can stop between any two. What keeps that safe is one rule:
//!
//! > A block's rows count only once its hash is in `accepted_blocks`.
//!
//! A commit deletes orphaned blocks from `accepted_blocks` *before* their rows, and
//! appends new blocks to it *after* theirs, so the ledger only ever names blocks whose
//! rows are all present. Opening the store reads the ledger's newest blocks from its
//! newest two partitions, and repairs what a stopped commit left behind:
//! rows of blocks the ledger does not name, at or above its oldest height, are deleted
//! before ingest resumes. A reader that must never see such rows reads up to the
//! ledger's tip, or joins on it.
//!
//! `reorgs` rows belong to no block, so they follow the mirror of that rule:
//!
//! > A retraction counts only once the blocks it orphans are out of `accepted_blocks`.
//!
//! A commit appends the retraction's row *before* it deletes the orphans from the
//! ledger, and opening the store deletes any row whose orphans the ledger still names.
//! The restart resumes on top of those orphans and detects the reorg again, so each
//! retraction is recorded once, and none is lost to a stop between the two.
//!
//! # One writer
//!
//! A chain's tables are written by one process, and hold that chain alone: rows are not
//! keyed by chain here, so a store is handed only the chain it was opened for. Within
//! the process, `maintenance` writes beside the store, to files the store no longer
//! touches. Unlike `PostgreSQL`'s advisory lock or `DuckDB`'s file lock, nothing here
//! enforces one process: a deployment runs one indexer per chain. A second writer on the same `uri` would append duplicate rows, and its open
//! would repair away the first's uncommitted ones. If that ever needs enforcing, the
//! design is a lease object at `<uri>/<chain>/_writer`: created with a conditional put,
//! renewed on a timer by conditional overwrite, checked before every commit, and taken
//! over once expired — at the cost of a restart after a crash waiting out the lease.
//!
//! # Open items
//!
//! Gaps decided against for now, with their options, are in
//! `docs/delta-lake-open-items.md`.

mod arrow;
mod expr;
mod lake;
mod maintenance;
mod open;
mod repair;
mod settings;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy_primitives::B256;
use deltalake::DeltaTable;
use deltalake::arrow::array::RecordBatch;
use deltalake::datafusion::prelude::Expr;
use deltalake::parquet::file::properties::WriterProperties;
use futures_util::stream::{self, StreamExt as _, TryStreamExt as _};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tracing::info;

use crate::sink::batch::Batch;
use crate::sink::store_error::{Operation, StoreError};
use crate::sink::table::{Row, Schema, Table, TableId};
use crate::sink::{EnvelopeSink, Restored, SinkError, Store};
use crate::window::LEDGER_WINDOW;
use crate::wire::envelope::{BlockMeta, ChainId, Envelope};

use self::expr::{at_or_above, of_blocks};
use self::lake::Lake;
use self::maintenance::Maintenance;
pub use self::settings::DeltaSettings;

/// How many tables an open, a repair, or a delete across the lake works on at once:
/// enough to hide the store's latency, few enough not to flood it with requests.
const CONCURRENT_TABLES: usize = 16;

/// The most rows one Arrow batch of a commit holds, so a large table's batches are built
/// on several threads at once. They are still written to the table as one commit.
const ROWS_PER_BATCH: usize = 1 << 16;

/// The longest one write to the lake may take before the store gives up on it: a commit,
/// with its wait for the last commit's ledger append, a rewind, or the last wait for the
/// ledger at the end of the stream.
///
/// A lake that hangs without failing, on an object store request that never completes,
/// would otherwise stall the whole process: the lake's channel fills, ingest blocks on
/// the send, and the real-time `PostgreSQL` path stops with it. A missed deadline fails
/// the store instead, so the process exits, its supervisor restarts it, and opening
/// repairs what the cut write left behind, as it does after any stop.
///
/// Ten minutes is far past any write that is still coming back: the commit-duration
/// histogram's buckets top out at 60 s for a large backfill commit, and every object
/// store request is bounded well below this by its client's own timeout and retries
/// (see [`DeltaSettings::storage`]).
const COMMIT_DEADLINE: Duration = Duration::from_mins(10);

/// The store: one Delta table per table of the run's schema.
#[derive(Debug)]
pub struct DeltaSink {
    chain: ChainId,
    schema: Arc<Schema>,
    /// Every table, in schema order.
    lakes: Vec<Lake>,
    /// Each table's position in `lakes`.
    positions: HashMap<TableId, usize>,
    batch: Batch,
    /// The committed blocks a reorg could still orphan, by hash, with their heights.
    committed: HashMap<String, u64>,
    /// Whether a commit failed part-way. The failed batch is gone, so every later write
    /// is refused until a reopen repairs the lake.
    failed: bool,
    writer: WriterProperties,
    /// The highest committed height, as maintenance reads it; zero before the first.
    published_tip: Arc<AtomicU64>,
    /// The background compaction and vacuum, when the settings ask for it. Held, never
    /// read: dropping the store stops the task.
    maintenance: Option<Maintenance>,
    /// The last commit's ledger append, while it runs on beside the next commit.
    ledger: PendingLedger,
    /// How long one write may take: [`COMMIT_DEADLINE`], which a test shortens.
    deadline: Duration,
    /// What opening read back, for the first [`restore`](Store::restore) to hand on
    /// rather than read again: the ledger and the contracts take tens of seconds on a
    /// large lake. Dropped once anything is written or rewound.
    opened: Option<Restored>,
}

impl DeltaSink {
    fn lake(&self, table: Table) -> &Lake {
        &self.lakes[self.positions[&TableId::Dataset(table)]]
    }

    /// The highest committed height.
    fn tip(&self) -> Option<u64> {
        self.committed.values().max().copied()
    }

    /// Appends `rows` to the table at `position`, when there are any, and waits for it.
    async fn append_some(&mut self, position: usize, rows: &[&Row]) -> Result<(), StoreError> {
        if rows.is_empty() {
            return Ok(());
        }
        let lake = &mut self.lakes[position];
        let batch = lake.record_batch(rows)?;
        let table = lake.append(&self.writer, vec![batch]).await?;
        lake.committed(table);
        Ok(())
    }

    /// Deletes from every table but the ledger, with `predicate` naming each table's rows,
    /// [`CONCURRENT_TABLES`] at a time. A retraction and a rewind take the blocks out of
    /// the ledger first, so the order among the rest does not matter.
    async fn delete_from_the_rest(
        &mut self,
        predicate: impl Fn(&Lake) -> Option<Expr>,
    ) -> Result<(), StoreError> {
        let ledger_id = TableId::Dataset(Table::AcceptedBlock);
        let writer = &self.writer;
        let mut deletes = Vec::new();
        for lake in &mut self.lakes {
            if lake.def.id == ledger_id {
                continue;
            }
            if let Some(predicate) = predicate(lake) {
                deletes.push(lake.delete(writer, predicate));
            }
        }
        stream::iter(deletes)
            .buffer_unordered(CONCURRENT_TABLES)
            .try_collect::<()>()
            .await
    }

    /// Writes the batch in the ledger's order, one step per rule in the module docs:
    ///
    /// 1. retractions into `reorgs`, while the ledger still names their orphans;
    /// 2. the orphans out of the ledger, then out of every other table;
    /// 3. every table's rows, all at once: each table has its own log, and no rule orders
    ///    one table's rows against another's;
    /// 4. the new blocks into the ledger, once their rows are all present, running on
    ///    while the next commit's rows are appended (see [`PendingLedger`]).
    ///
    /// A failure part-way, or a write cut off at [`COMMIT_DEADLINE`], leaves the lake as
    /// the next open repairs it. The batch is not kept for a retry: the store refuses
    /// every later write with [`StoreError::Failed`], and a restart resumes from the
    /// ledger.
    async fn commit(&mut self) -> Result<(), StoreError> {
        self.guarded(async |sink| {
            if sink.batch.is_empty() {
                return Ok(());
            }
            sink.write().await
        })
        .await
    }

    /// Runs `step`, a write to the lake, unless an earlier one failed, and refuses every
    /// later one if this one fails: a write that stopped part-way leaves what only a
    /// reopen repairs.
    ///
    /// A step still running at the [`deadline`](Self::deadline) fails with
    /// [`StoreError::Stalled`]. It is dropped where it stands, which stops the tasks it
    /// was waiting on (see [`join_every`]); what it had committed by then is what a stop
    /// at that moment would have left.
    async fn guarded<T>(
        &mut self,
        step: impl AsyncFnOnce(&mut Self) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        self.refuse_if_failed()?;
        let deadline = self.deadline;
        let result = tokio::time::timeout(deadline, step(self))
            .await
            .unwrap_or(Err(StoreError::Stalled { after: deadline }));
        self.failed = result.is_err();
        result
    }

    /// Refuses with [`StoreError::Failed`] once a write has failed.
    fn refuse_if_failed(&self) -> Result<(), StoreError> {
        if self.failed {
            return Err(StoreError::Failed);
        }
        Ok(())
    }

    /// Writes the batch for [`commit`](Self::commit), and logs what it wrote.
    async fn write(&mut self) -> Result<(), StoreError> {
        self.opened = None;
        self.refuse_replays()?;
        let started = Instant::now();
        // Taken out so the commit can take its rows while it writes, and put back cleared.
        let mut batch = std::mem::replace(&mut self.batch, Batch::new(Arc::clone(&self.schema)));
        let commit = self.take_apart(&mut batch)?;
        let summary = commit.summary;
        let timings = self.write_in_order(commit).await?;
        info!(
            chain = %self.chain,
            rows = summary.rows,
            orphaned = summary.orphaned,
            from = ?summary.from,
            to = ?self.tip(),
            tables = summary.tables,
            build_ms = millis(timings.build),
            write_ms = millis(timings.write),
            ledger_wait_ms = millis(timings.ledger_wait),
            elapsed_ms = millis(started.elapsed()),
            "lake commit"
        );
        batch.clear();
        self.batch = batch;
        Ok(())
    }

    /// Takes `batch`'s rows apart by the step of [`commit`](Self::commit) that writes
    /// them, building the ledger's Arrow batch from the accepted blocks' rows.
    fn take_apart(&self, batch: &mut Batch) -> Result<Commit, StoreError> {
        let orphans = self.committed_orphans(batch);
        let mut tables = batch.take_tables();
        let reorgs = tables
            .remove(&TableId::Dataset(Table::Reorg))
            .unwrap_or_default();
        let accepted = tables
            .remove(&TableId::Dataset(Table::AcceptedBlock))
            .unwrap_or_default();
        let appends: Vec<(usize, Vec<Row>)> = (0..self.lakes.len())
            .filter_map(|position| Some((position, tables.remove(&self.lakes[position].def.id)?)))
            .collect();
        let summary = Summary {
            rows: reorgs.len()
                + accepted.len()
                + appends.iter().map(|(_, rows)| rows.len()).sum::<usize>(),
            orphaned: orphans.as_ref().map_or(0, |(_, hashes)| hashes.len()),
            from: accepted.iter().filter_map(Row::block_number).min(),
            tables: appends.len(),
        };
        let ledger = self.ledger_batch(&accepted.iter().collect::<Vec<_>>())?;
        // What the ledger's window needs of the accepted blocks, once their rows are gone.
        let accepted = accepted
            .iter()
            .filter_map(|row| Some((row.block_hash()?.to_owned(), row.block_number()?)))
            .collect();
        Ok(Commit {
            reorgs,
            orphans,
            appends,
            ledger,
            accepted,
            summary,
        })
    }

    /// Writes `commit` in the ledger's order (see [`commit`](Self::commit)), and returns
    /// how long its slow parts took.
    async fn write_in_order(&mut self, commit: Commit) -> Result<Timings, StoreError> {
        let Commit {
            reorgs,
            orphans,
            appends,
            ledger,
            accepted,
            summary: _,
        } = commit;
        // A retraction's steps are ordered against the ledger, so it waits for the last
        // one; a commit without one never does.
        if !reorgs.is_empty() || orphans.is_some() {
            self.settle_ledger().await?;
        }
        // 1. Retractions into `reorgs`, while the ledger still names their orphans.
        let position = self.position(Table::Reorg);
        self.append_some(position, &reorgs.iter().collect::<Vec<_>>())
            .await?;
        drop(reorgs);
        // 2. The orphans out of the ledger, then out of every other table.
        if let Some((floor, hashes)) = &orphans {
            self.retract(*floor, hashes).await?;
        }
        // 3. Every table's rows, all at once.
        let (batches, build) = timed(self.record_batches(appends)).await;
        let (appended, write) = timed(self.append_all(batches?)).await;
        // The last commit's ledger append ran beside these appends; this waits out what is
        // left of it, which is usually nothing. Waited for even when an append failed, so
        // it is not left running.
        let (settled, ledger_wait) = timed(self.settle_ledger()).await;
        appended?;
        settled?;
        // 4. The new blocks into the ledger, running on beside the next commit.
        self.start_ledger(ledger);
        let orphaned = orphans.map(|(_, hashes)| hashes).unwrap_or_default();
        self.advance(&orphaned, accepted);
        Ok(Timings {
            build,
            write,
            ledger_wait,
        })
    }

    /// Starts appending `batch`, the accepted blocks, to the ledger (see
    /// [`PendingLedger`]); nothing when the commit accepts no block.
    fn start_ledger(&mut self, batch: Option<RecordBatch>) {
        let Some(batch) = batch else {
            return;
        };
        let lake = &self.lakes[self.positions[&TableId::Dataset(Table::AcceptedBlock)]];
        self.ledger.start(lake, &self.writer, batch);
    }

    /// `accepted` as the ledger's Arrow batch, for [`start_ledger`](Self::start_ledger);
    /// `None` when the commit accepts no block.
    fn ledger_batch(&self, accepted: &[&Row]) -> Result<Option<RecordBatch>, StoreError> {
        if accepted.is_empty() {
            return Ok(None);
        }
        let lake = &self.lakes[self.position(Table::AcceptedBlock)];
        lake.record_batch(accepted).map(Some)
    }

    /// Waits for the ledger append [`start_ledger`](Self::start_ledger) left running, if
    /// any, and takes the ledger as of it.
    async fn settle_ledger(&mut self) -> Result<(), StoreError> {
        if let Some(table) = self.ledger.settle().await? {
            let position = self.position(Table::AcceptedBlock);
            self.lakes[position].committed(table);
        }
        Ok(())
    }

    /// Each table's rows as Arrow batches, built in parallel: a task per table, and per
    /// [`ROWS_PER_BATCH`] rows of a large one, since one table, the logs, usually holds
    /// most of a commit's rows. Building them is the CPU half of a commit.
    ///
    /// The tasks run on the store's own runtime, so as many build at once as it has
    /// workers and no more. Threads of their own would each take a share of the cores
    /// beside them, and a large commit made over a hundred, crowding out every other
    /// part of the process (see `sink::store_runtime`).
    ///
    /// Each task owns its rows and frees them once their batch is built, so the rows
    /// go as the batches come: a commit never holds every row and every batch at once,
    /// which at the end of the build was its largest moment.
    async fn record_batches(
        &self,
        appends: Vec<(usize, Vec<Row>)>,
    ) -> Result<Vec<(usize, Vec<RecordBatch>)>, StoreError> {
        let tables = appends.len();
        // Every build starts before any is waited for.
        let mut builds = JoinSet::new();
        for (position, rows) in appends {
            let lake = &self.lakes[position];
            let mut rows = rows.into_iter();
            for chunk in 0_usize.. {
                let part: Vec<Row> = rows.by_ref().take(ROWS_PER_BATCH).collect();
                if part.is_empty() {
                    break;
                }
                let (arrow, def, width) =
                    (Arc::clone(&lake.arrow), Arc::clone(&lake.def), lake.width);
                builds.spawn(async move {
                    let built =
                        arrow::record_batch(&arrow, &def, width, &part.iter().collect::<Vec<_>>())
                            .map_err(StoreError::engine(Operation::Append, Some(&def.name)));
                    ((position, chunk), built)
                });
            }
        }
        let (built, failure) = join_every(builds).await;
        if let Some(error) = failure {
            return Err(error);
        }
        let mut batches: Vec<(usize, Vec<RecordBatch>)> = Vec::with_capacity(tables);
        for ((position, _), batch) in built {
            match batches.last_mut() {
                Some((last, table)) if *last == position => table.push(batch),
                _ => batches.push((position, vec![batch])),
            }
        }
        Ok(batches)
    }

    /// Appends each table's batch as a commit of its own log, every table on its own task
    /// so their Parquet encoding spreads over the store runtime's workers. Each table has
    /// its own log and no rule orders one table's rows against another's, so they go at
    /// once: most tables' appends are small, and their time is the store's round trips,
    /// which overlap only when they run together.
    async fn append_all(
        &mut self,
        batches: Vec<(usize, Vec<RecordBatch>)>,
    ) -> Result<(), StoreError> {
        let mut appends = JoinSet::new();
        for (position, batches) in batches {
            let append = self.lakes[position].append(&self.writer, batches);
            appends.spawn(async move { (position, append.await) });
        }
        let (appended, failure) = join_every(appends).await;
        for (position, table) in appended {
            self.lakes[position].committed(table);
        }
        failure.map_or(Ok(()), Err)
    }

    /// Refuses a batch that accepts a block already committed, unless the batch orphans
    /// it first: the lake appends rather than upserts, so the block's rows would be
    /// written twice. A restart that resumes below the lake's tip hands the lake blocks
    /// it holds, which [`reconcile`](Store::reconcile) drops before they reach a commit,
    /// so this never happens unless an invariant upstream broke.
    fn refuse_replays(&self) -> Result<(), StoreError> {
        let replayed = self
            .batch
            .rows
            .iter()
            .filter(|row| row.table().id == TableId::Dataset(Table::AcceptedBlock))
            .filter_map(Row::block_hash)
            .find(|hash| {
                self.committed.contains_key(*hash)
                    && !self
                        .batch
                        .orphaned
                        .values()
                        .any(|(_, orphaned)| orphaned.contains(*hash))
            });
        match replayed {
            Some(block) => Err(StoreError::Replayed {
                block: block.to_owned(),
            }),
            None => Ok(()),
        }
    }

    /// The committed blocks `batch` orphans, with the lowest height any of them is at.
    /// The others never left the buffer, which dropped them already.
    fn committed_orphans(&self, batch: &Batch) -> Option<(u64, Vec<String>)> {
        let floor = batch.orphaned.values().map(|(height, _)| *height).min()?;
        let hashes: Vec<String> = batch
            .orphaned
            .values()
            .flat_map(|(_, hashes)| hashes)
            .filter(|hash| self.committed.contains_key(*hash))
            .cloned()
            .collect();
        (!hashes.is_empty()).then_some((floor, hashes))
    }

    /// Deletes the orphaned blocks `hashes`, none below `floor`: out of the ledger first,
    /// so it never names a block whose rows are going, then out of every other table.
    async fn retract(&mut self, floor: u64, hashes: &[String]) -> Result<(), StoreError> {
        let of = |lake: &Lake| {
            Some(of_blocks(
                lake.block_columns()?,
                hashes,
                Some(floor),
                lake.width,
            ))
        };
        self.delete_from_the_ledger_then_the_rest(of).await
    }

    /// Deletes every block above `height`, for [`rewind`](Store::rewind).
    async fn delete_above(&mut self, height: u64) -> Result<(), StoreError> {
        let above = |lake: &Lake| {
            let (_, number) = lake.block_columns()?;
            Some(at_or_above(number, height.saturating_add(1), lake.width))
        };
        self.delete_from_the_ledger_then_the_rest(above).await
    }

    /// Deletes what `predicate` names from the ledger, then from every other table.
    async fn delete_from_the_ledger_then_the_rest(
        &mut self,
        predicate: impl Fn(&Lake) -> Option<Expr>,
    ) -> Result<(), StoreError> {
        let ledger = &mut self.lakes[self.positions[&TableId::Dataset(Table::AcceptedBlock)]];
        if let Some(predicate) = predicate(ledger) {
            ledger.delete(&self.writer, predicate).await?;
        }
        self.delete_from_the_rest(predicate).await
    }

    /// Moves the window of blocks a reorg could still orphan past a commit: its orphans
    /// out, the blocks it accepted in, and anything below the window dropped.
    fn advance(&mut self, orphaned: &[String], accepted: Vec<(String, u64)>) {
        for hash in orphaned {
            self.committed.remove(hash);
        }
        self.committed.extend(accepted);
        if let Some(tip) = self.tip() {
            let floor = tip.saturating_sub(LEDGER_WINDOW as u64);
            self.committed.retain(|_, height| *height >= floor);
        }
        self.publish_tip();
    }

    /// Shares the committed tip with maintenance, which compacts only what it has left
    /// behind.
    fn publish_tip(&self) {
        self.published_tip
            .store(self.tip().unwrap_or_default(), Ordering::Release);
    }

    fn position(&self, table: Table) -> usize {
        self.positions[&TableId::Dataset(table)]
    }
}

/// Opening the lake already repaired it, so a restore only reads back: the ledger as the
/// repair left it, and the contracts it kept.
impl Store for DeltaSink {
    /// The first restore after opening hands on what opening read, which nothing has
    /// changed since; a later one reads both again, at once.
    async fn restore(&mut self) -> Result<Restored, StoreError> {
        self.settle_ledger().await?;
        if let Some(opened) = self.opened.take() {
            return Ok(opened);
        }
        let (contracts, ledger) = tokio::try_join!(self.contracts(), self.read_ledger())?;
        Ok(Restored { contracts, ledger })
    }

    /// A held block whose hash changed is retracted at the next commit like any orphan:
    /// it is among the committed blocks opening read from the ledger.
    fn reconcile(&mut self, held: &[BlockMeta]) {
        self.batch.reconcile(held);
    }

    /// Deletes in the order a retraction does: the blocks out of the ledger first, then
    /// out of every other table. A stop part-way leaves rows of blocks the ledger no
    /// longer names, which the next open deletes.
    ///
    /// Final on failure, as a commit is: every later write is refused with
    /// [`StoreError::Failed`] until the store is reopened.
    async fn rewind(&mut self, height: u64) -> Result<(), StoreError> {
        self.guarded(async |sink| {
            sink.opened = None;
            sink.settle_ledger().await?;
            sink.delete_above(height).await?;
            sink.committed.retain(|_, committed| *committed <= height);
            sink.publish_tip();
            Ok(())
        })
        .await
    }
}

impl EnvelopeSink for DeltaSink {
    /// Buffers one envelope's rows. A `reorg` first drops every buffered row of the
    /// blocks it orphans; the committed ones are deleted at the next commit, and its own
    /// row is kept as the record of the retraction.
    ///
    /// Refused with [`StoreError::Failed`] once a commit has failed.
    async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
        self.refuse_if_failed()?;
        self.batch.push(&envelope)
    }

    /// Commits the batch. The drain decides how many blocks it holds; a block still
    /// buffered when the process stops is fetched again from the ledger's tip.
    ///
    /// A failed flush is final: the batch is not kept, and every later write is refused
    /// with [`StoreError::Failed`] until the store is reopened.
    async fn flush(&mut self) -> Result<(), SinkError> {
        self.commit().await?;
        Ok(())
    }

    fn buffered_bytes(&self) -> usize {
        self.batch.bytes()
    }

    /// Waits for the last commit's ledger append, so its blocks count once the stream
    /// ends. Final on failure, as a commit is.
    async fn finish(&mut self) -> Result<(), SinkError> {
        self.guarded(async |sink| sink.settle_ledger().await)
            .await?;
        Ok(())
    }
}

/// The last commit's ledger append, on a task of its own: the rows of its blocks are all
/// present, so their entries may go in any time, and the next commit's table appends
/// overlap it, which takes a whole commit's round trips off every commit. It is waited
/// for before anything else touches the ledger, before the next one starts, and by
/// [`finish`](EnvelopeSink::finish); a failure surfaces then.
///
/// Safe for the reason the ledger is written last at all: until an entry lands, its
/// block's rows are rows the ledger does not name, which a reopen deletes and ingest
/// fetches again. Every later commit's rows are, too, until its own entry lands.
#[derive(Debug, Default)]
struct PendingLedger(Option<JoinHandle<Result<DeltaTable, StoreError>>>);

impl PendingLedger {
    /// Starts appending `batch` to `ledger`, once the last append is settled.
    fn start(&mut self, ledger: &Lake, writer: &WriterProperties, batch: RecordBatch) {
        debug_assert!(self.0.is_none(), "the last ledger append is settled first");
        self.0 = Some(tokio::spawn(ledger.append(writer, vec![batch])));
    }

    /// Waits for the running append, if any, and hands back the ledger as of it.
    async fn settle(&mut self) -> Result<Option<DeltaTable>, StoreError> {
        let Some(task) = self.0.as_mut() else {
            return Ok(None);
        };
        let appended = task
            .await
            .unwrap_or_else(|join| std::panic::resume_unwind(join.into_panic()));
        self.0 = None;
        appended.map(Some)
    }

    /// Whether an append is still to be settled.
    #[cfg(test)]
    fn is_running(&self) -> bool {
        self.0.is_some()
    }
}

/// A batch taken apart by the step of a commit that writes each part (see
/// [`DeltaSink::commit`]).
struct Commit {
    /// The retractions' rows, for `reorgs`.
    reorgs: Vec<Row>,
    /// The committed blocks the batch orphans, with the lowest height any is at.
    orphans: Option<(u64, Vec<String>)>,
    /// Every other table's rows, by the table's position.
    appends: Vec<(usize, Vec<Row>)>,
    /// The accepted blocks as the ledger's Arrow batch; `None` when there are none.
    ledger: Option<RecordBatch>,
    /// The accepted blocks' hashes and heights, for the window of committed blocks.
    accepted: Vec<(String, u64)>,
    summary: Summary,
}

/// What a commit writes, for its log line.
#[derive(Clone, Copy)]
struct Summary {
    rows: usize,
    orphaned: usize,
    /// The lowest height the commit accepts.
    from: Option<u64>,
    /// How many tables besides the ledger and `reorgs` it appends to.
    tables: usize,
}

/// How long a commit's slow parts took, for its log line.
struct Timings {
    /// Building every table's Arrow batches.
    build: Duration,
    /// Appending them.
    write: Duration,
    /// Waiting out the last commit's ledger append once they were appended.
    ledger_wait: Duration,
}

/// Waits for every task, failed or not, and returns what each succeeded with, in the
/// order of their keys, and the first failure among them: none is left running, holding
/// its rows or committing, once the commit has given up. A task that panicked panics
/// here too.
async fn join_every<K: Ord + 'static, T: 'static>(
    mut tasks: JoinSet<(K, Result<T, StoreError>)>,
) -> (Vec<(K, T)>, Option<StoreError>) {
    let mut done = Vec::with_capacity(tasks.len());
    let mut failure = None;
    while let Some(joined) = tasks.join_next().await {
        match joined.unwrap_or_else(|join| std::panic::resume_unwind(join.into_panic())) {
            (key, Ok(value)) => done.push((key, value)),
            (_, Err(error)) => {
                failure.get_or_insert(error);
            }
        }
    }
    done.sort_by(|(left, _), (right, _)| left.cmp(right));
    (done, failure)
}

/// Runs `future`, and returns what it resolved to with how long it took.
async fn timed<T>(future: impl Future<Output = T>) -> (T, Duration) {
    let started = Instant::now();
    (future.await, started.elapsed())
}

/// A duration in whole milliseconds, for a log field.
fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// A hash as the lake stores it: lowercase `0x` hex.
fn hex(hash: &B256) -> String {
    format!("{hash:#x}")
}

#[cfg(test)]
mod tests;
