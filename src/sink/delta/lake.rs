//! One table of the lake: its handle, how every commit to it is made, and the
//! checkpoints started beside the writer.

use std::sync::{Arc, Mutex, PoisonError};

use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::datatypes::DataType as ArrowType;
use deltalake::arrow::datatypes::Schema as ArrowSchema;
use deltalake::datafusion::dataframe::DataFrame;
use deltalake::datafusion::error::DataFusionError;
use deltalake::datafusion::prelude::{Expr, SessionConfig, SessionContext, cast, ident, lit};
use deltalake::kernel::LogicalFileView;
use deltalake::kernel::transaction::CommitProperties;
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::protocol::SaveMode;
use deltalake::{DeltaTable, checkpoints};
use tokio::sync::Semaphore;
use tracing::warn;

use super::arrow::{self, PARTITION};
use super::expr::{seconds, texts, uint, uints};
use crate::sink::store_error::{EngineError, InvalidStoredValue, Operation, StoreError};
use crate::sink::table::{LEDGER_COLUMNS, Row, TableDef};
use crate::window::UndoWindow;
use crate::wire::envelope::BlockMeta;

/// How many versions of a table pass between the checkpoints the store starts: delta-rs's
/// own default interval.
pub(super) const CHECKPOINT_EVERY: u64 = 100;

/// How many checkpoints run at once. Each replays its table's whole log and holds its
/// state while writing it out, and the first open of a lake whose tables were never
/// checkpointed finds most of them due at once. delta-rs writes a checkpoint on the
/// task that asked for it, holding a runtime worker throughout, so few run at once:
/// opening's own reads run on the same workers.
pub(super) const CONCURRENT_CHECKPOINTS: usize = 2;

/// The Parquet streams each of opening's reads of the newest blocks takes: the ledger,
/// and each table's scan for a stopped commit's rows (`contracts` takes
/// [`CONTRACT_STREAMS`]). The tip writes a small file a commit, so each reads a few dozen
/// small files a table, about 40 of an Ethereum table for a 512-block window, each a
/// round trip of some 275 ms to R2. One stream a table read them a file at a time, 11 s
/// a table: the open's scan of 179 tables took 63 s that way, and 6 s over 16 streams.
pub(super) const STARTUP_STREAMS: usize = 16;

/// The Parquet streams a read of `contracts` takes, its files shared among them whatever
/// their size (see [`Lake::read_over`]). Each row is in the partition of the block that
/// created its contract, so a backfill handing over a factory's whole history leaves a
/// small file in each of thousands of partitions, and one stream a core spent minutes of
/// every restart waiting on the object store.
pub(super) const CONTRACT_STREAMS: usize = 32;

/// One table: its definition, the Arrow schema its batches are built against, and the
/// Delta table as of the last commit this process made to it.
#[derive(Debug)]
pub(super) struct Lake {
    pub(super) def: Arc<TableDef>,
    pub(super) arrow: Arc<ArrowSchema>,
    pub(super) table: DeltaTable,
    /// How many heights one partition covers, when the rows belong to a block.
    pub(super) width: u64,
    /// The newest version a checkpoint was started for, or the table's newest checkpoint
    /// as of opening (zero when it had none): the next is due [`CHECKPOINT_EVERY`]
    /// versions later.
    pub(super) checkpointed: u64,
    /// The permits every table's checkpoints share, [`CONCURRENT_CHECKPOINTS`] of them.
    pub(super) checkpoints: Arc<Semaphore>,
    /// The table read afresh from its newest checkpoint, for the next write to carry on
    /// from in place of `table` (see [`checkpoint_if_due`](Self::checkpoint_if_due)).
    pub(super) reopened: Arc<Mutex<Option<DeltaTable>>>,
}

impl Lake {
    pub(super) fn name(&self) -> &str {
        &self.def.name
    }

    /// Takes `table`, the table as of a commit this process made, and starts a checkpoint
    /// in the background once [`CHECKPOINT_EVERY`] versions have passed since the last.
    ///
    /// delta-rs would write it inside the commit that reaches the interval, rebuilding the
    /// table's state from storage first, which held that commit up for ten seconds and
    /// more. A checkpoint only shortens how much log a later open reads; any version it
    /// is written for is as good, so it runs beside the writer, and a failed one is
    /// logged and left to the next.
    pub(super) fn committed(&mut self, table: DeltaTable) {
        self.table = table;
        self.checkpoint_if_due();
    }

    /// Starts a checkpoint of the table as it stands, in the background, once
    /// [`CHECKPOINT_EVERY`] versions have passed since the last.
    ///
    /// Opening calls it too: a table committed to less often than that between restarts
    /// would otherwise never be checkpointed, and every open would read its whole log.
    /// One a stop interrupted is noticed at the next open and started again.
    ///
    /// Once written, the checkpoint is read back as a table of its own, which the next
    /// write carries on from (see [`base`](Self::base)). delta-rs keeps a table's files
    /// as the batches it read them in, one more for every commit since it was loaded,
    /// and only drops one once all its files are gone: a partition the tip is still
    /// writing to, two weeks of Ethereum, held thousands of batches of a single file,
    /// each a few kilobytes of arrays. The checkpoint holds every file in a few large
    /// batches, so a table read from it costs a fraction of the one it replaces, and
    /// while checkpoints succeed, about a hundred commits pile up on it at most.
    pub(super) fn checkpoint_if_due(&mut self) {
        let version = self.table.version().unwrap_or_default();
        if version.saturating_sub(self.checkpointed) < CHECKPOINT_EVERY {
            return;
        }
        self.checkpointed = version;
        let name = self.def.name.clone();
        let snapshot = self.table.clone();
        let permits = Arc::clone(&self.checkpoints);
        let reopened = Arc::clone(&self.reopened);
        tokio::spawn(async move {
            // Never closed, so a permit always comes.
            let Ok(_permit) = permits.acquire_owned().await else {
                return;
            };
            if let Err(error) = checkpoints::create_checkpoint(&snapshot, None).await {
                warn!(table = %name, version, %error, "lake checkpoint failed; the next is due later");
                return;
            }
            // The table as of the checkpoint is the one being replaced: freed before its
            // replacement is read, so the two are never held at once.
            let log_store = snapshot.log_store();
            drop(snapshot);
            let mut table = DeltaTable::new(log_store);
            if let Err(error) = table.load_version(version).await {
                warn!(table = %name, version, %error, "lake checkpoint unreadable; the next is read later");
                return;
            }
            let mut slot = reopened.lock().unwrap_or_else(PoisonError::into_inner);
            // Checkpoints of one table can finish out of order; the newest is kept.
            if slot.as_ref().and_then(DeltaTable::version) < Some(version) {
                *slot = Some(table);
            }
        });
    }

    /// `rows` as this table's Arrow batch: the CPU half of an append.
    pub(super) fn record_batch(&self, rows: &[&Row]) -> Result<RecordBatch, StoreError> {
        arrow::record_batch(&self.arrow, &self.def, self.width, rows)
            .map_err(StoreError::engine(Operation::Append, Some(self.name())))
    }

    /// The table the next commit starts from: the one a checkpoint left, when one is
    /// waiting, brought up to date with the commits since, which costs a listing of the
    /// log and a read of each of them, once a hundred commits; `table` otherwise, or when
    /// the update fails. A reread is only ever a saving, never a reason for a commit to
    /// fail.
    fn base(&self) -> impl Future<Output = DeltaTable> + Send + 'static {
        let name = self.def.name.clone();
        let current = self.table.clone();
        let reopened = self
            .reopened
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        async move {
            let Some(mut table) = reopened else {
                return current;
            };
            match table.update_state().await {
                Ok(()) => table,
                Err(error) => {
                    warn!(table = %name, %error, "lake checkpoint not brought up to date; committing without it");
                    current
                }
            }
        }
    }

    /// Appends `batches` as one commit of this table's own log, resolving to the table as of
    /// that commit. It owns what it needs, so it can run on a task of its own, where its
    /// Parquet encoding takes a core of its own too.
    pub(super) fn append(
        &self,
        writer: &WriterProperties,
        batches: Vec<RecordBatch>,
    ) -> impl Future<Output = Result<DeltaTable, StoreError>> + Send + 'static {
        let def = Arc::clone(&self.def);
        let writer = writer.clone();
        let base = self.base();
        async move {
            let mut write = base
                .await
                .write(batches)
                .with_save_mode(SaveMode::Append)
                .with_writer_properties(writer)
                .with_commit_properties(commit_properties());
            if arrow::partitioned(&def).is_some() {
                write = write.with_partition_columns([PARTITION]);
            }
            write
                .await
                .map_err(StoreError::engine(Operation::Append, Some(&def.name)))
        }
    }

    /// Deletes the rows `predicate` matches, as one commit of this table's own log.
    pub(super) async fn delete(
        &mut self,
        writer: &WriterProperties,
        predicate: Expr,
    ) -> Result<(), StoreError> {
        let (table, _) = self
            .base()
            .await
            .delete()
            .with_predicate(predicate)
            .with_writer_properties(writer.clone())
            .with_commit_properties(commit_properties())
            .await
            .map_err(StoreError::engine(Operation::Delete, Some(&self.def.name)))?;
        self.committed(table);
        Ok(())
    }

    /// The block hash and height columns, when the rows belong to a block.
    pub(super) fn block_columns(&self) -> Option<(&str, &str)> {
        Some((
            self.def.block_hash.as_deref()?,
            self.def.block_number.as_deref()?,
        ))
    }

    /// Runs `plan` over the table and collects what it returns.
    pub(super) async fn read(
        &self,
        plan: impl FnOnce(DataFrame) -> Result<DataFrame, DataFusionError>,
    ) -> Result<Vec<RecordBatch>, StoreError> {
        self.read_files(None, plan).await
    }

    /// As [`read`](Self::read), over `streams` Parquet streams rather than one a core, its
    /// files shared among them however few bytes they hold: for a read of many small
    /// files, where it mostly waits on the object store. `DataFusion` otherwise reads a
    /// scan of under 10 MB as one stream, a file at a time (see [`STARTUP_STREAMS`]).
    pub(super) async fn read_over(
        &self,
        streams: usize,
        plan: impl FnOnce(DataFrame) -> Result<DataFrame, DataFusionError>,
    ) -> Result<Vec<RecordBatch>, StoreError> {
        self.read_files(Some(streams), plan).await
    }

    /// Runs `plan` over the table and collects what it returns: over `streams` Parquet
    /// streams sharing the files whatever their size (see [`read_over`](Self::read_over)),
    /// or `DataFusion`'s default when absent.
    async fn read_files(
        &self,
        streams: Option<usize>,
        plan: impl FnOnce(DataFrame) -> Result<DataFrame, DataFusionError>,
    ) -> Result<Vec<RecordBatch>, StoreError> {
        let read = async {
            let ctx = streams.map_or_else(SessionContext::new, |streams| {
                SessionContext::new_with_config(
                    SessionConfig::new()
                        .with_target_partitions(streams)
                        .with_repartition_file_min_size(0),
                )
            });
            self.table.update_datafusion_session(&ctx.state())?;
            let provider = self.table.table_provider().await?;
            let frame = ctx.read_table(provider)?;
            Ok::<_, EngineError>(plan(frame)?.collect().await?)
        };
        read.await
            .map_err(StoreError::engine(Operation::Read, Some(self.name())))
    }

    /// The newest partition any of the table's files is in, from its file list rather
    /// than its rows. `None` for a table with no files.
    pub(super) fn newest_partition(&self) -> Result<Option<i64>, StoreError> {
        let mut newest = None;
        self.each_file(|partition, _| {
            newest = newest.max(Some(partition));
            Ok(())
        })?;
        Ok(newest)
    }

    /// Hands each of the table's files, as its file list names them, to `each`, with the
    /// partition it is in.
    pub(super) fn each_file(
        &self,
        mut each: impl FnMut(i64, &LogicalFileView) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        let read = || StoreError::engine(Operation::Read, Some(self.name()));
        let snapshot = self.table.snapshot().map_err(read())?;
        let files = snapshot.snapshot().try_log_data().map_err(read())?;
        for file in files.iter() {
            let values = file.partition_values_map();
            let value = values.get(PARTITION).cloned().flatten().unwrap_or_default();
            each(InvalidStoredValue::parse("block_range", &value)?, &file)?;
        }
        Ok(())
    }

    /// The lowest height of the newest blocks [`read_ledger`](Self::read_ledger) needs for
    /// `window`, as the statistics of the files of the partitions from `oldest` on say;
    /// `None` when they hold no file or one has no statistics to tell by.
    fn ledger_floor(&self, oldest: i64, window: UndoWindow) -> Result<Option<u64>, StoreError> {
        let mut files = Vec::new();
        let mut described = true;
        self.each_file(|partition, file| {
            if partition < oldest {
                return Ok(());
            }
            match file.stats().and_then(|stats| LedgerFile::of(&stats)) {
                Some(file) => files.push(file),
                None => described = false,
            }
            Ok(())
        })?;
        Ok((described && !files.is_empty()).then(|| ledger_floor(files, window.ledger())))
    }

    /// The newest [`UndoWindow::ledger`] accepted blocks of this table, the ledger, oldest
    /// first.
    ///
    /// Read from the ledger's newest two partitions alone, which hold at least the window
    /// (a partition is never narrower), so a restart does not scan every file the
    /// ledger ever wrote; and of those, the rows at or above the lowest height the window
    /// can reach, as the file list's statistics say (see [`ledger_floor`]), since the tip
    /// writes a small file a commit and a partition holds far more of them than a narrow
    /// window needs. Filtered on the height rather than handed the files, so the scan
    /// skips the others by their statistics and still shares the rest among
    /// [`STARTUP_STREAMS`] streams: delta-rs reads a scan restricted to named files as one
    /// stream, which took 8 s for Ethereum's 40 files, and 1.5 s filtered. A partition
    /// with a file its log records no statistics for is read whole.
    pub(super) async fn read_ledger(
        &self,
        window: UndoWindow,
    ) -> Result<Vec<BlockMeta>, StoreError> {
        let oldest = self.newest_partition()?.map(|newest| newest - 1);
        let floor = match oldest {
            Some(oldest) => self.ledger_floor(oldest, window)?,
            None => None,
        };
        let [height, hash, parent_hash, timestamp] = LEDGER_COLUMNS;
        let batches = self
            .read_files(Some(STARTUP_STREAMS), |frame| {
                let frame = match oldest {
                    Some(oldest) => frame.filter(ident(PARTITION).gt_eq(lit(oldest)))?,
                    None => frame,
                };
                let frame = match floor {
                    Some(floor) => frame.filter(ident(height).gt_eq(uint(floor)))?,
                    None => frame,
                };
                frame
                    .sort(vec![ident(height).sort(false, false)])?
                    .limit(0, Some(window.ledger()))?
                    .select(vec![
                        cast(ident(height), ArrowType::UInt64),
                        cast(ident(hash), ArrowType::Utf8),
                        cast(ident(parent_hash), ArrowType::Utf8),
                        seconds(timestamp),
                    ])
            })
            .await?;
        let mut ledger = Vec::new();
        for batch in &batches {
            let heights = uints(batch, 0, "accepted_blocks.height")?;
            let hashes = texts(batch, 1, "accepted_blocks.hash")?;
            let parents = texts(batch, 2, "accepted_blocks.parent_hash")?;
            let times = uints(batch, 3, "accepted_blocks.timestamp")?;
            for row in 0..batch.num_rows() {
                ledger.push(BlockMeta {
                    height: heights.value(row),
                    hash: InvalidStoredValue::parse("accepted_blocks.hash", hashes.value(row))?,
                    parent_hash: InvalidStoredValue::parse(
                        "accepted_blocks.parent_hash",
                        parents.value(row),
                    )?,
                    timestamp: times.value(row),
                });
            }
        }
        ledger.sort_by_key(|block| block.height);
        Ok(ledger)
    }
}

/// One of the ledger's files, as its statistics describe it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LedgerFile {
    /// Its lowest height.
    low: u64,
    /// Its highest height.
    high: u64,
    /// How many rows it holds.
    rows: u64,
}

impl LedgerFile {
    /// A file's description from the statistics Delta wrote for it, `None` when they do
    /// not say.
    fn of(stats: &str) -> Option<Self> {
        let stats: serde_json::Value = serde_json::from_str(stats).ok()?;
        let height = |bound: &str| stats.get(bound)?.get(LEDGER_COLUMNS[0])?.as_u64();
        Some(Self {
            low: height("minValues")?,
            high: height("maxValues")?,
            rows: stats.get("numRecords")?.as_u64()?,
        })
    }
}

/// The lowest height the newest `rows` rows of `files` can have: every one of them is at
/// or above it.
///
/// A height is in the ledger once, so the newest files together holding `rows` rows hold
/// that many at or above their lowest height, and no row below it is among the newest.
/// Any other file reaching that height may hold some of them too, and a filter on the
/// height keeps those. Counts rows rather than subtracting from the tip, so a gap in the
/// heights never comes back short: a ledger shorter than asked for reads as the store's
/// whole history, from zero.
fn ledger_floor(mut files: Vec<LedgerFile>, rows: usize) -> u64 {
    files.sort_by_key(|file| std::cmp::Reverse(file.high));
    let wanted = u64::try_from(rows).unwrap_or(u64::MAX);
    let (mut held, mut floor) = (0, 0);
    for file in &files {
        held += file.rows;
        floor = if held == file.rows {
            file.low
        } else {
            floor.min(file.low)
        };
        if held >= wanted {
            return floor;
        }
    }
    0
}

/// How every commit to the lake is made, by the store and by `maintenance` alike: without
/// delta-rs's cleanup of expired log entries, and without its checkpoints.
///
/// delta-rs otherwise runs that cleanup after every commit, and it lists the table's whole
/// `_delta_log` to do it, a list request per thousand entries. A table committed to at the
/// tip gains thousands of entries a day and keeps a month of them, so each commit would
/// cost tens of requests, and more every day. `maintenance` cleans each log once a day
/// instead. Checkpoints are started beside the writer (see [`Lake::committed`]).
pub(super) fn commit_properties() -> CommitProperties {
    CommitProperties::default()
        .with_cleanup_expired_logs(Some(false))
        .with_create_checkpoint(false)
}

#[cfg(test)]
mod tests {
    use super::{LedgerFile, ledger_floor};

    fn file(low: u64, high: u64, rows: u64) -> LedgerFile {
        LedgerFile { low, high, rows }
    }

    /// The floor is the newest files' lowest height, however many files sit below them.
    #[test]
    fn the_floor_is_where_the_newest_rows_begin() {
        let commits: Vec<_> = (0..100).map(|n| file(n * 10, n * 10 + 9, 10)).collect();
        assert_eq!(ledger_floor(commits, 25), 970);
    }

    /// A compacted file reaching below the commits above it lowers the floor once the
    /// newest rows reach into it.
    #[test]
    fn a_compacted_file_lowers_the_floor_when_the_newest_rows_reach_it() {
        let files = vec![file(0, 99, 100), file(100, 109, 10)];
        assert_eq!(ledger_floor(files.clone(), 5), 100);
        assert_eq!(ledger_floor(files, 11), 0);
    }

    /// A file below the newest by its highest height, but reaching above their floor, may
    /// hold some of the newest rows: a reorg's delete rewrites a file with what it keeps.
    /// The floor is the newest files' own, and a filter on it keeps that file's rows above.
    #[test]
    fn an_overlapping_file_leaves_the_floor_where_the_newest_files_put_it() {
        let files = vec![file(20, 30, 6), file(18, 25, 4), file(10, 19, 10)];
        assert_eq!(ledger_floor(files, 6), 20);
    }

    /// Too few rows anywhere reads from zero, and a gap in the heights lowers the floor
    /// rather than losing a row: the count, not the tip, decides.
    #[test]
    fn a_short_table_or_a_gap_never_comes_back_short() {
        let files = vec![file(1, 5, 5), file(6, 9, 4)];
        assert_eq!(ledger_floor(files, 100), 0);

        let gapped = vec![file(1, 5, 5), file(1_000, 1_003, 4)];
        assert_eq!(ledger_floor(gapped, 5), 1);
    }

    /// The statistics Delta writes describe a file; ones that do not say leave it
    /// undescribed.
    #[test]
    fn a_file_is_described_by_its_statistics() {
        let stats = r#"{"numRecords":3,"minValues":{"height":7},"maxValues":{"height":9}}"#;
        assert_eq!(LedgerFile::of(stats), Some(file(7, 9, 3)));
        assert_eq!(LedgerFile::of(r#"{"numRecords":3}"#), None);
        assert_eq!(LedgerFile::of("not json"), None);
    }
}
