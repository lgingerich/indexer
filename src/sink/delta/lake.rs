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
use super::expr::{seconds, texts, uints};
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
        self.read_files(None, None, plan).await
    }

    /// As [`read`](Self::read), over `streams` Parquet streams rather than one a core: for
    /// a table of many small files, where a read mostly waits on the object store.
    pub(super) async fn read_over(
        &self,
        streams: usize,
        plan: impl FnOnce(DataFrame) -> Result<DataFrame, DataFusionError>,
    ) -> Result<Vec<RecordBatch>, StoreError> {
        self.read_files(None, Some(streams), plan).await
    }

    /// Runs `plan` over the table, or only its files `paths` when given, over `streams`
    /// Parquet streams, one a core when absent, and collects what it returns.
    async fn read_files(
        &self,
        paths: Option<Vec<String>>,
        streams: Option<usize>,
        plan: impl FnOnce(DataFrame) -> Result<DataFrame, DataFusionError>,
    ) -> Result<Vec<RecordBatch>, StoreError> {
        let read = async {
            let ctx = streams.map_or_else(SessionContext::new, |streams| {
                SessionContext::new_with_config(
                    SessionConfig::new().with_target_partitions(streams),
                )
            });
            self.table.update_datafusion_session(&ctx.state())?;
            let provider = self.table.table_provider();
            let provider = match paths {
                Some(paths) => provider.with_file_paths(paths).await?,
                None => provider.await?,
            };
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

    /// The files of the partitions from `oldest` on that
    /// [`read_ledger`](Self::read_ledger) needs for `window`, `None` when they are none or
    /// one has no statistics to tell by.
    fn ledger_paths(
        &self,
        oldest: i64,
        window: UndoWindow,
    ) -> Result<Option<Vec<String>>, StoreError> {
        let mut files = Vec::new();
        let mut described = true;
        self.each_file(|partition, file| {
            if partition < oldest {
                return Ok(());
            }
            let path = file.path().into_owned();
            match file.stats().and_then(|stats| LedgerFile::of(path, &stats)) {
                Some(file) => files.push(file),
                None => described = false,
            }
            Ok(())
        })?;
        Ok((described && !files.is_empty()).then(|| ledger_files(files, window.ledger())))
    }

    /// The newest [`UndoWindow::ledger`] accepted blocks of this table, the ledger, oldest
    /// first.
    ///
    /// Read from the ledger's newest two partitions alone, which hold at least the window
    /// (a partition is never narrower), so a restart does not scan every file the
    /// ledger ever wrote; and of those, the files that can hold the blocks, as the file
    /// list's statistics say (see [`ledger_files`]), since the tip writes a small file a
    /// commit and a partition holds far more of them than a narrow window needs. A
    /// partition with a file its log records no statistics for is read whole.
    pub(super) async fn read_ledger(
        &self,
        window: UndoWindow,
    ) -> Result<Vec<BlockMeta>, StoreError> {
        let oldest = self.newest_partition()?.map(|newest| newest - 1);
        let paths = match oldest {
            Some(oldest) => self.ledger_paths(oldest, window)?,
            None => None,
        };
        let [height, hash, parent_hash, timestamp] = LEDGER_COLUMNS;
        let batches = self
            .read_files(paths, None, |frame| {
                let frame = match oldest {
                    Some(oldest) => frame.filter(ident(PARTITION).gt_eq(lit(oldest)))?,
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
    path: String,
    /// Its lowest height.
    low: u64,
    /// Its highest height.
    high: u64,
    /// How many rows it holds.
    rows: u64,
}

impl LedgerFile {
    /// `path`'s description from the statistics Delta wrote for it, `None` when they do
    /// not say.
    fn of(path: String, stats: &str) -> Option<Self> {
        let stats: serde_json::Value = serde_json::from_str(stats).ok()?;
        let height = |bound: &str| stats.get(bound)?.get(LEDGER_COLUMNS[0])?.as_u64();
        Some(Self {
            path,
            low: height("minValues")?,
            high: height("maxValues")?,
            rows: stats.get("numRecords")?.as_u64()?,
        })
    }
}

/// The paths of `files` the newest `rows` rows lie in.
///
/// A height is in the ledger once, so the newest files together holding `rows` rows hold
/// that many at or above their lowest height, and no row below it is among the newest.
/// Any other file reaching that height may hold some of them too. Counts rows rather
/// than subtracting from the tip, so a gap in the heights never comes back short: a
/// ledger shorter than asked for reads as the store's whole history.
fn ledger_files(mut files: Vec<LedgerFile>, rows: usize) -> Vec<String> {
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
            break;
        }
    }
    if held < wanted {
        floor = 0;
    }
    files
        .into_iter()
        .filter(|file| file.high >= floor)
        .map(|file| file.path)
        .collect()
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
    use super::{LedgerFile, ledger_files};

    fn file(path: &str, low: u64, high: u64, rows: u64) -> LedgerFile {
        LedgerFile {
            path: path.to_owned(),
            low,
            high,
            rows,
        }
    }

    fn sorted(mut paths: Vec<String>) -> Vec<String> {
        paths.sort();
        paths
    }

    /// Only the newest files that hold the rows are read, however many sit below them.
    #[test]
    fn the_ledger_reads_only_the_files_holding_its_newest_rows() {
        let commits: Vec<_> = (0..100)
            .map(|n| file(&format!("{n:03}"), n * 10, n * 10 + 9, 10))
            .collect();
        assert_eq!(sorted(ledger_files(commits, 25)), ["097", "098", "099"]);
    }

    /// A compacted file reaching the floor is read with the commits above it.
    #[test]
    fn a_compacted_file_is_read_when_the_newest_rows_reach_it() {
        let files = vec![file("big", 0, 99, 100), file("a", 100, 109, 10)];
        assert_eq!(sorted(ledger_files(files.clone(), 5)), ["a"]);
        assert_eq!(sorted(ledger_files(files, 11)), ["a", "big"]);
    }

    /// A file below the newest by its highest height, but reaching above their floor, may
    /// hold some of the newest rows: a reorg's delete rewrites a file with what it keeps.
    #[test]
    fn an_overlapping_file_is_read_too() {
        let files = vec![
            file("new", 20, 30, 6),
            file("rewritten", 18, 25, 4),
            file("old", 10, 19, 10),
        ];
        assert_eq!(sorted(ledger_files(files, 6)), ["new", "rewritten"]);
    }

    /// Too few rows anywhere reads everything, and a gap in the heights costs a file
    /// rather than a row: the count, not the tip, decides.
    #[test]
    fn a_short_table_or_a_gap_never_comes_back_short() {
        let files = vec![file("a", 1, 5, 5), file("b", 6, 9, 4)];
        assert_eq!(sorted(ledger_files(files, 100)), ["a", "b"]);

        let gapped = vec![file("old", 1, 5, 5), file("new", 1_000, 1_003, 4)];
        assert_eq!(sorted(ledger_files(gapped, 5)), ["new", "old"]);
    }

    /// The statistics Delta writes describe a file; ones that do not say leave it
    /// undescribed.
    #[test]
    fn a_file_is_described_by_its_statistics() {
        let stats = r#"{"numRecords":3,"minValues":{"height":7},"maxValues":{"height":9}}"#;
        assert_eq!(
            LedgerFile::of("p".to_owned(), stats),
            Some(file("p", 7, 9, 3))
        );
        assert_eq!(LedgerFile::of("p".to_owned(), r#"{"numRecords":3}"#), None);
        assert_eq!(LedgerFile::of("p".to_owned(), "not json"), None);
    }
}
