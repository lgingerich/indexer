//! One table of the lake: its handle, how every commit to it is made, and the
//! checkpoints started beside the writer.

use std::sync::{Arc, Mutex, PoisonError};

use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::datatypes::Schema as ArrowSchema;
use deltalake::datafusion::dataframe::DataFrame;
use deltalake::datafusion::error::DataFusionError;
use deltalake::datafusion::prelude::{Expr, SessionContext};
use deltalake::kernel::transaction::CommitProperties;
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::protocol::SaveMode;
use deltalake::{DeltaTable, checkpoints};
use tokio::sync::Semaphore;
use tracing::warn;

use super::arrow::{self, PARTITION};
use super::expr::parse;
use crate::sink::store_error::{EngineError, Operation, StoreError};
use crate::sink::table::{Row, TableDef};

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
        let read = async {
            let ctx = SessionContext::new();
            self.table.update_datafusion_session(&ctx.state())?;
            let frame = ctx.read_table(self.table.table_provider().await?)?;
            Ok::<_, EngineError>(plan(frame)?.collect().await?)
        };
        read.await
            .map_err(StoreError::engine(Operation::Read, Some(self.name())))
    }

    /// The newest partition any of the table's files is in, from its file list rather
    /// than its rows. `None` for a table with no files.
    pub(super) fn newest_partition(&self) -> Result<Option<i64>, StoreError> {
        let read = || StoreError::engine(Operation::Read, Some(self.name()));
        let snapshot = self.table.snapshot().map_err(read())?;
        let files = snapshot.snapshot().try_log_data().map_err(read())?;
        let mut newest = None;
        for file in files.iter() {
            let values = file.partition_values_map();
            let value = values.get(PARTITION).cloned().flatten().unwrap_or_default();
            let partition: i64 = parse("accepted_blocks.block_range", &value)?;
            newest = newest.max(Some(partition));
        }
        Ok(newest)
    }
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
