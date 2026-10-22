//! Keeping the lake cheap to read: compacting the partitions the tip has left behind,
//! and deleting the files no commit references any more.
//!
//! At the tip every commit writes a small file per table and partition it touches, a few
//! thousand a day per table, and a reorg's delete rewrites files and leaves the old ones
//! behind. Nothing else merges or removes them, so this task does, beside the writer:
//!
//! - **Compaction.** A partition is finished once the tip is past it by more than the
//!   reorg window: nothing appends to it or deletes from it again. Each finished
//!   partition still holding small files is rewritten into a few large ones, once.
//!   Tables with no partition, `contracts` and `reorgs`, are left alone: a reorg can
//!   still delete from any of their files.
//! - **Vacuum.** Files no commit references, and that have not been referenced for
//!   [`vacuum_retention_hours`](super::DeltaSettings::vacuum_retention_hours), are
//!   deleted. The retention is how long a reader may still be reading an older version.
//! - **Log cleanup.** Entries of the transaction log older than the same retention, and
//!   older than a checkpoint that replaces them, are deleted, once a day. delta-rs would
//!   otherwise do it after every commit, listing the whole log each time (see
//!   [`commit_properties`](super::lake::commit_properties)).
//!
//! Each pass reads every table afresh from its newest checkpoint and lets it go once
//! done: a table kept from one pass to the next grows by a batch for every commit it is
//! brought up to date with (see [`Lake::checkpoint_if_due`](super::lake::Lake::checkpoint_if_due)).
//!
//! It is a second writer to the same tables, which the lake's one-writer rule allows
//! because the two never touch the same file: the writer appends to the newest partition
//! and deletes only within the reorg window, and compaction rewrites only finished
//! partitions. Delta's commit conflict check sees that and retries the version race. A
//! failure is logged and tried again on the next pass; it never stops ingest.

use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use deltalake::checkpoints::cleanup_expired_logs_for;
use deltalake::logstore::LogStoreRef;
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::{DeltaTable, DeltaTableError, FilterOp, FilterValue};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{info, warn};

use super::arrow::PARTITION;
use super::lake::commit_properties;
use crate::metrics;
use crate::window::MAX_UNFINALIZED_BLOCKS;

/// How often the task looks for finished partitions to compact.
const COMPACT_EVERY: Duration = Duration::from_hours(1);

/// How often the task vacuums every table and cleans its log.
const VACUUM_EVERY: Duration = Duration::from_hours(24);

/// The file size compaction writes towards.
const TARGET_FILE_BYTES: NonZeroU64 = match NonZeroU64::new(128 * 1024 * 1024) {
    Some(bytes) => bytes,
    None => NonZeroU64::MIN,
};

/// How many output files one compaction writes at once. delta-rs defaults to one per
/// core, each holding a decoded partition's worth of rows on its way to a
/// [`TARGET_FILE_BYTES`] file, which beside the running writer is a peak of gigabytes.
/// Compaction runs off the write path, so it can take longer instead.
const COMPACTION_TASKS: usize = 1;

/// A file under this is small: one a tip commit wrote, not one compaction did. A
/// compacted partition holds at most one, the remainder of its last bin.
const SMALL_FILE_BYTES: i64 = 32 * 1024 * 1024;

/// The maintenance task, stopped when the store that started it is dropped.
#[derive(Debug)]
pub(super) struct Maintenance(JoinHandle<()>);

impl Drop for Maintenance {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Starts maintaining `tables`, each by name, the log store it is read through, and the
/// partition width it was opened with, against the committed tip the writer publishes in
/// `tip`. Files go after `retention`.
pub(super) fn spawn(
    tables: Vec<(String, LogStoreRef, u64)>,
    tip: Arc<AtomicU64>,
    retention: Duration,
    writer: WriterProperties,
) -> Maintenance {
    Maintenance(tokio::spawn(run(tables, tip, retention, writer)))
}

async fn run(
    tables: Vec<(String, LogStoreRef, u64)>,
    tip: Arc<AtomicU64>,
    retention: Duration,
    writer: WriterProperties,
) {
    let mut passes = tokio::time::interval_at(Instant::now() + COMPACT_EVERY, COMPACT_EVERY);
    passes.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut vacuumed = Instant::now();
    loop {
        passes.tick().await;
        let vacuum = vacuumed.elapsed() >= VACUUM_EVERY;
        for (name, log_store, width) in &tables {
            let mut table = DeltaTable::new(Arc::clone(log_store));
            if let Err(error) = table.load().await {
                metrics::lake_maintenance_failed("load");
                warn!(table = %name, %error, "lake table unreadable for maintenance; retrying next pass");
                continue;
            }
            if let Err(error) = compact(
                name,
                &mut table,
                *width,
                tip.load(Ordering::Acquire),
                &writer,
            )
            .await
            {
                metrics::lake_maintenance_failed("compact");
                warn!(table = %name, %error, "lake compaction failed; retrying next pass");
            }
            if vacuum && let Err(error) = self::vacuum(name, &mut table, retention).await {
                metrics::lake_maintenance_failed("vacuum");
                warn!(table = %name, %error, "lake vacuum failed; retrying next pass");
            }
            if vacuum && let Err(error) = clean_log(name, &mut table, retention).await {
                metrics::lake_maintenance_failed("log_cleanup");
                warn!(table = %name, %error, "lake log cleanup failed; retrying next pass");
            }
        }
        if vacuum {
            vacuumed = Instant::now();
        }
    }
}

/// Compacts every partition of the table `name`, `width` heights wide, the committed
/// `tip` has finished that still holds small files, returning how many it compacted. A
/// table with no partition has none to compact.
///
/// The width is the one opening checked the table records, so it is not read again here.
pub(super) async fn compact(
    name: &str,
    table: &mut DeltaTable,
    width: u64,
    tip: u64,
    writer: &WriterProperties,
) -> Result<usize, DeltaTableError> {
    table.update_state().await?;
    let mut small: BTreeMap<i64, usize> = BTreeMap::new();
    for file in table.snapshot()?.snapshot().try_log_data()?.iter() {
        let Some(Some(partition)) = file.partition_values_map().remove(PARTITION) else {
            continue;
        };
        let Ok(partition) = partition.parse::<i64>() else {
            continue;
        };
        if file.size() < SMALL_FILE_BYTES && finished(partition, width, tip) {
            *small.entry(partition).or_default() += 1;
        }
    }
    let mut compacted = 0;
    for (partition, files) in small.into_iter().filter(|(_, files)| *files > 1) {
        let value = partition.to_string();
        let filters = [(PARTITION, FilterOp::Eq, FilterValue::Scalar(&value))];
        let (optimized, metrics) = table
            .clone()
            .optimize()
            .with_filters(&filters)
            .with_target_size(TARGET_FILE_BYTES)
            .with_max_concurrent_tasks(COMPACTION_TASKS)
            .with_writer_properties(writer.clone())
            .with_commit_properties(commit_properties())
            .await?;
        *table = optimized;
        compacted += 1;
        info!(
            table = name,
            partition,
            small_files = files,
            removed = metrics.num_files_removed,
            added = metrics.num_files_added,
            "lake partition compacted"
        );
    }
    Ok(compacted)
}

/// Whether nothing at `tip` or after can still write to `partition`, `width` heights
/// wide: the tip is past its last height by more than the deepest reorg the pipeline
/// retracts.
fn finished(partition: i64, width: u64, tip: u64) -> bool {
    let Ok(partition) = u64::try_from(partition) else {
        return false;
    };
    (partition + 1)
        .saturating_mul(width)
        .saturating_add(MAX_UNFINALIZED_BLOCKS as u64)
        <= tip
}

/// Deletes the files of the table `name` no commit has referenced for `retention`,
/// returning how many.
pub(super) async fn vacuum(
    name: &str,
    table: &mut DeltaTable,
    retention: Duration,
) -> Result<usize, DeltaTableError> {
    table.update_state().await?;
    let retention = chrono::Duration::from_std(retention).unwrap_or(chrono::Duration::MAX);
    let (vacuumed, metrics) = table
        .clone()
        .vacuum()
        .with_retention_period(retention)
        // The settings' retention is the one that counts, not the table's default.
        .with_enforce_retention_duration(false)
        .with_commit_properties(commit_properties())
        .await?;
    *table = vacuumed;
    if !metrics.files_deleted.is_empty() {
        info!(
            table = name,
            files = metrics.files_deleted.len(),
            "lake files vacuumed"
        );
    }
    Ok(metrics.files_deleted.len())
}

/// Deletes the entries of the table `name`'s transaction log older than `retention`,
/// returning how many. Only entries a checkpoint already replaces go, so every version
/// newer than the oldest checkpoint kept can still be read; the files of older ones are
/// vacuumed after the same `retention` anyway.
pub(super) async fn clean_log(
    name: &str,
    table: &mut DeltaTable,
    retention: Duration,
) -> Result<usize, DeltaTableError> {
    table.update_state().await?;
    let Some(version) = table.version() else {
        return Ok(0);
    };
    let cutoff = SystemTime::now()
        .checked_sub(retention)
        .and_then(|cutoff| cutoff.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map_or(0, |cutoff| {
            i64::try_from(cutoff.as_millis()).unwrap_or(i64::MAX)
        });
    let cleaned =
        cleanup_expired_logs_for(version, table.log_store().as_ref(), cutoff, None).await?;
    if cleaned > 0 {
        info!(table = name, entries = cleaned, "lake log cleaned");
    }
    Ok(cleaned)
}
