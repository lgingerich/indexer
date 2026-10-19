//! **Temporary.** The one-off rewrite of a lake to another partition width, kept until
//! Ethereum's lake is rewritten to its narrower partitions. Then this module, the
//! `repartition` subcommand (`main.rs`, `runtime::repartition`, and
//! `RuntimeError::{NoLake, Repartition}`), and its tests (`tests/repartition.rs`) are
//! deleted; nothing else uses them.
//!
//! A table records the width it was written with in its [`PARTITION_BLOCKS`] property,
//! and the store refuses to open one whose width is not the settings'. [`repartition`]
//! changes it: every partitioned table of the run, one at a time and one old partition
//! at a time, is read back and written again as the new partitions, and each old
//! partition's files are swapped for their replacements in one commit. The table stays
//! where it is, its partitions in the same `block_range=<n>` directories, and every row
//! is kept: the replaced files are vacuumed after the retention like any compaction's.
//! Every file it writes keeps statistics on the columns the store names (see
//! [`stats_columns`](super::stats_columns)), whatever the files it replaces kept.
//!
//! # Resuming
//!
//! The first commit of a rewrite replaces the table's width with a marker naming both
//! widths, such as `rewriting from 100000 to 7200`, which is no width, so the store
//! refuses the table until the rewrite is done. Each partition's commit records in
//! [`REPARTITION_DONE`] the old partition it replaced, and a rewrite that stopped carries
//! on from there. The last commit records the new width and takes the progress out.
//!
//! Old partitions are rewritten in the order that keeps the two numberings apart:
//! narrowing, the new partitions of any old one are numbered at or above it, so the old
//! ones go from the newest down, and every one still to do is below every new one;
//! widening, the other way round. So the partitions still old are exactly those on the
//! far side of [`REPARTITION_DONE`].
//!
//! The lake's writer must be stopped first, and stay stopped: one that opened before the
//! rewrite started would keep appending at the old width. [`repartition`] refuses a table
//! committed to in the last [`QUIET`].

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use deltalake::arrow::array::RecordBatch;
use deltalake::datafusion::prelude::{Expr, SessionContext, cast, ident, lit};
use deltalake::kernel::transaction::CommitBuilder;
use deltalake::kernel::{Action, MetadataExt as _};
use deltalake::operations::write::WriterStatsConfig;
use deltalake::operations::write::writer::{DeltaWriter, WriterConfig};
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::{DeltaTable, DeltaTableError, Path, checkpoints, ensure_table_uri};
use futures_util::stream::{StreamExt as _, TryStreamExt as _};
use tracing::info;

use super::arrow::{self, DEFAULT_PARTITION_BLOCKS, PARTITION, PARTITION_BLOCKS};
use super::{DeltaSettings, Storage, check, commit_properties, keep_stats, lake_writer, millis};
use crate::sink::store_error::{EngineError, InvalidStoredValue, Operation, StoreError};
use crate::sink::table::{Schema, TableDef};

/// The table property naming the old partition a rewrite under way replaced last.
pub(super) const REPARTITION_DONE: &str = "indexer.repartition_done";

/// How long a table must have gone without a commit before it is rewritten: longer
/// than any commit interval a writer runs with, so a writer still running is noticed.
pub(super) const QUIET: Duration = Duration::from_mins(15);

/// The size the rewrite writes its files towards, as compaction does.
const TARGET_FILE_BYTES: u64 = 128 * 1024 * 1024;

/// How a partitioned table is laid out, as its properties record it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Layout {
    /// Every partition is this many heights wide.
    Width(u64),
    /// A rewrite from `from` to `to` heights a partition is under way, and has replaced
    /// every old partition up to `done`, when it has replaced any.
    Rewriting {
        from: u64,
        to: u64,
        done: Option<i64>,
    },
}

/// What [`PARTITION_BLOCKS`] holds while a rewrite from `from` to `to` is under way.
fn marker(from: u64, to: u64) -> String {
    format!("rewriting from {from} to {to}")
}

fn layout(table: &DeltaTable) -> Result<Layout, StoreError> {
    let snapshot = table
        .snapshot()
        .map_err(StoreError::engine(Operation::Open, None))?;
    let properties = snapshot.metadata().configuration();
    let Some(recorded) = properties.get(PARTITION_BLOCKS) else {
        return Ok(Layout::Width(DEFAULT_PARTITION_BLOCKS));
    };
    if let Ok(width) = recorded.parse() {
        return Ok(Layout::Width(width));
    }
    let widths = recorded
        .strip_prefix("rewriting from ")
        .and_then(|widths| widths.split_once(" to "))
        .and_then(|(from, to)| Some((from.parse().ok()?, to.parse().ok()?)));
    let Some((from, to)) = widths else {
        return Err(InvalidStoredValue::new(PARTITION_BLOCKS, recorded).into());
    };
    let done = properties
        .get(REPARTITION_DONE)
        .map(|done| parse(REPARTITION_DONE, done))
        .transpose()?;
    Ok(Layout::Rewriting { from, to, done })
}

fn parse<T: std::str::FromStr>(property: &'static str, value: &str) -> Result<T, StoreError> {
    value
        .parse()
        .map_err(|_| InvalidStoredValue::new(property, value).into())
}

/// Rewrites every partitioned table of `schema` under `<uri>/<chain>/` to the settings'
/// [`partition_blocks`](DeltaSettings::partition_blocks), carrying on from a rewrite that
/// stopped. A table not yet created, or already that wide, is left alone.
///
/// # Errors
///
/// Returns [`StoreError::Drift`] when a table does not match its definition, is being
/// rewritten to another width, or was committed to in the last 15 minutes, and
/// [`StoreError::Engine`] when a table cannot be read or written.
pub async fn repartition(
    settings: &DeltaSettings,
    schema: &Schema,
    chain: &str,
) -> Result<(), StoreError> {
    rewrite_lake(settings, schema, chain, QUIET).await
}

/// [`repartition`], refusing a table committed to in the last `quiet`.
///
/// One table at a time: a table's partition is read with every core already, and the
/// rewrite of one partition of `blocks` peaked at 5 GB, so several at once only adds up
/// their memory.
pub(super) async fn rewrite_lake(
    settings: &DeltaSettings,
    schema: &Schema,
    chain: &str,
    quiet: Duration,
) -> Result<(), StoreError> {
    let root = settings.uri.trim_end_matches('/');
    let storage = Storage::new(root, &settings.storage)?;
    let writer = lake_writer();
    let to = settings.partition_blocks;
    for def in schema
        .tables()
        .iter()
        .filter(|def| arrow::partitioned(def).is_some())
    {
        let uri = format!("{root}/{chain}/{}", def.name);
        rewrite_table(&storage, uri, def, to, quiet, &writer).await?;
    }
    Ok(())
}

/// Whether a table has been created at `uri`: a log entry exists. Asked first, since
/// delta-rs logs an error for every table it is asked to load that is not there.
async fn exists(storage: &Storage, uri: &str) -> Result<bool, StoreError> {
    let url = ensure_table_uri(uri).map_err(StoreError::engine(Operation::Open, Some(uri)))?;
    let log = Path::from_url_path(url.path())
        .map_err(StoreError::engine(Operation::Open, Some(uri)))?
        .join("_delta_log");
    let first = storage.root.list(Some(&log)).next().await;
    first
        .transpose()
        .map(|entry| entry.is_some())
        .map_err(StoreError::engine(Operation::Open, Some(uri)))
}

/// Rewrites the table of `def` at `uri` to `to` heights a partition.
async fn rewrite_table(
    storage: &Storage,
    uri: String,
    def: &Arc<TableDef>,
    to: u64,
    quiet: Duration,
    writer: &WriterProperties,
) -> Result<(), StoreError> {
    let name = def.name.as_str();
    let engine = |operation| StoreError::engine(operation, Some(name));
    if !exists(storage, &uri).await? {
        info!(
            table = name,
            "lake table not created yet; nothing to rewrite"
        );
        return Ok(());
    }
    let mut table = storage.table(&uri).await.map_err(engine(Operation::Open))?;
    let expected =
        arrow::delta_schema(def).map_err(StoreError::engine(Operation::Open, Some(name)))?;
    check(&table, &expected, name)?;
    let drift = |difference: String| StoreError::Drift {
        table: name.to_owned(),
        difference,
    };
    let (from, done) = match layout(&table)? {
        Layout::Width(from) if from == to => {
            info!(table = name, width = to, "lake table already that wide");
            return Ok(());
        }
        Layout::Width(from) => {
            refuse_if_written(&table, name, quiet).await?;
            start(&mut table, name, to).await?;
            (from, None)
        }
        Layout::Rewriting {
            from,
            to: target,
            done,
        } if target == to => (from, done),
        Layout::Rewriting { to: target, .. } => {
            return Err(drift(format!(
                "a rewrite to {target} heights a partition is under way, not to {to}"
            )));
        }
    };
    // Before any file is written, so every rewritten file keeps statistics on them.
    keep_stats(&mut table, def).await?;
    let narrowing = to < from;
    let mut old: Vec<i64> = partitions(&table, name)?
        .into_iter()
        .filter(|partition| match done {
            None => true,
            Some(done) if narrowing => *partition < done,
            Some(done) => *partition > done,
        })
        .collect();
    if narrowing {
        old.reverse();
    }
    info!(
        table = name,
        from,
        to,
        partitions = old.len(),
        resumed_after = ?done,
        "rewriting lake table"
    );
    for partition in old {
        rewrite_partition(&mut table, def, partition, to, writer).await?;
    }
    let finished = metadata(
        &table,
        &[
            (PARTITION_BLOCKS, Some(to.to_string())),
            (REPARTITION_DONE, None),
        ],
    )?;
    commit(
        &mut table,
        vec![finished],
        properties_set(PARTITION_BLOCKS, to.to_string()),
    )
    .await
    .map_err(engine(Operation::Append))?;
    // So the first open after the rewrite reads one checkpoint, not its commits.
    checkpoints::create_checkpoint(&table, None)
        .await
        .map_err(engine(Operation::Append))?;
    info!(table = name, width = to, "lake table rewritten");
    Ok(())
}

/// Records in the table `name` that a rewrite from its width to `to` heights a partition
/// is under way, which the store refuses to open.
pub(super) async fn start(table: &mut DeltaTable, name: &str, to: u64) -> Result<(), StoreError> {
    let from = match layout(table)? {
        Layout::Width(from) | Layout::Rewriting { from, .. } => from,
    };
    let started = marker(from, to);
    let action = metadata(table, &[(PARTITION_BLOCKS, Some(started.clone()))])?;
    commit(
        table,
        vec![action],
        properties_set(PARTITION_BLOCKS, started),
    )
    .await
    .map_err(StoreError::engine(Operation::Append, Some(name)))
}

/// Fails when `table` was committed to in the last `quiet`: its writer may still run.
async fn refuse_if_written(
    table: &DeltaTable,
    name: &str,
    quiet: Duration,
) -> Result<(), StoreError> {
    let latest = table
        .history(Some(1))
        .try_next()
        .await
        .map_err(StoreError::engine(Operation::Read, Some(name)))?;
    let Some(at) = latest.and_then(|commit| commit.timestamp) else {
        return Ok(());
    };
    let age = chrono::Utc::now().timestamp_millis().saturating_sub(at);
    if age < i64::try_from(quiet.as_millis()).unwrap_or(i64::MAX) {
        return Err(StoreError::Drift {
            table: name.to_owned(),
            difference: format!(
                "it was committed to {}s ago; stop its writer, then wait {} minutes",
                age / 1000,
                quiet.as_secs().div_ceil(60)
            ),
        });
    }
    Ok(())
}

/// Every partition any of `table`'s files is in.
fn partitions(table: &DeltaTable, name: &str) -> Result<BTreeSet<i64>, StoreError> {
    let read = || StoreError::engine(Operation::Read, Some(name));
    let snapshot = table.snapshot().map_err(read())?;
    let mut partitions = BTreeSet::new();
    for file in snapshot.snapshot().try_log_data().map_err(read())?.iter() {
        let value = file
            .partition_values_map()
            .remove(PARTITION)
            .flatten()
            .unwrap_or_default();
        partitions.insert(parse(PARTITION, &value)?);
    }
    Ok(partitions)
}

/// Writes the rows of the old `partition` of `table` again, `to` heights a partition, and
/// swaps its files for the new ones in one commit that records it done.
pub(super) async fn rewrite_partition(
    table: &mut DeltaTable,
    def: &TableDef,
    partition: i64,
    to: u64,
    writer: &WriterProperties,
) -> Result<(), StoreError> {
    let name = def.name.as_str();
    let engine = |operation| StoreError::engine(operation, Some(name));
    let started = tokio::time::Instant::now();
    let snapshot = table.snapshot().map_err(engine(Operation::Read))?;
    let value = partition.to_string();
    let mut replaced = Vec::new();
    let mut expected = Some(0);
    for file in snapshot
        .snapshot()
        .try_log_data()
        .map_err(engine(Operation::Read))?
        .iter()
    {
        if file.partition_values_map().remove(PARTITION).flatten() == Some(value.clone()) {
            expected = expected
                .zip(file.num_records())
                .map(|(sum, rows)| sum + rows);
            replaced.push(Action::Remove(file.remove_action(false)));
        }
    }
    let arrow =
        arrow::arrow_schema(def).map_err(StoreError::engine(Operation::Read, Some(name)))?;
    let stats = WriterStatsConfig::from_config(snapshot.snapshot().table_configuration());
    let config = WriterConfig::new(
        Arc::clone(&arrow),
        vec![PARTITION.to_owned()],
        Some(writer.clone()),
        std::num::NonZeroU64::new(TARGET_FILE_BYTES),
        None,
        stats.num_indexed_cols,
        stats.stats_columns,
    );
    let mut files = DeltaWriter::new(table.log_store().object_store(None), config);
    let rows = async {
        let ctx = SessionContext::new();
        table.update_datafusion_session(&ctx.state())?;
        let frame = ctx
            .read_table(table.table_provider().await?)?
            .filter(ident(PARTITION).eq(lit(partition)))?
            .select(columns(def, &arrow, to))?;
        let mut batches = frame.execute_stream().await?;
        let mut rows = 0;
        while let Some(batch) = batches.try_next().await? {
            // Built against the table's own schema, so a scan's string views and loosened
            // nullability are written as the table declares them.
            let batch = RecordBatch::try_new(Arc::clone(&arrow), batch.columns().to_vec())?;
            rows += batch.num_rows();
            files.write(&batch).await?;
        }
        Ok::<_, EngineError>(rows)
    };
    let rows = rows
        .await
        .map_err(StoreError::engine(Operation::Read, Some(name)))?;
    if let Some(expected) = expected
        && expected != rows
    {
        return Err(StoreError::Drift {
            table: name.to_owned(),
            difference: format!(
                "partition {partition} read back {rows} rows, not the {expected} its files hold"
            ),
        });
    }
    let mut actions: Vec<Action> = files
        .close()
        .await
        .map_err(engine(Operation::Append))?
        .into_iter()
        .map(|mut add| {
            add.data_change = false;
            Action::Add(add)
        })
        .collect();
    let added = actions.len();
    let removed = replaced.len();
    actions.extend(replaced);
    actions.push(metadata(table, &[(REPARTITION_DONE, Some(value.clone()))])?);
    let operation = DeltaOperation::Write {
        mode: SaveMode::Overwrite,
        partition_by: Some(vec![PARTITION.to_owned()]),
        predicate: Some(format!("{PARTITION} = {partition}")),
    };
    commit(table, actions, operation)
        .await
        .map_err(engine(Operation::Append))?;
    info!(
        table = name,
        partition,
        rows,
        removed,
        added,
        elapsed_ms = millis(started.elapsed()),
        "lake partition rewritten"
    );
    Ok(())
}

/// The table's columns as it declares them, with [`PARTITION`] recomputed from the block
/// height, `to` heights a partition.
fn columns(def: &TableDef, arrow: &deltalake::arrow::datatypes::Schema, to: u64) -> Vec<Expr> {
    let height = arrow::partitioned(def).unwrap_or_default();
    let width = i64::try_from(to).unwrap_or(i64::MAX);
    arrow
        .fields()
        .iter()
        .map(|field| {
            let name = field.name().as_str();
            if name == PARTITION {
                let height = cast(ident(height), deltalake::arrow::datatypes::DataType::Int64);
                (height / lit(width)).alias(PARTITION)
            } else {
                cast(ident(name), field.data_type().clone()).alias(name)
            }
        })
        .collect()
}

/// `table`'s metadata with each of `properties` set, or taken out when `None`.
fn metadata(
    table: &DeltaTable,
    properties: &[(&str, Option<String>)],
) -> Result<Action, StoreError> {
    let mut metadata = table
        .snapshot()
        .map_err(StoreError::engine(Operation::Append, None))?
        .metadata()
        .clone();
    for (key, value) in properties {
        metadata = match value {
            Some(value) => metadata.add_config_key((*key).to_owned(), value.clone()),
            None => metadata.remove_config_key(key),
        }
        .map_err(StoreError::engine(Operation::Append, None))?;
    }
    Ok(Action::Metadata(metadata))
}

/// The operation a commit setting `property` to `value` records.
fn properties_set(property: &str, value: String) -> DeltaOperation {
    DeltaOperation::SetTableProperties {
        properties: [(property.to_owned(), value)].into(),
    }
}

/// Commits `actions` to `table` and brings it up to the new version.
async fn commit(
    table: &mut DeltaTable,
    actions: Vec<Action>,
    operation: DeltaOperation,
) -> Result<(), DeltaTableError> {
    CommitBuilder::from(commit_properties())
        .with_actions(actions)
        .build(Some(table.snapshot()?), table.log_store(), operation)
        .await?;
    table.update_state().await
}
