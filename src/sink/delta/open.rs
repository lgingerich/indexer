//! Opening the lake: its tables, the object store they share, the checks an existing
//! table must pass before the run writes to it, and the repair that follows.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use deltalake::logstore::object_store::GetOptions;
use deltalake::logstore::{ObjectStoreRef, store_for};
use deltalake::parquet::basic::{Compression, ZstdLevel};
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::{
    DeltaTable, DeltaTableBuilder, DeltaTableError, ObjectStoreError, ensure_table_uri,
};
use futures_util::stream::{self, StreamExt as _, TryStreamExt as _};
use serde::Deserialize;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tracing::{info, warn};

use super::arrow::{self, PARTITION};
use super::lake::{CONCURRENT_CHECKPOINTS, Lake, commit_properties};
use super::{
    COMMIT_DEADLINE, CONCURRENT_TABLES, DeltaSettings, DeltaSink, PendingLedger, maintenance,
};
use crate::secret::Secret;
use crate::sink::batch::Batch;
use crate::sink::store_error::{self, ColumnDrift, Operation, StoreError};
use crate::sink::table::{
    CONTRACT_COLUMNS, ColumnType, LEDGER_COLUMNS, REORG_COLUMNS, Schema, Table, TableDef,
    TableError, TableId,
};
use crate::sink::{Restored, hex, millis};
use crate::wire::envelope::ChainId;

impl DeltaSink {
    /// Opens every table of `schema` under `<uri>/<chain>/`, creating the ones that do
    /// not exist, and repairs what a stopped commit left behind.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when a table cannot be opened, created, read, or
    /// repaired, [`StoreError::Reserved`] when a table declares the partition column,
    /// [`StoreError::Drift`] when an existing table does not match its definition,
    /// [`StoreError::Table`] when a startup read names a column its table lacks, and
    /// [`StoreError::Restore`] when the ledger holds a value that does not parse.
    pub async fn open(
        settings: &DeltaSettings,
        schema: Arc<Schema>,
        chain: &str,
    ) -> Result<Self, StoreError> {
        for (table, columns) in [
            (Table::Contract, &CONTRACT_COLUMNS[..]),
            (Table::AcceptedBlock, &LEDGER_COLUMNS[..]),
            (Table::Reorg, &REORG_COLUMNS[..]),
        ] {
            require(schema.dataset(table), columns)?;
        }
        let started = Instant::now();
        let root = settings.uri.trim_end_matches('/');
        let storage = Storage::new(root, &settings.storage)?;
        let checkpoints = Arc::new(Semaphore::new(CONCURRENT_CHECKPOINTS));
        // Each open is a few requests that wait on the store, and a run has a table per
        // event, so they overlap: one after another, a hundred tables took minutes.
        // Boxed, as is the repair's scan: the compiler cannot otherwise prove the open
        // `Send` through these closures' borrowed arguments, which running it on the
        // store's own runtime needs (rust-lang/rust#102211).
        let lakes: Vec<Lake> = stream::iter(schema.tables())
            .map(|def| {
                let uri = format!("{root}/{chain}/{}", def.name);
                open_table(
                    &storage,
                    uri,
                    def,
                    settings.partition_blocks,
                    Arc::clone(&checkpoints),
                )
            })
            .boxed()
            .buffered(CONCURRENT_TABLES)
            .try_collect()
            .await?;
        let opened = started.elapsed();
        let positions = lakes
            .iter()
            .enumerate()
            .map(|(position, lake)| (lake.def.id, position))
            .collect();
        let mut sink = Self {
            lakes,
            positions,
            batch: Batch::new(Arc::clone(&schema), ChainId::new(chain)),
            schema,
            committed: HashMap::new(),
            failed: false,
            writer: WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::default()))
                .build(),
            published_tip: Arc::new(AtomicU64::new(0)),
            maintenance: None,
            ledger: PendingLedger::default(),
            deadline: COMMIT_DEADLINE,
            opened: None,
        };
        let read = Instant::now();
        let ledger = sink.read_ledger().await?;
        let read = read.elapsed();
        let repaired = Instant::now();
        let contracts = sink.repair(&ledger).await?;
        let repaired = repaired.elapsed();
        sink.committed = ledger
            .iter()
            .map(|block| (hex(&block.hash), block.height))
            .collect();
        sink.opened = Some(Restored { contracts, ledger });
        sink.publish_tip();
        if settings.maintenance {
            // Started after the repair, so it never compacts what the repair deletes.
            let tables = sink
                .lakes
                .iter()
                .filter(|lake| arrow::partitioned(&lake.def).is_some())
                .map(|lake| (lake.def.name.clone(), lake.table.log_store(), lake.width))
                .collect();
            let retention = Duration::from_hours(settings.vacuum_retention_hours.max(1));
            sink.maintenance = Some(maintenance::spawn(
                tables,
                Arc::clone(&sink.published_tip),
                retention,
                sink.writer.clone(),
            ));
        }
        info!(
            %chain,
            uri = root,
            tables = sink.lakes.len(),
            tip = ?sink.tip(),
            open_ms = millis(opened),
            ledger_ms = millis(read),
            repair_ms = millis(repaired),
            elapsed_ms = millis(started.elapsed()),
            "delta lake opened"
        );
        Ok(sink)
    }
}

/// The object store every table of the lake is opened through: one client, so one
/// credential chain and one connection pool, rather than one of each per table.
struct Storage {
    /// The store, rooted at the bucket (or the filesystem root), as delta-rs scopes a
    /// shared store to each table's path itself.
    root: ObjectStoreRef,
    /// The settings' storage options, which each table's log store reads too.
    options: HashMap<String, String>,
}

impl Storage {
    fn new(uri: &str, options: &BTreeMap<String, Secret>) -> Result<Self, StoreError> {
        deltalake::aws::register_handlers(None);
        let options: HashMap<String, String> = options
            .iter()
            .map(|(key, value)| (key.clone(), value.expose().to_owned()))
            .collect();
        let open = || StoreError::engine(Operation::Open, Some(uri));
        let mut root = ensure_table_uri(uri).map_err(open())?;
        root.set_path("/");
        let root = store_for(&root, options.clone()).map_err(open())?;
        Ok(Self { root, options })
    }

    /// The table at `uri`, loaded, or not yet created there.
    async fn table(&self, uri: &str) -> Result<DeltaTable, DeltaTableError> {
        let url = ensure_table_uri(uri)?;
        let mut table = DeltaTableBuilder::from_url(url.clone())?
            .with_storage_backend(Arc::clone(&self.root), url)
            .with_storage_options(self.options.clone())
            .build()?;
        match table.load().await {
            Ok(()) | Err(DeltaTableError::NotATable(_)) => Ok(table),
            Err(error) => Err(error),
        }
    }
}

/// Opens the table of `def` at `uri`, creating it `width` heights to a partition when it
/// does not exist, checks an existing one against its definition and `width`, and
/// starts its checkpoint if one is due.
async fn open_table(
    storage: &Storage,
    uri: String,
    def: &Arc<TableDef>,
    width: u64,
    checkpoints: Arc<Semaphore>,
) -> Result<Lake, StoreError> {
    let name = def.name.as_str();
    if def.position(PARTITION).is_some() {
        return Err(StoreError::Reserved {
            table: name.to_owned(),
            column: PARTITION.to_owned(),
        });
    }
    let expected =
        arrow::delta_schema(def).map_err(StoreError::engine(Operation::Create, Some(name)))?;
    let mut table = storage
        .table(&uri)
        .await
        .map_err(StoreError::engine(Operation::Open, Some(name)))?;
    let partitioned = arrow::partitioned(def).is_some();
    let checkpointed = if table.version().is_none() {
        let partitions = partitioned.then(|| PARTITION.to_owned());
        let mut recorded = vec![(STATS_COLUMNS.to_owned(), Some(stats_columns(def)))];
        if partitioned {
            recorded.push((arrow::PARTITION_BLOCKS.to_owned(), Some(width.to_string())));
        }
        table = table
            .create()
            .with_table_name(name)
            .with_columns(expected.fields().cloned())
            .with_partition_columns(partitions)
            // The width is the indexer's own property, which delta-rs does not know.
            .with_raise_if_key_not_exists(false)
            .with_configuration(recorded)
            .await
            .map_err(StoreError::engine(Operation::Create, Some(name)))?;
        0
    } else {
        check(&table, &expected, name)?;
        if partitioned {
            check_width(&table, width, name)?;
        }
        keep_stats(&mut table, def).await?;
        // Only a hint: without it the next checkpoint comes early, which is no reason to
        // stop the run.
        match last_checkpoint(&table).await {
            Ok(version) => version.unwrap_or_default(),
            Err(error) => {
                warn!(table = name, %error, "lake checkpoint hint unreadable; counting from zero");
                0
            }
        }
    };
    let arrow =
        arrow::arrow_schema(def).map_err(StoreError::engine(Operation::Create, Some(name)))?;
    let mut lake = Lake {
        def: Arc::clone(def),
        arrow,
        table,
        width,
        checkpointed,
        checkpoints,
        reopened: Arc::default(),
    };
    lake.checkpoint_if_due();
    Ok(lake)
}

/// How many heights one partition of the table `name` covers, as its
/// [`PARTITION_BLOCKS`](arrow::PARTITION_BLOCKS) property records it: the default for a
/// table that records none, as every table written before the width was a setting.
fn partition_width(table: &DeltaTable, name: &str) -> Result<u64, StoreError> {
    let snapshot = table
        .snapshot()
        .map_err(StoreError::engine(Operation::Open, Some(name)))?;
    let Some(recorded) = snapshot
        .metadata()
        .configuration()
        .get(arrow::PARTITION_BLOCKS)
    else {
        return Ok(arrow::DEFAULT_PARTITION_BLOCKS);
    };
    recorded.parse().map_err(|_| StoreError::Drift {
        table: name.to_owned(),
        difference: format!("its partition width is recorded as `{recorded}`, not a number"),
    })
}

/// Fails when the table `name` is not `width` heights a partition.
fn check_width(table: &DeltaTable, width: u64, name: &str) -> Result<(), StoreError> {
    let stored = partition_width(table, name)?;
    if stored == width {
        return Ok(());
    }
    Err(StoreError::Drift {
        table: name.to_owned(),
        difference: format!(
            "its partitions are {stored} heights wide, not {width}; a table keeps the width \
             it was written with"
        ),
    })
}

/// The table property naming the columns each file keeps statistics on.
/// Delta's own, so every reader and writer follows it.
pub(super) const STATS_COLUMNS: &str = "delta.dataSkippingStatsColumns";

/// The columns every file of `def`'s table keeps statistics on, comma-separated: its
/// block height and its timestamps, the columns a file's rows fall in a narrow range of,
/// so a read filtering on them skips the files outside it.
///
/// Delta's default is the first 32 columns, which left the widest tables, such as
/// `transactions`, with none on their block height, and spent the rest on hashes and
/// addresses whose range in any file is nearly everything. Every file's statistics are
/// held in its table's file list, so they are kept to the columns that pay for it.
fn stats_columns(def: &TableDef) -> String {
    let reorgs = def.id == TableId::Dataset(Table::Reorg);
    let height = def
        .block_number
        .as_deref()
        .or_else(|| reorgs.then_some(REORG_COLUMNS[0]));
    let timestamps = def
        .columns
        .iter()
        .filter(|column| column.kind == ColumnType::Timestamp)
        .map(|column| column.name.as_ref());
    height
        .into_iter()
        .chain(timestamps)
        .collect::<Vec<_>>()
        .join(",")
}

/// Records [`stats_columns`] on `table` when it names others, as a table written before
/// they were chosen does: the files written from then on keep statistics on them, and
/// those already written keep what they have.
async fn keep_stats(table: &mut DeltaTable, def: &TableDef) -> Result<(), StoreError> {
    let wanted = stats_columns(def);
    let stored = table
        .snapshot()
        .map_err(StoreError::engine(Operation::Open, Some(&def.name)))?
        .metadata()
        .configuration()
        .get(STATS_COLUMNS)
        .cloned();
    if stored.as_deref() == Some(wanted.as_str()) {
        return Ok(());
    }
    *table = table
        .clone()
        .set_tbl_properties()
        .with_properties([(STATS_COLUMNS.to_owned(), wanted.clone())].into())
        .with_commit_properties(commit_properties())
        .await
        .map_err(StoreError::engine(Operation::Open, Some(&def.name)))?;
    info!(table = %def.name, columns = %wanted, "lake table keeps statistics on new columns");
    Ok(())
}

/// The version of `table`'s newest checkpoint, as its log's `_last_checkpoint` names it;
/// `None` when it names none. delta-rs reads the same file while loading, but keeps the
/// version to itself.
///
/// The file is only a hint, written after its checkpoint, so it may name an older one:
/// then the next checkpoint comes early, which costs one checkpoint and nothing else.
/// One that does not parse counts as absent, as it does for delta-rs.
async fn last_checkpoint(table: &DeltaTable) -> Result<Option<u64>, DeltaTableError> {
    #[derive(Deserialize)]
    struct Hint {
        version: u64,
    }
    let log_store = table.log_store();
    let path = log_store.log_path().clone().join("_last_checkpoint");
    let read = match log_store
        .object_store(None)
        .get_opts(&path, GetOptions::default())
        .await
    {
        Ok(read) => read,
        Err(ObjectStoreError::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let bytes = read.bytes().await?;
    Ok(serde_json::from_slice::<Hint>(&bytes)
        .ok()
        .map(|hint| hint.version))
}

/// Fails when `table` as stored does not match `expected`, its definition's Delta schema
/// (see [`store_error::check_columns`]). Each column's type and nullability are compared.
fn check(
    table: &DeltaTable,
    expected: &deltalake::kernel::StructType,
    name: &str,
) -> Result<(), StoreError> {
    let found = table
        .snapshot()
        .map_err(StoreError::engine(Operation::Open, Some(name)))?
        .schema();
    let columns = |schema: &deltalake::kernel::StructType| {
        schema
            .fields()
            .map(|field| (field.name().clone(), field.clone()))
            .collect::<Vec<_>>()
    };
    store_error::check_columns(
        name,
        &columns(expected),
        &columns(&found),
        |stored, field| {
            if stored.data_type() != field.data_type() {
                Some(ColumnDrift::Type {
                    found: stored.data_type().to_string(),
                    expected: field.data_type().to_string(),
                })
            } else if stored.is_nullable() != field.is_nullable() {
                Some(ColumnDrift::Nullability)
            } else {
                None
            }
        },
    )
}

/// Fails when `def` lacks any of `columns`, which a startup read names.
fn require(def: &TableDef, columns: &[&str]) -> Result<(), TableError> {
    for column in columns {
        def.require(column)?;
    }
    Ok(())
}
