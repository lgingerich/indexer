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
mod maintenance;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy_primitives::B256;
use deltalake::arrow::array::{AsArray as _, RecordBatch, StringArray, UInt64Array};
use deltalake::arrow::datatypes::{DataType as ArrowType, Schema as ArrowSchema, UInt64Type};
use deltalake::datafusion::dataframe::DataFrame;
use deltalake::datafusion::error::DataFusionError;
use deltalake::datafusion::prelude::{Expr, SessionContext, cast, ident, lit};
use deltalake::datafusion::scalar::ScalarValue;
use deltalake::logstore::{ObjectStoreRef, store_for};
use deltalake::parquet::basic::{Compression, ZstdLevel};
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::protocol::SaveMode;
use deltalake::{DeltaTable, DeltaTableBuilder, DeltaTableError, ensure_table_uri};
use serde::Deserialize;
use tokio::time::Instant;
use tracing::{info, warn};

use crate::ingest::pipeline::LEDGER_WINDOW;
use crate::secret::Secret;
use crate::sink::batch::Batch;
use crate::sink::channel::Batching;
use crate::sink::store_error::{EngineError, InvalidStoredValue, Operation, StoreError};
use crate::sink::table::{Row, Schema, Table, TableDef, TableError, TableId};
use crate::sink::{EnvelopeSink, Restored, SinkError, Store};
use crate::wire::envelope::{BlockMeta, ChainId, Envelope, StoredContract};

use self::arrow::{PARTITION, UINT_PRECISION};
use self::maintenance::Maintenance;

/// The `contracts` columns a restore reads back.
const CONTRACT_COLUMNS: [&str; 4] = ["protocol", "name", "address", "block_hash"];

/// The `accepted_blocks` columns opening the store reads back.
const LEDGER_COLUMNS: [&str; 4] = ["height", "hash", "parent_hash", "timestamp"];

/// The `reorgs` columns opening the store reads back.
const REORG_COLUMNS: [&str; 3] = ["height", "dedupe_key", "orphaned_hashes"];

/// The Delta Lake store's settings: where the tables live, how often to commit, and the
/// object store's own options, passed through.
///
/// ```toml
/// [sink.delta]
/// uri = "s3://indexer-lake"          # or a local directory
/// commit_interval_secs = 30
/// max_buffer_bytes = 268435456
/// maintenance = true
/// vacuum_retention_hours = 168
///
/// [sink.delta.storage]               # passed to the object store
/// endpoint = "https://s3.example.com"  # omit for AWS S3
/// region = "us-east-1"
/// access_key_id = { env = "S3_ACCESS_KEY_ID" }
/// secret_access_key = { env = "S3_SECRET_ACCESS_KEY" }
/// ```
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaSettings {
    /// Where the tables live: an `s3://bucket/prefix` URL, or a local directory. Each
    /// chain's tables go under `<uri>/<chain>/`. Required: there is no location that is
    /// right by default.
    #[serde(deserialize_with = "crate::config::non_empty")]
    pub uri: String,
    /// The longest a block waits in the buffer before a commit, in seconds, counted from
    /// when it arrived; a stalled chain still commits on time. Defaults to 30.
    ///
    /// At the tip this sets the file size: shorter puts rows in the lake sooner and makes
    /// more, smaller files.
    #[serde(default = "default_commit_interval_secs")]
    pub commit_interval_secs: u64,
    /// How many bytes of rows, roughly, may be buffered before a commit is made early.
    /// Defaults to 256 MiB. Reached while catching up, where it sets the file size.
    ///
    /// Counts the rows as they sit in memory. A commit builds each table's Arrow batch
    /// from them while they are still held, so it briefly needs as much again.
    #[serde(default = "default_max_buffer_bytes")]
    pub max_buffer_bytes: usize,
    /// Whether the store compacts and vacuums its own tables in the background. Defaults
    /// to true; turn it off only when another job does both.
    ///
    /// Compaction rewrites each partition the tip has left behind into a few large
    /// files, once. Vacuum deletes files no commit has referenced for
    /// [`vacuum_retention_hours`](Self::vacuum_retention_hours), once a day.
    #[serde(default = "default_maintenance")]
    pub maintenance: bool,
    /// How long a file no commit references is kept before vacuum deletes it, in hours:
    /// longer than any reader of an older version needs. Defaults to 168, a week. At
    /// least an hour is always kept, so a commit still writing its files is never
    /// vacuumed under it.
    #[serde(default = "default_vacuum_retention_hours")]
    pub vacuum_retention_hours: u64,
    /// S3 options, passed straight through, such as `endpoint`, `region`, and
    /// `access_key_id`; the `aws_`-prefixed spellings work too. Every value is a
    /// [`Secret`], so keys can be named by environment variable and none is logged.
    /// Credentials may also come from the standard `AWS_*` environment variables.
    #[serde(default)]
    pub storage: BTreeMap<String, Secret>,
}

impl DeltaSettings {
    /// When the storage drain commits: once the oldest buffered block has waited
    /// [`commit_interval_secs`](Self::commit_interval_secs), or once
    /// [`max_buffer_bytes`](Self::max_buffer_bytes) are buffered, whichever is first.
    pub(crate) fn batching(&self) -> Batching {
        Batching {
            records: usize::MAX,
            bytes: self.max_buffer_bytes,
            wait: Duration::from_secs(self.commit_interval_secs),
        }
    }
}

const fn default_commit_interval_secs() -> u64 {
    30
}

const fn default_max_buffer_bytes() -> usize {
    256 * 1024 * 1024
}

const fn default_maintenance() -> bool {
    true
}

const fn default_vacuum_retention_hours() -> u64 {
    168
}

/// One table: its definition, the Arrow schema its batches are built against, and the
/// Delta table as of the last commit this process made to it.
#[derive(Debug)]
struct Lake {
    def: Arc<TableDef>,
    arrow: Arc<ArrowSchema>,
    table: DeltaTable,
}

impl Lake {
    fn name(&self) -> &str {
        &self.def.name
    }

    /// The block hash and height columns, when the rows belong to a block.
    fn block_columns(&self) -> Option<(&str, &str)> {
        Some((
            self.def.block_hash.as_deref()?,
            self.def.block_number.as_deref()?,
        ))
    }

    /// Runs `plan` over the table and collects what it returns.
    async fn read(
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
    fn newest_partition(&self) -> Result<Option<i64>, StoreError> {
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
    writer: WriterProperties,
    /// The highest committed height, as maintenance reads it; zero before the first.
    published_tip: Arc<AtomicU64>,
    /// The background compaction and vacuum, when the settings ask for it. Held, never
    /// read: dropping the store stops the task.
    maintenance: Option<Maintenance>,
}

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
        let root = settings.uri.trim_end_matches('/');
        let storage = Storage::new(root, &settings.storage)?;
        let mut lakes = Vec::with_capacity(schema.tables().len());
        for def in schema.tables() {
            lakes.push(open_table(&storage, &format!("{root}/{chain}/{}", def.name), def).await?);
        }
        let positions = lakes
            .iter()
            .enumerate()
            .map(|(position, lake)| (lake.def.id, position))
            .collect();
        let mut sink = Self {
            chain: ChainId::new(chain),
            lakes,
            positions,
            batch: Batch::new(Arc::clone(&schema)),
            schema,
            committed: HashMap::new(),
            writer: WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::default()))
                .build(),
            published_tip: Arc::new(AtomicU64::new(0)),
            maintenance: None,
        };
        let ledger = sink.read_ledger().await?;
        sink.repair(&ledger).await?;
        sink.committed = ledger
            .iter()
            .map(|block| (hex(&block.hash), block.height))
            .collect();
        sink.publish_tip();
        if settings.maintenance {
            // Started after the repair, so it never compacts what the repair deletes.
            let tables = sink
                .lakes
                .iter()
                .filter(|lake| arrow::partitioned(&lake.def).is_some())
                .map(|lake| (lake.def.name.clone(), lake.table.clone()))
                .collect();
            let retention = Duration::from_hours(settings.vacuum_retention_hours.max(1));
            sink.maintenance = Some(maintenance::spawn(
                tables,
                Arc::clone(&sink.published_tip),
                retention,
                sink.writer.clone(),
            ));
        }
        info!(%chain, uri = root, tables = sink.lakes.len(), tip = ?sink.tip(), "delta lake opened");
        Ok(sink)
    }

    /// The contracts discovered on this chain that the lake holds, every one canonical:
    /// one created in an orphaned block was deleted with it, and one the ledger does not
    /// name was deleted when the store opened.
    async fn contracts(&self) -> Result<Vec<StoredContract>, StoreError> {
        let batches = self
            .lake(Table::Contract)
            .read(|frame| {
                // Ordered, so a restore reads the rows the same way every time.
                frame
                    .sort(vec![ident("dedupe_key").sort(true, false)])?
                    .select(texts_of(&CONTRACT_COLUMNS))
            })
            .await?;
        let mut contracts = Vec::new();
        for batch in &batches {
            let protocol = texts(batch, 0, "contracts.protocol")?;
            let name = texts(batch, 1, "contracts.name")?;
            let address = texts(batch, 2, "contracts.address")?;
            let block_hash = texts(batch, 3, "contracts.block_hash")?;
            for row in 0..batch.num_rows() {
                contracts.push(StoredContract {
                    protocol: protocol.value(row).to_owned(),
                    name: name.value(row).to_owned(),
                    address: parse("contracts.address", address.value(row))?,
                    block_hash: parse("contracts.block_hash", block_hash.value(row))?,
                });
            }
        }
        Ok(contracts)
    }

    fn lake(&self, table: Table) -> &Lake {
        &self.lakes[self.positions[&TableId::Dataset(table)]]
    }

    /// The highest committed height.
    fn tip(&self) -> Option<u64> {
        self.committed.values().max().copied()
    }

    /// The newest [`LEDGER_WINDOW`] accepted blocks, oldest first.
    ///
    /// Read from the ledger's newest two partitions alone, which hold far more blocks
    /// than the window, so a restart does not scan every file the ledger ever wrote.
    async fn read_ledger(&self) -> Result<Vec<BlockMeta>, StoreError> {
        let lake = self.lake(Table::AcceptedBlock);
        let newest = lake.newest_partition()?;
        let [height, hash, parent_hash, timestamp] = LEDGER_COLUMNS;
        let batches = lake
            .read(|frame| {
                let frame = match newest {
                    Some(newest) => frame.filter(ident(PARTITION).gt_eq(lit(newest - 1)))?,
                    None => frame,
                };
                let seconds = cast(
                    cast(ident(timestamp), ArrowType::Int64) / lit(1_000_000_i64),
                    ArrowType::UInt64,
                );
                frame
                    .sort(vec![ident(height).sort(false, false)])?
                    .limit(0, Some(LEDGER_WINDOW))?
                    .select(vec![
                        cast(ident(height), ArrowType::UInt64),
                        cast(ident(hash), ArrowType::Utf8),
                        cast(ident(parent_hash), ArrowType::Utf8),
                        seconds,
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
                    hash: parse("accepted_blocks.hash", hashes.value(row))?,
                    parent_hash: parse("accepted_blocks.parent_hash", parents.value(row))?,
                    timestamp: times.value(row),
                });
            }
        }
        ledger.sort_by_key(|block| block.height);
        Ok(ledger)
    }

    /// Deletes the rows of blocks `ledger` does not name, at or above its oldest height:
    /// what a commit that stopped part-way appended before its ledger entry. With no
    /// ledger at all, every block row is such a row.
    async fn repair(&mut self, ledger: &[BlockMeta]) -> Result<(), StoreError> {
        let named: HashSet<String> = ledger.iter().map(|block| hex(&block.hash)).collect();
        let floor = ledger.first().map(|block| block.height);
        let ledger_id = TableId::Dataset(Table::AcceptedBlock);
        for position in 0..self.lakes.len() {
            let lake = &self.lakes[position];
            let Some((hash, number)) = lake.block_columns() else {
                continue;
            };
            if lake.def.id == ledger_id {
                continue;
            }
            let batches = lake
                .read(|frame| {
                    let frame = match floor {
                        Some(floor) => frame.filter(at_or_above(number, floor))?,
                        None => frame,
                    };
                    frame.select(texts_of(&[hash]))?.distinct()
                })
                .await?;
            let mut stray = Vec::new();
            for batch in &batches {
                let hashes = texts(batch, 0, "block_hash")?;
                stray.extend(
                    hashes
                        .iter()
                        .flatten()
                        .filter(|block| !named.contains(*block))
                        .map(str::to_owned),
                );
            }
            if stray.is_empty() {
                continue;
            }
            warn!(
                table = lake.name(),
                blocks = stray.len(),
                "deleting rows a stopped commit left behind"
            );
            self.delete(position, of_blocks((hash, number), &stray, floor))
                .await?;
        }
        self.repair_reorgs(ledger).await
    }

    /// Deletes the `reorgs` rows of retractions that never finished: ones whose orphaned
    /// blocks `ledger` still names. A commit writes a retraction's row before it takes the
    /// orphans out of the ledger, so a stop in between leaves such a row; the restart
    /// resumes on top of the orphans, detects the reorg again, and records it once.
    ///
    /// A block orphaned and later canonical again is named by the ledger too, so its
    /// first retraction's row is deleted here as well. That costs an audit row, never a
    /// block's data.
    async fn repair_reorgs(&mut self, ledger: &[BlockMeta]) -> Result<(), StoreError> {
        let Some(floor) = ledger.first().map(|block| block.height) else {
            return Ok(());
        };
        let named: HashSet<B256> = ledger.iter().map(|block| block.hash).collect();
        let position = self.positions[&TableId::Dataset(Table::Reorg)];
        let [height, dedupe_key, orphaned_hashes] = REORG_COLUMNS;
        // A retraction's height is its lowest orphan's, so an unfinished one is at or
        // above the ledger's oldest block.
        let batches = self.lakes[position]
            .read(|frame| {
                frame
                    .filter(ident(height).gt_eq(uint(floor)))?
                    .select(texts_of(&[dedupe_key, orphaned_hashes]))
            })
            .await?;
        let mut unfinished = Vec::new();
        for batch in &batches {
            let keys = texts(batch, 0, "reorgs.dedupe_key")?;
            let orphans = texts(batch, 1, "reorgs.orphaned_hashes")?;
            for row in 0..batch.num_rows() {
                let orphans: Vec<B256> =
                    serde_json::from_str(orphans.value(row)).map_err(|_| {
                        InvalidStoredValue::new("reorgs.orphaned_hashes", orphans.value(row))
                    })?;
                if orphans.iter().any(|hash| named.contains(hash)) {
                    unfinished.push(keys.value(row).to_owned());
                }
            }
        }
        if unfinished.is_empty() {
            return Ok(());
        }
        warn!(
            rows = unfinished.len(),
            "deleting retractions a stopped commit left unfinished"
        );
        self.delete(position, in_texts(dedupe_key, &unfinished))
            .await
    }

    async fn delete(&mut self, position: usize, predicate: Expr) -> Result<(), StoreError> {
        let lake = &mut self.lakes[position];
        let (table, _) = lake
            .table
            .clone()
            .delete()
            .with_predicate(predicate)
            .with_writer_properties(self.writer.clone())
            .await
            .map_err(StoreError::engine(Operation::Delete, Some(&lake.def.name)))?;
        lake.table = table;
        Ok(())
    }

    async fn append(&mut self, position: usize, rows: &[&Row]) -> Result<(), StoreError> {
        let lake = &mut self.lakes[position];
        let name = Some(lake.def.name.as_str());
        let batch = arrow::record_batch(&lake.arrow, &lake.def, rows)
            .map_err(StoreError::engine(Operation::Append, name))?;
        let mut write = lake
            .table
            .clone()
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .with_writer_properties(self.writer.clone());
        if arrow::partitioned(&lake.def).is_some() {
            write = write.with_partition_columns([PARTITION]);
        }
        let table = write
            .await
            .map_err(StoreError::engine(Operation::Append, name))?;
        lake.table = table;
        Ok(())
    }

    /// Appends `rows` to the table at `position`, when there are any.
    async fn append_some(&mut self, position: usize, rows: &[&Row]) -> Result<(), StoreError> {
        if rows.is_empty() {
            return Ok(());
        }
        self.append(position, rows).await
    }

    /// Writes the batch in the ledger's order, one step per rule in the module docs:
    ///
    /// 1. retractions into `reorgs`, while the ledger still names their orphans;
    /// 2. the orphans out of the ledger, then out of every other table;
    /// 3. every table's rows;
    /// 4. the new blocks into the ledger, once their rows are all present.
    ///
    /// A failure part-way leaves the lake as the next open repairs it, so the batch is
    /// not retried here: the error stops storage, and a restart resumes from the ledger.
    async fn commit(&mut self) -> Result<(), StoreError> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let started = Instant::now();
        // Taken out so the commit can borrow its rows while it writes, and put back
        // cleared, keeping its allocation for the next one.
        let mut batch = std::mem::replace(&mut self.batch, Batch::new(Arc::clone(&self.schema)));
        let mut tables = batch.by_table();
        let reorgs = tables
            .remove(&TableId::Dataset(Table::Reorg))
            .unwrap_or_default();
        let accepted = tables
            .remove(&TableId::Dataset(Table::AcceptedBlock))
            .unwrap_or_default();
        let orphans = self.committed_orphans(&batch);

        self.append_some(self.position(Table::Reorg), &reorgs)
            .await?;
        if let Some((floor, hashes)) = &orphans {
            self.retract(*floor, hashes).await?;
        }
        let mut rows = reorgs.len() + accepted.len();
        for position in 0..self.lakes.len() {
            if let Some(table) = tables.remove(&self.lakes[position].def.id) {
                rows += table.len();
                self.append(position, &table).await?;
            }
        }
        self.append_some(self.position(Table::AcceptedBlock), &accepted)
            .await?;

        let first = accepted.iter().filter_map(|row| row.block_number()).min();
        let orphaned = orphans.map(|(_, hashes)| hashes).unwrap_or_default();
        self.advance(&orphaned, &accepted);
        info!(
            chain = %self.chain,
            rows,
            orphaned = orphaned.len(),
            from = ?first,
            to = ?self.tip(),
            elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "lake commit"
        );
        batch.clear();
        self.batch = batch;
        Ok(())
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
        let ledger = self.position(Table::AcceptedBlock);
        let others = (0..self.lakes.len()).filter(|&position| position != ledger);
        for position in std::iter::once(ledger).chain(others) {
            if let Some(columns) = self.lakes[position].block_columns() {
                let predicate = of_blocks(columns, hashes, Some(floor));
                self.delete(position, predicate).await?;
            }
        }
        Ok(())
    }

    /// Moves the window of blocks a reorg could still orphan past a commit: its orphans
    /// out, the blocks it accepted in, and anything below the window dropped.
    fn advance(&mut self, orphaned: &[String], accepted: &[&Row]) {
        for hash in orphaned {
            self.committed.remove(hash);
        }
        self.committed.extend(
            accepted
                .iter()
                .filter_map(|row| Some((row.block_hash()?.to_owned(), row.block_number()?))),
        );
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
    async fn restore(&mut self) -> Result<Restored, StoreError> {
        Ok(Restored {
            contracts: self.contracts().await?,
            ledger: self.read_ledger().await?,
        })
    }
}

impl EnvelopeSink for DeltaSink {
    /// Buffers one envelope's rows. A `reorg` first drops every buffered row of the
    /// blocks it orphans; the committed ones are deleted at the next commit, and its own
    /// row is kept as the record of the retraction.
    async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
        self.batch.push(&envelope)
    }

    /// Commits the batch. The drain decides how many blocks it holds; a block still
    /// buffered when the process stops is fetched again from the ledger's tip.
    async fn flush(&mut self) -> Result<(), SinkError> {
        self.commit().await?;
        Ok(())
    }

    fn buffered_bytes(&self) -> usize {
        self.batch.bytes()
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

/// Opens the table of `def` at `uri`, creating it when it does not exist, and checks an
/// existing one against its definition.
async fn open_table(storage: &Storage, uri: &str, def: &Arc<TableDef>) -> Result<Lake, StoreError> {
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
        .table(uri)
        .await
        .map_err(StoreError::engine(Operation::Open, Some(name)))?;
    if table.version().is_none() {
        let partitions = arrow::partitioned(def).map(|_| PARTITION.to_owned());
        table = table
            .create()
            .with_table_name(name)
            .with_columns(expected.fields().cloned())
            .with_partition_columns(partitions)
            .await
            .map_err(StoreError::engine(Operation::Create, Some(name)))?;
    } else {
        check(&table, &expected, name)?;
    }
    let arrow =
        arrow::arrow_schema(def).map_err(StoreError::engine(Operation::Create, Some(name)))?;
    Ok(Lake {
        def: Arc::clone(def),
        arrow,
        table,
    })
}

/// Fails when `table` as stored lacks a column, holds one it does not declare, or types
/// one differently, so a changed definition stops the run at startup rather than at its
/// first write.
fn check(
    table: &DeltaTable,
    expected: &deltalake::kernel::StructType,
    name: &str,
) -> Result<(), StoreError> {
    let found = table
        .snapshot()
        .map_err(StoreError::engine(Operation::Open, Some(name)))?
        .schema();
    let drift = |difference: String| StoreError::Drift {
        table: name.to_owned(),
        difference,
    };
    for field in expected.fields() {
        match found.field(field.name()) {
            None => return Err(drift(format!("it has no {} column", field.name()))),
            Some(stored) if stored.data_type() != field.data_type() => {
                return Err(drift(format!(
                    "{} is {}, not {}",
                    field.name(),
                    stored.data_type(),
                    field.data_type()
                )));
            }
            Some(stored) if stored.is_nullable() != field.is_nullable() => {
                return Err(drift(format!("{} differs in nullability", field.name())));
            }
            Some(_) => {}
        }
    }
    match found
        .fields()
        .find(|field| expected.field(field.name()).is_none())
    {
        Some(extra) => Err(drift(format!(
            "it has a {} column it should not",
            extra.name()
        ))),
        None => Ok(()),
    }
}

/// Fails when `def` lacks any of `columns`, which a startup read names.
fn require(def: &TableDef, columns: &[&str]) -> Result<(), TableError> {
    for column in columns {
        def.require(column)?;
    }
    Ok(())
}

/// `column >= height`, with the partition column bounding it too, so a scan or delete
/// skips every older partition without opening a file.
fn at_or_above(column: &str, height: u64) -> Expr {
    ident(PARTITION)
        .gt_eq(lit(arrow::partition_of(height)))
        .and(ident(column).gt_eq(uint(height)))
}

/// The rows of the blocks `hashes`, by a table's `(hash, height)` columns, none of them
/// below `floor` when there is one.
fn of_blocks((hash, height): (&str, &str), hashes: &[String], floor: Option<u64>) -> Expr {
    let blocks = in_texts(hash, hashes);
    match floor {
        Some(floor) => at_or_above(height, floor).and(blocks),
        None => blocks,
    }
}

/// A `Uint` value, such as a block height, as the lake stores it.
fn uint(value: u64) -> Expr {
    lit(ScalarValue::Decimal128(
        Some(i128::from(value)),
        UINT_PRECISION,
        0,
    ))
}

/// A hash as the lake stores it: lowercase `0x` hex.
fn hex(hash: &B256) -> String {
    format!("{hash:#x}")
}

/// `column IN (values)`.
fn in_texts(column: &str, values: &[String]) -> Expr {
    ident(column).in_list(
        values.iter().map(|value| lit(value.as_str())).collect(),
        false,
    )
}

/// `columns`, each cast to plain text: a scan may return a string view instead.
fn texts_of(columns: &[&str]) -> Vec<Expr> {
    columns
        .iter()
        .map(|column| cast(ident(*column), ArrowType::Utf8).alias(*column))
        .collect()
}

fn parse<T: std::str::FromStr>(column: &'static str, value: &str) -> Result<T, InvalidStoredValue> {
    value
        .parse()
        .map_err(|_| InvalidStoredValue::new(column, value))
}

/// Column `index` of a batch read back, which the read cast to text.
fn texts<'a>(
    batch: &'a RecordBatch,
    index: usize,
    column: &'static str,
) -> Result<&'a StringArray, InvalidStoredValue> {
    batch
        .column(index)
        .as_string_opt::<i32>()
        .ok_or_else(|| InvalidStoredValue::new(column, "not text"))
}

/// Column `index` of a batch read back, which the read cast to unsigned integers.
fn uints<'a>(
    batch: &'a RecordBatch,
    index: usize,
    column: &'static str,
) -> Result<&'a UInt64Array, InvalidStoredValue> {
    batch
        .column(index)
        .as_primitive_opt::<UInt64Type>()
        .ok_or_else(|| InvalidStoredValue::new(column, "not an unsigned integer"))
}

#[cfg(test)]
mod tests;
