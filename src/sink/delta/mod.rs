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
//! Blocks are written as they are accepted, not held back until they are final. Rows are
//! buffered and committed every [`commit_interval_secs`](DeltaSettings::commit_interval_secs),
//! or sooner once [`max_buffer_bytes`](DeltaSettings::max_buffer_bytes) are buffered, so
//! a commit holds many blocks and the buffer, not a later compaction, sets the file size.
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
//! rows are all present. Opening the store repairs what a stopped commit left behind:
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
//! A chain's tables are written by one process. Unlike `PostgreSQL`'s advisory lock or
//! `DuckDB`'s file lock, nothing here enforces that: a deployment runs one indexer per
//! chain. A second writer on the same `uri` would append duplicate rows, and its open
//! would repair away the first's uncommitted ones. If that ever needs enforcing, the
//! design is a lease object at `<uri>/<chain>/_writer`: created with a conditional put,
//! renewed on a timer by conditional overwrite, checked before every commit, and taken
//! over once expired — at the cost of a restart after a crash waiting out the lease.

mod arrow;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::B256;
use deltalake::arrow::array::{AsArray as _, RecordBatch, StringArray, UInt64Array};
use deltalake::arrow::datatypes::{DataType as ArrowType, Schema as ArrowSchema, UInt64Type};
use deltalake::datafusion::dataframe::DataFrame;
use deltalake::datafusion::error::DataFusionError;
use deltalake::datafusion::prelude::{Expr, SessionContext, cast, ident, lit};
use deltalake::datafusion::scalar::ScalarValue;
use deltalake::parquet::basic::{Compression, ZstdLevel};
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::protocol::SaveMode;
use deltalake::{DeltaTable, ensure_table_uri};
use serde::Deserialize;
use tokio::time::Instant;
use tracing::{info, warn};

use crate::secret::Secret;

use crate::ingest::pipeline::MAX_UNFINALIZED_BLOCKS;
use crate::sink::store_error::{InvalidStoredValue, Operation, StoreError};
use crate::sink::table::{Row, Schema, Table, TableDef, TableId, Value};
use crate::sink::{EnvelopeSink, SinkError};
use crate::wire::envelope::{BlockMeta, ChainId, Envelope, Event, StoredContract};

use self::arrow::PARTITION;

/// How many accepted blocks opening the store reads back: the undo window and its floor.
const LEDGER_WINDOW: usize = MAX_UNFINALIZED_BLOCKS + 1;

/// The Delta Lake store's settings: where the tables live, how often to commit, and the
/// object store's own options, passed through.
///
/// ```toml
/// [sink.delta]
/// uri = "s3://indexer-lake"          # or a local directory
/// commit_interval_secs = 30
/// max_buffer_bytes = 268435456
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
    /// The longest rows wait in the buffer before a commit, in seconds. Defaults to 30.
    ///
    /// At the tip this sets the file size: shorter puts rows in the lake sooner and makes
    /// more, smaller files.
    #[serde(default = "default_commit_interval_secs")]
    pub commit_interval_secs: u64,
    /// How many bytes of rows, roughly, may be buffered before a commit is made early.
    /// Defaults to 256 MiB. Reached while catching up, where it sets the file size.
    #[serde(default = "default_max_buffer_bytes")]
    pub max_buffer_bytes: usize,
    /// S3 options, passed straight through, such as `endpoint`, `region`, and
    /// `access_key_id`; the `aws_`-prefixed spellings work too. Every value is a
    /// [`Secret`], so keys can be named by environment variable and none is logged.
    /// Credentials may also come from the standard `AWS_*` environment variables.
    #[serde(default)]
    pub storage: BTreeMap<String, Secret>,
}

const fn default_commit_interval_secs() -> u64 {
    30
}

const fn default_max_buffer_bytes() -> usize {
    256 * 1024 * 1024
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
        let name = Some(self.name());
        let ctx = SessionContext::new();
        self.table
            .update_datafusion_session(&ctx.state())
            .map_err(StoreError::engine(Operation::Read, name))?;
        let provider = self
            .table
            .table_provider()
            .await
            .map_err(StoreError::engine(Operation::Read, name))?;
        let frame = ctx
            .read_table(provider)
            .map_err(StoreError::engine(Operation::Read, name))?;
        plan(frame)
            .map_err(StoreError::engine(Operation::Read, name))?
            .collect()
            .await
            .map_err(StoreError::engine(Operation::Read, name))
    }
}

/// What has been published since the last commit.
#[derive(Debug, Default)]
struct Buffer {
    /// Rows to append, in publish order.
    rows: Vec<Row>,
    /// Committed blocks a buffered `reorg` orphaned, by hash, to delete at the next
    /// commit.
    orphaned: BTreeSet<String>,
    /// Roughly how many bytes `rows` hold.
    bytes: usize,
}

impl Buffer {
    fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.orphaned.is_empty()
    }

    fn push(&mut self, row: Row) {
        self.bytes += row.values().iter().map(size).sum::<usize>();
        self.rows.push(row);
    }

    /// Drops every buffered row of the blocks `hashes` names.
    fn drop_blocks(&mut self, hashes: &BTreeSet<String>) {
        self.rows
            .retain(|row| !row.block_hash().is_some_and(|hash| hashes.contains(hash)));
        self.bytes = self
            .rows
            .iter()
            .flat_map(|row| row.values().iter().map(size))
            .sum();
    }

    /// The rows each table should append: the last buffered copy of each `dedupe_key`,
    /// in publish order.
    fn by_table(&self) -> HashMap<TableId, Vec<&Row>> {
        let mut seen = HashSet::new();
        let mut tables: HashMap<TableId, Vec<&Row>> = HashMap::new();
        for row in self.rows.iter().rev() {
            let id = row.table().id;
            if seen.insert((id, row.dedupe_key())) {
                tables.entry(id).or_default().push(row);
            }
        }
        for rows in tables.values_mut() {
            rows.reverse();
        }
        tables
    }
}

/// Roughly how many bytes a value takes in the buffer.
fn size(value: &Value) -> usize {
    match value {
        Value::Text(text) | Value::Document(text) => text.len(),
        Value::BigInt { .. } => 32,
        Value::List(items) => items.iter().map(size).sum(),
        Value::Null | Value::Uint(_) | Value::Int(_) | Value::Bool(_) | Value::Timestamp(_) => 8,
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
    buffer: Buffer,
    /// The committed blocks a reorg could still orphan, by hash, with their heights.
    committed: HashMap<String, u64>,
    /// The highest committed height.
    tip: Option<u64>,
    /// The ledger as opening the store read it, oldest first, until ingest takes it.
    restored: Vec<BlockMeta>,
    last_commit: Instant,
    commit_interval: Duration,
    max_buffer_bytes: usize,
    writer: WriterProperties,
}

impl DeltaSink {
    /// Opens every table of `schema` under `<uri>/<chain>/`, creating the ones that do
    /// not exist, and repairs what a stopped commit left behind.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when a table cannot be opened, created, read, or
    /// repaired, [`StoreError::Reserved`] when a table declares the partition column,
    /// [`StoreError::Drift`] when an existing table does not match its definition, and
    /// [`StoreError::Restore`] when the ledger holds a value that does not parse.
    pub async fn open(
        settings: &DeltaSettings,
        schema: Arc<Schema>,
        chain: &str,
    ) -> Result<Self, StoreError> {
        deltalake::aws::register_handlers(None);
        let storage: HashMap<String, String> = settings
            .storage
            .iter()
            .map(|(key, value)| (key.clone(), value.expose().to_owned()))
            .collect();
        let root = settings.uri.trim_end_matches('/');
        let mut lakes = Vec::with_capacity(schema.tables().len());
        for def in schema.tables() {
            lakes.push(open_table(&format!("{root}/{chain}/{}", def.name), &storage, def).await?);
        }
        let positions = lakes
            .iter()
            .enumerate()
            .map(|(position, lake)| (lake.def.id, position))
            .collect();
        let mut sink = Self {
            chain: ChainId::new(chain),
            schema,
            lakes,
            positions,
            buffer: Buffer::default(),
            committed: HashMap::new(),
            tip: None,
            restored: Vec::new(),
            last_commit: Instant::now(),
            commit_interval: Duration::from_secs(settings.commit_interval_secs),
            max_buffer_bytes: settings.max_buffer_bytes,
            writer: WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::default()))
                .build(),
        };
        let ledger = sink.read_ledger().await?;
        sink.repair(&ledger).await?;
        sink.tip = ledger.last().map(|block| block.height);
        sink.committed = ledger
            .iter()
            .map(|block| (format!("{:#x}", block.hash), block.height))
            .collect();
        info!(%chain, uri = root, tables = sink.lakes.len(), tip = ?sink.tip, "delta lake opened");
        sink.restored = ledger;
        Ok(sink)
    }

    /// The contracts discovered on this chain that the lake holds, every one canonical:
    /// one created in an orphaned block was deleted with it, and one the ledger does not
    /// name was deleted when the store opened.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when the read fails and [`StoreError::Restore`]
    /// when a stored value does not parse.
    pub async fn contracts(&self) -> Result<Vec<StoredContract>, StoreError> {
        let batches = self
            .lake(Table::Contract)
            .read(|frame| {
                // Ordered, so a restore reads the rows the same way every time.
                frame
                    .sort(vec![ident("dedupe_key").sort(true, false)])?
                    .select(texts_of(&["protocol", "name", "address", "block_hash"]))
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

    /// The newest `limit` accepted blocks, oldest first: the undo window ingest resumes
    /// from, as read when the store opened.
    pub fn ledger(&mut self, limit: usize) -> Vec<BlockMeta> {
        let mut ledger = std::mem::take(&mut self.restored);
        let skip = ledger.len().saturating_sub(limit);
        ledger.drain(..skip);
        ledger
    }

    /// Commits whatever is still buffered. Called once storage has drained, so a clean
    /// stop leaves nothing to fetch again.
    ///
    /// # Errors
    ///
    /// As [`EnvelopeSink::flush`].
    pub async fn close(&mut self) -> Result<(), StoreError> {
        self.commit().await
    }

    fn lake(&self, table: Table) -> &Lake {
        &self.lakes[self.positions[&TableId::Dataset(table)]]
    }

    /// The newest [`LEDGER_WINDOW`] accepted blocks, oldest first.
    async fn read_ledger(&self) -> Result<Vec<BlockMeta>, StoreError> {
        let batches = self
            .lake(Table::AcceptedBlock)
            .read(|frame| {
                let seconds = cast(
                    cast(ident("timestamp"), ArrowType::Int64) / lit(1_000_000_i64),
                    ArrowType::UInt64,
                );
                frame
                    .sort(vec![ident("height").sort(false, false)])?
                    .limit(0, Some(LEDGER_WINDOW))?
                    .select(vec![
                        cast(ident("height"), ArrowType::UInt64),
                        cast(ident("hash"), ArrowType::Utf8),
                        cast(ident("parent_hash"), ArrowType::Utf8),
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
        let named: HashSet<String> = ledger
            .iter()
            .map(|block| format!("{:#x}", block.hash))
            .collect();
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
            let bound = floor.map(|floor| at_or_above(number, floor));
            let batches = lake
                .read(|frame| {
                    let frame = match bound.clone() {
                        Some(bound) => frame.filter(bound)?,
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
            let predicate = in_texts(hash, &stray);
            let predicate = match bound {
                Some(bound) => bound.and(predicate),
                None => predicate,
            };
            self.delete(position, predicate).await?;
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
        // A retraction's height is its lowest orphan's, so an unfinished one is at or
        // above the ledger's oldest block.
        let batches = self.lakes[position]
            .read(|frame| {
                frame
                    .filter(ident("height").gt_eq(height(floor)))?
                    .select(texts_of(&["dedupe_key", "orphaned_hashes"]))
            })
            .await?;
        let mut unfinished = Vec::new();
        for batch in &batches {
            let keys = texts(batch, 0, "reorgs.dedupe_key")?;
            let orphans = texts(batch, 1, "reorgs.orphaned_hashes")?;
            for row in 0..batch.num_rows() {
                let orphans: Vec<B256> = serde_json::from_str(orphans.value(row))
                    .map_err(|_| invalid("reorgs.orphaned_hashes", orphans.value(row)))?;
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
        self.delete(position, in_texts("dedupe_key", &unfinished))
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
        if arrow::partitioned(&lake.def) {
            write = write.with_partition_columns([PARTITION]);
        }
        let table = write
            .await
            .map_err(StoreError::engine(Operation::Append, name))?;
        lake.table = table;
        Ok(())
    }

    /// Writes the buffer: retractions into `reorgs`; orphaned blocks out of the ledger,
    /// then out of every table; rows into every table; then the new blocks into the
    /// ledger.
    ///
    /// A failure part-way leaves the lake as the next open repairs it, so the buffer is
    /// not retried here: the error stops storage, and a restart resumes from the ledger.
    async fn commit(&mut self) -> Result<(), StoreError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let started = Instant::now();
        let buffer = std::mem::take(&mut self.buffer);
        let ledger = self.positions[&TableId::Dataset(Table::AcceptedBlock)];
        let mut grouped = buffer.by_table();
        let ledger_rows = grouped
            .remove(&TableId::Dataset(Table::AcceptedBlock))
            .unwrap_or_default();
        let mut appended = 0;

        // A retraction is recorded while the ledger still names what it orphans, so a
        // stop before the ledger delete leaves a record the next open deletes, and never
        // a retraction with no record.
        if let Some(rows) = grouped.remove(&TableId::Dataset(Table::Reorg)) {
            appended += rows.len();
            let position = self.positions[&TableId::Dataset(Table::Reorg)];
            self.append(position, &rows).await?;
        }
        if !buffer.orphaned.is_empty() {
            let orphaned: Vec<String> = buffer.orphaned.iter().cloned().collect();
            let floor = orphaned
                .iter()
                .filter_map(|hash| self.committed.get(hash))
                .min()
                .copied();
            let others = (0..self.lakes.len()).filter(|&position| position != ledger);
            for position in std::iter::once(ledger).chain(others) {
                let Some((hash, number)) = self.lakes[position].block_columns() else {
                    continue;
                };
                let predicate = in_texts(hash, &orphaned);
                let predicate = match floor {
                    Some(floor) => at_or_above(number, floor).and(predicate),
                    None => predicate,
                };
                self.delete(position, predicate).await?;
            }
            for hash in &orphaned {
                self.committed.remove(hash);
            }
        }

        for position in 0..self.lakes.len() {
            if let Some(rows) = grouped.remove(&self.lakes[position].def.id) {
                appended += rows.len();
                self.append(position, &rows).await?;
            }
        }
        if !ledger_rows.is_empty() {
            self.append(ledger, &ledger_rows).await?;
        }

        let accepted: Vec<(String, u64)> = ledger_rows
            .iter()
            .filter_map(|row| Some((row.block_hash()?.to_owned(), row.block_number()?)))
            .collect();
        let first = accepted.iter().map(|(_, height)| *height).min();
        if let Some(last) = accepted.iter().map(|(_, height)| *height).max() {
            let tip = self.tip.map_or(last, |tip| tip.max(last));
            self.tip = Some(tip);
            let floor = tip.saturating_sub(LEDGER_WINDOW as u64);
            self.committed.extend(accepted);
            self.committed.retain(|_, height| *height >= floor);
        }
        self.last_commit = Instant::now();
        info!(
            chain = %self.chain,
            rows = appended + ledger_rows.len(),
            orphaned = buffer.orphaned.len(),
            from = ?first,
            to = ?self.tip,
            elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "lake commit"
        );
        Ok(())
    }

    /// Whether the buffer is due: old enough, or large enough.
    fn due(&self) -> bool {
        !self.buffer.is_empty()
            && (self.buffer.bytes >= self.max_buffer_bytes
                || self.last_commit.elapsed() >= self.commit_interval)
    }
}

impl EnvelopeSink for DeltaSink {
    /// Buffers one envelope's rows. A `reorg` first drops every buffered row of the
    /// blocks it orphans, and marks the committed ones for deletion; its own row is kept
    /// as the record of the retraction.
    async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
        if let Event::Reorg(reorg) = &envelope.event
            && !reorg.orphaned_hashes.is_empty()
        {
            let hashes: BTreeSet<String> = reorg
                .orphaned_hashes
                .iter()
                .map(|hash| format!("{hash:#x}"))
                .collect();
            self.buffer.drop_blocks(&hashes);
            self.buffer.orphaned.extend(
                hashes
                    .into_iter()
                    .filter(|hash| self.committed.contains_key(hash)),
            );
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
            self.buffer.push(row);
        }
        let row = self.schema.row(&envelope.chain, &envelope.event)?;
        self.buffer.push(row);
        Ok(())
    }

    /// Commits when the buffer is due. Accepting a block is not making it durable: a
    /// block buffered when the process stops is fetched again from the ledger's tip.
    async fn flush(&mut self) -> Result<(), SinkError> {
        if self.due() {
            self.commit().await?;
        }
        Ok(())
    }
}

/// Opens the table of `def` at `uri`, creating it when it does not exist, and checks an
/// existing one against its definition.
async fn open_table(
    uri: &str,
    storage: &HashMap<String, String>,
    def: &Arc<TableDef>,
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
    let url = ensure_table_uri(uri).map_err(StoreError::engine(Operation::Open, Some(name)))?;
    let mut table = DeltaTable::try_from_url_with_storage_options(url, storage.clone())
        .await
        .map_err(StoreError::engine(Operation::Open, Some(name)))?;
    if table.version().is_none() {
        let partitions = arrow::partitioned(def).then(|| PARTITION.to_owned());
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

/// `column >= height`, with the partition column bounding it too, so a scan or delete
/// skips every older partition without opening a file.
fn at_or_above(column: &str, height: u64) -> Expr {
    ident(PARTITION)
        .gt_eq(lit(arrow::partition_of(height)))
        .and(ident(column).gt_eq(self::height(height)))
}

/// A block height as the lake stores it.
fn height(height: u64) -> Expr {
    lit(ScalarValue::Decimal128(Some(i128::from(height)), 20, 0))
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

/// A value read back that is not what its column holds.
fn invalid(column: &'static str, value: &str) -> InvalidStoredValue {
    InvalidStoredValue {
        column,
        value: value.to_owned(),
    }
}

fn parse<T: std::str::FromStr>(column: &'static str, value: &str) -> Result<T, InvalidStoredValue> {
    value.parse().map_err(|_| invalid(column, value))
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
        .ok_or_else(|| invalid(column, "not text"))
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
        .ok_or_else(|| invalid(column, "not an unsigned integer"))
}

#[cfg(test)]
mod tests;
