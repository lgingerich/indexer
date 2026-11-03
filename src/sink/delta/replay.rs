//! The lake read back for a backfill: the logs it already holds, decoded again for
//! protocols added after their blocks were indexed, and the rows that produces appended
//! where they are missing.
//!
//! The running store owns the lake (see the one-writer rule in the module docs): its
//! ledger, its repair, its maintenance. A backfill touches none of them. [`LogReader`]
//! only reads `logs`, `accepted_blocks`, `sessions`, and, for a derived table's senders,
//! `transactions`, and [`DecodedTables`] only appends to `decoded_logs` and the added
//! protocols' event and derived tables, below a bound no delete of the running store
//! reaches.
//!
//! Appends never conflict with the running store's commits: in Delta a commit that only
//! adds files conflicts with a concurrent one only if that one deletes a file it read,
//! and the running store's deletes are all above the bound. Each append adds only the
//! rows its table does not hold yet, by key, so a rerun, or a range the running store
//! decoded too, ends with each row once. The small files appends leave in old partitions
//! are compacted later by the running store's maintenance, as its own are.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, B256, Bytes};
use deltalake::DeltaTable;
use deltalake::arrow::array::{Array as _, AsArray as _, BooleanArray, RecordBatch};
use deltalake::arrow::datatypes::DataType as ArrowType;
use deltalake::datafusion::dataframe::DataFrame;
use deltalake::datafusion::error::DataFusionError;
use deltalake::datafusion::prelude::{Expr, SessionConfig, SessionContext, cast, ident};
use deltalake::parquet::basic::{Compression, ZstdLevel};
use deltalake::parquet::file::properties::WriterProperties;
use futures_util::stream::{self, StreamExt as _, TryStreamExt as _};
use tokio::sync::Semaphore;
use tracing::warn;

use super::expr::{in_texts, seconds, texts, texts_of, uints, within};
use super::lake::{CONCURRENT_CHECKPOINTS, Lake};
use super::open::{Missing, Storage, open_existing, open_table};
use super::{CONCURRENT_TABLES, append_all, record_batches};
use crate::secret::Secret;
use crate::sink::resume::MAX_RESUME_LAG;
use crate::sink::store_error::{EngineError, InvalidStoredValue, Operation, StoreError};
use crate::sink::table::{Row, Schema, Session, Table, TableId};
use crate::wire::datasets::evm::Log;

/// About how many bytes of `logs` files one read takes: enough that a read's round trips
/// are a small part of it, few enough that the few read at once hold little. A file is
/// never split, so a read may take more.
const READ_BYTES: u64 = 256 * 1024 * 1024;

/// How many times a backfill's commit is tried before it stops. An append conflicts only
/// with a concurrent commit that deletes a file it read, such as a reorg's in the running
/// store's window; the next try reads the table again.
const WRITE_ATTEMPTS: u32 = 4;

/// The oldest and newest blocks of the ledger of the chain's lake under `<uri>/<chain>/`,
/// reached with the object store options `storage`, and the bound below which a backfill
/// may write (see [`LogReader::bound`]); `None` when the ledger is empty. Opens the
/// ledger alone, without listing the other tables' files, so it is cheap to ask often.
///
/// # Errors
///
/// Returns [`StoreError::Engine`] when the table does not exist or cannot be read,
/// [`StoreError::Drift`] when it does not match its definition, and
/// [`StoreError::Restore`] when a stored value does not parse.
pub async fn ledger_ends(
    uri: &str,
    storage: &BTreeMap<String, Secret>,
    schema: &Schema,
    chain: &str,
) -> Result<Option<LedgerEnds>, StoreError> {
    let root = uri.trim_end_matches('/');
    let storage = Storage::new(root, storage)?;
    let def = schema.dataset(Table::AcceptedBlock);
    let uri = format!("{root}/{chain}/{}", def.name);
    let checkpoints = Arc::new(Semaphore::new(CONCURRENT_CHECKPOINTS));
    let ledger = open_table(&storage, uri, def, Missing::Refuse, checkpoints).await?;
    let ledger = ledger.read_ledger().await?;
    Ok(ledger
        .first()
        .zip(ledger.last())
        .map(|(oldest, newest)| LedgerEnds {
            oldest: *oldest,
            newest: *newest,
            bound: newest.height.saturating_sub(MAX_RESUME_LAG),
        }))
}

/// What [`ledger_ends`] reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LedgerEnds {
    /// The ledger's oldest block, a window below its newest.
    pub oldest: crate::wire::envelope::BlockMeta,
    /// Its newest block, the lake's tip.
    pub newest: crate::wire::envelope::BlockMeta,
    /// The highest block a backfill may write.
    pub bound: u64,
}

impl LedgerEnds {
    /// The height about `seconds` before the tip, from the ledger's pace, rounded down
    /// so a read from it starts no later.
    #[must_use]
    pub fn height_before(&self, seconds: u64) -> u64 {
        let heights = self.newest.height.saturating_sub(self.oldest.height).max(1);
        let elapsed = self
            .newest
            .timestamp
            .saturating_sub(self.oldest.timestamp)
            .max(1);
        let back = u128::from(seconds) * u128::from(heights) / u128::from(elapsed);
        self.newest
            .height
            .saturating_sub(u64::try_from(back).unwrap_or(u64::MAX))
    }
}

/// A chain's lake, read only: which files of `logs` hold which heights, the newest
/// block the running store committed, and what its current run decodes. Cheap to clone,
/// so reads can run on tasks of their own.
#[derive(Debug, Clone)]
pub struct LogReader {
    table: DeltaTable,
    /// How many heights one partition of `logs` covers.
    width: u64,
    /// Every file of `logs`, with the heights it holds, in height order.
    files: Arc<[LogFile]>,
    /// The newest block in the ledger, `None` when it is empty.
    tip: Option<u64>,
    /// The running store's newest session, `None` when it has written none.
    session: Option<Session>,
    /// `transactions`, when the reader was opened to read senders (see
    /// [`senders`](Self::senders)).
    transactions: Option<Arc<Lake>>,
}

impl LogReader {
    /// Opens `logs` and `accepted_blocks` under `<uri>/<chain>/`, reached with the object
    /// store options `storage`, and `transactions` too when `senders`, lists the files of
    /// `logs`, and reads the ledger's tip and the newest session. Writes nothing, a
    /// checkpoint included.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when a table does not exist or cannot be read,
    /// [`StoreError::Drift`] when one does not match its definition, and
    /// [`StoreError::Restore`] when a stored value does not parse.
    pub async fn open(
        uri: &str,
        storage: &BTreeMap<String, Secret>,
        schema: &Schema,
        chain: &str,
        senders: bool,
    ) -> Result<Self, StoreError> {
        let root = uri.trim_end_matches('/');
        let storage = Storage::new(root, storage)?;
        let checkpoints = Arc::new(Semaphore::new(CONCURRENT_CHECKPOINTS));
        let read = |table: Table| {
            let def = schema.dataset(table);
            let uri = format!("{root}/{chain}/{}", def.name);
            open_table(
                &storage,
                uri,
                def,
                Missing::Refuse,
                Arc::clone(&checkpoints),
            )
        };
        let (logs, ledger) = tokio::try_join!(read(Table::Log), read(Table::AcceptedBlock))?;
        let transactions = if senders {
            Some(Arc::new(read(Table::Transaction).await?))
        } else {
            None
        };
        let tip = ledger.read_ledger().await?.last().map(|block| block.height);
        let sessions = schema.dataset(Table::Session);
        let uri = format!("{root}/{chain}/{}", sessions.name);
        let session = match open_existing(&storage, uri, sessions).await? {
            Some(lake) => latest_session(&lake).await?,
            None => None,
        };
        Ok(Self {
            files: log_files(&logs)?.into(),
            width: logs.width,
            table: logs.table,
            tip,
            session,
            transactions,
        })
    }

    /// The sender, recipient, and input of each transaction of `hashes` in `blocks`, by
    /// hash, as
    /// `transactions` holds them; one it does not hold is absent. Reads only the partitions
    /// and files `blocks` falls in, four columns of them, on `streams` streams at most,
    /// and keeps only the rows of
    /// `hashes` as the batches stream past. The hashes are not a filter of the scan: one
    /// of thousands of values is deeper than `DataFusion` plans without overflowing.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when the reader was opened without `senders` or the
    /// read fails, and [`StoreError::Restore`] when a stored value does not parse.
    pub async fn senders(
        &self,
        blocks: &RangeInclusive<u64>,
        hashes: &[B256],
        streams: usize,
    ) -> Result<HashMap<B256, (Address, Option<Address>, Bytes)>, StoreError> {
        let Some(transactions) = &self.transactions else {
            return Err(StoreError::engine(Operation::Read, Some("transactions"))(
                "the reader was opened without senders",
            ));
        };
        let mut senders = HashMap::with_capacity(hashes.len());
        if hashes.is_empty() {
            return Ok(senders);
        }
        let wanted: HashSet<String> = hashes.iter().map(|hash| format!("{hash:#x}")).collect();
        let plan = |frame: DataFrame| {
            frame
                .filter(within("block_number", blocks, transactions.width))?
                .select(texts_of(&["hash", "from_address", "to_address", "input"]))
        };
        scan(
            (&transactions.table, "transactions"),
            None,
            streams,
            plan,
            |batch| {
                let hash = texts(batch, 0, "transactions.hash")?;
                let from = texts(batch, 1, "transactions.from_address")?;
                let to = texts(batch, 2, "transactions.to_address")?;
                let input = texts(batch, 3, "transactions.input")?;
                for row in 0..batch.num_rows() {
                    if !wanted.contains(hash.value(row)) {
                        continue;
                    }
                    let recipient = if to.is_null(row) {
                        None
                    } else {
                        Some(InvalidStoredValue::parse(
                            "transactions.to_address",
                            to.value(row),
                        )?)
                    };
                    senders.insert(
                        InvalidStoredValue::parse("transactions.hash", hash.value(row))?,
                        (
                            InvalidStoredValue::parse(
                                "transactions.from_address",
                                from.value(row),
                            )?,
                            recipient,
                            InvalidStoredValue::parse("transactions.input", input.value(row))?,
                        ),
                    );
                }
                Ok(())
            },
        )
        .await?;
        Ok(senders)
    }

    /// How many heights one partition of the lake's `logs` covers, the width the running
    /// store writes every table with.
    #[must_use]
    pub const fn width(&self) -> u64 {
        self.width
    }

    /// The highest block a backfill may write: the ledger's tip less
    /// four ledger windows. The running store deletes only above some height — a reorg's
    /// fork, a stopped commit's leftovers, both inside its window, and a restart's rewind
    /// to a slower store, never deeper than that lag — so nothing it ever deletes reaches
    /// a block at or below this one. `None` when the ledger is empty.
    #[must_use]
    pub fn bound(&self) -> Option<u64> {
        self.tip.map(|tip| tip.saturating_sub(MAX_RESUME_LAG))
    }

    /// The running store's newest session: from which block, and with which event
    /// definitions, its current run decodes. `None` before a run of an indexer that
    /// records them has written a block.
    #[must_use]
    pub const fn session(&self) -> Option<&Session> {
        self.session.as_ref()
    }

    /// `from..=to` split into the ranges one read each covers: the files of `logs`
    /// holding heights in it, in height order, gathered until about `READ_BYTES` of
    /// them, with files whose heights overlap always in one read. Every file is read
    /// once, however the lake laid its files out: a few blocks each at the tip, or a
    /// partition in a dozen once compacted. Heights no file holds are in no read.
    #[must_use]
    pub fn reads(&self, from: u64, to: u64) -> Vec<RangeInclusive<u64>> {
        reads(&self.files, from, to)
    }

    /// The logs `addresses` emitted in `blocks`, in block and log order, read over
    /// `streams` Parquet streams at most. Owns what it reads with, so it can run on a
    /// task of its own.
    ///
    /// Each batch the scan returns is made logs as it arrives and dropped, and the logs
    /// are sorted in place: the scan's text rows are several times the logs' size, so
    /// neither they nor a sorted copy of them are ever held whole.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when the read fails, a stored value that does not
    /// parse included.
    pub fn read(
        &self,
        blocks: RangeInclusive<u64>,
        addresses: &[Address],
        streams: usize,
    ) -> impl Future<Output = Result<Vec<Log>, StoreError>> + Send + 'static {
        let (table, width) = (self.table.clone(), self.width);
        let paths: Vec<String> = self
            .files
            .iter()
            .filter(|file| file.low <= *blocks.end() && *blocks.start() <= file.high)
            .map(|file| file.path.clone())
            .collect();
        let addresses: Vec<String> = addresses
            .iter()
            .map(|address| format!("{address:#x}"))
            .collect();
        async move {
            let mut logs = Vec::new();
            if paths.is_empty() {
                return Ok(logs);
            }
            let plan = |frame: DataFrame| {
                let mut columns: Vec<Expr> = ["log_index", "transaction_index", "block_number"]
                    .map(|column| cast(ident(column), ArrowType::UInt64).alias(column))
                    .into();
                columns.extend(texts_of(&TEXT_COLUMNS));
                columns.extend([ident("removed"), seconds("block_timestamp")]);
                frame
                    .filter(within("block_number", &blocks, width))?
                    .filter(in_texts("address", &addresses))?
                    .select(columns)
            };
            scan((&table, "logs"), Some(paths), streams, plan, |batch| {
                logs_of(batch, &mut logs)
            })
            .await?;
            logs.sort_unstable_by_key(|log| (log.block_number, log.log_index));
            Ok(logs)
        }
    }
}

/// The tables a backfill writes: `decoded_logs` and every event table of its schema.
#[derive(Debug)]
pub struct DecodedTables {
    /// Every table written, in schema order.
    lakes: Vec<Lake>,
    /// Each table's position in `lakes`.
    positions: HashMap<TableId, usize>,
    writer: WriterProperties,
}

impl DecodedTables {
    /// Opens `decoded_logs` and every event and derived table of `schema` under
    /// `<uri>/<chain>/`,
    /// reached with the object store options `storage`, creating any that does not exist
    /// yet `width` heights to a partition, the width of the lake read (see
    /// [`LogReader::width`]). Starts no checkpoint, which would be left unfinished when
    /// the backfill exits; the running store checkpoints the tables it opens.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when a table cannot be opened or created, and
    /// [`StoreError::Drift`] when an existing one does not match its definition or
    /// `width`.
    pub async fn open(
        uri: &str,
        storage: &BTreeMap<String, Secret>,
        schema: &Schema,
        chain: &str,
        width: u64,
    ) -> Result<Self, StoreError> {
        let root = uri.trim_end_matches('/');
        let storage = Storage::new(root, storage)?;
        let checkpoints = Arc::new(Semaphore::new(CONCURRENT_CHECKPOINTS));
        let lakes: Vec<Lake> = stream::iter(schema.tables().iter().filter(|def| {
            matches!(
                def.id,
                TableId::Event(_) | TableId::Derived(_) | TableId::Dataset(Table::Decoded)
            )
        }))
        .map(|def| {
            let uri = format!("{root}/{chain}/{}", def.name);
            open_table(
                &storage,
                uri,
                def,
                Missing::Create { width },
                Arc::clone(&checkpoints),
            )
        })
        .boxed()
        .buffered(CONCURRENT_TABLES)
        .try_collect()
        .await?;
        let positions = lakes
            .iter()
            .enumerate()
            .map(|(position, lake)| (lake.def.id, position))
            .collect();
        Ok(Self {
            lakes,
            positions,
            writer: WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::default()))
                .build(),
        })
    }

    /// Appends each of `rows` its table does not hold yet, by key, in one commit per table
    /// that has any, and returns how many it appended. In `decoded_logs` the keys are
    /// read for `protocols` alone, since other protocols' records share the table.
    ///
    /// Only appends, so it never conflicts with the running store's own appends, and only
    /// what is missing, so a rerun, a range the running store decoded too, or a commit
    /// whose outcome was unknown writes each row once. A commit refused by a concurrent
    /// delete, such as a reorg's in the running store's window, is tried again on a
    /// fresh copy of the table, with its keys read again.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when the keys cannot be read, a table's commit still
    /// fails after a few tries, or a row is of a table not written.
    pub async fn fill(
        &mut self,
        blocks: RangeInclusive<u64>,
        protocols: &[String],
        rows: Vec<Row>,
    ) -> Result<usize, StoreError> {
        let mut by_table: BTreeMap<usize, Vec<Row>> = BTreeMap::new();
        for row in rows {
            let Some(&position) = self.positions.get(&row.table().id) else {
                return Err(StoreError::engine(
                    Operation::Append,
                    Some(&row.table().name),
                )(
                    "a row of a table the backfill does not write"
                ));
            };
            by_table.entry(position).or_default().push(row);
        }
        let mut pending: Vec<(usize, Vec<Row>)> = by_table.into_iter().collect();
        let mut appended = 0;
        for attempt in 1..=WRITE_ATTEMPTS {
            let held = futures_util::future::try_join_all(
                pending
                    .iter()
                    .map(|(position, _)| keys(&self.lakes[*position], &blocks, protocols)),
            )
            .await?;
            let missing: Vec<(usize, Vec<Row>)> = pending
                .into_iter()
                .zip(held)
                .filter_map(|((position, rows), held)| {
                    let rows: Vec<Row> = rows
                        .into_iter()
                        .filter(|row| !held.contains(row.dedupe_key()))
                        .collect();
                    (!rows.is_empty()).then_some((position, rows))
                })
                .collect();
            if missing.is_empty() {
                break;
            }
            let batches = record_batches(&self.lakes, missing.clone()).await?;
            let mut failure = None;
            pending = Vec::new();
            let written = append_all(&self.lakes, &self.writer, batches).await;
            for ((position, table), (_, rows)) in written.into_iter().zip(missing) {
                match table {
                    // Not `committed`: a backfill starts no checkpoints.
                    Ok(table) => {
                        self.lakes[position].table = table;
                        appended += rows.len();
                    }
                    Err(error) => {
                        failure.get_or_insert(error);
                        pending.push((position, rows));
                    }
                }
            }
            let Some(error) = failure else {
                break;
            };
            if attempt == WRITE_ATTEMPTS {
                return Err(error);
            }
            warn!(attempt, %error, "backfill commit failed; trying again");
            tokio::time::sleep(Duration::from_secs(u64::from(attempt))).await;
            for &(position, _) in &pending {
                let lake = &mut self.lakes[position];
                lake.table
                    .update_state()
                    .await
                    .map_err(StoreError::engine(Operation::Open, Some(&lake.def.name)))?;
            }
        }
        Ok(appended)
    }
}

/// The keys `lake` holds in `blocks`: in `decoded_logs` only those of `protocols`.
async fn keys(
    lake: &Lake,
    blocks: &RangeInclusive<u64>,
    protocols: &[String],
) -> Result<HashSet<String>, StoreError> {
    let column = lake.def.block_number.as_deref().unwrap_or("block_number");
    let decoded = lake.def.id == TableId::Dataset(Table::Decoded);
    let batches = lake
        .read(|frame| {
            let frame = frame.filter(within(column, blocks, lake.width))?;
            let frame = if decoded {
                frame.filter(in_texts("protocol", protocols))?
            } else {
                frame
            };
            frame.select(texts_of(&["dedupe_key"]))
        })
        .await?;
    let mut keys = HashSet::new();
    for batch in &batches {
        keys.extend(
            texts(batch, 0, "dedupe_key")?
                .iter()
                .flatten()
                .map(str::to_owned),
        );
    }
    Ok(keys)
}

/// The newest row of `sessions`, the running store's current run.
async fn latest_session(sessions: &Lake) -> Result<Option<Session>, StoreError> {
    let batches = sessions
        .read(|frame| {
            frame
                .sort(vec![ident("block_number").sort(false, false)])?
                .limit(0, Some(1))?
                .select(vec![
                    cast(ident("block_number"), ArrowType::UInt64),
                    cast(ident("block_hash"), ArrowType::Utf8),
                    seconds("block_timestamp"),
                    cast(ident("events"), ArrowType::Utf8),
                ])
        })
        .await?;
    let Some(batch) = batches.iter().find(|batch| batch.num_rows() > 0) else {
        return Ok(None);
    };
    Ok(Some(Session {
        block_number: uints(batch, 0, "sessions.block_number")?.value(0),
        block_hash: InvalidStoredValue::parse(
            "sessions.block_hash",
            texts(batch, 1, "sessions.block_hash")?.value(0),
        )?,
        block_timestamp: uints(batch, 2, "sessions.block_timestamp")?.value(0),
        events: Session::events_from(texts(batch, 3, "sessions.events")?.value(0))?,
    }))
}

/// One file of `logs`: its path in the table, the lowest and highest heights it holds,
/// and its size.
#[derive(Debug, Clone)]
struct LogFile {
    path: String,
    low: u64,
    high: u64,
    /// Its size, compressed.
    bytes: u64,
}

/// [`LogReader::reads`] over `files`, in height order.
fn reads(files: &[LogFile], from: u64, to: u64) -> Vec<RangeInclusive<u64>> {
    let mut reads = Vec::new();
    let mut current: Option<(u64, u64, u64)> = None;
    for file in files
        .iter()
        .filter(|file| file.low <= to && from <= file.high)
    {
        current = match current {
            Some((low, high, bytes)) if file.low <= high || bytes < READ_BYTES => {
                Some((low, high.max(file.high), bytes.saturating_add(file.bytes)))
            }
            done => {
                reads.extend(done.map(|(low, high, _)| low..=high));
                Some((file.low, file.high, file.bytes))
            }
        };
    }
    reads.extend(current.map(|(low, high, _)| low..=high));
    reads
        .into_iter()
        .map(|read| (*read.start()).max(from)..=(*read.end()).min(to))
        .collect()
}

/// Every file of `logs`, in height order, with the heights it holds as its statistics
/// record them. A file without them is taken to hold its whole partition. The running
/// store commits a few blocks at a time, so most files hold a few heights, and a read
/// opens only its own.
fn log_files(logs: &Lake) -> Result<Vec<LogFile>, StoreError> {
    let mut files = Vec::new();
    logs.each_file(|partition, file| {
        let start = u64::try_from(partition)
            .unwrap_or_default()
            .saturating_mul(logs.width);
        let (low, high) = file
            .stats()
            .and_then(|stats| block_span(&stats))
            .unwrap_or_else(|| (start, start.saturating_add(logs.width - 1)));
        files.push(LogFile {
            path: file.path().into_owned(),
            low,
            high,
            bytes: u64::try_from(file.size()).unwrap_or_default(),
        });
        Ok(())
    })?;
    files.sort_by_key(|file| (file.low, file.high));
    Ok(files)
}

/// The lowest and highest `block_number` a file's statistics, as Delta writes them,
/// record.
fn block_span(stats: &str) -> Option<(u64, u64)> {
    let stats: serde_json::Value = serde_json::from_str(stats).ok()?;
    let height = |bound: &str| stats.get(bound)?.get("block_number")?.as_u64();
    Some((height("minValues")?, height("maxValues")?))
}

/// Runs `plan` over `table`, named `name`, or only its files `paths` when given, on
/// `streams` streams at most, with filters pushed into the scan so a filter's columns are
/// read first and the rest only for the rows it keeps, and hands each batch to `each` as
/// it arrives, so no more than a few batches are held at once.
async fn scan(
    (table, name): (&DeltaTable, &str),
    paths: Option<Vec<String>>,
    streams: usize,
    plan: impl FnOnce(DataFrame) -> Result<DataFrame, DataFusionError>,
    mut each: impl FnMut(&RecordBatch) -> Result<(), InvalidStoredValue>,
) -> Result<(), StoreError> {
    let read = async {
        let mut config = SessionConfig::new().with_target_partitions(streams);
        config.options_mut().execution.parquet.pushdown_filters = true;
        config.options_mut().execution.parquet.reorder_filters = true;
        let ctx = SessionContext::new_with_config(config);
        table.update_datafusion_session(&ctx.state())?;
        let provider = table.table_provider();
        let provider = match paths {
            Some(paths) => provider.with_file_paths(paths).await?,
            None => provider.await?,
        };
        let mut batches = plan(ctx.read_table(provider)?)?.execute_stream().await?;
        while let Some(batch) = batches.next().await {
            each(&batch?)?;
        }
        Ok::<_, EngineError>(())
    };
    read.await
        .map_err(StoreError::engine(Operation::Read, Some(name)))
}

/// The `logs` columns read as text, in the order [`logs_of`] takes them, after the three
/// integer columns.
const TEXT_COLUMNS: [&str; 8] = [
    "transaction_hash",
    "address",
    "topic0",
    "topic1",
    "topic2",
    "topic3",
    "data",
    "block_hash",
];

/// Appends the logs of `batch`, read as [`LogReader::read`] selects them, to `logs`.
fn logs_of(batch: &RecordBatch, logs: &mut Vec<Log>) -> Result<(), InvalidStoredValue> {
    const TOPICS: [&str; 4] = ["logs.topic0", "logs.topic1", "logs.topic2", "logs.topic3"];
    let log_index = uints(batch, 0, "logs.log_index")?;
    let transaction_index = uints(batch, 1, "logs.transaction_index")?;
    let block_number = uints(batch, 2, "logs.block_number")?;
    let transaction_hash = texts(batch, 3, "logs.transaction_hash")?;
    let address = texts(batch, 4, "logs.address")?;
    let topics = [
        texts(batch, 5, TOPICS[0])?,
        texts(batch, 6, TOPICS[1])?,
        texts(batch, 7, TOPICS[2])?,
        texts(batch, 8, TOPICS[3])?,
    ];
    let data = texts(batch, 9, "logs.data")?;
    let block_hash = texts(batch, 10, "logs.block_hash")?;
    let removed: &BooleanArray = batch
        .column(11)
        .as_boolean_opt()
        .ok_or_else(|| InvalidStoredValue::new("logs.removed", "not a boolean"))?;
    let block_timestamp = uints(batch, 12, "logs.block_timestamp")?;
    let topic = |index: usize, row: usize| -> Result<Option<B256>, InvalidStoredValue> {
        let values = topics[index];
        if values.is_null(row) {
            return Ok(None);
        }
        InvalidStoredValue::parse(TOPICS[index], values.value(row)).map(Some)
    };
    for row in 0..batch.num_rows() {
        logs.push(Log {
            log_index: log_index.value(row),
            transaction_hash: InvalidStoredValue::parse(
                "logs.transaction_hash",
                transaction_hash.value(row),
            )?,
            transaction_index: transaction_index.value(row),
            address: InvalidStoredValue::parse("logs.address", address.value(row))?,
            topic0: topic(0, row)?,
            topic1: topic(1, row)?,
            topic2: topic(2, row)?,
            topic3: topic(3, row)?,
            data: InvalidStoredValue::parse::<Bytes>("logs.data", data.value(row))?,
            removed: removed.value(row),
            block_number: block_number.value(row),
            block_hash: InvalidStoredValue::parse("logs.block_hash", block_hash.value(row))?,
            block_timestamp: block_timestamp.value(row),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{LogFile, READ_BYTES, block_span, reads};

    fn file(low: u64, high: u64, bytes: u64) -> LogFile {
        LogFile {
            path: String::new(),
            low,
            high,
            bytes,
        }
    }

    /// Small files gather into one read until it reaches its size; files whose heights
    /// overlap stay in one read whatever their size; heights no file holds, and those
    /// outside `from..=to`, are in none.
    #[test]
    fn reads_gather_files_by_size_and_keep_overlaps_together() {
        let small = READ_BYTES / 4;
        let files = [
            file(0, 9, small),
            file(10, 19, small),
            file(20, 29, small),
            file(30, 39, small),
            file(40, 49, small),
            file(45, 60, READ_BYTES),
            file(55, 70, READ_BYTES),
            file(100, 109, small),
        ];
        assert_eq!(reads(&files, 0, 200), [0..=39, 40..=70, 100..=109]);
        assert_eq!(reads(&files, 15, 104), [15..=70, 100..=104]);
        assert_eq!(reads(&files, 71, 99), []);
    }

    /// A file's heights come from the statistics Delta writes, where a `Uint` is a JSON
    /// integer; without them the file has no span of its own.
    #[test]
    fn a_files_heights_come_from_its_statistics() {
        let stats = r#"{"numRecords":3,"minValues":{"block_number":26198432,"log_index":0},"maxValues":{"block_number":26199999,"log_index":7},"nullCount":{}}"#;
        assert_eq!(block_span(stats), Some((26_198_432, 26_199_999)));
        assert_eq!(block_span(r#"{"numRecords":3}"#), None);
        assert_eq!(block_span("not json"), None);
    }
}
