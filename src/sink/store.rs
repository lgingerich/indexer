//! A SQL store, written once over the engine that runs it.
//!
//! [`SqlStore`] holds what every store does the same way: the tables it creates, the
//! batch it buffers between flushes, a reorg's deletes, the staged upsert, and the
//! startup reads. An [`Engine`] only runs statements, bulk-loads rows into a staging
//! table, and says how its SQL differs (see [`Dialect`]).
//!
//! # A flush
//!
//! One transaction: delete the rows of every block a buffered `reorg` orphaned, then for
//! each table with rows, create a staging table, bulk-load the rows into it, and merge it
//! with `INSERT … ON CONFLICT DO UPDATE` on `(chain, dedupe_key)`. Deleting first is what
//! makes a block that returns — orphaned, then canonical again in a later `reorg` — end up
//! stored. On failure the transaction rolls back and the whole batch stays buffered for a
//! retry, which deletes nothing it already deleted and upserts onto itself.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use thiserror::Error;

use crate::decode::StoredContract;
use crate::sink::sql::{self, Dialect};
use crate::sink::table::{Row, Schema, Table, TableDef, TableError, TableId, Value};
use crate::sink::{EnvelopeSink, SinkError};
use crate::wire::envelope::{AcceptedBlock, ChainId, Envelope, Event};

/// An engine's own error, carried as its cause.
pub type EngineError = Box<dyn std::error::Error + Send + Sync>;

/// What runs a [`SqlStore`]'s statements.
pub trait Engine: Dialect + Send {
    /// Runs one statement. With no parameters it may run as plain SQL, for statements an
    /// engine will not prepare, such as `BEGIN`.
    fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Result<(), EngineError>> + Send;

    /// Runs one query and returns its rows.
    fn query(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> impl Future<Output = Result<Vec<Vec<Value>>, EngineError>> + Send;

    /// Bulk-loads `rows` of `table` into the staging table `staging`.
    fn load(
        &mut self,
        table: &TableDef,
        staging: &str,
        rows: &[&Row],
    ) -> impl Future<Output = Result<(), EngineError>> + Send;
}

/// What a store was doing when its engine failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// Applying a setting.
    Configure,
    /// Opening or connecting to the database.
    Open,
    /// Creating the database schema, tables, or indexes.
    Create,
    /// Starting a flush's transaction.
    Begin,
    /// Deleting an orphaned block's rows.
    Delete,
    /// Creating, loading, or dropping a staging table.
    Stage,
    /// Merging staged rows into their table.
    Merge,
    /// Committing a flush.
    Commit,
    /// Reading stored contracts or accepted blocks at startup.
    Read,
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Configure => "apply setting",
            Self::Open => "open store",
            Self::Create => "create",
            Self::Begin => "begin transaction",
            Self::Delete => "delete orphaned rows of",
            Self::Stage => "stage",
            Self::Merge => "upsert",
            Self::Commit => "commit transaction",
            Self::Read => "read",
        })
    }
}

/// Why a store could not be opened, read, or written.
#[derive(Debug, Error)]
pub enum StoreError {
    /// The engine failed during `operation`, on `target` — a table, a setting, or a path —
    /// when there is one.
    #[error("{operation}{}: {source}", target.as_ref().map(|target| format!(" {target}")).unwrap_or_default())]
    Engine {
        /// What the store was doing.
        operation: Operation,
        /// What it was doing it to.
        target: Option<String>,
        /// The engine's own error.
        source: EngineError,
    },
    /// A value read back at startup did not parse.
    #[error(transparent)]
    Restore(#[from] InvalidStoredValue),
    /// A table's declaration is inconsistent.
    #[error(transparent)]
    Table(#[from] TableError),
    /// An existing table does not match its definition. Tables are not migrated, so the
    /// store needs a fresh database schema or the table dropped.
    #[error("table {table} does not match its definition: {difference}")]
    Drift {
        /// The table.
        table: String,
        /// The first difference found, such as a column with another type.
        difference: String,
    },
}

impl StoreError {
    /// Wraps an engine error from `operation` on `target`.
    pub(crate) fn engine(
        operation: Operation,
        target: Option<&str>,
    ) -> impl FnOnce(EngineError) -> Self {
        move |source| Self::Engine {
            operation,
            target: target.map(str::to_owned),
            source,
        }
    }
}

/// A value read back at startup that does not parse as its column's type.
#[derive(Debug, Error)]
#[error("stored {column} is invalid: {value}")]
pub struct InvalidStoredValue {
    /// The table and column, for example `contracts.address`.
    pub column: &'static str,
    /// The stored value.
    pub value: String,
}

/// One table, with every statement a flush runs against it rendered once.
#[derive(Debug)]
struct Prepared {
    def: Arc<TableDef>,
    /// The staging table, named by position so a 63-byte table name cannot push it past
    /// an identifier limit.
    staging: String,
    create_staging: String,
    merge: String,
    drop_staging: Option<String>,
    delete: Option<String>,
}

impl Prepared {
    fn new<D: Dialect>(position: usize, def: &Arc<TableDef>) -> Self {
        let staging = format!("staging_{position}");
        Self {
            create_staging: D::create_staging(def, &staging),
            merge: sql::upsert::<D>(def, &staging),
            drop_staging: D::drop_staging(&staging),
            delete: sql::delete_block(def),
            staging,
            def: Arc::clone(def),
        }
    }
}

/// The startup reads, rendered once.
#[derive(Debug)]
struct Reads {
    contracts: String,
    ledger: String,
    prune_ledger: String,
}

impl Reads {
    fn new(schema: &Schema) -> Result<Self, TableError> {
        let contracts = schema.dataset(Table::Contract);
        let ledger = schema.dataset(Table::AcceptedBlock);
        Ok(Self {
            // Ordered, so a restore reads the rows the same way every time; an engine
            // returns an unordered scan in whatever order its threads finish.
            contracts: sql::select(contracts, &["protocol", "name", "address", "block_hash"])
                .ordered_by("dedupe_key")
                .render()?,
            ledger: sql::select(ledger, &["height", "hash", "parent_hash", "timestamp"])
                .newest_first("height")
                .limit()
                .render()?,
            prune_ledger: sql::delete_below(ledger, "height")?,
        })
    }
}

/// A store: the run's tables in one database schema, written in one transaction per flush.
///
/// Rows stay buffered until a commit succeeds. A replay of `(chain, dedupe_key)` updates
/// that row; within a batch the last copy wins. No writes occur until
/// [`EnvelopeSink::flush`].
#[derive(Debug)]
pub struct SqlStore<E> {
    pub(crate) engine: E,
    batch: Batch,
    tables: Vec<Prepared>,
    reads: Reads,
}

impl<E: Engine> SqlStore<E> {
    /// Creates every table in `schema`, with its indexes, in the database schema
    /// `database_schema`, which becomes the session's default.
    ///
    /// A run names that schema for its chain — `base.logs` — so chains sharing a
    /// database keep their tables apart. Existing tables are reused, not migrated, and
    /// checked against their definitions.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when the schema or a table cannot be created,
    /// [`StoreError::Drift`] when an existing table does not match its definition, and
    /// [`StoreError::Table`] when a startup read names a column its table lacks.
    pub(crate) async fn new(
        mut engine: E,
        schema: Arc<Schema>,
        database_schema: &str,
    ) -> Result<Self, StoreError> {
        for statement in [
            sql::create_schema(database_schema),
            E::use_schema(database_schema),
        ] {
            engine
                .execute(&statement, &[])
                .await
                .map_err(StoreError::engine(Operation::Create, Some(database_schema)))?;
        }
        engine
            .execute("BEGIN", &[])
            .await
            .map_err(StoreError::engine(Operation::Begin, None))?;
        for def in schema.tables() {
            for statement in
                std::iter::once(sql::create_table::<E>(def)).chain(sql::create_indexes::<E>(def))
            {
                engine
                    .execute(&statement, &[])
                    .await
                    .map_err(StoreError::engine(Operation::Create, Some(&def.name)))?;
            }
        }
        engine
            .execute("COMMIT", &[])
            .await
            .map_err(StoreError::engine(Operation::Commit, None))?;
        for def in schema.tables() {
            check(&mut engine, def).await?;
        }
        Ok(Self {
            engine,
            tables: schema
                .tables()
                .iter()
                .enumerate()
                .map(|(position, def)| Prepared::new::<E>(position, def))
                .collect(),
            reads: Reads::new(&schema)?,
            batch: Batch::new(schema),
        })
    }

    /// The contracts discovered on `chain` that this store holds. One created in a block a
    /// `reorg` orphaned was deleted with that block, so every row here is canonical.
    ///
    /// Read once at startup, before the first write, so a restart decodes every contract
    /// a previous run discovered.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when the query fails and [`StoreError::Restore`]
    /// when a stored value does not parse.
    pub async fn contracts(&mut self, chain: &ChainId) -> Result<Vec<StoredContract>, StoreError> {
        let rows = self
            .engine
            .query(&self.reads.contracts, &[chain_param(chain)])
            .await
            .map_err(StoreError::engine(Operation::Read, Some("contracts")))?;
        rows.iter()
            .map(|row| {
                Ok(StoredContract {
                    protocol: text("contracts.protocol", column(row, 0))?.to_owned(),
                    name: text("contracts.name", column(row, 1))?.to_owned(),
                    address: parse("contracts.address", column(row, 2))?,
                    block_hash: parse("contracts.block_hash", column(row, 3))?,
                })
            })
            .collect()
    }

    /// The newest `limit` accepted blocks on `chain`, oldest first, after dropping every
    /// older row. An orphaned block's row was deleted by its `reorg`, so these are
    /// canonical.
    ///
    /// Read once at startup, before the first write: the result is the undo window a
    /// restart resumes from. Rows below the oldest one returned can never be read again,
    /// so they are deleted here, which is what keeps the ledger from growing without
    /// bound across restarts. Contiguity is not checked; the pipeline does that.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when the query or the delete fails and
    /// [`StoreError::Restore`] when a stored value does not parse.
    pub async fn ledger(
        &mut self,
        chain: &ChainId,
        limit: usize,
    ) -> Result<Vec<AcceptedBlock>, StoreError> {
        let read = || StoreError::engine(Operation::Read, Some("accepted_blocks"));
        let limit = Value::Int(i64::try_from(limit).unwrap_or(i64::MAX));
        let rows = self
            .engine
            .query(&self.reads.ledger, &[chain_param(chain), limit])
            .await
            .map_err(read())?;
        let mut ledger = rows
            .iter()
            .map(|row| {
                Ok(AcceptedBlock {
                    height: uint("accepted_blocks.height", column(row, 0))?,
                    hash: parse("accepted_blocks.hash", column(row, 1))?,
                    parent_hash: parse("accepted_blocks.parent_hash", column(row, 2))?,
                    timestamp: timestamp("accepted_blocks.timestamp", column(row, 3))?,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        ledger.reverse();
        if let Some(oldest) = ledger.first() {
            self.engine
                .execute(
                    &self.reads.prune_ledger,
                    &[chain_param(chain), Value::Uint(oldest.height)],
                )
                .await
                .map_err(read())?;
        }
        Ok(ledger)
    }

    /// How many rows are buffered for the next flush.
    #[cfg(test)]
    pub(crate) fn buffered(&self) -> usize {
        self.batch.rows.len()
    }

    /// Writes the batch in one transaction, then clears it.
    async fn write_batch(&mut self) -> Result<(), StoreError> {
        if self.batch.is_empty() {
            return Ok(());
        }
        self.engine
            .execute("BEGIN", &[])
            .await
            .map_err(StoreError::engine(Operation::Begin, None))?;
        if let Err(error) = self.write_tables().await {
            // The rollback's own failure says nothing the write's does not; the batch stays
            // buffered either way.
            let _ = self.engine.execute("ROLLBACK", &[]).await;
            return Err(error);
        }
        self.engine
            .execute("COMMIT", &[])
            .await
            .map_err(StoreError::engine(Operation::Commit, None))?;
        self.batch.clear();
        Ok(())
    }

    /// The deletes, then each table's stage, load, and merge.
    async fn write_tables(&mut self) -> Result<(), StoreError> {
        for table in &self.tables {
            let Some(delete) = &table.delete else {
                continue;
            };
            for (chain, hashes) in &self.batch.orphaned {
                for hash in hashes {
                    let params = [Value::Text(chain.clone()), Value::Text(hash.clone())];
                    self.engine
                        .execute(delete, &params)
                        .await
                        .map_err(StoreError::engine(Operation::Delete, Some(&table.def.name)))?;
                }
            }
        }
        let mut grouped = self.batch.by_table();
        for table in &self.tables {
            let Some(rows) = grouped.remove(&table.def.id) else {
                continue;
            };
            let name = Some(table.def.name.as_str());
            let stage = StoreError::engine(Operation::Stage, name);
            self.engine
                .execute(&table.create_staging, &[])
                .await
                .map_err(stage)?;
            self.engine
                .load(&table.def, &table.staging, &rows)
                .await
                .map_err(StoreError::engine(Operation::Stage, name))?;
            self.engine
                .execute(&table.merge, &[])
                .await
                .map_err(StoreError::engine(Operation::Merge, name))?;
            if let Some(drop) = &table.drop_staging {
                self.engine
                    .execute(drop, &[])
                    .await
                    .map_err(StoreError::engine(Operation::Stage, name))?;
            }
        }
        Ok(())
    }
}

impl<E: Engine> EnvelopeSink for SqlStore<E> {
    async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
        self.batch.push(&envelope)
    }

    async fn flush(&mut self) -> Result<(), SinkError> {
        self.write_batch().await?;
        Ok(())
    }
}

/// Fails when `table` as stored lacks a column, holds one it does not declare, or types
/// one differently, so a changed definition stops the run at startup rather than at its
/// first write.
async fn check<E: Engine>(engine: &mut E, table: &TableDef) -> Result<(), StoreError> {
    let rows = engine
        .query(E::DESCRIBE, &[Value::Text(table.name.clone())])
        .await
        .map_err(StoreError::engine(Operation::Read, Some(&table.name)))?;
    let mut stored = Vec::with_capacity(rows.len());
    for row in &rows {
        stored.push((
            text("column.name", column(row, 0))?,
            text("column.type", column(row, 1))?,
        ));
    }
    let drift = |difference: String| StoreError::Drift {
        table: table.name.clone(),
        difference,
    };
    for column in &table.columns {
        let expected = E::reported_type(column.kind);
        match stored.iter().find(|(name, _)| *name == column.name) {
            None => return Err(drift(format!("it has no {} column", column.name))),
            Some((_, found)) if !found.eq_ignore_ascii_case(&expected) => {
                return Err(drift(format!("{} is {found}, not {expected}", column.name)));
            }
            Some(_) => {}
        }
    }
    if let Some((name, _)) = stored
        .iter()
        .find(|(name, _)| table.position(name).is_none())
    {
        return Err(drift(format!("it has a {name} column it should not")));
    }
    Ok(())
}

fn chain_param(chain: &ChainId) -> Value {
    Value::Text(chain.as_str().to_owned())
}

/// The value at `index` of a row read back, or `Null` when the row is shorter: a short
/// row then fails as an invalid value of that column rather than out of bounds.
fn column(row: &[Value], index: usize) -> &Value {
    row.get(index).unwrap_or(&Value::Null)
}

/// A stored value that is not what its column holds.
fn invalid(column: &'static str, value: &Value) -> InvalidStoredValue {
    InvalidStoredValue {
        column,
        value: format!("{value:?}"),
    }
}

fn text<'a>(column: &'static str, value: &'a Value) -> Result<&'a str, InvalidStoredValue> {
    match value {
        Value::Text(text) => Ok(text),
        other => Err(invalid(column, other)),
    }
}

fn parse<T: std::str::FromStr>(
    column: &'static str,
    value: &Value,
) -> Result<T, InvalidStoredValue> {
    text(column, value)?
        .parse()
        .map_err(|_| invalid(column, value))
}

/// A `Uint` column, which an engine without unsigned integers reads back as a wide one.
fn uint(column: &'static str, value: &Value) -> Result<u64, InvalidStoredValue> {
    match value {
        Value::Uint(number) => Ok(*number),
        Value::BigInt {
            negative: false,
            magnitude,
        } => u64::try_from(*magnitude).map_err(|_| invalid(column, value)),
        other => Err(invalid(column, other)),
    }
}

fn timestamp(column: &'static str, value: &Value) -> Result<u64, InvalidStoredValue> {
    match value {
        Value::Timestamp(seconds) => Ok(*seconds),
        other => Err(invalid(column, other)),
    }
}

/// What a store has been handed since its last commit: the rows to write, and the
/// blocks a buffered `reorg` retracted.
///
/// A decoded record is two rows: its generic `decoded_logs` row, and its event's typed
/// row, from the run's [`Schema`].
///
/// A store holds only the canonical chain. A `reorg` orphans blocks that are either
/// already committed or still in this buffer — blocks are published in order and the
/// storage channel is FIFO, so an orphaned block can never arrive after its `reorg`.
/// [`push`](Self::push) drops the buffered ones at once, and the store deletes the
/// committed ones in the same transaction that writes [`rows`](Self::rows), before
/// writing them.
#[derive(Debug)]
struct Batch {
    /// Rows to upsert, in publish order.
    rows: Vec<Row>,
    /// Orphaned block hashes to delete from the store, as `0x` hex, by chain.
    orphaned: BTreeMap<String, BTreeSet<String>>,
    /// Every table the run writes, which renders each decoded record's typed row.
    schema: Arc<Schema>,
}

impl Batch {
    fn new(schema: Arc<Schema>) -> Self {
        Self {
            rows: Vec::new(),
            orphaned: BTreeMap::new(),
            schema,
        }
    }

    /// Buffers one envelope's rows. A `reorg` first drops every buffered row of the
    /// blocks it orphans and records them for deletion; its own row is kept as the
    /// record of the retraction.
    ///
    /// # Errors
    ///
    /// Returns [`SinkError::UnknownEvent`] for a decoded record no event table holds,
    /// which means it was decoded against a different catalog than the store opened with,
    /// and [`SinkError::Table`] when a row cannot be built.
    fn push(&mut self, envelope: &Envelope) -> Result<(), SinkError> {
        if let Event::Reorg(reorg) = &envelope.event
            && !reorg.orphaned_hashes.is_empty()
        {
            let chain = envelope.chain.as_str();
            let hashes: BTreeSet<String> = reorg
                .orphaned_hashes
                .iter()
                .map(|hash| format!("{hash:#x}"))
                .collect();
            self.rows.retain(|row| {
                row.chain() != chain || !row.block_hash().is_some_and(|h| hashes.contains(h))
            });
            self.orphaned
                .entry(chain.to_owned())
                .or_default()
                .extend(hashes);
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
            self.rows.push(row);
        }
        self.rows
            .push(self.schema.row(&envelope.chain, &envelope.event)?);
        Ok(())
    }

    /// The rows each table should load: the last buffered copy of each
    /// `(chain, dedupe_key)`, in publish order.
    ///
    /// A merge must not see a key twice — both engines refuse to update one conflict row
    /// twice in a statement — and "last" means last published, which only the buffer
    /// knows; the staging table's physical order does not promise it.
    fn by_table(&self) -> HashMap<TableId, Vec<&Row>> {
        let mut seen = HashSet::new();
        let mut tables: HashMap<TableId, Vec<&Row>> = HashMap::new();
        for row in self.rows.iter().rev() {
            let id = row.table().id;
            if seen.insert((id, row.chain(), row.dedupe_key())) {
                tables.entry(id).or_default().push(row);
            }
        }
        for rows in tables.values_mut() {
            rows.reverse();
        }
        tables
    }

    /// Whether there is nothing to commit. A `reorg` always buffers its own row, so a
    /// batch with deletions is never empty.
    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Forgets everything, once a commit has made it durable.
    fn clear(&mut self) {
        self.rows.clear();
        self.orphaned.clear();
    }
}
