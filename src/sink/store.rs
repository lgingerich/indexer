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
//! each table with rows, bulk-load the rows into its staging table and merge it with
//! `INSERT … ON CONFLICT DO UPDATE` on `(chain, dedupe_key)`, then drop the ledger rows
//! below the window a restart reads back. Deleting first is what makes a block that
//! returns — orphaned, then canonical again in a later `reorg` — end up stored. On failure
//! the transaction rolls back and the whole batch stays buffered for a retry, which
//! deletes nothing it already deleted and upserts onto itself.
//!
//! The staging tables are created once a session, not once a flush: a table created and
//! dropped every commit churns the database's catalog, which at the tip costs more than
//! the few rows it stages. A flush empties the ones it loaded with a `DELETE`, and once
//! a table has staged twenty thousand rows since it was last vacuumed, the store vacuums
//! it after the commit, on an engine that needs it.

use std::collections::HashMap;
use std::sync::Arc;

use crate::ingest::pipeline::LEDGER_WINDOW;
use crate::sink::batch::Batch;
use crate::sink::sql::{self, Dialect};
use crate::sink::store_error::{EngineError, InvalidStoredValue, Operation, StoreError};
use crate::sink::table::{Row, Schema, Table, TableDef, TableError, TableId, Value};
use crate::sink::{EnvelopeSink, Restored, SinkError, Store};
use crate::wire::envelope::{BlockMeta, ChainId, Envelope, StoredContract};

/// What runs a [`SqlStore`]'s statements.
pub trait Engine: Send {
    /// The SQL this engine speaks. A wrapper around another engine names the inner one's.
    type Dialect: Dialect;

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

    /// After a failed write, restores the engine's connection if the server closed it,
    /// returning whether it did, so the write can be retried on the new one. An engine
    /// with no connection to lose has nothing to restore.
    fn recover(&mut self) -> impl Future<Output = Result<bool, StoreError>> + Send {
        std::future::ready(Ok(false))
    }
}

/// How many times one flush is written again on a restored connection before its error
/// stands, so a connection that keeps failing as soon as it is restored stops the store.
const RETRIES: usize = 3;

/// How many rows a staging table takes before the store vacuums it: about one backlog
/// commit, which leaves it some ten megabytes of dead rows, and at the tip one vacuum in
/// hundreds of commits. Vacuuming that many takes milliseconds.
const VACUUM_ROWS: usize = 20_000;

/// One table, with every statement a flush runs against it rendered once.
#[derive(Debug)]
struct Prepared {
    def: Arc<TableDef>,
    /// The staging table, named by position so a 63-byte table name cannot push it past
    /// an identifier limit.
    staging: String,
    create_staging: String,
    merge: String,
    clear_staging: String,
    /// Rows loaded into the staging table since it was created or last vacuumed.
    staged: usize,
    delete: Option<String>,
    /// What a rewind runs: every block above a height, in one statement.
    delete_above: Option<String>,
}

impl Prepared {
    fn new<D: Dialect>(position: usize, def: &Arc<TableDef>) -> Self {
        let staging = format!("staging_{position}");
        Self {
            create_staging: D::create_staging(def, &staging),
            merge: sql::upsert::<D>(def, &staging),
            clear_staging: D::clear_staging(&staging),
            staged: 0,
            delete: sql::delete_blocks::<D>(def),
            delete_above: sql::delete_above(def),
            staging,
            def: Arc::clone(def),
        }
    }
}

/// The startup reads, and the ledger's prune, rendered once.
#[derive(Debug)]
struct Reads {
    contracts: String,
    ledger: String,
    /// Run by every flush that accepts a block.
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
    /// The chain a [`restore`](Store::restore) reads back.
    chain: ChainId,
    batch: Batch,
    tables: Vec<Prepared>,
    reads: Reads,
}

impl<E: Engine> SqlStore<E> {
    /// Creates every table in `schema`, with its indexes, in the database schema named
    /// for `chain`, which becomes the session's default.
    ///
    /// Each chain's tables live in its own schema — `base.logs` — so chains sharing a
    /// database keep their tables apart, and a [`restore`](Store::restore) reads back
    /// `chain`'s rows. Existing tables are reused, not migrated, and checked against
    /// their definitions.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when the schema or a table cannot be created,
    /// [`StoreError::Drift`] when an existing table does not match its definition, and
    /// [`StoreError::Table`] when a startup read names a column its table lacks.
    pub(crate) async fn new(
        mut engine: E,
        schema: Arc<Schema>,
        chain: &str,
    ) -> Result<Self, StoreError> {
        for statement in [sql::create_schema(chain), E::Dialect::use_schema(chain)] {
            engine
                .execute(&statement, &[])
                .await
                .map_err(StoreError::engine(Operation::Create, Some(chain)))?;
        }
        engine
            .execute("BEGIN", &[])
            .await
            .map_err(StoreError::engine(Operation::Begin, None))?;
        // Every table and index in one batch, so a remote database pays one round trip for
        // the schema rather than one a statement. Each is `IF NOT EXISTS`, so a rerun
        // creates only what is missing. A failure rolls back, so the session is not left in
        // an aborted transaction.
        let create = E::Dialect::UNTIMED
            .map(str::to_owned)
            .into_iter()
            .chain(schema.tables().iter().flat_map(|def| {
                std::iter::once(sql::create_table::<E::Dialect>(def))
                    .chain(sql::create_indexes::<E::Dialect>(def))
            }))
            .collect::<Vec<_>>()
            .join(";\n");
        if let Err(error) = engine.execute(&create, &[]).await {
            let _ = engine.execute("ROLLBACK", &[]).await;
            return Err(StoreError::engine(Operation::Create, Some(chain))(error));
        }
        engine
            .execute("COMMIT", &[])
            .await
            .map_err(StoreError::engine(Operation::Commit, None))?;
        let stored = describe(&mut engine).await?;
        for def in schema.tables() {
            check::<E::Dialect>(def, stored.get(&def.name).map_or(&[], Vec::as_slice))?;
        }
        let mut store = Self {
            engine,
            chain: ChainId::new(chain),
            tables: schema
                .tables()
                .iter()
                .enumerate()
                .map(|(position, def)| Prepared::new::<E::Dialect>(position, def))
                .collect(),
            reads: Reads::new(&schema)?,
            batch: Batch::new(schema),
        };
        store.create_staging().await?;
        Ok(store)
    }

    /// Creates every table's staging table, in one batch: once when the store opens, and
    /// again on each connection the engine restores, since they last only a session.
    async fn create_staging(&mut self) -> Result<(), StoreError> {
        let create = self
            .tables
            .iter()
            .map(|table| table.create_staging.as_str())
            .collect::<Vec<_>>()
            .join(";\n");
        self.engine
            .execute(&create, &[])
            .await
            .map_err(StoreError::engine(Operation::Stage, None))?;
        for table in &mut self.tables {
            table.staged = 0;
        }
        Ok(())
    }

    /// Vacuums, in one statement, every staging table that has taken [`VACUUM_ROWS`]
    /// since it was last vacuumed. Runs between transactions, as a vacuum must.
    async fn vacuum_staging(&mut self) -> Result<(), StoreError> {
        let full = self
            .tables
            .iter()
            .filter(|table| table.staged >= VACUUM_ROWS)
            .map(|table| table.staging.as_str())
            .collect::<Vec<_>>();
        if full.is_empty() {
            return Ok(());
        }
        if let Some(vacuum) = E::Dialect::vacuum_staging(&full) {
            self.engine
                .execute(&vacuum, &[])
                .await
                .map_err(StoreError::engine(Operation::Stage, None))?;
        }
        for table in &mut self.tables {
            if table.staged >= VACUUM_ROWS {
                table.staged = 0;
            }
        }
        Ok(())
    }

    /// The contracts discovered on `chain` that this store holds. One created in a block a
    /// `reorg` orphaned was deleted with that block, so every row here is canonical.
    ///
    /// Read by [`restore`](Store::restore), so a restart decodes every contract a
    /// previous run discovered.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when the query fails and [`StoreError::Restore`]
    /// when a stored value does not parse.
    pub(crate) async fn contracts(
        &mut self,
        chain: &ChainId,
    ) -> Result<Vec<StoredContract>, StoreError> {
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

    /// The newest `limit` accepted blocks on `chain`, oldest first. An orphaned block's
    /// row was deleted by its `reorg`, so these are canonical.
    ///
    /// Read by [`restore`](Store::restore), before the first write: the result is the
    /// undo window a restart resumes from. Each flush drops the rows below that window
    /// (see [`prune_ledger`](Self::prune_ledger)), so this reads a table of about
    /// [`LEDGER_WINDOW`] rows. Contiguity is not checked; the pipeline does that.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when the query fails and [`StoreError::Restore`]
    /// when a stored value does not parse.
    pub(crate) async fn ledger(
        &mut self,
        chain: &ChainId,
        limit: usize,
    ) -> Result<Vec<BlockMeta>, StoreError> {
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
                Ok(BlockMeta {
                    height: uint("accepted_blocks.height", column(row, 0))?,
                    hash: parse("accepted_blocks.hash", column(row, 1))?,
                    parent_hash: parse("accepted_blocks.parent_hash", column(row, 2))?,
                    timestamp: timestamp("accepted_blocks.timestamp", column(row, 3))?,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        ledger.reverse();
        Ok(ledger)
    }

    /// How many rows are buffered for the next flush.
    #[cfg(test)]
    pub(crate) fn buffered(&self) -> usize {
        self.batch.rows.len()
    }

    /// Writes the batch in one transaction, clears it, then vacuums the staging tables that
    /// are due. A vacuum that fails fails the flush, though its batch is committed.
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
        self.vacuum_staging().await
    }

    /// Deletes every block above `height` from each table whose rows belong to one, the
    /// ledger included, by height (see [`sql::delete_above`]).
    async fn delete_above(&mut self, height: u64) -> Result<(), StoreError> {
        let params = [chain_param(&self.chain), Value::Uint(height)];
        for table in &self.tables {
            let Some(delete) = &table.delete_above else {
                continue;
            };
            self.engine
                .execute(delete, &params)
                .await
                .map_err(StoreError::engine(Operation::Delete, Some(&table.def.name)))?;
        }
        Ok(())
    }

    /// Deletes each chain's ledger rows more than [`LEDGER_WINDOW`] below the newest block
    /// this flush accepts on it, as the lake keeps its window: no restart reads them, and
    /// left in place they grow the table, and the cost of every write to it, a row a block.
    ///
    /// `tips` are the newest accepted heights by chain. Each is canonical — a buffered
    /// `reorg` dropped the rows of the blocks it orphaned — and at or below the chain's tip,
    /// so the window kept is never short.
    async fn prune_ledger(&mut self, tips: HashMap<String, u64>) -> Result<(), StoreError> {
        for (chain, tip) in tips {
            let Some(floor) = tip.checked_sub(LEDGER_WINDOW as u64) else {
                continue;
            };
            self.engine
                .execute(
                    &self.reads.prune_ledger,
                    &[Value::Text(chain), Value::Uint(floor)],
                )
                .await
                .map_err(StoreError::engine(
                    Operation::Delete,
                    Some("accepted_blocks"),
                ))?;
        }
        Ok(())
    }

    /// The deletes, then each table's load, merge, and clear, then the ledger's prune.
    async fn write_tables(&mut self) -> Result<(), StoreError> {
        for table in &self.tables {
            let Some(delete) = &table.delete else {
                continue;
            };
            for (chain, (height, hashes)) in &self.batch.orphaned {
                let params = [
                    Value::Text(chain.clone()),
                    Value::Uint(*height),
                    Value::List(hashes.iter().cloned().map(Value::Text).collect()),
                ];
                self.engine
                    .execute(delete, &params)
                    .await
                    .map_err(StoreError::engine(Operation::Delete, Some(&table.def.name)))?;
            }
        }
        let mut grouped = self.batch.by_table();
        let mut tips: HashMap<String, u64> = HashMap::new();
        for row in grouped
            .get(&TableId::Dataset(Table::AcceptedBlock))
            .into_iter()
            .flatten()
        {
            if let Some(Value::Uint(height)) = row.value("height") {
                let tip = tips.entry(row.chain().to_owned()).or_default();
                *tip = (*tip).max(*height);
            }
        }
        for table in &mut self.tables {
            let Some(rows) = grouped.remove(&table.def.id) else {
                continue;
            };
            // Counted before the load, since rows a failed transaction loaded are dead too.
            table.staged += rows.len();
            let name = Some(table.def.name.as_str());
            self.engine
                .load(&table.def, &table.staging, &rows)
                .await
                .map_err(StoreError::engine(Operation::Stage, name))?;
            self.engine
                .execute(&table.merge, &[])
                .await
                .map_err(StoreError::engine(Operation::Merge, name))?;
            self.engine
                .execute(&table.clear_staging, &[])
                .await
                .map_err(StoreError::engine(Operation::Stage, name))?;
        }
        self.prune_ledger(tips).await
    }
}

impl<E: Engine + 'static> Store for SqlStore<E> {
    async fn restore(&mut self) -> Result<Restored, StoreError> {
        let chain = self.chain.clone();
        Ok(Restored {
            contracts: self.contracts(&chain).await?,
            ledger: self.ledger(&chain, LEDGER_WINDOW).await?,
        })
    }

    fn reconcile(&mut self, held: &[BlockMeta]) {
        self.batch.reconcile(held);
    }

    /// One transaction, so a stop part-way leaves the store as it was.
    async fn rewind(&mut self, height: u64) -> Result<(), StoreError> {
        self.engine
            .execute("BEGIN", &[])
            .await
            .map_err(StoreError::engine(Operation::Begin, None))?;
        if let Err(error) = self.delete_above(height).await {
            let _ = self.engine.execute("ROLLBACK", &[]).await;
            return Err(error);
        }
        self.engine
            .execute("COMMIT", &[])
            .await
            .map_err(StoreError::engine(Operation::Commit, None))
    }
}

impl<E: Engine> EnvelopeSink for SqlStore<E> {
    async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
        self.batch.push(&envelope)
    }

    /// A write that fails because the server closed the connection is written again once
    /// the engine [restores](Engine::recover) it: the batch stays buffered until a commit
    /// lands, and writing it again after a commit whose acknowledgement was lost upserts
    /// onto itself (see the module docs). The restored connection is a new session, so
    /// the staging tables are created on it first.
    async fn flush(&mut self) -> Result<(), SinkError> {
        let mut retries = 0;
        loop {
            let Err(error) = self.write_batch().await else {
                return Ok(());
            };
            if retries == RETRIES || !self.engine.recover().await? {
                return Err(error.into());
            }
            self.create_staging().await?;
            retries += 1;
        }
    }
}

/// Every table's stored columns in the session's schema, as name and type in column
/// order, by table name: one query for all of them.
async fn describe<E: Engine>(
    engine: &mut E,
) -> Result<HashMap<String, Vec<(String, String)>>, StoreError> {
    let rows = engine
        .query(E::Dialect::DESCRIBE, &[])
        .await
        .map_err(StoreError::engine(Operation::Read, None))?;
    let mut stored: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for row in &rows {
        stored
            .entry(text("table.name", column(row, 0))?.to_owned())
            .or_default()
            .push((
                text("column.name", column(row, 1))?.to_owned(),
                text("column.type", column(row, 2))?.to_owned(),
            ));
    }
    Ok(stored)
}

/// Fails when `table` as stored, its `stored` columns, lacks a column, holds one it does
/// not declare, or types one differently, so a changed definition stops the run at
/// startup rather than at its first write.
fn check<D: Dialect>(table: &TableDef, stored: &[(String, String)]) -> Result<(), StoreError> {
    let drift = |difference: String| StoreError::Drift {
        table: table.name.clone(),
        difference,
    };
    for column in &table.columns {
        let expected = D::reported_type(column.kind);
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
    InvalidStoredValue::new(column, format!("{value:?}"))
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
