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
    /// What a rewind runs: every block above a height, in one statement.
    delete_above: Option<String>,
}

impl Prepared {
    fn new<D: Dialect>(position: usize, def: &Arc<TableDef>, ledger: &TableDef) -> Self {
        let staging = format!("staging_{position}");
        Self {
            create_staging: D::create_staging(def, &staging),
            merge: sql::upsert::<D>(def, &staging),
            drop_staging: D::drop_staging(&staging),
            delete: sql::delete_blocks::<D>(def),
            delete_above: sql::delete_above(def, ledger),
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
        // creates only what is missing.
        let create = schema
            .tables()
            .iter()
            .flat_map(|def| {
                std::iter::once(sql::create_table::<E::Dialect>(def))
                    .chain(sql::create_indexes::<E::Dialect>(def))
            })
            .collect::<Vec<_>>()
            .join(";\n");
        engine
            .execute(&create, &[])
            .await
            .map_err(StoreError::engine(Operation::Create, Some(chain)))?;
        engine
            .execute("COMMIT", &[])
            .await
            .map_err(StoreError::engine(Operation::Commit, None))?;
        let stored = describe(&mut engine).await?;
        for def in schema.tables() {
            check::<E::Dialect>(def, stored.get(&def.name).map_or(&[], Vec::as_slice))?;
        }
        Ok(Self {
            engine,
            chain: ChainId::new(chain),
            tables: schema
                .tables()
                .iter()
                .enumerate()
                .map(|(position, def)| {
                    Prepared::new::<E::Dialect>(position, def, schema.dataset(Table::AcceptedBlock))
                })
                .collect(),
            reads: Reads::new(&schema)?,
            batch: Batch::new(schema),
        })
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

    /// The newest `limit` accepted blocks on `chain`, oldest first, after dropping every
    /// older row. An orphaned block's row was deleted by its `reorg`, so these are
    /// canonical.
    ///
    /// Read by [`restore`](Store::restore), before the first write: the result is the
    /// undo window a restart resumes from. Rows below the oldest one returned can never be read again,
    /// so they are deleted here, which is what keeps the ledger from growing without
    /// bound across restarts. Contiguity is not checked; the pipeline does that.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when the query or the delete fails and
    /// [`StoreError::Restore`] when a stored value does not parse.
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

    /// Deletes every block above `height` from each table whose rows belong to one.
    ///
    /// The blocks are the ledger's: a block's rows commit with its ledger entry, so every
    /// block above `height` with rows is named there. Each table takes one statement that
    /// finds them in the ledger by hash, as a reorg deletes, so the delete uses the hash
    /// index rather than scanning by height. The ledger goes last, since the others are
    /// found through it.
    async fn delete_above(&mut self, height: u64) -> Result<(), StoreError> {
        let params = [chain_param(&self.chain), Value::Uint(height)];
        let ledger = TableId::Dataset(Table::AcceptedBlock);
        let (others, last): (Vec<&Prepared>, Vec<&Prepared>) =
            self.tables.iter().partition(|table| table.def.id != ledger);
        for table in others.into_iter().chain(last) {
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

    /// The deletes, then each table's stage, load, and merge.
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

impl<E: Engine + 'static> Store for SqlStore<E> {
    async fn restore(&mut self) -> Result<Restored, StoreError> {
        let chain = self.chain.clone();
        Ok(Restored {
            contracts: self.contracts(&chain).await?,
            ledger: self.ledger(&chain, LEDGER_WINDOW).await?,
        })
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

    async fn flush(&mut self) -> Result<(), SinkError> {
        self.write_batch().await?;
        Ok(())
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
