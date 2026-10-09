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

use std::sync::Arc;

use crate::sink::batch::Batch;
use crate::sink::sql::{self, Dialect};
use crate::sink::store_error::{EngineError, InvalidStoredValue, Operation, StoreError};
use crate::sink::table::{Row, Schema, Table, TableDef, TableError, Value};
use crate::sink::{EnvelopeSink, SinkError};
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
            E::Dialect::use_schema(database_schema),
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
            for statement in std::iter::once(sql::create_table::<E::Dialect>(def))
                .chain(sql::create_indexes::<E::Dialect>(def))
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
                .map(|(position, def)| Prepared::new::<E::Dialect>(position, def))
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

    /// The deletes, then each table's stage, load, and merge.
    async fn write_tables(&mut self) -> Result<(), StoreError> {
        for table in &self.tables {
            let Some(delete) = &table.delete else {
                continue;
            };
            for (chain, (height, hashes)) in &self.batch.orphaned {
                for hash in hashes {
                    let params = [
                        Value::Text(chain.clone()),
                        Value::Uint(*height),
                        Value::Text(hash.clone()),
                    ];
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
        .query(E::Dialect::DESCRIBE, &[Value::Text(table.name.clone())])
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
        let expected = E::Dialect::reported_type(column.kind);
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
