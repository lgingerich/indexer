//! The `DuckDB` store: the shared [`SqlStore`] over an embedded database file.
//!
//! `DuckDB` is an embedded, single-writer engine, so this is an archive and analytics
//! endpoint — a local replica or an analytical sidecar — not the fan-out for many
//! consumers. A flush stages each table through the appender, then merges it on
//! `(chain, dedupe_key)`.
//!
//! Unsigned 64-bit integers are `UBIGINT`, and wider integers `BIGNUM`, which sums and
//! adds exactly; multiplying a `BIGNUM` turns it into a `DOUBLE`, so cast first when that
//! matters. The appender cannot append a list, so a list arrives as list text, which the
//! engine casts into its column exactly. Secondary indexes are not built: an index slows
//! every append, and the only one declared serves a reorg's rare delete.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use duckdb::types::{TimeUnit, ToSql, ToSqlOutput, Value as Duck, ValueRef};
use duckdb::{Connection, appender_params_from_iter, params_from_iter};
use serde::Deserialize;
use tracing::info;

use crate::secret::Secret;
use crate::sink::route::Tables;
use crate::sink::sql::{Dialect, ident};
use crate::sink::store::{Engine, SqlStore};
use crate::sink::store_error::{EngineError, Operation, StoreError};
use crate::sink::table::{ColumnType, Row, Schema, TableDef, Value};

/// The `DuckDB` database file written when the settings name no path.
const DEFAULT_PATH: &str = "indexer.duckdb";

/// The most records one commit may cover when the settings name no bound.
///
/// Sized above a single block's worth of envelopes, or the fold in
/// [`ChannelReceiver::drain`](crate::sink::channel::ChannelReceiver::drain) could never
/// join a backlog and the bound would be inert.
const DEFAULT_BATCH_RECORDS: usize = 500;

/// `DuckDB`'s settings: the database to write, and the engine settings passed through.
///
/// Beside the sink rather than in [`crate::config`] because these are `DuckDB`'s: the
/// engine settings are opaque keys the engine validates, and a build without the
/// `duckdb` feature has no use for either. What the *file* may say about `DuckDB` is the
/// `[sink.duckdb]` table, which [`Sinks::duckdb`](crate::config::Sinks::duckdb)
/// holds.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DuckDbSettings {
    /// The tables this store writes. Required.
    pub tables: Tables,
    /// The `DuckDB` database file to write, relative to the settings file. Defaults to
    /// `indexer.duckdb`.
    #[serde(default = "default_path")]
    pub path: PathBuf,
    /// The most records one store commit may cover.
    ///
    /// Storage receives one block at a time and commits each as it arrives. When it has
    /// fallen behind, it folds the blocks already waiting into one commit until this many
    /// records are reached, so a stalled store catches up in fewer, larger transactions.
    /// A block is never split, so a commit can run past the bound by up to one block.
    ///
    /// The bound only applies to the blocks already waiting, so a single block carrying
    /// this many envelopes commits on its own no matter what this is set to. Below that,
    /// a larger value folds more of a backlog into one transaction.
    #[serde(default = "default_batch_records")]
    pub batch_records: usize,
    /// Any other `DuckDB` setting, passed straight through.
    ///
    /// `DuckDB` accepts dozens of settings and this file does not restate them. Anything
    /// here reaches [`duckdb::Config::with`], which validates it, so a misspelled key is
    /// an error from the engine naming the setting rather than a silent no-op. The keys
    /// and their meanings are listed in `DuckDB`'s
    /// [configuration overview](https://duckdb.org/docs/stable/configuration/overview).
    ///
    /// ```toml
    /// [sink.duckdb.settings]
    /// threads = "4"
    /// max_memory = "1GB"
    /// s3_secret_access_key = { env = "S3_SECRET" }
    /// ```
    ///
    /// Values are [`Secret`]s, since some are credentials.
    #[serde(default)]
    pub settings: BTreeMap<String, Secret>,
}

fn default_path() -> PathBuf {
    PathBuf::from(DEFAULT_PATH)
}

const fn default_batch_records() -> usize {
    DEFAULT_BATCH_RECORDS
}

/// A store in `DuckDB`.
pub type DuckDbSink = SqlStore<DuckDb>;

/// An open `DuckDB` connection.
#[derive(Debug)]
pub struct DuckDb {
    pub(crate) connection: Connection,
}

impl DuckDbSink {
    /// Opens the database the settings name and creates the tables in the database
    /// schema named for `chain`.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when a setting is rejected, the database cannot be
    /// opened, or a table cannot be created.
    pub async fn open(
        settings: &DuckDbSettings,
        schema: Arc<Schema>,
        chain: &str,
    ) -> Result<Self, StoreError> {
        let path = settings.path.display().to_string();
        // In its own scope: the engine's configuration is not `Send`, so it must be gone
        // before the open awaits anything, for the open to run on the store's runtime.
        let connection = {
            let mut config = duckdb::Config::default();
            for (key, value) in &settings.settings {
                config = config
                    .with(key, value.expose())
                    .map_err(StoreError::engine(Operation::Configure, Some(key)))?;
            }
            Connection::open_with_flags(&settings.path, config)
                .map_err(StoreError::engine(Operation::Open, Some(&path)))?
        };
        info!(store = %path, "storage opened");
        Self::connected(connection, schema, chain).await
    }

    /// Takes an open connection and creates every table in `schema` in the database
    /// schema named for `chain`.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when the schema or a table cannot be created.
    pub async fn connected(
        connection: Connection,
        schema: Arc<Schema>,
        chain: &str,
    ) -> Result<Self, StoreError> {
        SqlStore::new(DuckDb { connection }, schema, chain).await
    }
}

impl Dialect for DuckDb {
    const INDEXES: bool = false;
    const DESCRIBE: &'static str = "SELECT table_name, column_name, data_type \
         FROM information_schema.columns WHERE table_schema = current_schema() \
         ORDER BY table_name, ordinal_position";

    // A list parameter binds as list text, which the cast reads back exactly.
    fn any_of(column: &str, param: &str) -> String {
        format!("list_contains(CAST({param} AS VARCHAR[]), {column})")
    }

    fn type_name(kind: ColumnType) -> String {
        match kind {
            // `UBIGINT`, not `BIGINT`: block numbers and gas amounts are unsigned, and a
            // signed 64-bit column cannot hold the top half of the range.
            ColumnType::Uint => "UBIGINT",
            ColumnType::Int => "BIGINT",
            // Arbitrary precision, so a `uint256` is exact. `HUGEINT` stops at 128 bits.
            ColumnType::BigInt => "BIGNUM",
            ColumnType::Text => "VARCHAR",
            ColumnType::Bool => "BOOLEAN",
            ColumnType::Timestamp => "TIMESTAMP",
            ColumnType::Document => "JSON",
            ColumnType::List(element) => return format!("{}[]", Self::type_name(*element)),
        }
        .to_owned()
    }

    fn use_schema(schema: &str) -> String {
        format!("USE {}", ident(schema))
    }

    // A temporary table is invisible to the appender, which looks up `main`, so staging
    // is a table there, created and dropped inside the flush's transaction.
    fn create_staging(table: &TableDef, staging: &str) -> String {
        format!(
            "CREATE OR REPLACE TABLE {} AS SELECT * FROM {} WHERE false",
            Self::staging(staging),
            ident(&table.name)
        )
    }

    fn staging(staging: &str) -> String {
        format!("main.{}", ident(staging))
    }

    fn drop_staging(staging: &str) -> Option<String> {
        Some(format!("DROP TABLE {}", Self::staging(staging)))
    }
}

impl Engine for DuckDb {
    type Dialect = Self;

    async fn execute(&mut self, sql: &str, values: &[Value]) -> Result<(), EngineError> {
        if values.is_empty() {
            self.connection.execute_batch(sql)?;
        } else {
            self.connection.execute(sql, params_from_iter(values))?;
        }
        Ok(())
    }

    async fn query(&mut self, sql: &str, values: &[Value]) -> Result<Vec<Vec<Value>>, EngineError> {
        let mut statement = self.connection.prepare(sql)?;
        let mut rows = statement.query(params_from_iter(values))?;
        let mut read = Vec::new();
        while let Some(row) = rows.next()? {
            let width = row.as_ref().column_count();
            read.push(
                (0..width)
                    .map(|index| value(row.get_ref(index)?))
                    .collect::<Result<_, EngineError>>()?,
            );
        }
        Ok(read)
    }

    async fn load(
        &mut self,
        _: &TableDef,
        staging: &str,
        rows: &[&Row],
    ) -> Result<(), EngineError> {
        let mut appender = self.connection.appender(staging)?;
        for row in rows {
            // Iterator parameters also support tables wider than 32 columns.
            appender.append_row(appender_params_from_iter(row.values()))?;
        }
        // Flush errors must be observed; Drop cannot report them.
        appender.flush()?;
        Ok(())
    }
}

/// A value read back at startup.
fn value(read: ValueRef<'_>) -> Result<Value, EngineError> {
    Ok(match read {
        ValueRef::Null => Value::Null,
        ValueRef::UBigInt(number) => Value::Uint(number),
        ValueRef::BigInt(number) => Value::Int(number),
        ValueRef::Boolean(flag) => Value::Bool(flag),
        ValueRef::Text(text) => Value::Text(String::from_utf8(text.to_vec())?),
        ValueRef::Timestamp(unit, value) => {
            let per_second = match unit {
                TimeUnit::Second => 1,
                TimeUnit::Millisecond => 1_000,
                TimeUnit::Microsecond => 1_000_000,
                TimeUnit::Nanosecond => 1_000_000_000,
            };
            Value::Timestamp(u64::try_from(value.div_euclid(per_second))?)
        }
        other => return Err(format!("cannot read {:?} as a value", other.data_type()).into()),
    })
}

// The engine conversion belongs at this boundary; text and JSON borrow the buffered
// row rather than cloning its strings for every append.
impl ToSql for Value {
    fn to_sql(&self) -> duckdb::Result<ToSqlOutput<'_>> {
        Ok(match self {
            Self::Null => ToSqlOutput::Borrowed(ValueRef::Null),
            Self::Uint(number) => ToSqlOutput::Borrowed(ValueRef::UBigInt(*number)),
            Self::Int(number) => ToSqlOutput::Borrowed(ValueRef::BigInt(*number)),
            // Decimal text, which the engine casts into the `BIGNUM` column exactly.
            Self::BigInt {
                negative,
                magnitude,
            } => ToSqlOutput::Owned(Duck::Text(format!(
                "{}{magnitude}",
                if *negative { "-" } else { "" }
            ))),
            Self::Text(text) | Self::Document(text) => {
                ToSqlOutput::Borrowed(ValueRef::Text(text.as_bytes()))
            }
            Self::Bool(flag) => ToSqlOutput::Borrowed(ValueRef::Boolean(*flag)),
            Self::Timestamp(seconds) => ToSqlOutput::Owned(Duck::Timestamp(
                TimeUnit::Second,
                i64::try_from(*seconds)
                    .map_err(|error| duckdb::Error::ToSqlConversionFailure(error.into()))?,
            )),
            Self::List(values) => {
                let mut text = String::new();
                list_literal(values, &mut text);
                ToSqlOutput::Owned(Duck::Text(text))
            }
        })
    }
}

/// A list as the text `DuckDB` casts to a list column: `[1, -2, "0xab", NULL]`.
///
/// Every text element is double-quoted with `\` and `"` escaped, so a comma, a bracket,
/// or the word `NULL` inside one stays part of the element.
fn list_literal(values: &[Value], out: &mut String) {
    out.push('[');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        match value {
            Value::Null => out.push_str("NULL"),
            Value::Uint(number) | Value::Timestamp(number) => out.push_str(&number.to_string()),
            Value::Int(number) => out.push_str(&number.to_string()),
            Value::BigInt {
                negative,
                magnitude,
            } => {
                if *negative {
                    out.push('-');
                }
                out.push_str(&magnitude.to_string());
            }
            Value::Bool(flag) => out.push_str(if *flag { "true" } else { "false" }),
            Value::Text(text) | Value::Document(text) => {
                out.push('"');
                for c in text.chars() {
                    if matches!(c, '"' | '\\') {
                        out.push('\\');
                    }
                    out.push(c);
                }
                out.push('"');
            }
            Value::List(inner) => list_literal(inner, out),
        }
    }
    out.push(']');
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::collections::{BTreeSet, HashSet};
    use std::sync::Arc;

    use alloy_primitives::{Address, B256, TxHash, U256};
    use duckdb::types::FromSql;
    use duckdb::{Connection, appender_params_from_iter};

    use crate::sink::table::{Schema, Table, TableId, Value};
    use crate::sink::{EnvelopeSink as _, Operation, SinkError, Store as _, StoreError};
    use crate::wire::envelope::{
        Block, BlockMeta, ChainId, Contract, Envelope, Event, Log, Reorg, Transaction,
    };

    use super::DuckDbSink;

    /// The dataset tables alone.
    fn datasets() -> Arc<Schema> {
        Arc::new(Schema::new().expect("the dataset tables"))
    }

    /// The `reorgs` table's DDL, to recreate it after a test drops it.
    fn reorg_ddl() -> String {
        crate::sink::sql::create_table::<super::DuckDb>(datasets().dataset(Table::Reorg))
    }

    /// An engine setting can be a credential, so its value is never printed.
    #[test]
    fn engine_setting_values_are_redacted() {
        let settings: super::DuckDbSettings =
            toml::from_str("tables = ['logs']\n[settings]\ns3_secret_access_key = 'hunter2'")
                .expect("settings");
        assert_eq!(
            settings.settings["s3_secret_access_key"].expose(),
            "hunter2"
        );
        let printed = format!("{settings:?}");
        assert!(
            printed.contains("s3_secret_access_key") && !printed.contains("hunter2"),
            "{printed}"
        );
    }

    fn memory() -> Connection {
        Connection::open_in_memory().expect("open in-memory DuckDB")
    }

    /// A store of the `base` chain over `connection`, with the tables of `schema`.
    async fn connect(connection: Connection, schema: Arc<Schema>) -> DuckDbSink {
        DuckDbSink::connected(connection, schema, "base")
            .await
            .expect("create the tables")
    }

    /// An in-memory store of the dataset tables.
    async fn sink() -> DuckDbSink {
        connect(memory(), datasets()).await
    }

    fn hash(byte: u8) -> B256 {
        B256::from([byte; 32])
    }

    fn chain() -> ChainId {
        ChainId::new("base")
    }

    /// One envelope of every kind, so a flush exercises every table at once.
    ///
    /// Values are distinct per field, so a column-order or column-name skew shows up as a
    /// wrong value rather than as a plausible-looking row.
    fn every_kind() -> Vec<Envelope> {
        vec![
            Envelope::new(
                chain(),
                Event::Block(Box::new(Block {
                    number: 100,
                    hash: hash(0x01),
                    parent_hash: hash(0x02),
                    timestamp: 1_700_000_000,
                    gas_limit: 30_000_000,
                    gas_used: 21_000,
                    transaction_count: 1,
                    ..Block::default()
                })),
            ),
            Envelope::new(
                chain(),
                Event::Transaction(Box::new(Transaction {
                    hash: TxHash::from([0x11; 32]),
                    transaction_index: 3,
                    from: Address::from([0x22; 20]),
                    value: U256::from(1_000),
                    // A wei price above `u64::MAX`, which is ~18.4 ETH — so this also
                    // checks the 128-bit columns are not truncated to 64.
                    gas_price: Some(u128::from(u64::MAX) + 1),
                    receipt_status: true,
                    receipt_gas_used: 21_000,
                    log_count: 1,
                    block_number: 100,
                    block_hash: hash(0x01),
                    ..Transaction::default()
                })),
            ),
            Envelope::new(
                chain(),
                Event::Log(Box::new(Log {
                    log_index: 7,
                    transaction_hash: TxHash::from([0x11; 32]),
                    transaction_index: 3,
                    address: Address::from([0x33; 20]),
                    topic0: Some(hash(0x44)),
                    block_number: 100,
                    block_hash: hash(0x01),
                    block_timestamp: 1_700_000_000,
                    ..Log::default()
                })),
            ),
            Envelope::new(
                chain(),
                Event::Contract(Box::new(Contract {
                    protocol: "uniswap_v3".to_owned(),
                    name: "UniswapV3Pool".to_owned(),
                    address: Address::from([0x77; 20]),
                    factory_address: Address::from([0x33; 20]),
                    transaction_hash: TxHash::from([0x11; 32]),
                    transaction_index: 3,
                    log_index: 7,
                    block_number: 100,
                    block_hash: hash(0x01),
                    block_timestamp: 1_700_000_000,
                })),
            ),
            Envelope::new(
                chain(),
                Event::Reorg(Reorg {
                    height: 100,
                    new_head_hash: hash(0x55),
                    orphaned_hashes: vec![hash(0x66)],
                }),
            ),
            Envelope::new(
                chain(),
                Event::AcceptedBlock(BlockMeta {
                    height: 100,
                    hash: hash(0x01),
                    parent_hash: hash(0x02),
                    timestamp: 1_700_000_000,
                }),
            ),
        ]
    }

    /// Hands `envelopes` to the store without committing them.
    async fn buffer(sink: &mut DuckDbSink, envelopes: impl IntoIterator<Item = Envelope>) {
        for envelope in envelopes {
            sink.publish(envelope).await.expect("row buffers");
        }
    }

    /// Hands `envelopes` to the store and commits them.
    async fn commit(sink: &mut DuckDbSink, envelopes: impl IntoIterator<Item = Envelope>) {
        buffer(sink, envelopes).await;
        sink.flush().await.expect("batch flushes");
    }

    /// Commits one envelope of every kind.
    async fn store(sink: &mut DuckDbSink) {
        commit(sink, every_kind()).await;
    }

    /// Asserts `flushed` failed in the engine while the store was doing `operation` to
    /// `table`.
    fn assert_failed(flushed: Result<(), SinkError>, operation: Operation, table: &str) {
        match flushed {
            Err(SinkError::Store(StoreError::Engine {
                operation: found,
                target,
                ..
            })) => assert_eq!((found, target.as_deref()), (operation, Some(table))),
            other => panic!("a {operation:?} failure on {table}, not {other:?}"),
        }
    }

    /// How many rows `every_kind` writes to `table`: one to each, as no fixture event
    /// is decoded.
    fn one_unless_decoded(table: Table) -> i64 {
        i64::from(table != Table::Decoded)
    }

    /// A wei price above `u64::MAX` survives the round trip as an exact number, which
    /// is the whole reason those columns are not `UBIGINT`.
    #[tokio::test]
    async fn a_wei_price_above_u64_max_is_not_truncated() {
        let mut sink = sink().await;
        store(&mut sink).await;

        let (price, doubled): (String, String) = sink
            .engine
            .connection
            .query_row(
                "SELECT gas_price::VARCHAR, (gas_price + gas_price)::VARCHAR FROM transactions",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("the price reads back");
        let expected = u128::from(u64::MAX) + 1;
        assert_eq!(price, expected.to_string());
        assert_eq!(doubled, (expected * 2).to_string(), "it adds exactly");
    }

    /// A block's wide row is written whole, in the order the generated DDL declares. The
    /// header is the schema, so this checks the two agree end to end.
    #[tokio::test]
    async fn a_wide_block_row_writes_every_column() {
        let mut sink = sink().await;
        store(&mut sink).await;

        let declared: Vec<String> = column(
            &sink.engine.connection,
            "SELECT column_name FROM duckdb_columns() WHERE table_name = 'blocks' \
             ORDER BY column_index",
        );
        let header: Vec<String> = datasets()
            .dataset(Table::Block)
            .columns
            .iter()
            .map(|column| column.name.to_string())
            .collect();
        assert_eq!(declared, header, "the DDL and the header must agree");

        let (number, block_hash, parent, timestamp, gas_limit, gas_used, tx_count): (
            u64,
            String,
            String,
            u64,
            u64,
            u64,
            u64,
        ) = sink
            .engine
            .connection
            .query_row(
                "SELECT number, hash, parent_hash, epoch(timestamp)::UBIGINT, gas_limit, \
                 gas_used, transaction_count FROM blocks",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .expect("block row reads back");

        assert_eq!(number, 100);
        assert_eq!(block_hash, format!("{:#x}", hash(0x01)));
        assert_eq!(parent, format!("{:#x}", hash(0x02)));
        assert_eq!(timestamp, 1_700_000_000);
        assert_eq!(gas_limit, 30_000_000);
        assert_eq!(gas_used, 21_000);
        assert_eq!(tx_count, 1);
    }

    /// An absent optional field is SQL `NULL`, not an empty string and not a zero.
    /// `withdrawals_root` is `None` on a pre-merge block, so a consumer filtering
    /// `WHERE withdrawals_root IS NOT NULL` has to get exactly those blocks. The fixture
    /// block leaves every optional field absent, so every nullable column is checked.
    #[tokio::test]
    async fn an_absent_optional_column_is_null() {
        let mut sink = sink().await;
        store(&mut sink).await;

        let schema = datasets();
        let nullable: Vec<&str> = schema
            .dataset(Table::Block)
            .columns
            .iter()
            .filter(|column| column.nullable)
            .map(|column| column.name.as_ref())
            .collect();
        assert!(nullable.contains(&"withdrawals_root"), "{nullable:?}");
        for name in nullable {
            let present: i64 = sink
                .engine
                .connection
                .query_row(
                    &format!("SELECT count(*) FROM blocks WHERE \"{name}\" IS NOT NULL"),
                    [],
                    |row| row.get(0),
                )
                .expect("null filter reads back");
            assert_eq!(present, 0, "the fixture block has no {name}");
        }
    }

    /// Nothing is durable before the flush, in any table, and everything buffered is
    /// written and gone from the buffer after it.
    #[tokio::test]
    async fn nothing_is_durable_until_the_flush() {
        let mut sink = sink().await;
        buffer(&mut sink, every_kind()).await;
        assert_eq!(
            sink.buffered(),
            every_kind().len(),
            "buffered but not written"
        );
        for table in Table::ALL {
            assert_eq!(
                row_count(&sink, table.name()),
                0,
                "{table} before the flush"
            );
        }

        sink.flush().await.expect("batch flushes");
        assert_eq!(sink.buffered(), 0, "the buffers are cleared");
        for table in Table::ALL {
            assert_eq!(
                row_count(&sink, table.name()),
                one_unless_decoded(table),
                "{table} after the flush"
            );
        }
    }

    /// A table failing late in a flush rolls back the tables written before it and
    /// keeps the batch, and the retry writes every row once.
    #[tokio::test]
    async fn a_late_table_failure_rolls_back_and_a_retry_writes_each_row_once() {
        let mut sink = sink().await;
        buffer(&mut sink, every_kind()).await;
        sink.engine
            .connection
            .execute_batch("DROP TABLE reorgs")
            .expect("remove a late table in the batch");

        assert_failed(sink.flush().await, Operation::Stage, "reorgs");
        assert_eq!(
            sink.buffered(),
            every_kind().len(),
            "failed batch stays buffered"
        );
        for (table, rows) in block_tables(&sink) {
            assert_eq!(rows, 0, "{table} rolls back");
        }

        sink.engine
            .connection
            .execute_batch(&reorg_ddl())
            .expect("restore the missing table");
        sink.flush()
            .await
            .expect("retry commits the original batch");
        sink.flush().await.expect("empty flush is a no-op");
        assert_eq!(sink.buffered(), 0);
        for table in Table::ALL {
            assert_eq!(
                row_count(&sink, table.name()),
                one_unless_decoded(table),
                "{table} after the retry"
            );
        }
    }

    /// A constraint the engine checks only at the merge fails the flush there, keeps the
    /// batch, and leaves the table empty; the retry writes the whole batch.
    #[tokio::test]
    async fn a_deferred_constraint_failure_keeps_the_batch_and_rolls_back() {
        let mut sink = sink().await;
        let constrained = reorg_ddl().replace(
            "\"height\" UBIGINT",
            "\"height\" UBIGINT CHECK (height < 1)",
        );
        sink.engine
            .connection
            .execute_batch(&format!("DROP TABLE reorgs; {constrained}"))
            .expect("constrain the reorg table");
        buffer(
            &mut sink,
            (0..2).map(|height| {
                Envelope::new(
                    chain(),
                    Event::Reorg(Reorg {
                        height,
                        new_head_hash: hash(u8::try_from(height).expect("heights 0 and 1")),
                        orphaned_hashes: vec![],
                    }),
                )
            }),
        )
        .await;

        assert_failed(sink.flush().await, Operation::Merge, "reorgs");
        assert_eq!(row_count(&sink, "reorgs"), 0);
        assert_eq!(sink.buffered(), 2);

        sink.engine
            .connection
            .execute_batch(&format!("DROP TABLE reorgs; {}", reorg_ddl()))
            .expect("remove the constraint");
        sink.flush().await.expect("retry the whole batch");
        assert_eq!(
            column::<u64>(
                &sink.engine.connection,
                "SELECT height FROM reorgs ORDER BY height"
            ),
            [0, 1]
        );
        assert_eq!(sink.buffered(), 0);
    }

    /// A batch that holds a key twice keeps its last copy, a later flush of the same key
    /// updates the stored row rather than appending a second, and a reorg replaces the
    /// orphaned branch's row with the replacement's.
    #[tokio::test]
    async fn a_replay_updates_the_row_and_a_reorg_replaces_the_branch() {
        let mut sink = sink().await;
        let orphaned = Log {
            log_index: 0,
            transaction_hash: TxHash::from([0x11; 32]),
            block_number: 100,
            block_hash: hash(0xaa),
            block_timestamp: 1,
            ..Log::default()
        };
        let log = |log: &Log, block_timestamp: u64| {
            let mut log = log.clone();
            log.block_timestamp = block_timestamp;
            Envelope::new(chain(), Event::Log(Box::new(log)))
        };
        commit(&mut sink, [log(&orphaned, 1), log(&orphaned, 5)]).await;
        assert_eq!(
            row_count(&sink, "logs"),
            1,
            "a repeated key in one batch is one row"
        );
        assert_eq!(log_timestamp(&sink), 5, "the later copy in the batch wins");

        // The same key in a later flush updates the stored row, not appends a second —
        // the merge's `DO UPDATE` branch, which a first flush into an empty table misses.
        commit(&mut sink, [log(&orphaned, 7)]).await;
        assert_eq!(
            row_count(&sink, "logs"),
            1,
            "a replay never appends a second"
        );
        assert_eq!(
            log_timestamp(&sink),
            7,
            "the replay's value replaces the stored one"
        );

        // The reorg deletes the stored branch; the replacement is its own key.
        let mut replacement = orphaned;
        replacement.block_hash = hash(0xbb);
        commit(&mut sink, [reorg(&[0xaa]), log(&replacement, 9)]).await;
        assert_eq!(
            column::<String>(&sink.engine.connection, "SELECT block_hash FROM logs"),
            [format!("{:#x}", hash(0xbb))],
            "only the replacement remains"
        );
        assert_eq!(log_timestamp(&sink), 9);
    }

    /// A reorg naming `orphaned` blocks, by their fixture hash bytes.
    fn reorg(orphaned: &[u8]) -> Envelope {
        Envelope::new(
            chain(),
            Event::Reorg(Reorg {
                height: 100,
                // Not the fixture marker's head, so this marker is its own row.
                new_head_hash: hash(0x56),
                orphaned_hashes: orphaned.iter().map(|byte| hash(*byte)).collect(),
            }),
        )
    }

    /// Every table but `reorgs`, with how many rows it holds.
    fn block_tables(sink: &DuckDbSink) -> Vec<(Table, i64)> {
        Table::ALL
            .into_iter()
            .filter(|table| *table != Table::Reorg)
            .map(|table| (table, row_count(sink, table.name())))
            .collect()
    }

    /// A reorg deletes a committed block's rows from every table that belongs to a
    /// block, the ledger included, and keeps itself as the record of the retraction.
    #[tokio::test]
    async fn a_reorg_deletes_a_committed_block_from_every_table() {
        let mut sink = sink().await;
        store(&mut sink).await;
        let other_chain = every_kind().into_iter().map(|mut envelope| {
            envelope.chain = ChainId::new("ethereum");
            envelope
        });
        commit(&mut sink, other_chain).await;

        commit(&mut sink, [reorg(&[0x01])]).await;

        for (table, rows) in block_tables(&sink) {
            let chains: Vec<String> = column(
                &sink.engine.connection,
                &format!("SELECT chain FROM \"{table}\""),
            );
            let expected = if table == Table::Decoded {
                Vec::new()
            } else {
                vec!["ethereum".to_owned()]
            };
            assert_eq!(
                chains, expected,
                "{table} keeps nothing of the orphaned block, and the other chain's row"
            );
            assert_eq!(rows, one_unless_decoded(table), "{table}");
        }
        assert_eq!(
            column::<String>(
                &sink.engine.connection,
                "SELECT new_head_hash FROM reorgs WHERE chain = 'base' ORDER BY new_head_hash"
            ),
            [format!("{:#x}", hash(0x55)), format!("{:#x}", hash(0x56))],
            "the fixture marker and this one"
        );
        assert_eq!(
            row_count(&sink, "reorgs"),
            3,
            "and the other chain's marker"
        );
    }

    /// An orphaned block still in the buffer is dropped before it is ever written.
    #[tokio::test]
    async fn a_reorg_drops_an_orphaned_block_from_the_same_batch() {
        let mut sink = sink().await;
        buffer(&mut sink, every_kind()).await;
        sink.publish(reorg(&[0x01])).await.expect("reorg buffers");
        assert_eq!(
            sink.buffered(),
            2,
            "only the two reorg markers stay buffered"
        );
        sink.flush().await.expect("batch commits");

        for (table, rows) in block_tables(&sink) {
            assert_eq!(rows, 0, "{table} never sees the orphaned block");
        }
        assert_eq!(row_count(&sink, "reorgs"), 2);
    }

    /// A block orphaned and then canonical again is stored, whether its return arrives in
    /// a later batch or the same one: the deletes run before the rows are written.
    #[tokio::test]
    async fn a_block_that_returns_after_a_reorg_is_stored() {
        let log = |block: u8| {
            Envelope::new(
                chain(),
                Event::Log(Box::new(Log {
                    log_index: 0,
                    transaction_hash: TxHash::from([0x11; 32]),
                    block_number: 100,
                    block_hash: hash(block),
                    ..Log::default()
                })),
            )
        };
        for (case, batches) in [
            (
                "across batches",
                vec![
                    vec![log(0xaa)],
                    vec![reorg(&[0xaa]), log(0xbb)],
                    vec![reorg(&[0xbb]), log(0xaa)],
                ],
            ),
            (
                "within one batch, with the block already committed",
                vec![
                    vec![log(0xaa)],
                    vec![reorg(&[0xaa]), log(0xbb), reorg(&[0xbb]), log(0xaa)],
                ],
            ),
        ] {
            let mut sink = sink().await;
            for batch in batches {
                commit(&mut sink, batch).await;
            }
            assert_eq!(
                column::<String>(
                    &sink.engine.connection,
                    "SELECT block_hash FROM logs ORDER BY block_hash"
                ),
                [format!("{:#x}", hash(0xaa))],
                "{case}"
            );
        }
    }

    /// A failed commit keeps the reorg's deletions with its rows, and the retry applies
    /// them.
    #[tokio::test]
    async fn a_failed_reorg_commit_keeps_its_deletes_for_the_retry() {
        let mut sink = sink().await;
        store(&mut sink).await;
        sink.publish(reorg(&[0x01])).await.expect("reorg buffers");
        sink.engine
            .connection
            .execute_batch("DROP TABLE reorgs")
            .expect("remove a table the batch writes");
        assert_failed(sink.flush().await, Operation::Stage, "reorgs");
        for (table, rows) in block_tables(&sink) {
            assert_eq!(
                rows,
                one_unless_decoded(table),
                "{table}'s delete rolled back"
            );
        }

        sink.engine
            .connection
            .execute_batch(&reorg_ddl())
            .expect("restore the missing table");
        sink.flush().await.expect("the retry commits");
        for (table, rows) in block_tables(&sink) {
            assert_eq!(rows, 0, "{table} is retracted on the retry");
        }
        assert_eq!(
            column::<String>(&sink.engine.connection, "SELECT new_head_hash FROM reorgs"),
            [format!("{:#x}", hash(0x56))],
            "the retry records the retraction"
        );
    }

    fn accepted(height: u64) -> BlockMeta {
        BlockMeta {
            height,
            hash: hash(u8::try_from(height).expect("small heights")),
            parent_hash: hash(u8::try_from(height - 1).expect("small heights")),
            timestamp: 1_700_000_000 + height,
        }
    }

    /// A rewind deletes every block above the height from every table, the ledger
    /// included, and leaves the blocks at or below it and other chains' rows alone.
    #[tokio::test]
    async fn a_rewind_deletes_every_block_above_the_height() {
        let mut sink = sink().await;
        for name in ["base", "ethereum"] {
            for height in 1..=5 {
                let meta = accepted(height);
                for event in [
                    Event::Block(Box::new(Block {
                        number: height,
                        hash: meta.hash,
                        ..Block::default()
                    })),
                    Event::AcceptedBlock(meta),
                ] {
                    sink.publish(Envelope::new(ChainId::new(name), event))
                        .await
                        .expect("buffer");
                }
            }
        }
        sink.flush().await.expect("flush");

        sink.rewind(3).await.expect("rewind");

        for (table, height) in [("blocks", "number"), ("accepted_blocks", "height")] {
            let kept = |chain: &str| -> Vec<u64> {
                column(
                    &sink.engine.connection,
                    &format!(
                        "SELECT {height} FROM {table} WHERE chain = '{chain}' ORDER BY {height}"
                    ),
                )
            };
            assert_eq!(
                kept("base"),
                [1, 2, 3],
                "{table} keeps base through the height"
            );
            assert_eq!(
                kept("ethereum"),
                [1, 2, 3, 4, 5],
                "{table} keeps the other chain"
            );
        }
    }

    /// The ledger reads back the newest unorphaned accepted blocks of one chain, oldest
    /// first, and drops that chain's rows below them.
    #[tokio::test]
    async fn the_ledger_reads_back_the_newest_canonical_window_and_prunes_below_it() {
        let mut sink = sink().await;
        assert!(sink.ledger(&chain(), 3).await.expect("empty").is_empty());
        let mut envelopes: Vec<Envelope> = (1..=10)
            .map(|height| Envelope::new(chain(), Event::AcceptedBlock(accepted(height))))
            .collect();
        envelopes.push(Envelope::new(
            ChainId::new("ethereum"),
            Event::AcceptedBlock(accepted(1)),
        ));
        // Block 10 was orphaned and deleted, so the tip falls back to 9.
        envelopes.push(Envelope::new(
            chain(),
            Event::Reorg(Reorg {
                height: 10,
                new_head_hash: hash(0xee),
                orphaned_hashes: vec![hash(10)],
            }),
        ));
        commit(&mut sink, envelopes).await;

        let ledger = sink.ledger(&chain(), 3).await.expect("read back");
        assert_eq!(ledger, [accepted(7), accepted(8), accepted(9)]);
        assert_eq!(
            column::<u64>(
                &sink.engine.connection,
                "SELECT height FROM accepted_blocks WHERE chain = 'base' ORDER BY height"
            ),
            [7, 8, 9],
            "the rows below the window and the orphaned row above were deleted"
        );
        assert_eq!(
            sink.ledger(&ChainId::new("ethereum"), 3)
                .await
                .expect("read back"),
            [accepted(1)],
            "another chain's ledger is untouched"
        );
    }

    /// A decoded record lands in `decoded_logs` and in its event's own table, where every
    /// argument is a typed column: wide integers exact and signed, so they sum without a
    /// cast. A reorg deletes the typed row with the rest of its block.
    #[tokio::test]
    async fn a_decoded_record_lands_in_its_typed_event_table() {
        let mut sink = connect(memory(), crate::sink::fixtures::schema()).await;
        let swap = crate::sink::fixtures::decoded_swap();
        let block = swap.block_hash;
        commit(
            &mut sink,
            [Envelope::new(chain(), Event::Decoded(Box::new(swap)))],
        )
        .await;

        assert_eq!(row_count(&sink, "decoded_logs"), 1);
        assert_eq!(row_count(&sink, "uniswap_v3_pool_swap"), 1);
        let (amount0, amount1, sqrt_price, liquidity, tick, sender): (
            String,
            String,
            String,
            String,
            i64,
            String,
        ) = sink
            .engine
            .connection
            .query_row(
                "SELECT amount0::VARCHAR, amount1::VARCHAR, sqrt_price_x96::VARCHAR, \
                 liquidity::VARCHAR, tick, sender FROM uniswap_v3_pool_swap",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .expect("the swap reads back");
        assert_eq!(amount0, "-3180585820646654");
        assert_eq!(amount1, "8586564");
        assert_eq!(sqrt_price, "4115542941155561242646778");
        assert_eq!(liquidity, "1367693693161775005");
        assert_eq!(tick, -197_317);
        assert_eq!(sender, "0x6ff5693b99212da76ad316178a184ab56d299b43");
        let total: String = sink
            .engine
            .connection
            .query_row(
                "SELECT (sum(sqrt_price_x96) + sum(liquidity))::VARCHAR FROM uniswap_v3_pool_swap",
                [],
                |row| row.get(0),
            )
            .expect("arithmetic on the column");
        // `BIGNUM` sums and adds exactly; multiplying it by an integer becomes a `DOUBLE`.
        assert_eq!(total, "4115544308849254404421783", "exact, not a float");

        commit(
            &mut sink,
            [Envelope::new(
                chain(),
                Event::Reorg(Reorg {
                    height: 1,
                    new_head_hash: hash(0x56),
                    orphaned_hashes: vec![block],
                }),
            )],
        )
        .await;
        assert_eq!(row_count(&sink, "uniswap_v3_pool_swap"), 0);
        assert_eq!(row_count(&sink, "decoded_logs"), 0);
    }

    /// A store routed only the event tables creates and fills those and the bookkeeping
    /// tables: a decoded record's `decoded_logs` row, and a raw log, are not kept.
    #[tokio::test]
    async fn a_store_writes_only_the_tables_routed_to_it() {
        let schema = crate::sink::fixtures::schema();
        let (events, mut expected): (HashSet<TableId>, BTreeSet<String>) = schema
            .tables()
            .iter()
            .filter(|def| matches!(def.id, TableId::Event(_)))
            .map(|def| (def.id, def.name.clone()))
            .unzip();
        expected.extend(["accepted_blocks", "contracts", "reorgs"].map(str::to_owned));
        let mut sink = connect(memory(), Arc::new(schema.select(&events))).await;
        let swap = crate::sink::fixtures::decoded_swap();
        let log = Log {
            block_number: swap.block_number,
            block_hash: swap.block_hash,
            ..Log::default()
        };
        commit(
            &mut sink,
            [
                Envelope::new(chain(), Event::Decoded(Box::new(swap))),
                Envelope::new(chain(), Event::Log(Box::new(log))),
            ],
        )
        .await;

        let created: BTreeSet<String> = column::<String>(
            &sink.engine.connection,
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'base'",
        )
        .into_iter()
        .collect();
        assert_eq!(created, expected, "only the routed and bookkeeping tables");
        assert_eq!(row_count(&sink, "uniswap_v3_pool_swap"), 1);
        for table in ["accepted_blocks", "reorgs", "contracts"] {
            assert_eq!(row_count(&sink, table), 0, "{table} is empty");
        }
    }

    /// Two chains sharing a database write to their own schemas, so each keeps its own
    /// tables.
    #[tokio::test]
    async fn each_chain_writes_to_its_own_schema() {
        let connection = memory();
        for name in ["base", "ethereum"] {
            let handle = connection.try_clone().expect("a second handle");
            let mut sink = DuckDbSink::connected(handle, datasets(), name)
                .await
                .expect("open");
            let mut envelope = every_kind().remove(0);
            envelope.chain = ChainId::new(name);
            commit(&mut sink, [envelope]).await;
        }
        for name in ["base", "ethereum"] {
            let chains: Vec<String> =
                column(&connection, &format!("SELECT chain FROM {name}.blocks"));
            assert_eq!(chains, [name], "{name}.blocks holds only its chain");
        }
    }

    /// A stored table that no longer matches its definition stops the store at startup,
    /// naming the column, rather than failing its first write.
    #[tokio::test]
    async fn a_drifted_table_is_a_startup_error() {
        for (ddl, difference) in [
            (
                reorg_ddl().replace("\"height\" UBIGINT", "\"height\" VARCHAR"),
                "height is VARCHAR, not UBIGINT",
            ),
            (
                reorg_ddl().replace("\"height\" UBIGINT NOT NULL, ", ""),
                "it has no height column",
            ),
            (
                reorg_ddl().replace("(\"height\"", "(\"extra\" INTEGER, \"height\""),
                "it has a extra column it should not",
            ),
        ] {
            let connection = memory();
            connection
                .execute_batch(&format!("CREATE SCHEMA base; USE base; {ddl}"))
                .expect("an old reorgs table");
            let error = DuckDbSink::connected(connection, datasets(), "base")
                .await
                .expect_err("the table drifted");
            assert!(
                matches!(&error, StoreError::Drift { table, difference: found }
                    if table == "reorgs" && found == difference),
                "expected {difference:?}, got {error}"
            );
        }
    }

    /// Connecting twice to the same file must not fail on the existing tables, nor
    /// empty them.
    #[tokio::test]
    async fn new_is_idempotent() {
        let path = std::env::temp_dir().join(format!("indexer-sink-{}.duckdb", std::process::id()));
        let open = || Connection::open(&path).expect("open temp database");
        let mut sink = connect(open(), crate::sink::fixtures::schema()).await;
        store(&mut sink).await;
        drop(sink);

        let sink = connect(open(), crate::sink::fixtures::schema()).await;
        for table in Table::ALL {
            assert_eq!(
                row_count(&sink, table.name()),
                one_unless_decoded(table),
                "{table} keeps its rows"
            );
        }
        // Leave nothing behind; the connection is released when the sink drops.
        drop(sink);
        std::fs::remove_file(&path).expect("remove temp database");
    }

    /// A decoded array of scalars lands as a native list of its element type, and a tuple
    /// as one JSON object keyed by its ABI names with every integer a decimal string, as
    /// Allium's decoded `params` are.
    #[tokio::test]
    async fn an_array_is_a_typed_list_and_a_tuple_one_object() {
        let mut sink = connect(memory(), crate::sink::fixtures::schema()).await;
        let created = crate::sink::fixtures::decoded_pool_created();
        commit(
            &mut sink,
            [Envelope::new(chain(), Event::Decoded(Box::new(created)))],
        )
        .await;

        let (extensions, words, word, orders, after_swap, timelock): (
            String,
            String,
            String,
            String,
            String,
            String,
        ) = sink
            .engine
            .connection
            .query_row(
                "SELECT extensions::VARCHAR, typeof(negative_bin_data_array), \
                 negative_bin_data_array[1]::VARCHAR, extension_orders::VARCHAR, \
                 (extension_orders->>'afterSwap')::BIGNUM::VARCHAR, \
                 price_provider_timelock::VARCHAR FROM metric_v1_factory_pool_created",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .expect("the pool reads back");
        assert_eq!(
            extensions,
            "[0xb1a246b1131ff328067c4aaf4f772ff351475244, \
             0xe4038fa09f0bb9963068afaf97be0c045155090d, \
             0xebc53e61078976118e384f110c262a263decb84b]"
        );
        assert_eq!(words, "BIGNUM[]");
        assert_eq!(
            word, "2510840694154681225832395181564014445733957084757624461722000",
            "a packed word is its exact integer"
        );
        assert_eq!(
            orders,
            r#"{"beforeAddLiquidity":"0","afterAddLiquidity":"0","beforeRemoveLiquidity":"0","afterRemoveLiquidity":"0","beforeSwap":"3","afterSwap":"10"}"#
        );
        assert_eq!(after_swap, "10");
        assert_eq!(timelock, U256::MAX.to_string());
    }

    /// A list's text casts into its column exactly: signed 256-bit elements keep every
    /// digit, and a text element keeps a quote, a comma, a bracket, a backslash, or the
    /// word `NULL` as its own characters.
    #[test]
    fn a_list_casts_into_its_column_exactly() {
        let connection = memory();
        connection
            .execute_batch("CREATE TABLE lists (numbers BIGNUM[], texts VARCHAR[], empty BIGNUM[])")
            .expect("create table");
        let texts = [r#"a,"b]"#, r"back\slash", "NULL", ""];
        let values = [
            Value::List(vec![
                Value::BigInt {
                    negative: true,
                    magnitude: U256::MAX,
                },
                Value::BigInt {
                    negative: false,
                    magnitude: U256::ZERO,
                },
            ]),
            Value::List(
                texts
                    .iter()
                    .map(|text| Value::Text((*text).to_owned()))
                    .chain([Value::Null])
                    .collect(),
            ),
            Value::List(Vec::new()),
        ];
        {
            let mut appender = connection.appender("lists").expect("appender");
            appender
                .append_row(appender_params_from_iter(&values))
                .expect("append");
        }
        let (numbers, read, nulls, empty): (String, Vec<String>, i64, i64) = connection
            .query_row(
                "SELECT numbers::VARCHAR, list_filter(texts, x -> x IS NOT NULL)::VARCHAR[], \
                 len(list_filter(texts, x -> x IS NULL)), len(empty) FROM lists",
                [],
                |row| {
                    let read = match row.get::<_, duckdb::types::Value>(1)? {
                        duckdb::types::Value::List(items) => items
                            .into_iter()
                            .map(|item| match item {
                                duckdb::types::Value::Text(text) => text,
                                other => panic!("a text element, not {other:?}"),
                            })
                            .collect(),
                        other => panic!("a list, not {other:?}"),
                    };
                    Ok((row.get(0)?, read, row.get(2)?, row.get(3)?))
                },
            )
            .expect("the lists read back");
        assert_eq!(numbers, format!("[-{}, 0]", U256::MAX));
        assert_eq!(read, texts);
        assert_eq!(nulls, 1);
        assert_eq!(empty, 0);
    }

    fn row_count(sink: &DuckDbSink, table: &str) -> i64 {
        sink.engine
            .connection
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .expect("count reads back")
    }

    /// The first column of every row `sql` selects.
    fn column<T: FromSql>(connection: &Connection, sql: &str) -> Vec<T> {
        connection
            .prepare(sql)
            .expect("prepare")
            .query_map([], |row| row.get(0))
            .expect("query")
            .collect::<Result<_, _>>()
            .expect("rows")
    }

    fn log_timestamp(sink: &DuckDbSink) -> u64 {
        sink.engine
            .connection
            .query_row(
                "SELECT epoch(block_timestamp)::UBIGINT FROM logs",
                [],
                |row| row.get(0),
            )
            .expect("timestamp reads back")
    }
}
