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

use crate::sink::sql::{Dialect, ident};
use crate::sink::store::{Engine, EngineError, Operation, SqlStore, StoreError};
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
/// `[sink.duckdb]` table, which [`Sink::DuckDb`](crate::config::Sink::DuckDb)
/// names.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DuckDbSettings {
    /// The `DuckDB` database file to write. Defaults to `indexer.duckdb`.
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
    /// ```
    pub settings: BTreeMap<String, String>,
}

impl Default for DuckDbSettings {
    fn default() -> Self {
        Self {
            path: PathBuf::from(DEFAULT_PATH),
            batch_records: DEFAULT_BATCH_RECORDS,
            settings: BTreeMap::default(),
        }
    }
}

/// A store in `DuckDB`.
pub type DuckDbSink = SqlStore<DuckDb>;

/// An open `DuckDB` connection.
#[derive(Debug)]
pub struct DuckDb {
    pub(crate) connection: Connection,
}

impl DuckDbSink {
    /// Opens the database the settings name and creates the tables in `database_schema`.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when a setting is rejected, the database cannot be
    /// opened, or a table cannot be created.
    pub async fn open(
        settings: &DuckDbSettings,
        schema: Arc<Schema>,
        database_schema: &str,
    ) -> Result<Self, StoreError> {
        let mut config = duckdb::Config::default();
        for (key, value) in &settings.settings {
            config = config.with(key, value).map_err(|error| {
                StoreError::engine(Operation::Configure, Some(key))(error.into())
            })?;
        }
        let path = settings.path.display().to_string();
        let connection = Connection::open_with_flags(&settings.path, config)
            .map_err(|error| StoreError::engine(Operation::Open, Some(&path))(error.into()))?;
        info!(store = %path, "storage opened");
        Self::connected(connection, schema, database_schema).await
    }

    /// Takes an open connection and creates every table in `schema` in the database
    /// schema `database_schema`.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when the schema or a table cannot be created.
    pub async fn connected(
        connection: Connection,
        schema: Arc<Schema>,
        database_schema: &str,
    ) -> Result<Self, StoreError> {
        SqlStore::new(DuckDb { connection }, schema, database_schema).await
    }
}

impl Dialect for DuckDb {
    const INDEXES: bool = false;
    const DESCRIBE: &'static str = "SELECT column_name, data_type FROM information_schema.columns \
         WHERE table_schema = current_schema() AND table_name = $1 ORDER BY ordinal_position";

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
    use alloy_primitives::{Address, B256, TxHash, U256};
    use duckdb::{Connection, appender_params_from_iter};

    use crate::sink::EnvelopeSink as _;
    use crate::wire::envelope::{
        AcceptedBlock, Block, ChainId, Contract, Envelope, Event, Log, Reorg, Transaction,
    };
    use std::sync::Arc;

    use crate::sink::table::{Schema, Table, Value};

    use super::DuckDbSink;

    /// The dataset tables alone.
    fn datasets() -> Arc<Schema> {
        Arc::new(Schema::new().expect("the dataset tables"))
    }

    /// The `reorgs` table's DDL, to recreate it after a test drops it.
    fn reorg_ddl() -> String {
        crate::sink::sql::create_table::<super::DuckDb>(datasets().dataset(Table::Reorg))
    }

    async fn sink() -> DuckDbSink {
        let connection = Connection::open_in_memory().expect("open in-memory DuckDB");
        DuckDbSink::connected(connection, datasets(), "base")
            .await
            .expect("create dataset tables")
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
                Event::AcceptedBlock(AcceptedBlock {
                    height: 100,
                    hash: hash(0x01),
                    parent_hash: hash(0x02),
                    timestamp: 1_700_000_000,
                }),
            ),
        ]
    }

    async fn store(sink: &mut DuckDbSink) {
        for envelope in every_kind() {
            sink.publish(envelope).await.expect("row buffers");
        }
        sink.flush().await.expect("batch flushes");
    }

    /// One event lands in its own table, and nowhere else. The point of dropping the
    /// single `events` table: a log is a row of typed columns in `logs`, not a row with
    /// most of them null.
    ///
    /// The fixture has no decoded record, so `decoded_logs` is expected to be empty rather than
    /// to hold one; the decode stage's own output is covered in `decode` and `runtime`.
    #[tokio::test]
    async fn each_event_kind_lands_in_its_own_table() {
        let mut sink = sink().await;
        store(&mut sink).await;

        for table in Table::ALL {
            let expected = i64::from(table != Table::Decoded);
            assert_eq!(
                row_count(&sink, table.name()),
                expected,
                "one row in {table}, and nothing anywhere else"
            );
        }
    }

    /// The columns are typed values, not JSON text: a consumer filters and compares them
    /// directly. Integers land as `UBIGINT` and hashes as `0x` hex, the encoding a node
    /// itself uses, so a value read from a typed column and the same value read from a
    /// JSON document compare equal.
    #[tokio::test]
    async fn a_log_row_is_typed_columns_rather_than_a_json_blob() {
        let mut sink = sink().await;
        let envelope = every_kind()
            .into_iter()
            .find(|envelope| matches!(envelope.event, Event::Log(_)))
            .expect("a log envelope");
        let expected_key = envelope.event.dedupe_key();
        sink.publish(envelope).await.expect("row buffers");
        sink.flush().await.expect("batch flushes");

        let (log_index, transaction_index, address, topic0, block_number, block_hash, key, chain):
            (u64, u64, String, String, u64, String, String, String) = sink.engine.connection
            .query_row(
                "SELECT log_index, transaction_index, address, topic0, block_number, \
                 block_hash, dedupe_key, chain FROM logs",
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
                        row.get(7)?,
                    ))
                },
            )
            .expect("log row reads back");

        assert_eq!(log_index, 7, "the log's own position in the block");
        assert_eq!(transaction_index, 3, "the emitting transaction's position");
        assert_eq!(address, format!("{:#x}", Address::from([0x33; 20])));
        assert_eq!(topic0, format!("{:#x}", hash(0x44)));
        assert_eq!(block_number, 100);
        assert_eq!(block_hash, format!("{:#x}", hash(0x01)));
        assert_eq!(key, expected_key, "the key is the row's identity");
        assert_eq!(chain, "base");
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

        let columns = i64::try_from(datasets().dataset(Table::Block).columns.len())
            .expect("a column count fits");
        let declared: i64 = sink
            .engine
            .connection
            .query_row(
                "SELECT count(*) FROM duckdb_columns() WHERE table_name = 'blocks'",
                [],
                |row| row.get(0),
            )
            .expect("column count reads back");
        assert_eq!(
            declared, columns,
            "the DDL and the header must agree on width"
        );

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
    /// `WHERE withdrawals_root IS NOT NULL` has to get exactly those blocks.
    #[tokio::test]
    async fn an_absent_optional_column_is_null() {
        let mut sink = sink().await;
        store(&mut sink).await;

        let present: i64 = sink
            .engine
            .connection
            .query_row(
                "SELECT count(*) FROM blocks WHERE withdrawals_root IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .expect("null filter reads back");
        assert_eq!(present, 0, "the fixture block has no withdrawals root");
    }

    /// Nothing is durable before the flush, and everything buffered is gone after it.
    #[tokio::test]
    async fn nothing_is_durable_until_the_flush() {
        let mut sink = sink().await;
        for envelope in every_kind() {
            sink.publish(envelope).await.expect("row buffers");
        }
        assert_eq!(
            sink.buffered(),
            every_kind().len(),
            "buffered but not written"
        );
        assert_eq!(row_count(&sink, "logs"), 0);

        sink.flush().await.expect("batch flushes");
        assert_eq!(sink.buffered(), 0, "the buffers are cleared");
        assert_eq!(row_count(&sink, "logs"), 1);
    }

    #[tokio::test]
    async fn a_late_table_failure_rolls_back_and_a_retry_writes_each_row_once() {
        let mut sink = sink().await;
        for envelope in every_kind() {
            sink.publish(envelope).await.expect("row buffers");
        }
        sink.engine
            .connection
            .execute_batch("DROP TABLE reorgs")
            .expect("remove a late table in the batch");

        assert!(matches!(
            sink.flush().await,
            Err(crate::sink::SinkError::Store(crate::sink::StoreError::Engine {
                operation: crate::sink::Operation::Stage,
                target: Some(table),
                ..
            })) if table == "reorgs"
        ));
        assert_eq!(
            sink.buffered(),
            every_kind().len(),
            "failed batch stays buffered"
        );
        for table in Table::ALL
            .into_iter()
            .filter(|table| *table != Table::Reorg)
        {
            assert_eq!(row_count(&sink, table.name()), 0, "{table} rolls back");
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
                i64::from(table != Table::Decoded)
            );
        }
    }

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
        for height in 0..2 {
            sink.publish(Envelope::new(
                chain(),
                Event::Reorg(Reorg {
                    height,
                    new_head_hash: hash(u8::try_from(height).expect("heights 0 and 1")),
                    orphaned_hashes: vec![],
                }),
            ))
            .await
            .expect("row buffers");
        }

        assert!(matches!(
            sink.flush().await,
            Err(crate::sink::SinkError::Store(crate::sink::StoreError::Engine {
                operation: crate::sink::Operation::Merge,
                target: Some(table),
                ..
            })) if table == "reorgs"
        ));
        assert_eq!(row_count(&sink, "reorgs"), 0);
        assert_eq!(sink.buffered(), 2);

        sink.engine
            .connection
            .execute_batch(&format!("DROP TABLE reorgs; {}", reorg_ddl()))
            .expect("remove the constraint");
        sink.flush().await.expect("retry the whole batch");
        assert_eq!(row_count(&sink, "reorgs"), 2);
        assert_eq!(sink.buffered(), 0);
    }

    /// A batch of several rows lands in one flush.
    #[tokio::test]
    async fn a_flush_writes_the_whole_batch() {
        let mut sink = sink().await;
        for height in 0..5 {
            sink.publish(Envelope::new(
                chain(),
                Event::Reorg(Reorg {
                    height,
                    new_head_hash: hash(u8::try_from(height).expect("five heights")),
                    orphaned_hashes: vec![],
                }),
            ))
            .await
            .expect("row buffers");
        }
        sink.flush().await.expect("batch flushes");

        assert_eq!(row_count(&sink, "reorgs"), 5);
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
        let mut replay = orphaned.clone();
        replay.block_timestamp = 5;
        sink.publish(Envelope::new(
            chain(),
            Event::Log(Box::new(orphaned.clone())),
        ))
        .await
        .expect("orphaned log buffers");
        sink.publish(Envelope::new(chain(), Event::Log(Box::new(replay))))
            .await
            .expect("replay buffers");
        sink.flush()
            .await
            .expect("a repeated key in one batch is one row");

        assert_eq!(row_count(&sink, "logs"), 1);
        assert_eq!(log_timestamp(&sink), 5, "the later copy in the batch wins");

        // The same key in a later flush updates the stored row, not appends a second —
        // the merge's `DO UPDATE` branch, which a first flush into an empty table misses.
        let mut redelivered = orphaned.clone();
        redelivered.block_timestamp = 7;
        sink.publish(Envelope::new(chain(), Event::Log(Box::new(redelivered))))
            .await
            .expect("redelivery buffers");
        sink.flush().await.expect("a redelivery updates the row");
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
        replacement.block_timestamp = 9;
        sink.publish(reorg(&[0xaa])).await.expect("reorg buffers");
        sink.publish(Envelope::new(chain(), Event::Log(Box::new(replacement))))
            .await
            .expect("replacement buffers");
        sink.flush()
            .await
            .expect("the reorg and its replacement commit");
        assert_eq!(row_count(&sink, "logs"), 1, "only the replacement remains");
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
        for envelope in other_chain {
            sink.publish(envelope).await.expect("row buffers");
        }
        sink.flush().await.expect("both chains commit");

        sink.publish(reorg(&[0x01])).await.expect("reorg buffers");
        sink.flush().await.expect("the reorg commits");

        for (table, rows) in block_tables(&sink) {
            let base: i64 = sink
                .engine
                .connection
                .query_row(
                    &format!("SELECT count(*) FROM \"{table}\" WHERE chain = 'base'"),
                    [],
                    |row| row.get(0),
                )
                .expect("count reads back");
            assert_eq!(base, 0, "{table} keeps nothing of the orphaned block");
            assert_eq!(
                rows,
                i64::from(table != Table::Decoded),
                "{table} keeps the other chain's row"
            );
        }
        assert_eq!(
            row_count(&sink, "reorgs"),
            3,
            "two fixture markers and this one"
        );
    }

    /// An orphaned block still in the buffer is dropped before it is ever written.
    #[tokio::test]
    async fn a_reorg_drops_an_orphaned_block_from_the_same_batch() {
        let mut sink = sink().await;
        for envelope in every_kind() {
            sink.publish(envelope).await.expect("row buffers");
        }
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
        let stored_blocks = |sink: &DuckDbSink| -> Vec<String> {
            sink.engine
                .connection
                .prepare("SELECT block_hash FROM logs ORDER BY block_hash")
                .expect("prepare")
                .query_map([], |row| row.get(0))
                .expect("query")
                .collect::<Result<_, _>>()
                .expect("rows")
        };

        // Across batches: A, then A', then A again.
        let mut sink = sink().await;
        for envelopes in [
            vec![log(0xaa)],
            vec![reorg(&[0xaa]), log(0xbb)],
            vec![reorg(&[0xbb]), log(0xaa)],
        ] {
            for envelope in envelopes {
                sink.publish(envelope).await.expect("row buffers");
            }
            sink.flush().await.expect("batch commits");
        }
        assert_eq!(stored_blocks(&sink), [format!("{:#x}", hash(0xaa))]);

        // Within one batch, with A already committed.
        let mut sink = DuckDbSink::connected(
            Connection::open_in_memory().expect("open DuckDB"),
            datasets(),
            "base",
        )
        .await
        .expect("create dataset tables");
        sink.publish(log(0xaa)).await.expect("row buffers");
        sink.flush().await.expect("A commits");
        for envelope in [reorg(&[0xaa]), log(0xbb), reorg(&[0xbb]), log(0xaa)] {
            sink.publish(envelope).await.expect("row buffers");
        }
        sink.flush()
            .await
            .expect("deleting and rewriting one key in a transaction commits");
        assert_eq!(stored_blocks(&sink), [format!("{:#x}", hash(0xaa))]);
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
        assert!(sink.flush().await.is_err(), "the commit fails");
        assert_eq!(row_count(&sink, "logs"), 1, "the delete rolled back");

        sink.engine
            .connection
            .execute_batch(&reorg_ddl())
            .expect("restore the missing table");
        sink.flush().await.expect("the retry commits");
        for (table, rows) in block_tables(&sink) {
            assert_eq!(rows, 0, "{table} is retracted on the retry");
        }
    }

    fn accepted(height: u64) -> AcceptedBlock {
        AcceptedBlock {
            height,
            hash: hash(u8::try_from(height).expect("small heights")),
            parent_hash: hash(u8::try_from(height - 1).expect("small heights")),
            timestamp: 1_700_000_000 + height,
        }
    }

    /// The ledger reads back the newest unorphaned accepted blocks of one chain, oldest
    /// first, and drops that chain's rows below them.
    #[tokio::test]
    async fn the_ledger_reads_back_the_newest_canonical_window_and_prunes_below_it() {
        let mut sink = sink().await;
        assert!(sink.ledger(&chain(), 3).await.expect("empty").is_empty());
        for height in 1..=10 {
            sink.publish(Envelope::new(
                chain(),
                Event::AcceptedBlock(accepted(height)),
            ))
            .await
            .expect("buffer");
        }
        sink.publish(Envelope::new(
            ChainId::new("ethereum"),
            Event::AcceptedBlock(accepted(1)),
        ))
        .await
        .expect("buffer");
        // Block 10 was orphaned and deleted, so the tip falls back to 9.
        sink.publish(Envelope::new(
            chain(),
            Event::Reorg(Reorg {
                height: 10,
                new_head_hash: hash(0xee),
                orphaned_hashes: vec![hash(10)],
            }),
        ))
        .await
        .expect("buffer");
        sink.flush().await.expect("flush");

        let ledger = sink.ledger(&chain(), 3).await.expect("read back");
        assert_eq!(ledger, [accepted(7), accepted(8), accepted(9)]);
        let remaining: Vec<u64> = sink
            .engine
            .connection
            .prepare("SELECT height FROM accepted_blocks WHERE chain = 'base' ORDER BY height")
            .expect("prepare")
            .query_map([], |row| row.get(0))
            .expect("query")
            .collect::<Result<_, _>>()
            .expect("rows");
        assert_eq!(remaining, [7, 8, 9], "the orphaned row above was deleted");
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
        let mut sink = DuckDbSink::connected(
            Connection::open_in_memory().expect("open DuckDB"),
            crate::sink::fixtures::schema(),
            "base",
        )
        .await
        .expect("create dataset and event tables");
        let swap = crate::sink::fixtures::decoded_swap();
        let block = swap.block_hash;
        sink.publish(Envelope::new(chain(), Event::Decoded(Box::new(swap))))
            .await
            .expect("row buffers");
        sink.flush().await.expect("batch commits");

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

        sink.publish(Envelope::new(
            chain(),
            Event::Reorg(Reorg {
                height: 1,
                new_head_hash: hash(0x56),
                orphaned_hashes: vec![block],
            }),
        ))
        .await
        .expect("reorg buffers");
        sink.flush().await.expect("the reorg commits");
        assert_eq!(row_count(&sink, "uniswap_v3_pool_swap"), 0);
        assert_eq!(row_count(&sink, "decoded_logs"), 0);
    }

    /// Two chains sharing a database write to their own schemas, so each keeps its own
    /// tables.
    #[tokio::test]
    async fn each_chain_writes_to_its_own_schema() {
        let connection = Connection::open_in_memory().expect("open DuckDB");
        for name in ["base", "ethereum"] {
            let handle = connection.try_clone().expect("a second handle");
            let mut sink = DuckDbSink::connected(handle, datasets(), name)
                .await
                .expect("open");
            let mut envelope = every_kind().remove(0);
            envelope.chain = ChainId::new(name);
            sink.publish(envelope).await.expect("row buffers");
            sink.flush().await.expect("batch commits");
        }
        for name in ["base", "ethereum"] {
            let chains: Vec<String> = connection
                .prepare(&format!("SELECT chain FROM {name}.blocks"))
                .expect("prepare")
                .query_map([], |row| row.get(0))
                .expect("query")
                .collect::<Result<_, _>>()
                .expect("rows");
            assert_eq!(chains, [name], "{name}.block holds only its chain");
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
            let connection = Connection::open_in_memory().expect("open DuckDB");
            connection
                .execute_batch(&format!("CREATE SCHEMA base; USE base; {ddl}"))
                .expect("an old reorgs table");
            let error = DuckDbSink::connected(connection, datasets(), "base")
                .await
                .expect_err("the table drifted");
            assert!(
                matches!(&error, crate::sink::StoreError::Drift { table, difference: found }
                    if table == "reorgs" && found == difference),
                "{error}"
            );
        }
    }

    /// Connecting twice to the same file must not fail on the existing tables.
    #[tokio::test]
    async fn new_is_idempotent() {
        let path = std::env::temp_dir().join(format!("indexer-sink-{}.duckdb", std::process::id()));
        for _ in 0..2 {
            DuckDbSink::connected(
                Connection::open(path.to_string_lossy().into_owned()).expect("open temp database"),
                crate::sink::fixtures::schema(),
                "base",
            )
            .await
            .expect("create or reuse the dataset tables");
        }
        // Leave nothing behind; the connection is released when the sinks drop.
        std::fs::remove_file(&path).expect("remove temp database");
    }

    /// The row a store writes says which table and what it holds, so the store never has
    /// to switch on the event to know where a row goes.
    #[test]
    fn a_row_names_its_table_and_carries_the_common_columns() {
        let log = Log {
            log_index: 7,
            block_number: 100,
            block_hash: hash(0x01),
            ..Log::default()
        };
        let event = Event::Log(Box::new(log));
        let row = datasets().row(&chain(), &event).expect("a row");

        assert_eq!(row.table().id, Table::Log.into());
        assert_eq!(row.chain(), "base");
        assert_eq!(row.dedupe_key(), event.dedupe_key());
        assert_eq!(row.values().len(), row.table().columns.len());
    }

    /// A decoded array of scalars lands as a native list of its element type, and a tuple
    /// as one JSON object keyed by its ABI names with every integer a decimal string, as
    /// Allium's decoded `params` are.
    #[tokio::test]
    async fn an_array_is_a_typed_list_and_a_tuple_one_object() {
        let mut sink = DuckDbSink::connected(
            Connection::open_in_memory().expect("open DuckDB"),
            crate::sink::fixtures::schema(),
            "base",
        )
        .await
        .expect("create dataset and event tables");
        let created = crate::sink::fixtures::decoded_pool_created();
        sink.publish(Envelope::new(chain(), Event::Decoded(Box::new(created))))
            .await
            .expect("row buffers");
        sink.flush().await.expect("batch commits");

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
        let connection = Connection::open_in_memory().expect("open DuckDB");
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
