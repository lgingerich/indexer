//! The `DuckDB` sink: a store, as a local queryable database.
//!
//! Where [`StdoutJsonSink`](crate::sink::StdoutJsonSink) is fire-and-forward, this one
//! persists: it writes each envelope to disk so a later process can query the history with
//! SQL. That split matters — `DuckDB` is an embedded, single-writer engine, so it is an
//! archive/analytics endpoint, not a horizontally-scaled egress. Keep it for a local
//! replica or an analytical sidecar, not as the fan-out for many consumers.
//!
//! # What lives here, and what does not
//!
//! What a log's columns *are* is not a `DuckDB` question. It is the same question for
//! every store, and [`crate::wire::row`] answers it once, beside the datasets it is about:
//! this sink receives a [`Row`] — a table, its column names, and values in that order —
//! and knows only what is genuinely `DuckDB`'s:
//!
//! - the DDL, generated from [`Table::columns`](crate::wire::row::Table::columns) so the
//!   schema and the data cannot disagree about what a table has;
//! - how a [`ColumnValue`] becomes a `duckdb` type;
//! - the appender, and the commit.
//!
//! So adding a store means writing one module that consumes the same rows, rather than
//! re-deciding for each of seven tables what a log is.
//!
//! # Column types
//!
//! Two mappings are worth stating, because the obvious choice is wrong for both:
//!
//! - [`Uint`](crate::wire::row::ColumnValue::Uint) is `UBIGINT`, not `BIGINT`. Block
//!   numbers and gas figures are unsigned; a signed 64-bit column cannot hold the top half
//!   of the range, and the value is not optional, so nothing is gained by the narrower
//!   type.
//! - [`Text`](crate::wire::row::ColumnValue::Text) and
//!   [`Document`](crate::wire::row::ColumnValue::Document) are `VARCHAR` and `JSON`. `0x`
//!   hex is what a node sends, so a value read from a typed column compares equal to the
//!   same value read out of the raw RPC response — which is what makes the typed tables a
//!   rewrite rather than a second dialect.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::Context as _;
use duckdb::Connection;
use duckdb::types::{ToSql, Value};
use serde::Deserialize;
use tracing::info;

use crate::sink::EnvelopeSink;
use crate::wire::envelope::Envelope;
use crate::wire::row::{ColumnType, ColumnValue, Row, Table, row_for};
/// The `DuckDB` file written when the settings name no path.
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
/// `[sink.duckdb]` table, which [`Storage::DuckDb`](crate::config::Sink::DuckDb)
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
            settings: BTreeMap::new(),
        }
    }
}

/// The `DuckDB` type a [`ColumnType`] is stored as.
///
/// The only place a `DuckDB` type name appears. A `ClickHouse` sink would have its own
/// mapping and read the same headers, which is the whole point of the split: the row
/// says what the data *is*, and this says what `DuckDB` calls it.
fn sql_type(kind: ColumnType) -> &'static str {
    match kind {
        // `UBIGINT`, not `BIGINT`: block numbers and gas *amounts* are unsigned, and a
        // signed 64-bit column cannot hold the top half of the range.
        ColumnType::Uint => "UBIGINT",
        // `VARCHAR` of `0x` hex, for a hash and for a price alike. `HUGEINT` would hold
        // a price numerically, but then a price read from this column would not equal
        // the same price in a raw RPC response, and hex is the encoding the node sends.
        ColumnType::Text => "VARCHAR",
        ColumnType::Bool => "BOOLEAN",
        ColumnType::Document => "JSON",
    }
}

/// A `CREATE TABLE IF NOT EXISTS` for one table, generated from its header.
///
/// Generated rather than written out, because a hand-written DDL and a row header are two
/// statements of the same fact. Generating it means a column added to the dataset appears
/// in the schema without anyone remembering the DDL, and a column that exists in the DDL
/// but not in the data cannot go unnoticed.
///
/// Every column is nullable, deliberately: a `NOT NULL` constraint here would be a claim
/// about the chain that this layer cannot make — a field is absent on some records and
/// present on others, and which is a fact about the data, not about the schema.
fn create_table(table: Table) -> String {
    let columns = table
        .columns()
        .iter()
        .map(|column| format!("{} {}", column.name, sql_type(column.kind)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("CREATE TABLE IF NOT EXISTS {} ({columns})", table.name())
}

/// Renders a row's values as the appender's parameters.
///
/// The only `duckdb::types::Value` construction in the file, so the mapping from the
/// neutral enum to the engine's is one function rather than one per column.
fn sql_value(value: &ColumnValue) -> Value {
    match value {
        ColumnValue::Null => Value::Null,
        ColumnValue::Uint(number) => Value::from(*number),
        // A `Document` is stored as the JSON text it already is, so it lands in the same
        // `VARCHAR` a `Text` does while its column is still typed `JSON`.
        ColumnValue::Text(text) => Value::Text(text.clone()),
        ColumnValue::Bool(flag) => Value::Boolean(*flag),
        ColumnValue::Document(json) => Value::Text(json.clone()),
    }
}

/// Appends envelopes to a local `DuckDB` database, one batch per [`flush`].
///
/// [`flush`]: EnvelopeSink::flush
pub struct DuckDbSink {
    connection: Connection,
    batches: Batches,
}

impl std::fmt::Debug for DuckDbSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDbSink")
            .field("buffered", &self.batches.len())
            .finish_non_exhaustive()
    }
}

impl DuckDbSink {
    /// Opens the database the settings named and returns a sink writing to it.
    ///
    /// The path and the engine settings are `DuckDB`'s, so opening lives here rather than
    /// in the runtime: this is the one place that knows [`duckdb::Config`] is how the
    /// engine takes its settings, and a test can go through it without a settings file.
    /// [`new`](Self::new) stays the way to supply a [`Connection`] of your own.
    ///
    /// # Errors
    ///
    /// Returns an error when an engine setting is rejected, the database cannot be
    /// opened, or the tables cannot be created.
    pub fn open(settings: &DuckDbSettings) -> anyhow::Result<Self> {
        let mut config = duckdb::Config::default();
        for (key, value) in &settings.settings {
            config = config
                .with(key, value)
                .with_context(|| format!("duckdb setting {key:?} was rejected"))?;
        }
        let connection = Connection::open_with_flags(&settings.path, config)
            .with_context(|| format!("open store at {}", settings.path.display()))?;
        info!(store = %settings.path.display(), "storage opened");
        Self::new(connection)
    }

    /// Takes ownership of `connection` and ensures every table exists.
    ///
    /// The runtime opens the connection with whatever path and settings it needs; the
    /// library owns only the schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the tables cannot be created.
    pub fn new(connection: Connection) -> anyhow::Result<Self> {
        let ddl = Table::ALL.map(create_table).join(";\n") + ";";
        connection
            .execute_batch(&ddl)
            .context("create dataset tables")?;
        Ok(Self {
            connection,
            batches: Batches::default(),
        })
    }

    /// Buffers one envelope as a row in its dataset's table.
    fn write(&mut self, envelope: &Envelope) -> anyhow::Result<()> {
        let row = row_for(&envelope.chain, &envelope.event)?;
        self.batches.push(row);
        Ok(())
    }
}

impl EnvelopeSink for DuckDbSink {
    async fn publish(&mut self, envelope: Envelope) -> anyhow::Result<()> {
        self.write(&envelope)
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.batches.append_to(&self.connection)
    }
}

/// Renders a row's values as the appender's parameters.
///
/// The only `duckdb::types::Value` construction in the file, so the mapping from the
/// neutral enum to the engine's is one function rather than one per column. Rendered per
/// flush rather than per publish, because a buffered row is the neutral one until the
/// commit that has to hand the engine its own type.
fn sql_params(row: &Row) -> Vec<Value> {
    row.values().iter().map(sql_value).collect()
}

/// The rows buffered for one flush, grouped by the table they belong to.
///
/// One list rather than one per table: grouping happens at the flush, because a store's
/// batching is a property of the commit and sorting on every publish would pay for
/// something only the flush needs. A block with no transactions then still opens no
/// appender for `transaction`.
#[derive(Debug, Default)]
struct Batches {
    rows: Vec<Row>,
}

impl Batches {
    fn push(&mut self, row: Row) {
        self.rows.push(row);
    }

    /// Writes every row, table by table, then clears the buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if an appender cannot be opened, a row appended, or a batch
    /// committed. The buffer is cleared only once every table has been written, so a
    /// failure part-way leaves the rows buffered rather than losing them.
    fn append_to(&mut self, connection: &Connection) -> anyhow::Result<()> {
        for table in Table::ALL {
            let rows: Vec<&Row> = self
                .rows
                .iter()
                .filter(|row| row.table() == table)
                .collect();
            if rows.is_empty() {
                continue;
            }
            let mut appender = connection
                .appender(table.name())
                .with_context(|| format!("open appender for {table}"))?;
            for row in rows {
                // `block` is wider than the appender's fixed-size row impls cover, so the
                // row goes in as a slice of trait objects — the shape the `duckdb` crate
                // documents for a wide table.
                let params = sql_params(row);
                let borrowed: Vec<&dyn ToSql> =
                    params.iter().map(|value| value as &dyn ToSql).collect();
                appender
                    .append_row(borrowed.as_slice())
                    .with_context(|| format!("append a {table} row"))?;
            }
            appender.flush().with_context(|| format!("flush {table}"))?;
        }
        self.rows.clear();
        Ok(())
    }

    /// The total rows buffered.
    fn len(&self) -> usize {
        self.rows.len()
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{Address, B256, TxHash};
    use duckdb::Connection;

    use crate::sink::EnvelopeSink as _;
    use crate::wire::envelope::{
        Block, ChainId, Envelope, Event, Finalized, Log, Receipt, Reorg, Transaction,
    };
    use crate::wire::row::{Table, row_for};

    use super::DuckDbSink;

    fn sink() -> DuckDbSink {
        let connection = Connection::open_in_memory().expect("open in-memory DuckDB");
        DuckDbSink::new(connection).expect("create dataset tables")
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
                    value: alloy_primitives::U256::from(1_000),
                    // A wei price above `u64::MAX`, which is ~18.4 ETH — so this also
                    // checks the 128-bit columns are not truncated to 64.
                    gas_price: Some(u128::from(u64::MAX) + 1),
                    block_number: 100,
                    block_hash: hash(0x01),
                    ..Transaction::default()
                })),
            ),
            Envelope::new(
                chain(),
                Event::Receipt(Box::new(Receipt {
                    transaction_hash: TxHash::from([0x11; 32]),
                    transaction_index: 3,
                    status: true,
                    gas_used: 21_000,
                    log_count: 1,
                    block_number: 100,
                    block_hash: hash(0x01),
                    ..Receipt::default()
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
                Event::Reorg(Reorg {
                    height: 100,
                    new_head_hash: hash(0x55),
                    orphaned_hashes: vec![hash(0x66)],
                }),
            ),
            Envelope::new(
                chain(),
                Event::Finalized(Finalized {
                    height: 100,
                    hash: hash(0x01),
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
    /// single `events` table: a log is a row of typed columns in `log`, not a row with
    /// most of them null.
    ///
    /// The fixture has no decoded record — only the decode stage produces those — so
    /// `decoded` is expected to be empty rather than to hold one.
    #[tokio::test]
    async fn each_event_kind_lands_in_its_own_table() {
        let mut sink = sink();
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
        let mut sink = sink();
        let envelope = every_kind()
            .into_iter()
            .find(|envelope| matches!(envelope.event, Event::Log(_)))
            .expect("a log envelope");
        let expected_key = envelope.event.dedupe_key();
        sink.publish(envelope).await.expect("row buffers");
        sink.flush().await.expect("batch flushes");

        let (log_index, transaction_index, address, topic0, block_number, block_hash, key, chain):
            (u64, u64, String, String, u64, String, String, String) = sink
            .connection
            .query_row(
                "SELECT log_index, transaction_index, address, topic0, block_number, \
                 block_hash, dedupe_key, chain FROM log",
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

    /// A wei price above `u64::MAX` survives the round trip, which is the whole reason
    /// those columns are not `UBIGINT`. Truncating here would silently bill a swap at
    /// zero.
    #[tokio::test]
    async fn a_wei_price_above_u64_max_is_not_truncated() {
        let mut sink = sink();
        store(&mut sink).await;

        let price: String = sink
            .connection
            .query_row("SELECT gas_price FROM transaction", [], |row| row.get(0))
            .expect("the price reads back");
        let expected = u128::from(u64::MAX) + 1;
        assert_eq!(price, format!("{expected:#x}"));
        assert_ne!(
            price,
            format!("{:#x}", 0),
            "a price must not collapse to zero"
        );
    }

    /// A block's wide row is written whole, in the order the generated DDL declares. The
    /// header is the schema, so this checks the two agree end to end.
    #[tokio::test]
    async fn a_wide_block_row_writes_every_column() {
        let mut sink = sink();
        store(&mut sink).await;

        let columns = i64::try_from(Table::Block.columns().len()).expect("a column count fits");
        let declared: i64 = sink
            .connection
            .query_row(
                "SELECT count(*) FROM duckdb_columns() WHERE table_name = 'block'",
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
            .connection
            .query_row(
                "SELECT number, hash, parent_hash, timestamp, gas_limit, gas_used, \
                 transaction_count FROM block",
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
        let mut sink = sink();
        store(&mut sink).await;

        let present: i64 = sink
            .connection
            .query_row(
                "SELECT count(*) FROM block WHERE withdrawals_root IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .expect("null filter reads back");
        assert_eq!(present, 0, "the fixture block has no withdrawals root");
    }

    /// Nothing is durable before the flush, and everything buffered is gone after it.
    #[tokio::test]
    async fn nothing_is_durable_until_the_flush() {
        let mut sink = sink();
        for envelope in every_kind() {
            sink.publish(envelope).await.expect("row buffers");
        }
        assert_eq!(sink.batches.len(), 6, "buffered but not written");
        assert_eq!(row_count(&sink, "log"), 0);

        sink.flush().await.expect("batch flushes");
        assert_eq!(sink.batches.len(), 0, "the buffers are cleared");
        assert_eq!(row_count(&sink, "log"), 1);
    }

    /// A batch of several rows lands in one flush.
    #[tokio::test]
    async fn a_flush_writes_the_whole_batch() {
        let mut sink = sink();
        for height in 0..5 {
            sink.publish(Envelope::new(
                chain(),
                Event::Finalized(Finalized {
                    height,
                    hash: hash(0x11),
                }),
            ))
            .await
            .expect("row buffers");
        }
        sink.flush().await.expect("batch flushes");

        assert_eq!(row_count(&sink, "finalized"), 5);
    }

    /// Connecting twice to the same file must not fail on the existing tables.
    #[tokio::test]
    async fn new_is_idempotent() {
        let path = std::env::temp_dir().join(format!("indexer-sink-{}.duckdb", std::process::id()));
        let open = || {
            DuckDbSink::new(
                Connection::open(path.to_string_lossy().into_owned()).expect("open temp database"),
            )
            .expect("create or reuse the dataset tables")
        };
        open();
        open();
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
        let row = row_for(&chain(), &event).expect("a log row");

        assert_eq!(row.table(), Table::Log);
        assert_eq!(row.chain(), "base");
        assert_eq!(row.dedupe_key(), event.dedupe_key());
        assert_eq!(row.values().len(), row.columns().len());
    }

    fn row_count(sink: &DuckDbSink, table: &str) -> i64 {
        sink.connection
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .expect("count reads back")
    }
}
