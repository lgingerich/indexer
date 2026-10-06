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
//! - the append into a staging table, the upsert onto `(chain, dedupe_key)`, and the commit.
//!
//! So adding a store means writing one module that consumes the same rows, rather than
//! re-deciding for each of six tables what a log is.
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

use duckdb::types::{ToSql, ToSqlOutput, ValueRef};
use duckdb::{Connection, appender_params_from_iter};
use serde::Deserialize;
use thiserror::Error;
use tracing::info;

use crate::sink::{EnvelopeSink, SinkError};
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
/// statements of the same fact. Nullability comes from [`Column::required`](crate::wire::row::Column::required).
/// `(chain, dedupe_key)` is unique so a replay can upsert.
fn create_table(table: Table) -> String {
    let columns = table
        .columns()
        .iter()
        .map(|column| {
            let nullability = if column.required { " NOT NULL" } else { "" };
            format!("{} {}{nullability}", column.name, sql_type(column.kind))
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("CREATE TABLE IF NOT EXISTS \"{table}\" ({columns}, UNIQUE (chain, dedupe_key))")
}

fn staging_table(table: Table) -> String {
    format!("staging_{table}")
}

/// Merges the staging table into the dataset table.
///
/// `DISTINCT ON` keeps one row per key when a batch holds a key twice, and `rowid` is the
/// staging table's insertion order, so the last copy wins — the same row a replay would
/// land on. `ON CONFLICT` then updates a row an earlier flush already wrote.
fn upsert_sql(table: Table) -> String {
    let columns = table.columns();
    let names = columns
        .iter()
        .map(|column| format!("\"{}\"", column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let assignments = columns
        .iter()
        .filter(|column| column.name != "chain" && column.name != "dedupe_key")
        .map(|column| format!("\"{name}\" = EXCLUDED.\"{name}\"", name = column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let staging = staging_table(table);
    format!(
        "INSERT INTO \"{table}\" ({names}) \
         SELECT DISTINCT ON (\"chain\", \"dedupe_key\") {names} FROM \"{staging}\" \
         ORDER BY \"chain\", \"dedupe_key\", rowid DESC \
         ON CONFLICT (\"chain\", \"dedupe_key\") DO UPDATE SET {assignments}"
    )
}

// The engine conversion belongs at this boundary; text and JSON borrow the buffered
// row rather than cloning its strings for every append.
impl ToSql for ColumnValue {
    fn to_sql(&self) -> duckdb::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match self {
            Self::Null => ValueRef::Null,
            Self::Uint(number) => ValueRef::UBigInt(*number),
            Self::Text(text) | Self::Document(text) => ValueRef::Text(text.as_bytes()),
            Self::Bool(flag) => ValueRef::Boolean(*flag),
        }))
    }
}

/// Upserts envelopes into a local `DuckDB` database, one atomic batch per [`flush`].
///
/// Rows stay buffered until commit succeeds. Failed batches roll back and remain
/// buffered. A replay of `(chain, dedupe_key)` updates that row; the last copy in the
/// batch wins.
///
/// [`flush`]: EnvelopeSink::flush
pub struct DuckDbSink {
    connection: Connection,
    rows: Vec<Row>,
}

impl std::fmt::Debug for DuckDbSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDbSink")
            .field("buffered", &self.rows.len())
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
    /// Returns [`StoreError::Setting`] when an engine setting is rejected,
    /// [`StoreError::Open`] when the database cannot be opened, and
    /// [`StoreError::Schema`] when the tables cannot be created.
    pub fn open(settings: &DuckDbSettings) -> Result<Self, StoreError> {
        let mut config = duckdb::Config::default();
        for (key, value) in &settings.settings {
            config = config
                .with(key, value)
                .map_err(|source| StoreError::Setting {
                    key: key.clone(),
                    source,
                })?;
        }
        let connection = Connection::open_with_flags(&settings.path, config).map_err(|source| {
            StoreError::Open {
                path: settings.path.display().to_string(),
                source,
            }
        })?;
        info!(store = %settings.path.display(), "storage opened");
        Self::new(connection)
    }

    /// Takes ownership of `connection` and ensures every table exists.
    ///
    /// Use this when supplying an existing connection rather than settings to [`open`](Self::open).
    /// Existing tables are reused, not migrated or validated against the current schema.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Schema`] if the tables cannot be created.
    pub fn new(connection: Connection) -> Result<Self, StoreError> {
        let ddl = Table::ALL.map(create_table).join(";\n") + ";";
        connection
            .execute_batch(&ddl)
            .map_err(|source| StoreError::Schema { source })?;
        Ok(Self {
            connection,
            rows: Vec::new(),
        })
    }
}

impl EnvelopeSink for DuckDbSink {
    async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
        self.rows.push(row_for(&envelope.chain, &envelope.event));
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), SinkError> {
        self.write_batch()?;
        Ok(())
    }
}

/// Why the store could not be opened, or could not accept a batch.
///
/// The leaf under [`crate::sink::SinkError::Store`], and the reason
/// the setting key and the store path are fields rather than a formatted message: both
/// are inputs the caller supplied, and an operator reading a log should be able to match
/// on the key rather than parse it back out of prose.
///
/// Every variant here is a [`duckdb::Error`], which is what keeps this enum narrower than
/// the layer above it: the engine is the only thing that can fail at these points.
///
/// The engine's error is carried rather than stringified, so the `#[error]` output reads
/// the same as a formatted message would while staying matchable by a caller.
#[derive(Debug, Error)]
pub enum StoreError {
    /// The engine refused one of the settings the file passed through.
    #[error("duckdb setting {key:?} was rejected: {source}")]
    Setting {
        /// The setting as written in `[sink.duckdb.settings]`.
        key: String,
        /// The engine's own reason.
        source: duckdb::Error,
    },
    /// The database file could not be opened.
    #[error("open store at {path}: {source}")]
    Open {
        /// The path as written in `[sink.duckdb]`.
        path: String,
        /// The engine's own reason.
        source: duckdb::Error,
    },
    /// The dataset tables could not be created.
    #[error("create dataset tables: {source}")]
    Schema {
        /// The engine's own reason.
        source: duckdb::Error,
    },
    /// An appender could not be opened for a table.
    #[error("open appender for {table}: {source}")]
    Appender {
        /// Which table's appender failed to open.
        table: &'static str,
        /// The engine's own reason.
        source: duckdb::Error,
    },
    /// A row could not be appended.
    #[error("append a {table} row: {source}")]
    Append {
        /// Which table rejected the row.
        table: &'static str,
        /// The engine's own reason.
        source: duckdb::Error,
    },
    /// The staging table for an upsert could not be prepared.
    #[error("prepare {table} upsert: {source}")]
    Prepare {
        /// Which table's staging table failed.
        table: &'static str,
        /// The engine's own reason.
        source: duckdb::Error,
    },
    /// The staged rows could not be merged into the table.
    #[error("upsert {table}: {source}")]
    Upsert {
        /// Which table rejected the merge.
        table: &'static str,
        /// The engine's own reason.
        source: duckdb::Error,
    },
    /// A table's buffered appends could not be flushed into the transaction.
    #[error("flush {table}: {source}")]
    Flush {
        /// Which table's flush failed.
        table: &'static str,
        /// The engine's own reason.
        source: duckdb::Error,
    },
    /// A batch transaction could not be started.
    #[error("begin store transaction: {source}")]
    Begin {
        /// The engine's own reason.
        source: duckdb::Error,
    },
    /// A batch transaction could not be committed.
    #[error("commit store transaction: {source}")]
    Commit {
        /// The engine's own reason.
        source: duckdb::Error,
    },
}

impl DuckDbSink {
    /// Writes every table in one transaction, then clears the buffer after commit.
    ///
    /// Each table is appended into a temporary staging table and merged with
    /// `INSERT … ON CONFLICT DO UPDATE`. A key a batch holds twice keeps its last copy. On
    /// failure the transaction rolls back and the whole batch stays buffered for a retry.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction, the staging append, or the upsert fails.
    fn write_batch(&mut self) -> Result<(), StoreError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let transaction = self
            .connection
            .transaction()
            .map_err(|source| StoreError::Begin { source })?;
        // ponytail: six fixed tables mean six linear scans, with no grouping buffer.
        // Group at publish time only if the table count or profiling warrants it.
        for table in Table::ALL {
            let mut rows = self
                .rows
                .iter()
                .filter(|row| row.table() == table)
                .peekable();
            if rows.peek().is_none() {
                continue;
            }
            let staging = staging_table(table);
            // A temporary table is invisible to the appender, which looks up `main`.
            // The staging table is created and dropped in this transaction.
            transaction
                .execute_batch(&format!(
                    "CREATE OR REPLACE TABLE \"{staging}\" AS SELECT * FROM \"{table}\" WHERE false"
                ))
                .map_err(|source| StoreError::Prepare {
                    table: table.name(),
                    source,
                })?;
            // The appender borrows the transaction, so it drops before the upsert.
            {
                let mut appender =
                    transaction
                        .appender(&staging)
                        .map_err(|source| StoreError::Appender {
                            table: table.name(),
                            source,
                        })?;
                for row in rows {
                    // Iterator parameters also support tables wider than 32 columns.
                    appender
                        .append_row(appender_params_from_iter(row.values()))
                        .map_err(|source| StoreError::Append {
                            table: table.name(),
                            source,
                        })?;
                }
                // Flush errors must be observed; Drop cannot report them.
                appender.flush().map_err(|source| StoreError::Flush {
                    table: table.name(),
                    source,
                })?;
            }
            transaction
                .execute_batch(&format!("{};\nDROP TABLE \"{staging}\"", upsert_sql(table)))
                .map_err(|source| StoreError::Upsert {
                    table: table.name(),
                    source,
                })?;
        }
        transaction
            .commit()
            .map_err(|source| StoreError::Commit { source })?;
        self.rows.clear();
        Ok(())
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
        Block, ChainId, Envelope, Event, Log, Receipt, Reorg, Transaction,
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
        assert_eq!(sink.rows.len(), 5, "buffered but not written");
        assert_eq!(row_count(&sink, "log"), 0);

        sink.flush().await.expect("batch flushes");
        assert_eq!(sink.rows.len(), 0, "the buffers are cleared");
        assert_eq!(row_count(&sink, "log"), 1);
    }

    #[tokio::test]
    async fn a_late_table_failure_rolls_back_and_a_retry_writes_each_row_once() {
        let mut sink = sink();
        for envelope in every_kind() {
            sink.publish(envelope).await.expect("row buffers");
        }
        sink.connection
            .execute_batch("DROP TABLE reorg")
            .expect("remove the last table in the batch");

        assert!(matches!(
            sink.flush().await,
            Err(crate::sink::SinkError::Store(super::StoreError::Prepare {
                table: "reorg",
                ..
            }))
        ));
        assert_eq!(sink.rows.len(), 5, "failed batch stays buffered");
        for table in Table::ALL
            .into_iter()
            .filter(|table| *table != Table::Reorg)
        {
            assert_eq!(row_count(&sink, table.name()), 0, "{table} rolls back");
        }

        sink.connection
            .execute_batch(&super::create_table(Table::Reorg))
            .expect("restore the missing table");
        sink.flush()
            .await
            .expect("retry commits the original batch");
        sink.flush().await.expect("empty flush is a no-op");
        assert_eq!(sink.rows.len(), 0);
        for table in Table::ALL {
            assert_eq!(
                row_count(&sink, table.name()),
                i64::from(table != Table::Decoded)
            );
        }
    }

    #[tokio::test]
    async fn a_deferred_constraint_failure_keeps_the_batch_and_rolls_back() {
        let mut sink = sink();
        let constrained = super::create_table(Table::Reorg)
            .replace("height UBIGINT", "height UBIGINT CHECK (height < 1)");
        sink.connection
            .execute_batch(&format!("DROP TABLE reorg; {constrained}"))
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
            Err(crate::sink::SinkError::Store(super::StoreError::Upsert {
                table: "reorg",
                ..
            }))
        ));
        assert_eq!(row_count(&sink, "reorg"), 0);
        assert_eq!(sink.rows.len(), 2);

        sink.connection
            .execute_batch(&format!(
                "DROP TABLE reorg; {}",
                super::create_table(Table::Reorg)
            ))
            .expect("remove the constraint");
        sink.flush().await.expect("retry the whole batch");
        assert_eq!(row_count(&sink, "reorg"), 2);
        assert_eq!(sink.rows.len(), 0);
    }

    /// A batch of several rows lands in one flush.
    #[tokio::test]
    async fn a_flush_writes_the_whole_batch() {
        let mut sink = sink();
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

        assert_eq!(row_count(&sink, "reorg"), 5);
    }

    /// A batch that holds a key twice keeps its last copy, a later flush of the same key
    /// updates the stored row rather than appending a second, and a replacement block has
    /// its own key so both reorg branches stay.
    #[tokio::test]
    async fn a_replay_updates_the_row_and_a_reorg_keeps_both_branches() {
        let mut sink = sink();
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

        assert_eq!(row_count(&sink, "log"), 1);
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
            row_count(&sink, "log"),
            1,
            "a replay never appends a second"
        );
        assert_eq!(
            log_timestamp(&sink),
            7,
            "the replay's value replaces the stored one"
        );

        // A replacement block's log is a different key, so the reorg's other branch stays.
        let mut replacement = orphaned;
        replacement.block_hash = hash(0xbb);
        replacement.block_timestamp = 9;
        sink.publish(Envelope::new(chain(), Event::Log(Box::new(replacement))))
            .await
            .expect("replacement buffers");
        sink.flush().await.expect("replacement is its own row");
        assert_eq!(row_count(&sink, "log"), 2);
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
        let row = row_for(&chain(), &event);

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

    fn log_timestamp(sink: &DuckDbSink) -> u64 {
        sink.connection
            .query_row("SELECT block_timestamp FROM log", [], |row| row.get(0))
            .expect("timestamp reads back")
    }
}
