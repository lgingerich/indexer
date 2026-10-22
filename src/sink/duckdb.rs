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
    // is a table there, replaced when the store opens and emptied after each merge.
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
    use std::sync::Arc;

    use alloy_primitives::U256;
    use duckdb::types::FromSql;
    use duckdb::{Connection, appender_params_from_iter};

    use crate::sink::StoreError;
    use crate::sink::store::Engine as _;
    use crate::sink::store::contract::{Harness, commit, datasets, every_kind, one_unless_decoded};
    use crate::sink::table::{Schema, Table, Value};
    use crate::wire::envelope::{ChainId, Envelope, Event};

    use super::{DuckDb, DuckDbSink};

    /// An in-memory database, and the test's own handle to it, with the store's schema
    /// as its default.
    struct Duck {
        reader: DuckDb,
    }

    impl Harness for Duck {
        type Engine = DuckDb;

        async fn open(schema: Arc<Schema>) -> (DuckDbSink, Self) {
            let connection = memory();
            let reader = connection.try_clone().expect("a second handle");
            let sink = connect(connection, schema, "base").await;
            reader
                .execute_batch("USE base")
                .expect("the store's schema");
            (
                sink,
                Self {
                    reader: DuckDb { connection: reader },
                },
            )
        }

        fn chain(&self) -> ChainId {
            ChainId::new("base")
        }

        async fn query(&mut self, sql: &str) -> Vec<Vec<Value>> {
            self.reader.query(sql, &[]).await.expect(sql)
        }

        async fn execute(&mut self, sql: &str) {
            self.reader.connection.execute_batch(sql).expect(sql);
        }

        async fn remove(self) {}
    }

    mod contract {
        crate::sink::store::contract::contract_tests!(super::Duck);
    }

    fn memory() -> Connection {
        Connection::open_in_memory().expect("open in-memory DuckDB")
    }

    /// A store of `chain` over `connection`, with the tables of `schema`.
    async fn connect(connection: Connection, schema: Arc<Schema>, chain: &str) -> DuckDbSink {
        DuckDbSink::new(DuckDb { connection }, schema, chain)
            .await
            .expect("create the tables")
    }

    /// The `reorgs` table's DDL.
    fn reorg_ddl() -> String {
        crate::sink::sql::create_table::<DuckDb>(datasets().dataset(Table::Reorg))
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

    /// Two chains sharing a database write to their own schemas, so each keeps its own
    /// tables.
    #[tokio::test]
    async fn each_chain_writes_to_its_own_schema() {
        let connection = memory();
        for name in ["base", "ethereum"] {
            let handle = connection.try_clone().expect("a second handle");
            let mut sink = connect(handle, datasets(), name).await;
            let block = every_kind(&ChainId::new(name)).remove(0);
            commit(&mut sink, [block]).await;
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
            let error = DuckDbSink::new(DuckDb { connection }, datasets(), "base")
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
        let mut sink = connect(open(), crate::sink::fixtures::schema(), "base").await;
        commit(&mut sink, every_kind(&ChainId::new("base"))).await;
        drop(sink);

        let connection = open();
        let reader = connection.try_clone().expect("a second handle");
        let sink = connect(connection, crate::sink::fixtures::schema(), "base").await;
        for table in Table::ALL {
            assert_eq!(
                column::<i64>(&reader, &format!("SELECT count(*) FROM base.{table}")),
                [one_unless_decoded(table)],
                "{table} keeps its rows"
            );
        }
        // Leave nothing behind; the file is released once both handles drop.
        drop((sink, reader));
        std::fs::remove_file(&path).expect("remove temp database");
    }

    /// A decoded array of scalars lands as a native list of its element type, and a tuple
    /// as one JSON object keyed by its ABI names with every integer a decimal string, as
    /// Allium's decoded `params` are.
    #[tokio::test]
    async fn an_array_is_a_typed_list_and_a_tuple_one_object() {
        let (mut sink, harness) = Duck::open(crate::sink::fixtures::schema()).await;
        let created = crate::sink::fixtures::decoded_pool_created();
        commit(
            &mut sink,
            [Envelope::new(
                harness.chain(),
                Event::Decoded(Box::new(created)),
            )],
        )
        .await;

        let (extensions, words, word, orders, after_swap, timelock): (
            String,
            String,
            String,
            String,
            String,
            String,
        ) = harness
            .reader
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
}
