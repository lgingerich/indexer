//! `PostgreSQL` 18 storage: the shared [`SqlStore`] over a `tokio-postgres` client.
//!
//! Tables live in a schema named for the chain, so chains sharing a database keep their
//! tables apart. A flush stages each table with binary `COPY`, which sends every value in
//! `PostgreSQL`'s native field format so the server parses no text, then merges it on
//! `(chain, dedupe_key)`, the primary key — the replica identity logical replication needs
//! to publish a delete. Unsigned 64-bit integers are `NUMERIC(20,0)`, since `PostgreSQL`
//! has no unsigned bigint, and wider integers `NUMERIC(78,0)`, exact to 256 bits.
//! Connection strings accept the driver's URL or keyword syntax; TLS uses platform
//! certificate validation and the connection's `sslmode`.

use std::error::Error;
use std::sync::Arc;

use alloy_primitives::U256;
use bytes::{Buf as _, BufMut, BytesMut};
use futures_util::pin_mut;
use postgres_native_tls::MakeTlsConnector;
use serde::Deserialize;
use tokio_postgres::Client;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{FromSql, IsNull, Kind, ToSql, Type, to_sql_checked};

use crate::config::Secret;
use crate::sink::sql::{self, Dialect, ident};
use crate::sink::store::{Engine, EngineError, Operation, SqlStore, StoreError};
use crate::sink::table::{ColumnType, Row, Schema, TableDef, Value};

/// `PostgreSQL` connection and backlog batching settings for `[sink.postgres]`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSettings {
    /// `PostgreSQL` URL or keyword connection string. Required; a [`Secret`], since it
    /// carries the password, so a deployment names it as `{ env = "NAME" }`.
    pub connection_string: Secret,
    /// Maximum records folded from queued blocks into one commit; blocks are never split.
    #[serde(default = "default_batch_records")]
    pub batch_records: usize,
}

const fn default_batch_records() -> usize {
    500
}

/// A store in `PostgreSQL`.
pub type PostgresSink = SqlStore<Postgres>;

/// A connected client, and the task driving its connection.
#[derive(Debug)]
pub struct Postgres {
    pub(crate) client: Client,
    connection_task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for Postgres {
    fn drop(&mut self) {
        if let Some(task) = &self.connection_task {
            task.abort();
        }
    }
}

impl PostgresSink {
    /// Connects using platform TLS, takes the single-writer lock on `database_schema`, and
    /// creates its tables before ingest starts.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when TLS, the connection, or table creation fails,
    /// and [`StoreError::Locked`] when another process holds the lock.
    pub async fn open(
        settings: &PostgresSettings,
        schema: Arc<Schema>,
        database_schema: &str,
    ) -> Result<Self, StoreError> {
        let open = || StoreError::engine(Operation::Open, None);
        let connector = MakeTlsConnector::new(
            native_tls::TlsConnector::new().map_err(|error| open()(error.into()))?,
        );
        let (client, connection) =
            tokio_postgres::connect(settings.connection_string.expose(), connector)
                .await
                .map_err(|error| open()(error.into()))?;
        let task = tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(%error, "PostgreSQL connection stopped");
            }
        });
        // One writer per database schema, held for the life of this connection.
        let locked: bool = client
            .query_one(
                "SELECT pg_try_advisory_lock(hashtext('indexer'), hashtext($1))",
                &[&database_schema],
            )
            .await
            .and_then(|row| row.try_get(0))
            .map_err(|error| open()(error.into()))?;
        if !locked {
            task.abort();
            return Err(StoreError::Locked {
                schema: database_schema.to_owned(),
            });
        }
        let engine = Postgres {
            client,
            connection_task: Some(task),
        };
        let store = SqlStore::new(engine, schema, database_schema).await?;
        tracing::info!("PostgreSQL storage opened");
        Ok(store)
    }

    /// Takes a connected client, whose connection future the caller is already driving,
    /// and creates every table in `schema` in the database schema `database_schema`.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when schema or table creation fails.
    pub async fn connected(
        client: Client,
        schema: Arc<Schema>,
        database_schema: &str,
    ) -> Result<Self, StoreError> {
        let engine = Postgres {
            client,
            connection_task: None,
        };
        SqlStore::new(engine, schema, database_schema).await
    }
}

impl Dialect for Postgres {
    const INDEXES: bool = true;
    const DESCRIBE: &'static str = "SELECT attname::TEXT, format_type(atttypid, atttypmod) \
         FROM pg_attribute WHERE attrelid = to_regclass(quote_ident($1)) AND attnum > 0 \
         AND NOT attisdropped ORDER BY attnum";

    fn type_name(kind: ColumnType) -> String {
        match kind {
            ColumnType::Uint => "NUMERIC(20,0)",
            ColumnType::Int => "BIGINT",
            ColumnType::BigInt => "NUMERIC(78,0)",
            ColumnType::Text => "TEXT",
            ColumnType::Bool => "BOOLEAN",
            ColumnType::Timestamp => "TIMESTAMP",
            ColumnType::Document => "JSONB",
            ColumnType::List(element) => return format!("{}[]", Self::type_name(*element)),
        }
        .to_owned()
    }

    fn reported_type(kind: ColumnType) -> String {
        match kind {
            ColumnType::Uint => "numeric(20,0)",
            ColumnType::Int => "bigint",
            ColumnType::BigInt => "numeric(78,0)",
            ColumnType::Text => "text",
            ColumnType::Bool => "boolean",
            ColumnType::Timestamp => "timestamp without time zone",
            ColumnType::Document => "jsonb",
            ColumnType::List(element) => return format!("{}[]", Self::reported_type(*element)),
        }
        .to_owned()
    }

    fn use_schema(schema: &str) -> String {
        format!("SET search_path TO {}", ident(schema))
    }

    fn create_staging(table: &TableDef, staging: &str) -> String {
        format!(
            "CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP",
            ident(staging),
            ident(&table.name)
        )
    }

    fn drop_staging(_: &str) -> Option<String> {
        None
    }
}

/// The wire type a column of `kind` is copied as.
fn pg_type(kind: ColumnType) -> Type {
    match kind {
        ColumnType::Uint | ColumnType::BigInt => Type::NUMERIC,
        ColumnType::Int => Type::INT8,
        ColumnType::Text => Type::TEXT,
        ColumnType::Bool => Type::BOOL,
        ColumnType::Timestamp => Type::TIMESTAMP,
        ColumnType::Document => Type::JSONB,
        // A list's element is a scalar (see `ColumnType::list`), so this is one dimension.
        ColumnType::List(element) => match pg_type(*element) {
            Type::INT8 => Type::INT8_ARRAY,
            Type::TEXT => Type::TEXT_ARRAY,
            Type::BOOL => Type::BOOL_ARRAY,
            Type::TIMESTAMP => Type::TIMESTAMP_ARRAY,
            Type::JSONB => Type::JSONB_ARRAY,
            _ => Type::NUMERIC_ARRAY,
        },
    }
}

/// Parameters as the driver takes them.
fn params(values: &[Value]) -> Vec<&(dyn ToSql + Sync)> {
    values
        .iter()
        .map(|value| value as &(dyn ToSql + Sync))
        .collect()
}

impl Engine for Postgres {
    async fn execute(&mut self, sql: &str, values: &[Value]) -> Result<(), EngineError> {
        if values.is_empty() {
            self.client.batch_execute(sql).await?;
        } else {
            self.client.execute(sql, &params(values)).await?;
        }
        Ok(())
    }

    async fn query(&mut self, sql: &str, values: &[Value]) -> Result<Vec<Vec<Value>>, EngineError> {
        let rows = self.client.query(sql, &params(values)).await?;
        rows.iter()
            .map(|row| {
                (0..row.len())
                    .map(|index| Ok(row.try_get(index)?))
                    .collect()
            })
            .collect()
    }

    async fn load(
        &mut self,
        table: &TableDef,
        staging: &str,
        rows: &[&Row],
    ) -> Result<(), EngineError> {
        let statement = format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT binary)",
            ident(staging),
            sql::column_list(table)
        );
        let types: Vec<Type> = table
            .columns
            .iter()
            .map(|column| pg_type(column.kind))
            .collect();
        let writer = BinaryCopyInWriter::new(self.client.copy_in(&statement).await?, &types);
        pin_mut!(writer);
        for row in rows {
            writer.as_mut().write_raw(row.values()).await?;
        }
        writer.as_mut().finish().await?;
        Ok(())
    }
}

/// Seconds from the Unix epoch to `PostgreSQL`'s, 2000-01-01.
const POSTGRES_EPOCH_SECONDS: i64 = 946_684_800;

/// `NUMERIC` binary layout: a sign and base-10_000 digits, most significant first.
///
/// A `uint256` needs at most twenty digits. Zero is the empty digit list `PostgreSQL`
/// expects.
fn write_numeric(negative: bool, magnitude: U256, out: &mut BytesMut) -> Result<(), String> {
    let base = U256::from(10_000_u16);
    let mut digits = Vec::new();
    let mut value = magnitude;
    while !value.is_zero() {
        let (quotient, remainder) = value.div_rem(base);
        digits.push(i16::try_from(remainder.to::<u16>()).map_err(|error| error.to_string())?);
        value = quotient;
    }
    let count = i16::try_from(digits.len()).map_err(|error| error.to_string())?;
    out.put_i16(count);
    out.put_i16((count - 1).max(0));
    out.put_i16(if negative && count > 0 { 0x4000 } else { 0 });
    out.put_i16(0);
    for digit in digits.iter().rev() {
        out.put_i16(*digit);
    }
    Ok(())
}

/// Reads an integral `NUMERIC`, the inverse of [`write_numeric`].
fn read_numeric(mut raw: &[u8]) -> Result<Value, Box<dyn Error + Sync + Send>> {
    if raw.len() < 8 {
        return Err("short NUMERIC".into());
    }
    let count = raw.get_i16();
    let weight = raw.get_i16();
    let sign = raw.get_u16();
    let _scale = raw.get_i16();
    if sign == 0xC000 {
        return Err("NUMERIC is NaN".into());
    }
    let base = U256::from(10_000_u16);
    let mut magnitude = U256::ZERO;
    for _ in 0..count {
        if raw.len() < 2 {
            return Err("short NUMERIC digits".into());
        }
        magnitude = magnitude * base + U256::from(u16::try_from(raw.get_i16())?);
    }
    // Trailing zero groups are omitted: the value is the digits times 10_000^(weight + 1
    // - count).
    for _ in 0..(i32::from(weight) + 1 - i32::from(count)).max(0) {
        magnitude *= base;
    }
    Ok(Value::BigInt {
        negative: sign == 0x4000,
        magnitude,
    })
}

/// One-dimensional array binary layout: dimensions, a null flag, the element type, the
/// length and lower bound, then each element as a length and its own binary form. An empty
/// array has no dimensions.
fn write_array(
    values: &[Value],
    element: &Type,
    out: &mut BytesMut,
) -> Result<(), Box<dyn Error + Sync + Send>> {
    out.put_i32(i32::from(!values.is_empty()));
    out.put_i32(i32::from(values.contains(&Value::Null)));
    out.put_u32(element.oid());
    if !values.is_empty() {
        out.put_i32(i32::try_from(values.len())?);
        out.put_i32(1);
    }
    for value in values {
        let start = out.len();
        out.put_i32(0);
        let length = match value.to_sql(element, out)? {
            IsNull::Yes => -1,
            IsNull::No => i32::try_from(out.len() - start - 4)?,
        };
        out[start..start + 4].copy_from_slice(&length.to_be_bytes());
    }
    Ok(())
}

impl ToSql for Value {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match (self, ty) {
            (Self::Null, _) => return Ok(IsNull::Yes),
            (Self::Uint(number), &Type::NUMERIC) => {
                write_numeric(false, U256::from(*number), out)?;
            }
            (
                Self::BigInt {
                    negative,
                    magnitude,
                },
                &Type::NUMERIC,
            ) => write_numeric(*negative, *magnitude, out)?,
            (Self::Int(number), &Type::INT8) => out.put_i64(*number),
            (Self::Text(text), &Type::TEXT) => out.extend_from_slice(text.as_bytes()),
            (Self::Bool(flag), &Type::BOOL) => out.put_u8(u8::from(*flag)),
            (Self::Timestamp(seconds), &Type::TIMESTAMP) => {
                let micros = (i64::try_from(*seconds)? - POSTGRES_EPOCH_SECONDS)
                    .checked_mul(1_000_000)
                    .ok_or("timestamp out of range")?;
                out.put_i64(micros);
            }
            (Self::Document(json), &Type::JSONB) => {
                out.put_u8(1);
                out.extend_from_slice(json.as_bytes());
            }
            (Self::List(values), ty) => match ty.kind() {
                Kind::Array(element) => write_array(values, element, out)?,
                _ => return Err(format!("cannot encode {self:?} as {ty}").into()),
            },
            _ => return Err(format!("cannot encode {self:?} as {ty}").into()),
        }
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match ty.kind() {
            Kind::Array(element) => <Self as ToSql>::accepts(element),
            _ => matches!(
                *ty,
                Type::NUMERIC
                    | Type::INT8
                    | Type::TEXT
                    | Type::BOOL
                    | Type::TIMESTAMP
                    | Type::JSONB
            ),
        }
    }

    to_sql_checked!();
}

/// What a startup read returns: the scalar columns the stores read back.
impl<'a> FromSql<'a> for Value {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(match *ty {
            Type::NUMERIC => read_numeric(raw)?,
            Type::INT8 => Self::Int(i64::from_sql(ty, raw)?),
            Type::TEXT | Type::VARCHAR => Self::Text(String::from_sql(ty, raw)?),
            Type::BOOL => Self::Bool(bool::from_sql(ty, raw)?),
            Type::TIMESTAMP => {
                let seconds =
                    i64::from_sql(&Type::INT8, raw)?.div_euclid(1_000_000) + POSTGRES_EPOCH_SECONDS;
                Self::Timestamp(u64::try_from(seconds)?)
            }
            _ => return Err(format!("cannot read {ty} as a value").into()),
        })
    }

    fn from_sql_null(_: &Type) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Self::Null)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(
            *ty,
            Type::NUMERIC | Type::INT8 | Type::TEXT | Type::VARCHAR | Type::BOOL | Type::TIMESTAMP
        )
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::B256;

    use crate::decode::StoredContract;
    use crate::sink::table::Table;
    use crate::sink::{EnvelopeSink as _, SinkError};
    use crate::wire::envelope::{ChainId, Envelope, Event, Reorg};

    use super::*;

    /// The dataset tables alone.
    fn datasets() -> Arc<Schema> {
        Arc::new(Schema::new().expect("the dataset tables"))
    }

    fn numeric_words(negative: bool, magnitude: U256) -> Vec<i16> {
        let mut out = BytesMut::new();
        write_numeric(negative, magnitude, &mut out).expect("a uint256 fits");
        out.chunks_exact(2)
            .map(|chunk| i16::from_be_bytes([chunk[0], chunk[1]]))
            .collect()
    }

    #[test]
    fn numeric_binary_keeps_zero_the_sign_and_the_full_256_bit_range() {
        assert_eq!(numeric_words(false, U256::ZERO), [0, 0, 0, 0]);
        assert_eq!(
            numeric_words(true, U256::ZERO),
            [0, 0, 0, 0],
            "no negative zero"
        );
        assert_eq!(
            numeric_words(false, U256::from(u64::MAX)),
            [5, 4, 0, 0, 1844, 6744, 737, 955, 1615]
        );
        assert_eq!(
            numeric_words(true, U256::from(12_345_u64)),
            [2, 1, 0x4000, 0, 1, 2345]
        );
        // 115792089237316195423570985008687907853269984665640564039457584007913129639935
        assert_eq!(
            numeric_words(false, U256::MAX),
            [
                20, 19, 0, 0, 11, 5792, 892, 3731, 6195, 4235, 7098, 5008, 6879, 785, 3269, 9846,
                6564, 564, 394, 5758, 4007, 9131, 2963, 9935
            ]
        );
    }

    #[test]
    fn numeric_binary_reads_back_what_it_wrote() {
        for (negative, magnitude) in [
            (false, U256::ZERO),
            (false, U256::from(10_000_u64)),
            (true, U256::from(12_345_u64)),
            (false, U256::MAX),
        ] {
            let mut out = BytesMut::new();
            write_numeric(negative, magnitude, &mut out).expect("a uint256 fits");
            assert_eq!(
                read_numeric(&out).expect("reads back"),
                Value::BigInt {
                    negative: negative && !magnitude.is_zero(),
                    magnitude
                }
            );
        }
    }

    /// Every table is keyed on its identity, and indexed and deleted by block exactly
    /// when a reorg retracts its rows.
    #[test]
    fn every_table_is_keyed_and_indexed_by_block_when_reorgs_delete_from_it() {
        let schema = crate::sink::fixtures::schema();
        assert!(
            schema
                .tables()
                .iter()
                .any(|table| table.name == "uniswap_v3_pool_swap"),
            "the shipped catalog's event tables are among them"
        );
        for table in schema.tables() {
            let label = &table.name;
            let ddl = sql::create_table::<Postgres>(table);
            assert!(
                ddl.contains("PRIMARY KEY (\"chain\", \"dedupe_key\")"),
                "{label} upserts on its identity and can publish a delete"
            );
            let retracted = table.id != Table::Reorg.into();
            assert_eq!(
                !sql::create_indexes::<Postgres>(table).is_empty(),
                retracted,
                "{label} is indexed by block exactly when a reorg deletes from it"
            );
            assert_eq!(sql::delete_block(table).is_some(), retracted);
            for column in &table.columns {
                let definition = format!(
                    "\"{}\" {}{}",
                    column.name,
                    Postgres::type_name(column.kind),
                    if column.nullable { "" } else { " NOT NULL" }
                );
                assert!(ddl.contains(&definition), "{definition} missing from {ddl}");
            }
        }
    }

    #[test]
    fn settings_require_a_connection_reject_typos_and_redact_credentials() {
        let settings: PostgresSettings =
            toml::from_str("connection_string = 'postgres://user:secret@localhost/indexer'")
                .expect("settings");
        assert_eq!(settings.batch_records, 500);
        assert!(!format!("{settings:?}").contains("secret"));
        assert!(toml::from_str::<PostgresSettings>("").is_err());
        assert!(
            toml::from_str::<PostgresSettings>(
                "connection_string = 'host=localhost'\nbatch_record = 1"
            )
            .is_err()
        );
    }

    fn db(sink: &PostgresSink) -> &Client {
        &sink.engine.client
    }

    /// A sink over the dataset tables only, for a test that decodes nothing.
    async fn dataset_sink(client: Client, database_schema: &str) -> PostgresSink {
        PostgresSink::connected(client, datasets(), database_schema)
            .await
            .expect("sink")
    }

    async fn count(sink: &PostgresSink, table: Table) -> i64 {
        db(sink)
            .query_one(&format!("SELECT count(*) FROM \"{table}\""), &[])
            .await
            .expect("count rows")
            .get(0)
    }

    /// A second writer of a database schema is refused while the first holds it.
    #[tokio::test]
    #[ignore = "requires INDEXER_TEST_POSTGRES_URL pointing to PostgreSQL 18"]
    async fn a_second_writer_of_a_schema_is_refused() {
        let url = std::env::var("INDEXER_TEST_POSTGRES_URL").expect("test database URL");
        let settings: PostgresSettings =
            toml::from_str(&format!("connection_string = '{url}'")).expect("settings");
        let schema = format!("lock_test_{}", std::process::id());
        let first = PostgresSink::open(&settings, datasets(), &schema)
            .await
            .expect("the first writer opens");
        assert!(matches!(
            PostgresSink::open(&settings, datasets(), &schema).await,
            Err(StoreError::Locked { .. })
        ));
        db(&first)
            .batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
            .await
            .expect("drop test schema");
    }

    /// A stored contract reads back scoped to its chain, and is deleted once a reorg
    /// orphans its creating block.
    #[tokio::test]
    #[ignore = "requires INDEXER_TEST_POSTGRES_URL pointing to PostgreSQL 18"]
    async fn stored_contracts_read_back_and_orphaned_ones_are_deleted() {
        use alloy_primitives::Address;

        use crate::wire::envelope::Contract;

        let url = std::env::var("INDEXER_TEST_POSTGRES_URL").expect("test database URL");
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .expect("connect");
        let task = tokio::spawn(connection);
        let schema = format!("contract_test_{}", std::process::id());
        client
            .batch_execute(&format!(
                "CREATE SCHEMA \"{schema}\"; SET search_path TO \"{schema}\""
            ))
            .await
            .expect("test schema");
        let mut sink = dataset_sink(client, &schema).await;
        let chain = ChainId::new("base");
        let contract = Contract {
            protocol: "uniswap_v3".to_owned(),
            name: "UniswapV3Pool".to_owned(),
            address: Address::from([0xd0; 20]),
            factory_address: Address::from([0xfa; 20]),
            transaction_hash: B256::with_last_byte(7),
            transaction_index: 3,
            log_index: 9,
            block_number: 51_000_000,
            block_hash: B256::with_last_byte(1),
            block_timestamp: 1_700_000_000,
        };
        for (chain, event) in [
            (chain.clone(), Event::Contract(Box::new(contract.clone()))),
            (
                ChainId::new("ethereum"),
                Event::Contract(Box::new(contract.clone())),
            ),
        ] {
            sink.publish(Envelope::new(chain, event))
                .await
                .expect("publish");
        }
        sink.flush().await.expect("commit");
        assert_eq!(
            sink.contracts(&chain).await.expect("read back"),
            [StoredContract {
                protocol: contract.protocol,
                name: contract.name,
                address: contract.address,
                block_hash: contract.block_hash,
            }]
        );

        let reorg = Reorg {
            height: 51_000_000,
            new_head_hash: B256::with_last_byte(2),
            orphaned_hashes: vec![B256::with_last_byte(1)],
        };
        sink.publish(Envelope::new(chain.clone(), Event::Reorg(reorg)))
            .await
            .expect("publish");
        sink.flush().await.expect("commit");
        assert!(sink.contracts(&chain).await.expect("read back").is_empty());
        assert_eq!(
            count(&sink, Table::Contract).await,
            1,
            "the other chain's contract stays"
        );

        db(&sink)
            .batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
            .await
            .expect("drop test schema");
        drop(sink);
        task.abort();
    }

    /// The ledger reads back the newest unorphaned accepted blocks of one chain, oldest
    /// first, and drops that chain's rows below them.
    #[tokio::test]
    #[ignore = "requires INDEXER_TEST_POSTGRES_URL pointing to PostgreSQL 18"]
    async fn the_ledger_reads_back_the_newest_canonical_window_and_prunes_below_it() {
        use crate::wire::envelope::AcceptedBlock;

        let url = std::env::var("INDEXER_TEST_POSTGRES_URL").expect("test database URL");
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .expect("connect");
        let task = tokio::spawn(connection);
        let schema = format!("ledger_test_{}", std::process::id());
        client
            .batch_execute(&format!(
                "CREATE SCHEMA \"{schema}\"; SET search_path TO \"{schema}\""
            ))
            .await
            .expect("test schema");
        let mut sink = dataset_sink(client, &schema).await;
        let chain = ChainId::new("base");
        let accepted = |height: u64| AcceptedBlock {
            height,
            hash: B256::with_last_byte(u8::try_from(height).expect("small heights")),
            parent_hash: B256::with_last_byte(u8::try_from(height - 1).expect("small heights")),
            timestamp: 1_700_000_000 + height,
        };
        assert!(sink.ledger(&chain, 3).await.expect("empty").is_empty());
        for height in 1..=10 {
            sink.publish(Envelope::new(
                chain.clone(),
                Event::AcceptedBlock(accepted(height)),
            ))
            .await
            .expect("publish");
        }
        sink.publish(Envelope::new(
            ChainId::new("ethereum"),
            Event::AcceptedBlock(accepted(1)),
        ))
        .await
        .expect("publish");
        sink.publish(Envelope::new(
            chain.clone(),
            Event::Reorg(Reorg {
                height: 10,
                new_head_hash: B256::with_last_byte(0xee),
                orphaned_hashes: vec![accepted(10).hash],
            }),
        ))
        .await
        .expect("publish");
        sink.flush().await.expect("commit");

        assert_eq!(
            sink.ledger(&chain, 3).await.expect("read back"),
            [accepted(7), accepted(8), accepted(9)]
        );
        assert_eq!(
            count(&sink, Table::AcceptedBlock).await,
            4,
            "7 through 9 on base, and ethereum's row; 10 was deleted"
        );
        assert_eq!(
            sink.ledger(&ChainId::new("ethereum"), 3)
                .await
                .expect("read back"),
            [accepted(1)]
        );

        db(&sink)
            .batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
            .await
            .expect("drop test schema");
        drop(sink);
        task.abort();
    }

    /// A reorg deletes its orphaned blocks' rows in every table — committed earlier or
    /// buffered in the same batch — and a block that returns is stored. Run with every
    /// table in a publication, which refuses a delete from a table without a replica
    /// identity, so it also proves the primary key is one.
    #[tokio::test]
    #[ignore = "requires INDEXER_TEST_POSTGRES_URL pointing to PostgreSQL 18"]
    async fn a_reorg_deletes_orphaned_blocks_under_a_publication() {
        use crate::wire::datasets::evm::{Block, Log};
        use crate::wire::envelope::AcceptedBlock;

        let url = std::env::var("INDEXER_TEST_POSTGRES_URL").expect("test database URL");
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .expect("connect");
        let task = tokio::spawn(connection);
        let schema = format!("reorg_test_{}", std::process::id());
        client
            .batch_execute(&format!(
                "CREATE SCHEMA \"{schema}\"; SET search_path TO \"{schema}\""
            ))
            .await
            .expect("test schema");
        let mut sink = dataset_sink(client, &schema).await;
        db(&sink)
            .batch_execute(&format!(
                "CREATE PUBLICATION \"{schema}\" FOR TABLES IN SCHEMA \"{schema}\""
            ))
            .await
            .expect("publication");
        let chain = ChainId::new("base");
        let block = |byte: u8| {
            let hash = B256::with_last_byte(byte);
            [
                Event::Block(Box::new(Block {
                    number: 100,
                    hash,
                    ..Block::default()
                })),
                Event::Log(Box::new(Log {
                    block_number: 100,
                    block_hash: hash,
                    ..Log::default()
                })),
                Event::AcceptedBlock(AcceptedBlock {
                    height: 100,
                    hash,
                    parent_hash: B256::ZERO,
                    timestamp: 0,
                }),
            ]
            .map(|event| Envelope::new(chain.clone(), event))
        };
        let reorg = |byte: u8| {
            Envelope::new(
                chain.clone(),
                Event::Reorg(Reorg {
                    height: 100,
                    new_head_hash: B256::with_last_byte(byte ^ 0xff),
                    orphaned_hashes: vec![B256::with_last_byte(byte)],
                }),
            )
        };
        let stored = async |sink: &PostgresSink| -> Vec<(String, String)> {
            db(sink)
                .query(
                    "SELECT 'blocks', hash FROM blocks UNION ALL \
                     SELECT 'logs', block_hash FROM logs UNION ALL \
                     SELECT 'accepted_blocks', hash FROM accepted_blocks ORDER BY 1",
                    &[],
                )
                .await
                .expect("rows")
                .iter()
                .map(|row| (row.get(0), row.get(1)))
                .collect()
        };
        let only = |byte: u8| {
            let hash = format!("{:#x}", B256::with_last_byte(byte));
            ["accepted_blocks", "blocks", "logs"].map(|table| (table.to_owned(), hash.clone()))
        };

        // A committed block, retracted by a later batch that also holds its replacement.
        for envelope in block(0xa1) {
            sink.publish(envelope).await.expect("publish");
        }
        sink.flush().await.expect("commit A");
        sink.publish(reorg(0xa1)).await.expect("publish");
        for envelope in block(0xb1) {
            sink.publish(envelope).await.expect("publish");
        }
        sink.flush().await.expect("commit the reorg");
        assert_eq!(stored(&sink).await, only(0xb1));

        // A' orphaned within one batch, and A canonical again after it.
        sink.publish(reorg(0xb1)).await.expect("publish");
        for envelope in block(0xa1) {
            sink.publish(envelope).await.expect("publish");
        }
        sink.flush().await.expect("commit the return");
        assert_eq!(stored(&sink).await, only(0xa1));
        assert_eq!(count(&sink, Table::Reorg).await, 2);

        db(&sink)
            .batch_execute(&format!(
                "DROP PUBLICATION \"{schema}\"; DROP SCHEMA \"{schema}\" CASCADE"
            ))
            .await
            .expect("drop test schema");
        drop(sink);
        task.abort();
    }

    /// A decoded array of scalars lands as a native array of its element type, and a
    /// tuple as one `JSONB` object keyed by its ABI names with every integer a decimal
    /// string, as Allium's decoded `params` are.
    #[tokio::test]
    #[ignore = "requires INDEXER_TEST_POSTGRES_URL pointing to PostgreSQL 18"]
    async fn an_array_is_a_typed_array_and_a_tuple_one_object() {
        let url = std::env::var("INDEXER_TEST_POSTGRES_URL").expect("test database URL");
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .expect("connect");
        let task = tokio::spawn(connection);
        let schema = format!("array_test_{}", std::process::id());
        client
            .batch_execute(&format!(
                "CREATE SCHEMA \"{schema}\"; SET search_path TO \"{schema}\""
            ))
            .await
            .expect("test schema");
        let mut sink = PostgresSink::connected(client, crate::sink::fixtures::schema(), &schema)
            .await
            .expect("sink");
        let created = crate::sink::fixtures::decoded_pool_created();
        sink.publish(Envelope::new(
            ChainId::new("base"),
            Event::Decoded(Box::new(created)),
        ))
        .await
        .expect("publish");
        sink.flush().await.expect("commit");

        let row = db(&sink)
            .query_one(
                "SELECT extensions, pg_typeof(negative_bin_data_array)::TEXT, \
                 negative_bin_data_array[1]::TEXT, cardinality(negative_bin_data_array), \
                 (extension_orders->>'afterSwap')::NUMERIC::TEXT, \
                 extension_orders->>'beforeSwap', price_provider_timelock::TEXT \
                 FROM metric_v1_factory_pool_created",
                &[],
            )
            .await
            .expect("the pool reads back");
        assert_eq!(
            row.get::<_, Vec<String>>(0),
            [
                "0xb1a246b1131ff328067c4aaf4f772ff351475244",
                "0xe4038fa09f0bb9963068afaf97be0c045155090d",
                "0xebc53e61078976118e384f110c262a263decb84b",
            ]
        );
        assert_eq!(row.get::<_, String>(1), "numeric[]");
        assert_eq!(
            row.get::<_, String>(2),
            "2510840694154681225832395181564014445733957084757624461722000"
        );
        assert_eq!(row.get::<_, i32>(3), 4);
        assert_eq!(row.get::<_, String>(4), "10");
        assert_eq!(row.get::<_, String>(5), "3");
        assert_eq!(row.get::<_, String>(6), U256::MAX.to_string());

        db(&sink)
            .batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
            .await
            .expect("drop test schema");
        drop(sink);
        task.abort();
    }

    /// A decoded record lands in its event's table with exact `NUMERIC` and `BIGINT`
    /// columns, and a reorg deletes it.
    #[tokio::test]
    #[ignore = "requires INDEXER_TEST_POSTGRES_URL pointing to PostgreSQL 18"]
    async fn a_decoded_record_lands_in_its_typed_event_table() {
        let url = std::env::var("INDEXER_TEST_POSTGRES_URL").expect("test database URL");
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .expect("connect");
        let task = tokio::spawn(connection);
        let schema = format!("event_test_{}", std::process::id());
        client
            .batch_execute(&format!(
                "CREATE SCHEMA \"{schema}\"; SET search_path TO \"{schema}\""
            ))
            .await
            .expect("test schema");
        let mut sink = PostgresSink::connected(client, crate::sink::fixtures::schema(), &schema)
            .await
            .expect("sink");
        let chain = ChainId::new("base");
        let swap = crate::sink::fixtures::decoded_swap();
        let block = swap.block_hash;
        sink.publish(Envelope::new(chain.clone(), Event::Decoded(Box::new(swap))))
            .await
            .expect("publish");
        sink.flush().await.expect("commit");

        let row = db(&sink)
            .query_one(
                "SELECT amount0::TEXT, amount1::TEXT, sqrt_price_x96::TEXT, liquidity::TEXT, \
                 tick, sender, (sqrt_price_x96 * 1000000)::TEXT FROM uniswap_v3_pool_swap",
                &[],
            )
            .await
            .expect("the swap reads back");
        assert_eq!(row.get::<_, String>(0), "-3180585820646654");
        assert_eq!(row.get::<_, String>(1), "8586564");
        assert_eq!(row.get::<_, String>(2), "4115542941155561242646778");
        assert_eq!(row.get::<_, String>(3), "1367693693161775005");
        assert_eq!(row.get::<_, i64>(4), -197_317);
        assert_eq!(
            row.get::<_, String>(5),
            "0x6ff5693b99212da76ad316178a184ab56d299b43"
        );
        assert_eq!(
            row.get::<_, String>(6),
            "4115542941155561242646778000000",
            "exact arithmetic"
        );

        sink.publish(Envelope::new(
            chain,
            Event::Reorg(Reorg {
                height: 1,
                new_head_hash: B256::with_last_byte(0xee),
                orphaned_hashes: vec![block],
            }),
        ))
        .await
        .expect("publish");
        sink.flush().await.expect("commit the reorg");
        let remaining: i64 = db(&sink)
            .query_one("SELECT count(*) FROM uniswap_v3_pool_swap", &[])
            .await
            .expect("count")
            .get(0);
        assert_eq!(remaining, 0, "the reorg deleted the typed row");

        db(&sink)
            .batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
            .await
            .expect("drop test schema");
        drop(sink);
        task.abort();
    }

    /// Run against an isolated `PostgreSQL` 18 database with `INDEXER_TEST_POSTGRES_URL`.
    /// Tables are created in a schema named for this process and dropped afterward.
    #[tokio::test]
    #[ignore = "requires INDEXER_TEST_POSTGRES_URL pointing to PostgreSQL 18"]
    async fn copy_is_atomic_retains_failed_batches_and_upserts_a_replay() {
        let url = std::env::var("INDEXER_TEST_POSTGRES_URL").expect("test database URL");
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .expect("connect");
        let task = tokio::spawn(connection);
        let schema = format!("sink_test_{}", std::process::id());
        client
            .batch_execute(&format!(
                "CREATE SCHEMA \"{schema}\"; SET search_path TO \"{schema}\""
            ))
            .await
            .expect("test schema");
        assert_eq!(
            client
                .query_one("SHOW server_version_num", &[])
                .await
                .expect("version")
                .get::<_, String>(0)
                .parse::<u32>()
                .expect("version number")
                / 10_000,
            18
        );
        let mut sink = dataset_sink(client, &schema).await;
        sink.flush().await.expect("empty flush");
        let chain = ChainId::new("tab\tnewline\nslash\\unicodeé");
        for height in [1, u64::MAX] {
            let reorg = Reorg {
                height,
                new_head_hash: B256::ZERO,
                orphaned_hashes: vec![B256::ZERO],
            };
            let envelope = Envelope::new(chain.clone(), Event::Reorg(reorg));
            sink.publish(envelope).await.expect("publish");
        }
        assert_eq!(count(&sink, Table::Reorg).await, 0);
        sink.flush().await.expect("commit");
        let rows = db(&sink)
            .query("SELECT height::text, chain FROM reorgs", &[])
            .await
            .expect("rows");
        assert_eq!(rows.len(), 1, "a repeated key in one batch is one row");
        let height: String = rows[0].get(0);
        assert_eq!(height, u64::MAX.to_string(), "last copy wins");
        assert_eq!(rows[0].get::<_, String>(1), chain.as_str());
        let rejected = Event::Reorg(Reorg {
            height: 42,
            new_head_hash: B256::ZERO,
            orphaned_hashes: vec![B256::ZERO],
        });
        sink.publish(Envelope::new(chain, rejected))
            .await
            .expect("reorg");
        db(&sink)
            .batch_execute(
                "ALTER TABLE reorgs ADD CONSTRAINT reject_height CHECK (height = 0) NOT VALID",
            )
            .await
            .expect("constraint");
        assert!(matches!(
            sink.flush().await,
            Err(SinkError::Store(StoreError::Engine { .. }))
        ));
        assert_eq!(sink.buffered(), 1);
        assert_eq!(count(&sink, Table::Reorg).await, 1);
        db(&sink)
            .batch_execute("ALTER TABLE reorgs DROP CONSTRAINT reject_height")
            .await
            .expect("remove constraint");
        sink.flush().await.expect("retry");
        assert!(sink.buffered() == 0);
        assert_eq!(count(&sink, Table::Reorg).await, 1);
        assert_eq!(
            db(&sink)
                .query_one("SELECT height::text FROM reorgs", &[])
                .await
                .expect("updated row")
                .get::<_, String>(0),
            "42",
            "the retried replay updates the stored row"
        );
        let hashes: serde_json::Value = serde_json::from_str(
            &db(&sink)
                .query_one("SELECT orphaned_hashes::text FROM reorgs", &[])
                .await
                .expect("JSONB")
                .get::<_, String>(0),
        )
        .expect("JSON");
        assert_eq!(hashes, serde_json::json!([format!("{:#x}", B256::ZERO)]));
        db(&sink)
            .batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
            .await
            .expect("drop test schema");
        drop(sink);
        task.await
            .expect("connection task")
            .expect("connection closes");
    }
}
