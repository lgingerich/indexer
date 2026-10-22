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
use std::time::Duration;

use alloy_primitives::U256;
use bytes::{Buf as _, BufMut, BytesMut};
use futures_util::pin_mut;
use postgres_native_tls::MakeTlsConnector;
use serde::Deserialize;
use tokio_postgres::Client;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{FromSql, IsNull, Kind, ToSql, Type, to_sql_checked};

use crate::metrics;
use crate::secret::Secret;
use crate::sink::channel::Batching;
use crate::sink::route::{Backend, Tables};
use crate::sink::sql::{self, Dialect, ident};
use crate::sink::store::{Engine, SqlStore};
use crate::sink::store_error::{EngineError, Operation, StoreError};
use crate::sink::table::{ColumnType, Row, Schema, TableDef, Value};

/// `PostgreSQL` connection and backlog batching settings for `[sink.postgres]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSettings {
    /// The tables this store writes. Required.
    pub tables: Tables,
    /// `PostgreSQL` URL or keyword connection string. Required; a [`Secret`], since it
    /// carries the password, so a deployment names it as `{ env = "NAME" }`.
    pub connection_string: Secret,
    /// Maximum records folded from queued blocks into one commit; blocks are never split.
    #[serde(default = "default_batch_records")]
    pub batch_records: usize,
    /// How long, in milliseconds, a block at the tip may wait for its commit so the blocks
    /// after it share that commit. Zero, the default, commits each block as it arrives.
    #[serde(default)]
    pub commit_wait_ms: u64,
}

impl PostgresSettings {
    /// When the store commits: up to [`batch_records`](Self::batch_records) records, as
    /// soon as they are gathered or once the oldest block has waited
    /// [`commit_wait_ms`](Self::commit_wait_ms).
    pub(crate) const fn batching(&self) -> Batching {
        Batching {
            wait: Duration::from_millis(self.commit_wait_ms),
            ..Batching::eager(self.batch_records)
        }
    }
}

/// Twenty thousand: a backlog then commits in batches that take about half a second,
/// where each commit's fixed cost — an fsync, a staging table per table, a dozen round
/// trips — no longer dominates. At the tip it changes nothing: a block commits as soon
/// as it arrives, alone.
const fn default_batch_records() -> usize {
    20_000
}

/// Session settings applied on open: server-side TCP keepalives, and a statement
/// timeout well above a commit's half second. Startup's table and index creation runs
/// without it (see [`Dialect::UNTIMED`]), since an index build has no such bound.
const SESSION_SETTINGS: &str = "SET tcp_keepalives_idle = 30; \
     SET tcp_keepalives_interval = 10; \
     SET tcp_keepalives_count = 3; \
     SET statement_timeout = '5min'";

/// A store in `PostgreSQL`.
pub type PostgresSink = SqlStore<Postgres>;

/// How long a store keeps trying to connect again once the server closes the
/// connection: longer than the server's keepalives take to end a session whose client
/// vanished and so release the writer lock it held (see [`SESSION_SETTINGS`]). Then the
/// error stands and the process stops, as it would have.
const RECONNECT_FOR: Duration = Duration::from_mins(2);

/// The first wait between attempts to connect again, doubled after each failed one up to
/// [`MAX_RECONNECT_DELAY`].
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// The longest wait between attempts to connect again.
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(15);

/// How long a failed statement waits for its connection to end before the failure is
/// taken for one of the statement's own. The driver fails the statement as the
/// connection breaks, a moment before the connection's task ends.
const CLOSE_WAIT: Duration = Duration::from_secs(1);

/// A connected client, and the task driving its connection.
#[derive(Debug)]
pub struct Postgres {
    client: Client,
    connection_task: tokio::task::JoinHandle<()>,
    /// How to connect again when the server closes the connection. Boxed: the parsed
    /// settings are most of the store's size, and read only on a reconnect.
    reconnect: Box<Reconnect>,
}

/// What connecting again needs: the parsed connection settings, and the chain whose
/// database schema the store writes and whose writer lock it holds.
#[derive(Debug)]
struct Reconnect {
    config: tokio_postgres::Config,
    chain: String,
}

impl Drop for Postgres {
    fn drop(&mut self) {
        self.connection_task.abort();
    }
}

impl PostgresSink {
    /// Connects using platform TLS, takes the single-writer lock on the database schema
    /// named for `chain`, and creates its tables before ingest starts.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when TLS, the connection, or table creation fails,
    /// and [`StoreError::Locked`] when another process holds the lock.
    pub async fn open(
        settings: &PostgresSettings,
        schema: Arc<Schema>,
        chain: &str,
    ) -> Result<Self, StoreError> {
        let mut config: tokio_postgres::Config = settings
            .connection_string
            .expose()
            .parse()
            .map_err(opening)?;
        // Probe an idle connection after 30 seconds, every 10, and drop it after 3
        // misses, so a dead server fails this connection in about a minute, not two hours.
        config
            .keepalives_idle(Duration::from_secs(30))
            .keepalives_interval(Duration::from_secs(10))
            .keepalives_retries(3);
        let (client, task) = connect_as_writer(&config, chain).await?;
        let engine = Postgres {
            client,
            connection_task: task,
            reconnect: Box::new(Reconnect {
                config,
                chain: chain.to_owned(),
            }),
        };
        let store = SqlStore::new(engine, schema, chain).await?;
        tracing::info!("PostgreSQL storage opened");
        Ok(store)
    }
}

/// Connects with `config` and takes the writer lock on the database schema named for
/// `chain`, returning the client and the task driving its connection. The lock is the
/// session's, so every new session takes it here, before it can write.
///
/// The connection uses platform TLS and [`SESSION_SETTINGS`].
///
/// # Errors
///
/// Returns [`StoreError::Engine`] when TLS, the connection, or a setting fails, and
/// [`StoreError::Locked`] when another session holds the lock.
async fn connect_as_writer(
    config: &tokio_postgres::Config,
    chain: &str,
) -> Result<(Client, tokio::task::JoinHandle<()>), StoreError> {
    let connector = MakeTlsConnector::new(
        native_tls::TlsConnector::new().map_err(StoreError::engine(Operation::Open, None))?,
    );
    let (client, connection) = config.connect(connector).await.map_err(opening)?;
    let task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            // The driver shows a server's error as `db error` alone; what the server said,
            // such as a FATAL `57P01` for a shutdown, is in its fields.
            if let Some(db) = error.as_db_error() {
                tracing::error!(
                    code = db.code().code(),
                    severity = db.severity(),
                    message = db.message(),
                    "PostgreSQL connection stopped"
                );
            } else {
                tracing::error!(error = %PgError(error), "PostgreSQL connection stopped");
            }
        }
    });
    // The same probes from the server's side, so a session whose process was killed
    // ends within a minute and releases the writer lock below. A statement stuck
    // behind such a session's locks fails after five minutes rather than hanging.
    if let Err(error) = client.batch_execute(SESSION_SETTINGS).await {
        task.abort();
        return Err(opening(error));
    }
    // One writer per database schema, held for the life of this connection.
    let locked: Result<bool, _> = client
        .query_one(
            "SELECT pg_try_advisory_lock(hashtext('indexer'), hashtext($1))",
            &[&chain],
        )
        .await
        .and_then(|row| row.try_get(0));
    match locked {
        Ok(true) => Ok((client, task)),
        Ok(false) => {
            task.abort();
            Err(StoreError::Locked {
                schema: chain.to_owned(),
            })
        }
        Err(error) => {
            task.abort();
            Err(opening(error))
        }
    }
}

impl Postgres {
    /// Connects again with `reconnect`, takes the writer lock again, and makes the chain's
    /// database schema the session's default, as opening the store did. The tables are
    /// already there.
    async fn reconnect(&mut self) -> Result<(), StoreError> {
        let Reconnect { config, chain } = &*self.reconnect;
        let (client, task) = connect_as_writer(config, chain).await?;
        if let Err(error) = client.batch_execute(&Self::use_schema(chain)).await {
            task.abort();
            return Err(opening(error));
        }
        self.client = client;
        std::mem::replace(&mut self.connection_task, task).abort();
        Ok(())
    }

    /// Whether the connection ended, waiting up to [`CLOSE_WAIT`] for it to.
    async fn closed(&self) -> bool {
        let deadline = tokio::time::Instant::now() + CLOSE_WAIT;
        while !self.client.is_closed() {
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        true
    }
}

/// A `tokio-postgres` error, shown with its cause. The driver's own `Display` names only
/// the kind — `db error`, `error communicating with the server` — and leaves what the
/// server said, or the I/O error, to its source.
#[derive(Debug)]
struct PgError(tokio_postgres::Error);

impl std::fmt::Display for PgError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)?;
        if let Some(db) = self.0.as_db_error() {
            write!(
                f,
                ": {} {}: {}",
                db.severity(),
                db.code().code(),
                db.message()
            )
        } else if let Some(source) = self.0.source() {
            write!(f, ": {source}")
        } else {
            Ok(())
        }
    }
}

// No `source`: `Display` already shows it, and a caller printing the chain would show it
// twice.
impl Error for PgError {}

/// A driver error while opening the store or connecting again.
fn opening(error: tokio_postgres::Error) -> StoreError {
    StoreError::engine(Operation::Open, None)(pg(error))
}

/// A driver error as an engine error that shows its cause.
fn pg(error: tokio_postgres::Error) -> EngineError {
    Box::new(PgError(error))
}

impl Dialect for Postgres {
    const INDEXES: bool = true;
    const UNTIMED: Option<&'static str> = Some("SET LOCAL statement_timeout = 0");
    const DESCRIBE: &'static str = "SELECT c.relname::TEXT, a.attname::TEXT, \
         format_type(a.atttypid, a.atttypmod) FROM pg_attribute a \
         JOIN pg_class c ON c.oid = a.attrelid \
         WHERE c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema()) \
         AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped \
         ORDER BY c.relname, a.attnum";

    fn any_of(column: &str, param: &str) -> String {
        format!("{column} = ANY({param})")
    }

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

    /// The type name, which the startup check compares without case, except that the
    /// server spells `TIMESTAMP` out in full.
    fn reported_type(kind: ColumnType) -> String {
        match kind {
            ColumnType::Timestamp => "timestamp without time zone".to_owned(),
            ColumnType::List(element) => format!("{}[]", Self::reported_type(*element)),
            other => Self::type_name(other),
        }
    }

    fn use_schema(schema: &str) -> String {
        format!("SET search_path TO {}", ident(schema))
    }

    /// A temp table emptied by a `DELETE` after its merge, not `ON COMMIT DELETE ROWS`: a
    /// commit that touched any temp table truncates every such table in the session,
    /// rebuilding each one's toast index and dropping its cached catalog entry, which the
    /// next use reads back from `pg_class`, `pg_attribute`, and `pg_index`.
    fn create_staging(table: &TableDef, staging: &str) -> String {
        format!(
            "CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS)",
            ident(staging),
            ident(&table.name)
        )
    }

    /// Autovacuum never visits a temp table, so the session reclaims what its deletes left.
    fn vacuum_staging(staging: &[&str]) -> Option<String> {
        let tables = staging.iter().map(|name| ident(name)).collect::<Vec<_>>();
        Some(format!("VACUUM {}", tables.join(", ")))
    }
}

/// The wire type a column of `kind` is copied as.
///
/// # Errors
///
/// Fails for a list of lists, which [`ColumnType::list`] never builds and no
/// one-dimensional array holds, rather than copying it as some other type.
fn pg_type(kind: ColumnType) -> Result<Type, EngineError> {
    Ok(match kind {
        ColumnType::Uint | ColumnType::BigInt => Type::NUMERIC,
        ColumnType::Int => Type::INT8,
        ColumnType::Text => Type::TEXT,
        ColumnType::Bool => Type::BOOL,
        ColumnType::Timestamp => Type::TIMESTAMP,
        ColumnType::Document => Type::JSONB,
        // A list's element is a scalar (see `ColumnType::list`), so this is one dimension.
        ColumnType::List(element) => match *element {
            ColumnType::Uint | ColumnType::BigInt => Type::NUMERIC_ARRAY,
            ColumnType::Int => Type::INT8_ARRAY,
            ColumnType::Text => Type::TEXT_ARRAY,
            ColumnType::Bool => Type::BOOL_ARRAY,
            ColumnType::Timestamp => Type::TIMESTAMP_ARRAY,
            ColumnType::Document => Type::JSONB_ARRAY,
            ColumnType::List(_) => return Err(format!("cannot copy a {kind:?} column").into()),
        },
    })
}

/// Parameters as the driver takes them.
fn params(values: &[Value]) -> Vec<&(dyn ToSql + Sync)> {
    values
        .iter()
        .map(|value| value as &(dyn ToSql + Sync))
        .collect()
}

impl Engine for Postgres {
    type Dialect = Self;

    async fn execute(&mut self, sql: &str, values: &[Value]) -> Result<(), EngineError> {
        if values.is_empty() {
            self.client.batch_execute(sql).await.map_err(pg)?;
        } else {
            self.client
                .execute(sql, &params(values))
                .await
                .map_err(pg)?;
        }
        Ok(())
    }

    async fn query(&mut self, sql: &str, values: &[Value]) -> Result<Vec<Vec<Value>>, EngineError> {
        let rows = self.client.query(sql, &params(values)).await.map_err(pg)?;
        rows.iter()
            .map(|row| {
                (0..row.len())
                    .map(|index| row.try_get(index).map_err(pg))
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
            .collect::<Result<_, _>>()?;
        let sink = self.client.copy_in(&statement).await.map_err(pg)?;
        let writer = BinaryCopyInWriter::new(sink, &types);
        pin_mut!(writer);
        for row in rows {
            writer.as_mut().write_raw(row.values()).await.map_err(pg)?;
        }
        writer.as_mut().finish().await.map_err(pg)?;
        Ok(())
    }

    /// When the server closed the connection — a restart, a failover, an operator ending
    /// the session — connects again, retrying for about two minutes (see
    /// `RECONNECT_FOR`). The failed transaction ended with the session, so the store
    /// is as its last commit left it, and the batch it kept can be written again.
    async fn recover(&mut self) -> Result<bool, StoreError> {
        if !self.closed().await {
            return Ok(false);
        }
        tracing::warn!("PostgreSQL connection lost; connecting again");
        let deadline = tokio::time::Instant::now() + RECONNECT_FOR;
        let mut delay = RECONNECT_DELAY;
        loop {
            match self.reconnect().await {
                Ok(()) => {
                    metrics::store_reconnected(Backend::Postgres.name());
                    tracing::info!("PostgreSQL connection restored; retrying the commit");
                    return Ok(true);
                }
                Err(error) => {
                    if tokio::time::Instant::now() + delay > deadline {
                        return Err(error);
                    }
                    tracing::warn!(%error, retry_in_ms = delay.as_millis(),
                        "PostgreSQL could not connect again");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(MAX_RECONNECT_DELAY);
                }
            }
        }
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
mod tests;
