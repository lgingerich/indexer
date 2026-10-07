//! `PostgreSQL` 18 storage using the shared typed rows and transactional binary `COPY`.
//!
//! Each flush loads all buffered datasets in one transaction. Binary `COPY` sends each
//! value in `PostgreSQL`'s native field format, so the server does not parse text, into a
//! temporary table that is then upserted on `(chain, dedupe_key)`. Only the last buffered
//! copy of a repeated key is loaded, since `PostgreSQL` refuses to touch one conflict row
//! twice in a statement. Unsigned integers use `NUMERIC(20,0)` (`PostgreSQL` has no
//! unsigned bigint), documents use `JSONB`, and hex values remain `TEXT`. Columns the
//! chain always provides are `NOT NULL`, and `(chain, dedupe_key)` is unique on each
//! table. Columns of an existing table are not migrated.
//! Connection strings accept the driver's URL or keyword syntax; TLS uses platform
//! certificate validation and the connection's `sslmode`.

use std::error::Error;

use bytes::{BufMut, BytesMut};
use futures_util::pin_mut;
use postgres_native_tls::MakeTlsConnector;
use serde::Deserialize;
use thiserror::Error;
use tokio_postgres::Client;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{IsNull, ToSql, Type, to_sql_checked};

use crate::config::Secret;
use crate::decode::StoredContract;
use crate::sink::{
    EnvelopeSink, InvalidStoredValue, SinkError, last_per_key, parse_stored, stored_block,
    stored_contract,
};
use crate::wire::envelope::{AcceptedBlock, ChainId, Envelope};
use crate::wire::row::{ColumnType, ColumnValue, Row, Table, row_for};

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

fn sql_type(kind: ColumnType) -> &'static str {
    match kind {
        ColumnType::Uint => "NUMERIC(20,0)",
        ColumnType::Text => "TEXT",
        ColumnType::Bool => "BOOLEAN",
        ColumnType::Document => "JSONB",
    }
}

fn pg_type(kind: ColumnType) -> Type {
    match kind {
        ColumnType::Uint => Type::NUMERIC,
        ColumnType::Text => Type::TEXT,
        ColumnType::Bool => Type::BOOL,
        ColumnType::Document => Type::JSONB,
    }
}

/// The block hashes a stored `reorg` on chain `$1` names as orphaned.
const ORPHANED: &str =
    "(SELECT jsonb_array_elements_text(orphaned_hashes) FROM \"reorg\" WHERE chain = $1)";

/// Table DDL rendered from [`Table::columns`]. `(chain, dedupe_key)` is unique so a
/// replay can upsert.
fn create_table(table: Table) -> String {
    let columns = table
        .columns()
        .iter()
        .map(|column| {
            let nullability = if column.required { " NOT NULL" } else { "" };
            format!("\"{}\" {}{nullability}", column.name, sql_type(column.kind))
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE TABLE IF NOT EXISTS \"{table}\" ({columns}, UNIQUE (\"chain\", \"dedupe_key\"))"
    )
}

fn staging_table(table: Table) -> String {
    format!("staging_{table}")
}

fn quoted_columns(table: Table) -> String {
    table
        .columns()
        .iter()
        .map(|column| format!("\"{}\"", column.name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Merges the staging table into the dataset table.
///
/// The staging table holds each key once (see `last_per_key`), so `ON CONFLICT` only
/// updates a row an earlier flush already wrote.
fn upsert_sql(table: Table) -> String {
    let names = quoted_columns(table);
    let assignments = table
        .columns()
        .iter()
        .filter(|column| column.name != "chain" && column.name != "dedupe_key")
        .map(|column| format!("\"{name}\" = EXCLUDED.\"{name}\"", name = column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let staging = staging_table(table);
    format!(
        "INSERT INTO \"{table}\" ({names}) SELECT {names} FROM \"{staging}\" \
         ON CONFLICT (\"chain\", \"dedupe_key\") DO UPDATE SET {assignments}"
    )
}

#[derive(Debug)]
struct CopyPlan {
    statement: String,
    types: Vec<Type>,
}

fn copy_plan(table: Table) -> CopyPlan {
    let columns = table.columns();
    let names = columns
        .iter()
        .map(|column| format!("\"{}\"", column.name))
        .collect::<Vec<_>>()
        .join(", ");
    CopyPlan {
        statement: format!(
            "COPY \"{}\" ({names}) FROM STDIN WITH (FORMAT binary)",
            staging_table(table)
        ),
        types: columns.iter().map(|column| pg_type(column.kind)).collect(),
    }
}

/// `NUMERIC` binary layout: base-10_000 digits, most significant first.
///
/// A `u64` needs at most five digits. Zero is the empty digit list `PostgreSQL` expects.
fn write_numeric(number: u64, out: &mut BytesMut) -> Result<(), std::num::TryFromIntError> {
    if number == 0 {
        out.put_i16(0);
        out.put_i16(0);
        out.put_i16(0);
        out.put_i16(0);
        return Ok(());
    }
    let mut digits = [0_i16; 5];
    let mut value = number;
    let mut count = 0_i16;
    while value > 0 {
        let index = usize::try_from(count)?;
        digits[index] = i16::try_from(value % 10_000)?;
        value /= 10_000;
        count += 1;
    }
    out.put_i16(count);
    out.put_i16(count - 1);
    out.put_i16(0);
    out.put_i16(0);
    for digit in digits[..usize::try_from(count)?].iter().rev() {
        out.put_i16(*digit);
    }
    Ok(())
}

impl ToSql for ColumnValue {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match (self, ty) {
            (Self::Null, _) => Ok(IsNull::Yes),
            (Self::Uint(number), &Type::NUMERIC) => {
                write_numeric(*number, out)?;
                Ok(IsNull::No)
            }
            (Self::Text(text), &Type::TEXT) => {
                out.extend_from_slice(text.as_bytes());
                Ok(IsNull::No)
            }
            (Self::Bool(flag), &Type::BOOL) => {
                out.put_u8(u8::from(*flag));
                Ok(IsNull::No)
            }
            (Self::Document(json), &Type::JSONB) => {
                out.put_u8(1);
                out.extend_from_slice(json.as_bytes());
                Ok(IsNull::No)
            }
            _ => Err(format!("cannot encode {self:?} as {ty}").into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC | Type::TEXT | Type::BOOL | Type::JSONB)
    }

    to_sql_checked!();
}

/// Upserts envelopes into `PostgreSQL`; buffered rows are cleared only after commit succeeds.
///
/// A failed flush rolls back the entire batch and leaves it buffered. A replay of
/// `(chain, dedupe_key)` updates that row. A connection loss during commit can leave its
/// outcome unknown, so retrying is not exactly-once delivery. No writes occur until
/// [`EnvelopeSink::flush`].
#[derive(Debug)]
pub struct PostgresSink {
    client: Client,
    rows: Vec<Row>,
    plans: [CopyPlan; Table::ALL.len()],
    merges: [String; Table::ALL.len()],
    connection_task: Option<tokio::task::JoinHandle<()>>,
}

impl PostgresSink {
    /// Connects using platform TLS and creates the dataset tables before ingest starts.
    ///
    /// # Errors
    ///
    /// Returns a typed error for TLS setup, connection, or schema creation failures.
    pub async fn open(settings: &PostgresSettings) -> Result<Self, StoreError> {
        let connector = MakeTlsConnector::new(native_tls::TlsConnector::new()?);
        let (client, connection) =
            tokio_postgres::connect(settings.connection_string.expose(), connector).await?;
        let task = tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(%error, "PostgreSQL connection stopped");
            }
        });
        let mut sink = Self::new(client).await?;
        sink.connection_task = Some(task);
        tracing::info!("PostgreSQL storage opened");
        Ok(sink)
    }

    /// Takes a connected client and creates the dataset tables.
    ///
    /// The caller must already be driving the client's connection future. Existing tables
    /// are reused without schema migration or validation.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] when table creation fails.
    pub async fn new(mut client: Client) -> Result<Self, StoreError> {
        let transaction = client.transaction().await?;
        transaction
            .batch_execute(&(Table::ALL.map(create_table).join(";\n") + ";"))
            .await?;
        transaction.commit().await?;
        Ok(Self {
            client,
            rows: Vec::new(),
            plans: Table::ALL.map(copy_plan),
            merges: Table::ALL.map(upsert_sql),
            connection_task: None,
        })
    }

    /// The contracts discovered on `chain` that this store holds, excluding any created in
    /// a block a stored `reorg` names as orphaned.
    ///
    /// Read once at startup, before the first write, so a restart decodes every contract
    /// a previous run discovered.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] when the query fails and [`StoreError::Restore`]
    /// when a stored address does not parse.
    pub async fn contracts(&self, chain: &ChainId) -> Result<Vec<StoredContract>, StoreError> {
        let rows = self
            .client
            .query(
                &format!(
                    "SELECT protocol, name, address, block_hash FROM \"contract\" \
                     WHERE chain = $1 AND block_hash NOT IN {ORPHANED}"
                ),
                &[&chain.as_str()],
            )
            .await?;
        let mut contracts = Vec::with_capacity(rows.len());
        for row in rows {
            contracts.push(stored_contract(
                row.get(0),
                row.get(1),
                row.get(2),
                row.get(3),
            )?);
        }
        Ok(contracts)
    }

    /// The newest `limit` accepted blocks on `chain` that no stored `reorg` orphans,
    /// oldest first, after dropping every older row.
    ///
    /// Read once at startup, before the first write: the result is the undo window a
    /// restart resumes from. Rows below the oldest one returned can never be read again,
    /// so they are deleted here, which is what keeps the ledger from growing without
    /// bound across restarts. Contiguity is not checked; the pipeline does that.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] when the query or the delete fails and
    /// [`StoreError::Restore`] when a stored value does not parse.
    pub async fn ledger(
        &self,
        chain: &ChainId,
        limit: usize,
    ) -> Result<Vec<AcceptedBlock>, StoreError> {
        // `NUMERIC` has no `FromSql` without an extra crate, so heights cross as text.
        let rows = self
            .client
            .query(
                &format!(
                    "SELECT height::TEXT, hash, parent_hash, \"timestamp\"::TEXT \
                     FROM \"accepted_block\" WHERE chain = $1 AND hash NOT IN {ORPHANED} \
                     ORDER BY height DESC LIMIT $2"
                ),
                &[&chain.as_str(), &i64::try_from(limit).unwrap_or(i64::MAX)],
            )
            .await?;
        let mut ledger = Vec::with_capacity(rows.len());
        for row in rows.iter().rev() {
            ledger.push(stored_block(
                parse_stored("accepted_block.height", row.get(0))?,
                row.get(1),
                row.get(2),
                parse_stored("accepted_block.timestamp", row.get(3))?,
            )?);
        }
        if let Some(oldest) = ledger.first() {
            self.client
                .execute(
                    "DELETE FROM \"accepted_block\" WHERE chain = $1 AND height < $2::TEXT::NUMERIC",
                    &[&chain.as_str(), &oldest.height.to_string()],
                )
                .await?;
        }
        Ok(ledger)
    }

    async fn write_batch(&mut self) -> Result<(), StoreError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let transaction = self.client.transaction().await?;
        // ponytail: eight fixed tables mean eight linear scans. Group at publish time
        // only if the number of datasets or profiling warrants a grouping buffer.
        for ((table, plan), merge) in Table::ALL.into_iter().zip(&self.plans).zip(&self.merges) {
            let rows = last_per_key(&self.rows, table);
            if rows.is_empty() {
                continue;
            }
            let staging = staging_table(table);
            transaction
                .batch_execute(&format!(
                    "CREATE TEMP TABLE \"{staging}\" (LIKE \"{table}\" INCLUDING DEFAULTS) \
                     ON COMMIT DROP"
                ))
                .await?;
            let writer =
                BinaryCopyInWriter::new(transaction.copy_in(&plan.statement).await?, &plan.types);
            pin_mut!(writer);
            for row in rows {
                writer.as_mut().write_raw(row.values()).await?;
            }
            writer.as_mut().finish().await?;
            transaction.batch_execute(merge).await?;
        }
        transaction.commit().await?;
        self.rows.clear();
        Ok(())
    }
}

impl Drop for PostgresSink {
    fn drop(&mut self) {
        if let Some(task) = &self.connection_task {
            task.abort();
        }
    }
}

impl EnvelopeSink for PostgresSink {
    async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
        self.rows.push(row_for(&envelope.chain, &envelope.event));
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), SinkError> {
        self.write_batch().await?;
        Ok(())
    }
}

/// Why `PostgreSQL` storage could not be connected, initialized, or committed.
#[derive(Debug, Error)]
pub enum StoreError {
    /// Platform TLS initialization failed.
    #[error("initialize PostgreSQL TLS: {0}")]
    Tls(#[from] native_tls::Error),
    /// `PostgreSQL` connection, schema, COPY, or transaction failure.
    #[error("PostgreSQL storage: {0}")]
    Database(#[from] tokio_postgres::Error),
    /// A value read back at startup did not parse.
    #[error(transparent)]
    Restore(#[from] InvalidStoredValue),
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::B256;

    use crate::wire::envelope::{ChainId, Event, Reorg};

    use super::*;

    fn numeric_words(number: u64) -> Vec<i16> {
        let mut out = BytesMut::new();
        write_numeric(number, &mut out).expect("a u64 fits in five numeric digits");
        out.chunks_exact(2)
            .map(|chunk| i16::from_be_bytes([chunk[0], chunk[1]]))
            .collect()
    }

    #[test]
    fn numeric_binary_keeps_zero_and_the_full_unsigned_range() {
        assert_eq!(numeric_words(0), [0, 0, 0, 0]);
        assert_eq!(
            numeric_words(u64::MAX),
            [5, 4, 0, 0, 1844, 6744, 737, 955, 1615]
        );
    }

    #[test]
    fn ddl_marks_required_columns_and_copy_uses_binary_field_order() {
        for table in Table::ALL {
            let ddl = create_table(table);
            let plan = copy_plan(table);
            assert!(
                ddl.contains("UNIQUE (\"chain\", \"dedupe_key\")"),
                "{table} upserts on its identity"
            );
            let merge = upsert_sql(table);
            assert!(
                merge.contains("ON CONFLICT (\"chain\", \"dedupe_key\") DO UPDATE SET"),
                "{table} merges on the identity"
            );
            assert!(
                merge.contains(&format!("FROM \"staging_{table}\"")),
                "{table} merges from its staging table"
            );
            assert!(
                !merge.contains("\"chain\" = EXCLUDED"),
                "the conflict columns stay the row's identity"
            );
            for column in table.columns() {
                let definition = format!(
                    "\"{}\" {}{}",
                    column.name,
                    sql_type(column.kind),
                    if column.required { " NOT NULL" } else { "" }
                );
                assert!(ddl.contains(&definition), "{definition} missing from {ddl}");
            }
            let names = table
                .columns()
                .iter()
                .map(|column| format!("\"{}\"", column.name))
                .collect::<Vec<_>>()
                .join(", ");
            assert_eq!(
                plan.statement,
                format!("COPY \"staging_{table}\" ({names}) FROM STDIN WITH (FORMAT binary)")
            );
            assert_eq!(plan.types.len(), table.columns().len());
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

    async fn count(sink: &PostgresSink, table: Table) -> i64 {
        sink.client
            .query_one(&format!("SELECT count(*) FROM \"{table}\""), &[])
            .await
            .expect("count rows")
            .get(0)
    }

    /// A stored contract reads back scoped to its chain, and is excluded once a stored
    /// reorg orphans its creating block.
    #[tokio::test]
    #[ignore = "requires INDEXER_TEST_POSTGRES_URL pointing to PostgreSQL 18"]
    async fn stored_contracts_read_back_and_orphaned_ones_are_excluded() {
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
        let mut sink = PostgresSink::new(client).await.expect("sink");
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

        sink.client
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
        let mut sink = PostgresSink::new(client).await.expect("sink");
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
            5,
            "7 through 10 on base, and ethereum's row"
        );
        assert_eq!(
            sink.ledger(&ChainId::new("ethereum"), 3)
                .await
                .expect("read back"),
            [accepted(1)]
        );

        sink.client
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
        let mut sink = PostgresSink::new(client).await.expect("sink");
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
        let rows = sink
            .client
            .query("SELECT height::text, chain FROM reorg", &[])
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
        sink.client
            .batch_execute(
                "ALTER TABLE reorg ADD CONSTRAINT reject_height CHECK (height = 0) NOT VALID",
            )
            .await
            .expect("constraint");
        assert!(matches!(
            sink.flush().await,
            Err(SinkError::Postgres(StoreError::Database(_)))
        ));
        assert_eq!(sink.rows.len(), 1);
        assert_eq!(count(&sink, Table::Reorg).await, 1);
        sink.client
            .batch_execute("ALTER TABLE reorg DROP CONSTRAINT reject_height")
            .await
            .expect("remove constraint");
        sink.flush().await.expect("retry");
        assert!(sink.rows.is_empty());
        assert_eq!(count(&sink, Table::Reorg).await, 1);
        assert_eq!(
            sink.client
                .query_one("SELECT height::text FROM reorg", &[])
                .await
                .expect("updated row")
                .get::<_, String>(0),
            "42",
            "the retried replay updates the stored row"
        );
        let hashes: serde_json::Value = serde_json::from_str(
            &sink
                .client
                .query_one("SELECT orphaned_hashes::text FROM reorg", &[])
                .await
                .expect("JSONB")
                .get::<_, String>(0),
        )
        .expect("JSON");
        assert_eq!(hashes, serde_json::json!([format!("{:#x}", B256::ZERO)]));
        sink.client
            .batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
            .await
            .expect("drop test schema");
        drop(sink);
        task.await
            .expect("connection task")
            .expect("connection closes");
    }
}
