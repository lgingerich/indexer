//! `PostgreSQL` 18 storage using the shared typed rows and transactional binary `COPY`.
//!
//! Tables live in a schema named for the chain, so chains sharing a database keep their
//! tables apart. Each flush loads all buffered datasets in one transaction. Binary `COPY` sends each
//! value in `PostgreSQL`'s native field format, so the server does not parse text, into a
//! temporary table that is then upserted on `(chain, dedupe_key)`. Only the last buffered
//! copy of a repeated key is loaded, since `PostgreSQL` refuses to touch one conflict row
//! twice in a statement. Unsigned integers use `NUMERIC(20,0)` (`PostgreSQL` has no
//! unsigned bigint), documents use `JSONB`, and hex values remain `TEXT`. Columns the
//! chain always provides are `NOT NULL`, and `(chain, dedupe_key)` is each table's primary
//! key — the replica identity logical replication needs to publish a delete. A `reorg`
//! deletes its orphaned blocks' rows in the same transaction, through an index on each
//! table's `(chain, block hash)`. Decoded event tables store signed 64-bit arguments as
//! `BIGINT` and wider integers as `NUMERIC(78,0)`, exact to 256 bits. Columns of an
//! existing table are not migrated: the block index is added to it at startup, but a
//! table created before the primary key keeps its `UNIQUE` constraint instead.
//! Connection strings accept the driver's URL or keyword syntax; TLS uses platform
//! certificate validation and the connection's `sslmode`.

use std::error::Error;
use std::sync::Arc;

use alloy_primitives::U256;
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
    Batch, EnvelopeSink, InvalidStoredValue, SinkError, parse_stored, quote_identifier,
    stored_block, stored_contract,
};
use crate::wire::envelope::{AcceptedBlock, ChainId, Envelope};
use crate::wire::row::{ColumnType, ColumnValue, Schema, TableDef};

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
        ColumnType::Int => "BIGINT",
        ColumnType::BigInt => "NUMERIC(78,0)",
        ColumnType::Text => "TEXT",
        ColumnType::Bool => "BOOLEAN",
        ColumnType::Document => "JSONB",
    }
}

fn pg_type(kind: ColumnType) -> Type {
    match kind {
        ColumnType::Uint | ColumnType::BigInt => Type::NUMERIC,
        ColumnType::Int => Type::INT8,
        ColumnType::Text => Type::TEXT,
        ColumnType::Bool => Type::BOOL,
        ColumnType::Document => Type::JSONB,
    }
}

/// A table's name as SQL.
fn quoted(table: &TableDef) -> String {
    format!("\"{}\"", table.name)
}

/// Table DDL rendered from its columns. `(chain, dedupe_key)` is the primary key, so a
/// replay can upsert and logical replication can publish a delete.
fn create_table(table: &TableDef) -> String {
    let columns = table
        .columns
        .iter()
        .map(|column| {
            let nullability = if column.required { " NOT NULL" } else { "" };
            format!("\"{}\" {}{nullability}", column.name, sql_type(column.kind))
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({columns}, PRIMARY KEY (\"chain\", \"dedupe_key\"))",
        quoted(table)
    )
}

/// `PostgreSQL`'s identifier limit, in bytes; a longer name is silently truncated.
const MAX_IDENTIFIER: usize = 63;

/// The name of a table's block index: `{table}_block_index`, or, when that would pass
/// [`MAX_IDENTIFIER`], the table name cut short with a hash of the whole name, so two
/// long tables never truncate to one index name.
fn block_index_name(table: &str) -> String {
    const SUFFIX: &str = "_block_index";
    let name = format!("{table}{SUFFIX}");
    if name.len() <= MAX_IDENTIFIER {
        return name;
    }
    let hash = alloy_primitives::hex::encode(&alloy_primitives::keccak256(table)[..4]);
    let keep = MAX_IDENTIFIER - SUFFIX.len() - hash.len() - 1;
    format!("{}_{hash}{SUFFIX}", &table[..keep])
}

/// The index a `reorg`'s delete finds a block's rows by, for a table whose rows belong
/// to a block. It lives in the table's schema.
fn create_block_index(table: &TableDef) -> Option<String> {
    table.block_hash_column.map(|column| {
        format!(
            "CREATE INDEX IF NOT EXISTS \"{}\" ON {} (\"chain\", \"{column}\")",
            block_index_name(&table.name),
            quoted(table)
        )
    })
}

/// Deletes chain `$1`'s rows of the blocks in `$2`, for a table whose rows belong to a
/// block.
fn delete_blocks_sql(table: &TableDef) -> Option<String> {
    table.block_hash_column.map(|column| {
        format!(
            "DELETE FROM {} WHERE \"chain\" = $1 AND \"{column}\" = ANY($2)",
            quoted(table)
        )
    })
}

fn quoted_columns(table: &TableDef) -> String {
    table
        .columns
        .iter()
        .map(|column| format!("\"{}\"", column.name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Merges the staging table into its table.
///
/// The staging table holds each key once (see `Batch::by_table`), so `ON CONFLICT` only
/// updates a row an earlier flush already wrote.
fn upsert_sql(table: &TableDef, staging: &str) -> String {
    let names = quoted_columns(table);
    let assignments = table
        .columns
        .iter()
        .filter(|column| column.name != "chain" && column.name != "dedupe_key")
        .map(|column| format!("\"{name}\" = EXCLUDED.\"{name}\"", name = column.name))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {} ({names}) SELECT {names} FROM \"{staging}\" \
         ON CONFLICT (\"chain\", \"dedupe_key\") DO UPDATE SET {assignments}",
        quoted(table)
    )
}

#[derive(Debug)]
struct CopyPlan {
    statement: String,
    types: Vec<Type>,
}

fn copy_plan(table: &TableDef, staging: &str) -> CopyPlan {
    CopyPlan {
        statement: format!(
            "COPY \"{staging}\" ({}) FROM STDIN WITH (FORMAT binary)",
            quoted_columns(table)
        ),
        types: table
            .columns
            .iter()
            .map(|column| pg_type(column.kind))
            .collect(),
    }
}

/// One table, with every statement a flush runs against it rendered once.
#[derive(Debug)]
struct Prepared {
    def: TableDef,
    /// Creates the session's temporary table a flush copies into, named by position so
    /// a 63-byte table name cannot push it past `PostgreSQL`'s identifier limit.
    create_staging: String,
    copy: CopyPlan,
    merge: String,
    delete: Option<String>,
}

impl Prepared {
    fn new(position: usize, def: TableDef) -> Self {
        let staging = format!("staging_{position}");
        Self {
            create_staging: format!(
                "CREATE TEMP TABLE \"{staging}\" (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP",
                quoted(&def)
            ),
            copy: copy_plan(&def, &staging),
            merge: upsert_sql(&def, &staging),
            delete: delete_blocks_sql(&def),
            def,
        }
    }
}

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

impl ToSql for ColumnValue {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match (self, ty) {
            (Self::Null, _) => Ok(IsNull::Yes),
            (Self::Uint(number), &Type::NUMERIC) => {
                write_numeric(false, U256::from(*number), out)?;
                Ok(IsNull::No)
            }
            (
                Self::BigInt {
                    negative,
                    magnitude,
                },
                &Type::NUMERIC,
            ) => {
                write_numeric(*negative, *magnitude, out)?;
                Ok(IsNull::No)
            }
            (Self::Int(number), &Type::INT8) => {
                out.put_i64(*number);
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
        matches!(
            *ty,
            Type::NUMERIC | Type::INT8 | Type::TEXT | Type::BOOL | Type::JSONB
        )
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
    batch: Batch,
    tables: Vec<Prepared>,
    connection_task: Option<tokio::task::JoinHandle<()>>,
}

impl PostgresSink {
    /// Connects using platform TLS and creates the tables in `database_schema` before
    /// ingest starts.
    ///
    /// # Errors
    ///
    /// Returns a typed error for TLS setup, connection, or schema creation failures.
    pub async fn open(
        settings: &PostgresSettings,
        schema: Arc<Schema>,
        database_schema: &str,
    ) -> Result<Self, StoreError> {
        let connector = MakeTlsConnector::new(native_tls::TlsConnector::new()?);
        let (client, connection) =
            tokio_postgres::connect(settings.connection_string.expose(), connector).await?;
        let task = tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(%error, "PostgreSQL connection stopped");
            }
        });
        let mut sink = Self::new(client, schema, database_schema).await?;
        sink.connection_task = Some(task);
        tracing::info!("PostgreSQL storage opened");
        Ok(sink)
    }

    /// Takes a connected client and creates every table in `schema`, with each one's
    /// block index, in the database schema `database_schema`.
    ///
    /// A run names that schema for its chain — `base.logs`, `base.uniswap_v3_pool_swap` —
    /// so chains sharing a database keep their tables apart. The schema is created if
    /// missing and becomes the session's `search_path`, so every statement after names
    /// tables unqualified. The caller must already be driving the client's connection
    /// future. Existing tables are reused without schema migration or validation.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Database`] when schema or table creation fails.
    pub async fn new(
        mut client: Client,
        schema: Arc<Schema>,
        database_schema: &str,
    ) -> Result<Self, StoreError> {
        let quoted = quote_identifier(database_schema);
        client
            .batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS {quoted}; SET search_path TO {quoted}"
            ))
            .await?;
        let tables: Vec<Prepared> = schema
            .tables()
            .iter()
            .enumerate()
            .map(|(position, def)| Prepared::new(position, def.clone()))
            .collect();
        let ddl = tables
            .iter()
            .map(|table| create_table(&table.def))
            .chain(
                tables
                    .iter()
                    .filter_map(|table| create_block_index(&table.def)),
            )
            .collect::<Vec<_>>()
            .join(";\n")
            + ";";
        let transaction = client.transaction().await?;
        transaction.batch_execute(&ddl).await?;
        transaction.commit().await?;
        Ok(Self {
            client,
            batch: Batch::new(schema),
            tables,
            connection_task: None,
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
    /// Returns [`StoreError::Database`] when the query fails and [`StoreError::Restore`]
    /// when a stored address does not parse.
    pub async fn contracts(&self, chain: &ChainId) -> Result<Vec<StoredContract>, StoreError> {
        let rows = self
            .client
            .query(
                "SELECT protocol, name, address, block_hash FROM \"contracts\" WHERE chain = $1",
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
                "SELECT height::TEXT, hash, parent_hash, \"timestamp\"::TEXT \
                 FROM \"accepted_blocks\" WHERE chain = $1 ORDER BY height DESC LIMIT $2",
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
                    "DELETE FROM \"accepted_blocks\" WHERE chain = $1 AND height < $2::TEXT::NUMERIC",
                    &[&chain.as_str(), &oldest.height.to_string()],
                )
                .await?;
        }
        Ok(ledger)
    }

    async fn write_batch(&mut self) -> Result<(), StoreError> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let transaction = self.client.transaction().await?;
        // Deletes before upserts, so a block orphaned and then canonical again within
        // this batch is deleted and rewritten rather than rewritten and deleted.
        for delete in self.tables.iter().filter_map(|table| table.delete.as_ref()) {
            for (chain, hashes) in &self.batch.orphaned {
                let hashes: Vec<&str> = hashes.iter().map(String::as_str).collect();
                transaction.execute(delete, &[chain, &hashes]).await?;
            }
        }
        let mut grouped = self.batch.by_table();
        for table in &self.tables {
            let Some(rows) = grouped.remove(&table.def.id) else {
                continue;
            };
            transaction.batch_execute(&table.create_staging).await?;
            let writer = BinaryCopyInWriter::new(
                transaction.copy_in(&table.copy.statement).await?,
                &table.copy.types,
            );
            pin_mut!(writer);
            for row in rows {
                writer.as_mut().write_raw(row.values()).await?;
            }
            writer.as_mut().finish().await?;
            transaction.batch_execute(&table.merge).await?;
        }
        transaction.commit().await?;
        self.batch.clear();
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
        self.batch.push(&envelope)
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
    use crate::wire::row::Table;

    use super::*;

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
    fn ddl_marks_required_columns_and_copy_uses_binary_field_order() {
        let schema = crate::sink::fixtures::schema();
        assert!(
            schema
                .tables()
                .iter()
                .any(|table| table.name == "uniswap_v3_pool_swap"),
            "the shipped catalog's event tables are among them"
        );
        for (position, table) in schema.tables().iter().cloned().enumerate() {
            let label = table.name.clone();
            let ddl = create_table(&table);
            let prepared = Prepared::new(position, table.clone());
            assert!(
                ddl.contains("PRIMARY KEY (\"chain\", \"dedupe_key\")"),
                "{label} upserts on its identity and can publish a delete"
            );
            let retracted = table.id != Table::Reorg.into();
            assert_eq!(
                create_block_index(&table).is_some(),
                retracted,
                "{label} is indexed by block exactly when a reorg deletes from it"
            );
            assert_eq!(prepared.delete.is_some(), retracted);
            assert!(
                prepared
                    .merge
                    .contains("ON CONFLICT (\"chain\", \"dedupe_key\") DO UPDATE SET"),
                "{label} merges on the identity"
            );
            assert!(
                prepared
                    .merge
                    .contains(&format!("FROM \"staging_{position}\"")),
                "{label} merges from its staging table"
            );
            assert!(
                !prepared.merge.contains("\"chain\" = EXCLUDED"),
                "the conflict columns stay the row's identity"
            );
            for column in table.columns.iter() {
                let definition = format!(
                    "\"{}\" {}{}",
                    column.name,
                    sql_type(column.kind),
                    if column.required { " NOT NULL" } else { "" }
                );
                assert!(ddl.contains(&definition), "{definition} missing from {ddl}");
            }
            assert_eq!(
                prepared.copy.statement,
                format!(
                    "COPY \"staging_{position}\" ({}) FROM STDIN WITH (FORMAT binary)",
                    quoted_columns(&table)
                )
            );
            assert_eq!(prepared.copy.types.len(), table.columns.len());
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

    /// A sink over the dataset tables only, for a test that decodes nothing.
    async fn dataset_sink(client: Client, database_schema: &str) -> PostgresSink {
        PostgresSink::new(client, Arc::default(), database_schema)
            .await
            .expect("sink")
    }

    async fn count(sink: &PostgresSink, table: Table) -> i64 {
        sink.client
            .query_one(&format!("SELECT count(*) FROM \"{table}\""), &[])
            .await
            .expect("count rows")
            .get(0)
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

        sink.client
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
        sink.client
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
            sink.client
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

        sink.client
            .batch_execute(&format!(
                "DROP PUBLICATION \"{schema}\"; DROP SCHEMA \"{schema}\" CASCADE"
            ))
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
        let mut sink = PostgresSink::new(client, crate::sink::fixtures::schema(), &schema)
            .await
            .expect("sink");
        let chain = ChainId::new("base");
        let swap = crate::sink::fixtures::decoded_swap();
        let block = swap.block_hash;
        sink.publish(Envelope::new(chain.clone(), Event::Decoded(Box::new(swap))))
            .await
            .expect("publish");
        sink.flush().await.expect("commit");

        let row = sink
            .client
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
        let remaining: i64 = sink
            .client
            .query_one("SELECT count(*) FROM uniswap_v3_pool_swap", &[])
            .await
            .expect("count")
            .get(0);
        assert_eq!(remaining, 0, "the reorg deleted the typed row");

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
        let rows = sink
            .client
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
        sink.client
            .batch_execute(
                "ALTER TABLE reorgs ADD CONSTRAINT reject_height CHECK (height = 0) NOT VALID",
            )
            .await
            .expect("constraint");
        assert!(matches!(
            sink.flush().await,
            Err(SinkError::Postgres(StoreError::Database(_)))
        ));
        assert_eq!(sink.batch.rows.len(), 1);
        assert_eq!(count(&sink, Table::Reorg).await, 1);
        sink.client
            .batch_execute("ALTER TABLE reorgs DROP CONSTRAINT reject_height")
            .await
            .expect("remove constraint");
        sink.flush().await.expect("retry");
        assert!(sink.batch.rows.is_empty());
        assert_eq!(count(&sink, Table::Reorg).await, 1);
        assert_eq!(
            sink.client
                .query_one("SELECT height::text FROM reorgs", &[])
                .await
                .expect("updated row")
                .get::<_, String>(0),
            "42",
            "the retried replay updates the stored row"
        );
        let hashes: serde_json::Value = serde_json::from_str(
            &sink
                .client
                .query_one("SELECT orphaned_hashes::text FROM reorgs", &[])
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
