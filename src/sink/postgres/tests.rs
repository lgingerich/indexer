//! The `PostgreSQL` store's own tests: its binary codec and DDL, which need no server, and
//! what only a live server shows — the writer lock, a reconnect, a publication, `COPY`,
//! and the staging vacuum. The [contract](crate::sink::store::contract) every SQL store
//! keeps runs here too.
//!
//! The tests that need a server are ignored unless asked for, and read
//! `INDEXER_TEST_POSTGRES_URL`, which must name a `PostgreSQL` 18 database of its own.
//! Each opens its store in a database schema named for the test's process and dropped
//! afterward, so parallel runs do not meet.

// The crate denies `expect`/`unwrap` to keep production paths honest; tests are allowed
// them per the repository test style, since a failed expectation there means the fixture
// or setup is wrong.
#![expect(clippy::expect_used)]

use std::sync::atomic::{AtomicUsize, Ordering};

use alloy_primitives::B256;

use crate::sink::store::contract::{Harness, datasets};
use crate::sink::table::{Table, TableId};
use crate::sink::{EnvelopeSink as _, SinkError};
use crate::wire::envelope::{ChainId, Envelope, Event, Reorg};

use super::*;

/// A store opened with [`PostgresSink::open`] in a database schema of its own on the
/// `INDEXER_TEST_POSTGRES_URL` database, and the test's own connection to that schema.
struct Pg {
    /// The test's connection, with the store's schema as its search path.
    admin: Client,
    /// The store's database schema, which is also the chain it was opened for.
    schema: String,
    connection: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
}

/// How many stores this process has opened, which names each one's schema.
static OPENED: AtomicUsize = AtomicUsize::new(0);

impl Pg {
    fn url() -> String {
        std::env::var("INDEXER_TEST_POSTGRES_URL").expect("test database URL")
    }

    /// The settings a store of the test database opens with.
    fn settings() -> PostgresSettings {
        toml::from_str(&format!(
            "tables = ['logs']\nconnection_string = '{}'",
            Self::url()
        ))
        .expect("settings")
    }

    /// The server process of the session holding the store's writer lock: the store's own.
    async fn writer(&self) -> i32 {
        self.admin
            .query_one(
                "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted \
                 AND classid = hashtext('indexer')::oid AND objid = hashtext($1)::oid \
                 AND objsubid = 2",
                &[&self.schema],
            )
            .await
            .expect("the writer's session")
            .get(0)
    }

    async fn count(&self, table: Table) -> i64 {
        self.admin
            .query_one(&format!("SELECT count(*) FROM \"{table}\""), &[])
            .await
            .expect("count rows")
            .get(0)
    }
}

impl Harness for Pg {
    type Engine = Postgres;

    async fn open(schema: Arc<Schema>) -> (PostgresSink, Self) {
        let name = format!(
            "store_{}_test_{}",
            OPENED.fetch_add(1, Ordering::Relaxed),
            std::process::id()
        );
        let sink = PostgresSink::open(&Self::settings(), schema, &name)
            .await
            .expect("open");
        let (admin, connection) = tokio_postgres::connect(&Self::url(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        let connection = tokio::spawn(connection);
        admin
            .batch_execute(&format!("SET search_path TO \"{name}\""))
            .await
            .expect("the store's schema");
        (
            sink,
            Self {
                admin,
                schema: name,
                connection,
            },
        )
    }

    fn chain(&self) -> ChainId {
        ChainId::new(&self.schema)
    }

    async fn query(&mut self, sql: &str) -> Vec<Vec<Value>> {
        self.admin
            .query(sql, &[])
            .await
            .expect(sql)
            .iter()
            .map(|row| (0..row.len()).map(|index| row.get(index)).collect())
            .collect()
    }

    async fn execute(&mut self, sql: &str) {
        self.admin.batch_execute(sql).await.expect(sql);
    }

    async fn remove(self) {
        self.admin
            .batch_execute(&format!("DROP SCHEMA \"{}\" CASCADE", self.schema))
            .await
            .expect("drop test schema");
        self.connection.abort();
    }
}

mod contract {
    crate::sink::store::contract::contract_tests!(
        super::Pg,
        #[ignore = "requires INDEXER_TEST_POSTGRES_URL pointing to PostgreSQL 18"]
    );
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
            20, 19, 0, 0, 11, 5792, 892, 3731, 6195, 4235, 7098, 5008, 6879, 785, 3269, 9846, 6564,
            564, 394, 5758, 4007, 9131, 2963, 9935
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

/// Every table is keyed on its identity, and indexed and deleted by block exactly when a
/// reorg retracts its rows.
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
            ddl.ends_with(", PRIMARY KEY (\"chain\", \"dedupe_key\"))"),
            "{label} upserts on its identity and can publish a delete: {ddl}"
        );
        let retracted = table.id != Table::Reorg.into();
        let block = table.block_hash.as_ref().zip(table.block_number.as_ref());
        assert_eq!(
            block.is_some(),
            retracted,
            "{label} names its block exactly when a reorg deletes from it"
        );
        let by_block: Vec<String> = block
            .into_iter()
            .map(|(_, number)| format!(" (\"chain\", \"{number}\")"))
            .collect();
        let indexes = sql::create_indexes::<Postgres>(table);
        // A derived table adds the indexes its reads need after the block's.
        let derived = matches!(table.id, TableId::Derived(_));
        assert!(
            indexes.len() == by_block.len() || derived && indexes.len() > by_block.len(),
            "{label} is indexed by block exactly when a reorg deletes from it: {indexes:?}"
        );
        for (index, columns) in indexes.iter().zip(&by_block) {
            assert!(
                index.ends_with(columns),
                "{label} is indexed on{columns}: {index}"
            );
        }
        assert_eq!(
            sql::delete_blocks::<Postgres>(table).is_some(),
            retracted,
            "{label}"
        );
        for column in &table.columns {
            // Every column is followed by another or by the key, so the separator keeps a
            // nullable column from matching a required one's prefix.
            let definition = format!(
                "\"{}\" {}{}, ",
                column.name,
                Postgres::type_name(column.kind),
                if column.nullable { "" } else { " NOT NULL" }
            );
            assert!(ddl.contains(&definition), "{definition} missing from {ddl}");
        }
    }
}

/// The derived tables the app reads by pool or account carry the indexes those reads use,
/// after the block's.
#[test]
fn the_metric_tables_are_indexed_for_the_apps_reads() {
    let schema = crate::sink::fixtures::schema();
    for (table, expected) in [
        (
            "metric_v1_swaps",
            &[r#"("chain", "address", "block_number", "log_index")"#][..],
        ),
        (
            "metric_v1_bin_fills",
            &[r#"("chain", "address", "block_number")"#],
        ),
        (
            "metric_v1_liquidity",
            &[
                r#"("chain", "owner")"#,
                r#"("chain", "transaction_from_address")"#,
                r#"("chain", "address", "owner")"#,
            ],
        ),
        (
            "metric_v1_pool_bins",
            &[r#"("chain", "pool", "transaction_from_address")"#],
        ),
    ] {
        let def = schema
            .derived_table(table)
            .unwrap_or_else(|| panic!("the shipped catalog declares {table}"));
        let indexes = sql::create_indexes::<Postgres>(def);
        let added: Vec<&str> = indexes
            .iter()
            .skip(1)
            .map(|index| index.rsplit_once(" ON ").map_or("", |(_, on)| on))
            .map(|on| on.trim_start_matches(&format!("\"{table}\" ")))
            .collect();
        assert_eq!(added, expected, "{table}: {indexes:?}");
    }
}

#[test]
fn settings_require_a_connection_reject_typos_and_redact_credentials() {
    let settings: PostgresSettings = toml::from_str(
        "tables = ['logs']\nconnection_string = 'postgres://user:secret@localhost/indexer'",
    )
    .expect("settings");
    let debug = format!("{settings:?}");
    assert!(
        !debug.contains("secret") && debug.contains("[redacted]"),
        "{debug}"
    );
    for (toml, reason) in [
        ("tables = ['logs']", "missing field `connection_string`"),
        (
            "tables = ['logs']\nconnection_string = 'host=localhost'\nbatch_record = 1",
            "unknown field `batch_record`",
        ),
    ] {
        let error = toml::from_str::<PostgresSettings>(toml).expect_err(toml);
        assert!(error.to_string().contains(reason), "{toml}: {error}");
    }
}

/// A second writer of a database schema is refused while the first holds it.
#[tokio::test]
#[ignore = "requires INDEXER_TEST_POSTGRES_URL pointing to PostgreSQL 18"]
async fn a_second_writer_of_a_schema_is_refused() {
    let (_first, harness) = Pg::open(datasets()).await;
    assert!(matches!(
        PostgresSink::open(&Pg::settings(), datasets(), &harness.schema).await,
        Err(StoreError::Locked { .. })
    ));
    harness.remove().await;
}

/// A reorg deletes its orphaned blocks' rows in every table — committed earlier or
/// buffered in the same batch — and a block that returns is stored. Run with every table
/// in a publication, which refuses a delete from a table without a replica identity, so
/// it proves the primary key is one.
#[tokio::test]
#[ignore = "requires INDEXER_TEST_POSTGRES_URL pointing to PostgreSQL 18"]
async fn a_reorg_deletes_orphaned_blocks_under_a_publication() {
    use crate::wire::datasets::evm::{Block, Log};
    use crate::wire::envelope::BlockMeta;

    let (mut sink, harness) = Pg::open(datasets()).await;
    let schema = &harness.schema;
    harness
        .admin
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
            Event::AcceptedBlock(BlockMeta {
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
    let stored = async || -> Vec<(String, String)> {
        harness
            .admin
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
    assert_eq!(stored().await, only(0xb1));

    // A' orphaned within one batch, and A canonical again after it.
    sink.publish(reorg(0xb1)).await.expect("publish");
    for envelope in block(0xa1) {
        sink.publish(envelope).await.expect("publish");
    }
    sink.flush().await.expect("commit the return");
    assert_eq!(stored().await, only(0xa1));
    assert_eq!(harness.count(Table::Reorg).await, 2);

    harness
        .admin
        .batch_execute(&format!("DROP PUBLICATION \"{schema}\""))
        .await
        .expect("drop the publication");
    harness.remove().await;
}

/// A decoded array of scalars lands as a native array of its element type, and a tuple as
/// one `JSONB` object keyed by its ABI names with every integer a decimal string, as
/// Allium's decoded `params` are.
#[tokio::test]
#[ignore = "requires INDEXER_TEST_POSTGRES_URL pointing to PostgreSQL 18"]
async fn an_array_is_a_typed_array_and_a_tuple_one_object() {
    let (mut sink, harness) = Pg::open(crate::sink::fixtures::schema()).await;
    let created = crate::sink::fixtures::decoded_pool_created();
    sink.publish(Envelope::new(
        ChainId::new("base"),
        Event::Decoded(Box::new(created)),
    ))
    .await
    .expect("publish");
    sink.flush().await.expect("commit");

    let row = harness
        .admin
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

    harness.remove().await;
}

/// `NUMERIC` multiplies exactly, where `DuckDB`'s `BIGNUM` turns into a `DOUBLE`.
#[tokio::test]
#[ignore = "requires INDEXER_TEST_POSTGRES_URL pointing to PostgreSQL 18"]
async fn a_wide_integer_multiplies_exactly() {
    let (mut sink, harness) = Pg::open(crate::sink::fixtures::schema()).await;
    let swap = crate::sink::fixtures::decoded_swap();
    sink.publish(Envelope::new(
        ChainId::new("base"),
        Event::Decoded(Box::new(swap)),
    ))
    .await
    .expect("publish");
    sink.flush().await.expect("commit");

    let product: String = harness
        .admin
        .query_one(
            "SELECT (sqrt_price_x96 * 1000000)::TEXT FROM uniswap_v3_pool_swap",
            &[],
        )
        .await
        .expect("the swap reads back")
        .get(0);
    assert_eq!(product, "4115542941155561242646778000000");
    harness.remove().await;
}

/// A flush on a connection the server ended connects again, takes the writer lock again,
/// and commits the batch it kept to the chain's database schema.
#[tokio::test]
#[ignore = "requires INDEXER_TEST_POSTGRES_URL pointing to PostgreSQL 18"]
async fn a_flush_after_the_server_ends_the_session_connects_again() {
    let (mut sink, harness) = Pg::open(datasets()).await;
    let ended = harness.writer().await;
    let reorg = Event::Reorg(Reorg {
        height: 7,
        new_head_hash: B256::ZERO,
        orphaned_hashes: vec![B256::ZERO],
    });
    sink.publish(Envelope::new(ChainId::new("base"), reorg))
        .await
        .expect("publish");
    harness
        .admin
        .query_one("SELECT pg_terminate_backend($1)", &[&ended])
        .await
        .expect("end the session");

    sink.flush().await.expect("the flush connects again");
    assert_eq!(sink.buffered(), 0);
    assert_ne!(
        harness.writer().await,
        ended,
        "a new session holds the lock"
    );
    assert_eq!(
        harness.count(Table::Reorg).await,
        1,
        "in the chain's schema"
    );
    assert!(
        matches!(
            PostgresSink::open(&Pg::settings(), datasets(), &harness.schema).await,
            Err(StoreError::Locked { .. })
        ),
        "the new session holds the writer lock"
    );
    harness.remove().await;
}

/// `COPY` is atomic with the merge, a failed batch stays buffered for its retry, and a
/// replay updates the stored row, for a chain whose name holds the characters text `COPY`
/// would have to escape.
#[tokio::test]
#[ignore = "requires INDEXER_TEST_POSTGRES_URL pointing to PostgreSQL 18"]
async fn copy_is_atomic_retains_failed_batches_and_upserts_a_replay() {
    let (mut sink, harness) = Pg::open(datasets()).await;
    assert_eq!(
        harness
            .admin
            .query_one("SHOW server_version_num", &[])
            .await
            .expect("version")
            .get::<_, String>(0)
            .parse::<u32>()
            .expect("version number")
            / 10_000,
        18
    );
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
    assert_eq!(harness.count(Table::Reorg).await, 0);
    sink.flush().await.expect("commit");
    let rows = harness
        .admin
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
    harness
        .admin
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
    assert_eq!(harness.count(Table::Reorg).await, 1);
    harness
        .admin
        .batch_execute("ALTER TABLE reorgs DROP CONSTRAINT reject_height")
        .await
        .expect("remove constraint");
    sink.flush().await.expect("retry");
    assert_eq!(sink.buffered(), 0);
    assert_eq!(harness.count(Table::Reorg).await, 1);
    assert_eq!(
        harness
            .admin
            .query_one("SELECT height::text FROM reorgs", &[])
            .await
            .expect("updated row")
            .get::<_, String>(0),
        "42",
        "the retried replay updates the stored row"
    );
    let hashes: serde_json::Value = serde_json::from_str(
        &harness
            .admin
            .query_one("SELECT orphaned_hashes::text FROM reorgs", &[])
            .await
            .expect("JSONB")
            .get::<_, String>(0),
    )
    .expect("JSON");
    assert_eq!(hashes, serde_json::json!([format!("{:#x}", B256::ZERO)]));
    harness.remove().await;
}

/// A flush leaves its staging tables' deleted rows in place until they have taken twenty
/// thousand rows, then vacuums them back to empty files.
#[tokio::test]
#[ignore = "requires INDEXER_TEST_POSTGRES_URL pointing to PostgreSQL 18"]
async fn staging_tables_are_vacuumed_once_they_take_enough_rows() {
    /// The size of the staging tables in the store's own session's temporary schema.
    async fn staged_bytes(harness: &Pg) -> i64 {
        harness
            .admin
            .query_one(
                "SELECT coalesce(sum(pg_relation_size(c.oid)), 0)::BIGINT FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.relname LIKE 'staging\\_%' AND n.nspname = (\
                     SELECT 'pg_temp_' || id FROM pg_stat_get_backend_idset() id \
                     WHERE pg_stat_get_backend_pid(id) = $1)",
                &[&harness.writer().await],
            )
            .await
            .expect("staging size")
            .get(0)
    }

    let (mut sink, harness) = Pg::open(datasets()).await;
    let chain = ChainId::new("base");
    let publish = async |sink: &mut PostgresSink, heads: std::ops::Range<u64>| {
        for head in heads {
            let reorg = Reorg {
                height: head,
                new_head_hash: B256::from(U256::from(head)),
                orphaned_hashes: vec![B256::ZERO],
            };
            sink.publish(Envelope::new(chain.clone(), Event::Reorg(reorg)))
                .await
                .expect("publish");
        }
        sink.flush().await.expect("commit");
    };
    publish(&mut sink, 0..100).await;
    assert!(
        staged_bytes(&harness).await > 0,
        "a delete leaves the table's pages"
    );
    publish(&mut sink, 100..20_000).await;
    assert_eq!(harness.count(Table::Reorg).await, 20_000);
    assert_eq!(
        staged_bytes(&harness).await,
        0,
        "the vacuum empties the files"
    );
    harness.remove().await;
}
