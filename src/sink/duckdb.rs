//! The `DuckDB` sink: envelopes into a local, queryable database.
//!
//! Where [`StdoutJsonSink`](crate::sink::StdoutJsonSink) is fire-and-forward, this
//! one is a *store*: it appends each event as a row so a later process can query
//! the history locally with SQL. That split matters — `DuckDB` is an embedded,
//! single-writer engine, so it is an archive/analytics endpoint, not a
//! horizontally-scaled egress. Keep it for a local replica or an analytical
//! sidecar, not as the fan-out for many consumers.
//!
//! # Schema
//!
//! One normalized table per dataset, mirroring `crate::datasets::evm` — a row maps
//! to the struct field for field, and children are referenced by scalar key rather
//! than embedded (a `block` row holds transaction *hashes*, not transactions).
//! Control signals get their own tables.
//!
//! | table           | natural key                       | source                        |
//! |-----------------|-----------------------------------|-------------------------------|
//! | `block`         | `(number, hash)`                  | [`Block`]                     |
//! | `transaction`   | `hash`                            | [`Transaction`]               |
//! | `receipt`       | `transaction_hash`                | [`Receipt`]                   |
//! | `log`           | `(transaction_hash, log_index)`   | [`Log`]                       |
//! | `reorg`         | `(height, new_head_hash)`         | [`Reorg`]                     |
//! | `finalized`     | `(height, hash)`                  | [`Finalized`]                 |
//!
//! Every table also carries `sequence` (publish order) and `dedupe_key` (the
//! envelope's stable identity), so a reader can order the stream and deduplicate a
//! replay without re-deriving either. The store is append-only — it does not
//! enforce the key, because the pipeline stops on a write error and a legitimate
//! at-least-once replay must not look like a failure; deduplicate on `dedupe_key`
//! as the envelope documents.
//!
//! # Field mapping
//!
//! - Integers map to their natural `DuckDB` width: `u64`/`u128`/`u8` become
//!   `UBIGINT`/`UHUGEINT`/`UTINYINT`, `bool` a `BOOLEAN`.
//! - Byte arrays (hashes, addresses, bloom, calldata) become `BLOB` — 20 or 32
//!   bytes, not hex text. Display with `hex(hash)`; compare a literal with
//!   `unhex('...')`.
//! - `U256` fields (`difficulty`, `value`, …) become `VARCHAR` holding the decimal
//!   string, which is lossless: `DECIMAL(38, 0)` is narrower than `u256`, so no
//!   numeric column can hold every value. `CAST` in SQL when a value fits.
//! - The genuinely nested fields — a block's `ommers`/`transaction_hashes`, a
//!   transaction's `access_list`/`blob_versioned_hashes`/`authorization_list`, a
//!   reorg's `orphaned_hashes` — become `JSON` columns. They are the only nesting
//!   in the model, and `duckdb-rs` cannot bind lists or structs (its appender and
//!   `INSERT` binding both reject them), so JSON is the faithful carrier; query
//!   into them with `DuckDB`'s JSON functions.
//!
//! `ponytail:` one `Mutex<Connection>` serializes every publish. `DuckDB` allows a
//! single writer anyway, so this is correctness rather than a new ceiling; move to
//! a dedicated writer task if contention ever shows up, and fall back to a
//! `timescale`/Postgres-shaped store for concurrent multi-process writers.

use std::sync::Mutex;

use anyhow::Context as _;
use duckdb::{Connection, ToSql};

use crate::envelope::{Block, Envelope, Event, Finalized, Log, Receipt, Reorg, Transaction};
use crate::sink::EventSink;

/// DDL for every table, run once at connect.
///
/// `IF NOT EXISTS` makes `connect` idempotent across restarts. The plain (not
/// unique) indexes on `dedupe_key` serve the documented reader dedupe lookup
/// without failing a replay, which a unique constraint would.
const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS block (
    sequence                 UBIGINT NOT NULL,
    dedupe_key               VARCHAR NOT NULL,
    number                   UBIGINT NOT NULL,
    hash                     BLOB    NOT NULL,
    parent_hash              BLOB    NOT NULL,
    timestamp                UBIGINT NOT NULL,
    nonce                    BLOB    NOT NULL,
    ommers_hash              BLOB    NOT NULL,
    transactions_root        BLOB    NOT NULL,
    state_root               BLOB    NOT NULL,
    receipts_root            BLOB    NOT NULL,
    withdrawals_root         BLOB,
    logs_bloom               BLOB    NOT NULL,
    miner                    BLOB    NOT NULL,
    difficulty               VARCHAR NOT NULL,
    total_difficulty         VARCHAR,
    size                     VARCHAR,
    extra_data               BLOB    NOT NULL,
    gas_limit                UBIGINT NOT NULL,
    gas_used                 UBIGINT NOT NULL,
    transaction_count        UBIGINT NOT NULL,
    base_fee_per_gas         UBIGINT,
    blob_gas_used            UBIGINT,
    excess_blob_gas          UBIGINT,
    parent_beacon_block_root BLOB,
    ommers                   JSON    NOT NULL,
    transaction_hashes       JSON    NOT NULL
);
CREATE INDEX IF NOT EXISTS block_dedupe_key ON block (dedupe_key);

CREATE TABLE IF NOT EXISTS transaction (
    sequence                 UBIGINT  NOT NULL,
    dedupe_key               VARCHAR  NOT NULL,
    hash                     BLOB     NOT NULL,
    nonce                    UBIGINT  NOT NULL,
    transaction_index        UBIGINT  NOT NULL,
    from_address             BLOB     NOT NULL,
    to_address               BLOB,
    value                    VARCHAR  NOT NULL,
    gas                      UBIGINT  NOT NULL,
    gas_price                UHUGEINT,
    max_fee_per_gas          UHUGEINT NOT NULL,
    max_priority_fee_per_gas UHUGEINT,
    max_fee_per_blob_gas     UHUGEINT,
    input                    BLOB     NOT NULL,
    transaction_type         UTINYINT NOT NULL,
    chain_id                 UBIGINT,
    access_list              JSON,
    blob_versioned_hashes    JSON,
    authorization_list       JSON,
    block_timestamp          UBIGINT  NOT NULL,
    block_number             UBIGINT  NOT NULL,
    block_hash               BLOB     NOT NULL
);
CREATE INDEX IF NOT EXISTS transaction_dedupe_key ON transaction (dedupe_key);

CREATE TABLE IF NOT EXISTS receipt (
    sequence            UBIGINT  NOT NULL,
    dedupe_key          VARCHAR  NOT NULL,
    transaction_hash    BLOB     NOT NULL,
    transaction_index   UBIGINT  NOT NULL,
    from_address        BLOB     NOT NULL,
    to_address          BLOB,
    status              BOOLEAN  NOT NULL,
    transaction_type    UTINYINT NOT NULL,
    gas_used            UBIGINT  NOT NULL,
    cumulative_gas_used UBIGINT  NOT NULL,
    effective_gas_price UHUGEINT NOT NULL,
    contract_address    BLOB,
    logs_bloom          BLOB     NOT NULL,
    blob_gas_used       UBIGINT,
    blob_gas_price      UHUGEINT,
    log_count           UBIGINT  NOT NULL,
    block_number        UBIGINT  NOT NULL,
    block_hash          BLOB     NOT NULL
);
CREATE INDEX IF NOT EXISTS receipt_dedupe_key ON receipt (dedupe_key);

CREATE TABLE IF NOT EXISTS log (
    sequence          UBIGINT  NOT NULL,
    dedupe_key        VARCHAR  NOT NULL,
    log_index         UBIGINT  NOT NULL,
    transaction_hash  BLOB     NOT NULL,
    transaction_index UBIGINT  NOT NULL,
    address           BLOB     NOT NULL,
    topic0            BLOB,
    topic1            BLOB,
    topic2            BLOB,
    topic3            BLOB,
    data              BLOB     NOT NULL,
    removed           BOOLEAN  NOT NULL,
    block_number      UBIGINT  NOT NULL,
    block_hash        BLOB     NOT NULL
);
CREATE INDEX IF NOT EXISTS log_dedupe_key ON log (dedupe_key);

CREATE TABLE IF NOT EXISTS reorg (
    sequence        UBIGINT NOT NULL,
    dedupe_key      VARCHAR NOT NULL,
    height          UBIGINT NOT NULL,
    new_head_hash   BLOB    NOT NULL,
    orphaned_hashes JSON    NOT NULL
);
CREATE INDEX IF NOT EXISTS reorg_dedupe_key ON reorg (dedupe_key);

CREATE TABLE IF NOT EXISTS finalized (
    sequence   UBIGINT NOT NULL,
    dedupe_key VARCHAR NOT NULL,
    height     UBIGINT NOT NULL,
    hash       BLOB    NOT NULL
);
CREATE INDEX IF NOT EXISTS finalized_dedupe_key ON finalized (dedupe_key);";

/// Where to put the `DuckDB` database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckDbConfig {
    /// Path to the database file, or `:memory:` for an ephemeral one.
    pub path: String,
}

impl DuckDbConfig {
    /// Builds a config from a database path.
    #[must_use]
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }

    /// A config for an ephemeral in-memory database.
    #[must_use]
    pub fn in_memory() -> Self {
        Self::new(":memory:")
    }
}

/// Appends events to a local `DuckDB` database.
pub struct DuckDbSink {
    connection: Mutex<Connection>,
}

impl std::fmt::Debug for DuckDbSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDbSink").finish_non_exhaustive()
    }
}

impl DuckDbSink {
    /// Opens (or creates) the database and ensures every table exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened or the schema cannot be
    /// created.
    pub fn connect(config: &DuckDbConfig) -> anyhow::Result<Self> {
        let connection = Connection::open(config.path.as_str()).context("open DuckDB database")?;
        connection
            .execute_batch(SCHEMA)
            .context("create sink schema")?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    /// Appends one envelope to its dataset's table.
    ///
    /// # Errors
    ///
    /// Returns an error if the event cannot be rendered, the lock is poisoned, or
    /// `DuckDB` rejects the write.
    fn write(&self, envelope: &Envelope) -> anyhow::Result<()> {
        let sequence = envelope.sequence;
        let dedupe_key = envelope.event.dedupe_key();
        match &envelope.event {
            Event::Block(block) => self.append_block(sequence, &dedupe_key, block),
            Event::Transaction(transaction) => {
                self.append_transaction(sequence, &dedupe_key, transaction)
            }
            Event::Receipt(receipt) => self.append_receipt(sequence, &dedupe_key, receipt),
            Event::Log(log) => self.append_log(sequence, &dedupe_key, log),
            Event::Reorg(reorg) => self.append_reorg(sequence, &dedupe_key, reorg),
            Event::Finalized(finalized) => self.append_finalized(sequence, &dedupe_key, finalized),
        }
    }

    /// Appends one row to `table` and flushes it.
    ///
    /// The flush is the durability point; the guard never leaves this call, so the
    /// lock does not span an await.
    fn append(&self, table: &str, row: &[&dyn ToSql]) -> anyhow::Result<()> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("DuckDB connection lock poisoned"))?;
        let mut appender = connection.appender(table).context("open appender")?;
        appender.append_row(row).context("append row")?;
        appender.flush().context("flush appender")?;
        Ok(())
    }

    fn append_block(&self, sequence: u64, dedupe_key: &str, block: &Block) -> anyhow::Result<()> {
        let difficulty = block.difficulty.to_string();
        let total_difficulty = block.total_difficulty.map(|value| value.to_string());
        let size = block.size.map(|value| value.to_string());
        let withdrawals_root = block.withdrawals_root.map(|hash| hash.to_vec());
        let parent_beacon_block_root = block.parent_beacon_block_root.map(|hash| hash.to_vec());
        let ommers = json(&block.ommers)?;
        let transaction_hashes = json(&block.transaction_hashes)?;
        // Byte fields are `&[u8]`; the double reference binds the unsized slice to
        // the `&dyn ToSql` bucket the appender takes.
        let (hash, parent_hash) = (block.hash.as_slice(), block.parent_hash.as_slice());
        let (nonce, miner) = (block.nonce.as_slice(), block.miner.as_slice());
        let (ommers_hash, state_root) = (block.ommers_hash.as_slice(), block.state_root.as_slice());
        let (transactions_root, receipts_root) = (
            block.transactions_root.as_slice(),
            block.receipts_root.as_slice(),
        );
        let (logs_bloom, extra_data) = (block.logs_bloom.as_slice(), &block.extra_data[..]);
        let row: &[&dyn ToSql] = &[
            &sequence,
            &dedupe_key,
            &block.number,
            &hash,
            &parent_hash,
            &block.timestamp,
            &nonce,
            &ommers_hash,
            &transactions_root,
            &state_root,
            &receipts_root,
            &withdrawals_root,
            &logs_bloom,
            &miner,
            &difficulty,
            &total_difficulty,
            &size,
            &extra_data,
            &block.gas_limit,
            &block.gas_used,
            &block.transaction_count,
            &block.base_fee_per_gas,
            &block.blob_gas_used,
            &block.excess_blob_gas,
            &parent_beacon_block_root,
            &ommers,
            &transaction_hashes,
        ];
        self.append("block", row)
    }

    fn append_transaction(
        &self,
        sequence: u64,
        dedupe_key: &str,
        transaction: &Transaction,
    ) -> anyhow::Result<()> {
        let value = transaction.value.to_string();
        let to_address = transaction.to.map(|address| address.to_vec());
        let access_list = opt_json(transaction.access_list.as_ref())?;
        let blob_versioned_hashes = opt_json(transaction.blob_versioned_hashes.as_ref())?;
        let authorization_list = opt_json(transaction.authorization_list.as_ref())?;
        let (hash, from) = (transaction.hash.as_slice(), transaction.from.as_slice());
        let (input, block_hash) = (&transaction.input[..], transaction.block_hash.as_slice());
        let row: &[&dyn ToSql] = &[
            &sequence,
            &dedupe_key,
            &hash,
            &transaction.nonce,
            &transaction.transaction_index,
            &from,
            &to_address,
            &value,
            &transaction.gas,
            &transaction.gas_price,
            &transaction.max_fee_per_gas,
            &transaction.max_priority_fee_per_gas,
            &transaction.max_fee_per_blob_gas,
            &input,
            &transaction.transaction_type,
            &transaction.chain_id,
            &access_list,
            &blob_versioned_hashes,
            &authorization_list,
            &transaction.block_timestamp,
            &transaction.block_number,
            &block_hash,
        ];
        self.append("transaction", row)
    }

    fn append_receipt(
        &self,
        sequence: u64,
        dedupe_key: &str,
        receipt: &Receipt,
    ) -> anyhow::Result<()> {
        let to_address = receipt.to.map(|address| address.to_vec());
        let contract_address = receipt.contract_address.map(|address| address.to_vec());
        let (transaction_hash, from) =
            (receipt.transaction_hash.as_slice(), receipt.from.as_slice());
        let (logs_bloom, block_hash) =
            (receipt.logs_bloom.as_slice(), receipt.block_hash.as_slice());
        let row: &[&dyn ToSql] = &[
            &sequence,
            &dedupe_key,
            &transaction_hash,
            &receipt.transaction_index,
            &from,
            &to_address,
            &receipt.status,
            &receipt.transaction_type,
            &receipt.gas_used,
            &receipt.cumulative_gas_used,
            &receipt.effective_gas_price,
            &contract_address,
            &logs_bloom,
            &receipt.blob_gas_used,
            &receipt.blob_gas_price,
            &receipt.log_count,
            &receipt.block_number,
            &block_hash,
        ];
        self.append("receipt", row)
    }

    fn append_log(&self, sequence: u64, dedupe_key: &str, log: &Log) -> anyhow::Result<()> {
        let topic0 = log.topic0.map(|topic| topic.to_vec());
        let topic1 = log.topic1.map(|topic| topic.to_vec());
        let topic2 = log.topic2.map(|topic| topic.to_vec());
        let topic3 = log.topic3.map(|topic| topic.to_vec());
        let (transaction_hash, address) = (log.transaction_hash.as_slice(), log.address.as_slice());
        let (data, block_hash) = (&log.data[..], log.block_hash.as_slice());
        let row: &[&dyn ToSql] = &[
            &sequence,
            &dedupe_key,
            &log.log_index,
            &transaction_hash,
            &log.transaction_index,
            &address,
            &topic0,
            &topic1,
            &topic2,
            &topic3,
            &data,
            &log.removed,
            &log.block_number,
            &block_hash,
        ];
        self.append("log", row)
    }

    fn append_reorg(&self, sequence: u64, dedupe_key: &str, reorg: &Reorg) -> anyhow::Result<()> {
        let orphaned_hashes = json(&reorg.orphaned_hashes)?;
        let new_head_hash = reorg.new_head_hash.as_slice();
        let row: &[&dyn ToSql] = &[
            &sequence,
            &dedupe_key,
            &reorg.height,
            &new_head_hash,
            &orphaned_hashes,
        ];
        self.append("reorg", row)
    }

    fn append_finalized(
        &self,
        sequence: u64,
        dedupe_key: &str,
        finalized: &Finalized,
    ) -> anyhow::Result<()> {
        let hash = finalized.hash.as_slice();
        let row: &[&dyn ToSql] = &[&sequence, &dedupe_key, &finalized.height, &hash];
        self.append("finalized", row)
    }
}

impl EventSink for DuckDbSink {
    async fn publish(&self, envelope: &Envelope) -> anyhow::Result<()> {
        self.write(envelope)
    }
}

/// Serializes a nested value to the compact JSON a `JSON` column holds.
fn json<T: serde::Serialize + ?Sized>(value: &T) -> anyhow::Result<String> {
    Ok(serde_json::to_string(value)?)
}

/// Serializes an optional nested value, or `NULL` when absent.
fn opt_json<T: serde::Serialize>(value: Option<&T>) -> anyhow::Result<Option<String>> {
    value.map(json).transpose()
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{Address, B256, TxHash};

    use crate::envelope::{
        Block, ChainId, Envelope, Event, Finalized, Log, Receipt, Reorg, Transaction,
    };
    use crate::sink::duckdb::{DuckDbConfig, DuckDbSink};

    fn sink() -> DuckDbSink {
        DuckDbSink::connect(&DuckDbConfig::in_memory()).expect("open in-memory DuckDB")
    }

    fn hash(byte: u8) -> B256 {
        B256::from([byte; 32])
    }

    /// Publishes one of every variant: four datasets into four tables and the two
    /// control signals into their own.
    #[test]
    fn each_variant_lands_in_its_own_table() {
        let sink = sink();
        let events = [
            Event::Block(Box::new(Block {
                number: 42,
                hash: hash(0x11),
                parent_hash: hash(0x10),
                timestamp: 1_700_000_000,
                transaction_hashes: vec![TxHash::from([0xab; 32])],
                ..Block::default()
            })),
            Event::Transaction(Box::new(Transaction {
                hash: TxHash::from([0xab; 32]),
                nonce: 7,
                from: Address::from([0x22; 20]),
                to: Some(Address::from([0x33; 20])),
                chain_id: Some(8453),
                gas_price: Some(1_000_000_000),
                block_number: 42,
                block_hash: hash(0x11),
                ..Transaction::default()
            })),
            Event::Receipt(Box::new(Receipt {
                transaction_hash: TxHash::from([0xab; 32]),
                status: true,
                block_number: 42,
                block_hash: hash(0x11),
                ..Receipt::default()
            })),
            Event::Log(Box::new(Log {
                log_index: 3,
                transaction_hash: TxHash::from([0xab; 32]),
                address: Address::from([0x44; 20]),
                topic0: Some(hash(0x07)),
                block_number: 42,
                block_hash: hash(0x11),
                ..Log::default()
            })),
            Event::Reorg(Reorg {
                height: 40,
                new_head_hash: hash(0x12),
                orphaned_hashes: vec![hash(0x13)],
            }),
            Event::Finalized(Finalized {
                height: 39,
                hash: hash(0x14),
            }),
        ];
        for (sequence, event) in events.into_iter().enumerate() {
            let sequence = u64::try_from(sequence).expect("index fits");
            sink.write(&Envelope::new(ChainId::new("base"), sequence, event))
                .expect("event writes");
        }

        let connection = sink.connection.lock().expect("lock");
        for table in [
            "block",
            "transaction",
            "receipt",
            "log",
            "reorg",
            "finalized",
        ] {
            let count: u64 = connection
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .expect("count reads");
            assert_eq!(count, 1, "{table} should hold exactly one row");
        }
    }

    /// The typed columns carry the scalar fields; `U256` is a decimal string and
    /// a null topic is `NULL`, not a zero hash.
    #[test]
    fn scalar_fields_map_to_typed_columns() {
        let sink = sink();
        sink.write(&Envelope::new(
            ChainId::new("base"),
            5,
            Event::Log(Box::new(Log {
                log_index: 3,
                transaction_hash: TxHash::from([0xab; 32]),
                address: Address::from([0x44; 20]),
                topic0: Some(hash(0x07)),
                data: alloy_primitives::Bytes::from_static(&[0xde, 0xad]),
                removed: false,
                block_number: 42,
                block_hash: hash(0x11),
                ..Log::default()
            })),
        ))
        .expect("log writes");

        let connection = sink.connection.lock().expect("lock");
        let (sequence, index, topic0, topic1, data, block_number): (
            u64,
            u64,
            Vec<u8>,
            Option<Vec<u8>>,
            Vec<u8>,
            u64,
        ) = connection
            .query_row(
                "SELECT sequence, log_index, topic0, topic1, data, block_number FROM log",
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
            .expect("row reads back");
        assert_eq!(sequence, 5);
        assert_eq!(index, 3);
        assert_eq!(topic0, hash(0x07).to_vec());
        assert_eq!(topic1, None, "an absent topic is NULL");
        assert_eq!(data, vec![0xde, 0xad]);
        assert_eq!(block_number, 42);
    }

    /// A `U256` renders as its full decimal string, and a nested list round-trips
    /// through the `JSON` column.
    #[test]
    fn wide_integers_and_nested_lists_survive() {
        let sink = sink();
        let difficulty = alloy_primitives::U256::MAX;
        sink.write(&Envelope::new(
            ChainId::new("base"),
            1,
            Event::Block(Box::new(Block {
                number: 1,
                hash: hash(0x11),
                difficulty,
                ommers: vec![hash(0x30)],
                transaction_hashes: vec![TxHash::from([0xab; 32])],
                ..Block::default()
            })),
        ))
        .expect("block writes");

        let connection = sink.connection.lock().expect("lock");
        let (stored_difficulty, ommers, hashes): (String, String, String) = connection
            .query_row(
                "SELECT difficulty, ommers, transaction_hashes FROM block",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("row reads back");
        assert_eq!(stored_difficulty, difficulty.to_string());
        assert_eq!(
            serde_json::from_str::<Vec<String>>(&ommers).expect("ommers is a JSON list"),
            vec![format!("0x{}", "30".repeat(32))]
        );
        assert_eq!(
            serde_json::from_str::<Vec<String>>(&hashes).expect("hashes are a JSON list"),
            vec![format!("0x{}", "ab".repeat(32))]
        );
    }

    /// Connecting twice to the same file must not fail on the existing schema.
    #[test]
    fn connect_is_idempotent() {
        let path = std::env::temp_dir().join(format!("indexer-sink-{}.duckdb", std::process::id()));
        let config = DuckDbConfig::new(path.to_string_lossy().into_owned());
        DuckDbSink::connect(&config).expect("first connect creates the schema");
        DuckDbSink::connect(&config).expect("second connect reuses the schema");
        // Leave nothing behind; the lock is released when the sinks drop.
        std::fs::remove_file(&path).expect("remove temp database");
    }
}
