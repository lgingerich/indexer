//! A dataset rendered as storage-agnostic rows: what a store persists, and nothing
//! about how.
//!
//! # Why this exists
//!
//! The question "what columns does a log have, and what type is each?" is not a `DuckDB`
//! question. It is the same question for every store — `ClickHouse`, a Parquet file, a
//! Kafka topic — and answering it once per store means the second one re-derives it and
//! drifts. So the answer lives here, beside the datasets it is about, and a store is left
//! with only what is genuinely its own: the DDL, the mapping from these types to its own,
//! and the commit.
//!
//! # A row is a header and its values
//!
//! [`Row`] pairs a [`Table`] with its columns and the values that line up with them
//! positionally. The pairing is the contract, and it is enforced once in [`Row::new`]
//! rather than trusted: the compiler cannot count a `vec!` against a header, so a
//! mismatch is a construction error instead of a row nobody notices.
//!
//! The column *type* is part of the header, not a parallel list, for the same reason — a
//! name and a type are one fact about a column, and splitting them is how a schema and its
//! data come to disagree. Each store maps [`ColumnType`] to its own vocabulary; a
//! `ClickHouse` sink and a `DuckDB` one would both read the same headers and produce
//! different DDL, which is the point.
//!
//! # Why these types
//!
//! [`ColumnType`] is the intersection of what these targets can express, not a lowest
//! common denominator of one engine's quirks:
//!
//! - [`Uint`](ColumnType::Uint) — block numbers, indices, gas. 64-bit *unsigned*, so a
//!   store must not reach for a signed 64-bit column and silently lose the top half of the
//!   range.
//! - [`Text`](ColumnType::Text) — hashes, addresses, and `U256` values, all as the `0x`
//!   hex the node itself sends, so a value here compares equal to the same value read out
//!   of a raw JSON document. That is what makes a typed table a rewrite rather than a
//!   second dialect.
//! - [`Bool`](ColumnType::Bool) — receipt status, a log's `removed` flag.
//! - [`Document`](ColumnType::Document) — a value that does not flatten into scalars. Two
//!   uses today: a decoded record's arguments, which vary per event, and the hash lists a
//!   block carries, whose length is known only at read time.
//!
//! Absence is not a type. A field the chain does not have is a [`ColumnValue::Null`] in a
//! column that otherwise holds its real type, because `NULL` and "no such field" are
//! different questions and only one of them is answered by the column's type.

use std::fmt;

use alloy_primitives::{Bytes, U256};

use crate::wire::datasets::evm::{Block, Log, Receipt, Transaction};
use crate::wire::envelope::{ChainId, Decoded, Event, Finalized, Reorg};

/// One table a store persists.
///
/// A dataset per table, not an event per row in one table: a `log` is not a `receipt` with
/// most fields null, and a `reorg` has no dataset payload at all, so a single wide table is
/// mostly nulls and a consumer has to filter on a discriminator to reach real data.
///
/// Named rather than stringly typed so a store matches on it exhaustively, and a new
/// dataset is a compile error at every store rather than a table nothing writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Table {
    /// Block headers and metadata.
    Block,
    /// Transactions, from their block's array.
    Transaction,
    /// Transaction receipts.
    Receipt,
    /// Logs, one row per log.
    Log,
    /// Decoded event records, whose arguments ride as a document.
    Decoded,
    /// Reorg markers: which block hashes stopped being canonical.
    Reorg,
    /// Finality watermarks.
    Finalized,
}

impl Table {
    /// Every table, in the order a store would create them.
    ///
    /// A `const` list rather than a derive: seven cases do not justify a dependency, and a
    /// hand-written list is one a reader can check.
    pub const ALL: [Self; 7] = [
        Self::Block,
        Self::Transaction,
        Self::Receipt,
        Self::Log,
        Self::Decoded,
        Self::Reorg,
        Self::Finalized,
    ];

    /// The name a store files this table under.
    ///
    /// Singular, matching the dataset names rather than SQL's usual plural, so the table,
    /// the Rust type, and the event's `type` tag all say the same word.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Block => "block",
            Self::Transaction => "transaction",
            Self::Receipt => "receipt",
            Self::Log => "log",
            Self::Decoded => "decoded",
            Self::Reorg => "reorg",
            Self::Finalized => "finalized",
        }
    }

    /// This table's columns, in order, each with the type it stores.
    ///
    /// Every header ends with [`COMMON_COLUMNS`]; a test checks that, so a table cannot
    /// quietly become unkeyable.
    #[must_use]
    pub const fn columns(self) -> &'static [Column] {
        match self {
            Self::Block => BLOCK_COLUMNS,
            Self::Transaction => TRANSACTION_COLUMNS,
            Self::Receipt => RECEIPT_COLUMNS,
            Self::Log => LOG_COLUMNS,
            Self::Decoded => DECODED_COLUMNS,
            Self::Reorg => REORG_COLUMNS,
            Self::Finalized => FINALIZED_COLUMNS,
        }
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// One column of a table: its name and what it stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Column {
    /// The name, which is the field's own — a chain field keeps its chain name
    /// (`from_address` rather than `from`, since `from` is a SQL keyword), and a store may
    /// rename it.
    pub name: &'static str,
    /// What the column holds.
    pub kind: ColumnType,
}

impl Column {
    /// A text column, the shape of every hash and address.
    const fn text(name: &'static str) -> Self {
        Self {
            name,
            kind: ColumnType::Text,
        }
    }

    /// An unsigned 64-bit column, for a number, an index, or a gas figure.
    const fn uint(name: &'static str) -> Self {
        Self {
            name,
            kind: ColumnType::Uint,
        }
    }

    /// A boolean column.
    const fn boolean(name: &'static str) -> Self {
        Self {
            name,
            kind: ColumnType::Bool,
        }
    }

    /// A document column, for a value that does not flatten.
    const fn document(name: &'static str) -> Self {
        Self {
            name,
            kind: ColumnType::Document,
        }
    }
}

/// What a column holds, in the vocabulary every target shares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    /// A 64-bit unsigned integer: a block number, an index, or a gas *amount*.
    ///
    /// 64 bits because that is what the protocol bounds those to, and what `alloy` types
    /// them as: `gas`, `gas_used`, `gas_limit`, `cumulative_gas_used` and every index are
    /// `u64` in both `alloy-consensus` and `alloy-rpc-types-eth`. A store using a signed
    /// 64-bit column would lose the top half of the range.
    Uint,
    /// A `0x` hex quantity or a fixed-width hash: a block or transaction hash, an address,
    /// and every price in wei.
    ///
    /// Prices are here rather than in [`Uint`](Self::Uint) because they are 128 bits — a
    /// `u128` in `alloy`, and a `*big.Int` in `go-ethereum`, where gas *amounts* are
    /// `uint64` and monetary values deliberately are not. `u64::MAX` wei is 18.4 ETH, and
    /// EIP-1559's 12.5% per-block growth reaches that from 1 gwei in 201 blocks, so the
    /// wider range is one a price spike actually occupies.
    ///
    /// Hex rather than a numeric column, because hex is what the node sends: a value read
    /// from a typed column compares equal to the same value in a raw RPC response. A
    /// store that wants to compute on it casts at query time.
    Text,
    /// A boolean.
    Bool,
    /// A structured value that does not flatten into scalars.
    Document,
}

/// The two columns every table carries, and the reason a store can key and partition
/// without knowing which dataset a row came from.
///
/// `chain` because one store may hold several chains and a key is scoped to its chain;
/// `dedupe_key` because that is the row's identity — the value a store deduplicates and
/// upserts on.
pub const COMMON_COLUMNS: [Column; 2] = [Column::text("chain"), Column::text("dedupe_key")];

/// One value in a row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnValue {
    /// Absent: the chain has no such field for this record. Never a stand-in for zero or
    /// empty, which are values the chain does have.
    Null,
    /// A 64-bit unsigned integer.
    Uint(u64),
    /// Text in the node's own encoding: lowercase `0x` hex.
    Text(String),
    /// A boolean.
    Bool(bool),
    /// A structured value that does not flatten.
    ///
    /// A distinct variant rather than `Text` so a store can type the column as a document
    /// instead of inferring that from a string.
    Document(String),
}

impl ColumnValue {
    /// A hash, address, or any other `0x`-hex fixed-width value.
    ///
    /// One constructor for all of them, so "how does a `B256` render" has one answer in
    /// one place rather than one per call site.
    #[must_use]
    pub fn hex<H: fmt::LowerHex>(value: H) -> Self {
        Self::Text(format!("{value:#x}"))
    }

    /// A `U256`, as `0x` hex like every other large value.
    ///
    /// 256 bits has no exact numeric column in any of these targets without a decimal width,
    /// and hex is what the node sends.
    #[must_use]
    pub fn u256(value: U256) -> Self {
        Self::Text(format!("{value:#x}"))
    }

    /// A 128-bit price in wei, as `0x` hex like every other quantity.
    ///
    /// Named for what it is rather than left to a `u128 as` cast at the call site, because
    /// the width is the whole reason a price is not a [`Uint`](Self::Uint): `u64::MAX` wei
    /// is 18.4 ETH, and EIP-1559's 12.5% per-block growth passes that from 1 gwei in 201
    /// blocks. A store stores it as text, because hex is what the node sends; the distinct
    /// constructor is here so a reader can see *why* a price is not a number here.
    #[must_use]
    pub fn wei(value: u128) -> Self {
        Self::Text(format!("{value:#x}"))
    }

    /// Dynamic bytes, as `0x` hex, so calldata and log data are shaped like every other
    /// value rather than arriving as a blob a consumer decodes differently.
    #[must_use]
    pub fn bytes(value: &Bytes) -> Self {
        Self::Text(format!("{value:#x}"))
    }

    /// An optional value: `None` is [`Null`](Self::Null), not a default.
    ///
    /// The one place absence is decided, so no store has to ask whether an absent field
    /// and a zero are the same thing. They are not.
    #[must_use]
    pub fn some<T>(value: Option<T>, render: impl FnOnce(T) -> Self) -> Self {
        value.map_or(Self::Null, render)
    }

    /// An optional document, as a document or as null.
    ///
    /// Absent is null rather than a `"null"` document, so a consumer filtering
    /// `WHERE access_list IS NOT NULL` gets the transactions that have one.
    #[must_use]
    pub fn optional_document<T: serde::Serialize>(value: &Option<T>) -> Self {
        value
            .as_ref()
            .map_or(Self::Null, |inner| Self::Document(json(inner)))
    }

    /// A required document, for a value that is always present.
    #[must_use]
    pub fn document<T: serde::Serialize>(value: &T) -> Self {
        Self::Document(json(value))
    }

    /// A JSON array of hashes, for a list whose length is known only at read time.
    ///
    /// A [`Document`](Self::Document) rather than text, because the columns holding one
    /// declare themselves documents: a store types the column as a document from the
    /// header, and a value that arrived as a string would contradict it.
    #[must_use]
    pub fn hex_list<H: serde::Serialize>(values: &[H]) -> Self {
        Self::Document(json(&values))
    }
}

/// Renders any serializable value as JSON, with a total fallback.
///
/// The values rendered here are plain data — hashes, lists, ABI values — so a failure is
/// not reachable. This is the one place that says so, rather than a `to_string` followed
/// by an `unwrap` at each of the twenty call sites that would otherwise need one.
fn json<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| String::from("null"))
}

/// One row's worth of a dataset: the table, and the values in its column order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    table: Table,
    values: Vec<ColumnValue>,
}

impl Row {
    /// Builds a row for `table`, taking the values in [`Table::columns`] order.
    ///
    /// # Errors
    ///
    /// Returns an error if `values` does not have one entry per column.
    pub fn new(table: Table, values: Vec<ColumnValue>) -> Result<Self, RowError> {
        let columns = table.columns().len();
        if values.len() != columns {
            return Err(RowError::Width {
                table,
                columns,
                values: values.len(),
            });
        }
        Ok(Self { table, values })
    }

    /// Which table this row belongs to.
    #[must_use]
    pub const fn table(&self) -> Table {
        self.table
    }

    /// This table's columns, in the order [`values`](Self::values) are given.
    #[must_use]
    pub fn columns(&self) -> &'static [Column] {
        self.table.columns()
    }

    /// The values, in [`columns`](Self::columns) order.
    #[must_use]
    pub fn values(&self) -> &[ColumnValue] {
        &self.values
    }

    /// The value in the named column.
    ///
    /// # Panics
    ///
    /// Panics if `column` is not in this table, which is a bug in the caller rather than a
    /// runtime condition — this exists for the two columns every table has, and for tests.
    #[must_use]
    pub fn value(&self, column: &str) -> &ColumnValue {
        let index = self
            .table
            .columns()
            .iter()
            .position(|col| col.name == column)
            .unwrap_or_else(|| panic!("{column} is a column of the {} table", self.table));
        &self.values[index]
    }

    /// The named column's value as text.
    ///
    /// # Panics
    ///
    /// Panics if the column is absent or does not hold text.
    #[must_use]
    pub fn text(&self, column: &str) -> &str {
        match self.value(column) {
            ColumnValue::Text(value) => value,
            other => panic!("{column} does not hold text, it holds {other:?}"),
        }
    }

    /// The row's identity: the key a store deduplicates and upserts on.
    #[must_use]
    pub fn dedupe_key(&self) -> &str {
        self.text("dedupe_key")
    }

    /// The chain this row came from.
    #[must_use]
    pub fn chain(&self) -> &str {
        self.text("chain")
    }
}

/// A row that does not match its own table's columns.
#[derive(Debug, thiserror::Error)]
pub enum RowError {
    /// The values and the columns disagree on how many there are.
    #[error("the {table} table has {columns} columns but the row has {values} values")]
    Width {
        /// The table the row was for.
        table: Table,
        /// How many columns the table declares.
        columns: usize,
        /// How many values were given.
        values: usize,
    },
}

/// Renders an event as the row a store persists.
///
/// Every event has exactly one row, a control signal included — a reorg carries only what
/// the signal says, but it is still a row, so a store has no variant to handle and no
/// event that is silently dropped.
///
/// # Errors
///
/// Returns an error if a row's values do not match its table's columns, which the
/// constants here make unreachable and which is therefore the check that keeps them
/// honest.
pub fn row_for(chain: &ChainId, event: &Event) -> Result<Row, RowError> {
    let common = || {
        vec![
            ColumnValue::Text(chain.as_str().to_owned()),
            ColumnValue::Text(event.dedupe_key()),
        ]
    };
    let build = |table: Table, mut values: Vec<ColumnValue>| {
        values.extend(common());
        Row::new(table, values)
    };

    match event {
        Event::Block(b) => build(Table::Block, block_values(b)),
        Event::Transaction(t) => build(Table::Transaction, transaction_values(t)),
        Event::Receipt(r) => build(Table::Receipt, receipt_values(r)),
        Event::Log(l) => build(Table::Log, log_values(l)),
        Event::Reorg(r) => build(Table::Reorg, reorg_values(r)),
        Event::Finalized(f) => build(Table::Finalized, finalized_values(f)),
        Event::Decoded(d) => build(Table::Decoded, decoded_values(d)),
    }
}

/// The `block` table.
const BLOCK_COLUMNS: &[Column] = &[
    Column::uint("number"),
    Column::text("hash"),
    Column::text("parent_hash"),
    Column::uint("timestamp"),
    Column::text("nonce"),
    Column::text("ommers_hash"),
    Column::text("transactions_root"),
    Column::text("state_root"),
    Column::text("receipts_root"),
    Column::text("withdrawals_root"),
    Column::text("logs_bloom"),
    Column::text("miner"),
    Column::text("difficulty"),
    Column::text("total_difficulty"),
    Column::text("size"),
    Column::text("extra_data"),
    Column::uint("gas_limit"),
    Column::uint("gas_used"),
    Column::uint("transaction_count"),
    Column::uint("base_fee_per_gas"),
    Column::uint("blob_gas_used"),
    Column::uint("excess_blob_gas"),
    Column::text("parent_beacon_block_root"),
    Column::document("ommers"),
    Column::document("transaction_hashes"),
    COMMON_COLUMNS[0],
    COMMON_COLUMNS[1],
];

fn block_values(b: &Block) -> Vec<ColumnValue> {
    vec![
        ColumnValue::Uint(b.number),
        ColumnValue::hex(b.hash),
        ColumnValue::hex(b.parent_hash),
        ColumnValue::Uint(b.timestamp),
        ColumnValue::hex(b.nonce),
        ColumnValue::hex(b.ommers_hash),
        ColumnValue::hex(b.transactions_root),
        ColumnValue::hex(b.state_root),
        ColumnValue::hex(b.receipts_root),
        ColumnValue::some(b.withdrawals_root, ColumnValue::hex),
        ColumnValue::hex(b.logs_bloom),
        ColumnValue::hex(b.miner),
        ColumnValue::u256(b.difficulty),
        ColumnValue::some(b.total_difficulty, ColumnValue::u256),
        ColumnValue::some(b.size, ColumnValue::u256),
        ColumnValue::bytes(&b.extra_data),
        ColumnValue::Uint(b.gas_limit),
        ColumnValue::Uint(b.gas_used),
        ColumnValue::Uint(b.transaction_count),
        ColumnValue::some(b.base_fee_per_gas, ColumnValue::Uint),
        ColumnValue::some(b.blob_gas_used, ColumnValue::Uint),
        ColumnValue::some(b.excess_blob_gas, ColumnValue::Uint),
        ColumnValue::some(b.parent_beacon_block_root, ColumnValue::hex),
        ColumnValue::hex_list(&b.ommers),
        ColumnValue::hex_list(&b.transaction_hashes),
    ]
}

/// The `transaction` table.
const TRANSACTION_COLUMNS: &[Column] = &[
    Column::text("hash"),
    Column::uint("nonce"),
    Column::uint("transaction_index"),
    Column::text("from_address"),
    Column::text("to_address"),
    Column::text("value"),
    Column::uint("gas"),
    Column::text("gas_price"),
    Column::text("max_fee_per_gas"),
    Column::text("max_priority_fee_per_gas"),
    Column::text("max_fee_per_blob_gas"),
    Column::text("input"),
    Column::uint("transaction_type"),
    Column::uint("chain_id"),
    Column::document("access_list"),
    Column::document("blob_versioned_hashes"),
    Column::document("authorization_list"),
    Column::uint("block_timestamp"),
    Column::uint("block_number"),
    Column::text("block_hash"),
    COMMON_COLUMNS[0],
    COMMON_COLUMNS[1],
];

fn transaction_values(t: &Transaction) -> Vec<ColumnValue> {
    vec![
        ColumnValue::hex(t.hash),
        ColumnValue::Uint(t.nonce),
        ColumnValue::Uint(t.transaction_index),
        ColumnValue::hex(t.from),
        ColumnValue::some(t.to, ColumnValue::hex),
        ColumnValue::u256(t.value),
        ColumnValue::Uint(t.gas),
        ColumnValue::some(t.gas_price, ColumnValue::wei),
        ColumnValue::wei(t.max_fee_per_gas),
        ColumnValue::some(t.max_priority_fee_per_gas, ColumnValue::wei),
        ColumnValue::some(t.max_fee_per_blob_gas, ColumnValue::wei),
        ColumnValue::bytes(&t.input),
        ColumnValue::Uint(u64::from(t.transaction_type)),
        ColumnValue::some(t.chain_id, ColumnValue::Uint),
        ColumnValue::optional_document(&t.access_list),
        ColumnValue::optional_document(&t.blob_versioned_hashes),
        ColumnValue::optional_document(&t.authorization_list),
        ColumnValue::Uint(t.block_timestamp),
        ColumnValue::Uint(t.block_number),
        ColumnValue::hex(t.block_hash),
    ]
}

/// The `receipt` table.
const RECEIPT_COLUMNS: &[Column] = &[
    Column::text("transaction_hash"),
    Column::uint("transaction_index"),
    Column::text("from_address"),
    Column::text("to_address"),
    Column::boolean("status"),
    Column::uint("transaction_type"),
    Column::uint("gas_used"),
    Column::uint("cumulative_gas_used"),
    Column::text("effective_gas_price"),
    Column::text("contract_address"),
    Column::text("logs_bloom"),
    Column::uint("blob_gas_used"),
    Column::text("blob_gas_price"),
    Column::uint("log_count"),
    Column::uint("block_number"),
    Column::text("block_hash"),
    COMMON_COLUMNS[0],
    COMMON_COLUMNS[1],
];

fn receipt_values(r: &Receipt) -> Vec<ColumnValue> {
    vec![
        ColumnValue::hex(r.transaction_hash),
        ColumnValue::Uint(r.transaction_index),
        ColumnValue::hex(r.from),
        ColumnValue::some(r.to, ColumnValue::hex),
        ColumnValue::Bool(r.status),
        ColumnValue::Uint(u64::from(r.transaction_type)),
        ColumnValue::Uint(r.gas_used),
        ColumnValue::Uint(r.cumulative_gas_used),
        ColumnValue::wei(r.effective_gas_price),
        ColumnValue::some(r.contract_address, ColumnValue::hex),
        ColumnValue::hex(r.logs_bloom),
        ColumnValue::some(r.blob_gas_used, ColumnValue::Uint),
        ColumnValue::some(r.blob_gas_price, ColumnValue::wei),
        ColumnValue::Uint(r.log_count),
        ColumnValue::Uint(r.block_number),
        ColumnValue::hex(r.block_hash),
    ]
}

/// The `log` table.
const LOG_COLUMNS: &[Column] = &[
    Column::uint("log_index"),
    Column::text("transaction_hash"),
    Column::uint("transaction_index"),
    Column::text("address"),
    Column::text("topic0"),
    Column::text("topic1"),
    Column::text("topic2"),
    Column::text("topic3"),
    Column::text("data"),
    Column::boolean("removed"),
    Column::uint("block_number"),
    Column::text("block_hash"),
    Column::uint("block_timestamp"),
    COMMON_COLUMNS[0],
    COMMON_COLUMNS[1],
];

fn log_values(l: &Log) -> Vec<ColumnValue> {
    vec![
        ColumnValue::Uint(l.log_index),
        ColumnValue::hex(l.transaction_hash),
        ColumnValue::Uint(l.transaction_index),
        ColumnValue::hex(l.address),
        ColumnValue::some(l.topic0, ColumnValue::hex),
        ColumnValue::some(l.topic1, ColumnValue::hex),
        ColumnValue::some(l.topic2, ColumnValue::hex),
        ColumnValue::some(l.topic3, ColumnValue::hex),
        ColumnValue::bytes(&l.data),
        ColumnValue::Bool(l.removed),
        ColumnValue::Uint(l.block_number),
        ColumnValue::hex(l.block_hash),
        ColumnValue::Uint(l.block_timestamp),
    ]
}

/// The `reorg` table.
///
/// `orphaned_hashes` is a document rather than a set of rows, and it is the one event
/// where the list *is* the payload: it names every block hash that stopped being
/// canonical. The count is small and known only at read time, so a list column beats a
/// child table a consumer has to join to answer "is this block canonical yet?".
const REORG_COLUMNS: &[Column] = &[
    Column::uint("height"),
    Column::text("new_head_hash"),
    Column::document("orphaned_hashes"),
    COMMON_COLUMNS[0],
    COMMON_COLUMNS[1],
];

fn reorg_values(r: &Reorg) -> Vec<ColumnValue> {
    vec![
        ColumnValue::Uint(r.height),
        ColumnValue::hex(r.new_head_hash),
        ColumnValue::hex_list(&r.orphaned_hashes),
    ]
}

/// The `finalized` table.
const FINALIZED_COLUMNS: &[Column] = &[
    Column::uint("height"),
    Column::text("hash"),
    COMMON_COLUMNS[0],
    COMMON_COLUMNS[1],
];

fn finalized_values(f: &Finalized) -> Vec<ColumnValue> {
    vec![ColumnValue::Uint(f.height), ColumnValue::hex(f.hash)]
}

/// The `decoded` table.
///
/// One `record` column, and that is the whole reason this table is different: a decoded
/// record's arguments vary per event, so `amount0` may be a `uint256` on one event and an
/// `address` on another, and there is no fixed column set to lift. The identity columns
/// are still typed, so a store can key and join on this row without reading the record.
///
/// Not a reason to drop the dataset — an unstored decoded row is data loss — so the row
/// exists and the flattening is what waits.
const DECODED_COLUMNS: &[Column] = &[
    Column::document("record"),
    COMMON_COLUMNS[0],
    COMMON_COLUMNS[1],
];

fn decoded_values(d: &Decoded) -> Vec<ColumnValue> {
    vec![ColumnValue::document(&d)]
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{Address, B256, TxHash};

    use crate::wire::envelope::{
        Block, ChainId, Decoded, Event, Finalized, Log, Receipt, Reorg, Transaction,
    };

    use super::{COMMON_COLUMNS, ColumnType, ColumnValue, Row, Table, row_for};

    fn hash(byte: u8) -> B256 {
        B256::from([byte; 32])
    }

    fn chain() -> ChainId {
        ChainId::new("ethereum")
    }

    fn every_kind() -> Vec<Event> {
        vec![
            Event::Block(Box::new(Block {
                number: 100,
                hash: hash(0x01),
                // Populated, not defaulted: an empty list still renders a document, but
                // a real one is what a mismatched variant shows up against.
                ommers: vec![hash(0x02)],
                transaction_hashes: vec![TxHash::from([0x11; 32])],
                ..Block::default()
            })),
            Event::Transaction(Box::new(Transaction {
                hash: TxHash::from([0x11; 32]),
                block_number: 100,
                block_hash: hash(0x01),
                ..Transaction::default()
            })),
            Event::Receipt(Box::new(Receipt {
                transaction_hash: TxHash::from([0x11; 32]),
                block_number: 100,
                block_hash: hash(0x01),
                ..Receipt::default()
            })),
            Event::Log(Box::new(Log {
                log_index: 7,
                transaction_hash: TxHash::from([0x11; 32]),
                block_number: 100,
                block_hash: hash(0x01),
                ..Log::default()
            })),
            Event::Decoded(Box::new(Decoded {
                name: "Swap".to_owned(),
                address: Address::from([0xd0; 20]),
                protocol: "uniswap_v3".to_owned(),
                selector: hash(0x07),
                signature: "Swap(address)".to_owned(),
                anonymous: false,
                transaction_hash: TxHash::from([0x11; 32]),
                transaction_index: 3,
                log_index: 7,
                indexed: Vec::new(),
                body: Vec::new(),
                block_number: 100,
                block_hash: hash(0x01),
                block_timestamp: 1_700_000_000,
            })),
            Event::Reorg(Reorg {
                height: 100,
                new_head_hash: hash(0x01),
                orphaned_hashes: vec![hash(0x02)],
            }),
            Event::Finalized(Finalized {
                height: 100,
                hash: hash(0x01),
            }),
        ]
    }

    /// Every event is a row, so a store has no variant to handle and nothing is silently
    /// dropped. A control signal is a row like any other.
    #[test]
    fn every_event_becomes_exactly_one_row() {
        let events = every_kind();
        assert_eq!(events.len(), Table::ALL.len(), "one fixture per table");
        for event in &events {
            let row = row_for(&chain(), event).expect("every event renders");
            assert_eq!(row.values().len(), row.columns().len());
            assert_eq!(row.chain(), "ethereum");
            assert_eq!(row.dedupe_key(), event.dedupe_key());
        }
    }

    /// The positional contract is enforced once, here, so a column added to a header
    /// without a value cannot compile into a row nobody notices.
    #[test]
    fn a_row_whose_values_do_not_match_its_header_is_an_error() {
        let short = Row::new(Table::Finalized, vec![ColumnValue::Uint(1)]);
        assert!(
            short.is_err(),
            "one value against a four-column table must not build"
        );
    }

    /// Every table ends with the two common columns, so a store can key and partition any
    /// row without knowing which dataset it came from. Checked rather than assumed,
    /// because a table that forgot them would still write and be unkeyable.
    #[test]
    fn every_table_carries_the_common_columns() {
        for table in Table::ALL {
            let columns = table.columns();
            let tail = &columns[columns.len() - COMMON_COLUMNS.len()..];
            assert_eq!(tail, COMMON_COLUMNS, "the {table} table ends with them");
        }
    }

    /// Every value's variant matches its column's declared type. The two are one contract
    /// — a store types the column from the header and writes the value — so a value that
    /// arrives as a different variant is a row that lies about itself, and the failure is
    /// invisible until another store consumes the variant directly.
    ///
    /// The populated fixture is what makes this real: an all-`Null` row would satisfy a
    /// weaker check, because `Null` belongs to every column.
    #[test]
    fn every_value_matches_its_declared_column_type() {
        let chain = ChainId::new("ethereum");
        for event in every_kind() {
            let row = row_for(&chain, &event).expect("every event renders");
            let columns = row.columns();
            for (column, value) in columns.iter().zip(row.values()) {
                let compatible = match column.kind {
                    // A `Uint` column holds a number or nothing; every other variant is
                    // a contradiction, including a document.
                    ColumnType::Uint => matches!(value, ColumnValue::Uint(_) | ColumnValue::Null),
                    ColumnType::Text => matches!(value, ColumnValue::Text(_) | ColumnValue::Null),
                    ColumnType::Bool => matches!(value, ColumnValue::Bool(_) | ColumnValue::Null),
                    ColumnType::Document => {
                        matches!(value, ColumnValue::Document(_) | ColumnValue::Null)
                    }
                };
                assert!(
                    compatible,
                    "the {} table's {column:?} column is {:?} but the value is {value:?}",
                    row.table(),
                    column.kind
                );
            }
        }
    }

    /// Gas *amounts* and gas *prices* are different widths, and the columns say so — but
    /// only in that the price is not a [`ColumnType::Uint`]. `alloy` draws the same line
    /// (`gas`, `gas_used`, `gas_limit` are `u64`; `gas_price`, `max_fee_per_gas` are
    /// `u128`), and so does `go-ethereum` (`uint64` amounts, `*big.Int` prices), so a
    /// price column must not be typed as a 64-bit number.
    ///
    /// Both are stored as `0x` hex text, because that is what the node sends, so this
    /// checks the *distinction that matters* rather than inventing a type per width: a
    /// price is never `Uint`, and an amount always is.
    #[test]
    fn a_price_is_not_a_64_bit_number_but_an_amount_is() {
        for (table, price) in [
            (Table::Transaction, "gas_price"),
            (Table::Transaction, "max_fee_per_gas"),
            (Table::Transaction, "max_priority_fee_per_gas"),
            (Table::Transaction, "max_fee_per_blob_gas"),
            (Table::Receipt, "effective_gas_price"),
            (Table::Receipt, "blob_gas_price"),
        ] {
            let column = table
                .columns()
                .iter()
                .find(|column| column.name == price)
                .unwrap_or_else(|| panic!("the {table} table has a {price} column"));
            assert_ne!(
                column.kind,
                ColumnType::Uint,
                "{price} is 128 bits and must not be a 64-bit column"
            );
        }

        // The amounts, which really are 64-bit.
        for (table, amount) in [
            (Table::Block, "gas_limit"),
            (Table::Block, "gas_used"),
            (Table::Block, "transaction_count"),
            (Table::Transaction, "gas"),
            (Table::Transaction, "nonce"),
            (Table::Transaction, "transaction_index"),
            (Table::Receipt, "gas_used"),
            (Table::Receipt, "cumulative_gas_used"),
            (Table::Receipt, "transaction_index"),
            (Table::Log, "log_index"),
            (Table::Log, "transaction_index"),
        ] {
            let column = table
                .columns()
                .iter()
                .find(|column| column.name == amount)
                .unwrap_or_else(|| panic!("the {table} table has an {amount} column"));
            assert_eq!(
                column.kind,
                ColumnType::Uint,
                "{amount} is a 64-bit gas amount or index"
            );
        }
    }

    /// A price above `u64::MAX` survives the round trip as text. `u64::MAX` wei is 18.4
    /// ETH, and EIP-1559's 12.5% per-block growth reaches that from 1 gwei in 201 blocks —
    /// so a store that read the column as a 64-bit number would truncate inside a price
    /// spike, not in a hypothetical.
    #[test]
    fn a_price_above_u64_max_round_trips_through_the_row() {
        let price = u128::from(u64::MAX) + 1;
        let transaction = Transaction {
            gas_price: Some(price),
            ..Transaction::default()
        };
        let event = Event::Transaction(Box::new(transaction));
        let row = row_for(&chain(), &event).expect("a transaction row");
        let columns = row.columns().to_vec();
        let index = columns
            .iter()
            .position(|column| column.name == "gas_price")
            .expect("the column exists");

        assert_eq!(
            row.values()[index],
            ColumnValue::Text(format!("{price:#x}")),
            "a price wider than 64 bits must not be truncated"
        );
    }

    /// A field the chain does not have is null, never zero. `withdrawals_root` is `None`
    /// before EIP-4895, and a consumer filtering on it must get exactly those blocks.
    #[test]
    fn an_absent_field_is_null_and_not_zero() {
        let block = Block {
            number: 100,
            hash: hash(0x01),
            ..Block::default()
        };
        let row = row_for(&chain(), &Event::Block(Box::new(block))).expect("a block row");
        let columns = row.columns().to_vec();
        let index = columns
            .iter()
            .position(|c| c.name == "withdrawals_root")
            .expect("the column exists");
        assert_eq!(row.values()[index], ColumnValue::Null);
        assert_ne!(row.values()[index], ColumnValue::Uint(0));
    }

    /// A hash renders as the `0x` hex a node sends, so a value in a typed column compares
    /// equal to the same value read out of a raw RPC response.
    #[test]
    fn a_hash_renders_as_the_nodes_own_hex() {
        assert_eq!(
            ColumnValue::hex(hash(0xaa)),
            ColumnValue::Text(format!("0x{}", "aa".repeat(32)))
        );
    }

    /// The two branches of a reorg are separate rows, keyed differently — which is the
    /// property that makes a retraction addressable rather than an overwrite.
    #[test]
    fn a_log_and_its_replacement_do_not_share_a_row() {
        let orphaned = Log {
            log_index: 0,
            transaction_hash: TxHash::from([0x01; 32]),
            block_number: 100,
            block_hash: hash(0xaa),
            ..Log::default()
        };
        let mut replacement = orphaned.clone();
        replacement.block_hash = hash(0xbb);

        let a = row_for(&chain(), &Event::Log(Box::new(orphaned))).expect("a row");
        let b = row_for(&chain(), &Event::Log(Box::new(replacement))).expect("a row");
        assert_ne!(a.dedupe_key(), b.dedupe_key());
    }
}
