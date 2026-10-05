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
//! # A table is declared once
//!
//! A table is a header of [`Column`]s and, per row, one [`ColumnValue`] per column. The
//! two are declared *together*, as one `(Column, fn(&Dataset) -> ColumnValue)` entry per
//! field in a per-table cell table, so a column and its value cannot be reordered,
//! renamed, or dropped independently: there is no second positional list for them to
//! drift against, and [`Row`]'s width is the number of cells.
//!
//! The column *type* is part of each cell, not a parallel list, because a name and a type
//! are one fact about a column and splitting them is how a schema and its data come to
//! disagree. Each store maps [`ColumnType`] to its own vocabulary; a `ClickHouse` sink and
//! a `DuckDB` one would both read the same headers and produce different DDL, which is
//! the point.
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
    /// Decoded event records, whose arguments ride as documents.
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
    /// The header and the rows are two views of one declaration: the columns come from the
    /// same cell table [`row_for`] builds each row from, so a store's DDL cannot disagree
    /// with the rows it is asked to hold. Every header ends with [`COMMON_COLUMNS`].
    #[must_use]
    pub fn columns(self) -> Vec<Column> {
        let mut columns = match self {
            Self::Block => cells_columns(&BLOCK_CELLS),
            Self::Transaction => cells_columns(&TRANSACTION_CELLS),
            Self::Receipt => cells_columns(&RECEIPT_CELLS),
            Self::Log => cells_columns(&LOG_CELLS),
            Self::Decoded => cells_columns(&DECODED_CELLS),
            Self::Reorg => cells_columns(&REORG_CELLS),
            Self::Finalized => cells_columns(&FINALIZED_CELLS),
        };
        columns.extend(COMMON_COLUMNS);
        columns
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The columns of a cell table, in order.
///
/// Shared by every per-table arm of [`Table::columns`], so the header is derived from the
/// same cells the row is.
fn cells_columns<C>(cells: &[(Column, C)]) -> Vec<Column> {
    cells.iter().map(|(column, _)| *column).collect()
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
/// upserts on. Both the header ([`Table::columns`]) and the row ([`row_for`]) take them
/// from here, so the two cannot name them differently.
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

/// Renders any serializable value as JSON.
///
/// The values rendered here are plain data — hashes, lists, ABI values — so a failure is
/// not reachable. It panics rather than falling back to a `"null"` document: a document is
/// the row asserting the value is present, so a fallback would persist a value that says it
/// is empty, which is a lie a store keeps. Failing here is loud and loses nothing.
fn json<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|error| panic!("a row value is plain data and must serialize: {error}"))
}

/// One table's columns, each paired with the value that fills it for this row.
///
/// A single list is the whole point: a table is declared once, as this, rather than as a
/// header and a positional value list that [`Row`] has to check against it. The length is
/// the row's width, so the two cannot disagree.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Columns(Vec<(Column, ColumnValue)>);

impl Row {
    /// Which table this row belongs to.
    #[must_use]
    pub const fn table(&self) -> Table {
        self.table
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
            .columns
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

/// Renders an event as the row a store persists.
///
/// Every event has exactly one row, a control signal included — a reorg carries only what
/// the signal says, but it is still a row, so a store has no variant to handle and no
/// event that is silently dropped.
///
/// Total: the row is built from the table's own cell list, so it is always as wide as the
/// table and there is no width to check. A store appends it and cannot be handed a row
/// that does not fit.
#[must_use]
pub fn row_for(chain: &ChainId, event: &Event) -> Row {
    let (table, cells) = match event {
        Event::Block(b) => (Table::Block, block_cells(b)),
        Event::Transaction(t) => (Table::Transaction, transaction_cells(t)),
        Event::Receipt(r) => (Table::Receipt, receipt_cells(r)),
        Event::Log(l) => (Table::Log, log_cells(l)),
        Event::Decoded(d) => (Table::Decoded, decoded_cells(d)),
        Event::Reorg(r) => (Table::Reorg, reorg_cells(r)),
        Event::Finalized(f) => (Table::Finalized, finalized_cells(f)),
    };
    Row::new(table, cells.with_common(chain, &event.dedupe_key()))
}

/// One row's worth of a dataset: the table, its columns, and the values that line up with
/// them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    table: Table,
    columns: Vec<Column>,
    values: Vec<ColumnValue>,
}

impl Row {
    /// Builds a row from its table's cells, splitting them into the header and the values.
    fn new(table: Table, cells: Columns) -> Self {
        let (columns, values) = cells.0.into_iter().unzip();
        Self {
            table,
            columns,
            values,
        }
    }

    /// This table's columns, in the order [`values`](Self::values) are given.
    #[must_use]
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// The values, in [`columns`](Self::columns) order.
    #[must_use]
    pub fn values(&self) -> &[ColumnValue] {
        &self.values
    }
}

impl Columns {
    /// Appends the two columns every table ends with, for this row.
    ///
    /// Takes the names from [`COMMON_COLUMNS`] so the header ([`Table::columns`]) and the
    /// row cannot disagree on them.
    fn with_common(mut self, chain: &ChainId, key: &str) -> Self {
        let values = [
            ColumnValue::Text(chain.as_str().to_owned()),
            ColumnValue::Text(key.to_owned()),
        ];
        self.0.extend(COMMON_COLUMNS.into_iter().zip(values));
        self
    }
}

/// The `block` table, as one cell per column.
const BLOCK_CELLS: [(Column, fn(&Block) -> ColumnValue); 25] = [
    (Column::uint("number"), |b| ColumnValue::Uint(b.number)),
    (Column::text("hash"), |b| ColumnValue::hex(b.hash)),
    (Column::text("parent_hash"), |b| {
        ColumnValue::hex(b.parent_hash)
    }),
    (Column::uint("timestamp"), |b| {
        ColumnValue::Uint(b.timestamp)
    }),
    (Column::text("nonce"), |b| ColumnValue::hex(b.nonce)),
    (Column::text("ommers_hash"), |b| {
        ColumnValue::hex(b.ommers_hash)
    }),
    (Column::text("transactions_root"), |b| {
        ColumnValue::hex(b.transactions_root)
    }),
    (Column::text("state_root"), |b| {
        ColumnValue::hex(b.state_root)
    }),
    (Column::text("receipts_root"), |b| {
        ColumnValue::hex(b.receipts_root)
    }),
    (Column::text("withdrawals_root"), |b| {
        ColumnValue::some(b.withdrawals_root, ColumnValue::hex)
    }),
    (Column::text("logs_bloom"), |b| {
        ColumnValue::hex(b.logs_bloom)
    }),
    (Column::text("miner"), |b| ColumnValue::hex(b.miner)),
    (Column::text("difficulty"), |b| {
        ColumnValue::u256(b.difficulty)
    }),
    (Column::text("total_difficulty"), |b| {
        ColumnValue::some(b.total_difficulty, ColumnValue::u256)
    }),
    (Column::text("size"), |b| {
        ColumnValue::some(b.size, ColumnValue::u256)
    }),
    (Column::text("extra_data"), |b| {
        ColumnValue::bytes(&b.extra_data)
    }),
    (Column::uint("gas_limit"), |b| {
        ColumnValue::Uint(b.gas_limit)
    }),
    (Column::uint("gas_used"), |b| ColumnValue::Uint(b.gas_used)),
    (Column::uint("transaction_count"), |b| {
        ColumnValue::Uint(b.transaction_count)
    }),
    (Column::uint("base_fee_per_gas"), |b| {
        ColumnValue::some(b.base_fee_per_gas, ColumnValue::Uint)
    }),
    (Column::uint("blob_gas_used"), |b| {
        ColumnValue::some(b.blob_gas_used, ColumnValue::Uint)
    }),
    (Column::uint("excess_blob_gas"), |b| {
        ColumnValue::some(b.excess_blob_gas, ColumnValue::Uint)
    }),
    (Column::text("parent_beacon_block_root"), |b| {
        ColumnValue::some(b.parent_beacon_block_root, ColumnValue::hex)
    }),
    (Column::document("ommers"), |b| {
        ColumnValue::hex_list(&b.ommers)
    }),
    (Column::document("transaction_hashes"), |b| {
        ColumnValue::hex_list(&b.transaction_hashes)
    }),
];

/// Builds the `block` table's cells for one block.
fn block_cells(b: &Block) -> Columns {
    Columns(
        BLOCK_CELLS
            .iter()
            .map(|(column, render)| (*column, render(b)))
            .collect(),
    )
}

/// The `transaction` table, as one cell per column.
const TRANSACTION_CELLS: [(Column, fn(&Transaction) -> ColumnValue); 20] = [
    (Column::text("hash"), |t| ColumnValue::hex(t.hash)),
    (Column::uint("nonce"), |t| ColumnValue::Uint(t.nonce)),
    (Column::uint("transaction_index"), |t| {
        ColumnValue::Uint(t.transaction_index)
    }),
    (Column::text("from_address"), |t| ColumnValue::hex(t.from)),
    (Column::text("to_address"), |t| {
        ColumnValue::some(t.to, ColumnValue::hex)
    }),
    (Column::text("value"), |t| ColumnValue::u256(t.value)),
    (Column::uint("gas"), |t| ColumnValue::Uint(t.gas)),
    (Column::text("gas_price"), |t| {
        ColumnValue::some(t.gas_price, ColumnValue::wei)
    }),
    (Column::text("max_fee_per_gas"), |t| {
        ColumnValue::some(t.max_fee_per_gas, ColumnValue::wei)
    }),
    (Column::text("max_priority_fee_per_gas"), |t| {
        ColumnValue::some(t.max_priority_fee_per_gas, ColumnValue::wei)
    }),
    (Column::text("max_fee_per_blob_gas"), |t| {
        ColumnValue::some(t.max_fee_per_blob_gas, ColumnValue::wei)
    }),
    (Column::text("input"), |t| ColumnValue::bytes(&t.input)),
    (Column::uint("transaction_type"), |t| {
        ColumnValue::Uint(u64::from(t.transaction_type))
    }),
    (Column::uint("chain_id"), |t| {
        ColumnValue::some(t.chain_id, ColumnValue::Uint)
    }),
    (Column::document("access_list"), |t| {
        ColumnValue::optional_document(&t.access_list)
    }),
    (Column::document("blob_versioned_hashes"), |t| {
        ColumnValue::optional_document(&t.blob_versioned_hashes)
    }),
    (Column::document("authorization_list"), |t| {
        ColumnValue::optional_document(&t.authorization_list)
    }),
    (Column::uint("block_timestamp"), |t| {
        ColumnValue::Uint(t.block_timestamp)
    }),
    (Column::uint("block_number"), |t| {
        ColumnValue::Uint(t.block_number)
    }),
    (Column::text("block_hash"), |t| {
        ColumnValue::hex(t.block_hash)
    }),
];

/// Builds the `transaction` table's cells for one transaction.
fn transaction_cells(t: &Transaction) -> Columns {
    Columns(
        TRANSACTION_CELLS
            .iter()
            .map(|(column, render)| (*column, render(t)))
            .collect(),
    )
}

/// The `receipt` table, as one cell per column.
const RECEIPT_CELLS: [(Column, fn(&Receipt) -> ColumnValue); 17] = [
    (Column::text("transaction_hash"), |r| {
        ColumnValue::hex(r.transaction_hash)
    }),
    (Column::uint("transaction_index"), |r| {
        ColumnValue::Uint(r.transaction_index)
    }),
    (Column::text("from_address"), |r| ColumnValue::hex(r.from)),
    (Column::text("to_address"), |r| {
        ColumnValue::some(r.to, ColumnValue::hex)
    }),
    (Column::boolean("status"), |r| ColumnValue::Bool(r.status)),
    (Column::uint("transaction_type"), |r| {
        ColumnValue::Uint(u64::from(r.transaction_type))
    }),
    (Column::uint("gas_used"), |r| ColumnValue::Uint(r.gas_used)),
    (Column::uint("cumulative_gas_used"), |r| {
        ColumnValue::Uint(r.cumulative_gas_used)
    }),
    (Column::text("effective_gas_price"), |r| {
        ColumnValue::wei(r.effective_gas_price)
    }),
    (Column::text("contract_address"), |r| {
        ColumnValue::some(r.contract_address, ColumnValue::hex)
    }),
    (Column::text("logs_bloom"), |r| {
        ColumnValue::hex(r.logs_bloom)
    }),
    (Column::uint("blob_gas_used"), |r| {
        ColumnValue::some(r.blob_gas_used, ColumnValue::Uint)
    }),
    (Column::text("blob_gas_price"), |r| {
        ColumnValue::some(r.blob_gas_price, ColumnValue::wei)
    }),
    (Column::uint("log_count"), |r| {
        ColumnValue::Uint(r.log_count)
    }),
    (Column::uint("block_timestamp"), |r| {
        ColumnValue::Uint(r.block_timestamp)
    }),
    (Column::uint("block_number"), |r| {
        ColumnValue::Uint(r.block_number)
    }),
    (Column::text("block_hash"), |r| {
        ColumnValue::hex(r.block_hash)
    }),
];

/// Builds the `receipt` table's cells for one receipt.
fn receipt_cells(r: &Receipt) -> Columns {
    Columns(
        RECEIPT_CELLS
            .iter()
            .map(|(column, render)| (*column, render(r)))
            .collect(),
    )
}

/// The `log` table, as one cell per column.
const LOG_CELLS: [(Column, fn(&Log) -> ColumnValue); 13] = [
    (Column::uint("log_index"), |l| {
        ColumnValue::Uint(l.log_index)
    }),
    (Column::text("transaction_hash"), |l| {
        ColumnValue::hex(l.transaction_hash)
    }),
    (Column::uint("transaction_index"), |l| {
        ColumnValue::Uint(l.transaction_index)
    }),
    (Column::text("address"), |l| ColumnValue::hex(l.address)),
    (Column::text("topic0"), |l| {
        ColumnValue::some(l.topic0, ColumnValue::hex)
    }),
    (Column::text("topic1"), |l| {
        ColumnValue::some(l.topic1, ColumnValue::hex)
    }),
    (Column::text("topic2"), |l| {
        ColumnValue::some(l.topic2, ColumnValue::hex)
    }),
    (Column::text("topic3"), |l| {
        ColumnValue::some(l.topic3, ColumnValue::hex)
    }),
    (Column::text("data"), |l| ColumnValue::bytes(&l.data)),
    (Column::boolean("removed"), |l| ColumnValue::Bool(l.removed)),
    (Column::uint("block_number"), |l| {
        ColumnValue::Uint(l.block_number)
    }),
    (Column::text("block_hash"), |l| {
        ColumnValue::hex(l.block_hash)
    }),
    (Column::uint("block_timestamp"), |l| {
        ColumnValue::Uint(l.block_timestamp)
    }),
];

/// Builds the `log` table's cells for one log.
fn log_cells(l: &Log) -> Columns {
    Columns(
        LOG_CELLS
            .iter()
            .map(|(column, render)| (*column, render(l)))
            .collect(),
    )
}

/// The `decoded` table, as one cell per column.
///
/// The two documents hold what varies per event — the indexed and non-indexed arguments —
/// while everything that identifies the row is a typed column: a store can key, join,
/// filter, and partition on `protocol`, `address`, `selector`, `block_hash`,
/// `block_timestamp`, and `log_index` without parsing the record. The argument *values*
/// still vary in type per event, so they stay documents; the identity does not, and keeping
/// it locked in a document would force every query back through JSON.
const DECODED_CELLS: [(Column, fn(&Decoded) -> ColumnValue); 14] = [
    (Column::text("name"), |d| ColumnValue::Text(d.name.clone())),
    (Column::text("address"), |d| ColumnValue::hex(d.address)),
    (Column::text("protocol"), |d| {
        ColumnValue::Text(d.protocol.clone())
    }),
    (Column::text("selector"), |d| ColumnValue::hex(d.selector)),
    (Column::text("signature"), |d| {
        ColumnValue::Text(d.signature.clone())
    }),
    (Column::boolean("anonymous"), |d| {
        ColumnValue::Bool(d.anonymous)
    }),
    (Column::text("transaction_hash"), |d| {
        ColumnValue::hex(d.transaction_hash)
    }),
    (Column::uint("transaction_index"), |d| {
        ColumnValue::Uint(d.transaction_index)
    }),
    (Column::uint("log_index"), |d| {
        ColumnValue::Uint(d.log_index)
    }),
    (Column::document("indexed"), |d| {
        ColumnValue::document(&d.indexed)
    }),
    (Column::document("body"), |d| ColumnValue::document(&d.body)),
    (Column::uint("block_number"), |d| {
        ColumnValue::Uint(d.block_number)
    }),
    (Column::text("block_hash"), |d| {
        ColumnValue::hex(d.block_hash)
    }),
    (Column::uint("block_timestamp"), |d| {
        ColumnValue::Uint(d.block_timestamp)
    }),
];

/// Builds the `decoded` table's cells for one decoded record.
fn decoded_cells(d: &Decoded) -> Columns {
    Columns(
        DECODED_CELLS
            .iter()
            .map(|(column, render)| (*column, render(d)))
            .collect(),
    )
}

/// The `reorg` table, as one cell per column.
///
/// `orphaned_hashes` is a document rather than a set of rows, and it is the one event
/// where the list *is* the payload: it names every block hash that stopped being
/// canonical. The count is small and known only at read time, so a list column beats a
/// child table a consumer has to join to answer "is this block canonical yet?".
const REORG_CELLS: [(Column, fn(&Reorg) -> ColumnValue); 3] = [
    (Column::uint("height"), |r| ColumnValue::Uint(r.height)),
    (Column::text("new_head_hash"), |r| {
        ColumnValue::hex(r.new_head_hash)
    }),
    (Column::document("orphaned_hashes"), |r| {
        ColumnValue::hex_list(&r.orphaned_hashes)
    }),
];

/// Builds the `reorg` table's cells for one reorg.
fn reorg_cells(r: &Reorg) -> Columns {
    Columns(
        REORG_CELLS
            .iter()
            .map(|(column, render)| (*column, render(r)))
            .collect(),
    )
}

/// The `finalized` table, as one cell per column.
const FINALIZED_CELLS: [(Column, fn(&Finalized) -> ColumnValue); 2] = [
    (Column::uint("height"), |f| ColumnValue::Uint(f.height)),
    (Column::text("hash"), |f| ColumnValue::hex(f.hash)),
];

/// Builds the `finalized` table's cells for one watermark.
fn finalized_cells(f: &Finalized) -> Columns {
    Columns(
        FINALIZED_CELLS
            .iter()
            .map(|(column, render)| (*column, render(f)))
            .collect(),
    )
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

    use super::{COMMON_COLUMNS, ColumnType, ColumnValue, Table, row_for};

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
            let row = row_for(&chain(), event);
            assert_eq!(row.values().len(), row.columns().len());
            assert_eq!(row.chain(), "ethereum");
            assert_eq!(row.dedupe_key(), event.dedupe_key());
        }
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
            let row = row_for(&chain, &event);
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
            let columns = table.columns();
            let column = columns
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
            let columns = table.columns();
            let column = columns
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
        let row = row_for(&chain(), &event);
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
        let row = row_for(&chain(), &Event::Block(Box::new(block)));
        let columns = row.columns().to_vec();
        let index = columns
            .iter()
            .position(|c| c.name == "withdrawals_root")
            .expect("the column exists");
        assert_eq!(row.values()[index], ColumnValue::Null);
        assert_ne!(row.values()[index], ColumnValue::Uint(0));
    }

    /// An absent maximum fee is null, not zero: a legacy transaction carries no cap, and a
    /// consumer must be able to tell that from a transaction that capped its fee at zero.
    #[test]
    fn an_absent_max_fee_is_null_and_not_zero() {
        let transaction = Transaction {
            max_fee_per_gas: None,
            ..Transaction::default()
        };
        let event = Event::Transaction(Box::new(transaction));
        let row = row_for(&chain(), &event);
        assert_eq!(row.value("max_fee_per_gas"), &ColumnValue::Null);
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

    /// The decoded row's identity is typed columns, not a document a consumer has to parse
    /// to key or join on.
    #[test]
    fn a_decoded_row_keys_and_joins_on_typed_columns() {
        let decoded = Decoded {
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
        };
        let event = Event::Decoded(Box::new(decoded.clone()));
        let row = row_for(&chain(), &event);

        assert_eq!(row.text("protocol"), "uniswap_v3");
        assert_eq!(row.text("address"), format!("{:#x}", decoded.address));
        assert_eq!(row.text("block_hash"), format!("{:#x}", decoded.block_hash));
        assert_eq!(row.value("log_index"), &ColumnValue::Uint(7));
        assert_eq!(
            row.value("block_timestamp"),
            &ColumnValue::Uint(1_700_000_000)
        );
        // The variable arguments stay documents.
        assert!(matches!(row.value("indexed"), ColumnValue::Document(_)));
        assert!(matches!(row.value("body"), ColumnValue::Document(_)));
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

        let a = row_for(&chain(), &Event::Log(Box::new(orphaned)));
        let b = row_for(&chain(), &Event::Log(Box::new(replacement)));
        assert_ne!(a.dedupe_key(), b.dedupe_key());
    }
}
