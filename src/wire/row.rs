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

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use alloy_primitives::{B256, Bytes, U256};

use crate::wire::datasets::evm::{Block, Log, Receipt, Transaction};
use crate::wire::envelope::{AcceptedBlock, ChainId, Contract, Decoded, DecodedArg, Event, Reorg};
use crate::wire::typed::TypedValue;

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
    /// Contracts discovered from a factory's creation event.
    Contract,
    /// Reorg markers: which block hashes stopped being canonical.
    Reorg,
    /// Accepted block identities: the ledger a restart resumes from.
    AcceptedBlock,
}

impl Table {
    /// Every table, in the order a store would create them.
    ///
    /// A `const` list rather than a derive: eight cases do not justify a dependency, and
    /// a hand-written list is one a reader can check.
    pub const ALL: [Self; 8] = [
        Self::Block,
        Self::Transaction,
        Self::Receipt,
        Self::Log,
        Self::Decoded,
        Self::Contract,
        Self::Reorg,
        Self::AcceptedBlock,
    ];

    /// The name a store files this table under.
    ///
    /// Plural, as Allium names its tables (`blocks`, `logs`, `decoded.logs`), while the
    /// Rust type and the event's `type` tag name one record. The decoded records' table is
    /// `decoded_logs`, since a run's tables share one schema.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Block => "blocks",
            Self::Transaction => "transactions",
            Self::Receipt => "receipts",
            Self::Log => "logs",
            Self::Decoded => "decoded_logs",
            Self::Contract => "contracts",
            Self::Reorg => "reorgs",
            Self::AcceptedBlock => "accepted_blocks",
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
            Self::Contract => cells_columns(&CONTRACT_CELLS),
            Self::Reorg => cells_columns(&REORG_CELLS),
            Self::AcceptedBlock => cells_columns(&ACCEPTED_BLOCK_CELLS),
        };
        columns.extend(COMMON_COLUMNS);
        columns
    }

    /// The column naming the block each row belongs to, or `None` for a table whose rows
    /// belong to no single block.
    ///
    /// This is what a store deletes by when a reorg orphans a block: every row of every
    /// table that answers `Some` is retracted with its block. Exhaustive, so a new table
    /// cannot be added without deciding whether a reorg removes its rows. `reorgs` answers
    /// `None` because its rows are the record of what was retracted, not part of a block.
    #[must_use]
    pub const fn block_hash_column(self) -> Option<&'static str> {
        match self {
            Self::Block | Self::AcceptedBlock => Some("hash"),
            Self::Transaction | Self::Receipt | Self::Log | Self::Decoded | Self::Contract => {
                Some("block_hash")
            }
            Self::Reorg => None,
        }
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
    cells.iter().map(|(column, _)| column.clone()).collect()
}

/// One column of a table: its name and what it stores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// The name, which is the field's own — a chain field keeps its chain name
    /// (`from_address` rather than `from`, since `from` is a SQL keyword), and a store may
    /// rename it.
    ///
    /// Borrowed for a dataset table, whose columns are declared in this file; owned for
    /// a decoded event's table, whose columns come from an ABI at startup.
    pub name: Cow<'static, str>,
    /// What the column holds.
    pub kind: ColumnType,
    /// Whether every row has a value.
    ///
    /// A store renders this as `NOT NULL`. It is false only for a cell whose renderer can
    /// return [`ColumnValue::Null`], which is a fact about that field, not about the engine.
    pub required: bool,
}

impl Column {
    /// A text column, the shape of every hash and address.
    pub(crate) const fn text(name: &'static str) -> Self {
        Self::new(name, ColumnType::Text)
    }

    /// An unsigned 64-bit column, for a number, an index, or a gas figure.
    pub(crate) const fn uint(name: &'static str) -> Self {
        Self::new(name, ColumnType::Uint)
    }

    /// A boolean column.
    pub(crate) const fn boolean(name: &'static str) -> Self {
        Self::new(name, ColumnType::Bool)
    }

    /// A document column, for a value that does not flatten.
    pub(crate) const fn document(name: &'static str) -> Self {
        Self::new(name, ColumnType::Document)
    }

    const fn new(name: &'static str, kind: ColumnType) -> Self {
        Self {
            name: Cow::Borrowed(name),
            kind,
            required: true,
        }
    }

    /// A column whose name is only known at runtime: a decoded event's argument.
    #[must_use]
    pub fn named(name: String, kind: ColumnType, required: bool) -> Self {
        Self {
            name: Cow::Owned(name),
            kind,
            required,
        }
    }

    /// Marks a column whose renderer can return [`ColumnValue::Null`].
    #[must_use]
    pub(crate) const fn optional(mut self) -> Self {
        self.required = false;
        self
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
    /// A 64-bit signed integer: a decoded `int8` through `int64`, such as a Uniswap tick.
    Int,
    /// An exact integer wider than 64 bits, up to 256 and signed or not: a decoded
    /// `uint256` amount or `int256` delta.
    ///
    /// Numeric rather than hex text, unlike the chain's own wide values, because these are
    /// what a consumer sums: a store keeps them in an exact arbitrary-precision type
    /// (`NUMERIC(78,0)` in `PostgreSQL`, `BIGNUM` in `DuckDB`) so aggregates need no cast.
    BigInt,
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
    /// A 64-bit signed integer.
    Int(i64),
    /// An integer of any width up to 256 bits, signed or not, held exactly.
    BigInt {
        /// Whether the value is below zero.
        negative: bool,
        /// The absolute value.
        magnitude: U256,
    },
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

/// Which table a row belongs to: a dataset table, or a decoded event's table by its
/// position among the [`Schema`]'s event tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TableId {
    /// A dataset table.
    Dataset(Table),
    /// A decoded event's table.
    Event(usize),
}

impl From<Table> for TableId {
    fn from(table: Table) -> Self {
        Self::Dataset(table)
    }
}

/// One table a store creates and writes.
#[derive(Debug, Clone)]
pub struct TableDef {
    /// Which table this is; rows name it by the same id.
    pub id: TableId,
    /// The table's name.
    pub name: String,
    /// The columns, in order, ending with [`COMMON_COLUMNS`].
    pub columns: Arc<[Column]>,
    /// The column naming each row's block, which a reorg deletes by; `None` for a table
    /// whose rows belong to no block.
    pub block_hash_column: Option<&'static str>,
}

/// Every table a run writes: the dataset tables, then one typed table per decoded event,
/// generated from the ABIs at startup.
///
/// An event's table has the log's columns, then one column per argument, in ABI order,
/// then the block's columns:
///
/// ```text
/// uniswap_v3_pool_swap
///   address, transaction_hash, transaction_index, log_index,
///   sender, recipient, amount0, amount1, sqrt_price_x96, liquidity, tick,
///   block_number, block_hash, block_timestamp, chain, dedupe_key
/// ```
///
/// An argument's column is its name in `snake_case`; an unnamed argument is `arg{n}`, and
/// a name that repeats an earlier column gets `_{n}` appended. A row's key is the decoded
/// record's own `dedupe_key`.
#[derive(Debug, Clone)]
pub struct Schema {
    tables: Vec<TableDef>,
    /// Each event table's position in `tables`, by the record's protocol, contract, and
    /// event definition.
    events: HashMap<(String, String, B256), usize>,
}

impl Default for Schema {
    /// The dataset tables alone.
    fn default() -> Self {
        let tables = Table::ALL
            .map(|table| TableDef {
                id: table.into(),
                name: table.name().to_owned(),
                columns: table.columns().into(),
                block_hash_column: table.block_hash_column(),
            })
            .to_vec();
        Self {
            tables,
            events: HashMap::new(),
        }
    }
}

impl Schema {
    /// Every table, dataset tables first.
    #[must_use]
    pub fn tables(&self) -> &[TableDef] {
        &self.tables
    }

    /// Adds the table for one event, whose arguments are `params` in ABI order with
    /// their ABI names. Returns `false`, adding nothing, when a table is already named
    /// `name`.
    pub fn add_event(
        &mut self,
        (protocol, contract, event_id): (&str, &str, B256),
        name: String,
        params: Vec<Column>,
    ) -> bool {
        if self.tables.iter().any(|table| table.name == name) {
            return false;
        }
        let mut columns = vec![
            Column::text("address"),
            Column::text("transaction_hash"),
            Column::uint("transaction_index"),
            Column::uint("log_index"),
        ];
        let trailing = [
            Column::uint("block_number"),
            Column::text("block_hash"),
            Column::uint("block_timestamp"),
        ];
        for (position, param) in params.into_iter().enumerate() {
            let mut column = snake_case(&param.name);
            if column.is_empty() {
                column = format!("arg{position}");
            }
            if columns
                .iter()
                .chain(&trailing)
                .chain(&COMMON_COLUMNS)
                .any(|existing| existing.name == column.as_str())
            {
                column = format!("{column}_{position}");
            }
            columns.push(Column::named(column, param.kind, param.required));
        }
        columns.extend(trailing);
        columns.extend(COMMON_COLUMNS);
        let index = self.events.len();
        self.events.insert(
            (protocol.to_owned(), contract.to_owned(), event_id),
            self.tables.len(),
        );
        self.tables.push(TableDef {
            id: TableId::Event(index),
            name,
            columns: columns.into(),
            block_hash_column: Some("block_hash"),
        });
        true
    }

    /// A decoded record's row in its event's table, or `None` when no table holds its
    /// event — which a record decoded against the same catalog never is.
    #[must_use]
    pub fn event_row(&self, chain: &ChainId, decoded: &Decoded) -> Option<Row> {
        let key = (
            decoded.protocol.clone(),
            decoded.contract.clone(),
            decoded.event_id,
        );
        let table = &self.tables[*self.events.get(&key)?];
        let mut arguments: Vec<&DecodedArg> = decoded.indexed.iter().chain(&decoded.body).collect();
        arguments.sort_by_key(|argument| argument.position);
        let mut values = vec![
            ColumnValue::hex(decoded.address),
            ColumnValue::hex(decoded.transaction_hash),
            ColumnValue::Uint(decoded.transaction_index),
            ColumnValue::Uint(decoded.log_index),
        ];
        values.extend(
            arguments
                .iter()
                .zip(&table.columns[4..])
                .map(|(argument, column)| cell(column.kind, argument)),
        );
        values.extend([
            ColumnValue::Uint(decoded.block_number),
            ColumnValue::hex(decoded.block_hash),
            ColumnValue::Uint(decoded.block_timestamp),
            ColumnValue::Text(chain.as_str().to_owned()),
            ColumnValue::Text(decoded.dedupe_key()),
        ]);
        Some(Row {
            table: table.id,
            columns: Arc::clone(&table.columns),
            values,
        })
    }
}

/// `sqrtPriceX96` as `sqrt_price_x96`: words split at a lower-to-upper change and before
/// the last capital of a run, lowercased, with leading underscores dropped and anything
/// but letters and digits replaced by `_`.
#[must_use]
pub fn snake_case(name: &str) -> String {
    let chars: Vec<char> = name.trim_start_matches('_').chars().collect();
    let mut out = String::with_capacity(chars.len() + 4);
    for (index, &c) in chars.iter().enumerate() {
        if c.is_ascii_uppercase() && index > 0 {
            let previous = chars[index - 1];
            let next_is_lower = chars.get(index + 1).is_some_and(char::is_ascii_lowercase);
            if previous.is_ascii_lowercase()
                || previous.is_ascii_digit()
                || (previous.is_ascii_uppercase() && next_is_lower)
            {
                out.push('_');
            }
        }
        out.push(if c.is_ascii_alphanumeric() {
            c.to_ascii_lowercase()
        } else {
            '_'
        });
    }
    out
}

/// One decoded argument's cell, in the column type its table declares.
///
/// The decoder has already checked each value against its declared width, so a `uint64`
/// fits [`ColumnValue::Uint`] and an `int64` fits [`ColumnValue::Int`]. A `string` that is
/// not text, or holds a NUL `PostgreSQL` would reject, is null; the raw log keeps its
/// bytes. Arrays and tuples are documents in the same form the `decoded_logs` table uses.
fn cell(kind: ColumnType, argument: &DecodedArg) -> ColumnValue {
    match (kind, &argument.value) {
        (ColumnType::Uint, TypedValue::Uint { value, .. }) => {
            ColumnValue::Uint(value.saturating_to())
        }
        (ColumnType::Int, TypedValue::Int { value, .. }) => ColumnValue::Int(value.as_i64()),
        (ColumnType::BigInt, TypedValue::Uint { value, .. }) => ColumnValue::BigInt {
            negative: false,
            magnitude: *value,
        },
        (ColumnType::BigInt, TypedValue::Int { value, .. }) => ColumnValue::BigInt {
            negative: value.is_negative(),
            magnitude: value.unsigned_abs(),
        },
        (ColumnType::Bool, TypedValue::Bool { value }) => ColumnValue::Bool(*value),
        (ColumnType::Text, TypedValue::Address { value }) => ColumnValue::hex(value),
        (ColumnType::Text, TypedValue::IndexedHash { value }) => ColumnValue::hex(value),
        (ColumnType::Text, TypedValue::Function { value }) => ColumnValue::hex(value),
        (ColumnType::Text, TypedValue::FixedBytes { value, .. } | TypedValue::Bytes { value }) => {
            ColumnValue::bytes(value)
        }
        (ColumnType::Text, TypedValue::String { text, .. }) => text
            .as_ref()
            .filter(|text| !text.contains('\0'))
            .map_or(ColumnValue::Null, |text| ColumnValue::Text(text.clone())),
        (ColumnType::Document, value) => ColumnValue::document(value),
        _ => ColumnValue::Null,
    }
}

impl Row {
    /// Which table this row belongs to.
    #[must_use]
    pub const fn table(&self) -> TableId {
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
            .unwrap_or_else(|| panic!("{column} is a column of the {:?} table", self.table));
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

    /// The hash of the block this row belongs to, from
    /// [`Table::block_hash_column`] or an event table's `block_hash`; `None` for a table
    /// whose rows belong to no block.
    #[must_use]
    pub fn block_hash(&self) -> Option<&str> {
        let column = match self.table {
            TableId::Dataset(table) => table.block_hash_column(),
            TableId::Event(_) => Some("block_hash"),
        };
        column.map(|column| self.text(column))
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
        Event::Contract(c) => (Table::Contract, contract_cells(c)),
        Event::Reorg(r) => (Table::Reorg, reorg_cells(r)),
        Event::AcceptedBlock(a) => (Table::AcceptedBlock, accepted_block_cells(a)),
    };
    Row::new(table, cells.with_common(chain, &event.dedupe_key()))
}

/// One row's worth of a dataset: the table, its columns, and the values that line up with
/// them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    table: TableId,
    columns: Arc<[Column]>,
    values: Vec<ColumnValue>,
}

impl Row {
    /// Builds a row from its table's cells, splitting them into the header and the values.
    fn new(table: Table, cells: Columns) -> Self {
        let (columns, values): (Vec<_>, _) = cells.0.into_iter().unzip();
        Self {
            table: table.into(),
            columns: columns.into(),
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

/// The `blocks` table, as one cell per column.
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
    (Column::text("withdrawals_root").optional(), |b| {
        ColumnValue::some(b.withdrawals_root, ColumnValue::hex)
    }),
    (Column::text("logs_bloom"), |b| {
        ColumnValue::hex(b.logs_bloom)
    }),
    (Column::text("miner"), |b| ColumnValue::hex(b.miner)),
    (Column::text("difficulty"), |b| {
        ColumnValue::u256(b.difficulty)
    }),
    (Column::text("total_difficulty").optional(), |b| {
        ColumnValue::some(b.total_difficulty, ColumnValue::u256)
    }),
    (Column::text("size").optional(), |b| {
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
    (Column::uint("base_fee_per_gas").optional(), |b| {
        ColumnValue::some(b.base_fee_per_gas, ColumnValue::Uint)
    }),
    (Column::uint("blob_gas_used").optional(), |b| {
        ColumnValue::some(b.blob_gas_used, ColumnValue::Uint)
    }),
    (Column::uint("excess_blob_gas").optional(), |b| {
        ColumnValue::some(b.excess_blob_gas, ColumnValue::Uint)
    }),
    (Column::text("parent_beacon_block_root").optional(), |b| {
        ColumnValue::some(b.parent_beacon_block_root, ColumnValue::hex)
    }),
    (Column::document("ommers"), |b| {
        ColumnValue::hex_list(&b.ommers)
    }),
    (Column::document("transaction_hashes"), |b| {
        ColumnValue::hex_list(&b.transaction_hashes)
    }),
];

/// Builds the `blocks` table's cells for one block.
fn block_cells(b: &Block) -> Columns {
    Columns(
        BLOCK_CELLS
            .iter()
            .map(|(column, render)| (column.clone(), render(b)))
            .collect(),
    )
}

/// The `transactions` table, as one cell per column.
const TRANSACTION_CELLS: [(Column, fn(&Transaction) -> ColumnValue); 20] = [
    (Column::text("hash"), |t| ColumnValue::hex(t.hash)),
    (Column::uint("nonce"), |t| ColumnValue::Uint(t.nonce)),
    (Column::uint("transaction_index"), |t| {
        ColumnValue::Uint(t.transaction_index)
    }),
    (Column::text("from_address"), |t| ColumnValue::hex(t.from)),
    (Column::text("to_address").optional(), |t| {
        ColumnValue::some(t.to, ColumnValue::hex)
    }),
    (Column::text("value"), |t| ColumnValue::u256(t.value)),
    (Column::uint("gas"), |t| ColumnValue::Uint(t.gas)),
    (Column::text("gas_price").optional(), |t| {
        ColumnValue::some(t.gas_price, ColumnValue::wei)
    }),
    (Column::text("max_fee_per_gas").optional(), |t| {
        ColumnValue::some(t.max_fee_per_gas, ColumnValue::wei)
    }),
    (Column::text("max_priority_fee_per_gas").optional(), |t| {
        ColumnValue::some(t.max_priority_fee_per_gas, ColumnValue::wei)
    }),
    (Column::text("max_fee_per_blob_gas").optional(), |t| {
        ColumnValue::some(t.max_fee_per_blob_gas, ColumnValue::wei)
    }),
    (Column::text("input"), |t| ColumnValue::bytes(&t.input)),
    (Column::uint("transaction_type"), |t| {
        ColumnValue::Uint(u64::from(t.transaction_type))
    }),
    (Column::uint("chain_id").optional(), |t| {
        ColumnValue::some(t.chain_id, ColumnValue::Uint)
    }),
    (Column::document("access_list").optional(), |t| {
        ColumnValue::optional_document(&t.access_list)
    }),
    (Column::document("blob_versioned_hashes").optional(), |t| {
        ColumnValue::optional_document(&t.blob_versioned_hashes)
    }),
    (Column::document("authorization_list").optional(), |t| {
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

/// Builds the `transactions` table's cells for one transaction.
fn transaction_cells(t: &Transaction) -> Columns {
    Columns(
        TRANSACTION_CELLS
            .iter()
            .map(|(column, render)| (column.clone(), render(t)))
            .collect(),
    )
}

/// The `receipts` table, as one cell per column.
const RECEIPT_CELLS: [(Column, fn(&Receipt) -> ColumnValue); 17] = [
    (Column::text("transaction_hash"), |r| {
        ColumnValue::hex(r.transaction_hash)
    }),
    (Column::uint("transaction_index"), |r| {
        ColumnValue::Uint(r.transaction_index)
    }),
    (Column::text("from_address"), |r| ColumnValue::hex(r.from)),
    (Column::text("to_address").optional(), |r| {
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
    (Column::text("contract_address").optional(), |r| {
        ColumnValue::some(r.contract_address, ColumnValue::hex)
    }),
    (Column::text("logs_bloom"), |r| {
        ColumnValue::hex(r.logs_bloom)
    }),
    (Column::uint("blob_gas_used").optional(), |r| {
        ColumnValue::some(r.blob_gas_used, ColumnValue::Uint)
    }),
    (Column::text("blob_gas_price").optional(), |r| {
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

/// Builds the `receipts` table's cells for one receipt.
fn receipt_cells(r: &Receipt) -> Columns {
    Columns(
        RECEIPT_CELLS
            .iter()
            .map(|(column, render)| (column.clone(), render(r)))
            .collect(),
    )
}

/// The `logs` table, as one cell per column.
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
    (Column::text("topic0").optional(), |l| {
        ColumnValue::some(l.topic0, ColumnValue::hex)
    }),
    (Column::text("topic1").optional(), |l| {
        ColumnValue::some(l.topic1, ColumnValue::hex)
    }),
    (Column::text("topic2").optional(), |l| {
        ColumnValue::some(l.topic2, ColumnValue::hex)
    }),
    (Column::text("topic3").optional(), |l| {
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

/// Builds the `logs` table's cells for one log.
fn log_cells(l: &Log) -> Columns {
    Columns(
        LOG_CELLS
            .iter()
            .map(|(column, render)| (column.clone(), render(l)))
            .collect(),
    )
}

/// The `decoded_logs` table, as one cell per column.
///
/// The two documents hold what varies per event — the indexed and non-indexed arguments —
/// while everything that identifies the row is a typed column: a store can key, join,
/// filter, and partition on `protocol`, `address`, `selector`, `event_id`, `block_hash`,
/// `block_timestamp`, and `log_index` without parsing the record. The argument *values*
/// still vary in type per event, so they stay documents; the identity does not, and keeping
/// it locked in a document would force every query back through JSON.
const DECODED_CELLS: [(Column, fn(&Decoded) -> ColumnValue); 16] = [
    (Column::text("name"), |d| ColumnValue::Text(d.name.clone())),
    (Column::text("address"), |d| ColumnValue::hex(d.address)),
    (Column::text("protocol"), |d| {
        ColumnValue::Text(d.protocol.clone())
    }),
    (Column::text("contract"), |d| {
        ColumnValue::Text(d.contract.clone())
    }),
    (Column::text("selector"), |d| ColumnValue::hex(d.selector)),
    (Column::text("event_id"), |d| ColumnValue::hex(d.event_id)),
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

/// Builds the `decoded_logs` table's cells for one decoded record.
fn decoded_cells(d: &Decoded) -> Columns {
    Columns(
        DECODED_CELLS
            .iter()
            .map(|(column, render)| (column.clone(), render(d)))
            .collect(),
    )
}

/// The `contracts` table, as one cell per column.
///
/// The provenance columns of Allium's `dex.pools`, generalized past pools: what was
/// created, by which factory, and where its creation log sits.
const CONTRACT_CELLS: [(Column, fn(&Contract) -> ColumnValue); 10] = [
    (Column::text("protocol"), |c| {
        ColumnValue::Text(c.protocol.clone())
    }),
    (Column::text("name"), |c| ColumnValue::Text(c.name.clone())),
    (Column::text("address"), |c| ColumnValue::hex(c.address)),
    (Column::text("factory_address"), |c| {
        ColumnValue::hex(c.factory_address)
    }),
    (Column::text("transaction_hash"), |c| {
        ColumnValue::hex(c.transaction_hash)
    }),
    (Column::uint("transaction_index"), |c| {
        ColumnValue::Uint(c.transaction_index)
    }),
    (Column::uint("log_index"), |c| {
        ColumnValue::Uint(c.log_index)
    }),
    (Column::uint("block_number"), |c| {
        ColumnValue::Uint(c.block_number)
    }),
    (Column::text("block_hash"), |c| {
        ColumnValue::hex(c.block_hash)
    }),
    (Column::uint("block_timestamp"), |c| {
        ColumnValue::Uint(c.block_timestamp)
    }),
];

/// Builds the `contracts` table's cells for one discovered contract.
fn contract_cells(c: &Contract) -> Columns {
    Columns(
        CONTRACT_CELLS
            .iter()
            .map(|(column, render)| (column.clone(), render(c)))
            .collect(),
    )
}

/// The `reorgs` table, as one cell per column.
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

/// Builds the `reorgs` table's cells for one reorg.
fn reorg_cells(r: &Reorg) -> Columns {
    Columns(
        REORG_CELLS
            .iter()
            .map(|(column, render)| (column.clone(), render(r)))
            .collect(),
    )
}

/// The `accepted_blocks` table, as one cell per column: the identity and linkage a
/// restart needs, and nothing else.
const ACCEPTED_BLOCK_CELLS: [(Column, fn(&AcceptedBlock) -> ColumnValue); 4] = [
    (Column::uint("height"), |a| ColumnValue::Uint(a.height)),
    (Column::text("hash"), |a| ColumnValue::hex(a.hash)),
    (Column::text("parent_hash"), |a| {
        ColumnValue::hex(a.parent_hash)
    }),
    (Column::uint("timestamp"), |a| {
        ColumnValue::Uint(a.timestamp)
    }),
];

/// Builds the `accepted_blocks` table's cells for one accepted block.
fn accepted_block_cells(a: &AcceptedBlock) -> Columns {
    Columns(
        ACCEPTED_BLOCK_CELLS
            .iter()
            .map(|(column, render)| (column.clone(), render(a)))
            .collect(),
    )
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{Address, B256, Bytes, I256, TxHash, U256};

    use crate::wire::envelope::{
        AcceptedBlock, Block, ChainId, Contract, Decoded, DecodedArg, Event, Log, Receipt, Reorg,
        Transaction,
    };
    use crate::wire::typed::{AbiType, TypedValue};

    use super::{COMMON_COLUMNS, ColumnType, ColumnValue, Schema, Table, TableId, row_for};

    /// A decoded record with no arguments, for a test to fill in.
    fn decoded() -> Decoded {
        Decoded {
            event_id: hash(0x08),
            name: "E".to_owned(),
            address: Address::from([0xd0; 20]),
            protocol: "p".to_owned(),
            contract: "C".to_owned(),
            selector: hash(0x07),
            signature: "E()".to_owned(),
            anonymous: false,
            transaction_hash: TxHash::from([0x11; 32]),
            transaction_index: 3,
            log_index: 7,
            indexed: Vec::new(),
            body: Vec::new(),
            block_number: 100,
            block_hash: hash(0x01),
            block_timestamp: 1_700_000_000,
        }
    }

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
                contract: "UniswapV3Pool".to_owned(),
                event_id: hash(0x08),
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
            Event::Contract(Box::new(Contract {
                protocol: "uniswap_v3".to_owned(),
                name: "UniswapV3Pool".to_owned(),
                address: Address::from([0xd0; 20]),
                factory_address: Address::from([0xfa; 20]),
                transaction_hash: TxHash::from([0x11; 32]),
                transaction_index: 3,
                log_index: 6,
                block_number: 100,
                block_hash: hash(0x01),
                block_timestamp: 1_700_000_000,
            })),
            Event::Reorg(Reorg {
                height: 100,
                new_head_hash: hash(0x01),
                orphaned_hashes: vec![hash(0x02)],
            }),
            Event::AcceptedBlock(AcceptedBlock {
                height: 100,
                hash: hash(0x01),
                parent_hash: hash(0x02),
                timestamp: 1_700_000_000,
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

    /// Every table but `reorgs` names its block, by a text column it really has, so a
    /// reorg retracts its rows. A table that answered `None` by mistake would keep an
    /// orphaned branch forever.
    #[test]
    fn every_table_but_reorg_names_its_block() {
        for event in every_kind() {
            let row = row_for(&chain(), &event);
            let TableId::Dataset(table) = row.table() else {
                panic!("a dataset event is a dataset row");
            };
            if table == Table::Reorg {
                assert_eq!(row.block_hash(), None, "a reorg belongs to no block");
                continue;
            }
            let column = table
                .block_hash_column()
                .expect("every other table belongs to a block");
            assert!(
                table
                    .columns()
                    .iter()
                    .any(|c| c.name == column && c.kind == ColumnType::Text && c.required),
                "{table}.{column} is a required text column"
            );
            assert_eq!(
                row.block_hash(),
                Some(format!("{:#x}", hash(0x01)).as_str())
            );
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
            assert!(
                tail.iter().all(|column| column.required),
                "chain and dedupe_key are present on every row"
            );
        }
    }

    /// Absence is declared on the column, beside the renderer that can return null.
    #[test]
    fn only_fields_the_chain_can_omit_are_optional() {
        let optional = [
            ("blocks", "withdrawals_root"),
            ("blocks", "total_difficulty"),
            ("blocks", "size"),
            ("blocks", "base_fee_per_gas"),
            ("blocks", "blob_gas_used"),
            ("blocks", "excess_blob_gas"),
            ("blocks", "parent_beacon_block_root"),
            ("transactions", "to_address"),
            ("transactions", "gas_price"),
            ("transactions", "max_fee_per_gas"),
            ("transactions", "max_priority_fee_per_gas"),
            ("transactions", "max_fee_per_blob_gas"),
            ("transactions", "chain_id"),
            ("transactions", "access_list"),
            ("transactions", "blob_versioned_hashes"),
            ("transactions", "authorization_list"),
            ("receipts", "to_address"),
            ("receipts", "contract_address"),
            ("receipts", "blob_gas_used"),
            ("receipts", "blob_gas_price"),
            ("logs", "topic0"),
            ("logs", "topic1"),
            ("logs", "topic2"),
            ("logs", "topic3"),
        ];
        let mut found = 0;
        for table in Table::ALL {
            for column in table.columns() {
                if !column.required {
                    found += 1;
                    assert!(
                        optional.contains(&(table.name(), column.name.as_ref())),
                        "{table}.{} is optional without being a field the chain can omit",
                        column.name
                    );
                }
            }
        }
        assert_eq!(found, optional.len());
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
                    ColumnType::Int => matches!(value, ColumnValue::Int(_) | ColumnValue::Null),
                    ColumnType::BigInt => {
                        matches!(value, ColumnValue::BigInt { .. } | ColumnValue::Null)
                    }
                    ColumnType::Text => matches!(value, ColumnValue::Text(_) | ColumnValue::Null),
                    ColumnType::Bool => matches!(value, ColumnValue::Bool(_) | ColumnValue::Null),
                    ColumnType::Document => {
                        matches!(value, ColumnValue::Document(_) | ColumnValue::Null)
                    }
                };
                assert!(
                    compatible,
                    "the {:?} table's {column:?} column is {:?} but the value is {value:?}",
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
            contract: "UniswapV3Pool".to_owned(),
            event_id: hash(0x08),
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
        assert_eq!(row.text("event_id"), format!("{:#x}", decoded.event_id));
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

    #[test]
    fn names_become_snake_case() {
        for (name, snake) in [
            ("sqrtPriceX96", "sqrt_price_x96"),
            ("amount0", "amount0"),
            ("ETHAmount", "eth_amount"),
            ("UniswapV3Pool", "uniswap_v3_pool"),
            ("_from", "from"),
        ] {
            assert_eq!(super::snake_case(name), snake, "{name}");
        }
    }

    fn arg(position: usize, name: &str, value: TypedValue) -> DecodedArg {
        DecodedArg {
            position,
            name: name.to_owned(),
            abi_type: AbiType {
                kind: String::new(),
                components: Vec::new(),
            },
            value,
        }
    }

    /// A schema with one event table, `p_c_e`, for event `0x09…` of contract `p.C`.
    fn event_schema() -> Schema {
        let column = |name: &str, kind| super::Column::named(name.to_owned(), kind, true);
        let mut schema = Schema::default();
        assert!(schema.add_event(
            ("p", "C", hash(0x09)),
            "p_c_e".to_owned(),
            vec![
                column("amount", ColumnType::BigInt),
                column("tick", ColumnType::Int),
                column("", ColumnType::Uint),
                column("blockHash", ColumnType::Text),
                super::Column::named("note".to_owned(), ColumnType::Text, false),
                column("ids", ColumnType::Document),
            ],
        ));
        schema
    }

    /// An event's table gets a column per argument named from the ABI: unnamed ones by
    /// position, and one repeating a log column with its position appended. A name is
    /// taken once.
    #[test]
    fn an_event_table_names_a_column_per_argument() {
        let mut schema = event_schema();
        let table = schema.tables().last().expect("the event table");
        let names: Vec<&str> = table.columns.iter().map(|c| c.name.as_ref()).collect();
        assert_eq!(
            names[4..10],
            ["amount", "tick", "arg2", "block_hash_3", "note", "ids"]
        );
        assert!(!schema.add_event(("p", "C", hash(0x0a)), "p_c_e".to_owned(), Vec::new()));
    }

    /// A decoded record fills its event's table: wide integers exact with their sign, a
    /// string that is not storable text as null, an array as a document.
    #[test]
    fn a_decoded_record_fills_its_event_table() {
        let schema = event_schema();
        let id = hash(0x09);
        let decoded = Decoded {
            event_id: id,
            protocol: "p".to_owned(),
            contract: "C".to_owned(),
            indexed: vec![arg(
                3,
                "blockHash",
                TypedValue::IndexedHash { value: hash(0x07) },
            )],
            body: vec![
                arg(
                    0,
                    "amount",
                    TypedValue::Int {
                        value: I256::MIN,
                        bits: 256,
                    },
                ),
                arg(
                    1,
                    "tick",
                    TypedValue::Int {
                        value: I256::try_from(-197_317).expect("fits"),
                        bits: 24,
                    },
                ),
                arg(
                    2,
                    "",
                    TypedValue::Uint {
                        value: U256::from(3000),
                        bits: 24,
                    },
                ),
                arg(
                    4,
                    "note",
                    TypedValue::String {
                        value: Bytes::from_static(b"a\0b"),
                        text: Some("a\0b".to_owned()),
                    },
                ),
                arg(
                    5,
                    "ids",
                    TypedValue::Array {
                        value: vec![TypedValue::Bool { value: true }],
                    },
                ),
            ],
            ..decoded()
        };
        let row = schema
            .event_row(&chain(), &decoded)
            .expect("the event has a table");
        assert_eq!(row.table(), TableId::Event(0));
        assert_eq!(row.dedupe_key(), decoded.dedupe_key());
        assert_eq!(
            row.value("amount"),
            &ColumnValue::BigInt {
                negative: true,
                magnitude: I256::MIN.unsigned_abs(),
            }
        );
        assert_eq!(row.value("tick"), &ColumnValue::Int(-197_317));
        assert_eq!(row.value("arg2"), &ColumnValue::Uint(3000));
        assert_eq!(row.text("block_hash_3"), format!("{:#x}", hash(0x07)));
        assert_eq!(
            row.value("note"),
            &ColumnValue::Null,
            "PostgreSQL rejects a NUL"
        );
        assert!(matches!(row.value("ids"), ColumnValue::Document(_)));
        assert_eq!(row.text("block_hash"), format!("{:#x}", decoded.block_hash));

        let other = Decoded {
            contract: "Other".to_owned(),
            ..decoded
        };
        assert!(schema.event_row(&chain(), &other).is_none());
    }
}
