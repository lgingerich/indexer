//! The tables a store persists, declared once and rendered by every store.
//!
//! A table is a [`TableDef`]: a name, typed columns, the indexes a store should build,
//! and the column a reorg deletes by. Dataset tables are declared with a
//! `DatasetBuilder` that pairs each column with the field it
//! reads, so the column's type and nullability come from the field's Rust type through
//! [`Cell`] and cannot disagree with the values written into it. Each decoded event gets a
//! table generated from its ABI at startup (see [`Schema::add_event`]).
//!
//! Nothing here knows SQL. A store maps [`ColumnType`] to its own types and renders the
//! statements (see `sink::sql`); a future store reads the same definitions.
//!
//! # Column types
//!
//! [`ColumnType`] is what every target can express:
//!
//! - [`Uint`](ColumnType::Uint) — block numbers, indices, gas amounts: 64-bit unsigned.
//! - [`Int`](ColumnType::Int) — a decoded `int8` through `int64`, such as a Uniswap tick.
//! - [`BigInt`](ColumnType::BigInt) — an exact integer up to 256 bits, signed or not:
//!   wei amounts, prices, difficulty, and decoded `uint256` arguments. Numeric, as Allium
//!   stores them, so a consumer sums without a cast.
//! - [`Text`](ColumnType::Text) — hashes, addresses, and bytes as the node's `0x` hex, and
//!   plain text.
//! - [`Bool`](ColumnType::Bool) and [`Timestamp`](ColumnType::Timestamp), a UTC instant
//!   with second precision.
//! - [`List`](ColumnType::List) — a decoded array of scalars, each element typed.
//! - [`Document`](ColumnType::Document) — JSON, for a value that does not flatten.
//!
//! Absence is not a type. A field the chain does not have is [`Value::Null`] in a column
//! that otherwise holds its real type.

mod cell;
mod datasets;
mod events;

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use alloy_primitives::B256;

use crate::wire::envelope::{ChainId, Event};

pub use cell::{Cell, Timestamp, Value};
pub use events::snake_case;

/// What a column holds, in the vocabulary every target shares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    /// A 64-bit unsigned integer: a block number, an index, or a gas amount.
    Uint,
    /// A 64-bit signed integer: a decoded `int8` through `int64`.
    Int,
    /// An exact integer up to 256 bits, signed or not.
    BigInt,
    /// `0x` hex or plain text.
    Text,
    /// A boolean.
    Bool,
    /// A UTC instant, to the second.
    Timestamp,
    /// A list of one scalar type. Build one with [`list`](Self::list), which keeps the
    /// element a scalar.
    List(&'static Self),
    /// A structured value that does not flatten, as JSON.
    Document,
}

impl ColumnType {
    /// A list of `element`, or `None` when `element` is not one of the scalars an ABI
    /// array holds: a list of lists or of documents is a document instead.
    #[must_use]
    pub const fn list(element: Self) -> Option<Self> {
        Some(Self::List(match element {
            Self::Uint => &Self::Uint,
            Self::Int => &Self::Int,
            Self::BigInt => &Self::BigInt,
            Self::Text => &Self::Text,
            Self::Bool => &Self::Bool,
            Self::Timestamp | Self::List(_) | Self::Document => return None,
        }))
    }
}

/// One column of a table: its name, what it stores, and whether a row may leave it null.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// The name. A chain field keeps its chain name (`from_address` rather than `from`,
    /// since `from` is a SQL keyword).
    pub name: Cow<'static, str>,
    /// What the column holds.
    pub kind: ColumnType,
    /// Whether a row may hold [`Value::Null`] here; a store renders the rest `NOT NULL`.
    pub nullable: bool,
}

impl Column {
    /// A column named `name`.
    #[must_use]
    pub fn new(name: impl Into<Cow<'static, str>>, kind: ColumnType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            kind,
            nullable,
        }
    }
}

/// Every dataset table, by name. A store files a row under its table's [`TableId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Table {
    /// Block headers and metadata.
    Block,
    /// Transactions, from their block's array, each joined to its receipt.
    Transaction,
    /// Logs, one row per log.
    Log,
    /// Decoded event records, whose arguments ride as documents in their raw ABI form.
    Decoded,
    /// Contracts discovered from a factory's creation event.
    Contract,
    /// Reorg markers: which block hashes stopped being canonical.
    Reorg,
    /// Accepted block identities: the ledger a restart resumes from.
    AcceptedBlock,
}

impl Table {
    /// Every dataset table, in the order a store creates them.
    pub const ALL: [Self; 7] = [
        Self::Block,
        Self::Transaction,
        Self::Log,
        Self::Decoded,
        Self::Contract,
        Self::Reorg,
        Self::AcceptedBlock,
    ];

    /// The table's name. Plural, as Allium names its tables.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Block => "blocks",
            Self::Transaction => "transactions",
            Self::Log => "logs",
            Self::Decoded => "decoded_logs",
            Self::Contract => "contracts",
            Self::Reorg => "reorgs",
            Self::AcceptedBlock => "accepted_blocks",
        }
    }
}

impl std::fmt::Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

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

/// A secondary index: the columns it covers, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Index {
    /// The indexed columns.
    pub columns: Vec<Cow<'static, str>>,
}

/// One table a store creates and writes.
///
/// Every table ends with `chain` and `dedupe_key`, which together are its primary key: a
/// store upserts on them, so a replayed row updates rather than duplicates. Built with
/// [`TableDef::builder`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDef {
    /// Which table this is; rows name it by the same id.
    pub id: TableId,
    /// The table's name.
    pub name: String,
    /// The columns, in order, ending with `chain` and `dedupe_key`.
    pub columns: Vec<Column>,
    /// The indexes a store builds beside the primary key.
    pub indexes: Vec<Index>,
    /// The column naming each row's block, which a reorg deletes by; `None` for a table
    /// whose rows belong to no block.
    pub block_hash: Option<Cow<'static, str>>,
    /// The column holding that block's number, which bounds a reorg's delete from below.
    pub block_number: Option<Cow<'static, str>>,
}

/// The primary key every table shares, in key order.
pub const PRIMARY_KEY: [&str; 2] = ["chain", "dedupe_key"];

/// `PostgreSQL`'s identifier limit, in bytes; a longer name is silently truncated.
pub(crate) const MAX_IDENTIFIER: usize = 63;

/// `name`, or, when it would pass [`MAX_IDENTIFIER`], `name` cut short with a hash of the
/// whole of it, so two long names never truncate to one.
pub(crate) fn fit_identifier(name: String) -> String {
    if name.len() <= MAX_IDENTIFIER {
        return name;
    }
    let hash = alloy_primitives::hex::encode(&alloy_primitives::keccak256(&name)[..4]);
    let mut end = MAX_IDENTIFIER - hash.len() - 1;
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}_{hash}", &name[..end])
}

impl TableDef {
    /// Starts the table `name`, identified as `id`.
    #[must_use]
    pub fn builder(id: TableId, name: impl Into<String>) -> TableBuilder {
        TableBuilder {
            def: Self {
                id,
                name: name.into(),
                columns: Vec::new(),
                indexes: Vec::new(),
                block_hash: None,
                block_number: None,
            },
        }
    }

    /// The position of the column named `name`.
    #[must_use]
    pub fn position(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|column| column.name == name)
    }

    /// The position of the column named `name`, or the error naming it.
    ///
    /// # Errors
    ///
    /// Returns [`TableError::UnknownColumn`] when the table has no such column.
    pub fn require(&self, name: &str) -> Result<usize, TableError> {
        self.position(name)
            .ok_or_else(|| TableError::UnknownColumn {
                table: self.name.clone(),
                column: name.to_owned(),
            })
    }
}

/// Why a table could not be declared or a row of it built.
#[derive(Debug, thiserror::Error)]
pub enum TableError {
    /// Two columns of one table share a name.
    #[error("{table}.{column} is declared twice")]
    DuplicateColumn {
        /// The table.
        table: String,
        /// The repeated name.
        column: String,
    },
    /// An index, the reorg column, or a statement names a column the table does not have.
    #[error("{table} has no {column} column")]
    UnknownColumn {
        /// The table.
        table: String,
        /// The missing column.
        column: String,
    },
    /// A column a reorg deletes by is nullable or of the wrong type.
    #[error("{table}.{column} names each row's block, so it must be a required {expected}")]
    BlockColumn {
        /// The table.
        table: String,
        /// The column.
        column: String,
        /// What the column must hold.
        expected: &'static str,
    },
    /// A table is already named this.
    #[error("a table is already named {table}")]
    DuplicateTable {
        /// The repeated name.
        table: String,
    },
    /// A row's values do not line up with its table's columns: a decoded record carrying
    /// a different number of arguments than its event's table has columns.
    #[error("a {table} row has {found} values for {expected} columns")]
    Width {
        /// The table.
        table: String,
        /// The values the table's columns need, before the primary key.
        expected: usize,
        /// The values the row had.
        found: usize,
    },
    /// A document value could not be rendered as JSON.
    #[error("render a {table} document: {source}")]
    Json {
        /// The table.
        table: String,
        /// `serde_json`'s reason.
        source: serde_json::Error,
    },
}

/// Declares a [`TableDef`] one property at a time.
#[derive(Debug)]
pub struct TableBuilder {
    def: TableDef,
}

impl TableBuilder {
    /// Adds a column.
    #[must_use]
    pub fn column(mut self, column: Column) -> Self {
        self.def.columns.push(column);
        self
    }

    /// Adds an index on `columns`.
    #[must_use]
    pub fn index(mut self, columns: &[&'static str]) -> Self {
        self.def.indexes.push(Index {
            columns: columns.iter().map(|&column| column.into()).collect(),
        });
        self
    }

    /// Names the columns holding each row's block hash and number, and indexes the hash
    /// under `chain`, so a reorg deletes an orphaned block's rows by index.
    #[must_use]
    pub fn reorg_by(mut self, hash: &'static str, number: &'static str) -> Self {
        self.def.block_hash = Some(hash.into());
        self.def.block_number = Some(number.into());
        self.index(&["chain", hash])
    }

    /// Finishes the table, appending `chain` and `dedupe_key`.
    ///
    /// # Errors
    ///
    /// Returns [`TableError::DuplicateColumn`] when a column name repeats,
    /// [`TableError::UnknownColumn`] when an index or the reorg column names a column the
    /// table does not have, and [`TableError::BlockColumn`] when a reorg column is
    /// nullable or of the wrong type.
    pub fn build(mut self) -> Result<TableDef, TableError> {
        for name in PRIMARY_KEY {
            self.def
                .columns
                .push(Column::new(name, ColumnType::Text, false));
        }
        let def = self.def;
        for (position, column) in def.columns.iter().enumerate() {
            if def.position(&column.name) != Some(position) {
                return Err(TableError::DuplicateColumn {
                    table: def.name.clone(),
                    column: column.name.to_string(),
                });
            }
        }
        for name in def.indexes.iter().flat_map(|index| &index.columns) {
            def.require(name)?;
        }
        for (name, kind, expected) in [
            (&def.block_hash, ColumnType::Text, "text"),
            (&def.block_number, ColumnType::Uint, "unsigned integer"),
        ] {
            let Some(name) = name else { continue };
            let column = &def.columns[def.require(name)?];
            if column.kind != kind || column.nullable {
                return Err(TableError::BlockColumn {
                    table: def.name.clone(),
                    column: name.to_string(),
                    expected,
                });
            }
        }
        Ok(def)
    }
}

/// One table's row: a value per column, in column order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    table: Arc<TableDef>,
    /// Every column's value, the primary key included.
    values: Vec<Value>,
    chain: String,
    dedupe_key: String,
}

impl Row {
    /// A row of `table` holding `values`, then the two primary-key columns.
    ///
    /// # Errors
    ///
    /// Returns [`TableError::Width`] when `values` does not fill every column before the
    /// primary key.
    fn new(
        table: &Arc<TableDef>,
        mut values: Vec<Value>,
        chain: &str,
        dedupe_key: String,
    ) -> Result<Self, TableError> {
        let expected = table.columns.len() - PRIMARY_KEY.len();
        if values.len() != expected {
            return Err(TableError::Width {
                table: table.name.clone(),
                expected,
                found: values.len(),
            });
        }
        values.extend([
            Value::Text(chain.to_owned()),
            Value::Text(dedupe_key.clone()),
        ]);
        Ok(Self {
            table: Arc::clone(table),
            values,
            chain: chain.to_owned(),
            dedupe_key,
        })
    }

    /// The table this row belongs to.
    #[must_use]
    pub fn table(&self) -> &TableDef {
        &self.table
    }

    /// The values, in column order.
    #[must_use]
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// The value in the named column, or `None` when the table has no such column.
    #[must_use]
    pub fn value(&self, column: &str) -> Option<&Value> {
        self.values.get(self.table.position(column)?)
    }

    /// The named column's text, or `None` when the table has no such column or it does
    /// not hold text.
    #[must_use]
    pub fn text(&self, column: &str) -> Option<&str> {
        match self.value(column)? {
            Value::Text(text) => Some(text),
            _ => None,
        }
    }

    /// The row's identity: the key a store deduplicates and upserts on.
    #[must_use]
    pub fn dedupe_key(&self) -> &str {
        &self.dedupe_key
    }

    /// The chain this row came from.
    #[must_use]
    pub fn chain(&self) -> &str {
        &self.chain
    }

    /// The hash of the block this row belongs to; `None` for a table whose rows belong to
    /// no block. The builder holds that column to required text.
    #[must_use]
    pub fn block_hash(&self) -> Option<&str> {
        self.text(self.table.block_hash.as_deref()?)
    }

    /// The height of the block this row belongs to; `None` for a table whose rows belong
    /// to no block. The builder holds that column to a required unsigned integer.
    #[must_use]
    pub fn block_number(&self) -> Option<u64> {
        match self.value(self.table.block_number.as_deref()?)? {
            Value::Uint(height) => Some(*height),
            _ => None,
        }
    }
}

/// Every table a run writes: the dataset tables, then one typed table per decoded event.
#[derive(Debug, Clone)]
pub struct Schema {
    /// The dataset tables and how each reads its record.
    datasets: Arc<datasets::DatasetTables>,
    tables: Vec<Arc<TableDef>>,
    /// Each event table's position in `tables`, by the record's protocol, contract, and
    /// event definition.
    events: HashMap<(String, String, B256), usize>,
}

impl Schema {
    /// The dataset tables alone.
    ///
    /// # Errors
    ///
    /// Returns a [`TableError`] when a dataset table's declaration is inconsistent.
    pub fn new() -> Result<Self, TableError> {
        let datasets = datasets::DatasetTables::new()?;
        Ok(Self {
            tables: Table::ALL
                .iter()
                .map(|&table| Arc::clone(datasets.def(table)))
                .collect(),
            datasets: Arc::new(datasets),
            events: HashMap::new(),
        })
    }

    /// Every table, dataset tables first.
    #[must_use]
    pub fn tables(&self) -> &[Arc<TableDef>] {
        &self.tables
    }

    /// A dataset table's definition.
    #[must_use]
    pub fn dataset(&self, table: Table) -> &Arc<TableDef> {
        self.datasets.def(table)
    }

    /// An event's row in its dataset table.
    ///
    /// Every event is a row, a control signal included, so a store has no variant to
    /// handle and no event that is silently dropped.
    ///
    /// # Errors
    ///
    /// Returns [`TableError::Json`] when a document field does not render.
    pub fn row(&self, chain: &ChainId, event: &Event) -> Result<Row, TableError> {
        self.datasets.row(chain, event)
    }
}
