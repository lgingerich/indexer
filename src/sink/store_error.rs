//! Why a store could not be opened, read, or written: the one error every store —
//! `DuckDB`, `PostgreSQL`, and the Delta Lake — returns, so a caller matches on what failed
//! rather than on which backend it was.
//!
//! An engine's own error travels as the cause of [`StoreError::Engine`], tagged with the
//! [`Operation`] that failed and what it failed on. Everything else is a failure the store
//! itself detects, typed here once.

use std::fmt;

use thiserror::Error;

use crate::sink::table::TableError;

/// An engine's own error, carried as its cause.
pub type EngineError = Box<dyn std::error::Error + Send + Sync>;

/// What a store was doing when its engine failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// Applying a setting.
    Configure,
    /// Opening the store: connecting to a database, or opening a table's location.
    Open,
    /// Creating the store's tables, and for a database its schema and indexes.
    Create,
    /// Starting a flush's transaction, in a store that has them.
    Begin,
    /// Deleting the rows of orphaned blocks, or of blocks a stopped commit left behind.
    Delete,
    /// Creating, loading, or dropping a staging table.
    Stage,
    /// Merging staged rows into their table.
    Merge,
    /// Appending rows to a table that is not merged into, such as a lake's.
    Append,
    /// Committing a flush's transaction, in a store that has them.
    Commit,
    /// Reading back what a previous run stored, at startup.
    Read,
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Configure => "apply setting",
            Self::Open => "open store",
            Self::Create => "create",
            Self::Begin => "begin transaction",
            Self::Delete => "delete rows of",
            Self::Stage => "stage",
            Self::Merge => "upsert",
            Self::Append => "append to",
            Self::Commit => "commit transaction",
            Self::Read => "read",
        })
    }
}

/// Why a store could not be opened, read, or written.
#[derive(Debug, Error)]
pub enum StoreError {
    /// The engine failed during `operation`, on `target` — a table, a setting, or a path —
    /// when there is one.
    #[error("{operation}{}: {source}", target.as_ref().map(|target| format!(" {target}")).unwrap_or_default())]
    Engine {
        /// What the store was doing.
        operation: Operation,
        /// What it was doing it to.
        target: Option<String>,
        /// The engine's own error.
        source: EngineError,
    },
    /// A value read back at startup did not parse.
    #[error(transparent)]
    Restore(#[from] InvalidStoredValue),
    /// A table's declaration is inconsistent.
    #[error(transparent)]
    Table(#[from] TableError),
    /// Another process already writes this database schema.
    #[error("another indexer is already writing database schema {schema}")]
    Locked {
        /// The database schema.
        schema: String,
    },
    /// A table declares a column the store reserves for itself, such as the lake's
    /// partition column.
    #[error("{table}.{column} is reserved by the store")]
    Reserved {
        /// The table.
        table: String,
        /// The column.
        column: String,
    },
    /// An existing table does not match its definition. Tables are not migrated, so the
    /// store needs a fresh location or the table dropped.
    #[error("table {table} does not match its definition: {difference}")]
    Drift {
        /// The table.
        table: String,
        /// The first difference found, such as a column with another type.
        difference: String,
    },
}

impl StoreError {
    /// Wraps an engine error from `operation` on `target`.
    pub(crate) fn engine<E: Into<EngineError>>(
        operation: Operation,
        target: Option<&str>,
    ) -> impl FnOnce(E) -> Self {
        move |source| Self::Engine {
            operation,
            target: target.map(str::to_owned),
            source: source.into(),
        }
    }
}

/// A value read back at startup that does not parse as its column's type.
#[derive(Debug, Error)]
#[error("stored {column} is invalid: {value}")]
pub struct InvalidStoredValue {
    /// The table and column, for example `contracts.address`.
    pub column: &'static str,
    /// The stored value.
    pub value: String,
}

impl InvalidStoredValue {
    /// The value `value`, read back from `column`, which does not parse.
    pub(crate) fn new(column: &'static str, value: impl Into<String>) -> Self {
        Self {
            column,
            value: value.into(),
        }
    }
}
