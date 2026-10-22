//! Why a store could not be opened, read, or written: the one error every store —
//! `DuckDB`, `PostgreSQL`, and the Delta Lake — returns, so a caller matches on what failed
//! rather than on which backend it was.
//!
//! An engine's own error travels as the cause of [`StoreError::Engine`], tagged with the
//! [`Operation`] that failed and what it failed on. Everything else is a failure the store
//! itself detects, typed here once.

use std::fmt;
use std::time::Duration;

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
    /// Creating, loading, emptying, or vacuuming a staging table.
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
    /// An earlier commit failed part-way, so the store refuses further writes: the
    /// failed batch is gone, and only reopening the store repairs what it left behind.
    #[error("an earlier commit failed; the store must be reopened")]
    Failed,
    /// A write did not finish within its deadline, so the store gave up on it rather than
    /// wait on a request that may never complete. Like a failed commit, it refuses every
    /// later write, and only reopening the store repairs what it left behind.
    #[error("a write did not finish within {}s; the store must be reopened", after.as_secs())]
    Stalled {
        /// The deadline it missed.
        after: Duration,
    },
    /// A batch accepts a block the store already committed, without a `reorg` orphaning
    /// it first. A store that appends rather than upserts would write it twice.
    #[error("block {block} is already committed")]
    Replayed {
        /// The block's hash, as `0x` hex.
        block: String,
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
    /// Whether opening the store again fails the same way, so a supervisor should stop
    /// restarting it: a table whose stored shape or declaration is wrong, or a stored
    /// value that does not parse. Each needs an operator — a table dropped, a definition
    /// fixed — not another try.
    ///
    /// An engine failure may pass, and so may [`Locked`](Self::Locked): during a deploy
    /// the old process can hold the lock for a moment after the new one starts.
    #[must_use]
    pub fn is_permanent(&self) -> bool {
        match self {
            Self::Drift { .. } | Self::Reserved { .. } | Self::Table(_) | Self::Restore(_) => true,
            Self::Engine { .. }
            | Self::Locked { .. }
            | Self::Failed
            | Self::Stalled { .. }
            | Self::Replayed { .. } => false,
        }
    }

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

    /// `value`, read back from `column` as text, parsed as what the column holds: a hash,
    /// an address, or a number a store keeps as text.
    pub(crate) fn parse<T: std::str::FromStr>(
        column: &'static str,
        value: &str,
    ) -> Result<T, Self> {
        value.parse().map_err(|_| Self::new(column, value))
    }
}

/// How one stored column differs from its definition, besides being absent or extra.
#[derive(Debug)]
pub(crate) enum ColumnDrift {
    /// It holds another type, each as the store names types.
    Type {
        /// The stored type.
        found: String,
        /// The type its definition maps to.
        expected: String,
    },
    /// It allows nulls where its definition does not, or the other way round.
    Nullability,
}

/// Fails with [`StoreError::Drift`] when `stored`, the columns of `table` as the store
/// reports them, lacks one of `expected`, holds one `expected` does not declare, or holds
/// one that `differs` from its declaration, so a changed definition stops the run at
/// startup rather than at its first write.
///
/// Every store checks its tables with this, each comparing what its engine reports: a
/// database its type names, the lake its Arrow fields. The first difference is the one
/// reported, in the definition's column order.
pub(crate) fn check_columns<N: AsRef<str>, M: AsRef<str>, T>(
    table: &str,
    expected: &[(N, T)],
    stored: &[(M, T)],
    differs: impl Fn(&T, &T) -> Option<ColumnDrift>,
) -> Result<(), StoreError> {
    let drift = |difference: String| StoreError::Drift {
        table: table.to_owned(),
        difference,
    };
    for (name, declared) in expected {
        let name = name.as_ref();
        let Some((_, found)) = stored.iter().find(|(stored, _)| stored.as_ref() == name) else {
            return Err(drift(format!("it has no {name} column")));
        };
        match differs(found, declared) {
            Some(ColumnDrift::Type { found, expected }) => {
                return Err(drift(format!("{name} is {found}, not {expected}")));
            }
            Some(ColumnDrift::Nullability) => {
                return Err(drift(format!("{name} differs in nullability")));
            }
            None => {}
        }
    }
    let declared = |name: &str| expected.iter().any(|(column, _)| column.as_ref() == name);
    match stored.iter().find(|(name, _)| !declared(name.as_ref())) {
        Some((name, _)) => Err(drift(format!(
            "it has a {} column it should not",
            name.as_ref()
        ))),
        None => Ok(()),
    }
}
