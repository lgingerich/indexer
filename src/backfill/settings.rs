//! A backfill's settings file, what stops one, and what it did.

use std::path::{Path, PathBuf};

use alloy_primitives::Address;

use super::USAGE;
use crate::config::BackfillRun;
use crate::decode::{CatalogError, DecodeError, DeriveError};
use crate::sink::StoreError;
use crate::sink::delta::DeltaSettings;
use crate::sink::delta::LockError;
use crate::sink::table::TableError;
use crate::window::UndoWindow;
use crate::wire::envelope::ChainId;

/// What a backfill runs with: the `[backfill]` section of the running indexer's settings
/// file, `indexer.<chain>.toml`, with the chain its `[ingest]` follows, the manifests its
/// `[decode]` loads, the lake its `[sink.delta]` writes, and the database its
/// `[sink.postgres]` writes.
#[derive(Debug)]
pub struct BackfillSettings {
    /// The chain, as the running indexer names it in `[ingest] chain`.
    pub chain: String,
    /// What to backfill: `[backfill]`.
    pub run: BackfillRun,
    /// Every manifest the running indexer decodes, which its `[sink.postgres] tables`
    /// name; the backfill's are a subtree of them.
    pub decoded: PathBuf,
    /// The lake the running indexer writes.
    pub lake: DeltaSettings,
    /// The running indexer's `PostgreSQL` store, to fill too: the decoded tables it
    /// routes there get the same rows, upserted. Absent, only the lake is filled.
    #[cfg(feature = "postgres")]
    pub postgres: Option<crate::sink::PostgresSettings>,
}

impl BackfillError {
    /// The running indexer's run started at `from`, above the `to` given, under `window`.
    pub(super) const fn session_above_to(from: u64, to: u64, window: UndoWindow) -> Self {
        Self::SessionAboveTo {
            from,
            to,
            ready: from + window.resume_lag(),
        }
    }
}

impl BackfillSettings {
    /// Reads the running indexer's settings file at `path`, each secret it names from
    /// `env` (see [`crate::secret::process_env`]), for its `[backfill]`.
    ///
    /// # Errors
    ///
    /// Returns [`BackfillError::Settings`] when the file cannot be read or is not valid
    /// settings, and [`BackfillError::Missing`] when it has no `[backfill]` or no
    /// `[sink.delta]`.
    pub fn from_file(path: &Path, env: crate::secret::Env) -> Result<Self, BackfillError> {
        Self::of(crate::config::Settings::from_file(path, env)?)
    }

    /// The backfill `settings` name.
    pub(super) fn of(settings: crate::config::Settings) -> Result<Self, BackfillError> {
        let decoded = settings.protocols_path();
        let run = settings.backfill.ok_or(BackfillError::Missing {
            section: "[backfill]",
        })?;
        let lake = settings.sink.delta.ok_or(BackfillError::Missing {
            section: "[sink.delta]",
        })?;
        Ok(Self {
            chain: settings.ingest.chain,
            decoded: decoded.unwrap_or_else(|| run.protocols.clone()),
            run,
            lake,
            #[cfg(feature = "postgres")]
            postgres: settings.sink.postgres,
        })
    }
}

/// Why a backfill could not run, or stopped.
#[derive(Debug, thiserror::Error)]
pub enum BackfillError {
    /// The command line is wrong.
    #[error("{problem}; usage: {USAGE}")]
    Usage {
        /// What is wrong with it.
        problem: String,
    },
    /// The settings file could not be read, or is not valid settings.
    #[error(transparent)]
    Settings(#[from] crate::config::SettingsError),
    /// The settings file has no section a backfill needs.
    #[error("the settings file has no {section}, which a backfill reads")]
    Missing {
        /// The section.
        section: &'static str,
    },
    /// The manifests could not be loaded.
    #[error("protocol manifests could not be loaded: {0}")]
    Catalog(#[from] CatalogError),
    /// A contract `only` names is not one the manifests' factories created in the lake.
    #[error("{address} in `only` is not a contract these manifests' factories created in the lake")]
    NotDiscovered {
        /// The contract.
        address: Address,
    },
    /// The running indexer did not record every handed-over contract in time.
    #[error(
        "the running indexer recorded {recorded} of {handed} handed-over contracts within \
         {seconds}s, not {first} among others; is it running a build that watches the lake's \
         backfills, with these manifests?"
    )]
    NotHandedOver {
        /// How many were handed over.
        handed: usize,
        /// How many it recorded.
        recorded: usize,
        /// One it did not.
        first: Address,
        /// How long the backfill waited.
        seconds: u64,
    },
    /// No contract of the manifests is listed on the chain.
    #[error("no contract of these manifests is listed on {chain}")]
    NoContracts {
        /// The settings' chain.
        chain: ChainId,
    },
    /// The lake's ledger is empty, so where the running store writes is unknown.
    #[error("the lake has no accepted blocks: nothing has been indexed to backfill from")]
    EmptyLedger,
    /// `to` is above the bound the running store's deletes may reach.
    #[error(
        "to = {to} is above {bound}, the highest block no delete of the running indexer \
         reaches"
    )]
    ToAboveBound {
        /// The block asked for.
        to: u64,
        /// The bound.
        bound: u64,
    },
    /// A store of the running indexer has recorded no session: it has not run, with an
    /// indexer that records them, since this lake's history began.
    #[error(
        "the running indexer's {store} has recorded no session: deploy an indexer that \
         records sessions, with these manifests, and let it write a block"
    )]
    NoSession {
        /// The store.
        store: &'static str,
    },
    /// The running indexer's current run does not decode a protocol, so the blocks above
    /// the backfill would be left undecoded.
    #[error(
        "the running indexer's {store} does not decode {protocol} since its restart at block \
         {from}: deploy the manifests to it first"
    )]
    NotDecoding {
        /// The store.
        store: &'static str,
        /// The protocol.
        protocol: String,
        /// The first block of its current run.
        from: u64,
    },
    /// The running indexer's current run decodes a protocol with other event definitions,
    /// so the backfill's rows would not match its keys.
    #[error(
        "the running indexer's {store} decodes {protocol} with other event definitions than \
         these manifests: run the backfill with the manifests the indexer runs"
    )]
    EventsDiffer {
        /// The store.
        store: &'static str,
        /// The protocol.
        protocol: String,
    },
    /// The running indexer's current run started above the `to` the settings give, so
    /// the blocks between would be left undecoded.
    #[error(
        "the running indexer decodes from block {from}, above to = {to}: raise `to` to it, or \
         leave `to` out and the backfill waits until the lake's tip passes {ready}"
    )]
    SessionAboveTo {
        /// The first block of its current run.
        from: u64,
        /// The `to` given.
        to: u64,
        /// The lake tip whose bound passes `from`.
        ready: u64,
    },
    /// Another backfill holds the lake, or took it over.
    #[error("{0}")]
    Lock(#[from] LockError),
    /// `[sink.postgres] tables` names something the manifests do not have.
    #[cfg(feature = "postgres")]
    #[error("[sink.postgres] tables: {source}")]
    Route {
        /// What was wrong with the list.
        source: crate::sink::route::RouteError,
    },
    /// The `PostgreSQL` store refused a write.
    #[cfg(feature = "postgres")]
    #[error("database: {source}")]
    Database {
        /// Why.
        source: crate::sink::SinkError,
    },
    /// A store could not be read or written.
    #[error("store: {0}")]
    Store(#[from] StoreError),
    /// A decoded record could not be made a row.
    #[error("row: {0}")]
    Table(#[from] TableError),
    /// A decoded record has no event table: the catalog and the schema disagree.
    #[error("{protocol}.{contract} {event} has no event table")]
    UnknownEvent {
        /// The record's protocol.
        protocol: String,
        /// The record's contract.
        contract: String,
        /// The record's event.
        event: String,
    },
    /// `HyperSync` could not be searched for the factories' logs before the lake.
    #[error("searching HyperSync for what the factories created before the lake: {0}")]
    Search(#[from] crate::ingest::source::SourceError),
    /// The decoder broke an invariant.
    #[error("decode: {0}")]
    Decode(#[from] DecodeError),
    /// A derivation computed a row that does not match its table.
    #[error("derive: {0}")]
    Derive(#[from] DeriveError),
}

impl BackfillError {
    /// Whether rerunning as is would fail the same way, so a supervisor should not retry.
    #[must_use]
    pub fn is_config(&self) -> bool {
        match self {
            Self::Usage { .. }
            | Self::Settings(_)
            | Self::Missing { .. }
            | Self::Catalog(_)
            | Self::NotDiscovered { .. }
            | Self::NoContracts { .. }
            | Self::EmptyLedger
            | Self::ToAboveBound { .. }
            | Self::NoSession { .. }
            | Self::NotDecoding { .. }
            | Self::EventsDiffer { .. }
            | Self::SessionAboveTo { .. }
            | Self::Lock(LockError::Held { .. }) => true,
            #[cfg(feature = "postgres")]
            Self::Route { .. } => true,
            #[cfg(feature = "postgres")]
            Self::Database { .. } => false,
            Self::Store(error) | Self::Lock(LockError::Store(error)) => error.is_permanent(),
            Self::Lock(LockError::Lost)
            | Self::Table(_)
            | Self::UnknownEvent { .. }
            | Self::NotHandedOver { .. }
            | Self::Search(_)
            | Self::Decode(_)
            | Self::Derive(_) => false,
        }
    }
}

/// What a backfill did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Backfilled {
    /// The first block backfilled.
    pub from: u64,
    /// The last block backfilled.
    pub to: u64,
    /// Reads made: a few hundred megabytes of `logs` files each.
    pub reads: usize,
    /// Commits made: each of at most [`COMMIT_BLOCKS`](super::COMMIT_BLOCKS) heights and [`COMMIT_LOGS`](super::COMMIT_LOGS) logs.
    pub commits: usize,
    /// Logs of the manifests' contracts read.
    pub logs: usize,
    /// Of them, the ones decoded.
    pub decoded: usize,
    /// Rows appended to the lake: the decoded rows its tables did not hold yet, two a
    /// record, one in `decoded_logs` and one in its event's table.
    pub appended: usize,
    /// Records decoded from the logs of the events manifests' `before_lake` names, fetched
    /// from below the lake's first block. Counted in `logs` and `decoded` too.
    pub before_lake: usize,
    /// Contracts this run's discovery found below the running indexer's restart.
    pub discovered: usize,
    /// Contracts handed over, by this run or an earlier one, and backfilled.
    pub handed: usize,
    /// When the settings give `to` and some handed-over contract is decoded by the
    /// running indexer only above it, the highest block below that: a rerun from
    /// `to + 1` once the bound reaches it leaves no block between the two. Without `to`
    /// the backfill waits for the bound to reach that block, and this is `None`.
    pub rerun_through: Option<u64>,
}
