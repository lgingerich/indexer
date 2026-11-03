//! A backfill's settings file, what stops one, and what it did.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use alloy_primitives::Address;
use serde::Deserialize;

use super::USAGE;
use crate::decode::{CatalogError, DecodeError, DeriveError};
use crate::secret::Secret;
use crate::sink::StoreError;
use crate::sink::delta::LockError;
use crate::sink::resume::MAX_RESUME_LAG;
use crate::sink::table::TableError;
use crate::wire::envelope::ChainId;

/// A backfill's settings file, `indexer.<chain>.backfill.toml`. TOML; every table refuses
/// an unknown key, and a secret is named as in the indexer's settings, `{ env = "NAME" }`.
///
/// ```toml
/// chain = "ethereum"
/// protocols = "protocols/uniswap/v4"   # the manifests to backfill
/// # from = 25984373                    # the lake's first block when absent
/// # to = 26200000                      # the bound when absent (see [`LogReader::bound`](crate::sink::delta::LogReader::bound))
/// # only = ["0x…"]                     # discovered contracts to hand over; every one when absent
/// # out = "/tmp/indexer-backfill"      # write here instead; the lake is only read
///
/// # [postgres]                         # also fill the running indexer's database
/// # tables = ["contract_events"]       # as its [sink.postgres] routes them
/// # connection_string = { env = "INDEXER_POSTGRES_URL" }
///
/// [lake]
/// uri = "s3://blockchain-data"
///
/// [lake.storage]                       # passed to the object store, as [sink.delta.storage]
/// endpoint = { env = "S3_ENDPOINT" }
/// region = "auto"
/// access_key_id = { env = "S3_ACCESS_KEY_ID" }
/// secret_access_key = { env = "S3_SECRET_ACCESS_KEY" }
/// ```
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackfillSettings {
    /// The chain, as the running indexer names it in `[ingest] chain`.
    #[serde(deserialize_with = "crate::config::non_empty")]
    pub chain: String,
    /// The manifests to backfill, relative to the settings file.
    pub protocols: PathBuf,
    /// The first block to backfill; the lake's first when absent.
    #[serde(default)]
    pub from: Option<u64>,
    /// The last block to backfill; the bound no delete of the running store reaches when
    /// absent (see [`LogReader::bound`](crate::sink::delta::LogReader::bound)), and never above it.
    #[serde(default)]
    pub to: Option<u64>,
    /// The discovered contracts to hand over and backfill, when only some are wanted, each
    /// one the manifests' factories created in the lake. Every one when absent. The
    /// manifests' listed contracts are backfilled either way.
    #[serde(default)]
    pub only: Option<Vec<Address>>,
    /// Another lake to write to, such as a local directory, instead of the chain's own,
    /// which is then only read: to try a backfill out without touching what the running
    /// indexer serves. Reached with no storage options.
    #[serde(default)]
    pub out: Option<String>,
    /// The lake the running indexer writes, as its `[sink.delta]` names it.
    pub lake: LakeSettings,
    /// The running indexer's `PostgreSQL` store, as its `[sink.postgres]` names it, to
    /// fill too: the decoded tables it routes there get the same rows, upserted. Absent,
    /// only the lake is filled.
    #[cfg(feature = "postgres")]
    #[serde(default)]
    pub postgres: Option<crate::sink::PostgresSettings>,
}

/// Where the lake is: `[lake]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LakeSettings {
    /// The lake's root, as `[sink.delta] uri`: each chain's tables are under
    /// `<uri>/<chain>/`.
    #[serde(deserialize_with = "crate::config::non_empty")]
    pub uri: String,
    /// The object store's options, as `[sink.delta.storage]`.
    #[serde(default)]
    pub storage: BTreeMap<String, Secret>,
}

impl BackfillSettings {
    /// Reads the settings file at `path`, each secret it names from `env`, as the
    /// indexer's settings read theirs (see [`crate::secret::process_env`]).
    ///
    /// # Errors
    ///
    /// Returns [`BackfillError::Read`] when the file cannot be read and
    /// [`BackfillError::Parse`] when it is not valid settings, or names a secret whose
    /// variable is unset.
    pub fn from_file(path: &Path, env: crate::secret::Env) -> Result<Self, BackfillError> {
        let text = std::fs::read_to_string(path).map_err(|source| BackfillError::Read {
            path: path.display().to_string(),
            source,
        })?;
        Self::parse(&text, path.parent().unwrap_or_else(|| Path::new("")), env)
    }

    /// Parses settings text, resolving a relative `protocols` against `dir`, as the
    /// indexer's settings resolve theirs, and each secret it names from `env`.
    pub(super) fn parse(
        text: &str,
        dir: &Path,
        env: crate::secret::Env,
    ) -> Result<Self, BackfillError> {
        let mut settings: Self = crate::secret::reading(env, || toml::from_str(text))
            .map_err(|source| BackfillError::Parse { source })?;
        settings.protocols = dir.join(&settings.protocols);
        Ok(settings)
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
    /// The settings file could not be read.
    #[error("read settings at {path}: {source}")]
    Read {
        /// The path that failed.
        path: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// The settings file is not valid backfill settings.
    #[error("invalid settings: {source}")]
    Parse {
        /// `toml`'s error, which carries the line and column.
        source: toml::de::Error,
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
    /// The running indexer's current run started above the bound, so the blocks between
    /// would be left undecoded.
    #[error(
        "the running indexer's {store} decodes from block {from}, above the bound {bound}: \
         wait until the lake's tip passes {}",
        from + MAX_RESUME_LAG
    )]
    SessionAboveBound {
        /// The store.
        store: &'static str,
        /// The first block of its current run.
        from: u64,
        /// The bound.
        bound: u64,
    },
    /// Another backfill holds the lake, or took it over.
    #[error("{0}")]
    Lock(#[from] LockError),
    /// `[postgres] tables` names something the manifests do not have.
    #[cfg(feature = "postgres")]
    #[error("[postgres] tables: {source}")]
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
            | Self::Read { .. }
            | Self::Parse { .. }
            | Self::Catalog(_)
            | Self::NotDiscovered { .. }
            | Self::NoContracts { .. }
            | Self::EmptyLedger
            | Self::ToAboveBound { .. }
            | Self::NoSession { .. }
            | Self::NotDecoding { .. }
            | Self::EventsDiffer { .. }
            | Self::SessionAboveBound { .. }
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
    /// Contracts this run's discovery found below the running indexer's restart.
    pub discovered: usize,
    /// Contracts handed over, by this run or an earlier one, and backfilled.
    pub handed: usize,
    /// When some handed-over contract is decoded by the running indexer only above `to`,
    /// the highest block below that: a rerun from `to + 1` once the bound reaches it
    /// leaves no block between the two.
    pub rerun_through: Option<u64>,
}
