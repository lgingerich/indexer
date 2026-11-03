//! `indexer backfill`: decodes protocols added after their blocks were indexed, from the
//! logs the lake already holds, so adding a manifest does not mean indexing the chain
//! again. Nothing is fetched from a node or `HyperSync`.
//!
//! ```bash
//! indexer backfill indexer.ethereum.backfill.toml
//! ```
//!
//! A backfill has a settings file of its own, `indexer.<chain>.backfill.toml` (see
//! [`BackfillSettings`]): the chain, the lake, and the manifests to backfill, a subtree
//! of the running indexer's, so the tables they declare are the ones it writes.
//!
//! # Writing beside the running indexer
//!
//! The backfill writes into the running indexer's own tables without stopping it, and
//! never collides with it:
//!
//! - **Appends only.** It appends to `decoded_logs` and the event tables only the rows
//!   they do not hold yet, by key (see [`DecodedTables`]). In Delta a commit that only
//!   appends conflicts with no commit of the running store's, and a rerun, or a range the
//!   running indexer decoded too, writes each record once. In `PostgreSQL` it upserts.
//! - **Below the bound.** It writes no block above the lake's tip less
//!   four ledger windows (see [`LogReader::bound`]), which no delete of the running store
//!   reaches.
//! - **One at a time.** It holds the lake's backfill lease throughout (see
//!   [`RunLock`]).
//!
//! # Handing over without a gap
//!
//! Every store records a session with the first block it writes after a restart: that
//! block, and every event definition its run decodes (see
//! [`Session`]). Before writing to a store, the backfill
//! checks its newest session: the running indexer decodes every protocol being
//! backfilled, with these manifests' event definitions, from a block at or below the
//! backfill's last. Then the backfill covers every block below its restart and the
//! running indexer every block from it, with the same keys. So:
//!
//! 1. Deploy the manifests to the running indexer.
//! 2. Wait until the lake's tip is four ledger windows past its restart.
//! 3. Run the backfill. A refusal names what is missing.
//!
//! The backfill reads `logs` a few hundred megabytes of files at a time (see
//! [`LogReader`]), so each file is read once, and writes in commits of at most
//! [`COMMIT_BLOCKS`] heights and [`COMMIT_LOGS`] logs. A stopped run is run again, or
//! resumed by setting `from` to the `resume_from` its last read's log line names.
//!
//! A protocol feeding a derived table fills it too, as the running indexer's decode stage
//! does: each record's rows carry its transaction's sender and recipient, read from the
//! lake's `transactions` for just the records decoded (see [`LogReader::senders`]).
//!
//! # Contracts discovered below the restart
//!
//! A protocol that discovers its contracts (`created_by`) decodes, in the running
//! indexer, only the contracts created from its restart on. Those created below it the
//! backfill finds first: it reads its factories' logs below the restart, and their
//! children's when those create others in turn, until a pass finds no new factory. It
//! hands what it found to the running indexer, through a queue in the lake (see
//! [`HandoffQueue`]), which adds each to its decoder at a block it records in its
//! `handoffs`, and waits for those records. The running indexer decodes each contract
//! from its block on, and the backfill below it; `only` narrows the contracts handed
//! over and backfilled to some.
//!
//! The records are near the tip, above the bound, so a first run fills each handed-over
//! contract only through the bound, and says from where to run again once the tip has
//! passed them by the lag; a rerun hands over nothing it handed before.
//!
//! # Not handled
//!
//! Contracts created before the lake's first block are not in it to discover: they
//! need seeds, or their factories' logs fetched from a node (see the README's
//! "Stored-log replay").

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::{Range, RangeInclusive};
use std::path::{Path, PathBuf};

use alloy_primitives::{Address, B256};
use futures_util::stream::{self, StreamExt as _};
use serde::Deserialize;
use tracing::{info, warn};

use crate::decode::{
    Catalog, CatalogError, DecodeError, DecodedAs, Decoder, DeriveError, Record, Sent,
};
use crate::secret::Secret;
use crate::sink::StoreError;
use crate::sink::delta::{DecodedTables, HandoffQueue, LockError, LogReader, RunLock};
use crate::sink::resume::MAX_RESUME_LAG;
use crate::sink::table::{Row, Schema, Session, SessionEvent, TableError};
use crate::wire::datasets::evm::{Contract, Decoded, Log};
use crate::wire::envelope::{ChainId, Event};

mod lookback;
pub(crate) use lookback::Lookback;

/// How the subcommand is called.
pub const USAGE: &str = "indexer backfill <indexer.<chain>.backfill.toml>";

/// How many reads run at once: the next while one is decoded and written. More were no
/// faster, since a read already spreads over every core, and each holds its logs until
/// written.
const READ_AHEAD: usize = 2;

/// How long a backfill waits for the running indexer to record the contracts it handed
/// over: its watch reads the queue every ten seconds while a backfill holds the lake, and
/// each record lands with the store's next commit.
const HANDOFF_WAIT: std::time::Duration = std::time::Duration::from_mins(15);

/// How often it reads the records while it waits.
const HANDOFF_POLL: std::time::Duration = if cfg!(test) {
    std::time::Duration::from_millis(100)
} else {
    std::time::Duration::from_secs(10)
};

/// The most heights one commit covers, aligned to multiples of it.
pub const COMMIT_BLOCKS: u64 = 10_000;

/// The most logs one commit is decoded from, unless a single block holds more, so a
/// commit's rows stay a few hundred megabytes however busy the contract.
pub const COMMIT_LOGS: usize = 50_000;

/// A backfill's settings file, `indexer.<chain>.backfill.toml`. TOML; every table refuses
/// an unknown key, and a secret is named as in the indexer's settings, `{ env = "NAME" }`.
///
/// ```toml
/// chain = "ethereum"
/// protocols = "protocols/uniswap/v4"   # the manifests to backfill
/// # from = 25984373                    # the lake's first block when absent
/// # to = 26200000                      # the bound when absent (see [`LogReader::bound`])
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
    /// absent (see [`LogReader::bound`]), and never above it.
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
    fn parse(text: &str, dir: &Path, env: crate::secret::Env) -> Result<Self, BackfillError> {
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
    /// Commits made: each of at most [`COMMIT_BLOCKS`] heights and [`COMMIT_LOGS`] logs.
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

/// Runs the subcommand: loads the settings at `path`, then backfills.
///
/// # Errors
///
/// As [`backfill`], and [`BackfillError::Read`] and [`BackfillError::Parse`] for the
/// settings.
pub async fn run(path: &Path) -> Result<Backfilled, BackfillError> {
    backfill(&BackfillSettings::from_file(
        path,
        crate::secret::process_env,
    )?)
    .await
}

/// Decodes the logs of `settings`' manifests' contracts in its lake from `from` (the
/// lake's first block when absent) through `to` (the bound when absent, see
/// [`LogReader::bound`]), and appends each decoded row its table does not hold yet: in
/// the lake, or in `out` when given, and in the `PostgreSQL` store when one is named.
///
/// Holds the lake's backfill lease throughout, so no other backfill writes beside it.
/// Before writing to the running indexer's stores, checks each one's newest session:
/// its current run decodes the protocols, with these manifests' event definitions, from
/// a block at or below `to`, so the backfill and the running indexer leave no block
/// between them. Decodes on the calling worker thread, as Tokio's multi-threaded runtime
/// allows.
///
/// # Errors
///
/// Refuses manifests that do not load or list no contract on the chain; an empty ledger; a `to` above the bound; a store whose session
/// does not cover the protocols (see the module docs); and a lake another backfill
/// holds. Stops on a store that cannot be read or written, or a lease taken over.
pub async fn backfill(settings: &BackfillSettings) -> Result<Backfilled, BackfillError> {
    let chain = ChainId::new(&settings.chain);
    let catalog = Catalog::load(&settings.protocols, &chain)?;
    let addresses: Vec<Address> = catalog.seeds.iter().map(|&(address, _)| address).collect();
    if addresses.is_empty() {
        return Err(BackfillError::NoContracts { chain });
    }
    let protocols: Vec<String> = catalog
        .entries
        .iter()
        .map(|entry| entry.protocol.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let lake = &settings.lake;
    let target = settings.out.as_deref().unwrap_or(&lake.uri);
    let storage = match settings.out {
        Some(_) => BTreeMap::new(),
        None => lake.storage.clone(),
    };
    let run = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_secs())
    );
    let lock =
        RunLock::acquire(target, &storage, chain.as_str(), &run, &protocols.join(",")).await?;
    let filled = fill(
        settings, &chain, catalog, &addresses, &protocols, &lock, &storage,
    )
    .await;
    let released = lock.release().await;
    let done = filled?;
    released?;
    Ok(done)
}

/// [`backfill`]'s work, under the lease `lock`, writing to the lake reached with
/// `storage`.
async fn fill(
    settings: &BackfillSettings,
    chain: &ChainId,
    catalog: Catalog,
    addresses: &[Address],
    protocols: &[String],
    lock: &RunLock,
    storage: &BTreeMap<String, Secret>,
) -> Result<Backfilled, BackfillError> {
    let schema = catalog.schema().clone();
    let derives = catalog.derives();
    let discovers = catalog.discovers();
    let mut decoder = Decoder::new(catalog, []);
    let lake = &settings.lake;
    let reader =
        LogReader::open(&lake.uri, &lake.storage, &schema, chain.as_str(), derives).await?;
    let bound = reader.bound().ok_or(BackfillError::EmptyLedger)?;
    let to = match settings.to {
        Some(to) if to > bound => return Err(BackfillError::ToAboveBound { to, bound }),
        Some(to) => to,
        None => bound,
    };
    let wanted = schema.decoded_events();
    match &settings.out {
        // The running indexer writes the chain's own lake, not this one, so its session
        // says nothing about what this one holds.
        Some(out) => info!(out, "writing to another lake; the chain's is only read"),
        None => covers("lake", reader.session(), &wanted, protocols, to)?,
    }
    let target = settings.out.as_deref().unwrap_or(&lake.uri);
    let mut tables =
        DecodedTables::open(target, storage, &schema, chain.as_str(), reader.width()).await?;
    let mut database = Database::open(settings, &schema, chain, &wanted, protocols, to).await?;

    let from = settings.from.unwrap_or(0);
    // Each read owns a copy of the reader, so the next is read while one is decoded and
    // written. They share the cores: each Parquet stream holds a row group of up to a
    // million rows while it reads, so a stream per core per read would hold gigabytes.
    let streams = cores().div_ceil(READ_AHEAD);
    let mut done = Backfilled::default();
    let mut addresses = addresses.to_vec();
    if discovers {
        // The running indexer discovers what is created from its restart on; with `out`,
        // there is no running indexer to hand anything to.
        let restart = settings.out.is_none().then(|| reader.session()).flatten();
        let below = restart.map_or(to, |session| session.block_number.saturating_sub(1));
        let found = discover(&mut decoder, &reader, from..=below.min(to), streams).await?;
        done.discovered = found.len();
        let handed = match &settings.out {
            None => {
                handover(
                    settings,
                    &schema,
                    chain,
                    protocols,
                    found,
                    reader.width(),
                    lock,
                )
                .await?
            }
            // Nothing decodes them above `to`, so there is nothing to wait for.
            Some(_) => select(settings.only.as_deref(), found)?
                .into_iter()
                .map(|contract| (contract, to.saturating_add(1)))
                .collect(),
        };
        done.handed = handed.len();
        let restart = restart.map(|session| session.block_number);
        done.rerun_through = adopt(&mut decoder, &mut addresses, &handed, restart, to);
    }
    let addresses = &addresses[..];

    let reads = reader.reads(from, to);
    done.from = reads.first().map_or(to, |read| *read.start());
    done.to = to;
    info!(%chain, ?protocols, contracts = addresses.len(), from = done.from, to,
        reads = reads.len(), "backfilling from the lake's logs");
    let mut logs = stream::iter(reads.clone())
        .map(|blocks| tokio::spawn(reader.read(blocks, addresses, streams)))
        .buffered(READ_AHEAD);
    for read in reads {
        let Some(logs) = logs.next().await else {
            break;
        };
        let logs = logs.unwrap_or_else(|join| std::panic::resume_unwind(join.into_panic()))?;
        let (mut records, mut appended) = (0, 0);
        for (blocks, positions) in parts(&logs, &read) {
            let found = tokio::task::block_in_place(|| {
                logs[positions]
                    .iter()
                    .map(|log| decode(&mut decoder, chain, log))
                    .filter_map(Result::transpose)
                    .collect::<Result<Vec<_>, _>>()
            })?;
            records += found.len();
            let events = derive(&decoder, &reader, chain, found, derives, streams).await?;
            lock.check()?;
            appended += tables
                .fill(blocks, protocols, rows(chain, &schema, &events)?)
                .await?;
            database.fill(chain, events).await?;
            done.commits += 1;
        }
        done.reads += 1;
        done.logs += logs.len();
        done.decoded += records;
        done.appended += appended;
        info!(%chain, from = read.start(), to = read.end(), logs = logs.len(), decoded = records,
            appended, resume_from = read.end().saturating_add(1), "range backfilled");
    }
    report(chain, &done);
    Ok(done)
}

/// Logs what a backfill did, and from where to run it again if it is not done.
fn report(chain: &ChainId, done: &Backfilled) {
    info!(%chain, from = done.from, to = done.to, reads = done.reads, commits = done.commits,
        logs = done.logs, decoded = done.decoded, appended = done.appended,
        discovered = done.discovered, handed = done.handed, "backfill done");
    if let Some(through) = done.rerun_through {
        warn!(%chain, from = done.to.saturating_add(1), through,
            "the running indexer decodes some handed-over contracts only above `to`: run \
             again from here once the bound reaches `through`");
    }
}

/// Finds the contracts the decoder's factories created in `blocks` of the lake: reads
/// their logs, adding each child they create, then the logs of any child that creates
/// others in turn, until a pass finds no new factory. Returns them in creation order.
async fn discover(
    decoder: &mut Decoder,
    reader: &LogReader,
    blocks: RangeInclusive<u64>,
    streams: usize,
) -> Result<Vec<Contract>, BackfillError> {
    let mut read = HashSet::new();
    let mut found = Vec::new();
    loop {
        let parents: Vec<Address> = decoder
            .parents()
            .into_iter()
            .filter(|&parent| read.insert(parent))
            .collect();
        if parents.is_empty() || blocks.is_empty() {
            break;
        }
        for range in reader.reads(*blocks.start(), *blocks.end()) {
            let logs = reader.read(range, &parents, streams).await?;
            tokio::task::block_in_place(|| {
                for log in &logs {
                    let Some((record, claims)) = decoder.decode(log) else {
                        continue;
                    };
                    if let Err(error) = record
                        && error.is_invariant()
                    {
                        return Err(BackfillError::from(error));
                    }
                    found.extend(claims.contracts().cloned());
                    decoder.admit(claims);
                }
                Ok(())
            })?;
        }
        info!(
            factories = parents.len(),
            found = found.len(),
            from = blocks.start(),
            to = blocks.end(),
            "discovered contracts below the restart"
        );
    }
    found.sort_by_key(|contract| (contract.block_number, contract.log_index));
    Ok(found)
}

/// Adds `handed`, each with the first block its record names, to `decoder`, for one an
/// earlier run handed over that this run's discovery did not find, and to the
/// `addresses` the fill reads. Returns the highest block below where the running
/// indexer decodes any of them from, its record or `restart` if later, when it is above
/// `to`.
fn adopt(
    decoder: &mut Decoder,
    addresses: &mut Vec<Address>,
    handed: &[(Contract, u64)],
    restart: Option<u64>,
    to: u64,
) -> Option<u64> {
    let mut decoded_from = None;
    for (contract, first) in handed {
        decoder.adopt(contract);
        addresses.push(contract.address);
        decoded_from =
            decoded_from.max(Some(restart.map_or(*first, |restart| restart.max(*first))));
    }
    addresses.sort_unstable();
    addresses.dedup();
    decoded_from
        .map(|first| first.saturating_sub(1))
        .filter(|&through| through > to)
}

/// `contracts`, narrowed to those `only` names when it names any.
///
/// # Errors
///
/// Returns [`BackfillError::NotDiscovered`] for an address `only` names that is not
/// among them.
fn select(
    only: Option<&[Address]>,
    contracts: Vec<Contract>,
) -> Result<Vec<Contract>, BackfillError> {
    let Some(only) = only else {
        return Ok(contracts);
    };
    let mut by_address: BTreeMap<Address, Contract> = contracts
        .into_iter()
        .map(|contract| (contract.address, contract))
        .collect();
    only.iter()
        .map(|&address| {
            by_address
                .remove(&address)
                .ok_or(BackfillError::NotDiscovered { address })
        })
        .collect()
}

/// Hands the running indexer `found`, with what earlier runs handed over of `protocols`,
/// narrowed to `only`, through the lake's queue, and waits until it records each.
/// Returns each with the first block it decodes it in.
async fn handover(
    settings: &BackfillSettings,
    schema: &Schema,
    chain: &ChainId,
    protocols: &[String],
    found: Vec<Contract>,
    width: u64,
    lock: &RunLock,
) -> Result<Vec<(Contract, u64)>, BackfillError> {
    let lake = &settings.lake;
    let mut queue =
        HandoffQueue::open(&lake.uri, &lake.storage, schema, chain.as_str(), width).await?;
    let mut candidates: BTreeMap<Address, Contract> = queue
        .read()
        .await?
        .into_iter()
        .filter(|contract| protocols.contains(&contract.protocol))
        .map(|contract| (contract.address, contract))
        .collect();
    for contract in found {
        candidates.entry(contract.address).or_insert(contract);
    }
    let handed = select(settings.only.as_deref(), candidates.into_values().collect())?;
    lock.check()?;
    let queued = queue.append(schema, chain, &handed).await?;
    info!(%chain, handed = handed.len(), queued, "contracts handed to the running indexer");
    let started = tokio::time::Instant::now();
    loop {
        let recorded =
            crate::sink::delta::recorded(&lake.uri, &lake.storage, schema, chain.as_str()).await?;
        let mut first: HashMap<Address, u64> = HashMap::new();
        for handoff in recorded {
            first
                .entry(handoff.address)
                .and_modify(|block| *block = (*block).min(handoff.block_number))
                .or_insert(handoff.block_number);
        }
        let missing: Vec<Address> = handed
            .iter()
            .map(|contract| contract.address)
            .filter(|address| !first.contains_key(address))
            .collect();
        let Some(&missed) = missing.first() else {
            return Ok(handed
                .into_iter()
                .map(|contract| {
                    let block = first[&contract.address];
                    (contract, block)
                })
                .collect());
        };
        if started.elapsed() >= HANDOFF_WAIT {
            return Err(BackfillError::NotHandedOver {
                handed: handed.len(),
                recorded: handed.len() - missing.len(),
                first: missed,
                seconds: HANDOFF_WAIT.as_secs(),
            });
        }
        info!(%chain, waiting = missing.len(), "waiting for the running indexer to record the handed-over contracts");
        lock.check()?;
        tokio::time::sleep(HANDOFF_POLL).await;
    }
}

/// Refuses a backfill through `to` unless `store`'s newest `session` decodes each of
/// `protocols` with the event definitions of `wanted`, the backfill's, since a block at
/// or below `to`. Then the running indexer decodes every block above `to`, and the
/// backfill every one at or below it, with the same keys.
fn covers(
    store: &'static str,
    session: Option<&Session>,
    wanted: &[SessionEvent],
    protocols: &[String],
    to: u64,
) -> Result<(), BackfillError> {
    let session = session.ok_or(BackfillError::NoSession { store })?;
    for protocol in protocols {
        let events = |events: &[SessionEvent]| -> BTreeSet<(String, B256)> {
            events
                .iter()
                .filter(|event| &event.protocol == protocol)
                .map(|event| (event.contract.clone(), event.event_id))
                .collect()
        };
        let running = events(&session.events);
        if running.is_empty() {
            return Err(BackfillError::NotDecoding {
                store,
                protocol: protocol.clone(),
                from: session.block_number,
            });
        }
        if running != events(wanted) {
            return Err(BackfillError::EventsDiffer {
                store,
                protocol: protocol.clone(),
            });
        }
    }
    if session.block_number > to {
        return Err(BackfillError::SessionAboveBound {
            store,
            from: session.block_number,
            bound: to,
        });
    }
    Ok(())
}

/// The running indexer's `PostgreSQL` store, when the settings name one: filled beside
/// it with the decoded records, upserted, which takes no lock since the backfill writes
/// only rows below the bound, which it never deletes.
struct Database {
    #[cfg(feature = "postgres")]
    store: Option<crate::sink::PostgresSink>,
}

impl Database {
    /// Opens the store the settings name, if any, with the tables its `tables` route of
    /// `schema`, and checks its session as the lake's is.
    #[cfg_attr(
        not(feature = "postgres"),
        expect(
            clippy::unused_async,
            reason = "the same signature as the `postgres` build's, which connects"
        )
    )]
    async fn open(
        settings: &BackfillSettings,
        schema: &Schema,
        chain: &ChainId,
        wanted: &[SessionEvent],
        protocols: &[String],
        to: u64,
    ) -> Result<Self, BackfillError> {
        #[cfg(feature = "postgres")]
        {
            let Some(postgres) = &settings.postgres else {
                return Ok(Self { store: None });
            };
            let routed = postgres
                .tables
                .resolve(schema)
                .map_err(|source| BackfillError::Route { source })?;
            // A table kept to the pools matched to Metric's pairs is the running indexer's
            // to fill, which knows the pools (see `crate::decode::pairs`).
            let whole: HashSet<_> = routed.tables.difference(&routed.matched).copied().collect();
            let mut store = crate::sink::PostgresSink::open(
                postgres,
                std::sync::Arc::new(schema.select(&whole)),
                chain.as_str(),
                crate::sink::postgres::Writer::Beside,
            )
            .await?;
            covers(
                "database",
                store.session().await?.as_ref(),
                wanted,
                protocols,
                to,
            )?;
            Ok(Self { store: Some(store) })
        }
        #[cfg(not(feature = "postgres"))]
        {
            let _ = (settings, schema, chain, wanted, protocols, to);
            Ok(Self {})
        }
    }

    /// Upserts `events`, decoded and derived records, into the tables the store routes, in
    /// one transaction.
    #[cfg_attr(
        not(feature = "postgres"),
        expect(
            clippy::unused_async,
            reason = "the same signature as the `postgres` build's, which writes"
        )
    )]
    async fn fill(&mut self, chain: &ChainId, events: Vec<Event>) -> Result<(), BackfillError> {
        #[cfg(feature = "postgres")]
        if let Some(store) = &mut self.store {
            use crate::sink::EnvelopeSink as _;
            for event in events {
                store
                    .publish(crate::wire::envelope::Envelope::new(chain.clone(), event))
                    .await
                    .map_err(|source| BackfillError::Database { source })?;
            }
            store
                .flush()
                .await
                .map_err(|source| BackfillError::Database { source })?;
        }
        #[cfg(not(feature = "postgres"))]
        let _ = (chain, events);
        Ok(())
    }
}

/// How many threads the machine runs at once.
fn cores() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// The commits `logs`, sorted, of `read` are written in, each with the positions of its
/// logs: together they cover the read's blocks from the first log on. A commit never
/// crosses a multiple of [`COMMIT_BLOCKS`], nor splits a block, and holds at most
/// [`COMMIT_LOGS`] logs unless one block alone holds more.
fn parts(logs: &[Log], read: &RangeInclusive<u64>) -> Vec<(RangeInclusive<u64>, Range<usize>)> {
    let window = |height: u64| height / COMMIT_BLOCKS;
    let mut positions = Vec::new();
    let mut begin = 0;
    for end in 1..=logs.len() {
        let ends_here = logs.get(end).is_none_or(|log| {
            let last = &logs[end - 1];
            window(last.block_number) != window(log.block_number)
                || (end - begin >= COMMIT_LOGS && last.block_number != log.block_number)
        });
        if ends_here {
            positions.push(begin..end);
            begin = end;
        }
    }
    let mut parts = Vec::with_capacity(positions.len());
    for (index, logs_in) in positions.iter().enumerate() {
        let height = logs[logs_in.start].block_number;
        let start = match index.checked_sub(1).map(|previous| &positions[previous]) {
            Some(previous) if window(logs[previous.start].block_number) == window(height) => height,
            _ => (height - height % COMMIT_BLOCKS).max(*read.start()),
        };
        let end = match positions.get(index + 1) {
            Some(next) if window(logs[next.start].block_number) == window(height) => {
                logs[next.start].block_number - 1
            }
            _ => (height - height % COMMIT_BLOCKS)
                .saturating_add(COMMIT_BLOCKS - 1)
                .min(*read.end()),
        };
        parts.push((start..=end, logs_in.clone()));
    }
    parts
}

/// The rows `events` write: each decoded record's event-table and `decoded_logs` rows,
/// and each derived record's in its derived table.
fn rows(chain: &ChainId, schema: &Schema, events: &[Event]) -> Result<Vec<Row>, BackfillError> {
    let mut rows = Vec::with_capacity(2 * events.len());
    for event in events {
        if let Event::Decoded(record) = event {
            rows.push(schema.event_row(chain, record)?.ok_or_else(|| {
                BackfillError::UnknownEvent {
                    protocol: record.protocol.clone(),
                    contract: record.contract.clone(),
                    event: record.name.clone(),
                }
            })?);
        }
        rows.push(schema.row(chain, event)?);
    }
    Ok(rows)
}

/// `found`, the records decoded from one commit's logs, as the events the running
/// indexer publishes after their logs: each record, then the rows its derivations compute
/// from it. When the manifests `derive`, each row carries its transaction's sender and
/// recipient, read from the lake's `transactions` for just these records; a transaction
/// the lake does not hold leaves them empty, as a log whose transaction was not fetched
/// does.
async fn derive(
    decoder: &Decoder,
    reader: &LogReader,
    chain: &ChainId,
    found: Vec<(Decoded, Option<DecodedAs>)>,
    derives: bool,
    streams: usize,
) -> Result<Vec<Event>, BackfillError> {
    let bound = || found.iter().filter(|(_, event)| event.is_some());
    let first = bound().map(|(record, _)| record.block_number).min();
    let last = bound().map(|(record, _)| record.block_number).max();
    let senders = match (derives, first.zip(last)) {
        // Only the blocks the records are in, which a busy contract's commit spans and a
        // quiet one's does not.
        (true, Some((first, last))) => {
            let hashes: BTreeSet<B256> =
                bound().map(|(record, _)| record.transaction_hash).collect();
            let hashes: Vec<B256> = hashes.into_iter().collect();
            reader.senders(&(first..=last), &hashes, streams).await?
        }
        _ => HashMap::new(),
    };
    let mut events = Vec::with_capacity(found.len());
    let mut unsent = 0_usize;
    for (record, event) in found {
        let rows = match event.filter(|_| derives) {
            Some(event) => {
                let sent = senders
                    .get(&record.transaction_hash)
                    .map(|(from, to, input)| Sent {
                        hash: record.transaction_hash,
                        from: *from,
                        to: *to,
                        input: input.clone(),
                    });
                let rows = decoder.derive(chain, event, &record, sent.as_ref())?;
                if sent.is_none() && !rows.is_empty() {
                    unsent += 1;
                }
                rows
            }
            None => Vec::new(),
        };
        events.push(Event::Decoded(Box::new(record)));
        events.extend(rows.into_iter().map(|row| Event::Derived(Box::new(row))));
    }
    if unsent > 0 {
        warn!(%chain, from = first, to = last, records = unsent,
            "transactions the lake does not hold; their derived rows carry no sender");
    }
    Ok(events)
}

/// `log`'s record, when it is a known contract's known event, with the event it decoded
/// as. A log that does not decode is logged and skipped, as the running indexer skips
/// it; a decoder that broke an invariant stops the backfill.
fn decode(
    decoder: &mut Decoder,
    chain: &ChainId,
    log: &Log,
) -> Result<Option<(Decoded, Option<DecodedAs>)>, BackfillError> {
    let Some((record, claims)) = decoder.decode(log) else {
        return Ok(None);
    };
    // A contract the range creates decodes from its creation on, as in the running
    // indexer.
    decoder.admit(claims);
    match record {
        Ok(Record { decoded, event }) => Ok(Some((decoded, Some(event)))),
        Err(error) if error.is_invariant() => Err(error.into()),
        Err(error) => {
            warn!(%chain, address = %log.address, block = log.block_number,
                transaction = %log.transaction_hash, log_index = log.log_index,
                selector = ?log.topic0, ?error, "log decode failed; skipped");
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests;
