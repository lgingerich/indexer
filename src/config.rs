//! The indexer's settings file, and the builders each stage takes.
//!
//! TOML, read once at startup. Three conventions carry the contract:
//!
//! - **Required means no value could be right by accident.** A field with no
//!   `#[serde(default)]` has no default, so serde refuses a document that omits it and
//!   names the field. Required `String`s also carry `deserialize_with = "non_empty"`,
//!   since `chain = ""` is present, not absent.
//! - **A typo is a startup error, not a silent default.** Every table is
//!   `deny_unknown_fields`, and an unknown sink backend is refused rather than falling
//!   back to another one.
//! - **A secret is named, not written.** A [`Secret`] field takes `{ env = "NAME" }`, read
//!   from the environment as the file loads, so the file stays in git while the platform
//!   injects the value. A literal is accepted too, for a public endpoint or local
//!   development. The environment is the parse's input (an [`Env`]), so a test parses a
//!   deployment's file with stand-in values.
//!
//! Where records end up. The backend is the table: `[sink.duckdb]` opens `DuckDB` and
//! holds its settings, `[sink.delta]` the lake, and so on — any combination, one of
//! each, every one listing the `tables` it writes (see [`Sinks`]). `[sink.stdout]` prints
//! instead, and is the only sink when named. Every other field not shown below may be
//! omitted.
//!
//! Named for [`crate::sink`] rather than for storage: a sink is where envelopes go, and
//! not all of them are kept. `stdout` is a sink that writes nowhere, and a webhook or a
//! Kafka topic would be one too.
//!
//! ```toml
//! [ingest]
//! chain = "base"
//! http_url = "https://base-rpc.publicnode.com"   # or { env = "INDEXER_HTTP_URL" }
//! ws_url = "wss://base-rpc.publicnode.com"
//! # ...or, instead of the two endpoints, backfill from `HyperSync` and stop:
//! # [ingest.hypersync]
//! # url = "https://8453.hypersync.xyz"
//! # token = { env = "INDEXER_ENVIO_HYPERSYNC_TOKEN" }
//!
//! [sink.duckdb]
//! tables = ["blocks", "transactions", "logs", "decoded_logs", "contract_events"]
//! path = "indexer.duckdb"
//!
//! [sink.duckdb.settings]
//! threads = "4"
//!
//! # ...beside `[sink.postgres]` or `[sink.delta]`, or instead `[sink.stdout]`, to
//! # print the stream and open no store.
//!
//! [decode]
//! protocols = "protocols"      # the protocol manifests; optional
//!
//! [metrics]
//! listen = "0.0.0.0:9100"      # the Prometheus endpoint; optional
//! ```

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde::de::{self, Unexpected};

use crate::metrics::MetricsSettings;
use crate::secret::{self, Env, Secret};
use crate::sink::route::{Backend, Tables};

/// Deserializes a required string, refusing an empty or whitespace-only one.
///
/// A `#[serde(default)]`-less field already makes absence an error, but a present empty
/// value is not absence: `http_url = ""` is a deployment that starts and connects to
/// nothing, and `chain = ""` stamps every event with a blank chain. Putting the check on
/// the field keeps it with the field's documentation and lets serde name it, rather than
/// a hand-written pass over every required key after the fact.
pub(crate) fn non_empty<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value.trim().is_empty() {
        return Err(de::Error::invalid_value(
            Unexpected::Str(&value),
            &"a non-empty value",
        ));
    }
    Ok(value)
}

/// The whole settings file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    /// The chain to follow. Required: the pipeline starts at ingest.
    pub ingest: IngestSettings,
    /// Where records go. Required: a run with nowhere to send them is not a deployment.
    pub sink: Sinks,
    /// What the decode stage decodes.
    #[serde(default)]
    pub decode: DecodeSettings,
    /// Where the Prometheus endpoint listens. Absent serves and records nothing; see
    /// [`crate::metrics`].
    #[serde(default)]
    pub metrics: Option<MetricsSettings>,
    /// What `indexer backfill` backfills from the lake. The running indexer ignores it.
    #[serde(default)]
    pub backfill: Option<BackfillRun>,
    /// The directory the settings file lives in, which a relative path in it resolves
    /// against. Filled by [`Settings::from_file`]; empty for text given to
    /// [`Settings::parse`], which leaves a relative path as written. Not a file key, so a `dir` in the file is
    /// still reported as a typo.
    #[serde(skip)]
    dir: PathBuf,
}

/// How ingest follows a chain.
///
/// `chain` is required. So is one source, and only one: `http_url` and `ws_url` together,
/// a node's JSON-RPC that follows the chain, or `[ingest.hypersync]`, which backfills the
/// buried heights and stops (see [`HyperSyncSettings`]). A source cannot default — a
/// default would silently index nothing — so naming neither, half of the pair, or both
/// is a parse error, as is a key that only the other source reads. Each key's meaning is
/// documented where it lands: here, or on [`IngestSource`].
#[derive(Debug, Deserialize)]
#[serde(try_from = "IngestTable")]
pub struct IngestSettings {
    /// The chain id stamped on every event. Required, and non-empty.
    pub chain: String,
    /// The one source the table names.
    ///
    /// What ingest fetches from it is not set here: it is what the sinks' `tables` need
    /// (see [`crate::sink::route::datasets`]).
    pub source: IngestSource,
    /// The first height to index on an empty store. Omitted starts at the observed head,
    /// which skips earlier history; a value indexes that height and every one after it
    /// before following live heads. A value above the sampled head is a startup error
    /// rather than a silent clamp. A store that already holds accepted blocks resumes
    /// after them instead: a value at or below the block after the stored tip is
    /// ignored, and one above it is a startup error, since it would leave a gap.
    /// A `HyperSync` backfill on an empty store needs it, since a backfill that starts at
    /// the head has nothing to do; one that only prints to `[sink.stdout]` always starts
    /// empty, so omitting it there is refused as the file loads.
    pub start_block: Option<u64>,
    /// The last height to index. Omitted follows live heads without end on `http_url`
    /// and `ws_url`, and on `HyperSync` stops at the buried heights under the head it
    /// samples first; a value indexes through that height, no further than those on
    /// `HyperSync`, and exits instead of following the chain. A value below
    /// `start_block` is refused as the file loads; one above the sampled head, or below
    /// the head where a run without a start begins, is a startup error. A store that
    /// already holds it has nothing to do, and the run exits without indexing.
    ///
    /// A stop inside the reorg window under the head is indexed as the chain stood then:
    /// a fork that later replaces it is not retracted, since no run follows it.
    pub stop_block: Option<u64>,
    /// `finality_depth`: how many blocks under the head a fork may still reach on this
    /// chain. Heights deeper than it are backfilled straight through, a range at a time,
    /// and a `HyperSync` backfill stops there; only the blocks above it go one at a time
    /// through the reorg-aware path, so a shallower depth hands a backfill to the chain's
    /// head sooner. It is not how deep a fork can be retracted: a run retracts up to
    /// [`undo_window`](Self::undo_window) blocks whatever this is. Omitted is that same
    /// depth; zero, or deeper than the window, is refused.
    pub finality_depth: NonZeroU64,
    /// `undo_window`: how deep a fork a run can retract, and so how many blocks each
    /// store's ledger keeps and a restart reads back (see
    /// [`UndoWindow`](crate::window::UndoWindow)). A deeper fork stops the run. Omitted
    /// is 4,096, which suits any chain and is the most allowed.
    pub undo_window: crate::window::UndoWindow,
}

/// The source `[ingest]` names.
#[derive(Debug)]
pub enum IngestSource {
    /// A node's JSON-RPC for blocks, and its WebSocket for heads.
    Rpc {
        /// `http_url`: the JSON-RPC endpoint used for blocks, receipts, and logs. A
        /// [`Secret`], since a provider's URL usually carries its API key.
        http_url: Secret,
        /// `ws_url`: the WebSocket endpoint used for heads. A [`Secret`], like
        /// `http_url`.
        ws_url: Secret,
        /// `backfill_concurrency`: how many backfill fetches are in flight at once.
        /// Blocks are still accepted one at a time, in height order; this only overlaps
        /// the requests, so the node is read while the stores write. Zero is refused, and
        /// omitted is four. Each fetch counts against the provider's rate limit, so a
        /// provider that throttles hard wants this low.
        backfill_concurrency: NonZeroUsize,
    },
    /// `[ingest.hypersync]`: Envio `HyperSync`, for a backfill that stops at the reorg
    /// window. It fetches one range at a time, since each range is far larger than an
    /// RPC node's, so it takes no `backfill_concurrency`.
    HyperSync(HyperSyncSettings),
}

/// `[ingest]` as written, which [`IngestSettings`] is resolved from.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IngestTable {
    #[serde(deserialize_with = "non_empty")]
    chain: String,
    #[serde(default)]
    http_url: Option<Secret>,
    #[serde(default)]
    ws_url: Option<Secret>,
    #[serde(default)]
    hypersync: Option<HyperSyncSettings>,
    #[serde(default)]
    start_block: Option<u64>,
    #[serde(default)]
    stop_block: Option<u64>,
    #[serde(default)]
    backfill_concurrency: Option<NonZeroUsize>,
    #[serde(default)]
    finality_depth: Option<NonZeroU64>,
    #[serde(default)]
    undo_window: Option<usize>,
}

/// Four: enough to keep the node busy while a store commits, few enough that a
/// provider's per-second limit is not the first thing it finds.
const fn default_backfill_concurrency() -> NonZeroUsize {
    NonZeroUsize::MIN.saturating_add(3)
}

impl TryFrom<IngestTable> for IngestSettings {
    type Error = &'static str;

    /// Resolves the one source the table names, refusing neither, half of the RPC pair,
    /// both, or `backfill_concurrency` beside `HyperSync`.
    fn try_from(table: IngestTable) -> Result<Self, Self::Error> {
        let source = match (table.http_url, table.ws_url, table.hypersync) {
            (Some(http_url), Some(ws_url), None) => IngestSource::Rpc {
                http_url,
                ws_url,
                backfill_concurrency: table
                    .backfill_concurrency
                    .unwrap_or_else(default_backfill_concurrency),
            },
            (None, None, Some(hypersync)) => {
                if table.backfill_concurrency.is_some() {
                    return Err("[ingest] backfill_concurrency is for http_url and ws_url; \
                         [ingest.hypersync] takes its own concurrency");
                }
                IngestSource::HyperSync(hypersync)
            }
            (None, None, None) => {
                return Err("[ingest] name http_url and ws_url, or [ingest.hypersync]");
            }
            (_, _, Some(_)) => {
                return Err(
                    "[ingest.hypersync] replaces http_url and ws_url; name one or the other",
                );
            }
            _ => return Err("[ingest] http_url and ws_url are named together"),
        };
        let undo_window = match table.undo_window {
            Some(blocks) => crate::window::UndoWindow::new(blocks)
                .ok_or("[ingest] undo_window is at least 1 and at most 4,096")?,
            None => crate::window::UndoWindow::DEFAULT,
        };
        let deepest = undo_window.blocks() as u64;
        let finality_depth = match table.finality_depth {
            Some(depth) if depth.get() > deepest => {
                return Err("[ingest] finality_depth is at most undo_window, \
                     the deepest fork a run retracts");
            }
            Some(depth) => depth,
            None => NonZeroU64::new(deepest).unwrap_or(NonZeroU64::MIN),
        };
        Ok(Self {
            chain: table.chain,
            source,
            start_block: table.start_block,
            stop_block: table.stop_block,
            finality_depth,
            undo_window,
        })
    }
}

/// A backfill from [Envio HyperSync](https://docs.envio.dev/docs/`HyperSync`/overview).
///
/// `HyperSync` serves a chain's history thousands of blocks per request, but has no live
/// heads. A run on it indexes the buried heights — everything below the reorg window
/// under the head — and exits; following the chain is a restart with `http_url` and
/// `ws_url` in place of this table, which resumes after what the backfill committed.
///
/// ```toml
/// [ingest.hypersync]
/// url = "https://4663.hypersync.xyz"
/// token = { env = "INDEXER_ENVIO_HYPERSYNC_TOKEN" }
/// span = 2000          # heights per range; optional
/// concurrency = 3      # ranges in flight; optional
/// ```
///
/// Backfill fetches ranges of `span` heights, `concurrency` of them at once, and accepts
/// them in height order, so the next ranges download while one is accepted. Each range in
/// flight, or answered and waiting for the one below it, is held whole in memory: about
/// `span` × `concurrency` blocks with every dataset, so a chain with heavy blocks wants a
/// smaller span.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperSyncSettings {
    /// The chain's `HyperSync` endpoint, such as `https://4663.hypersync.xyz`. Required.
    #[serde(deserialize_with = "non_empty")]
    pub url: String,
    /// The Envio API token. Required; a [`Secret`].
    pub token: Secret,
    /// How many heights one backfill range covers. Zero is refused; omitted is 2,000.
    #[serde(default = "default_hypersync_span")]
    pub span: NonZeroU64,
    /// How many ranges backfill keeps in flight. Zero is refused; omitted is 2.
    #[serde(default = "default_hypersync_concurrency")]
    pub concurrency: NonZeroUsize,
}

/// 2,000 heights: on BSC, about a gigabyte held per range.
const fn default_hypersync_span() -> NonZeroU64 {
    match NonZeroU64::new(2_000) {
        Some(span) => span,
        None => NonZeroU64::MIN,
    }
}

/// Two ranges: one is fetched while the other is accepted.
const fn default_hypersync_concurrency() -> NonZeroUsize {
    NonZeroUsize::MIN.saturating_add(1)
}

/// Where records go: one table per backend, each holding that backend's settings and the
/// [`Tables`] it writes.
///
/// The backend *is* the table, which is what makes backend-specific keys unambiguous:
/// `[sink.duckdb.settings]` is inside the table that named `DuckDB`. A run writes any
/// combination of stores, at most one of each, and every store says which tables it
/// writes — there is no default, so what lands where is read off the file. What ingest
/// fetches follows from the union (see [`crate::sink::route`]).
///
/// ```toml
/// [sink.delta]
/// tables = ["blocks", "transactions", "logs", "decoded_logs"]
/// uri = "s3://indexer-lake"
///
/// [sink.postgres]
/// tables = ["contract_events"]
/// connection_string = { env = "INDEXER_POSTGRES_URL" }
/// ```
///
/// Naming none is an error, and so is `[sink.stdout]` beside a store: printing keeps no
/// ledger, so it cannot resume with the stores (see `crate::sink::resume`). A store
/// backend is behind its feature, so a build without the engine does not carry a store
/// it cannot open: there, naming `[sink.duckdb]` is an unknown key — a startup error
/// naming the table, rather than settings that parse and fail later.
///
/// A sink is not necessarily a store. `stdout` keeps nothing, and that is the point of
/// the wider name: the layer is where envelopes go.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sinks {
    /// Print the stream as newline-delimited JSON, for watching what the indexer would
    /// write. No store is opened and nothing is persisted.
    #[serde(default)]
    pub stdout: Option<crate::sink::stdout::StdoutSettings>,
    /// An embedded `DuckDB` database, opened and committed by its own task.
    #[cfg(feature = "duckdb")]
    #[serde(default)]
    pub duckdb: Option<crate::sink::duckdb::DuckDbSettings>,
    /// A remote `PostgreSQL` database, using asynchronous transactional COPY.
    #[cfg(feature = "postgres")]
    #[serde(default)]
    pub postgres: Option<crate::sink::postgres::PostgresSettings>,
    /// Delta Lake tables on S3 or an S3-compatible store: the cheap raw-data lake.
    #[cfg(feature = "delta")]
    #[serde(default)]
    pub delta: Option<crate::sink::delta::DeltaSettings>,
}

impl Sinks {
    /// Each named sink and the tables it writes, in a fixed order.
    #[must_use]
    pub fn tables(&self) -> Vec<(Backend, &Tables)> {
        let mut named = Vec::new();
        if let Some(stdout) = &self.stdout {
            named.push((Backend::Stdout, &stdout.tables));
        }
        #[cfg(feature = "duckdb")]
        if let Some(duckdb) = &self.duckdb {
            named.push((Backend::DuckDb, &duckdb.tables));
        }
        #[cfg(feature = "postgres")]
        if let Some(postgres) = &self.postgres {
            named.push((Backend::Postgres, &postgres.tables));
        }
        #[cfg(feature = "delta")]
        if let Some(delta) = &self.delta {
            named.push((Backend::Delta, &delta.tables));
        }
        named
    }

    /// Where the stream goes: printed, with the tables `[sink.stdout]` lists, or into
    /// every store named, each with its own settings. The one place the two are told
    /// apart, so nothing past it that opens a store can be handed stdout.
    ///
    /// # Errors
    ///
    /// Returns [`SettingsError::NoSink`] when no sink is named, and
    /// [`SettingsError::StdoutBesideStore`] when `stdout` is named beside a store.
    pub fn destination(&self) -> Result<Destination, SettingsError> {
        #[cfg(feature = "store")]
        {
            let stores = self.stores();
            match (&self.stdout, stores.is_empty()) {
                (Some(_), false) => return Err(SettingsError::StdoutBesideStore),
                (None, false) => return Ok(Destination::Store(stores)),
                (_, true) => {}
            }
        }
        self.stdout
            .as_ref()
            .map(|stdout| Destination::Print(stdout.tables.clone()))
            .ok_or(SettingsError::NoSink)
    }

    /// Every store named, in the fixed order of [`tables`](Self::tables).
    #[cfg(feature = "store")]
    fn stores(&self) -> Vec<StoreSettings> {
        let mut stores = Vec::new();
        #[cfg(feature = "duckdb")]
        stores.extend(self.duckdb.clone().map(StoreSettings::DuckDb));
        #[cfg(feature = "postgres")]
        stores.extend(self.postgres.clone().map(StoreSettings::Postgres));
        #[cfg(feature = "delta")]
        stores.extend(self.delta.clone().map(StoreSettings::Delta));
        stores
    }
}

/// Where the stream goes, as [`Sinks::destination`] resolves it: a run prints or stores,
/// never both.
#[derive(Debug)]
pub enum Destination {
    /// `[sink.stdout]`: print these tables, and open no store.
    Print(Tables),
    /// Every store named, in a fixed order. Never empty.
    #[cfg(feature = "store")]
    Store(Vec<StoreSettings>),
}

/// One store's settings, of whichever backend its `[sink.<backend>]` names.
#[cfg(feature = "store")]
#[derive(Debug, Clone)]
pub enum StoreSettings {
    /// `[sink.duckdb]`.
    #[cfg(feature = "duckdb")]
    DuckDb(crate::sink::duckdb::DuckDbSettings),
    /// `[sink.postgres]`.
    #[cfg(feature = "postgres")]
    Postgres(crate::sink::postgres::PostgresSettings),
    /// `[sink.delta]`.
    #[cfg(feature = "delta")]
    Delta(crate::sink::delta::DeltaSettings),
}

#[cfg(feature = "store")]
impl StoreSettings {
    /// The store's backend.
    #[must_use]
    pub const fn backend(&self) -> crate::sink::route::StoreBackend {
        use crate::sink::route::StoreBackend;
        match self {
            #[cfg(feature = "duckdb")]
            Self::DuckDb(_) => StoreBackend::DuckDb,
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => StoreBackend::Postgres,
            #[cfg(feature = "delta")]
            Self::Delta(_) => StoreBackend::Delta,
        }
    }

    /// The tables the store writes.
    #[must_use]
    pub const fn tables(&self) -> &Tables {
        match self {
            #[cfg(feature = "duckdb")]
            Self::DuckDb(duckdb) => &duckdb.tables,
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => &postgres.tables,
            #[cfg(feature = "delta")]
            Self::Delta(delta) => &delta.tables,
        }
    }

    /// The lake's settings, when this is `[sink.delta]`.
    #[cfg(feature = "delta")]
    #[cfg_attr(
        not(any(feature = "duckdb", feature = "postgres")),
        expect(
            clippy::unnecessary_wraps,
            reason = "a build with other stores has settings that are not the lake's"
        )
    )]
    pub(crate) const fn delta(&self) -> Option<&crate::sink::delta::DeltaSettings> {
        match self {
            #[cfg(feature = "duckdb")]
            Self::DuckDb(_) => None,
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => None,
            Self::Delta(delta) => Some(delta),
        }
    }

    /// When the store's drain commits, as its settings say.
    pub(crate) fn batching(&self) -> crate::sink::channel::Batching {
        match self {
            #[cfg(feature = "duckdb")]
            Self::DuckDb(duckdb) => duckdb.batching(),
            #[cfg(feature = "postgres")]
            Self::Postgres(postgres) => postgres.batching(),
            #[cfg(feature = "delta")]
            Self::Delta(delta) => delta.batching(),
        }
    }
}

/// What the decode stage decodes: a directory of protocol manifests.
///
/// The manifests are kept out of the indexer's settings because they are a different kind
/// of thing: the settings file is deployment topology — endpoints, paths — and the
/// manifests are a catalog of protocols that grows on its own schedule.
///
/// A table, not an `Option`, because decode always runs: an absent `protocols` means an
/// empty catalog and nothing is decoded, which is a legitimate way to run and is said at
/// startup rather than being silent. See [`crate::decode::Catalog`] for the manifest shape.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DecodeSettings {
    /// The protocols directory, relative to the settings file's directory. Absent means
    /// an empty catalog.
    pub protocols: Option<PathBuf>,
}

/// `[backfill]`: what `indexer backfill <this file>` decodes from the logs the lake of
/// `[sink.delta]` holds, writing to that lake and to the `[sink.postgres]` database (see
/// `crate::backfill`).
///
/// ```toml
/// [backfill]
/// protocols = "protocols/uniswap/v4"   # the manifests to backfill
/// # from = 25984373                    # the lake's first block when absent
/// # to = 26200000                      # the bound when absent
/// # only = ["0x…"]                     # discovered contracts to hand over; every one when absent
/// # out = "/tmp/indexer-backfill"      # a trial: written instead, fills no database
///
/// # [backfill.hypersync]               # finds what factories created before the lake's first block
/// # url = "https://4663.hypersync.xyz"
/// # token = { env = "INDEXER_ENVIO_HYPERSYNC_TOKEN" }
/// ```
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackfillRun {
    /// The manifests to backfill, a subtree of `[decode] protocols`, relative to the
    /// settings file.
    pub protocols: PathBuf,
    /// The first block to backfill; the lake's first when absent.
    #[serde(default)]
    pub from: Option<u64>,
    /// The last block to backfill; the bound no delete of the running store reaches when
    /// absent, and never above it.
    #[serde(default)]
    pub to: Option<u64>,
    /// The discovered contracts to hand over and backfill, when only some are wanted.
    /// Every one when absent.
    #[serde(default)]
    pub only: Option<Vec<alloy_primitives::Address>>,
    /// Another lake to write to, such as a local directory, instead of the chain's own,
    /// which is then only read: a trial, which fills no database. Reached with no storage
    /// options.
    #[serde(default)]
    pub out: Option<String>,
    /// `[backfill.hypersync]`: the chain's `HyperSync` endpoint, searched for the logs
    /// the factories emitted before the lake's first block, so the contracts they
    /// created then are found too. Absent, only those created in the lake are.
    #[serde(default)]
    pub hypersync: Option<HyperSyncEndpoint>,
}

/// A chain's `HyperSync` endpoint and the token it is read with.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperSyncEndpoint {
    /// The endpoint, such as `https://4663.hypersync.xyz`. Required.
    #[serde(deserialize_with = "non_empty")]
    pub url: String,
    /// The Envio API token. Required; a [`Secret`].
    pub token: Secret,
}

/// Why a settings file could not be turned into [`Settings`].
///
/// Typed because the two failures are different problems — an unreadable path and a
/// rejected document — and a caller that recovers from one (falling back to a default
/// file) may not want to swallow the other. Neither variant names the file: `toml` gives
/// the line and column, and the caller is the one that knows the path.
#[derive(Debug, thiserror::Error)]
pub enum SettingsError {
    /// The file could not be read.
    #[error("read settings at {path}: {source}")]
    Read {
        /// The path that failed.
        path: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// The document did not parse, or a required value was absent.
    #[error("invalid settings: {source}")]
    Parse {
        /// `toml`'s error, which carries the line and column.
        source: toml::de::Error,
    },
    /// No `[sink.<backend>]` is named, so there is nowhere to write.
    #[error("invalid settings: name at least one [sink.<backend>]")]
    NoSink,
    /// `[sink.stdout]` is named beside a store.
    #[error("invalid settings: [sink.stdout] cannot be named beside a store")]
    StdoutBesideStore,
    /// `stop_block` is below `start_block`. A fresh run would index nothing, and a
    /// resumed one could only find the stop already stored or the start past its tip.
    #[error("invalid settings: [ingest] stop_block {stop} is below start_block {start}")]
    StopBelowStart {
        /// `stop_block`.
        stop: u64,
        /// `start_block`.
        start: u64,
    },
    /// `[ingest.hypersync]` with no `start_block` and only `[sink.stdout]`. A backfill
    /// without a start begins after the blocks its stores hold, and printing keeps none,
    /// so it would begin at the head, where nothing is buried.
    #[error(
        "invalid settings: [ingest.hypersync] beside only [sink.stdout] needs \
         [ingest] start_block, since printing keeps no blocks to resume after"
    )]
    BackfillWithoutStart,
}

impl Settings {
    /// Parses settings from TOML text, filling defaults, reading each `{ env = "NAME" }`
    /// secret from `env`, and rejecting what is required but absent, an `[ingest]` that
    /// does not name one source, and sinks that cannot run together.
    ///
    /// This is the in-memory form of [`Settings::from_file`], for a caller that has the
    /// text already. A relative path in the text resolves against the process's working
    /// directory rather than a file's, since there is no file; prefer
    /// [`Settings::from_file`].
    ///
    /// # Errors
    ///
    /// Returns [`SettingsError::Parse`] when the text does not parse, a required value
    /// or a named variable is absent, or `[ingest]` does not name one source,
    /// [`SettingsError::NoSink`] or [`SettingsError::StdoutBesideStore`] when its sinks
    /// cannot run, and [`SettingsError::StopBelowStart`] or
    /// [`SettingsError::BackfillWithoutStart`] when its heights
    /// cannot run whatever the chain and the stores hold. A malformed document reports
    /// its line and column; an unknown field is an error rather than being ignored, so a
    /// misspelled key is caught at startup instead of silently leaving a setting at its
    /// default.
    pub fn parse(text: &str, env: Env) -> Result<Self, SettingsError> {
        let mut settings: Self = secret::reading(env, || toml::from_str(text))
            .map_err(|source| SettingsError::Parse { source })?;
        settings.check()?;
        settings.share_undo_window();
        Ok(settings)
    }

    /// Hands each store the chain's undo window, which `[ingest]` names and every store's
    /// ledger is sized by.
    fn share_undo_window(&mut self) {
        let window = self.ingest.undo_window;
        #[cfg(feature = "duckdb")]
        if let Some(duckdb) = &mut self.sink.duckdb {
            duckdb.undo_window = window;
        }
        #[cfg(feature = "postgres")]
        if let Some(postgres) = &mut self.sink.postgres {
            postgres.undo_window = window;
        }
        #[cfg(feature = "delta")]
        if let Some(delta) = &mut self.sink.delta {
            delta.undo_window = window;
        }
        let _ = window;
    }

    /// Refuses settings that can only fail once run, whatever the chain and the stores
    /// hold: no sink, `stdout` beside a store, a `stop_block` below the `start_block`,
    /// and a `HyperSync` backfill with no `start_block` that only prints, which has no
    /// stored blocks to begin after. What depends on the chain's head or the stores'
    /// history is left to the run.
    fn check(&self) -> Result<(), SettingsError> {
        let destination = self.sink.destination()?;
        let IngestSettings {
            source,
            start_block,
            stop_block,
            ..
        } = &self.ingest;
        if let (Some(start), Some(stop)) = (*start_block, *stop_block)
            && stop < start
        {
            return Err(SettingsError::StopBelowStart { stop, start });
        }
        if let (IngestSource::HyperSync(_), None, Destination::Print(_)) =
            (source, start_block, destination)
        {
            return Err(SettingsError::BackfillWithoutStart);
        }
        Ok(())
    }

    /// Reads settings from a TOML file, as [`Settings::parse`] does, and resolves the
    /// relative paths in it against the file's directory.
    ///
    /// # Errors
    ///
    /// Returns [`SettingsError::Read`] when the file cannot be read, and otherwise what
    /// [`Settings::parse`] returns.
    pub fn from_file(path: impl AsRef<Path>, env: Env) -> Result<Self, SettingsError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|source| SettingsError::Read {
            path: path.display().to_string(),
            source,
        })?;
        let mut settings = Self::parse(&text, env)?;
        // Remember where the file lives, so a relative path in it resolves against that
        // directory rather than the process's working directory. A bare filename's
        // parent is empty, and joining onto an empty path is the path as written.
        settings.dir = path.parent().unwrap_or_else(|| Path::new("")).to_path_buf();
        if let Some(backfill) = &mut settings.backfill {
            backfill.protocols = settings.dir.join(&backfill.protocols);
        }
        #[cfg(feature = "duckdb")]
        if let Some(duckdb) = &mut settings.sink.duckdb
            && duckdb.path != Path::new(":memory:")
        {
            duckdb.path = settings.dir.join(&duckdb.path);
        }
        // A lake in a local directory resolves the same way. A URL — `s3://`, `file://` —
        // names its own location and is left as written; it would otherwise read as a
        // relative path, which is what `Path` makes of `s3://bucket`.
        #[cfg(feature = "delta")]
        if let Some(delta) = &mut settings.sink.delta
            && !delta.uri.contains("://")
        {
            delta.uri = settings.dir.join(&delta.uri).display().to_string();
        }
        Ok(settings)
    }

    /// The protocols directory decode loads, resolved against the settings file's
    /// directory.
    ///
    /// `None` when none is named, which is an empty catalog rather than an error. Through
    /// [`Settings::from_file`] the path is absolute-or-relative-to-the-file; text parsed
    /// from a string has no directory, so the path is returned as written.
    #[must_use]
    pub fn protocols_path(&self) -> Option<PathBuf> {
        self.decode
            .protocols
            .as_ref()
            .map(|protocols| self.dir.join(protocols))
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests;
