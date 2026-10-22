//! The pipeline the settings describe, assembled and run.
//!
//! The shape is fixed — `ingest → decode → storage` — so what varies is a few choices the
//! settings file already states. This module reads them and builds the pipeline, so the
//! binary is "load the settings, run it" rather than a place that names every layer.
//!
//! # One ingest task, a runtime per store
//!
//! ```text
//! main:      ingest ─▶ DecodingSink ─▶ Router ─┬─ ChannelSink ══╗ bounded channel of blocks
//!                      (direct calls, no queue) └─ ChannelSink ══╬═══════╗
//! lake:      DeltaSink    ◀─ ChannelReceiver::drain ◀════════════╝       ║
//! postgres:  PostgresSink ◀─ ChannelReceiver::drain ◀════════════════════╝
//!            (each label is a runtime of its own, with its own threads)
//! ```
//!
//! Ingest and decode are one task because decoding a block is far cheaper than the block
//! time and needs no decoupling. Each store drains its own channel because a store
//! stalls, and the channel in front of it is what keeps a stall from stopping ingest;
//! see `crate::sink::channel`. And each store runs on a runtime of its own, with its
//! own threads, because tokio cannot interrupt a task: on one shared runtime, the lake's
//! seconds of Parquet encoding held every worker while a database commit and ingest
//! waited for one. See `crate::sink::store_runtime`. The router hands each store the
//! envelopes of the tables it writes; see [`crate::sink::route`].
//!
//! # What the settings decide
//!
//! - **The sinks** are the `[sink.<backend>]` tables, each with the `tables` it writes.
//!   With `[sink.stdout]` no store is opened at all: the stream is printed instead.
//! - **Ingest** follows the chain `[ingest]` names, fetching what the sinks' tables
//!   need and nothing else, from a node's JSON-RPC — or, with `[ingest.hypersync]`,
//!   backfills from `HyperSync` and stops.
//! - **Decode** uses the protocol manifests `[decode] protocols` names. Leaving it out
//!   decodes nothing, which is a legitimate way to run and is said at startup; a directory
//!   with no manifest in it is a startup error.
//!   Contracts a previous run discovered are read back from the stores before ingest
//!   starts, so a restart decodes them; `[sink.stdout]` has no store and starts with the
//!   manifests' seeds only.
//! - **Where ingest starts.** Every store's ledger of accepted blocks is read back before
//!   ingest starts, every store is brought to the newest block they all hold (see
//!   `crate::sink::resume`), and a run resumes after it; see `crate::ingest::pipeline`.
//!   Empty ledgers start at `ingest.start_block` or the head; a `start_block` the stored
//!   history already reaches is ignored. `[sink.stdout]`, which
//!   has no store, always starts fresh. A run with `ingest.stop_block` ends there.
//!
//! # Shutdown
//!
//! Ingest ends by failing, by reaching `ingest.stop_block`, or, in a backfill-only run,
//! by finishing its backfill. Either way the router's channels close, so every store
//! drains the blocks already queued and stops. If a store fails first, ingest
//! finds out at its next block, when the send to it fails, and stops the rest the same
//! way. Either way a store's error is reported ahead of ingest's, since a dead store is
//! the cause and a failed send the symptom.
//!
//! `SIGTERM` or `SIGINT` — a deploy's `docker stop`, or Ctrl-C — stops the run as
//! asked rather than killing it: ingest stops at once, every store lets the commit it
//! has under way run on, commits what it has gathered if that fits a deadline under the
//! host's stop timeout, and drops the blocks still queued (see `crate::sink::channel`),
//! and the process exits 0. A second signal does nothing more; the supervisor's own
//! timeout is the backstop.
//!
//! A stop is heard in one of three places, each a `select!` biased toward it, and what it
//! abandons differs:
//!
//! - **While the stores open** (`Pipeline::run`): the stores opened so far are dropped.
//!   Opening commits no block, so there is nothing to finish, and a restart opens them
//!   again.
//! - **While the stores are brought to one block** (`Pipeline::run_with_stores`): the
//!   resume is dropped part-way, as a crash would leave it. A rewind it had under way is
//!   a store's write like any other, which a crash survives, so the next start repeats
//!   the resume from wherever the stores stand.
//! - **While ingest runs** (`Pipeline::run_with_stores`, and the stdout run in
//!   `Pipeline::run`): ingest is dropped, which to it is a crash, since what it has not
//!   handed to storage is fetched again from the ledgers. The stores are not dropped:
//!   each hears the same stop in its own drain, finishes as above, and is joined.
//!
//! So only the last one waits on anything, and only for the stores.
//!
//! The indexer never cuts a commit short, but the host does: `docker stop -t 30` kills
//! the process 30 seconds after the signal, and a lake commit of a full buffer while
//! catching up can take about a minute. A stop then ends as a crash does, which every
//! store survives — nothing a commit wrote counts until its ledger entry does — and the
//! next start fetches those blocks again from the ledger's tip. A stop at the tip, where
//! a commit takes seconds, finishes in time.
//!
//! Exiting 0 means a supervisor that restarts only on failure (systemd's
//! `Restart=on-failure`) leaves the run stopped. That is the point for a deploy's stop,
//! but a `SIGTERM` from anything else, such as an operator's `docker stop`, also leaves
//! the chain down until something starts it again.

use std::collections::HashSet;
use std::sync::Arc;

use thiserror::Error;
use tracing::info;

use crate::config::{Destination, IngestSource, Settings, SettingsError};
use crate::decode::{Catalog, CatalogError, Decoder, DecodingSink};
use crate::ingest::pipeline::{Machine, PipelineError};
use crate::ingest::source::evm::{http_client, ws_connect};
use crate::ingest::source::{
    AnySource, BlockSource, EvmSource, HyperSyncRanges, HyperSyncSource, SourceError,
};
use crate::sink::route::{self, Backend, RouteError, Router, StoreBackend};
use crate::sink::stop::Stop;
use crate::sink::table::{Schema, TableId};
use crate::sink::{SinkError, StdoutJsonSink};
use crate::wire::envelope::ChainId;

#[cfg(feature = "store")]
mod stores;

/// Why the indexer stopped.
///
/// The top of the chain and the only place the layers meet, so each variant names *which*
/// part stopped rather than restating what went wrong: the cause is already typed one
/// level down and travels intact inside the variant. Every variant prints that cause —
/// `{source}` or `transparent` — so the `Display` the process boundary logs shows every
/// layer in one line, where `Debug` would bury the same chain under the raw values behind
/// it.
///
/// The assembly failures are kept apart from the runtime ones on purpose. A bad settings
/// file or unreadable manifests are fixed by editing a file and restarting; a pipeline
/// failure is what a running indexer reports when it stops. Collapsing them would leave a
/// caller unable to tell "this deployment never started" from "this run died".
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// The settings could not be read or parsed.
    #[error("settings could not be loaded: {0}")]
    Settings(#[from] SettingsError),
    /// The protocol manifests could not be loaded, so nothing would have decoded.
    #[error("protocol manifests could not be loaded: {0}")]
    Catalog(#[from] CatalogError),
    /// A sink's `tables` name something the run does not have.
    #[error("[sink.{store}] tables: {source}")]
    Route {
        /// The sink.
        store: Backend,
        /// What was wrong with its list.
        source: RouteError,
    },
    /// The chain source could not be built from its endpoints.
    #[error("chain source could not be built: {0}")]
    Source(#[from] SourceError),
    /// A store could not be opened, so there was nowhere to write.
    #[cfg(feature = "store")]
    #[error("{store} could not be opened: {source}")]
    OpenStore {
        /// The store.
        store: StoreBackend,
        /// Why it could not be opened.
        source: crate::sink::StoreError,
    },
    /// A store's runtime could not be made: the operating system refused its threads.
    #[cfg(feature = "store")]
    #[error("{store}'s runtime could not be started: {source}")]
    StoreRuntime {
        /// The store.
        store: StoreBackend,
        /// The operating system's error.
        source: std::io::Error,
    },
    /// The stores could not be read back and brought to one block.
    #[cfg(feature = "store")]
    #[error("storage could not resume: {0}")]
    Resume(#[from] crate::sink::resume::ResumeError),
    /// Ingest stopped: a source failed, a sink refused an envelope, or the head
    /// subscription ended.
    #[error("ingest stopped: {0}")]
    Ingest(#[from] PipelineError),
    /// A store stopped. Reported ahead of ingest's error, because a dead store is the
    /// cause there and the failed send is only the symptom.
    #[error("{store} stopped: {source}")]
    Storage {
        /// The store.
        store: StoreBackend,
        /// Why it stopped.
        source: SinkError,
    },
    /// A store's task panicked instead of returning an error, so nothing below it ever
    /// ran to explain why.
    ///
    /// Its own variant because `JoinError` says the task died, not that the store
    /// failed, and a panic in a writer is a different bug from a rejected write.
    #[error("{store} task panicked: {source}")]
    StorageTaskPanicked {
        /// The store.
        store: StoreBackend,
        /// `tokio`'s join error, which carries the panic payload.
        source: tokio::task::JoinError,
    },
    /// The Prometheus endpoint could not be served: its address is taken or invalid.
    #[error("metrics endpoint could not be served: {0}")]
    Metrics(#[from] metrics_exporter_prometheus::BuildError),
}

impl RuntimeError {
    /// Whether the settings, not the moment, are at fault: rerunning the same settings
    /// against the same stores fails the same way, so a supervisor should stop rather
    /// than restart. These are an unreadable settings file or manifests, a sink naming a
    /// table the run lacks, and a start or stop height that does not fit the chain or
    /// the stores' history, and stores that cannot be opened or brought to one block as
    /// they stand: a table that does not match its definition, or stores with no block
    /// in common.
    ///
    /// A height above the head is counted too, though the chain would grow past it: a
    /// height the chain has not reached is a mistyped one far more often than an early
    /// one. Everything else — an endpoint or store that failed, a reorg — may pass, so
    /// it is not.
    #[must_use]
    pub fn is_config(&self) -> bool {
        match self {
            Self::Settings(_) | Self::Catalog(_) | Self::Route { .. } => true,
            #[cfg(feature = "store")]
            Self::OpenStore { source, .. } => source.is_permanent(),
            #[cfg(feature = "store")]
            Self::Resume(error) => error.is_permanent(),
            Self::Ingest(error) => matches!(
                error,
                PipelineError::StartAboveHistory { .. }
                    | PipelineError::BackfillWithoutStart
                    | PipelineError::StartNotBuried { .. }
                    | PipelineError::StartAboveHead { .. }
                    | PipelineError::StopAboveHead { .. }
                    | PipelineError::StopBelowStart { .. }
            ),
            _ => false,
        }
    }
}

/// The pipeline the settings describe: the parts, built and ready.
///
/// Generic over its source so a simulation runs the same assembly over an in-memory
/// chain; a deployment's is the [`AnySource`] [`Pipeline::from_settings`] builds.
#[derive(Debug)]
pub(crate) struct Pipeline<B = AnySource> {
    source: B,
    /// A fresh run's first height; `None` starts at the head.
    start_block: Option<u64>,
    /// The last height to index; `None` follows live heads.
    stop_block: Option<u64>,
    decoder: Decoder,
    /// Every table the run has: the dataset tables and each decoded event's.
    schema: Arc<Schema>,
}

/// Where [`Pipeline::run`] sends the stream, resolved from the settings once: the tables
/// to print, or each store to open, with its settings and the tables it writes.
///
/// Apart from [`Pipeline`] so only a pipeline assembled from settings can be run that way:
/// [`Pipeline::from_settings`] is the one place one is made. A pipeline from
/// [`Pipeline::new`] has none and is run with its stores handed over
/// (`Pipeline::run_with_stores`).
#[derive(Debug)]
pub(crate) enum Routes {
    /// `[sink.stdout]`, and the tables it prints.
    Print(HashSet<TableId>),
    /// Every store named, in a fixed order. Never empty.
    #[cfg(feature = "store")]
    Store(Vec<StoreRoute>),
}

/// One store [`Pipeline::run`] opens: its settings, and the tables routed to it.
#[cfg(feature = "store")]
#[derive(Debug)]
pub(crate) struct StoreRoute {
    settings: crate::config::StoreSettings,
    tables: HashSet<TableId>,
}

impl Pipeline {
    /// Assembles the pipeline the settings describe, and the [`Routes`] it runs with.
    ///
    /// Opens nothing: the stores are connected in [`Pipeline::run`], and the source's
    /// WebSocket at its first subscription, so building fails on unreadable manifests
    /// or a sink's unknown table without touching a database file or the network.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::Catalog`] when the manifests cannot be loaded,
    /// [`RuntimeError::Route`] when a sink's `tables` do not resolve, and
    /// [`RuntimeError::Source`] when the HTTP client cannot be built.
    pub(crate) fn from_settings(settings: &Settings) -> Result<(Self, Routes), RuntimeError> {
        let catalog = settings
            .protocols_path()
            .map_or_else(Catalog::empty, |path| {
                Catalog::load(path, &ChainId::new(&settings.ingest.chain))
            })?;
        let resolve = |store: Backend, tables: &route::Tables| {
            tables
                .resolve(catalog.schema())
                .map_err(|source| RuntimeError::Route { store, source })
        };
        let routes = match settings.sink.destination()? {
            Destination::Print(tables) => Routes::Print(resolve(Backend::Stdout, &tables)?),
            #[cfg(feature = "store")]
            Destination::Store(stores) => Routes::Store(
                stores
                    .into_iter()
                    .map(|settings| {
                        let tables = resolve(settings.backend().into(), settings.tables())?;
                        Ok(StoreRoute { settings, tables })
                    })
                    .collect::<Result<_, RuntimeError>>()?,
            ),
        };
        let datasets = route::datasets(routes.tables());
        info!(%datasets, "fetching what the sinks write");
        let chain = settings.ingest.chain.as_str();
        let source = match &settings.ingest.source {
            IngestSource::Rpc {
                http_url,
                ws_url,
                backfill_concurrency,
            } => AnySource::Rpc(EvmSource::new(
                chain,
                http_client(http_url.expose())?,
                ws_connect(ws_url.expose()),
                datasets,
                *backfill_concurrency,
                settings.ingest.finality_depth,
            )),
            IngestSource::HyperSync(hypersync) => {
                info!(url = %hypersync.url, "backfilling from HyperSync; the run stops at the reorg window");
                AnySource::HyperSync(HyperSyncSource::new(
                    chain,
                    &hypersync.url,
                    hypersync.token.expose(),
                    datasets,
                    HyperSyncRanges {
                        span: hypersync.span,
                        concurrency: hypersync.concurrency,
                    },
                    settings.ingest.finality_depth,
                )?)
            }
        };
        let pipeline = Self::new(
            source,
            settings.ingest.start_block,
            settings.ingest.stop_block,
            catalog,
        );
        Ok((pipeline, routes))
    }
}

impl Routes {
    /// The tables each sink writes, whose union ingest fetches.
    fn tables(&self) -> Vec<&HashSet<TableId>> {
        match self {
            Self::Print(tables) => vec![tables],
            #[cfg(feature = "store")]
            Self::Store(stores) => stores.iter().map(|store| &store.tables).collect(),
        }
    }
}

impl<B: BlockSource + 'static> Pipeline<B> {
    /// Assembles a pipeline reading `source` and decoding what `catalog` declares.
    pub(crate) fn new(
        source: B,
        start_block: Option<u64>,
        stop_block: Option<u64>,
        catalog: Catalog,
    ) -> Self {
        Self {
            source,
            start_block,
            stop_block,
            schema: Arc::new(catalog.schema().clone()),
            decoder: Decoder::new(catalog),
        }
    }

    /// Runs until ingest or storage stops, or `stop` is requested: opens the stores
    /// `routes` name, or prints the tables `[sink.stdout]` does.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::OpenStore`] when a store cannot be opened,
    /// [`RuntimeError::Resume`] when the stores cannot be brought to one block, and
    /// [`RuntimeError::Ingest`] or [`RuntimeError::Storage`] when a part fails. Each
    /// carries that part's own error, because "storage stopped" says nothing about why.
    pub(crate) async fn run(self, routes: Routes, mut stop: Stop) -> Result<(), RuntimeError> {
        // A build without a store can only print, so its match has one arm.
        #[cfg_attr(not(feature = "store"), expect(clippy::infallible_destructuring_match))]
        let tables = match routes {
            Routes::Print(tables) => tables,
            #[cfg(feature = "store")]
            Routes::Store(routes) => {
                // A stop while opening drops what is open; see "Shutdown" in the module docs.
                let stores = tokio::select! {
                    biased;
                    () = stop.wait() => return Ok(()),
                    stores = self.open(routes) => stores?,
                };
                return Box::pin(self.run_with_stores(stores, stop)).await;
            }
        };
        // A stop drops ingest; stdout has nothing to finish.
        tokio::select! {
            biased;
            () = stop.wait() => Ok(()),
            printed = self.print(tables) => printed,
        }
    }

    /// Prints `tables`, as `[sink.stdout]` names them. No store, so nothing to restore: a
    /// run starts fresh from the manifests' seeds.
    async fn print(self, tables: HashSet<TableId>) -> Result<(), RuntimeError> {
        let Self {
            source,
            start_block,
            stop_block,
            decoder,
            schema,
        } = self;
        let sinks = vec![(StdoutJsonSink::new(), tables)];
        let sink = DecodingSink::new(decoder, Router::new(schema, sinks));
        Machine::new(source, sink, Vec::new())
            .run(start_block, stop_block)
            .await?;
        Ok(())
    }
}

/// Loads the settings at `path` and runs the pipeline they describe.
///
/// # Errors
///
/// Returns [`RuntimeError`] naming whichever layer stopped: the settings if they could
/// not be read, the pipeline if it could not be assembled, and otherwise the part that
/// failed while running.
pub async fn run(path: &str) -> Result<(), RuntimeError> {
    let settings = Settings::from_file(path, crate::secret::process_env)?;
    info!(
        settings = path,
        chain = %settings.ingest.chain,
        protocols = settings
            .protocols_path()
            .map_or_else(|| "-".to_owned(), |path| path.display().to_string()),
        storage = ?settings.sink,
        "starting"
    );
    if let Some(metrics) = &settings.metrics {
        crate::metrics::serve(metrics, &settings.ingest.chain)?;
        info!(listen = %metrics.listen, "serving metrics");
    }
    let (request, stop) = Stop::new();
    tokio::spawn(async move {
        let signal = termination().await;
        info!(
            signal,
            "stopping: finishing the commits under way; what is not committed is fetched again on the next start"
        );
        // The run may have ended already, and nothing is left to stop.
        let _ = request.send(true);
    });
    let (pipeline, routes) = Pipeline::from_settings(&settings)?;
    let ran = pipeline.run(routes, stop.clone()).await;
    if ran.is_ok() && stop.requested() {
        info!("stopped on request");
    }
    ran
}

/// Loads the settings at `path` and assembles the pipeline they describe, without
/// running it: no store is opened and nothing is fetched. What `indexer --check` runs,
/// so a settings file and the variables it names are found wanting before a deploy
/// restarts a chain on them.
///
/// # Errors
///
/// As [`run`] before it opens the stores: [`RuntimeError::Settings`],
/// [`RuntimeError::Catalog`], [`RuntimeError::Route`], or [`RuntimeError::Source`].
pub fn check(path: &str) -> Result<(), RuntimeError> {
    let settings = Settings::from_file(path, crate::secret::process_env)?;
    Pipeline::from_settings(&settings)?;
    Ok(())
}

/// Waits for the first signal that asks the process to stop: `SIGTERM`, which `docker
/// stop` and systemd send, or `SIGINT`, from Ctrl-C. Returns its name.
///
/// A signal that cannot be listened for is not waited on, so a failure to install a
/// handler leaves that signal's default — ending the process — rather than stopping the
/// run at once.
async fn termination() -> &'static str {
    let interrupt = async {
        match tokio::signal::ctrl_c().await {
            Ok(()) => "SIGINT",
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let terminate = async {
            match signal(SignalKind::terminate()) {
                Ok(mut terminate) => {
                    terminate.recv().await;
                    "SIGTERM"
                }
                Err(_) => std::future::pending().await,
            }
        };
        tokio::select! {
            signal = terminate => signal,
            signal = interrupt => signal,
        }
    }
    #[cfg(not(unix))]
    interrupt.await
}

#[cfg(all(test, feature = "duckdb"))]
mod tests;
