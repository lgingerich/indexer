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
//! and the process exits 0. Before ingest starts, the stores are being opened and
//! brought to one block, which a restart repeats, so a stop there ends the run without
//! waiting. A second signal does nothing more; the supervisor's own timeout is the
//! backstop.
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
#[cfg(feature = "store")]
use tracing::{Instrument as _, info_span};

use crate::config::{IngestSource, Settings, SettingsError};
use crate::decode::{Catalog, CatalogError, Decoder, DecodingSink};
use crate::ingest::pipeline::{Machine, PipelineError};
use crate::ingest::source::evm::{http_client, ws_connect};
use crate::ingest::source::{
    AnySource, BlockSource, EvmSource, HyperSyncRanges, HyperSyncSource, SourceError,
};
#[cfg(feature = "store")]
use crate::sink;
#[cfg(feature = "store")]
use crate::sink::any_store::AnyStore;
#[cfg(feature = "store")]
use crate::sink::channel::Batching;
#[cfg(feature = "store")]
use crate::sink::resume::{Opened, ResumeError};
use crate::sink::route::{self, Backend, RouteError, Router};
use crate::sink::stop::Stop;
#[cfg(feature = "store")]
use crate::sink::store_runtime::StoreRuntime;
use crate::sink::table::{Schema, TableId};
use crate::sink::{SinkError, StdoutJsonSink};
use crate::wire::envelope::ChainId;
#[cfg(feature = "store")]
use crate::wire::envelope::{BlockMeta, StoredContract};

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
        store: Backend,
        /// Why it could not be opened.
        source: sink::StoreError,
    },
    /// A store's runtime could not be made: the operating system refused its threads.
    #[cfg(feature = "store")]
    #[error("{store}'s runtime could not be started: {source}")]
    StoreRuntime {
        /// The store.
        store: Backend,
        /// The operating system's error.
        source: std::io::Error,
    },
    /// The stores could not be read back and brought to one block.
    #[cfg(feature = "store")]
    #[error("storage could not resume: {0}")]
    Resume(#[from] ResumeError),
    /// Ingest stopped: a source failed, a sink refused an envelope, or the head
    /// subscription ended.
    #[error("ingest stopped: {0}")]
    Ingest(#[from] PipelineError),
    /// A store stopped. Reported ahead of ingest's error, because a dead store is the
    /// cause there and the failed send is only the symptom.
    #[error("{store} stopped: {source}")]
    Storage {
        /// The store.
        store: Backend,
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
        store: Backend,
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

/// Each sink the settings name and the tables it writes: what [`Pipeline::run`] opens.
///
/// Apart from [`Pipeline`] so only a pipeline assembled from settings can be run that way:
/// [`Pipeline::from_settings`] is the one place one is made, from settings that name at
/// least one sink, so it is never empty. A pipeline from [`Pipeline::new`] has none and
/// is run with its stores handed over (`Pipeline::run_with_stores`). Were the routes a
/// field that such a pipeline left empty, `run` would read the empty list as "no stdout,
/// so store", and open every configured store with no tables.
#[derive(Debug)]
pub(crate) struct Routes(Vec<(Backend, HashSet<TableId>)>);

impl Routes {
    /// Whether the run prints rather than stores. The settings never name stdout beside a
    /// store, so a run does one or the other.
    #[cfg(feature = "store")]
    fn prints(&self) -> bool {
        self.0
            .iter()
            .any(|(backend, _)| *backend == Backend::Stdout)
    }
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
        let routes = settings
            .sink
            .tables()
            .into_iter()
            .map(|(store, tables)| {
                let resolved = tables
                    .resolve(catalog.schema())
                    .map_err(|source| RuntimeError::Route { store, source })?;
                Ok((store, resolved))
            })
            .collect::<Result<Vec<_>, RuntimeError>>()?;
        let datasets = route::datasets(routes.iter().map(|(_, tables)| tables));
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
        Ok((pipeline, Routes(routes)))
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
    pub(crate) async fn run(
        self,
        routes: Routes,
        settings: &Settings,
        mut stop: Stop,
    ) -> Result<(), RuntimeError> {
        #[cfg(feature = "store")]
        if !routes.prints() {
            let stores = tokio::select! {
                biased;
                () = stop.wait() => return Ok(()),
                stores = self.open(&routes, settings) => stores?,
            };
            return Box::pin(self.run_with_stores(stores, stop)).await;
        }
        #[cfg(not(feature = "store"))]
        let _ = settings;
        tokio::select! {
            biased;
            () = stop.wait() => Ok(()),
            printed = self.print(routes) => printed,
        }
    }

    /// Prints the tables `[sink.stdout]` names. No store, so no tables and nothing to
    /// restore: a run starts fresh from the manifests' seeds.
    async fn print(self, routes: Routes) -> Result<(), RuntimeError> {
        let Self {
            source,
            start_block,
            stop_block,
            decoder,
            schema,
        } = self;
        let sinks = routes
            .0
            .into_iter()
            .map(|(_, tables)| (StdoutJsonSink::new(), tables))
            .collect();
        let sink = DecodingSink::new(decoder, Router::new(schema, sinks));
        Machine::new(source, sink, Vec::new())
            .run(start_block, stop_block)
            .await?;
        Ok(())
    }

    /// Opens every store the settings name, each with the tables `routes` sends it and on a
    /// runtime of its own (see `crate::sink::store_runtime`). Opened before ingest
    /// starts, so a bad path or connection fails at startup rather than after the first
    /// block.
    #[cfg(feature = "store")]
    async fn open(
        &self,
        routes: &Routes,
        settings: &Settings,
    ) -> Result<Vec<Opened<AnyStore>>, RuntimeError> {
        let chain = &settings.ingest.chain;
        let mut stores = Vec::new();
        #[cfg(feature = "duckdb")]
        if let Some(duckdb) = &settings.sink.duckdb {
            let (settings, chain) = (duckdb.clone(), chain.clone());
            let batching = Batching::eager(duckdb.batch_records);
            let opened = self.open_store(routes, Backend::DuckDb, batching, async move |schema| {
                Ok(AnyStore::DuckDb(Box::new(
                    sink::DuckDbSink::open(&settings, schema, &chain).await?,
                )))
            });
            stores.push(opened.await?);
        }
        #[cfg(feature = "postgres")]
        if let Some(postgres) = &settings.sink.postgres {
            let (settings, chain) = (postgres.clone(), chain.clone());
            let batching = postgres.batching();
            let opened =
                self.open_store(routes, Backend::Postgres, batching, async move |schema| {
                    Ok(AnyStore::Postgres(Box::new(
                        sink::PostgresSink::open(&settings, schema, &chain).await?,
                    )))
                });
            stores.push(opened.await?);
        }
        #[cfg(feature = "delta")]
        if let Some(delta) = &settings.sink.delta {
            let (settings, chain) = (delta.clone(), chain.clone());
            let batching = delta.batching();
            let opened = self.open_store(routes, Backend::Delta, batching, async move |schema| {
                Ok(AnyStore::Delta(Box::new(
                    sink::DeltaSink::open(&settings, schema, &chain).await?,
                )))
            });
            stores.push(opened.await?);
        }
        Ok(stores)
    }

    /// Opens `backend` with `open`, handed the schema of the tables routed to it, on a
    /// runtime of the store's own, so what opening spawns — a connection's task, the
    /// lake's checkpoints and maintenance — runs there too.
    #[cfg(feature = "store")]
    async fn open_store<F>(
        &self,
        routes: &Routes,
        backend: Backend,
        batching: Batching,
        open: impl FnOnce(Arc<Schema>) -> F,
    ) -> Result<Opened<AnyStore>, RuntimeError>
    where
        F: Future<Output = Result<AnyStore, sink::StoreError>> + Send + 'static,
    {
        let (schema, tables) = self.route(routes, backend);
        let runtime =
            StoreRuntime::dedicated(backend).map_err(|source| RuntimeError::StoreRuntime {
                store: backend,
                source,
            })?;
        let store = runtime.run(open(schema)).await.map_err(opening(backend))?;
        Ok(Opened {
            backend,
            store,
            batching,
            tables,
            runtime,
        })
    }

    /// The tables `routes` sends `backend`, and the schema of them a store opens with.
    #[cfg(feature = "store")]
    fn route(&self, routes: &Routes, backend: Backend) -> (Arc<Schema>, HashSet<TableId>) {
        let tables = routes
            .0
            .iter()
            .find(|(named, _)| *named == backend)
            .map(|(_, tables)| tables.clone())
            .unwrap_or_default();
        (Arc::new(self.schema.select(&tables)), tables)
    }

    /// Runs against open `stores`: brings them to one block, then ingests into each the
    /// tables routed to it, every store committing as its batching says, until ingest
    /// or a store stops or `stop` is requested.
    ///
    /// # Errors
    ///
    /// As [`Pipeline::run`], less opening the stores.
    #[cfg(feature = "store")]
    pub(crate) async fn run_with_stores<S: sink::Store>(
        self,
        stores: Vec<Opened<S>>,
        mut stop: Stop,
    ) -> Result<(), RuntimeError> {
        let Self {
            source,
            start_block,
            stop_block,
            decoder,
            schema,
            ..
        } = self;
        let chain = source.chain().clone();
        let (stores, resumed) = tokio::select! {
            biased;
            () = stop.wait() => return Ok(()),
            resumed = sink::resume::resume(stores) => resumed?,
        };
        let decoder = restore(decoder, &chain, resumed.contracts);
        let ledger = ledger(&chain, resumed.ledger);

        let mut sinks = Vec::with_capacity(stores.len());
        let mut drains = Vec::with_capacity(stores.len());
        for Opened {
            backend,
            mut store,
            batching,
            tables,
            runtime,
        } in stores
        {
            let (sender, receiver) = sink::channel::ChannelSink::new(backend);
            // On the store's own runtime, so its commits never wait behind ingest's work
            // or another store's, nor theirs behind its own.
            let stopping = stop.clone();
            let drain = async move { receiver.drain(&mut store, batching, stopping).await }
                .instrument(info_span!("storage", store = %backend));
            let drain = runtime.spawn(drain);
            // The runtime is held until its drain is joined, and let go after.
            drains.push((backend, drain, runtime));
            sinks.push((sender, tables));
        }

        let ingest = Machine::new(
            source,
            DecodingSink::new(decoder, Router::new(schema, sinks)),
            ledger,
        )
        .run(start_block, stop_block);
        // Dropping ingest part-way is a crash to it: what it has not handed to storage is
        // fetched again from the stores' ledgers on the next start.
        let ingest = tokio::select! {
            biased;
            () = stop.wait() => Ok(()),
            ingest = ingest => ingest,
        };

        // Ingest's halves of the channels are gone by now, so each store drains what is
        // queued, or on a stop finishes its commit under way, and ends. The join order is
        // [`finish`].
        let mut stopped = Vec::with_capacity(drains.len());
        for (backend, drain, _runtime) in drains {
            stopped.push((backend, drain.await));
        }
        finish(stopped, ingest)
    }
}

/// Adds the contracts a previous run discovered, as the stores read them back.
#[cfg(feature = "store")]
fn restore(mut decoder: Decoder, chain: &ChainId, stored: Vec<StoredContract>) -> Decoder {
    let restored = decoder.restore(stored);
    info!(%chain, restored, contracts = decoder.contracts(), "discovered contracts restored");
    decoder
}

/// The accepted blocks a previous run committed, as ingest takes them.
#[cfg(feature = "store")]
fn ledger(chain: &ChainId, stored: Vec<BlockMeta>) -> Vec<BlockMeta> {
    if let (Some(oldest), Some(tip)) = (stored.first(), stored.last()) {
        info!(%chain, from = oldest.height, tip = tip.height, hash = %tip.hash,
            "accepted blocks restored");
    } else {
        info!(%chain, "no accepted blocks stored; starting fresh");
    }
    stored
}

/// The error for `store` failing to open.
#[cfg(feature = "store")]
fn opening(store: Backend) -> impl FnOnce(sink::StoreError) -> RuntimeError {
    move |source| RuntimeError::OpenStore { store, source }
}

/// How each store's drain ended, as its task returned it.
#[cfg(feature = "store")]
type Stopped = (
    Backend,
    Result<Result<u64, SinkError>, tokio::task::JoinError>,
);

/// Reports how the tasks stopped.
///
/// The stores come first, in the order they were opened. A store's error is a rejected
/// write, and its panic is a `JoinError`: the writer died outside the store's own error
/// path (an unwrap on a row width, say), which is neither a store failure nor an ingest
/// failure. Ingest's error on this path is the failed send that followed —
/// [`crate::sink::SinkError::StorageClosed`] — so it is returned only when every store
/// itself finished.
#[cfg(feature = "store")]
fn finish(stopped: Vec<Stopped>, ingest: Result<(), PipelineError>) -> Result<(), RuntimeError> {
    for (store, drained) in stopped {
        match drained {
            Ok(Ok(stored)) => info!(%store, stored, "storage stopped"),
            Ok(Err(source)) => return Err(RuntimeError::Storage { store, source }),
            Err(source) => return Err(RuntimeError::StorageTaskPanicked { store, source }),
        }
    }
    ingest?;
    Ok(())
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
    let ran = pipeline.run(routes, &settings, stop.clone()).await;
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
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are allowed
// them per the repository test style, since a failed expectation there means the fixture
// or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use alloy_dyn_abi::DynSolValue;
    use alloy_primitives::{Address, B256, keccak256};

    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::config::Settings;
    use crate::decode::{Catalog, Decoder, DecodingSink};
    use crate::ingest::pipeline::{Machine, PipelineError};
    use crate::ingest::source::{BlockSource, FetchedBlock, HeadStream, SourceError};
    use crate::sink::channel::Batching;
    use crate::sink::resume::Opened;
    use crate::sink::route::Backend;
    use crate::sink::stop::Stop;
    use crate::sink::store_runtime::StoreRuntime;
    use crate::sink::table::Schema;
    use crate::sink::{self, DuckDbSink, EnvelopeSink as _, SinkError, Store as _};
    use crate::sink::{Operation, StoreError};
    use crate::wire::envelope::{BlockMeta, ChainId, Envelope, Event, Log, Reorg, StoredContract};

    use super::{Pipeline, RuntimeError, finish, ledger};

    /// The shipped protocols, as an absolute path so [`Settings::parse`] carries no
    /// directory to resolve it against.
    fn protocols() -> String {
        format!("{}/protocols", env!("CARGO_MANIFEST_DIR"))
    }

    const SETTINGS: &str = r#"
[ingest]
chain = "base"
http_url = "https://example.invalid"
ws_url = "wss://example.invalid"

[sink.duckdb]
tables = ["blocks", "transactions", "logs", "decoded_logs", "contract_events"]
"#;

    /// The store's write error is the one returned when ingest then fails because the
    /// receiver is gone. That second failure is the failed send, and reporting it would
    /// hide the write that caused it.
    #[test]
    fn a_failed_store_is_reported_ahead_of_the_closed_send() {
        let connection = duckdb::Connection::open_in_memory().expect("open in-memory DuckDB");
        connection
            .execute_batch("CREATE TABLE blocks (not_a_column INTEGER)")
            .expect("schema");
        let source = connection
            .execute("INSERT INTO blocks VALUES (1, 2)", [])
            .expect_err("two values cannot fit one column");
        let storage = Ok(Err(SinkError::Store(StoreError::Engine {
            operation: Operation::Stage,
            target: Some("blocks".to_owned()),
            source: source.into(),
        })));
        let ingest = Err(PipelineError::Sink(SinkError::StorageClosed));

        let error = finish(vec![(Backend::DuckDb, storage)], ingest)
            .expect_err("the store rejected the batch");
        assert!(
            matches!(
                &error,
                RuntimeError::Storage {
                    store: Backend::DuckDb,
                    source: SinkError::Store(StoreError::Engine {
                        target: Some(table),
                        ..
                    }),
                } if table == "blocks"
            ),
            "the append error must be the one reported, not the closed send: {error:?}"
        );
    }

    /// A store that cannot open or resume as it stands is the settings' fault, so the
    /// process exits as a config error and the supervisor waits for a fix rather than
    /// restarting into the same failure; a store that may come back is not.
    #[test]
    fn a_store_that_cannot_start_as_it_stands_is_a_config_error() {
        use crate::sink::resume::ResumeError;

        let open = |source| RuntimeError::OpenStore {
            store: Backend::Postgres,
            source,
        };
        for (case, error, config) in [
            (
                "a drifted table",
                open(StoreError::Drift {
                    table: "logs".to_owned(),
                    difference: "it has no data column".to_owned(),
                }),
                true,
            ),
            (
                "a locked schema",
                open(StoreError::Locked {
                    schema: "base".to_owned(),
                }),
                false,
            ),
            (
                "an unreachable store",
                open(StoreError::Engine {
                    operation: Operation::Open,
                    target: None,
                    source: "connection refused".into(),
                }),
                false,
            ),
            (
                "stores with no common block",
                RuntimeError::Resume(ResumeError::NoCommonBlock {
                    tips: vec![(Backend::Postgres, 9), (Backend::Delta, 5)],
                }),
                true,
            ),
            (
                "a ledger that timed out",
                RuntimeError::Resume(ResumeError::Restore {
                    store: Backend::Delta,
                    source: StoreError::Engine {
                        operation: Operation::Read,
                        target: Some("accepted_blocks".to_owned()),
                        source: "timed out".into(),
                    },
                }),
                false,
            ),
        ] {
            assert_eq!(error.is_config(), config, "{case}: {error}");
        }
    }

    /// The shipped protocols load, so the example settings a reader copies stay runnable.
    #[test]
    fn the_shipped_protocols_assemble() {
        let decode = format!("\n[decode]\nprotocols = {:?}\n", protocols());
        let settings = Settings::parse(&format!("{SETTINGS}{decode}"), crate::secret::process_env)
            .expect("settings parse");
        Pipeline::from_settings(&settings).expect("the pipeline builds");
    }

    /// The shipped protocols' schema, which a store opens with to hold what [`decoder`]
    /// decodes.
    fn schema() -> Arc<Schema> {
        sink::fixtures::schema()
    }

    /// `store`, writing every table of [`schema`].
    fn every_table<S>(backend: Backend, store: S, batching: Batching) -> Opened<S> {
        Opened {
            backend,
            store,
            batching,
            tables: schema().tables().iter().map(|def| def.id).collect(),
            runtime: StoreRuntime::current(),
        }
    }

    fn decoder() -> Decoder {
        Decoder::new(
            Catalog::load(protocols(), &ChainId::new("base")).expect("shipped protocols load"),
        )
    }

    /// A Uniswap V3 `PoolCreated` from the Base factory naming `pool`, in `block`.
    fn pool_created(pool: Address, block: B256) -> Envelope {
        let log = Log {
            address: "0x33128a8fC17869897dcE68Ed026d694621f6FDfD"
                .parse()
                .expect("factory"),
            topic0: Some(keccak256(
                "PoolCreated(address,address,uint24,int24,address)",
            )),
            topic1: Some(B256::with_last_byte(1)),
            topic2: Some(B256::with_last_byte(2)),
            topic3: Some(B256::with_last_byte(3)),
            data: DynSolValue::Tuple(vec![
                DynSolValue::Int(alloy_primitives::I256::try_from(60).expect("int"), 24),
                DynSolValue::Address(pool),
            ])
            .abi_encode_params()
            .into(),
            log_index: 1,
            block_number: 10,
            block_hash: block,
            ..Log::default()
        };
        Envelope::new(ChainId::new("base"), Event::Log(Box::new(log)))
    }

    /// A real Uniswap V3 `Swap`, re-addressed to `pool`.
    fn swap(pool: Address) -> Envelope {
        let mut swap: Envelope = serde_json::from_str(
            include_str!("../examples/fixtures/uniswap_v3_swaps.ndjson")
                .lines()
                .next()
                .expect("a fixture line"),
        )
        .expect("the fixture is a published envelope");
        let Event::Log(log) = &mut swap.event else {
            panic!("log");
        };
        log.address = pool;
        swap
    }

    fn count(reader: &duckdb::Connection, table: &str) -> i64 {
        reader
            .query_row(&format!("SELECT count(*) FROM base.{table}"), [], |row| {
                row.get(0)
            })
            .expect("count")
    }

    /// The whole hand-off, minus the network: a factory's creation log goes through
    /// decode, over the channel, and into the store with the pool it discovered; the
    /// pool's swap decodes in the same batch; a restart reads the pool back and decodes
    /// it; and once a reorg orphans the creating block, the pool is deleted with it and a
    /// restart no longer decodes it.
    #[tokio::test]
    async fn a_discovered_contract_is_stored_and_survives_a_restart() {
        let pool = Address::from([0xd0; 20]);
        let creating = B256::with_last_byte(0xcc);
        let chain = ChainId::new("base");

        let connection = duckdb::Connection::open_in_memory().expect("open in-memory DuckDB");
        let reader = connection.try_clone().expect("a second handle");
        let mut store = DuckDbSink::connected(connection, schema(), "base")
            .await
            .expect("create tables");
        let (blocks, receiver) = sink::channel::ChannelSink::new(Backend::DuckDb);
        let storage = tokio::spawn(async move {
            receiver
                .drain(&mut store, Batching::eager(500), Stop::never())
                .await
        });

        let mut decoding = DecodingSink::new(decoder(), blocks);
        decoding
            .publish(pool_created(pool, creating))
            .await
            .expect("creation");
        decoding.publish(swap(pool)).await.expect("swap");
        decoding.flush().await.expect("flush");
        drop(decoding);
        storage.await.expect("no panic").expect("storage drains");

        // One table per dataset: the raw logs, their decodes, and the discovery.
        assert_eq!(
            [
                count(&reader, "logs"),
                count(&reader, "decoded_logs"),
                count(&reader, "contracts")
            ],
            [2, 2, 1]
        );
        // And each decode in its event's own typed table.
        assert_eq!(
            [
                count(&reader, "uniswap_v3_uniswap_v3_factory_pool_created"),
                count(&reader, "uniswap_v3_pool_swap")
            ],
            [1, 1]
        );
        // The decoded rows key back to the raw logs they came from. Joined on the
        // columns: a key prefix would also match log 3 to the decodes of log 30.
        let orphans: i64 = reader
            .query_row(
                "SELECT count(*) FROM base.decoded_logs d WHERE NOT EXISTS \
                 (SELECT 1 FROM base.logs l WHERE l.block_hash = d.block_hash \
                 AND l.transaction_hash = d.transaction_hash AND l.log_index = d.log_index)",
                [],
                |row| row.get(0),
            )
            .expect("join");
        assert_eq!(orphans, 0);

        // A restart: a fresh decoder knows the pool once the store's rows are restored.
        let mut store =
            DuckDbSink::connected(reader.try_clone().expect("handle"), schema(), "base")
                .await
                .expect("reopen");
        let stored = store.contracts(&chain).await.expect("read back");
        assert_eq!(stored, [stored_pool(pool, creating)]);
        let mut restarted = decoder();
        assert_eq!(restarted.restore(stored), 1);
        let Event::Log(log) = swap(pool).event else {
            panic!("log");
        };
        assert!(restarted.decode(&log).expect("decode").is_some());

        // The creating block is orphaned: the stored row is deleted, so nothing restores.
        store
            .publish(Envelope::new(
                chain.clone(),
                Event::Reorg(Reorg {
                    height: 10,
                    new_head_hash: B256::with_last_byte(0xdd),
                    orphaned_hashes: vec![creating],
                }),
            ))
            .await
            .expect("publish");
        store.flush().await.expect("flush");
        assert!(store.contracts(&chain).await.expect("read back").is_empty());
        assert_eq!(count(&reader, "contracts"), 0);
    }

    /// A fixed chain: the head and, per height, the block's identity and its events. Its
    /// head subscription ends at once, so [`Machine::run`] returns after startup has
    /// converged — a stand-in for a process that indexes to the head and then stops.
    struct FakeChain {
        chain: ChainId,
        head: u64,
        blocks: HashMap<u64, (BlockMeta, Vec<Event>)>,
    }

    impl FakeChain {
        /// Linear blocks `1..=head`, each hashed `tag:height` and empty: a logs-only run
        /// sees most blocks with no matching log.
        fn new(head: u64, tag: u8) -> Self {
            let mut chain = Self {
                chain: ChainId::new("base"),
                head,
                blocks: HashMap::new(),
            };
            chain.branch(1, head, tag);
            chain
        }

        /// Replaces `from..=through` with blocks hashed `tag:height`, linked to the block
        /// below `from`.
        fn branch(&mut self, from: u64, through: u64, tag: u8) {
            for height in from..=through {
                let parent_hash = self
                    .blocks
                    .get(&(height - 1))
                    .map_or(B256::ZERO, |(meta, _)| meta.hash);
                let meta = BlockMeta {
                    height,
                    hash: block_hash(tag, height),
                    parent_hash,
                    timestamp: height,
                };
                self.blocks.insert(height, (meta, Vec::new()));
            }
            self.head = self.head.max(through);
        }

        /// Adds a log to the block at `height`, stamped with that block's identity.
        fn log(&mut self, height: u64, envelope: Envelope) {
            let (meta, events) = self.blocks.get_mut(&height).expect("block");
            let Event::Log(mut log) = envelope.event else {
                panic!("log");
            };
            log.block_number = height;
            log.block_hash = meta.hash;
            events.push(Event::Log(log));
        }
    }

    fn block_hash(tag: u8, height: u64) -> B256 {
        let mut bytes = [0; 32];
        bytes[0] = tag;
        bytes[24..].copy_from_slice(&height.to_be_bytes());
        B256::from(bytes)
    }

    impl BlockSource for FakeChain {
        fn chain(&self) -> &ChainId {
            &self.chain
        }
        async fn subscribe_heads(&self) -> Result<HeadStream, SourceError> {
            Ok(Box::pin(futures_util::stream::empty()))
        }
        async fn fetch_header(&self, height: Option<u64>) -> Result<BlockMeta, SourceError> {
            Ok(self.blocks[&height.unwrap_or(self.head)].0)
        }
        async fn fetch_block(
            &self,
            height: u64,
            _head: Option<&BlockMeta>,
        ) -> Result<FetchedBlock, SourceError> {
            let (meta, events) = self.blocks[&height].clone();
            Ok(FetchedBlock { meta, events })
        }
    }

    /// One process lifetime against the store behind `connection`: restore what the store
    /// holds, index `chain` to its head through decode and the storage channel, then stop
    /// when the head subscription ends and let storage drain, as a crash after the last
    /// commit would leave it.
    async fn run_once(connection: &duckdb::Connection, chain: FakeChain, start: Option<u64>) {
        let store =
            DuckDbSink::connected(connection.try_clone().expect("handle"), schema(), "base")
                .await
                .expect("open");
        let catalog =
            Catalog::load(protocols(), &ChainId::new("base")).expect("shipped protocols load");
        let result = Pipeline::new(chain, start, None, catalog)
            .run_with_stores(
                vec![every_table(Backend::DuckDb, store, Batching::eager(500))],
                Stop::never(),
            )
            .await;
        ran_to_head(&result);
    }

    /// Asserts that a run indexed to the head and stopped only because the head
    /// subscription ended, as every [`FakeChain`] run does.
    fn ran_to_head(result: &Result<(), RuntimeError>) {
        assert!(
            matches!(
                result,
                Err(RuntimeError::Ingest(PipelineError::SubscriptionClosed))
            ),
            "{result:?}"
        );
    }

    /// The pool [`pool_created`] names, stored with the block that created it.
    fn stored_pool(pool: Address, block_hash: B256) -> StoredContract {
        StoredContract {
            protocol: "uniswap_v3".to_owned(),
            name: "UniswapV3Pool".to_owned(),
            address: pool,
            block_hash,
        }
    }

    /// The ledger of blocks `1..=through`, hashed as [`FakeChain`] hashes them: tagged
    /// `0xa0` below `fork` and `0xb0` from it.
    fn chain_through(through: u64, fork: u64) -> Vec<(u64, String)> {
        (1..=through)
            .map(|height| {
                let tag = if height < fork { 0xa0 } else { 0xb0 };
                (height, format!("{:#x}", block_hash(tag, height)))
            })
            .collect()
    }

    /// A restored ledger as `(height, hash)` pairs, to compare with [`chain_through`].
    #[cfg(feature = "delta")]
    fn ledger_of(restored: &sink::Restored) -> Vec<(u64, String)> {
        restored
            .ledger
            .iter()
            .map(|block| (block.height, format!("{:#x}", block.hash)))
            .collect()
    }

    fn heights(reader: &duckdb::Connection) -> Vec<(u64, String)> {
        reader
            .prepare("SELECT height, hash FROM base.accepted_blocks ORDER BY height, hash")
            .expect("prepare")
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .expect("query")
            .collect::<Result<_, _>>()
            .expect("rows")
    }

    /// Three process lifetimes against one store. The first indexes from a start height;
    /// the second resumes after it while the chain only grew, discovering a pool created
    /// while nothing was running; the third resumes after a reorg replaced the pool's
    /// block while nothing was running. The store ends with no gap in its ledger, one
    /// reorg naming the stored suffix, nothing of the orphaned blocks, and the pool
    /// retracted — its later swap does not decode.
    #[tokio::test]
    async fn a_restart_resumes_from_the_store_and_reconciles_a_fork_while_down() {
        let pool = Address::from([0xd0; 20]);
        let connection = duckdb::Connection::open_in_memory().expect("open in-memory DuckDB");

        run_once(&connection, FakeChain::new(4, 0xa0), Some(1)).await;
        assert_eq!(
            heights(&connection),
            chain_through(4, u64::MAX),
            "every block, empty ones included"
        );

        let mut grown = FakeChain::new(6, 0xa0);
        grown.log(5, pool_created(pool, B256::ZERO));
        run_once(&connection, grown, None).await;
        assert_eq!(
            heights(&connection),
            chain_through(6, u64::MAX),
            "resumed after 4 with no gap"
        );
        assert_eq!(count(&connection, "reorgs"), 0);
        assert_eq!(
            count(&connection, "contracts"),
            1,
            "created while down, discovered"
        );

        let mut forked = FakeChain::new(6, 0xa0);
        forked.log(5, pool_created(pool, B256::ZERO));
        forked.branch(5, 7, 0xb0);
        forked.log(7, swap(pool));
        run_once(&connection, forked, None).await;

        let orphaned: String = connection
            .query_row("SELECT orphaned_hashes FROM base.reorgs", [], |row| {
                row.get(0)
            })
            .expect("one reorg");
        let expected: Vec<String> = [6, 5]
            .map(|height| format!("{:#x}", block_hash(0xa0, height)))
            .to_vec();
        assert_eq!(
            serde_json::from_str::<Vec<String>>(&orphaned).expect("hash list"),
            expected
        );
        assert_eq!(count(&connection, "reorgs"), 1);
        assert_eq!(
            [
                count(&connection, "logs"),
                count(&connection, "decoded_logs"),
                count(&connection, "contracts")
            ],
            [1, 0, 0],
            "the creation went with its orphaned block; only the undecoded swap remains"
        );
        assert_eq!(
            heights(&connection),
            chain_through(7, 5),
            "the orphaned blocks left the ledger"
        );
        let mut store =
            DuckDbSink::connected(connection.try_clone().expect("handle"), schema(), "base")
                .await
                .expect("open");
        let restored = store.restore().await.expect("restore");
        assert!(restored.contracts.is_empty());
        assert_eq!(
            restored
                .ledger
                .iter()
                .map(|block| (block.height, format!("{:#x}", block.hash)))
                .collect::<Vec<_>>(),
            chain_through(7, 5)
        );
        assert!(
            restored
                .ledger
                .windows(2)
                .all(|pair| pair[1].parent_hash == pair[0].hash),
            "the restored ledger is one linked chain"
        );
    }

    /// A start height above a stored ledger's tip would leave a gap, so the run refuses.
    #[tokio::test]
    async fn a_start_height_above_a_stored_ledger_is_refused() {
        let connection = duckdb::Connection::open_in_memory().expect("open in-memory DuckDB");
        run_once(&connection, FakeChain::new(3, 0xa0), Some(1)).await;
        let mut store =
            DuckDbSink::connected(connection.try_clone().expect("handle"), schema(), "base")
                .await
                .expect("open");
        let chain = ChainId::new("base");
        let restored = ledger(&chain, store.restore().await.expect("restore").ledger);
        let machine = Machine::new(FakeChain::new(5, 0xa0), store, restored);
        let result = machine.run(Some(5), None).await;
        assert!(
            matches!(
                result,
                Err(PipelineError::StartAboveHistory { start: 5, tip: 3 })
            ),
            "{result:?}"
        );
    }

    /// A lake directory for one test, removed when the guard drops.
    #[cfg(feature = "delta")]
    struct LakeDir(std::path::PathBuf);

    #[cfg(feature = "delta")]
    impl Drop for LakeDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// A lake in a directory of its own named for `test`, writing `tables`, and the guard
    /// that removes it. The interval is far longer than any run, so rows reach the lake
    /// only through the drain's flush when the channel closes.
    #[cfg(feature = "delta")]
    fn lake(test: &str, tables: &str) -> (LakeDir, sink::DeltaSettings) {
        let dir = LakeDir(
            std::env::temp_dir().join(format!("indexer-runtime-{test}-{}", std::process::id())),
        );
        let settings = sink::DeltaSettings {
            tables: serde_json::from_str(tables).expect("tables"),
            uri: dir.0.display().to_string(),
            commit_interval_secs: 3600,
            max_buffer_bytes: usize::MAX,
            maintenance: false,
            vacuum_retention_hours: 168,
            partition_blocks: 100_000,
            storage: std::collections::BTreeMap::new(),
        };
        (dir, settings)
    }

    /// One process lifetime against the lake `settings` names, as [`run_once`] is against
    /// a database: restore, index to the head, stop. The interval is far longer than the
    /// run, so every row reaches the lake through the drain's flush when the channel
    /// closes.
    #[cfg(feature = "delta")]
    async fn run_lake(settings: &sink::DeltaSettings, chain: FakeChain, start: Option<u64>) {
        let store = sink::DeltaSink::open(settings, schema(), "base")
            .await
            .expect("the lake opens");
        let catalog =
            Catalog::load(protocols(), &ChainId::new("base")).expect("shipped protocols load");
        let result = Box::pin(Pipeline::new(chain, start, None, catalog).run_with_stores(
            vec![every_table(Backend::Delta, store, settings.batching())],
            Stop::never(),
        ))
        .await;
        ran_to_head(&result);
    }

    /// What a fresh open of the lake restores.
    #[cfg(feature = "delta")]
    async fn restored(settings: &sink::DeltaSettings) -> sink::Restored {
        sink::DeltaSink::open(settings, schema(), "base")
            .await
            .expect("the lake opens")
            .restore()
            .await
            .expect("the lake restores")
    }

    /// The restart test of [`a_restart_resumes_from_the_store_and_reconciles_a_fork_while_down`]
    /// against the lake: three process lifetimes, the second discovering a pool created
    /// while down and the third reconciling a fork that replaced the pool's block. Each
    /// commits only when storage drains at the end, so a lost final flush would show as
    /// an empty restore.
    #[cfg(feature = "delta")]
    #[tokio::test]
    async fn a_lake_restart_resumes_from_its_ledger_and_reconciles_a_fork_while_down() {
        let (_dir, settings) = lake("lake", r#"["contract_events"]"#);
        let pool = Address::from([0xd0; 20]);

        run_lake(&settings, FakeChain::new(4, 0xa0), Some(1)).await;
        assert_eq!(
            ledger_of(&restored(&settings).await),
            chain_through(4, u64::MAX)
        );

        let mut grown = FakeChain::new(6, 0xa0);
        grown.log(5, pool_created(pool, B256::ZERO));
        run_lake(&settings, grown, None).await;
        let after = restored(&settings).await;
        assert_eq!(
            ledger_of(&after),
            chain_through(6, u64::MAX),
            "resumed after 4 with no gap"
        );
        assert_eq!(
            after.contracts,
            [stored_pool(pool, block_hash(0xa0, 5))],
            "created while down, discovered"
        );

        let mut forked = FakeChain::new(6, 0xa0);
        forked.log(5, pool_created(pool, B256::ZERO));
        forked.branch(5, 7, 0xb0);
        forked.log(7, swap(pool));
        run_lake(&settings, forked, None).await;

        let after = restored(&settings).await;
        assert!(
            after.contracts.is_empty(),
            "the creation went with its block"
        );
        assert_eq!(ledger_of(&after), chain_through(7, 5));
        assert!(
            after
                .ledger
                .windows(2)
                .all(|pair| pair[1].parent_hash == pair[0].hash),
            "the restored ledger is one linked chain"
        );
    }

    /// Two stores, the database ahead of the lake when the process stops, and the chain
    /// forked below the database's tip while nothing ran. The restart resumes from the
    /// block both hold; the database retracts each of its orphaned blocks as ingest hands
    /// that height again, so neither keeps the orphaned branch and the lake is never
    /// handed a block it has; both end on the same canonical chain.
    #[cfg(feature = "delta")]
    #[tokio::test]
    async fn stores_at_different_heights_resume_from_the_block_they_share() {
        let (_dir, settings) = lake("two", r#"["blocks"]"#);
        let connection = duckdb::Connection::open_in_memory().expect("open in-memory DuckDB");
        let pool = Address::from([0xd0; 20]);
        let both = async |chain: FakeChain, start: Option<u64>| {
            run_both(&connection, &settings, chain, start).await;
        };

        both(FakeChain::new(4, 0xa0), Some(1)).await;

        // The database alone runs on: it holds 5 and 6, and a pool created at 6.
        let mut grown = FakeChain::new(6, 0xa0);
        grown.log(6, pool_created(pool, B256::ZERO));
        run_once(&connection, grown, None).await;
        assert_eq!(heights(&connection), chain_through(6, u64::MAX));
        assert_eq!(count(&connection, "contracts"), 1);

        // While down, 5 and 6 were replaced.
        let mut forked = FakeChain::new(6, 0xa0);
        forked.branch(5, 7, 0xb0);
        both(forked, None).await;

        let expected = chain_through(7, 5);
        assert_eq!(
            heights(&connection),
            expected,
            "the database holds one chain"
        );
        assert_eq!(
            count(&connection, "contracts"),
            0,
            "the pool went with its orphaned block"
        );
        assert_eq!(
            count(&connection, "reorgs"),
            2,
            "each orphaned block the database held is retracted as ingest reaches it"
        );
        assert_eq!(
            ledger_of(&restored(&settings).await),
            expected,
            "the lake holds the same chain"
        );
    }

    /// One process lifetime of a database and a lake together, the database first, as
    /// [`run_once`] is for the database alone.
    #[cfg(feature = "delta")]
    async fn run_both(
        connection: &duckdb::Connection,
        settings: &sink::DeltaSettings,
        chain: FakeChain,
        start: Option<u64>,
    ) {
        use crate::sink::any_store::AnyStore;

        let database =
            DuckDbSink::connected(connection.try_clone().expect("handle"), schema(), "base")
                .await
                .expect("open");
        let lake = sink::DeltaSink::open(settings, schema(), "base")
            .await
            .expect("the lake opens");
        let catalog =
            Catalog::load(protocols(), &ChainId::new("base")).expect("shipped protocols load");
        // Each store drains on a runtime of its own, as in a deployment; unlike one, they
        // were opened on the test's runtime.
        let own = |opened: Opened<AnyStore>| Opened {
            runtime: StoreRuntime::dedicated(opened.backend).expect("a runtime"),
            ..opened
        };
        let stores = vec![
            own(every_table(
                Backend::DuckDb,
                AnyStore::DuckDb(Box::new(database)),
                Batching::eager(500),
            )),
            own(every_table(
                Backend::Delta,
                AnyStore::Delta(Box::new(lake)),
                settings.batching(),
            )),
        ];
        let result = Box::pin(
            Pipeline::new(chain, start, None, catalog).run_with_stores(stores, Stop::never()),
        )
        .await;
        ran_to_head(&result);
    }

    /// The database ahead of the lake on the chain that stayed canonical, as every
    /// deploy leaves them: the restart deletes nothing from the database and writes
    /// nothing again into it. A value changed by hand in a held block's row survives,
    /// which neither a delete nor an upsert of the block would let it.
    #[cfg(feature = "delta")]
    #[tokio::test]
    async fn a_store_ahead_on_the_canonical_chain_keeps_its_rows() {
        let (_dir, settings) = lake("ahead", r#"["blocks"]"#);
        let connection = duckdb::Connection::open_in_memory().expect("open in-memory DuckDB");
        let pool = Address::from([0xd0; 20]);
        let chain = || {
            let mut chain = FakeChain::new(6, 0xa0);
            chain.log(6, pool_created(pool, B256::ZERO));
            chain
        };

        run_both(&connection, &settings, FakeChain::new(4, 0xa0), Some(1)).await;
        // The database alone runs on to 6, discovering a pool there.
        run_once(&connection, chain(), None).await;
        connection
            .execute_batch("UPDATE base.contracts SET name = 'kept'")
            .expect("mark the held row");

        let mut grown = chain();
        grown.branch(7, 7, 0xa0);
        run_both(&connection, &settings, grown, None).await;

        let name: String = connection
            .query_row("SELECT name FROM base.contracts", [], |row| row.get(0))
            .expect("the pool is still stored");
        assert_eq!(
            name, "kept",
            "the held block was neither deleted nor rewritten"
        );
        assert_eq!(count(&connection, "reorgs"), 0);
        let expected = chain_through(7, u64::MAX);
        assert_eq!(
            heights(&connection),
            expected,
            "the database holds one chain"
        );
        let lake = restored(&settings).await;
        assert_eq!(
            ledger_of(&lake),
            expected,
            "the lake caught up to the same chain"
        );
        assert_eq!(
            lake.contracts,
            [stored_pool(pool, block_hash(0xa0, 6))],
            "the lake discovered the pool once"
        );
    }
}
