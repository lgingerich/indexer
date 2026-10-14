//! The pipeline the settings describe, assembled and run.
//!
//! The shape is fixed — `ingest → decode → storage` — so what varies is a few choices the
//! settings file already states. This module reads them and builds the pipeline, so the
//! binary is "load the settings, run it" rather than a place that names every layer.
//!
//! # One ingest task, a task per store
//!
//! ```text
//! task 1:  ingest ─▶ DecodingSink ─▶ Router ─┬─ ChannelSink ══╗ bounded channel of blocks
//!                    (direct calls, no queue) └─ ChannelSink ══╬═══════╗
//! task 2:  DeltaSink    ◀─ ChannelReceiver::drain ◀════════════╝       ║
//! task 3:  PostgresSink ◀─ ChannelReceiver::drain ◀════════════════════╝
//! ```
//!
//! Ingest and decode are one task because decoding a block is far cheaper than the block
//! time and needs no decoupling. Each store is its own task because a store stalls, and
//! the channel in front of it is what keeps a stall from stopping ingest; see
//! `crate::sink::channel`. A store gets its own task — spawned, not merely polled
//! alongside ingest — because its writes block, and a blocked poll would stall ingest
//! anyway. The router hands each store the envelopes of the tables it writes; see
//! [`crate::sink::route`].
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
use crate::ingest::source::{AnySource, BlockSource, EvmSource, HyperSyncSource, SourceError};
#[cfg(feature = "store")]
use crate::sink;
#[cfg(feature = "store")]
use crate::sink::any_store::AnyStore;
#[cfg(any(feature = "duckdb", feature = "postgres"))]
use crate::sink::channel::Batching;
#[cfg(feature = "store")]
use crate::sink::resume::{Opened, ResumeError};
use crate::sink::route::{self, Backend, RouteError, Router};
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
    /// the stores' history.
    ///
    /// A height above the head is counted too, though the chain would grow past it: a
    /// height the chain has not reached is a mistyped one far more often than an early
    /// one. Everything else — an endpoint or store that failed, a reorg — may pass, so
    /// it is not.
    #[must_use]
    pub fn is_config(&self) -> bool {
        match self {
            Self::Settings(_) | Self::Catalog(_) | Self::Route { .. } => true,
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
    /// Each sink the settings name and the tables it writes. Empty for a pipeline built
    /// with [`Pipeline::new`], whose caller hands its stores their tables directly.
    routes: Vec<(Backend, HashSet<TableId>)>,
}

impl Pipeline {
    /// Assembles the pipeline the settings describe.
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
    pub(crate) fn from_settings(settings: &Settings) -> Result<Self, RuntimeError> {
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
            )),
            IngestSource::HyperSync(hypersync) => {
                info!(url = %hypersync.url, "backfilling from HyperSync; the run stops at the reorg window");
                AnySource::HyperSync(HyperSyncSource::new(
                    chain,
                    &hypersync.url,
                    hypersync.token.expose(),
                    datasets,
                )?)
            }
        };
        Ok(Self {
            routes,
            ..Self::new(
                source,
                settings.ingest.start_block,
                settings.ingest.stop_block,
                catalog,
            )
        })
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
            routes: Vec::new(),
        }
    }

    /// Runs until ingest or storage stops.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::OpenStore`] when a store cannot be opened,
    /// [`RuntimeError::Resume`] when the stores cannot be brought to one block, and
    /// [`RuntimeError::Ingest`] or [`RuntimeError::Storage`] when a part fails. Each
    /// carries that part's own error, because "storage stopped" says nothing about why.
    pub(crate) async fn run(self, settings: &Settings) -> Result<(), RuntimeError> {
        // The settings never name stdout beside a store, so a run either prints or
        // stores.
        #[cfg(feature = "store")]
        if self
            .routes
            .iter()
            .all(|(backend, _)| *backend != Backend::Stdout)
        {
            let stores = self.open(settings).await?;
            return Box::pin(self.run_with_stores(stores)).await;
        }
        #[cfg(not(feature = "store"))]
        let _ = settings;
        self.print().await
    }

    /// Prints the tables `[sink.stdout]` names. No store, so no tables and nothing to
    /// restore: a run starts fresh from the manifests' seeds.
    async fn print(self) -> Result<(), RuntimeError> {
        let Self {
            source,
            start_block,
            stop_block,
            decoder,
            schema,
            routes,
        } = self;
        let sinks = routes
            .into_iter()
            .map(|(_, tables)| (StdoutJsonSink::new(), tables))
            .collect();
        let sink = DecodingSink::new(decoder, Router::new(schema, sinks));
        Machine::new(source, sink, Vec::new())
            .run(start_block, stop_block)
            .await?;
        Ok(())
    }

    /// Opens every store the settings name, each with the tables routed to it. Opened
    /// before ingest starts, so a bad path or connection fails at startup rather than
    /// after the first block.
    #[cfg(feature = "store")]
    async fn open(&self, settings: &Settings) -> Result<Vec<Opened<AnyStore>>, RuntimeError> {
        let chain = settings.ingest.chain.as_str();
        let mut stores = Vec::new();
        #[cfg(feature = "duckdb")]
        if let Some(duckdb) = &settings.sink.duckdb {
            let (schema, tables) = self.route(Backend::DuckDb);
            let store = sink::DuckDbSink::open(duckdb, schema, chain)
                .await
                .map_err(opening(Backend::DuckDb))?;
            stores.push(Opened {
                backend: Backend::DuckDb,
                store: AnyStore::DuckDb(store),
                batching: Batching::eager(duckdb.batch_records),
                tables,
            });
        }
        #[cfg(feature = "postgres")]
        if let Some(postgres) = &settings.sink.postgres {
            let (schema, tables) = self.route(Backend::Postgres);
            let store = sink::PostgresSink::open(postgres, schema, chain)
                .await
                .map_err(opening(Backend::Postgres))?;
            stores.push(Opened {
                backend: Backend::Postgres,
                store: AnyStore::Postgres(store),
                batching: Batching::eager(postgres.batch_records),
                tables,
            });
        }
        #[cfg(feature = "delta")]
        if let Some(delta) = &settings.sink.delta {
            let (schema, tables) = self.route(Backend::Delta);
            let store = sink::DeltaSink::open(delta, schema, chain)
                .await
                .map_err(opening(Backend::Delta))?;
            stores.push(Opened {
                backend: Backend::Delta,
                store: AnyStore::Delta(Box::new(store)),
                batching: delta.batching(),
                tables,
            });
        }
        Ok(stores)
    }

    /// The tables routed to `backend`, and the schema of them a store opens with.
    #[cfg(feature = "store")]
    fn route(&self, backend: Backend) -> (Arc<Schema>, HashSet<TableId>) {
        let tables = self
            .routes
            .iter()
            .find(|(named, _)| *named == backend)
            .map(|(_, tables)| tables.clone())
            .unwrap_or_default();
        (Arc::new(self.schema.select(&tables)), tables)
    }

    /// Runs against open `stores`: brings them to one block, then ingests into each the
    /// tables routed to it, every store committing as its batching says.
    ///
    /// # Errors
    ///
    /// As [`Pipeline::run`], less opening the stores.
    #[cfg(feature = "store")]
    pub(crate) async fn run_with_stores<S: sink::Store>(
        self,
        mut stores: Vec<Opened<S>>,
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
        let resumed = sink::resume::resume(&mut stores).await?;
        let decoder = restore(decoder, &chain, resumed.contracts);
        let ledger = ledger(&chain, resumed.ledger);

        let mut sinks = Vec::with_capacity(stores.len());
        let mut drains = Vec::with_capacity(stores.len());
        for Opened {
            backend,
            mut store,
            batching,
            tables,
        } in stores
        {
            let (sender, receiver) = sink::channel::ChannelSink::new(backend);
            // `ponytail:` the store's writes block, so this holds one runtime worker for
            // the length of each flush. Fine on the multi-threaded runtime the binary
            // uses; a dedicated blocking thread is the upgrade if a store gets slow
            // enough to starve other tasks.
            let drain = async move { receiver.drain(&mut store, batching).await }
                .instrument(info_span!("storage", store = %backend));
            drains.push((backend, tokio::spawn(drain)));
            sinks.push((sender, tables));
        }

        let ingest = Machine::new(
            source,
            DecodingSink::new(decoder, Router::new(schema, sinks)),
            ledger,
        )
        .run(start_block, stop_block)
        .await;

        // Ingest's halves of the channels are gone by now, so each store drains what is
        // queued and ends. The join order is [`finish`].
        let mut stopped = Vec::with_capacity(drains.len());
        for (backend, drain) in drains {
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
    let settings = Settings::from_file(path)?;
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
    Pipeline::from_settings(&settings)?.run(&settings).await
}

#[cfg(all(test, feature = "duckdb"))]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are allowed
// them per the repository test style, since a failed expectation there means the fixture
// or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::str::FromStr as _;

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
    use crate::sink::table::Schema;
    use crate::sink::{self, DuckDbSink, EnvelopeSink as _, SinkError, Store as _};
    use crate::sink::{Operation, StoreError};
    use crate::wire::envelope::{BlockMeta, ChainId, Envelope, Event, Log, Reorg, StoredContract};

    use super::{Pipeline, RuntimeError, finish, ledger};

    /// The shipped protocols, as an absolute path so [`Settings::from_str`] carries no
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

    /// The shipped protocols load, so the example settings a reader copies stay runnable.
    #[test]
    fn the_shipped_protocols_assemble() {
        let decode = format!("\n[decode]\nprotocols = {:?}\n", protocols());
        let settings = Settings::from_str(&format!("{SETTINGS}{decode}")).expect("settings parse");
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
        let storage =
            tokio::spawn(async move { receiver.drain(&mut store, Batching::eager(500)).await });

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
        // The decoded rows key back to the raw logs they came from.
        let orphans: i64 = reader
            .query_row(
                "SELECT count(*) FROM base.decoded_logs d WHERE NOT EXISTS \
                 (SELECT 1 FROM base.logs l WHERE starts_with(d.dedupe_key, l.dedupe_key))",
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
        assert_eq!(
            stored,
            [StoredContract {
                protocol: "uniswap_v3".to_owned(),
                name: "UniswapV3Pool".to_owned(),
                address: pool,
                block_hash: creating,
            }]
        );
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
            .run_with_stores(vec![every_table(
                Backend::DuckDb,
                store,
                Batching::eager(500),
            )])
            .await;
        assert!(
            matches!(
                result,
                Err(RuntimeError::Ingest(PipelineError::SubscriptionClosed))
            ),
            "{result:?}"
        );
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
            heights(&connection).len(),
            4,
            "every block, empty ones included"
        );

        let mut grown = FakeChain::new(6, 0xa0);
        grown.log(5, pool_created(pool, B256::ZERO));
        run_once(&connection, grown, None).await;
        let ledger: Vec<u64> = heights(&connection).iter().map(|(h, _)| *h).collect();
        assert_eq!(ledger, [1, 2, 3, 4, 5, 6], "resumed after 4 with no gap");
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
            heights(&connection).len(),
            7,
            "the orphaned blocks left the ledger"
        );
        let mut store =
            DuckDbSink::connected(connection.try_clone().expect("handle"), schema(), "base")
                .await
                .expect("open");
        let restored = store.restore().await.expect("restore");
        assert!(restored.contracts.is_empty());
        let canonical = restored.ledger;
        assert_eq!(
            canonical
                .iter()
                .map(|block| block.height)
                .collect::<Vec<_>>(),
            [1, 2, 3, 4, 5, 6, 7]
        );
        assert!(
            canonical
                .windows(2)
                .all(|pair| pair[1].parent_hash == pair[0].hash),
            "the restored ledger is one linked chain"
        );
        assert_eq!(canonical[6].hash, block_hash(0xb0, 7));
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
        assert!(matches!(
            machine.run(Some(5), None).await,
            Err(PipelineError::StartAboveHistory { start: 5, tip: 3 })
        ));
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
        let result = Box::pin(
            Pipeline::new(chain, start, None, catalog).run_with_stores(vec![every_table(
                Backend::Delta,
                store,
                settings.batching(),
            )]),
        )
        .await;
        assert!(
            matches!(
                result,
                Err(RuntimeError::Ingest(PipelineError::SubscriptionClosed))
            ),
            "{result:?}"
        );
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
        let dir = LakeDir(
            std::env::temp_dir().join(format!("indexer-runtime-lake-{}", std::process::id())),
        );
        let settings = sink::DeltaSettings {
            tables: serde_json::from_str(r#"["contract_events"]"#).expect("tables"),
            uri: dir.0.display().to_string(),
            commit_interval_secs: 3600,
            max_buffer_bytes: usize::MAX,
            maintenance: false,
            vacuum_retention_hours: 168,
            storage: std::collections::BTreeMap::new(),
        };
        let heights = |restored: &sink::Restored| -> Vec<u64> {
            restored.ledger.iter().map(|block| block.height).collect()
        };
        let pool = Address::from([0xd0; 20]);

        run_lake(&settings, FakeChain::new(4, 0xa0), Some(1)).await;
        assert_eq!(heights(&restored(&settings).await), [1, 2, 3, 4]);

        let mut grown = FakeChain::new(6, 0xa0);
        grown.log(5, pool_created(pool, B256::ZERO));
        run_lake(&settings, grown, None).await;
        let after = restored(&settings).await;
        assert_eq!(
            heights(&after),
            [1, 2, 3, 4, 5, 6],
            "resumed after 4 with no gap"
        );
        assert_eq!(after.contracts.len(), 1, "created while down, discovered");

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
        assert_eq!(heights(&after), [1, 2, 3, 4, 5, 6, 7]);
        assert!(
            after
                .ledger
                .windows(2)
                .all(|pair| pair[1].parent_hash == pair[0].hash),
            "the restored ledger is one linked chain"
        );
        assert_eq!(after.ledger[6].hash, block_hash(0xb0, 7));
    }

    /// Two stores, the database ahead of the lake when the process stops, and the chain
    /// forked below the database's tip while nothing ran. The restart rewinds the
    /// database to the block both hold, so neither keeps the orphaned branch and the lake
    /// is never handed a block it has; both end on the same canonical chain.
    #[cfg(feature = "delta")]
    #[tokio::test]
    async fn stores_at_different_heights_resume_from_the_block_they_share() {
        use crate::sink::any_store::AnyStore;

        let dir = LakeDir(
            std::env::temp_dir().join(format!("indexer-runtime-two-{}", std::process::id())),
        );
        let settings = sink::DeltaSettings {
            tables: serde_json::from_str(r#"["blocks"]"#).expect("tables"),
            uri: dir.0.display().to_string(),
            commit_interval_secs: 3600,
            max_buffer_bytes: usize::MAX,
            maintenance: false,
            vacuum_retention_hours: 168,
            storage: std::collections::BTreeMap::new(),
        };
        let connection = duckdb::Connection::open_in_memory().expect("open in-memory DuckDB");
        let pool = Address::from([0xd0; 20]);
        let both = async |chain: FakeChain, start: Option<u64>| {
            let database =
                DuckDbSink::connected(connection.try_clone().expect("handle"), schema(), "base")
                    .await
                    .expect("open");
            let lake = sink::DeltaSink::open(&settings, schema(), "base")
                .await
                .expect("the lake opens");
            let catalog =
                Catalog::load(protocols(), &ChainId::new("base")).expect("shipped protocols load");
            let stores = vec![
                every_table(
                    Backend::DuckDb,
                    AnyStore::DuckDb(database),
                    Batching::eager(500),
                ),
                every_table(
                    Backend::Delta,
                    AnyStore::Delta(Box::new(lake)),
                    settings.batching(),
                ),
            ];
            let result =
                Box::pin(Pipeline::new(chain, start, None, catalog).run_with_stores(stores)).await;
            assert!(
                matches!(
                    result,
                    Err(RuntimeError::Ingest(PipelineError::SubscriptionClosed))
                ),
                "{result:?}"
            );
        };

        both(FakeChain::new(4, 0xa0), Some(1)).await;

        // The database alone runs on: it holds 5 and 6, and a pool created at 6.
        let mut grown = FakeChain::new(6, 0xa0);
        grown.log(6, pool_created(pool, B256::ZERO));
        run_once(&connection, grown, None).await;
        assert_eq!(heights(&connection).len(), 6);
        assert_eq!(count(&connection, "contracts"), 1);

        // While down, 5 and 6 were replaced.
        let mut forked = FakeChain::new(6, 0xa0);
        forked.branch(5, 7, 0xb0);
        both(forked, None).await;

        let expected: Vec<(u64, String)> = (1..=7)
            .map(|height| {
                let tag = if height < 5 { 0xa0 } else { 0xb0 };
                (height, format!("{:#x}", block_hash(tag, height)))
            })
            .collect();
        assert_eq!(
            heights(&connection),
            expected,
            "the database holds one chain"
        );
        assert_eq!(
            count(&connection, "contracts"),
            0,
            "the pool went with its rewound block"
        );
        assert_eq!(
            count(&connection, "reorgs"),
            0,
            "a rewind retracts nothing from the chain"
        );
        let restored = restored(&settings).await;
        assert_eq!(
            restored
                .ledger
                .iter()
                .map(|block| (block.height, format!("{:#x}", block.hash)))
                .collect::<Vec<_>>(),
            expected,
            "the lake holds the same chain"
        );
    }
}
