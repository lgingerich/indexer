//! What a run that stores adds: opening the stores, bringing them to one block, and
//! running ingest into them. A build without a store has none of it, so the whole module
//! is behind the `store` feature rather than each item in it.

use std::sync::Arc;

use futures_util::future::try_join_all;
use tracing::{Instrument as _, info, info_span, warn};

use crate::decode::{Catalog, Decoder, DecodingSink, Matching, MatchingLinks, SharedKept};
use crate::ingest::pipeline::{Machine, PipelineError};
use crate::ingest::source::BlockSource;
use crate::sink::any_store::AnyStore;
use crate::sink::resume::Opened;
use crate::sink::route::{Router, StoreBackend};
use crate::sink::stop::Stop;
use crate::sink::store_runtime::StoreRuntime;
use crate::sink::{self, SinkError, StoredContract};
use crate::window::Ledger;
use crate::wire::envelope::ChainId;

use super::{Pipeline, RuntimeError, StoreRoute};

/// How many batches of handed-over contracts wait for the decode stage, which takes them
/// at each block's marker. The watch sends a batch a read, and waits when this many are
/// queued.
#[cfg(feature = "delta")]
const HANDED_QUEUE: usize = 16;

impl<B: BlockSource + 'static> Pipeline<B> {
    /// Opens every store `routes` names, each with the tables routed to it and on a
    /// runtime of its own (see `crate::sink::store_runtime`), so what opening spawns — a
    /// connection's task, the lake's checkpoints and maintenance — runs there too. Opened
    /// before ingest starts, so a bad path or connection fails at startup rather than
    /// after the first block.
    pub(super) async fn open(
        &self,
        routes: Vec<StoreRoute>,
    ) -> Result<Vec<Opened<AnyStore>>, RuntimeError> {
        let chain = self.source.chain().as_str().to_owned();
        // Each store opens on its own runtime, so they open at once rather than each
        // waiting out the last.
        let opens = routes.into_iter().map(|StoreRoute { settings, tables }| {
            let backend = settings.backend();
            let batching = settings.batching();
            let schema = Arc::new(self.schema.select(&tables.tables));
            let chain = chain.clone();
            async move {
                let runtime = StoreRuntime::dedicated(backend).map_err(|source| {
                    RuntimeError::StoreRuntime {
                        store: backend,
                        source,
                    }
                })?;
                let store = runtime
                    .run(async move { AnyStore::open(settings, schema, &chain).await })
                    .await
                    .map_err(opening(backend))?;
                Ok(Opened {
                    backend,
                    store,
                    batching,
                    tables,
                    runtime,
                })
            }
        });
        try_join_all(opens).await
    }

    /// Runs against open `stores`: brings them to one block, then ingests into each the
    /// tables routed to it, every store committing as its batching says, until ingest
    /// or a store stops or `stop` is requested.
    ///
    /// # Errors
    ///
    /// As [`Pipeline::run`], less opening the stores.
    pub(crate) async fn run_with_stores<S: sink::Store>(
        self,
        stores: Vec<Opened<S>>,
        mut stop: Stop,
    ) -> Result<(), RuntimeError> {
        let Self {
            source,
            start_block,
            stop_block,
            catalog,
            schema,
            #[cfg(feature = "delta")]
            watch,
            pairs,
        } = self;
        let chain = source.chain().clone();
        // A stop while resuming drops it part-way; see "Shutdown" in the module docs.
        let (stores, resumed) = tokio::select! {
            biased;
            () = stop.wait() => return Ok(()),
            resumed = sink::resume::resume(stores) => resumed?,
        };
        let matching = pairs.as_ref().and_then(|pairs| {
            let matching = Matching::new(&catalog, &chain, pairs.lookback);
            if matching.is_none() {
                warn!(%chain, "[decode.pairs] is set, but the manifests have no Metric factory \
                    and protocol to match on this chain; nothing is matched");
            }
            matching
        });
        let kept = matching
            .as_ref()
            .map_or_else(SharedKept::default, Matching::kept);
        let decoder = restore(catalog, &chain, resumed.contracts);
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

        let decoding = DecodingSink::new(decoder, Router::new(Arc::clone(&schema), kept, sinks))
            .holding(resumed.held);
        // The matching's fetches and fills, each on a task of its own, ended with ingest.
        let (decoding, matching) = match (matching, pairs) {
            (Some(matching), Some(pairs)) => {
                let (tasks, links) = matching_tasks(pairs, &chain)?;
                (
                    decoding.with_matching(matching, resumed.matched, links),
                    Some(tasks),
                )
            }
            _ => (decoding, None),
        };
        // The watch on the lake's backfills, on a task of its own so ingest never waits
        // on it, and ended with ingest.
        #[cfg(feature = "delta")]
        let (decoding, watching) = match watch {
            Some(watch) => {
                let (handed, handing) = tokio::sync::mpsc::channel(HANDED_QUEUE);
                let watching = tokio::spawn(watch.run(schema, chain.clone(), handed));
                (
                    decoding.with_handoffs(resumed.handoffs, handing),
                    Some(watching),
                )
            }
            None => (decoding, None),
        };
        #[cfg(not(feature = "delta"))]
        let _ = (schema, resumed.handoffs);
        let ingest = Machine::new(source, decoding, ledger).run(start_block, stop_block);
        // A stop drops ingest, a crash to it; the stores hear the stop in their drains.
        let ingest = tokio::select! {
            biased;
            () = stop.wait() => Ok(()),
            ingest = ingest => ingest,
        };
        #[cfg(feature = "delta")]
        if let Some(watching) = watching {
            watching.abort();
        }
        if let Some(matching) = matching {
            matching.end().await;
        }

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

/// How long the end of a run waits for a fill under way to stop at its next commit and
/// let go of the lake's lease, before cutting it off; one cut off leaves the lease to go
/// stale.
#[cfg(feature = "delta")]
const FILL_STOP: std::time::Duration = std::time::Duration::from_mins(1);

/// The matching's tasks: the searches, and the fills, on a runtime of their own.
struct MatchingTasks {
    lookup: tokio::task::JoinHandle<()>,
    #[cfg(feature = "delta")]
    fill: Option<(
        tokio::task::JoinHandle<()>,
        StoreRuntime,
        tokio::sync::watch::Sender<bool>,
    )>,
}

impl MatchingTasks {
    /// Ends the tasks: the searches at once, a fill once it stops at its next commit and
    /// lets go of the lake's lease, or after [`FILL_STOP`].
    async fn end(self) {
        self.lookup.abort();
        #[cfg(feature = "delta")]
        if let Some((fill, _runtime, halt)) = self.fill {
            let _ = halt.send(true);
            let abort = fill.abort_handle();
            if tokio::time::timeout(FILL_STOP, fill).await.is_err() {
                abort.abort();
            }
        }
    }
}

/// Starts the tasks that fetch matched pools' creation logs and fill their history, as
/// `pairs` provides for, and returns them with the decode stage's links to them. A run
/// without a lake starts no fill, and what is sent to fill is dropped. A fill reads the
/// lake and decodes on a runtime of its own, so ingest's never waits behind it.
fn matching_tasks(
    pairs: super::pairs::Pairs,
    chain: &ChainId,
) -> Result<(MatchingTasks, MatchingLinks), RuntimeError> {
    use tokio::sync::mpsc;
    let (queries, asked) = mpsc::channel(super::pairs::QUEUE);
    let (answer, found) = mpsc::channel(super::pairs::QUEUE);
    let (fills, to_fill) = mpsc::channel(super::pairs::QUEUE);
    let (done, filled) = mpsc::channel(super::pairs::QUEUE);
    let lookup = tokio::spawn(super::pairs::lookup(
        pairs.search,
        chain.clone(),
        asked,
        answer,
    ));
    #[cfg(feature = "delta")]
    let fill = if let Some(fill) = pairs.fill {
        let runtime = StoreRuntime::dedicated(StoreBackend::Delta).map_err(|source| {
            RuntimeError::StoreRuntime {
                store: StoreBackend::Delta,
                source,
            }
        })?;
        let (halt, halted) = Stop::new();
        let task = runtime.spawn(fill.run(to_fill, done, halted));
        Some((task, runtime, halt))
    } else {
        warn!(%chain, "no lake; matched pools' history is not filled");
        None
    };
    #[cfg(not(feature = "delta"))]
    {
        warn!(%chain, "no lake; matched pools' history is not filled");
        drop((to_fill, done));
    }
    let tasks = MatchingTasks {
        lookup,
        #[cfg(feature = "delta")]
        fill,
    };
    let links = MatchingLinks {
        queries,
        found,
        fills,
        filled,
    };
    Ok((tasks, links))
}

/// The decoder over `catalog` with the contracts a previous run discovered, as the
/// stores read them back.
fn restore(catalog: Catalog, chain: &ChainId, stored: Vec<StoredContract>) -> Decoder {
    let decoder = Decoder::new(catalog, stored);
    info!(%chain, restored = decoder.discovered(), contracts = decoder.contracts(),
        "discovered contracts restored");
    decoder
}

/// The accepted blocks a previous run committed, as ingest takes them.
pub(super) fn ledger(chain: &ChainId, stored: Ledger) -> Ledger {
    if let (Some(oldest), Some(tip)) = (stored.blocks().first(), stored.blocks().last()) {
        info!(%chain, from = oldest.height, tip = tip.height, hash = %tip.hash,
            "accepted blocks restored");
    } else {
        info!(%chain, "no accepted blocks stored; starting fresh");
    }
    stored
}

/// The error for `store` failing to open.
fn opening(store: StoreBackend) -> impl FnOnce(sink::StoreError) -> RuntimeError {
    move |source| RuntimeError::OpenStore { store, source }
}

/// How each store's drain ended, as its task returned it.
pub(super) type Stopped = (
    StoreBackend,
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
pub(super) fn finish(
    stopped: Vec<Stopped>,
    ingest: Result<(), PipelineError>,
) -> Result<(), RuntimeError> {
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
