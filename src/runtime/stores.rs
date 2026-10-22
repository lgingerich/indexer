//! What a run that stores adds: opening the stores, bringing them to one block, and
//! running ingest into them. A build without a store has none of it, so the whole module
//! is behind the `store` feature rather than each item in it.

use std::collections::HashSet;
use std::sync::Arc;

use tracing::{Instrument as _, info, info_span};

use crate::config::Settings;
use crate::decode::{Decoder, DecodingSink};
use crate::ingest::pipeline::{Ledger, Machine, PipelineError};
use crate::ingest::source::BlockSource;
use crate::sink::any_store::AnyStore;
use crate::sink::channel::Batching;
use crate::sink::resume::Opened;
use crate::sink::route::{Backend, Router};
use crate::sink::stop::Stop;
use crate::sink::store_runtime::StoreRuntime;
use crate::sink::table::{Schema, TableId};
use crate::sink::{self, SinkError, StoredContract};
use crate::wire::envelope::ChainId;

use super::{Pipeline, Routes, RuntimeError};

impl<B: BlockSource + 'static> Pipeline<B> {
    /// Opens every store the settings name, each with the tables `routes` sends it and on a
    /// runtime of its own (see `crate::sink::store_runtime`). Opened before ingest
    /// starts, so a bad path or connection fails at startup rather than after the first
    /// block.
    pub(super) async fn open(
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
        // A stop while resuming drops it part-way; see "Shutdown" in the module docs.
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
        // A stop drops ingest, a crash to it; the stores hear the stop in their drains.
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

impl Routes {
    /// Whether the run prints rather than stores. The settings never name stdout beside a
    /// store, so a run does one or the other.
    pub(super) fn prints(&self) -> bool {
        self.0
            .iter()
            .any(|(backend, _)| *backend == Backend::Stdout)
    }
}

/// Adds the contracts a previous run discovered, as the stores read them back.
fn restore(mut decoder: Decoder, chain: &ChainId, stored: Vec<StoredContract>) -> Decoder {
    let restored = decoder.restore(stored);
    info!(%chain, restored, contracts = decoder.contracts(), "discovered contracts restored");
    decoder
}

/// The accepted blocks a previous run committed, as ingest takes them.
pub(super) fn ledger(chain: &ChainId, stored: Ledger) -> Ledger {
    if let (Some(oldest), Some(tip)) = (stored.blocks.first(), stored.blocks.last()) {
        info!(%chain, from = oldest.height, tip = tip.height, hash = %tip.hash,
            "accepted blocks restored");
    } else {
        info!(%chain, "no accepted blocks stored; starting fresh");
    }
    stored
}

/// The error for `store` failing to open.
fn opening(store: Backend) -> impl FnOnce(sink::StoreError) -> RuntimeError {
    move |source| RuntimeError::OpenStore { store, source }
}

/// How each store's drain ended, as its task returned it.
pub(super) type Stopped = (
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
