//! Any store a build can open, as one type, so a run can hold several kinds at once.
//!
//! Each store's drain runs on its own task, so nothing calls a store through this type
//! on the hot path but the drain itself, which `match`es once per call.

use std::sync::Arc;

use crate::config::StoreSettings;
#[cfg(feature = "delta")]
use crate::sink::DeltaSink;
#[cfg(feature = "duckdb")]
use crate::sink::DuckDbSink;
#[cfg(feature = "postgres")]
use crate::sink::PostgresSink;
use crate::sink::table::Schema;
use crate::sink::{EnvelopeSink, Restored, SinkError, Store, StoreError};
use crate::wire::envelope::{BlockMeta, Envelope};

/// One opened store, of whichever backend the settings named.
///
/// Every variant is boxed: the stores differ in size by hundreds of bytes, so whichever
/// is the largest in a build would otherwise set the size of all of them, and which one
/// that is changes with the features. A store is opened once per run, so the box costs
/// nothing that matters.
#[derive(Debug)]
pub(crate) enum AnyStore {
    /// An embedded `DuckDB` database.
    #[cfg(feature = "duckdb")]
    DuckDb(Box<DuckDbSink>),
    /// A remote `PostgreSQL` database.
    #[cfg(feature = "postgres")]
    Postgres(Box<PostgresSink>),
    /// Delta Lake tables on object storage.
    #[cfg(feature = "delta")]
    Delta(Box<DeltaSink>),
}

/// Calls `$call` on whichever store `$store` holds.
macro_rules! each {
    ($store:expr, $sink:ident => $call:expr) => {
        match $store {
            #[cfg(feature = "duckdb")]
            AnyStore::DuckDb($sink) => $call,
            #[cfg(feature = "postgres")]
            AnyStore::Postgres($sink) => $call,
            #[cfg(feature = "delta")]
            AnyStore::Delta($sink) => $call,
        }
    };
}

impl AnyStore {
    /// Opens the store `settings` describe, of `schema`'s tables, for `chain`.
    ///
    /// # Errors
    ///
    /// Returns what the backend's own open returns.
    pub(crate) async fn open(
        settings: StoreSettings,
        schema: Arc<Schema>,
        chain: &str,
    ) -> Result<Self, StoreError> {
        Ok(match settings {
            #[cfg(feature = "duckdb")]
            StoreSettings::DuckDb(duckdb) => {
                Self::DuckDb(Box::new(DuckDbSink::open(&duckdb, schema, chain).await?))
            }
            #[cfg(feature = "postgres")]
            StoreSettings::Postgres(postgres) => Self::Postgres(Box::new(
                PostgresSink::open(&postgres, schema, chain).await?,
            )),
            #[cfg(feature = "delta")]
            StoreSettings::Delta(delta) => {
                Self::Delta(Box::new(DeltaSink::open(&delta, schema, chain).await?))
            }
        })
    }
}

impl EnvelopeSink for AnyStore {
    async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
        each!(self, sink => sink.publish(envelope).await)
    }

    async fn flush(&mut self) -> Result<(), SinkError> {
        each!(self, sink => sink.flush().await)
    }

    async fn finish(&mut self) -> Result<(), SinkError> {
        each!(self, sink => sink.finish().await)
    }

    fn buffered_bytes(&self) -> usize {
        each!(self, sink => sink.buffered_bytes())
    }
}

impl Store for AnyStore {
    async fn restore(&mut self) -> Result<Restored, StoreError> {
        each!(self, sink => sink.restore().await)
    }

    fn reconcile(&mut self, held: &[BlockMeta]) {
        each!(self, sink => sink.reconcile(held));
    }

    async fn rewind(&mut self, height: u64) -> Result<(), StoreError> {
        each!(self, sink => sink.rewind(height).await)
    }
}
