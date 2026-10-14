//! Any store a build can open, as one type, so a run can hold several kinds at once.
//!
//! Each store's drain runs on its own task, so nothing calls a store through this type
//! on the hot path but the drain itself, which `match`es once per call.

#[cfg(feature = "delta")]
use crate::sink::DeltaSink;
#[cfg(feature = "duckdb")]
use crate::sink::DuckDbSink;
#[cfg(feature = "postgres")]
use crate::sink::PostgresSink;
use crate::sink::{EnvelopeSink, Restored, SinkError, Store, StoreError};
use crate::wire::envelope::Envelope;

/// One opened store, of whichever backend the settings named.
#[derive(Debug)]
pub(crate) enum AnyStore {
    /// An embedded `DuckDB` database.
    #[cfg(feature = "duckdb")]
    DuckDb(DuckDbSink),
    /// A remote `PostgreSQL` database.
    #[cfg(feature = "postgres")]
    Postgres(PostgresSink),
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

    async fn rewind(&mut self, height: u64) -> Result<(), StoreError> {
        each!(self, sink => sink.rewind(height).await)
    }
}
