//! The egress boundary: where events go.
//!
//! [`EventSink`] is the seam between publishing and the rest of the system.
//!
//! The sink is a dumb serializing boundary: it renders the [`Envelope`] as-is and
//! knows nothing about chains. A message-format version would belong here once a
//! broker keeps history a consumer reads across a shape change — Redis/Kafka-style
//! topics are the first case — because that is the only setting where one consumer
//! sees two shapes interleaved. Add it at v1 when the broker lands, with a written
//! compatibility policy (or a schema registry), before it produces anything.
//!
//! Sinks do not own their transport. A runtime builds and tunes the engine client
//! (a `librdkafka` producer, a `DuckDB` connection) and injects it, so the crate
//! stays a library and the runtime owns connection pools, timeouts, and callbacks.

use crate::envelope::Envelope;

#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod stdout;

#[cfg(feature = "duckdb")]
pub use duckdb::DuckDbSink;
#[cfg(feature = "kafka")]
pub use kafka::KafkaSink;
pub use stdout::StdoutJsonSink;

/// Receives events in per-chain sequence order.
///
/// The pipeline drives the sink through an exclusive borrow, so a sink may keep
/// batch state across calls — a rendered row, an open appender, an in-flight
/// producer window — instead of paying the engine's per-record cost on every
/// event. [`EventSink::flush`] is the batch and durability point; the pipeline
/// calls it once per processed block.
///
/// A slow `publish` or `flush` applies backpressure to the whole pipeline. That is
/// safe only because consumers read from their own queues downstream; until the
/// bus lands, a slow sink is a slow indexer, not a silently lossy one.
pub trait EventSink: Send {
    /// Publishes one event.
    ///
    /// May buffer; the event is not durable until [`EventSink::flush`] succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if the event cannot be accepted. The caller stops the
    /// pipeline rather than skipping the event.
    fn publish(&mut self, envelope: &Envelope) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Makes everything published since the last flush durable, as one batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffered events cannot be delivered. The caller
    /// stops the pipeline rather than continuing past a lost batch.
    fn flush(&mut self) -> impl Future<Output = anyhow::Result<()>> + Send {
        async { Ok(()) }
    }
}
