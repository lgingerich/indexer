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
/// The pipeline publishes inside an exclusive borrow of the sink, so a slow sink
/// applies backpressure to the whole pipeline. That is safe only because
/// consumers read from their own queues downstream; until the bus lands, a slow
/// sink is a slow indexer, not a silently lossy one.
pub trait EventSink: Send + Sync {
    /// Publishes one event.
    ///
    /// # Errors
    ///
    /// Returns an error if the event cannot be delivered. The caller stops the
    /// pipeline rather than skipping the event.
    fn publish(&self, envelope: &Envelope) -> impl Future<Output = anyhow::Result<()>> + Send;
}
