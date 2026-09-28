//! The bus boundary: where events go, and where they come from.
//!
//! [`EventSink`] is the seam between publishing and the rest of the system, and
//! [`EnvelopeSource`] is its mirror for a consumer. Both sit on the same bus, so a
//! process that decodes is a consumer of one topic and a producer of another.
//!
//! Both speak [`Envelope`]. This crate also owns the store's runtime, because the sink
//! and the process that drives it only make sense together.
//!
//! A sink is a dumb serializing boundary: it renders the [`Envelope`] as-is and knows
//! nothing about chains. The schema version is a field on the envelope, not a property
//! of a sink's framing — see [`crate::wire::envelope::SCHEMA_VERSION`] for why it is not a
//! broker header.
//!
//! Sinks and sources do not own their transport. A runtime builds and tunes the engine
//! client (a `librdkafka` producer or consumer, a `DuckDB` connection) and injects it,
//! so the crate stays a library and the runtime owns connection pools, group ids,
//! timeouts, and callbacks.

#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod stdout;

#[cfg(feature = "duckdb")]
pub use duckdb::DuckDbSink;
#[cfg(feature = "kafka")]
pub use kafka::{KafkaSink, KafkaSource};
pub use stdout::StdoutJsonSink;

use crate::wire::envelope::Envelope;

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

/// Receives events in per-chain sequence order.
///
/// The consumer side of the bus, and the mirror of [`EventSink`]. A stage that
/// transforms the stream — decode today, stream processing next — is a source on one
/// topic and a sink on another, which is why both seams live at this boundary rather
/// than one here and one in each stage.
///
/// Returning an owned [`Envelope`] rather than a borrowed one is deliberate: the item
/// outlives the broker's buffer, so a compiler-enforced copy keeps a transform from
/// holding a view into a consumer's fetch queue across an await.
///
/// # Checkpointing
///
/// Offsets are the checkpoint, and committing them is deliberately *not* part of this
/// trait. A consumer's correct commit point is a property of how it delivers — after a
/// flush, after a batch, never — so a stage owning its runtime owns that decision.
/// A source that committed per record would make at-least-once delivery unachievable.
pub trait EnvelopeSource: Send {
    /// The next envelope, or `None` when the stream has ended.
    ///
    /// # Errors
    ///
    /// Returns an error when the payload cannot be read: a broker failure, or bytes
    /// that are not an envelope. The caller stops rather than skipping, since skipping
    /// a record the source has already advanced past is data loss.
    fn next(&mut self) -> impl Future<Output = anyhow::Result<Option<Envelope>>> + Send;
}
