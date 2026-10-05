//! Where envelopes go: the [`EnvelopeSink`] contract and the sinks that implement it.
//!
//! Ingest hands each envelope of a block to one sink and flushes at the block boundary.
//! [`DecodingSink`](crate::decode::DecodingSink) wraps a sink and adds decoded records to
//! what it forwards, so the layers compose by nesting rather than by a queue between
//! each pair:
//!
//! ```text
//! ingest ─▶ DecodingSink ─▶ ChannelSink ═ channel ═▶ ChannelReceiver::drain ─▶ DuckDbSink
//!           (same task, direct calls)                (own task, the store's writer)
//! ```
//!
//! - `channel` — the one hop that crosses tasks: a bounded in-process channel of
//!   blocks, from decode to storage. It is what lets a slow store stall without stalling
//!   ingest.
//! - [`duckdb`] — the store, an embedded `DuckDB` database.
//! - [`stdout`] — newline-delimited JSON, for watching the stream.
//!
//! Sinks do not own their engine: a sink takes a connection or opens one behind
//! [`duckdb::DuckDbSink::open`], and runs over a test double either way, so client
//! settings live in one place.

/// The bounded channel from decode to storage: the one hop that crosses tasks.
///
/// Behind the `duckdb` feature, because its only consumer is the store's writer in
/// [`runtime`](crate::runtime) — a `stdout` run has no store to stall, so it has no
/// second task and nothing for the channel to carry. A build without the engine does
/// not compile a queue it cannot fill.
#[cfg(feature = "duckdb")]
pub(crate) mod channel;
#[cfg(feature = "duckdb")]
pub mod duckdb;
pub mod stdout;

#[cfg(feature = "duckdb")]
pub use duckdb::{DuckDbSettings, DuckDbSink};
pub use stdout::{StdoutJsonSink, StdoutSettings};

use thiserror::Error;

use crate::wire::envelope::Envelope;

/// Receives envelopes in per-chain order, as the pipeline publishes them.
///
/// The driver holds the sink through an exclusive borrow, so it may buffer across calls
/// — a rendered row, an open appender, a block awaiting its send — instead of paying the
/// engine's per-record cost. [`flush`](EnvelopeSink::flush) is the batch boundary; the
/// ingest pipeline calls it once per block, so everything published between two flushes
/// is one block's worth. A slow sink applies backpressure to whatever drives it.
pub trait EnvelopeSink: Send {
    /// Accepts one envelope.
    ///
    /// Takes the envelope by value: each sink either keeps it (a channel buffer) or
    /// renders it, and none needs a copy, so ownership moves down the chain instead of
    /// cloning per hop. May buffer; nothing is durable until [`EnvelopeSink::flush`]
    /// succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if the envelope cannot be accepted. The caller stops rather
    /// than skipping it.
    fn publish(&mut self, envelope: Envelope)
    -> impl Future<Output = Result<(), SinkError>> + Send;

    /// Ends the batch: hands everything published since the last flush onward, as one.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffered envelopes cannot be delivered. The caller stops
    /// rather than continuing past a lost batch.
    fn flush(&mut self) -> impl Future<Output = Result<(), SinkError>> + Send {
        async { Ok(()) }
    }
}

/// Why a sink could not accept, render, or deliver an envelope.
///
/// The layer's own error, and the reason [`EnvelopeSink`] is typed rather than generic:
/// every sink that implements the trait has to say what can go wrong, so a caller can
/// branch on it instead of reading a string. The variants that matter are structural
/// rather than textual — a caller distinguishes a dead store from a failed HTTP status
/// from a malformed row by matching, not by formatting.
///
/// Leaf errors arrive through `#[from]`, so a sink propagates them with `?` rather than
/// wrapping them in a message: [`duckdb::StoreError`] and `serde_json`'s and
/// `std::io`'s own errors each name their cause better than this layer could.
#[derive(Debug, Error)]
pub enum SinkError {
    /// The store this sink writes to has stopped, so the batch cannot be delivered.
    ///
    /// Distinct from a *failed* store: nothing went wrong, the destination is simply
    /// gone. A channel sink reports this when the receiving half was dropped.
    #[error("storage has stopped, so the batch cannot be delivered")]
    StorageClosed,
    /// Rendering the envelope for a transport failed.
    #[error("serialize envelope: {0}")]
    Serialize(#[from] serde_json::Error),
    /// Writing the rendered envelope failed.
    #[error("write envelope: {0}")]
    Write(#[from] std::io::Error),
    /// A store rejected the write, or could not be opened.
    ///
    /// Transparent, so the store's own variants — the setting it refused, the path it
    /// could not open — survive to the caller instead of being flattened to a string.
    #[cfg(feature = "duckdb")]
    #[error(transparent)]
    Store(#[from] duckdb::StoreError),
}
