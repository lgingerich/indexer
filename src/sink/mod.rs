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
//! - [`channel`] — the one hop that crosses tasks: a bounded in-process channel of
//!   blocks, from decode to storage. It is what lets a slow store stall without stalling
//!   ingest.
//! - [`duckdb`] — the store, an embedded `DuckDB` database.
//! - [`stdout`] — newline-delimited JSON, for watching the stream.
//!
//! Sinks do not own their engine: the runtime opens and tunes the `DuckDB` connection and
//! injects it, so client settings live in one place and a sink runs over a test double.

pub mod channel;
#[cfg(feature = "duckdb")]
pub mod duckdb;
pub mod stdout;

pub use channel::{ChannelReceiver, ChannelSink};
#[cfg(feature = "duckdb")]
pub use duckdb::DuckDbSink;
pub use stdout::StdoutJsonSink;

use anyhow::Result;

use crate::wire::envelope::Envelope;

/// Receives envelopes in per-chain sequence order.
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
    fn publish(&mut self, envelope: Envelope) -> impl Future<Output = Result<()>> + Send;

    /// Ends the batch: hands everything published since the last flush onward, as one.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffered envelopes cannot be delivered. The caller stops
    /// rather than continuing past a lost batch.
    fn flush(&mut self) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
}
