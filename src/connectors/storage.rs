//! Draining a source into a store, with no opinion about either.
//!
//! This is the loop and the policy around it: when to flush, when a source has gone
//! quiet, when to advance the checkpoint. The loop itself is
//! [`super::drain::run`], shared with the decode stage; this module only decides the
//! store's policy and hands the loop a straight copy. It names no broker and opens no
//! database — the caller supplies both — which is the same contract [`crate::ingest`]
//! and [`crate::decode`] have, and is what lets the same loop drive a file, a topic, or a
//! test double.
//!
//! By default the stage runs until the source ends or the process stops, because that is
//! what a live stream needs. A bounded run sets a drain bound, which stops once the
//! source has been idle long enough — see [`StorageBuilder::drain`].
//!
//! # Why it lives here
//!
//! It has no domain logic, unlike `ingest` (which decodes chain data and orders it) and
//! `decode` (which reads ABI-encoded logs). It is the counterpart of the sinks in this
//! module, and exists so a caller does not write the same flush-commit-checkpoint loop
//! per destination.

use std::time::Duration;

use anyhow::Result;

use crate::config::BatchConfig;
use crate::connectors::{EnvelopeSource, EventSink};

/// Builds and runs the storage stage.
///
/// It drains a source into a sink and knows neither the broker nor the store: the
/// connection is opened by the caller and handed in, the same way [`crate::ingest`] takes
/// its sink and [`super::Storage`]'s own peers take theirs. What lives here is the policy
/// — when to flush, when a source has gone quiet — none of which depends on which engine
/// is behind the sink.
#[derive(Debug)]
pub struct Storage {
    batch: BatchConfig,
    drain: Option<Duration>,
}

impl Storage {
    /// Starts a build.
    #[must_use]
    pub fn builder() -> StorageBuilder {
        StorageBuilder::new()
    }

    /// Drains `source` into `sink`.
    ///
    /// Ends when the source ends, or when it has been idle for the configured drain
    /// bound, or never when none is set. Returns how many records were stored.
    ///
    /// # Errors
    ///
    /// Returns an error when a record cannot be read or a row cannot be written. An idle
    /// source is not an error, and is not confused with a failed one.
    pub async fn run<S, K>(&self, source: &mut S, sink: &mut K) -> Result<u64>
    where
        S: EnvelopeSource,
        K: EventSink,
    {
        // A straight copy: the store persists each record as it arrives.
        super::drain::run(source, sink, self.batch, self.drain, |envelope, out| {
            out.push(envelope);
        })
        .await
    }
}

/// Builds a [`Storage`].
#[derive(Debug)]
pub struct StorageBuilder {
    batch: BatchConfig,
    drain: Option<Duration>,
}

impl StorageBuilder {
    /// Starts a build.
    #[must_use]
    pub fn new() -> Self {
        Self {
            batch: BatchConfig::default(),
            // No drain bound by default, because the default must suit a live stream: a
            // stage that stops whenever a source goes quiet for a few seconds would take
            // the process down with it. A bounded run opts in explicitly.
            drain: None,
        }
    }

    /// Sets how many records to buffer before flushing.
    #[must_use]
    pub const fn batch(mut self, batch: BatchConfig) -> Self {
        self.batch = batch;
        self
    }

    /// Stops once the source has been idle for `drain`.
    ///
    /// For a bounded run — a backfill, a test, a one-shot drain — not for a live stream,
    /// where idleness is normal and stopping is a fault.
    #[must_use]
    pub const fn drain(mut self, drain: Duration) -> Self {
        self.drain = Some(drain);
        self
    }

    /// Finishes the build.
    #[must_use]
    pub fn build(self) -> Storage {
        Storage {
            batch: self.batch,
            drain: self.drain,
        }
    }
}

impl Default for StorageBuilder {
    fn default() -> Self {
        Self::new()
    }
}
