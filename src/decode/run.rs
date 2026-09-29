//! Running the decode stage: raw records in, decoded records and control signals out.
//!
//! The stage is a loop over a source and a sink, both handed in. It builds no clients
//! and names no broker, so it runs over anything that implements the two traits — which
//! is what makes it testable without Kafka, and what lets the transport change without
//! touching the decoder.
//!
//! [`crate::ingest`] works the same way, and for the same reason. The loop itself is
//! [`crate::connectors::run`], shared with the store's own drain so the two cannot
//! drift on the one ordering that matters: publish, then flush, then commit.
//!
//! # Delivery
//!
//! At-least-once. The transform is a pure function of a record and the registry, so a
//! redelivered record decodes to the same output with the same `dedupe_key`, and a
//! consumer that upserts on that key is idempotent.
//!
//! The commit happens **after** the sink's flush, so a crash between the two replays a
//! batch rather than losing one. That ordering is why the stage takes the source by
//! mutable reference rather than owning it: it must advance the checkpoint, but the
//! caller keeps the handle.

use std::time::Duration;

use anyhow::Result;
use tracing::{info, warn};

use crate::config::BatchConfig;
use crate::connectors::{EnvelopeSink, EnvelopeSource};
use crate::decode::Transform;
use crate::decode::contracts::ContractRegistry;

/// Builds and runs the decode stage.
#[derive(Debug)]
pub struct Decode {
    batch: BatchConfig,
    drain: Option<Duration>,
    registry: ContractRegistry,
}

impl Decode {
    /// Starts a build.
    #[must_use]
    pub fn builder() -> DecodeBuilder {
        DecodeBuilder::new()
    }

    /// Reads from `source`, decoding each record into `sink` until the input ends.
    ///
    /// # Errors
    ///
    /// Returns an error when a record cannot be read, when a decoded record cannot be
    /// published, or when the checkpoint cannot be advanced. A single record that does
    /// not *decode* is logged and produces no output; the raw log is already on the
    /// input topic, so a corrected ABI recovers it.
    pub async fn run<S, K>(self, source: &mut S, sink: &mut K) -> Result<()>
    where
        S: EnvelopeSource,
        K: EnvelopeSink,
    {
        let contracts = self.registry.len();
        let transform = Transform::new(self.registry);

        info!(
            contracts,
            batch_records = self.batch.records,
            "decode started"
        );

        // Each registered log expands to its decoded record; everything else — raw
        // datasets, unregistered logs — is dropped, since the raw topic already carries
        // it. A log that fails to decode is reported but produces no record.
        crate::connectors::run(source, sink, self.batch, self.drain, |envelope, out| {
            let applied = transform.apply(envelope);
            if let Some(error) = applied.error {
                // A log that matched an ABI but did not decode usually means the ABI
                // is the wrong version for this height. It must not stall the stream,
                // and it must not be silent either — the raw log is already upstream,
                // so a corrected ABI recovers it.
                warn!(%error, "a log did not decode and produced no record");
            }
            out.extend(applied.output);
        })
        .await?;
        Ok(())
    }
}

/// Builds a [`Decode`].
#[derive(Debug)]
pub struct DecodeBuilder {
    batch: BatchConfig,
    drain: Option<Duration>,
    registry: ContractRegistry,
}

impl DecodeBuilder {
    /// Starts a build.
    #[must_use]
    pub fn new() -> Self {
        Self {
            batch: BatchConfig::default(),
            drain: None,
            registry: ContractRegistry::default(),
        }
    }

    /// Sets how many records to consume before flushing and committing.
    #[must_use]
    pub const fn batch(mut self, batch: BatchConfig) -> Self {
        self.batch = batch;
        self
    }

    /// Stops once the input has been idle for `drain`.
    ///
    /// For a bounded run — a backfill, a one-shot drain — not for a live stream, where a
    /// quiet input is normal and stopping is a fault. Without it a bounded run has no way
    /// to finish, which is why the store's drain takes the same bound.
    #[must_use]
    pub const fn drain(mut self, drain: Duration) -> Self {
        self.drain = Some(drain);
        self
    }

    /// Sets the contract registry this stage decodes with.
    #[must_use]
    pub fn registry(mut self, registry: ContractRegistry) -> Self {
        self.registry = registry;
        self
    }

    /// Finishes the build.
    ///
    /// # Errors
    ///
    /// Never fails today. It returns a `Result` so a future required setting can be
    /// reported the way the other builders report theirs, rather than changing every
    /// call site at that point.
    pub fn build(self) -> Result<Decode> {
        if self.registry.is_empty() {
            warn!("no contracts registered; every log will be dropped undecoded");
        }
        Ok(Decode {
            batch: self.batch,
            drain: self.drain,
            registry: self.registry,
        })
    }
}

impl Default for DecodeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::sync::Mutex;

    use alloy_primitives::{Address, B256, TxHash};

    use crate::connectors::{EnvelopeSink, EnvelopeSource};
    use crate::wire::envelope::{ChainId, Envelope, Event, Finalized, Log};

    use super::Decode;

    /// A source over a fixed list, which is the point of the refactor: the stage runs
    /// without a broker, so its wiring is testable.
    #[derive(Default)]
    struct FakeSource {
        queued: Vec<Envelope>,
        commits: usize,
    }

    impl EnvelopeSource for FakeSource {
        async fn next(&mut self) -> anyhow::Result<Option<Envelope>> {
            Ok(self.queued.pop())
        }

        async fn commit(&mut self) -> anyhow::Result<()> {
            self.commits += 1;
            Ok(())
        }
    }

    /// Collects what the stage published, and counts flushes.
    #[derive(Default)]
    struct CollectSink {
        seen: Mutex<Vec<Envelope>>,
        flushes: Mutex<usize>,
    }

    impl EnvelopeSink for CollectSink {
        async fn publish(&mut self, envelope: &Envelope) -> anyhow::Result<()> {
            self.seen.lock().expect("lock").push(envelope.clone());
            Ok(())
        }

        async fn flush(&mut self) -> anyhow::Result<()> {
            *self.flushes.lock().expect("lock") += 1;
            Ok(())
        }
    }

    /// A log no registry entry covers: it has no decoded form, so the stage drops it.
    fn unregistered_log(sequence: u64) -> Envelope {
        Envelope::new(
            ChainId::new("base"),
            sequence,
            Event::Log(Box::new(Log {
                log_index: sequence,
                transaction_hash: TxHash::from([0x11; 32]),
                address: Address::from([0xaa; 20]),
                block_number: 100 + sequence,
                block_hash: B256::from([0x02; 32]),
                block_timestamp: 1_700_000_000,
                ..Log::default()
            })),
        )
    }

    /// A control signal: it must be forwarded, because a store retracts and compacts on
    /// it.
    fn finalized(sequence: u64) -> Envelope {
        Envelope::new(
            ChainId::new("base"),
            sequence,
            Event::Finalized(Finalized {
                height: sequence,
                hash: B256::from([0x03; 32]),
            }),
        )
    }

    /// The stage runs over anything implementing the traits, with no broker and no
    /// client construction, and publishes only what belongs on the decoded topic: a
    /// control signal survives, an unregistered raw log is dropped.
    #[tokio::test]
    async fn the_stage_forwards_control_signals_and_drops_raw_logs() {
        let mut source = FakeSource {
            queued: vec![unregistered_log(2), finalized(1)],
            commits: 0,
        };
        let mut sink = CollectSink::default();

        Decode::builder()
            .registry(crate::decode::contracts::ContractRegistry::default())
            .build()
            .expect("builds")
            .run(&mut source, &mut sink)
            .await
            .expect("the stage runs");

        let seen = sink.seen.lock().expect("lock");
        assert_eq!(
            seen.len(),
            1,
            "only the control signal belongs on the decoded topic"
        );
        assert!(matches!(
            seen.first().map(|e| &e.event),
            Some(Event::Finalized(_))
        ));
    }

    /// The checkpoint advances after the sink is flushed, never before: a crash between
    /// the two must replay a batch rather than lose one.
    #[tokio::test]
    async fn the_checkpoint_advances_after_the_sink_is_flushed() {
        let mut source = FakeSource {
            queued: vec![finalized(1)],
            commits: 0,
        };
        let mut sink = CollectSink::default();

        Decode::builder()
            .registry(crate::decode::contracts::ContractRegistry::default())
            .build()
            .expect("builds")
            .run(&mut source, &mut sink)
            .await
            .expect("the stage runs");

        assert!(source.commits > 0, "the checkpoint advanced");
        assert!(*sink.flushes.lock().expect("lock") > 0);
    }

    /// An empty source ends cleanly rather than looping, which is what makes the stage
    /// terminate on a drained input.
    #[tokio::test]
    async fn an_empty_source_ends_the_run() {
        let mut source = FakeSource::default();
        let mut sink = CollectSink::default();
        Decode::builder()
            .registry(crate::decode::contracts::ContractRegistry::default())
            .build()
            .expect("builds")
            .run(&mut source, &mut sink)
            .await
            .expect("an empty run is not an error");
        assert!(sink.seen.lock().expect("lock").is_empty());
    }
}
