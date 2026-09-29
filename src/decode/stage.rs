//! Running the decode stage: raw envelopes in, decoded envelopes out.
//!
//! The stage is a loop over a source and a sink, both handed in. It builds no clients
//! and names no broker, so it runs over anything that implements the two traits — which
//! is what makes it testable without Kafka, and what lets the transport change without
//! touching the decoder.
//!
//! [`crate::ingest`] works the same way, and for the same reason.
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

use std::time::Instant;

use anyhow::Result;
use tracing::{info, warn};

use crate::config::BatchConfig;
use crate::connectors::{EnvelopeSource, EventSink};
use crate::decode::Transform;
use crate::decode::contracts::ContractRegistry;

/// Builds and runs the decode stage.
#[derive(Debug)]
pub struct Decode {
    batch: BatchConfig,
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
    /// not *decode* is logged and skipped rather than failing the run, because the raw
    /// record is already on the input topic and a corrected ABI recovers it.
    pub async fn run<S, K>(self, source: &mut S, sink: &mut K) -> Result<()>
    where
        S: EnvelopeSource,
        K: EventSink,
    {
        let contracts = self.registry.len();
        let transform = Transform::new(self.registry);

        info!(
            contracts,
            batch_records = self.batch.records,
            "decode started"
        );

        let mut pending = 0;
        let mut last_flush = Instant::now();
        loop {
            let Some(envelope) = source.next().await? else {
                info!("input ended");
                break;
            };

            let outputs = match transform.apply(envelope) {
                Ok(outputs) => outputs,
                Err(error) => {
                    // One undecodable log must not stall the stream, but it must not be
                    // silent either: the usual cause is an ABI from the wrong block
                    // range, and the raw log is already on the input topic, so skipping
                    // it loses nothing a corrected ABI could not recover.
                    warn!(%error, "skipping a record that does not decode");
                    continue;
                }
            };
            for output in &outputs {
                sink.publish(output).await?;
            }
            pending += 1;

            // Flush on either bound, then commit: the checkpoint never advances past
            // bytes that are not yet on the output.
            if pending >= self.batch.records || last_flush.elapsed() >= self.batch.every {
                sink.flush().await?;
                source.commit().await?;
                pending = 0;
                last_flush = Instant::now();
            }
        }

        sink.flush().await?;
        source.commit().await?;
        Ok(())
    }
}

/// Builds a [`Decode`].
#[derive(Debug)]
pub struct DecodeBuilder {
    batch: BatchConfig,
    registry: ContractRegistry,
}

impl DecodeBuilder {
    /// Starts a build.
    #[must_use]
    pub fn new() -> Self {
        Self {
            batch: BatchConfig::default(),
            registry: ContractRegistry::default(),
        }
    }

    /// Sets how many records to consume before flushing and committing.
    #[must_use]
    pub const fn batch(mut self, batch: BatchConfig) -> Self {
        self.batch = batch;
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
            warn!("no contracts registered; every log will pass through undecoded");
        }
        Ok(Decode {
            batch: self.batch,
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

    use crate::connectors::{EnvelopeSource, EventSink};
    use crate::wire::envelope::{ChainId, Envelope, Event, Log};

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

    impl EventSink for CollectSink {
        async fn publish(&mut self, envelope: &Envelope) -> anyhow::Result<()> {
            self.seen.lock().expect("lock").push(envelope.clone());
            Ok(())
        }

        async fn flush(&mut self) -> anyhow::Result<()> {
            *self.flushes.lock().expect("lock") += 1;
            Ok(())
        }
    }

    /// A log no registry entry covers, so it takes the pass-through path.
    fn passthrough(sequence: u64) -> Envelope {
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

    /// The stage runs over anything implementing the traits, with no broker and no
    /// client construction. This is what the refactor bought: before it the loop could
    /// only be exercised against a live Kafka, which is why a stamping bug slipped past
    /// every unit test.
    #[tokio::test]
    async fn the_stage_runs_over_an_in_memory_source_and_sink() {
        let mut source = FakeSource {
            queued: vec![passthrough(2), passthrough(1)],
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
            2,
            "an unregistered log is forwarded, not dropped"
        );
        assert!(matches!(
            seen.first().map(|e| &e.event),
            Some(Event::Log(_))
        ));
    }

    /// The checkpoint advances after the sink is flushed, never before: a crash between
    /// the two must replay a batch rather than lose one.
    #[tokio::test]
    async fn the_checkpoint_advances_after_the_sink_is_flushed() {
        let mut source = FakeSource {
            queued: vec![passthrough(1)],
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
