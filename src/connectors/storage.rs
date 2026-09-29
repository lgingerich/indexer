//! Draining a source into a store, with no opinion about either.
//!
//! This is the loop and the policy around it: when to flush, when a source has gone
//! quiet, when to advance the checkpoint. It names no broker and opens no database — the
//! caller supplies both — which is the same contract [`crate::ingest`] and
//! [`crate::decode`] have, and is what lets the same loop drive a file, a topic, or a
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

use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::time::error::Elapsed;

use crate::config::BatchConfig;
use crate::connectors::{EnvelopeSource, EventSink};
use crate::wire::envelope::Envelope;

/// What one turn of the drain loop concluded from a read attempt.
///
/// Extracted from the loop because getting it wrong is a data-loss bug rather than a
/// cosmetic one: a failed broker reported as a drained topic loses everything after the
/// failure and looks like success. Splitting the three outcomes is what makes that
/// impossible to reintroduce by accident.
#[derive(Debug, PartialEq, Eq)]
enum Step {
    /// A record to store.
    Record(Box<Envelope>),
    /// Nothing to read, and the topic has not been idle long enough to stop.
    Idle,
    /// The topic has ended, or has been idle past its drain bound.
    Stop,
}

/// Decides what a read attempt means.
///
/// `outcome` is the result of a bounded read: `Err` from the timeout means nothing
/// arrived, and `Ok` carries the source's own result. `saw_a_record` is whether this
/// topic has produced anything yet, which is what keeps the drain bound from firing
/// during the consumer group's join.
///
/// # Errors
///
/// Propagates a source error, which is a broker or decoding failure. It must never be
/// folded into [`Step::Stop`]: that is what makes a broken broker look like a finished
/// stream.
fn classify(
    outcome: Result<Result<Option<Envelope>, anyhow::Error>, Elapsed>,
    idle_for: Duration,
    drain: Option<Duration>,
    saw_a_record: bool,
) -> Result<Step> {
    match outcome {
        // A record: always work, regardless of any bound.
        Ok(Ok(Some(envelope))) => Ok(Step::Record(Box::new(envelope))),
        // The source itself ended.
        Ok(Ok(None)) => Ok(Step::Stop),
        // A source error is a failure, never a quiet stop.
        Ok(Err(error)) => Err(error),
        // Idleness only counts once the topic has been read from at least once. Before
        // that, silence is a consumer group still joining, and treating it as a drained
        // topic makes a short bound report success having stored nothing.
        Err(_elapsed) if saw_a_record && drain.is_some_and(|drain| idle_for >= drain) => {
            Ok(Step::Stop)
        }
        Err(_elapsed) => Ok(Step::Idle),
    }
}

/// Builds and runs the storage stage.
///
/// It drains a source into a sink and knows neither the broker nor the store: the
/// connection is opened by the caller and handed in, the same way [`crate::ingest`] takes
/// its sink and [`super::Storage`]'s own peers take theirs. What lives here is the loop
/// and the policy around it — when to flush, when a topic has gone quiet, when to
/// advance the checkpoint — and none of that depends on which engine is behind the sink.
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
    /// bound, or never when none is set.
    ///
    /// # Errors
    ///
    /// Returns an error when a record cannot be read or a row cannot be written. An idle
    /// source is not an error, and is not confused with a failed one: an idle read is
    /// kept distinct from a failed one rather than reported as a drained topic.
    pub async fn run<S, K>(&self, source: &mut S, sink: &mut K) -> Result<u64>
    where
        S: EnvelopeSource,
        K: EventSink,
    {
        let mut stored = 0_u64;
        let mut pending = 0;
        let mut last_flush = Instant::now();
        // Time since the last record, and whether any has arrived yet. The idle clock
        // must not start until the first record, because a consumer group takes a moment
        // to join and assign partitions: counting that as idleness makes a short drain
        // bound fire before the source has been read at all, and report "drained" having
        // stored nothing.
        //
        // `ponytail:` a genuinely empty source therefore never trips the bound, so a
        // bounded run against one waits for the process to be stopped. Distinguishing
        // "joined and empty" from "not yet joined" means reading the source's assignment,
        // which is the upgrade path.
        let mut saw_a_record = false;
        let mut idle_since = Instant::now();

        loop {
            // A timeout per record is what makes the idle bound observable: a source
            // with nothing to read blocks, so silence has to be measured against a
            // deadline rather than watched for.
            let outcome = tokio::time::timeout(self.batch.every, source.next()).await;
            let envelope = match classify(outcome, idle_since.elapsed(), self.drain, saw_a_record)?
            {
                Step::Record(envelope) => *envelope,
                Step::Idle => continue,
                Step::Stop => break,
            };

            sink.publish(&envelope).await?;
            stored += 1;
            pending += 1;
            saw_a_record = true;
            idle_since = Instant::now();

            if pending >= self.batch.records || last_flush.elapsed() >= self.batch.every {
                sink.flush().await?;
                source.commit().await?;
                pending = 0;
                last_flush = Instant::now();
            }
        }

        sink.flush().await?;
        source.commit().await?;
        Ok(stored)
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

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::time::Duration;

    use tokio::time::error::Elapsed;

    use crate::wire::envelope::{ChainId, Envelope, Event, Finalized};

    use super::{Step, classify};

    /// A real elapsed timeout, since `Elapsed` cannot be constructed directly.
    async fn timed_out() -> Result<Result<Option<Envelope>, anyhow::Error>, Elapsed> {
        tokio::time::timeout(
            Duration::ZERO,
            std::future::pending::<Result<Option<Envelope>, anyhow::Error>>(),
        )
        .await
    }

    fn envelope() -> Envelope {
        Envelope::new(
            ChainId::new("base"),
            1,
            Event::Finalized(Finalized {
                height: 1,
                hash: alloy_primitives::B256::from([0x11; 32]),
            }),
        )
    }

    /// Idleness *before the first record* is a consumer group joining, not a drained
    /// topic. This is the regression the graceful fix missed: a short drain bound fired
    /// during the join and reported success having stored nothing.
    #[tokio::test]
    async fn idleness_before_the_first_record_is_not_a_stop() {
        let outcome = timed_out().await;
        let step = classify(
            outcome,
            Duration::from_mins(10),
            Some(Duration::from_secs(1)),
            false,
        )
        .expect("idle is fine");
        assert_eq!(
            step,
            Step::Idle,
            "a bound must not fire before the topic has been read from"
        );
    }

    /// A source error must surface, never be reported as a finished stream. Folding the
    /// two together is how a broken broker produced a partial table and logged it as
    /// `"topic drained"` — the bug this function exists to prevent.
    #[test]
    fn a_source_error_is_propagated_not_treated_as_a_stop() {
        let outcome = Ok(Err(anyhow::anyhow!("broker is unreachable")));
        let result = classify(outcome, Duration::from_mins(10), None, true);
        assert!(
            result.is_err(),
            "a failed read must not be mistaken for a drained topic"
        );
    }

    /// A source error is a failure even when a drain bound is set, so a bounded run
    /// cannot mask one either.
    #[test]
    fn a_source_error_is_propagated_even_with_a_drain_bound() {
        let outcome = Ok(Err(anyhow::anyhow!("broker is unreachable")));
        let result = classify(
            outcome,
            Duration::from_mins(10),
            Some(Duration::from_secs(5)),
            true,
        );
        assert!(result.is_err());
    }

    /// Idleness without a bound is normal on a live stream, so it must loop rather than
    /// stop. Stopping here would end the process on a quiet block.
    #[tokio::test]
    async fn idleness_without_a_bound_is_not_a_stop() {
        let outcome = timed_out().await;
        let step = classify(outcome, Duration::from_mins(10), None, true).expect("idle is fine");
        assert_eq!(step, Step::Idle);
    }

    /// With a bound, idleness past it stops the topic — which is what makes a bounded
    /// run terminate.
    #[tokio::test]
    async fn idleness_past_a_bound_stops() {
        let outcome = timed_out().await;
        let step = classify(
            outcome,
            Duration::from_secs(10),
            Some(Duration::from_secs(5)),
            true,
        )
        .expect("idle is fine");
        assert_eq!(step, Step::Stop);
    }

    /// Idleness *under* a bound keeps looping, so a brief lull does not end a bounded
    /// run early.
    #[tokio::test]
    async fn idleness_under_a_bound_keeps_looping() {
        let outcome = timed_out().await;
        let step = classify(
            outcome,
            Duration::from_secs(1),
            Some(Duration::from_secs(5)),
            true,
        )
        .expect("idle is fine");
        assert_eq!(step, Step::Idle);
    }

    /// A record is work even when the topic has been idle past its bound: a bound stops
    /// an *idle* topic, never one that is still delivering.
    #[test]
    fn a_record_is_work_regardless_of_the_bound() {
        let outcome = Ok(Ok(Some(envelope())));
        let step = classify(
            outcome,
            Duration::from_mins(10),
            Some(Duration::from_secs(5)),
            true,
        )
        .expect("a record is fine");
        assert!(matches!(step, Step::Record(_)));
    }

    /// The source ending is a stop, which is how a finite topic finishes.
    #[test]
    fn the_source_ending_stops() {
        let outcome: Result<Result<Option<Envelope>, anyhow::Error>, _> = Ok(Ok(None));
        let step = classify(outcome, Duration::from_secs(0), None, false).expect("end is fine");
        assert_eq!(step, Step::Stop);
    }
}
