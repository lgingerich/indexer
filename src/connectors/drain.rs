//! The stage loop: drain a source into a sink, batching and checkpointing.
//!
//! Every stage that consumes a stream and publishes one runs this: [`Storage`]
//! copies records straight through, and [`crate::decode::Decode`] expands each into
//! its decoded output(s) first. It names no broker, opens no database, and knows no
//! domain — the caller supplies both ends and the per-record expansion — which is
//! what lets the same loop drive a file, a topic, or a test double.
//!
//! [`Storage`]: crate::connectors::Storage
//!
//! # Why it lives in one place
//!
//! There is exactly one correct order: publish, then flush, then commit. Committing
//! before the sink is durable replays a batch on a crash (safe); committing after is
//! impossible to get wrong here, but doing it before the *flush* would lose one. A
//! second copy of this loop is a second chance to invert that, so there is one.

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
    /// A record to publish.
    Record(Box<Envelope>),
    /// Nothing to read, and the source has not been idle long enough to stop.
    Idle,
    /// The source has ended, or has been idle past its drain bound.
    Stop,
}

/// Decides what a read attempt means.
///
/// `outcome` is the result of a bounded read: `Err` from the timeout means nothing
/// arrived, and `Ok` carries the source's own result. `saw_a_record` is whether this
/// source has produced anything yet, which is what keeps the drain bound from firing
/// during a consumer group's join.
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
        // Idleness only counts once the source has been read from at least once. Before
        // that, silence is a consumer group still joining, and treating it as a drained
        // topic makes a short bound report success having stored nothing.
        Err(_elapsed) if saw_a_record && drain.is_some_and(|drain| idle_for >= drain) => {
            Ok(Step::Stop)
        }
        Err(_elapsed) => Ok(Step::Idle),
    }
}

/// Drains `source` into `sink`, expanding each record through `expand`.
///
/// `expand` pushes the envelopes to publish for one input record into the reused
/// buffer — one for a straight copy, or several for a transform like decode. The buffer
/// is cleared between records, so a hot copy stage does not allocate per record.
///
/// Ends when the source ends, or when it has been idle for `drain`, or never when
/// `drain` is `None`. Returns how many envelopes were published.
///
/// # Errors
///
/// Returns an error when a record cannot be read, an envelope cannot be published, or
/// the checkpoint cannot be advanced. An idle source is not an error, and is not
/// confused with a failed one: an idle read is kept distinct from a failed one rather
/// than reported as a drained source.
pub async fn run<S, K, F>(
    source: &mut S,
    sink: &mut K,
    batch: BatchConfig,
    drain: Option<Duration>,
    mut expand: F,
) -> Result<u64>
where
    S: EnvelopeSource,
    K: EventSink,
    F: FnMut(Envelope, &mut Vec<Envelope>),
{
    let mut published = 0_u64;
    let mut pending = 0_usize;
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
    let mut buffer: Vec<Envelope> = Vec::with_capacity(2);

    loop {
        // A timeout per record is what makes the idle bound observable: a source
        // with nothing to read blocks, so silence has to be measured against a
        // deadline rather than watched for.
        let outcome = tokio::time::timeout(batch.every, source.next()).await;
        match classify(outcome, idle_since.elapsed(), drain, saw_a_record)? {
            Step::Record(envelope) => {
                buffer.clear();
                expand(*envelope, &mut buffer);
                for output in &buffer {
                    sink.publish(output).await?;
                    published += 1;
                    pending += 1;
                }
                saw_a_record = true;
                idle_since = Instant::now();
            }
            Step::Idle => {
                // Nothing new arrived, but a partial batch may have aged past the time
                // bound: flush it rather than holding records in the sink through the
                // lull. Without this the time bound only fires when the *next* record
                // lands, so a batch can sit unflushed for as long as the source is quiet.
                if pending > 0 && last_flush.elapsed() >= batch.every {
                    sink.flush().await?;
                    source.commit().await?;
                    pending = 0;
                    last_flush = Instant::now();
                }
                continue;
            }
            Step::Stop => break,
        }

        // Flush on either bound, then commit: the checkpoint never advances past
        // bytes that are not yet durable.
        if pending >= batch.records || last_flush.elapsed() >= batch.every {
            sink.flush().await?;
            source.commit().await?;
            pending = 0;
            last_flush = Instant::now();
        }
    }

    sink.flush().await?;
    source.commit().await?;
    Ok(published)
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::time::Duration;

    use tokio::time::error::Elapsed;

    use crate::config::BatchConfig;
    use crate::connectors::{EnvelopeSource, EventSink};
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
            "a bound must not fire before the source has been read from"
        );
    }

    /// A source error must surface, never be reported as a finished stream. Folding the
    /// two together is how a broken broker produced a partial table and logged it as
    /// `"drained"` — the bug this function exists to prevent.
    #[test]
    fn a_source_error_is_propagated_not_treated_as_a_stop() {
        let outcome = Ok(Err(anyhow::anyhow!("broker is unreachable")));
        let result = classify(outcome, Duration::from_mins(10), None, true);
        assert!(
            result.is_err(),
            "a failed read must not be mistaken for a drained source"
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

    /// With a bound, idleness past it stops the source — which is what makes a bounded
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

    /// A record is work even when the source has been idle past its bound: a bound stops
    /// an *idle* source, never one that is still delivering.
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

    /// The source ending is a stop, which is how a finite source finishes.
    #[test]
    fn the_source_ending_stops() {
        let outcome: Result<Result<Option<Envelope>, anyhow::Error>, _> = Ok(Ok(None));
        let step = classify(outcome, Duration::from_secs(0), None, false).expect("end is fine");
        assert_eq!(step, Step::Stop);
    }

    /// A partial batch must go durable on the *time* bound even while the source is
    /// quiet. The regression: the idle arm used to `continue` before the bound check, so
    /// a record could sit in the sink until the next record arrived — unbounded on a
    /// quiet chain, and invisible to a test whose source never goes quiet.
    #[tokio::test]
    async fn a_partial_batch_flushes_during_a_lull() {
        /// One record, then a source that blocks forever.
        struct OneThenQuiet {
            queued: Option<Envelope>,
            commits: usize,
        }

        impl EnvelopeSource for OneThenQuiet {
            async fn next(&mut self) -> anyhow::Result<Option<Envelope>> {
                match self.queued.take() {
                    Some(envelope) => Ok(Some(envelope)),
                    None => std::future::pending().await,
                }
            }

            async fn commit(&mut self) -> anyhow::Result<()> {
                self.commits += 1;
                Ok(())
            }
        }

        /// Counts flushes and what was published before each.
        #[derive(Default)]
        struct CountingSink {
            flushes: usize,
        }

        impl EventSink for CountingSink {
            async fn publish(&mut self, _envelope: &Envelope) -> anyhow::Result<()> {
                Ok(())
            }

            async fn flush(&mut self) -> anyhow::Result<()> {
                self.flushes += 1;
                Ok(())
            }
        }

        let mut source = OneThenQuiet {
            queued: Some(envelope()),
            commits: 0,
        };
        let mut sink = CountingSink::default();
        // A tiny time bound: the first read returns the record, the second times out.
        let batch = BatchConfig::new(1_000, Duration::from_millis(1));

        // The source never ends, so bound the whole run. The first idle read after the
        // record must flush the partial batch before the run is cancelled.
        let _ = tokio::time::timeout(
            Duration::from_millis(100),
            super::run(&mut source, &mut sink, batch, None, |envelope, out| {
                out.push(envelope);
            }),
        )
        .await;

        assert!(
            sink.flushes >= 1,
            "the partial batch must flush on the time bound during the lull"
        );
        assert!(
            source.commits >= 1,
            "the checkpoint advanced with the flush"
        );
    }
}
