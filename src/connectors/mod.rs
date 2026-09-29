//! The bus boundary: the sink/source traits, the connectors, and the drain loop.
//!
//! [`EnvelopeSink`] publishes to the bus and [`EnvelopeSource`] consumes from it. Both
//! speak [`Envelope`] and neither names a chain, so a stage that transforms the stream
//! is a source on one topic and a sink on another. [`run`] drains one into the other.
//!
//! Connectors do not own their transport: the runtime builds and tunes the client
//! (`librdkafka`, `DuckDB`) and injects it. No stage constructs one, which is what lets
//! a stage run over a file or a test double and keeps client settings in one place.

#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod memory;
pub mod stdout;

#[cfg(feature = "duckdb")]
pub use duckdb::DuckDbSink;
#[cfg(feature = "kafka")]
pub use kafka::{KafkaSink, KafkaSource};
pub use memory::{MemoryBus, MemorySink, MemorySource};
pub use stdout::StdoutJsonSink;

use std::time::{Duration, Instant};

use anyhow::Result;

use crate::config::BatchConfig;
use crate::wire::envelope::Envelope;

/// Receives envelopes in per-chain sequence order.
///
/// The driver holds the sink through an exclusive borrow, so it may buffer across calls
/// — a rendered row, an open appender, an in-flight producer — instead of paying the
/// engine's per-record cost. [`flush`](EnvelopeSink::flush) is the batch and durability
/// point; the pipeline calls it once per block. A slow sink applies backpressure to the
/// whole pipeline, which is safe only because downstream consumers read from their own
/// queues.
pub trait EnvelopeSink: Send {
    /// Publishes one envelope.
    ///
    /// May buffer; the envelope is not durable until [`EnvelopeSink::flush`] succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if the envelope cannot be accepted. The caller stops rather
    /// than skipping it.
    fn publish(&mut self, envelope: &Envelope) -> impl Future<Output = Result<()>> + Send;

    /// Makes everything published since the last flush durable, as one batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffered envelopes cannot be delivered. The caller stops
    /// rather than continuing past a lost batch.
    fn flush(&mut self) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
}

/// Yields envelopes in per-chain sequence order.
///
/// The consumer side of the bus, and the mirror of [`EnvelopeSink`]. Returns an owned
/// [`Envelope`] because the item outlives the broker's buffer, so a copy keeps a
/// transform from holding a view into the fetch queue across an await.
///
/// [`commit`](EnvelopeSource::commit) advances the checkpoint, and the stage decides
/// *when* to call it — after its sink's flush, never per record. A source that committed
/// per record would make at-least-once delivery unachievable; one that never committed
/// would replay from the beginning. A source with no checkpoint implements it as a no-op,
/// which is why the default is `Ok(())`.
pub trait EnvelopeSource: Send {
    /// The next envelope, or `None` when the stream has ended.
    ///
    /// # Errors
    ///
    /// Returns an error when the payload cannot be read. The caller stops rather than
    /// skipping, since skipping a record the source has already advanced past is data
    /// loss.
    fn next(&mut self) -> impl Future<Output = Result<Option<Envelope>>> + Send;

    /// Marks everything returned so far as processed.
    ///
    /// Called after the stage's output is durable, so a crash between the two replays a
    /// batch rather than losing one.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint cannot be advanced.
    fn commit(&mut self) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
}

/// Whether a bounded run has gone idle long enough to stop.
///
/// The run is bounded only when `drain` is set; a live source going quiet is normal, so
/// `None` never stops. The idle clock does not start until the first record — silence
/// before that is a consumer group still joining, and firing the bound there would report
/// "drained" having stored nothing, so `idle_since` is `None` until then.
fn drained(idle_since: Option<Instant>, drain: Option<Duration>) -> bool {
    let (Some(since), Some(drain)) = (idle_since, drain) else {
        return false;
    };
    since.elapsed() >= drain
}

/// Drains `source` into `sink`, expanding each record through `expand`.
///
/// Every stage that consumes a stream and publishes one runs this: a store copies
/// records straight through (`|envelope, out| out.push(envelope)`), and
/// [`crate::decode::Decode`] expands each into its decoded record first. `expand`
/// pushes the envelopes to publish for one input record into the reused buffer; the
/// buffer is cleared between records, so a hot copy stage does not allocate per record.
///
/// There is exactly one correct order — publish, then flush, then commit — so there is
/// one copy of it. Committing before the sink is durable replays a batch on a crash
/// (safe); doing it before the *flush* would lose one.
///
/// Timing is event-driven rather than polled: the loop sleeps until the next deadline
/// (the batch's time bound, or the drain bound once idle) instead of waking on a fixed
/// cadence, so each deadline is independent and a record flushes on the *shorter* of the
/// batch's two bounds. See [`BatchConfig`].
///
/// Ends when the source ends, or when idle for longer than `drain` (`None` never drains,
/// which is what a live stream wants). Returns how many envelopes were published.
///
/// # Errors
///
/// Returns an error when a record cannot be read, an envelope cannot be published, or
/// the checkpoint cannot be advanced. An idle source is not an error, and is not
/// confused with a failed one: a source error propagates rather than being reported as a
/// drained source, which would lose everything after it and look like success.
pub async fn run<S, K, F>(
    source: &mut S,
    sink: &mut K,
    batch: BatchConfig,
    drain: Option<Duration>,
    mut expand: F,
) -> Result<u64>
where
    S: EnvelopeSource,
    K: EnvelopeSink,
    F: FnMut(Envelope, &mut Vec<Envelope>),
{
    let mut published = 0_u64;
    let mut pending = 0_usize;
    // When the pending batch must flush, and when an idle source has waited long enough
    // to stop. Both are `None` until the first record: the idle clock must not start
    // before then, because a consumer group takes a moment to join and assign
    // partitions, and counting that as idleness lets a short drain bound fire before the
    // source has been read at all and report "drained" having stored nothing.
    //
    // `ponytail:` a genuinely empty source therefore never trips the drain bound, so a
    // bounded run against one waits for the process to be stopped. Telling "joined and
    // empty" from "not yet joined" means reading the source's assignment, the upgrade path.
    let mut flush_at: Option<Instant> = None;
    let mut idle_since: Option<Instant> = None;
    // One slot: today's `expand` pushes at most one envelope per record (a straight copy,
    // or decode's decoded record). The buffer is reused across records so a hot stage
    // does not allocate per record; growth is fine if an `expand` ever emits two.
    let mut buffer: Vec<Envelope> = Vec::with_capacity(1);

    loop {
        // Wake on whichever deadline the loop is currently waiting for — the batch's
        // flush bound when a batch is pending, otherwise the drain bound. Waiting for
        // `next()` and the drain bound together is what makes an idle source stop: the
        // source never ends on its own, so a bounded run has no other way to finish.
        if drained(idle_since, drain) {
            break;
        }
        let flush_deadline = flush_at.unwrap_or_else(|| Instant::now() + batch.every);
        let sleeper = tokio::time::sleep_until(flush_deadline.into());
        tokio::select! {
            biased;
            // The source ended, which is how a finite source finishes.
            result = source.next() => {
                let Some(envelope) = result? else {
                    break;
                };
                buffer.clear();
                expand(envelope, &mut buffer);
                for output in &buffer {
                    sink.publish(output).await?;
                    published += 1;
                    pending += 1;
                }
                // The idle clock starts now, and the batch's clock starts if it had not.
                let now = Instant::now();
                idle_since = Some(now);
                flush_at.get_or_insert(now + batch.every);
            }
            // Either deadline passed: flush the pending batch and advance the checkpoint.
            () = sleeper => {
                if pending > 0 {
                    sink.flush().await?;
                    source.commit().await?;
                    pending = 0;
                }
                flush_at = None;
            }
        }

        // The size bound flushes as soon as it is reached, without waiting for the time.
        if pending >= batch.records {
            sink.flush().await?;
            source.commit().await?;
            pending = 0;
            flush_at = None;
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
    use std::time::{Duration, Instant};

    use crate::config::BatchConfig;
    use crate::connectors::{EnvelopeSink, EnvelopeSource};
    use crate::wire::envelope::{ChainId, Envelope, Event, Finalized};

    use super::drained;

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

    /// A bound must not fire before the source has been read from: silence during a
    /// consumer group's join is not a drained topic, and counting it as one reports
    /// success having stored nothing.
    #[test]
    fn a_bound_does_not_fire_before_the_first_record() {
        assert!(!drained(None, Some(Duration::from_secs(1))));
    }

    /// No bound is a live stream, where idleness is normal and stopping is a fault.
    #[test]
    fn idleness_without_a_bound_never_stops() {
        assert!(!drained(Some(Instant::now()), None));
    }

    /// Past the bound, an idle source stops — which is what makes a bounded run end.
    #[test]
    fn idleness_past_a_bound_stops() {
        let idle_since = Instant::now()
            .checked_sub(Duration::from_secs(10))
            .expect("the clock is past boot");
        assert!(drained(Some(idle_since), Some(Duration::from_secs(5))));
    }

    /// Under the bound, a brief lull keeps looping rather than ending a bounded run
    /// early.
    #[test]
    fn idleness_under_a_bound_does_not_stop() {
        assert!(!drained(Some(Instant::now()), Some(Duration::from_secs(5))));
    }

    /// A partial batch must go durable on the *time* bound while the source is quiet.
    /// The source never ends, so the only thing that can flush the lone record is the
    /// batch's time bound firing on its own deadline.
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

        impl EnvelopeSink for CountingSink {
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
        // A tiny time bound: the lone record must flush once it ages past it.
        let batch = BatchConfig::new(1_000, Duration::from_millis(1));

        // The source never ends, so bound the whole run. The batch's time bound must
        // fire and flush the partial batch before the run is cancelled.
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

    /// An idle source stops a bounded run: the source never ends, so the drain bound is
    /// the only thing that can finish it.
    #[tokio::test]
    async fn an_idle_source_stops_a_bounded_run() {
        #[derive(Default)]
        struct CountingSink {
            flushes: usize,
        }

        impl EnvelopeSink for CountingSink {
            async fn publish(&mut self, _envelope: &Envelope) -> anyhow::Result<()> {
                Ok(())
            }

            async fn flush(&mut self) -> anyhow::Result<()> {
                self.flushes += 1;
                Ok(())
            }
        }

        // One record first, so the idle clock starts, then silence.
        struct OneThenQuiet {
            queued: Option<Envelope>,
        }

        impl EnvelopeSource for OneThenQuiet {
            async fn next(&mut self) -> anyhow::Result<Option<Envelope>> {
                match self.queued.take() {
                    Some(envelope) => Ok(Some(envelope)),
                    None => std::future::pending().await,
                }
            }
        }

        let mut source = OneThenQuiet {
            queued: Some(envelope()),
        };
        let mut sink = CountingSink::default();
        let batch = BatchConfig::new(1_000, Duration::from_millis(1));

        // The drain bound must end the run rather than hanging forever.
        let stored = tokio::time::timeout(
            Duration::from_millis(500),
            super::run(
                &mut source,
                &mut sink,
                batch,
                Some(Duration::from_millis(10)),
                |envelope, out| out.push(envelope),
            ),
        )
        .await
        .expect("the drain bound ends the run")
        .expect("the run is not an error");

        assert_eq!(stored, 1, "the one record was published");
    }

    /// A source error surfaces rather than being reported as a drained source, so a
    /// broken broker cannot look like a finished stream.
    #[tokio::test]
    async fn a_source_error_is_propagated_not_read_as_end() {
        struct Broken;

        impl EnvelopeSource for Broken {
            async fn next(&mut self) -> anyhow::Result<Option<Envelope>> {
                anyhow::bail!("broker is unreachable")
            }
        }

        #[derive(Default)]
        struct NoopSink;

        impl EnvelopeSink for NoopSink {
            async fn publish(&mut self, _envelope: &Envelope) -> anyhow::Result<()> {
                Ok(())
            }
        }

        let mut source = Broken;
        let mut sink = NoopSink;
        let batch = BatchConfig::new(500, Duration::from_millis(1));

        let error = super::run(&mut source, &mut sink, batch, None, |envelope, out| {
            out.push(envelope);
        })
        .await
        .expect_err("a failed read must not be mistaken for a drained source");

        assert!(error.to_string().contains("unreachable"), "{error}");
    }
}
