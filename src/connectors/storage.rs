//! Running the storage stage: topics in, rows in a local store.
//!
//! One consumer group per topic, because an offset is per group: one group spanning two
//! topics would commit a single position across both.
//!
//! By default the stage runs until the process stops, because that is what a live stream
//! needs. A bounded run sets a drain bound, which stops a topic once it has been idle for
//! long enough — see [`StorageBuilder::drain`].
//!
//! This lives beside the sink it drives rather than in a module of its own. A stage
//! module elsewhere in the tree holds domain logic — `ingest` decodes chain data and
//! orders it, `decode` reads ABI-encoded logs — and this holds none: it wires a
//! [`KafkaSource`] to a [`DuckDbSink`], both defined here, and drains.

use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::time::error::Elapsed;

use anyhow::{Context as _, Result};
use duckdb::Connection;
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer as _, StreamConsumer};
use tracing::info;

use crate::config::{BatchConfig, KafkaConfig};
use crate::connectors::{DuckDbSink, EnvelopeSource as _, EventSink as _, KafkaSource};
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
#[derive(Debug)]
pub struct Storage {
    kafka: KafkaConfig,
    batch: BatchConfig,
    topics: Vec<String>,
    database: PathBuf,
    drain: Option<Duration>,
}

impl Storage {
    /// Starts a build against `kafka`.
    #[must_use]
    pub fn builder(kafka: KafkaConfig) -> StorageBuilder {
        StorageBuilder::new(kafka)
    }

    /// Drains each topic into the store.
    ///
    /// Ends when every topic has been idle for the configured drain bound, or never
    /// when none is set.
    ///
    /// # Errors
    ///
    /// Returns an error when the store cannot be opened, the broker cannot be reached,
    /// or a row cannot be written. An idle topic is not an error, and is not confused
    /// with a failed one: see the loop below.
    pub async fn run(self) -> Result<()> {
        let connection = Connection::open(&self.database)
            .with_context(|| format!("open store at {}", self.database.display()))?;
        let mut sink = DuckDbSink::new(connection)?;

        info!(
            topics = %self.topics.join(","),
            database = %self.database.display(),
            group = %self.kafka.group,
            "storage started"
        );

        let mut stored = 0_u64;
        for topic in &self.topics {
            // Named for the topic, so committing one topic's position cannot move
            // another's.
            let consumer: StreamConsumer = ClientConfig::new()
                .set("bootstrap.servers", &self.kafka.brokers)
                .set("group.id", format!("{}-{topic}", self.kafka.group))
                .set("auto.offset.reset", "earliest")
                .set("enable.auto.commit", "false")
                .create()
                .context("create consumer")?;
            consumer
                .subscribe(&[topic])
                .with_context(|| format!("subscribe to {topic}"))?;

            let mut source = KafkaSource::new(consumer);
            let mut pending = 0;
            let mut last_flush = Instant::now();
            let mut topic_stored = 0_u64;
            // Time since the last record, and whether any has arrived yet. The idle
            // clock must not start until the first record, because a consumer group
            // takes a moment to join and assign partitions: counting that as idleness
            // makes a short drain bound fire before the topic has been read at all, and
            // report "drained" having stored nothing.
            //
            // `ponytail:` a topic that is genuinely empty at startup therefore never
            // trips the bound, so a bounded run against one waits for the process to be
            // stopped. Distinguishing "joined and empty" from "not yet joined" means
            // reading `assignment()`, which is the upgrade path.
            let mut saw_a_record = false;
            let mut idle_since = Instant::now();

            loop {
                // A timeout per record is what makes the idle bound observable: a
                // consumer with nothing to read blocks, so silence has to be measured
                // against a deadline rather than watched for.
                let outcome = tokio::time::timeout(self.batch.every, source.next()).await;
                let envelope =
                    match classify(outcome, idle_since.elapsed(), self.drain, saw_a_record)? {
                        Step::Record(envelope) => *envelope,
                        Step::Idle => continue,
                        Step::Stop => break,
                    };

                sink.publish(&envelope).await?;
                stored += 1;
                topic_stored += 1;
                pending += 1;
                saw_a_record = true;
                idle_since = Instant::now();

                if pending >= self.batch.records || last_flush.elapsed() >= self.batch.every {
                    sink.flush().await?;
                    source.commit()?;
                    pending = 0;
                    last_flush = Instant::now();
                }
            }

            sink.flush().await?;
            source.commit()?;
            info!(topic, rows = topic_stored, "topic drained");
        }

        info!(stored, "storage stopped");
        Ok(())
    }
}

/// Builds a [`Storage`].
#[derive(Debug)]
pub struct StorageBuilder {
    kafka: KafkaConfig,
    batch: BatchConfig,
    topics: Vec<String>,
    database: PathBuf,
    drain: Option<Duration>,
}

impl StorageBuilder {
    /// Starts a build against `kafka`.
    #[must_use]
    pub fn new(kafka: KafkaConfig) -> Self {
        Self {
            kafka,
            batch: BatchConfig::default(),
            topics: Vec::new(),
            database: PathBuf::from("indexer.duckdb"),
            // No drain bound by default, because the default must suit a live stream:
            // a stage that stops whenever a topic goes quiet for a few seconds would
            // take the process down with it. A bounded run opts in explicitly.
            drain: None,
        }
    }

    /// Adds a topic to drain.
    #[must_use]
    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.topics.push(topic.into());
        self
    }

    /// Drains several topics.
    #[must_use]
    pub fn topics<I, S>(mut self, topics: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.topics.extend(topics.into_iter().map(Into::into));
        self
    }

    /// Sets the store's path.
    #[must_use]
    pub fn database(mut self, path: impl Into<PathBuf>) -> Self {
        self.database = path.into();
        self
    }

    /// Sets how many records to buffer before flushing.
    #[must_use]
    pub const fn batch(mut self, batch: BatchConfig) -> Self {
        self.batch = batch;
        self
    }

    /// Stops each topic once it has been idle for `drain`.
    ///
    /// For a bounded run — a backfill, a test, a one-shot drain — not for a live stream,
    /// where idleness is normal and stopping is a fault.
    #[must_use]
    pub const fn drain(mut self, drain: Duration) -> Self {
        self.drain = Some(drain);
        self
    }

    /// Finishes the build.
    ///
    /// # Errors
    ///
    /// Returns an error when no topic was named, since a storage stage with nothing to
    /// read would start and immediately stop.
    pub fn build(self) -> Result<Storage> {
        if self.topics.is_empty() {
            anyhow::bail!("storage needs at least one topic");
        }
        Ok(Storage {
            kafka: self.kafka,
            batch: self.batch,
            topics: self.topics,
            database: self.database,
            drain: self.drain,
        })
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
