//! Running the storage stage: topics in, rows in a local store.
//!
//! One consumer group per topic, because an offset is per group: one group spanning two
//! topics would commit a single position across both. An idle bound makes a bounded run
//! terminate rather than block forever on a broker that has nothing more to give.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use duckdb::Connection;
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer as _, StreamConsumer};
use tracing::info;

use crate::config::{BatchConfig, KafkaConfig};
use crate::connectors::{DuckDbSink, EnvelopeSource as _, EventSink as _, KafkaSource};

/// Builds and runs the storage stage.
#[derive(Debug)]
pub struct Storage {
    kafka: KafkaConfig,
    batch: BatchConfig,
    topics: Vec<String>,
    database: PathBuf,
    drain: Duration,
}

impl Storage {
    /// Starts a build against `kafka`.
    #[must_use]
    pub fn builder(kafka: KafkaConfig) -> StorageBuilder {
        StorageBuilder::new(kafka)
    }

    /// Drains each topic into the store, then stops.
    ///
    /// # Errors
    ///
    /// Returns an error when the store cannot be opened, the broker cannot be reached,
    /// or a row cannot be written. A topic with nothing to read is not an error: it is
    /// drained, which is how a bounded run finishes.
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
            let mut idle_since = Instant::now();
            let mut last_flush = Instant::now();
            let mut topic_stored = 0_u64;

            loop {
                // A timeout per record is what makes the idle bound observable: a
                // consumer with nothing to read blocks, so silence has to be measured
                // against a deadline rather than watched for.
                let Ok(Ok(Some(envelope))) =
                    tokio::time::timeout(self.batch.every, source.next()).await
                else {
                    if idle_since.elapsed() >= self.drain {
                        break;
                    }
                    continue;
                };

                sink.publish(&envelope).await?;
                stored += 1;
                topic_stored += 1;
                pending += 1;
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
    drain: Duration,
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
            drain: Duration::from_secs(5),
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

    /// Sets how long a topic may be idle before it is treated as drained.
    #[must_use]
    pub const fn drain(mut self, drain: Duration) -> Self {
        self.drain = drain;
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
