//! The storage stage: where the pipeline's envelopes are persisted.
//!
//! The terminal sink: it consumes every topic the pipeline produces to and writes each
//! envelope to `DuckDB`. Nothing downstream depends on how it drains, so it is the
//! simplest stage in the chain — consume, publish, flush, commit, with no transform in
//! between.
//!
//! It lives beside [`DuckDbSink`] rather than in its own crate because it *is* the
//! store: the sink is the mechanism and this is the runtime that drives it, and the two
//! only make sense together.
//!
//! [`DuckDbSink`]: connectors::DuckDbSink

use std::process::ExitCode;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use connectors::{DuckDbSink, EnvelopeSource as _, EventSink as _, KafkaSource};
use duckdb::Connection;
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer as _, StreamConsumer};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .json()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            error!(%error, "storage stopped");
            ExitCode::FAILURE
        }
    }
}

/// What the process needs to reach the broker, the store, and shape its batches.
struct Config {
    brokers: String,
    group: String,
    topics: Vec<String>,
    database: String,
    batch: usize,
    batch_timeout: Duration,
    /// How long a topic may be idle before it is treated as drained.
    ///
    /// The topics never truly end on a live pipeline, so this is what lets a bounded
    /// run terminate rather than block forever on an idle consumer. `ponytail:` a run
    /// against live topics wants a stop signal instead, which is the runner's job.
    drain: Duration,
}

impl Config {
    /// Reads the environment, failing on anything missing rather than guessing.
    fn from_env() -> Result<Self> {
        let get = |key: &str| std::env::var(key).with_context(|| format!("{key} must be set"));

        Ok(Self {
            brokers: get("KAFKA_BROKERS")?,
            group: std::env::var("STORAGE_GROUP").unwrap_or_else(|_| "indexer-storage".to_owned()),
            topics: std::env::var("STORAGE_TOPICS")
                .unwrap_or_else(|_| "raw.chain,decoded.chain".to_owned())
                .split(',')
                .map(str::trim)
                .filter(|topic| !topic.is_empty())
                .map(str::to_owned)
                .collect(),
            database: std::env::var("STORAGE_DATABASE")
                .unwrap_or_else(|_| "indexer.duckdb".to_owned()),
            batch: std::env::var("STORAGE_BATCH")
                .unwrap_or_else(|_| "500".to_owned())
                .parse()
                .context("STORAGE_BATCH must be a number")?,
            batch_timeout: Duration::from_millis(
                std::env::var("STORAGE_BATCH_MS")
                    .unwrap_or_else(|_| "1000".to_owned())
                    .parse()
                    .context("STORAGE_BATCH_MS must be a number")?,
            ),
            drain: Duration::from_secs(
                std::env::var("STORAGE_DRAIN_SECS")
                    .unwrap_or_else(|_| "5".to_owned())
                    .parse()
                    .context("STORAGE_DRAIN_SECS must be a number")?,
            ),
        })
    }
}

async fn run() -> Result<()> {
    let config = Config::from_env()?;

    let connection = Connection::open(&config.database)
        .with_context(|| format!("open store at {}", config.database))?;
    let mut sink = DuckDbSink::new(connection)?;

    info!(
        topics = %config.topics.join(","),
        database = %config.database,
        group = %config.group,
        "storage started"
    );

    let mut stored = 0_u64;
    for topic in &config.topics {
        // One group per topic, named for it: an offset is per group, so one group
        // spanning three topics would commit a single position across all of them.
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", format!("{}-{topic}", config.group))
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
            // A timeout per record is what makes the idle bound observable: a consumer
            // with nothing to read blocks, so silence has to be measured against a
            // deadline rather than watched for.
            let Ok(Ok(Some(envelope))) =
                tokio::time::timeout(config.batch_timeout, source.next()).await
            else {
                if idle_since.elapsed() >= config.drain {
                    break;
                }
                continue;
            };

            sink.publish(&envelope).await?;
            stored += 1;
            topic_stored += 1;
            pending += 1;
            idle_since = Instant::now();

            if pending >= config.batch || last_flush.elapsed() >= config.batch_timeout {
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
