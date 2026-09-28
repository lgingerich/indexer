//! The materialize stage: decoded envelopes in, table rows out.
//!
//! Consumes the decoded topic and writes rows through a [`RowSink`]. It is the same
//! shape as the decode stage one hop down — a source on one topic, a sink on the other
//! side of the same transform — because both are stateless stages on the bus.
//!
//! # Delivery
//!
//! At-least-once, and idempotent by construction. `materialize::tables` is a pure
//! function of a decoded record, so a redelivered record produces the same rows, and a
//! store that upserts on their identity columns is unaffected by the replay.
//!
//! The commit happens **after** the row sink's flush, so a crash between the two
//! replays a batch rather than losing one.
//!
//! # What it does not do
//!
//! It does not deduplicate against what is already on disk. The identity columns it
//! writes are what a store would upsert on, but the JSON-lines sink appends, so a
//! replay appends the same rows again. A real store closes that with a key; this sink
//! is for inspecting output, not for accumulating it.

use std::process::ExitCode;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use connectors::{EnvelopeSource as _, KafkaSource};
use materialize::{JsonLinesRowSink, Row as _, RowSink as _, tables};
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer as _, StreamConsumer};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use wire::envelope::Event;

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
            error!(%error, "materialize stopped");
            ExitCode::FAILURE
        }
    }
}

/// What the process needs to reach the broker and shape its batches.
struct Config {
    brokers: String,
    group: String,
    input_topic: String,
    output_directory: std::path::PathBuf,
    batch: usize,
    batch_timeout: Duration,
}

impl Config {
    /// Reads the environment, failing on anything missing rather than guessing.
    fn from_env() -> Result<Self> {
        let get = |key: &str| std::env::var(key).with_context(|| format!("{key} must be set"));

        Ok(Self {
            brokers: get("KAFKA_BROKERS")?,
            group: std::env::var("MATERIALIZE_GROUP")
                .unwrap_or_else(|_| "indexer-materialize".to_owned()),
            input_topic: std::env::var("MATERIALIZE_INPUT_TOPIC")
                .unwrap_or_else(|_| "decoded.chain".to_owned()),
            output_directory: std::env::var("MATERIALIZE_OUTPUT_DIR")
                .unwrap_or_else(|_| "tables".to_owned())
                .into(),
            batch: std::env::var("MATERIALIZE_BATCH")
                .unwrap_or_else(|_| "500".to_owned())
                .parse()
                .context("MATERIALIZE_BATCH must be a number")?,
            batch_timeout: Duration::from_millis(
                std::env::var("MATERIALIZE_BATCH_MS")
                    .unwrap_or_else(|_| "1000".to_owned())
                    .parse()
                    .context("MATERIALIZE_BATCH_MS must be a number")?,
            ),
        })
    }
}

async fn run() -> Result<()> {
    let config = Config::from_env()?;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .set("group.id", &config.group)
        .set("auto.offset.reset", "earliest")
        // The commit point is this process's decision, after a flush; see the module
        // docs.
        .set("enable.auto.commit", "false")
        .create()
        .context("create consumer")?;
    consumer
        .subscribe(&[&config.input_topic])
        .context("subscribe to input topic")?;

    let mut source = KafkaSource::new(consumer);
    let mut sink = JsonLinesRowSink::new(&config.output_directory)?;

    info!(
        input = %config.input_topic,
        output = %config.output_directory.display(),
        group = %config.group,
        "materialize stage started"
    );

    let mut pending = 0;
    let mut last_flush = Instant::now();
    // Counted so a run's output is checkable: one faithful row per decoded event, plus
    // one semantic row for each event an extractor recognizes.
    let (mut events, mut trades_written) = (0_u64, 0_u64);

    loop {
        let Some(envelope) = source.next().await? else {
            info!("input topic ended");
            break;
        };
        let Event::Decoded(decoded) = &envelope.event else {
            // Raw logs, blocks, transactions, receipts, and the control signals all
            // pass through the decoded topic by design; only decoded records have
            // tables to build.
            continue;
        };

        let produced = tables(decoded);
        sink.write(produced.event.table(), &produced.event).await?;
        events += 1;
        if let Some(trade) = produced.trade {
            sink.write(trade.table(), &trade).await?;
            trades_written += 1;
        }
        pending += 1;

        // Flush on either bound, then commit: the offset never advances past rows that
        // are not yet durable.
        if pending >= config.batch || last_flush.elapsed() >= config.batch_timeout {
            sink.flush().await?;
            source.commit()?;
            pending = 0;
            last_flush = Instant::now();
        }
    }

    sink.flush().await?;
    source.commit()?;
    if events == 0 {
        warn!("no decoded records seen; nothing was materialized");
    }
    info!(events, trades = trades_written, "materialize stage stopped");
    Ok(())
}
