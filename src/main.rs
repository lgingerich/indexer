//! The indexer's entry point: builds every stage and runs them together.
//!
//! One binary, started as a process. Each stage is built explicitly here rather than
//! reading the environment for itself, so what runs is visible in one place and the
//! stages themselves stay constructible in a test.
//!
//! # What runs
//!
//! Ingest follows a chain's live tip and publishes to `raw.chain`. Decode consumes that
//! and publishes to `decoded.chain`. Storage drains both into `DuckDB`. They run
//! concurrently and stop together: a stage that ends for good — an ingest subscription
//! that closes, a decode input that ends — stops the process, because continuing without
//! it would leave a stream that looks alive but is not.
//!
//! # Environment
//!
//! Read once, here, and turned into builders. Nothing below this file looks at the
//! environment.
//!
//! | Variable | Required | Meaning |
//! | --- | --- | --- |
//! | `EVM_CHAIN` | for ingest | Chain id stamped on every event |
//! | `EVM_HTTP_URL` | for ingest | JSON-RPC endpoint for blocks and receipts |
//! | `EVM_WS_URL` | for ingest | WebSocket endpoint for heads |
//! | `KAFKA_BROKERS` | yes | Bootstrap servers |
//! | `RAW_TOPIC` | no | Defaults to `raw.chain` |
//! | `DECODED_TOPIC` | no | Defaults to `decoded.chain` |
//! | `DECODE_ABIS` | no | `chain:address:abi.json`, comma-separated |
//! | `STORAGE_DATABASE` | no | Defaults to `indexer.duckdb` |
//! | `STDOUT` | no | `1` to print ingest to stdout instead of the broker |

use std::process::ExitCode;
use std::time::Duration;

use anyhow::{Context as _, Result};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use indexer::config::{BatchConfig, KafkaConfig};
use indexer::connectors::StdoutJsonSink;
use indexer::connectors::Storage;
use indexer::decode::Decode;
use indexer::ingest::Ingest;

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
            error!(%error, "indexer stopped");
            ExitCode::FAILURE
        }
    }
}

/// Reads the environment, builds every stage, and runs them until one stops.
async fn run() -> Result<()> {
    let brokers = std::env::var("KAFKA_BROKERS").context("KAFKA_BROKERS must be set")?;
    let raw_topic = std::env::var("RAW_TOPIC").unwrap_or_else(|_| "raw.chain".to_owned());
    let decoded_topic =
        std::env::var("DECODED_TOPIC").unwrap_or_else(|_| "decoded.chain".to_owned());
    let batch = BatchConfig::new(500, Duration::from_secs(1));

    let ingest = build_ingest(&raw_topic)?;
    let decode = Decode::builder(
        KafkaConfig::builder(brokers.clone())
            .group("indexer-decode")
            .input_topic(raw_topic.clone())
            .output_topic(decoded_topic.clone())
            .build()?,
    )
    .abis(abi_registrations())
    .context("read DECODE_ABIS")?
    .batch(batch)
    .build()?;
    let storage = Storage::builder(
        KafkaConfig::builder(brokers.clone())
            .group("indexer-storage")
            .input_topic(raw_topic.clone())
            .build()?,
    )
    .topics([raw_topic.clone(), decoded_topic.clone()])
    .database(std::env::var("STORAGE_DATABASE").unwrap_or_else(|_| "indexer.duckdb".to_owned()))
    .batch(batch)
    .build()?;

    info!("starting ingest, decode, and storage");

    // Ingest publishes through whichever sink it was built with; the other two run on
    // the bus it feeds.
    let ingest_run = async {
        let Some(ingest) = ingest else {
            // Ingest is not configured; let the other stages run on their own.
            return std::future::pending().await;
        };
        if std::env::var("STDOUT").is_ok_and(|value| value == "1") {
            ingest.run(StdoutJsonSink::new()).await
        } else {
            let producer: rdkafka::producer::BaseProducer = rdkafka::ClientConfig::new()
                .set("bootstrap.servers", &brokers)
                .create()
                .context("create ingest producer")?;
            ingest
                .run(indexer::connectors::KafkaSink::new(
                    producer,
                    raw_topic.clone(),
                ))
                .await
        }
    };

    // Whichever stage stops first ends the process: a stream missing a stage looks
    // alive while quietly falling behind.
    tokio::select! {
        result = ingest_run => result.context("ingest stopped"),
        result = decode.run() => result.context("decode stopped"),
        result = storage.run() => result.context("storage stopped"),
    }
}

/// Builds the ingest stage from the environment, or `None` when it is not configured.
///
/// Ingest is optional so the decode and storage stages can be run against a topic that
/// was filled elsewhere.
fn build_ingest(raw_topic: &str) -> Result<Option<Ingest>> {
    let Ok(chain) = std::env::var("EVM_CHAIN") else {
        info!("EVM_CHAIN unset; running without ingest");
        return Ok(None);
    };
    let http_url = std::env::var("EVM_HTTP_URL").context("EVM_HTTP_URL must be set")?;
    let ws_url = std::env::var("EVM_WS_URL").context("EVM_WS_URL must be set")?;
    let _ = raw_topic;
    Ok(Some(
        Ingest::builder(chain)
            .http_url(http_url)
            .ws_url(ws_url)
            .build()?,
    ))
}

/// The ABI registrations from `DECODE_ABIS`, which is a comma-separated list.
fn abi_registrations() -> Vec<String> {
    std::env::var("DECODE_ABIS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_owned)
        .collect()
}
