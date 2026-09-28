//! The indexer's entry point: reads the settings file, builds every stage, runs them.
//!
//! One binary, started as a process. Each stage is built explicitly here rather than
//! reading settings for itself, so what runs is visible in one place and the stages stay
//! constructible in a test.
//!
//! # What runs
//!
//! Ingest follows a chain's live tip and publishes to the raw topic. Decode consumes that
//! and publishes to the decoded topic. Storage drains both into `DuckDB`. They run
//! concurrently and stop together: a stage that ends for good — an ingest subscription
//! that closes, a decode input that ends — stops the process, because continuing without
//! it would leave a stream that looks alive but is not.
//!
//! # Settings
//!
//! A TOML file, named by the first argument or `indexer.toml`. Every setting and its
//! default is documented in [`indexer::config`], and the required ones error at startup
//! naming the field.
//!
//! ```bash
//! cargo run --release -- settings.toml
//! ```

use std::process::ExitCode;

use anyhow::{Context as _, Result};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use indexer::config::{KafkaConfig, Settings};
use indexer::connectors::{KafkaSink, StdoutJsonSink, Storage};
use indexer::decode::Decode;
use indexer::ingest::Ingest;

/// The settings file used when none is named on the command line.
const DEFAULT_SETTINGS: &str = "indexer.toml";

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

/// Reads the settings file and runs every configured stage until one stops.
async fn run() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_SETTINGS.to_owned());
    let settings = Settings::from_file(&path)?;

    let batch = settings.batch();
    let raw_topic = settings.raw_topic().to_owned();
    let decoded_topic = settings.kafka.decoded_topic.clone();

    let ingest = settings
        .ingest
        .as_ref()
        .map(|ingest| {
            Ingest::builder(&ingest.chain)
                .http_url(&ingest.http_url)
                .ws_url(&ingest.ws_url)
                .build()
        })
        .transpose()?;

    let decode = Decode::builder(
        KafkaConfig::builder(&settings.kafka.brokers)
            .group(format!("{}-decode", settings.kafka.group_prefix))
            .input_topic(raw_topic.clone())
            .output_topic(decoded_topic.clone())
            .build()?,
    )
    .abis(settings.storage.abis.clone())?
    .batch(batch)
    .build()?;

    let mut storage = Storage::builder(
        KafkaConfig::builder(&settings.kafka.brokers)
            .group(format!("{}-storage", settings.kafka.group_prefix))
            .input_topic(raw_topic.clone())
            .build()?,
    )
    .topics([raw_topic.clone(), decoded_topic.clone()])
    .database(settings.storage.database.clone())
    .batch(batch);
    if let Some(drain) = settings.drain() {
        storage = storage.drain(drain);
    }
    let storage = storage.build()?;

    info!(
        settings = %path,
        raw_topic = %raw_topic,
        decoded_topic = %decoded_topic,
        ingest = ingest.is_some(),
        "starting"
    );

    // Ingest publishes through whichever sink the settings chose; the other two run on
    // the bus it feeds.
    let brokers = settings.kafka.brokers.clone();
    let stdout = settings.ingest.as_ref().is_some_and(|ingest| ingest.stdout);
    let ingest_run = async {
        let Some(ingest) = ingest else {
            // Ingest is not configured; the other stages run on their own.
            return std::future::pending().await;
        };
        if stdout {
            ingest.run(StdoutJsonSink::new()).await
        } else {
            let producer: rdkafka::producer::BaseProducer = rdkafka::ClientConfig::new()
                .set("bootstrap.servers", &brokers)
                .create()
                .context("create ingest producer")?;
            ingest
                .run(KafkaSink::new(producer, raw_topic.clone()))
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
