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

use indexer::config::Settings;
use indexer::connectors::{DuckDbSink, KafkaSink, StdoutJsonSink, Storage};
use indexer::decode::Decode;
use indexer::decode::contracts::ContractRegistry;
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
            // `{error:?}` prints the whole `anyhow` chain, where `{error}` prints only
            // the outermost context. A stage failure is wrapped in "decode stopped", so
            // the Display form loses the cause — which is the only part worth having.
            error!(error = ?error, "indexer stopped");
            ExitCode::FAILURE
        }
    }
}

/// Reads the settings file and runs every configured stage until one stops.
///
/// This is the only place that knows what the transport is. Every stage takes its source
/// and sink as parameters, so swapping Kafka for a file or a test double is a change
/// here and nowhere else.
async fn run() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_SETTINGS.to_owned());
    let settings = Settings::from_file(&path)?;

    let batch = settings.batch();
    let raw_topic = settings.raw_topic().to_owned();
    let decoded_topic = settings.kafka.decoded_topic.clone();

    // ABI paths in the settings file read as relative to it, not to the process's
    // working directory, so the file stays portable.
    let registry = ContractRegistry::load(&settings.storage.protocol, settings_dir(&path))?;
    let mut decode = Decode::builder().registry(registry).batch(batch);
    let mut storage = Storage::builder().batch(batch);
    if let Some(drain) = settings.drain() {
        // The same bound for both consumers, so a bounded run lets decode finish its
        // topic rather than waiting forever on an input dry storage already drained.
        decode = decode.drain(drain);
        storage = storage.drain(drain);
    }
    let decode = decode.build()?;
    let storage = storage.build();

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

    info!(
        settings = %path,
        raw_topic = %raw_topic,
        decoded_topic = %decoded_topic,
        ingest = ingest.is_some(),
        "starting"
    );

    let client = settings.client_config();
    let stdout = settings.ingest.as_ref().is_some_and(|ingest| ingest.stdout);

    // Ingest publishes through whichever sink the settings chose. It takes its sink as a
    // value, since it owns the pipeline that drives it.
    let ingest_run = async {
        let Some(ingest) = ingest else {
            // Ingest is not configured; the other stages run on their own.
            return std::future::pending().await;
        };
        if stdout {
            ingest.run(StdoutJsonSink::new()).await
        } else {
            let producer: rdkafka::producer::BaseProducer =
                client.create().context("create ingest producer")?;
            ingest
                .run(KafkaSink::new(producer, raw_topic.clone()))
                .await
        }
    };

    // Decode and storage commit offsets after their sink's flush, so they borrow the
    // source rather than owning it — the checkpoint is theirs to advance, the handle is
    // not theirs to keep.
    let decode_run = async {
        let mut consumer = settings.consumer(&decoded_consumer_group(&settings), &raw_topic)?;
        let producer: rdkafka::producer::BaseProducer =
            client.create().context("create decode producer")?;
        let mut sink = KafkaSink::new(producer, decoded_topic.clone());
        decode.run(&mut consumer, &mut sink).await
    };

    let storage_run = async {
        let connection = settings.store_connection()?;
        let mut sink = DuckDbSink::new(connection)?;
        let mut stored = 0_u64;
        // One consumer group per topic, because an offset is per group: one group
        // spanning two topics would commit a single position across both.
        for topic in [&raw_topic, &decoded_topic] {
            let mut source = settings.consumer(
                &format!("{}-storage-{topic}", settings.kafka.group_prefix),
                topic,
            )?;
            stored += storage.run(&mut source, &mut sink).await?;
            info!(topic, "drained");
        }
        info!(stored, "storage stopped");
        Ok::<(), anyhow::Error>(())
    };

    // Whichever stage stops first ends the process: a stream missing a stage looks
    // alive while quietly falling behind. The stage's own error is carried with it,
    // because "decode stopped" says nothing about why.
    tokio::select! {
        result = ingest_run => result.context("ingest stopped")?,
        result = decode_run => result.context("decode stopped")?,
        result = storage_run => result.context("storage stopped")?,
    }
    Ok(())
}

/// The consumer group decode commits its offsets under.
fn decoded_consumer_group(settings: &Settings) -> String {
    format!("{}-decode", settings.kafka.group_prefix)
}

/// The directory a settings file lives in, which its relative paths resolve against.
///
/// A bare filename has no parent, so the working directory stands in for it.
fn settings_dir(path: &str) -> std::path::PathBuf {
    std::path::Path::new(path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map_or_else(
            || std::path::PathBuf::from("."),
            std::path::Path::to_path_buf,
        )
}
