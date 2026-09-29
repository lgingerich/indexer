//! The pipeline the settings describe, assembled and run.
//!
//! The stage types are fixed — there is only ever `ingest → decode → storage` — so what
//! varies is a few choices the settings file already states. This module reads them and
//! builds the pipeline, so the binary is "load the settings, run it" rather than a place
//! that names every stage and connector.
//!
//! # What the settings decide
//!
//! Two choices, and three consequences:
//!
//! - **Bus** — `[bus] kind = "kafka"` or `"memory"`. The one transport choice.
//! - **Store** — `[storage] kind = "duckdb"`. The other.
//! - **Ingest runs iff `[ingest]` is present.** Configuring a chain *is* the statement
//!   that it should be followed.
//! - **Decode runs iff `[decode] registry` names a registry that has entries.** Decode is
//!   purely additive: with none it would drop every raw dataset and forward only the
//!   control signals, which ingest already publishes to the raw topic storage reads, so
//!   skipping it loses nothing.
//! - **Storage reads the raw topic always, and the decoded topic only when decode ran.**
//!   The decoded topic only carries records if something put them there.
//!
//! # Assembly
//!
//! Every endpoint — producer, consumer, store — is created *before* any stage runs, so a
//! record published first thing is never missed by a subscriber that had not attached
//! yet. The three stages then run concurrently, and the first to stop ends the process.

use anyhow::{Context as _, Result};
use tracing::{info, warn};

use crate::config::{BusKind, Settings};
use crate::connectors::{self, DuckDbSink, EnvelopeSink, EnvelopeSource, MemoryBus};
use crate::decode::Decode;
use crate::decode::registry::ContractRegistry;
use crate::ingest::Ingest;
use crate::ingest::source::Merged;
use crate::wire::envelope::Envelope;

/// The settings file used when none is named on the command line.
pub const DEFAULT_SETTINGS: &str = "indexer.toml";

/// A bus: how a topic sink and a topic source are built.
///
/// The sink and source are associated types rather than boxed, because the connectors
/// return `impl Future` from their trait methods and a trait object cannot carry that —
/// so the transport is chosen at compile time and a build has exactly one.
pub trait Transport {
    /// Publishes to one topic.
    type Sink: EnvelopeSink;
    /// Reads from one topic.
    type Source: EnvelopeSource;

    /// A sink that publishes to `topic`.
    ///
    /// # Errors
    ///
    /// Returns an error when the client cannot be built, naming the topic.
    fn sink(&self, topic: &str) -> Result<Self::Sink>;

    /// A source that reads `topic` under consumer group `group`.
    ///
    /// The group is the caller's because a topic's consumers commit under different
    /// groups — decode reads raw, storage reads raw and decoded under its own — and an
    /// offset is per group. A transport with no offsets ignores it.
    ///
    /// # Errors
    ///
    /// Returns an error when the client cannot be built or the subscription fails.
    fn source(&self, topic: &str, group: &str) -> Result<Self::Source>;
}

impl Transport for MemoryBus {
    type Sink = connectors::memory::MemorySink;
    type Source = connectors::memory::MemorySource;

    fn sink(&self, topic: &str) -> Result<Self::Sink> {
        Ok(MemoryBus::sink(self, topic))
    }

    fn source(&self, topic: &str, _group: &str) -> Result<Self::Source> {
        // In-memory delivery has no offsets, so the group is unused.
        Ok(MemoryBus::source(self, topic))
    }
}

/// The Kafka-protocol transport: one producer and one consumer per topic.
#[cfg(feature = "kafka")]
#[derive(Debug)]
struct KafkaTransport {
    client: rdkafka::ClientConfig,
}

#[cfg(feature = "kafka")]
impl KafkaTransport {
    /// A client from `[bus.kafka]`, shared by every producer and consumer this builds.
    fn new(settings: &Settings) -> Self {
        let mut client = rdkafka::ClientConfig::new();
        client.set("bootstrap.servers", &settings.bus.kafka.brokers);
        for (key, value) in &settings.bus.kafka.properties {
            client.set(key, value);
        }
        Self { client }
    }
}

#[cfg(feature = "kafka")]
impl Transport for KafkaTransport {
    type Sink = connectors::KafkaSink;
    type Source = connectors::KafkaSource;

    fn sink(&self, topic: &str) -> Result<Self::Sink> {
        // Each sink gets its own producer: the producer is not shared across the stages,
        // which publish concurrently.
        let producer = self
            .client
            .create()
            .with_context(|| format!("create a producer for {topic}"))?;
        Ok(connectors::KafkaSink::new(producer, topic))
    }

    fn source(&self, topic: &str, group: &str) -> Result<Self::Source> {
        use rdkafka::consumer::Consumer as _;

        let consumer: rdkafka::consumer::StreamConsumer = self
            .client
            .clone()
            .set("group.id", group)
            // A fresh group starts at the beginning rather than skipping the history it
            // was created to read.
            .set("auto.offset.reset", "earliest")
            // The commit point is the stage's decision, after its sink's flush.
            .set("enable.auto.commit", "false")
            .create()
            .with_context(|| format!("create a consumer for {topic}"))?;
        consumer
            .subscribe(&[topic])
            .with_context(|| format!("subscribe to {topic}"))?;
        Ok(connectors::KafkaSource::new(consumer))
    }
}

/// A source that yields nothing and ends, so storage reads one topic through the same
/// [`Merged`] path it uses for two.
#[derive(Debug, Default)]
struct Empty;

impl EnvelopeSource for Empty {
    async fn next(&mut self) -> Result<Option<Envelope>> {
        Ok(None)
    }
}

/// The pipeline the settings describe: the stages, wired and ready.
#[derive(Debug)]
pub struct Pipeline {
    /// The ingest stage, if a chain is configured.
    ingest: Option<Ingest>,
    /// The decode stage, if a non-empty registry is configured.
    decode: Option<Decode>,
}

impl Pipeline {
    /// Assembles the pipeline the settings describe.
    ///
    /// # Errors
    ///
    /// Returns an error when the registry cannot be read, an ingest endpoint is missing,
    /// or the decode stage cannot be built.
    pub fn from_settings(settings: &Settings) -> Result<Self> {
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

        let registry = settings.registry_path().map_or_else(
            || Ok(ContractRegistry::default()),
            ContractRegistry::from_file,
        )?;
        let decode = if registry.is_empty() {
            if settings.decode.registry.is_some() {
                warn!("a registry was named but is empty; no log will decode");
            }
            None
        } else {
            let mut builder = Decode::builder().registry(registry).batch(settings.batch());
            if let Some(drain) = settings.drain() {
                builder = builder.drain(drain);
            }
            Some(builder.build()?)
        };

        Ok(Self { ingest, decode })
    }

    /// Runs every configured stage until one stops.
    ///
    /// # Errors
    ///
    /// Returns an error when a stage fails. The stage's own error is carried with it,
    /// because "decode stopped" says nothing about why.
    pub async fn run(self, settings: &Settings) -> Result<()> {
        match settings.bus.kind {
            BusKind::Memory => self.run_over(MemoryBus::new(), settings).await,
            BusKind::Kafka => {
                #[cfg(feature = "kafka")]
                {
                    self.run_over(KafkaTransport::new(settings), settings).await
                }
                #[cfg(not(feature = "kafka"))]
                anyhow::bail!(
                    "bus.kind = \"kafka\" needs the `kafka` feature; rebuild with \
                     --features kafka, or set bus.kind = \"memory\""
                )
            }
        }
    }

    /// The pipeline over `transport`, generic so a memory or Kafka bus is one body.
    async fn run_over<T: Transport>(self, transport: T, settings: &Settings) -> Result<()> {
        let bus = &settings.bus;
        let decoding = self.decode.is_some();

        // Build every endpoint before any stage runs: an in-memory topic drops a record
        // published before its subscriber attached, so a first block must not precede
        // storage's subscription.
        let mut store = connect_store(settings)?;
        let stdout = settings.ingest.as_ref().is_some_and(|ingest| ingest.stdout);
        let mut ingest_sink = if self.ingest.is_some() && !stdout {
            Some(transport.sink(&bus.raw_topic)?)
        } else {
            None
        };
        let mut decoded_sink = if decoding {
            Some(transport.sink(&bus.decoded_topic)?)
        } else {
            None
        };
        let mut raw_for_decode = if decoding {
            Some(transport.source(&bus.raw_topic, &bus.group("decode"))?)
        } else {
            None
        };
        // Storage reads the raw topic under its own group, and the decoded topic under a
        // second one, because an offset is per group and per topic. The topic is part of
        // the group name: it is a durability contract, and the name is what a deployment
        // upgrades under, so it must not change when the stage's shape does.
        let raw_for_store = transport.source(&bus.raw_topic, &bus.storage_group(&bus.raw_topic))?;
        let decoded_for_store = if decoding {
            Some(transport.source(&bus.decoded_topic, &bus.storage_group(&bus.decoded_topic))?)
        } else {
            None
        };

        let ingest = async {
            let (Some(stage), sink) = (self.ingest, ingest_sink.take()) else {
                // No chain configured: nothing to follow, so this never resolves.
                return std::future::pending().await;
            };
            match sink {
                Some(sink) => stage.run(sink).await,
                None => stage.run(connectors::StdoutJsonSink::new()).await,
            }
        };

        let decode = async {
            let (Some(stage), Some(mut source), Some(mut sink)) =
                (self.decode, raw_for_decode.take(), decoded_sink.take())
            else {
                return std::future::pending().await;
            };
            stage.run(&mut source, &mut sink).await
        };

        let storage = async {
            let batch = settings.batch();
            let drain = settings.drain();
            let stored = if let Some(second) = decoded_for_store {
                let mut source = Merged::new(raw_for_store, second);
                connectors::run(&mut source, &mut store, batch, drain, copy).await?
            } else {
                let mut source = Merged::new(raw_for_store, Empty);
                connectors::run(&mut source, &mut store, batch, drain, copy).await?
            };
            info!(stored, "storage stopped");
            Ok::<(), anyhow::Error>(())
        };

        // Whichever stage stops first ends the process: a stream missing a stage looks
        // alive while quietly falling behind.
        tokio::select! {
            result = ingest => result.context("ingest stopped")?,
            result = decode => result.context("decode stopped")?,
            result = storage => result.context("storage stopped")?,
        }
        Ok(())
    }
}

/// A straight copy: the store persists each record as it arrives.
fn copy(envelope: Envelope, out: &mut Vec<Envelope>) {
    out.push(envelope);
}

/// Opens the store the settings chose.
///
/// `[storage] kind` is validated by serde at parse, so an unknown backend is already an
/// error by the time this runs; `duckdb` is the only kind today. A second backend is a
/// variant there and a branch here.
///
/// # Errors
///
/// Returns an error when an engine setting is rejected or the database cannot be opened.
fn connect_store(settings: &Settings) -> Result<DuckDbSink> {
    let duckdb = &settings.storage.duckdb;
    let mut config = duckdb::Config::default();
    for (key, value) in &duckdb.settings {
        config = config
            .with(key, value)
            .with_context(|| format!("duckdb setting {key:?} was rejected"))?;
    }
    let connection = duckdb::Connection::open_with_flags(&duckdb.path, config)
        .with_context(|| format!("open store at {}", duckdb.path.display()))?;
    info!(store = %duckdb.path.display(), "storage opened");
    DuckDbSink::new(connection)
}

/// The settings file's path as given on the command line, or [`DEFAULT_SETTINGS`].
#[must_use]
pub fn settings_path() -> String {
    std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_SETTINGS.to_owned())
}

/// Loads the settings at `path` and runs the pipeline they describe.
///
/// # Errors
///
/// Returns an error when the settings cannot be read, or the pipeline cannot be built or
/// run.
pub async fn run(path: &str) -> Result<()> {
    let settings = Settings::from_file(path)?;
    info!(
        settings = path,
        chain = settings.ingest.as_ref().map_or("-", |ingest| ingest.chain.as_str()),
        raw_topic = %settings.bus.raw_topic,
        decoded_topic = %settings.bus.decoded_topic,
        registry = settings
            .registry_path()
            .map_or_else(|| "-".to_owned(), |path| path.display().to_string()),
        store = %settings.storage.duckdb.path.display(),
        "starting"
    );
    Pipeline::from_settings(&settings)?.run(&settings).await
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are allowed
// them per the repository test style, since a failed expectation there means the fixture
// or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::str::FromStr as _;

    use crate::config::Settings;

    use super::Pipeline;

    /// The shipped registry, as an absolute path so [`Settings::from_str`] carries no
    /// directory to resolve it against.
    fn registry() -> String {
        format!("{}/registry.toml", env!("CARGO_MANIFEST_DIR"))
    }

    /// No chain and no registry is the degenerate pipeline: nothing to follow and nothing
    /// to decode. Storage still runs, because it always does.
    #[test]
    fn a_bare_config_assembles_no_ingest_and_no_decode() {
        let settings = Settings::from_str("[bus]\nkind = \"memory\"\n").expect("settings parse");
        let pipeline = Pipeline::from_settings(&settings).expect("the pipeline builds");
        assert!(pipeline.ingest.is_none(), "no chain is configured");
        assert!(pipeline.decode.is_none(), "no registry is configured");
    }

    /// A configured chain is the statement that ingest runs — there is no separate
    /// on/off switch to disagree with the presence of the endpoints.
    #[test]
    fn a_chain_config_assembles_ingest() {
        let settings = Settings::from_str(
            r#"
[ingest]
chain = "base"
http_url = "https://example.invalid"
ws_url = "wss://example.invalid"

[bus]
kind = "memory"
"#,
        )
        .expect("settings parse");
        let pipeline = Pipeline::from_settings(&settings).expect("the pipeline builds");
        assert!(pipeline.ingest.is_some(), "a chain means ingest runs");
        assert!(pipeline.decode.is_none(), "no registry is configured");
    }

    /// A registry with entries is the statement that decode runs; nothing else has to be
    /// switched on.
    #[test]
    fn a_non_empty_registry_assembles_decode() {
        let settings = Settings::from_str(&format!(
            "[bus]\nkind = \"memory\"\n[decode]\nregistry = {:?}\n",
            registry()
        ))
        .expect("settings parse");
        let pipeline = Pipeline::from_settings(&settings).expect("the pipeline builds");
        assert!(pipeline.decode.is_some(), "a registry means decode runs");
        assert!(pipeline.ingest.is_none(), "no chain is configured");
    }
}
