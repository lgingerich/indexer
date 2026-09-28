//! Running the decode stage: raw envelopes in, decoded envelopes out.
//!
//! Consumes one topic, applies the stateless [`Transform`], and produces to another.
//! Everything that makes this safe lives in the transform; this module is the loop that
//! drives it, plus the commit policy that decides how much is replayed after a crash.
//!
//! # Delivery
//!
//! At-least-once. The transform is a pure function of a record and the registry, so a
//! redelivered record decodes to the same output with the same `dedupe_key`, and a
//! consumer that upserts on that key is idempotent.
//!
//! The commit happens **after** the sink's flush, so a crash between the two replays a
//! batch rather than losing one. That ordering is the whole reason the source does not
//! commit on its own.

use std::time::Instant;

use anyhow::{Context as _, Result};
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer as _, StreamConsumer};
use rdkafka::producer::BaseProducer;
use tracing::{info, warn};

use crate::config::{BatchConfig, KafkaConfig};
use crate::connectors::{EnvelopeSource as _, EventSink as _, KafkaSink, KafkaSource};
use crate::decode::Transform;
use crate::decode::contracts::ContractRegistry;

/// Builds and runs the decode stage.
#[derive(Debug)]
pub struct Decode {
    kafka: KafkaConfig,
    batch: BatchConfig,
    registry: ContractRegistry,
    /// The runtime's librdkafka properties, applied to every client this stage builds.
    client: ClientConfig,
}

impl Decode {
    /// Starts a build against `kafka`.
    #[must_use]
    pub fn builder(kafka: KafkaConfig) -> DecodeBuilder {
        DecodeBuilder::new(kafka)
    }

    /// Consumes the input topic, decoding each record onto the output topic until the
    /// input ends.
    ///
    /// # Errors
    ///
    /// Returns an error when the broker cannot be reached, when a record cannot be read,
    /// or when a produce fails. A single record that does not *decode* is logged and
    /// skipped rather than failing the run, because the raw record is already on the
    /// input topic and a corrected ABI recovers it.
    pub async fn run(self) -> Result<()> {
        let output_topic = self
            .kafka
            .output_topic
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("decode kafka output_topic is required"))?
            .to_owned();

        let transform = Transform::new(self.registry);

        // The runtime's properties first, then the stage's own, so a passthrough can
        // still be overridden by the setting it would otherwise conflict with.
        let mut client = self.client.clone();
        client
            .set("group.id", &self.kafka.group)
            // A fresh group starts at the beginning rather than skipping the history it
            // was created to read.
            .set("auto.offset.reset", "earliest")
            // The commit point is this stage's decision, after a flush.
            .set("enable.auto.commit", "false");
        let consumer: StreamConsumer = client.create().context("create consumer")?;
        consumer
            .subscribe(&[&self.kafka.input_topic])
            .context("subscribe to input topic")?;

        let producer: BaseProducer = self.client.create().context("create producer")?;

        let mut source = KafkaSource::new(consumer);
        let mut sink = KafkaSink::new(producer, output_topic.clone());

        info!(
            input = %self.kafka.input_topic,
            output = %output_topic,
            group = %self.kafka.group,
            "decode started"
        );

        let mut pending = 0;
        let mut last_flush = Instant::now();
        loop {
            let Some(envelope) = source.next().await? else {
                info!("input topic ended");
                break;
            };

            let outputs = match transform.apply(envelope) {
                Ok(outputs) => outputs,
                Err(error) => {
                    // One undecodable log must not stall the stream, but it must not be
                    // silent either: the usual cause is an ABI from the wrong block
                    // range, and the raw log is already on the input topic, so skipping
                    // it loses nothing a corrected ABI could not recover.
                    warn!(%error, "skipping a record that does not decode");
                    continue;
                }
            };
            for output in &outputs {
                sink.publish(output).await?;
            }
            pending += 1;

            // Flush on either bound, then commit: the offset never advances past bytes
            // that are not yet on the output topic.
            if pending >= self.batch.records || last_flush.elapsed() >= self.batch.every {
                sink.flush().await?;
                source.commit()?;
                pending = 0;
                last_flush = Instant::now();
            }
        }

        sink.flush().await?;
        source.commit()?;
        Ok(())
    }
}

/// Builds a [`Decode`].
#[derive(Debug)]
pub struct DecodeBuilder {
    kafka: KafkaConfig,
    batch: BatchConfig,
    registry: ContractRegistry,
    client: ClientConfig,
}

impl DecodeBuilder {
    /// Starts a build against `kafka`.
    #[must_use]
    pub fn new(kafka: KafkaConfig) -> Self {
        let mut client = ClientConfig::new();
        client.set("bootstrap.servers", &kafka.brokers);
        Self {
            kafka,
            batch: BatchConfig::default(),
            registry: ContractRegistry::default(),
            client,
        }
    }

    /// Sets how many records to consume before flushing and committing.
    #[must_use]
    pub const fn batch(mut self, batch: BatchConfig) -> Self {
        self.batch = batch;
        self
    }

    /// Sets the librdkafka properties every client this stage builds inherits.
    ///
    /// The runtime's passthrough lands here, so a property set once reaches both the
    /// producer and the consumer.
    #[must_use]
    pub fn client(mut self, client: ClientConfig) -> Self {
        self.client = client;
        self
    }

    /// Sets the contract registry this stage decodes with.
    #[must_use]
    pub fn registry(mut self, registry: ContractRegistry) -> Self {
        self.registry = registry;
        self
    }

    /// Finishes the build, discovering every ABI in the directory.
    ///
    /// # Errors
    ///
    /// Returns an error when a file in the directory is not `{chain}.{address}.json`,
    /// when its address does not parse, or when its contents are not an ABI, and when
    /// the Kafka configuration has no output topic.
    pub fn build(self) -> Result<Decode> {
        if self.registry.is_empty() {
            warn!("no contracts registered; every log will pass through undecoded");
        } else {
            info!(addresses = self.registry.len(), "loaded contract registry");
        }
        if self.kafka.output_topic.is_none() {
            anyhow::bail!("decode kafka output_topic is required");
        }
        Ok(Decode {
            kafka: self.kafka,
            batch: self.batch,
            registry: self.registry,
            client: self.client,
        })
    }
}
