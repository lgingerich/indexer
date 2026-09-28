//! The Kafka sink: envelopes to a Kafka-protocol topic.
//!
//! This is the Kafka *protocol*, not Kafka the product: Redpanda speaks the same
//! wire protocol, so one sink covers both it and Apache Kafka, and no
//! Redpanda-specific code belongs here.
//!
//! The client is [`rdkafka`](https://docs.rs/rdkafka), the maintained Rust binding
//! to `librdkafka`. It is the production-grade choice: `librdkafka` owns the
//! accumulator, batching, compression, retries, and idempotence, so the sink
//! hands it one record and gets a delivery report. The cost is a C build
//! dependency — `rdkafka-sys` compiles `librdkafka` with `cc` — which the crate
//! otherwise avoids (see the `native-tls` note in `Cargo.toml`); the pure-Rust
//! `rskafka` was rejected for a thinner producer.
//!
//! Two contracts are settled here, shared with the other sinks:
//!
//! - **Rendering.** Bytes come from `serde_json::to_string(envelope)`, the same
//!   encoding [`StdoutJsonSink`](crate::sink::StdoutJsonSink) writes, so stdout
//!   and the broker never drift apart.
//! - **Key.** Each record is keyed by its chain, not by
//!   [`Event::dedupe_key`](crate::envelope::Event::dedupe_key). `librdkafka`'s
//!   default partitioner places equal keys on one partition, so keying by chain
//!   keeps that chain's stream together and preserves per-chain
//!   [`sequence`](crate::envelope::Envelope::sequence) order across whatever
//!   partition count the topic has. Keying by identity would spread a chain
//!   across partitions and force consumers to buffer and reorder. `ponytail:` one
//!   chain therefore lands on one partition, so a single chain's write throughput
//!   is capped by that partition; shard by chain *and* height, or move to a log
//!   with a total order, if that ceiling is ever hit.

use std::time::Duration;

use anyhow::Context as _;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use crate::envelope::Envelope;
use crate::sink::EventSink;

/// How long to retry while the producer's queue is full before giving up.
///
/// Only the enqueue phase: `message.timeout.ms` bounds the delivery itself.
const QUEUE_TIMEOUT: Duration = Duration::from_secs(5);

/// Where to reach the broker and what topic to produce to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaConfig {
    /// Comma-separated `host:port` bootstrap servers.
    pub brokers: String,
    /// The topic every chain's events are produced to.
    pub topic: String,
}

impl KafkaConfig {
    /// Builds a config from bootstrap servers and a topic.
    #[must_use]
    pub fn new(brokers: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
            topic: topic.into(),
        }
    }
}

/// Produces envelopes to a Kafka-protocol topic.
pub struct KafkaSink {
    producer: FutureProducer,
    topic: String,
}

impl std::fmt::Debug for KafkaSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSink")
            .field("topic", &self.topic)
            .finish_non_exhaustive()
    }
}

impl KafkaSink {
    /// Creates the producer for `config`'s broker.
    ///
    /// `librdkafka` connects lazily and maintains the metadata, so this does not
    /// round-trip to the broker; the first [`publish`](EventSink::publish)
    /// surfaces an unreachable broker as an error.
    ///
    /// # Errors
    ///
    /// Returns an error if the producer cannot be created, for example when a
    /// config value is not one `librdkafka` accepts.
    pub fn connect(config: &KafkaConfig) -> anyhow::Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            // Bound delivery so a stuck broker fails the pipeline instead of
            // buffering events without bound; this is the durability ceiling.
            .set("message.timeout.ms", "30000")
            .create()
            .context("create producer")?;
        Ok(Self {
            producer,
            topic: config.topic.clone(),
        })
    }

    /// The partition key for an envelope: its chain.
    ///
    /// See the module docs for why this, and not the dedupe key.
    #[must_use]
    pub fn partition_key(envelope: &Envelope) -> &str {
        envelope.chain.as_str()
    }

    /// The topic this sink produces to.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }
}

impl EventSink for KafkaSink {
    async fn publish(&self, envelope: &Envelope) -> anyhow::Result<()> {
        let payload = serde_json::to_string(envelope)?;
        let record = FutureRecord::to(&self.topic)
            .payload(payload.as_bytes())
            .key(Self::partition_key(envelope));
        self.producer
            .send(record, Timeout::After(QUEUE_TIMEOUT))
            .await
            .map_err(|(error, _message)| error)
            .context("produce to broker")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::envelope::{ChainId, Envelope, Event, Finalized};
    use crate::sink::kafka::{KafkaConfig, KafkaSink};

    fn envelope(chain: &str) -> Envelope {
        Envelope::new(
            ChainId::new(chain),
            3,
            Event::Finalized(Finalized {
                height: 9,
                hash: alloy_primitives::B256::from([0x22; 32]),
            }),
        )
    }

    /// The record is keyed by chain, so every event of one chain partitions
    /// together and keeps its `sequence` order.
    #[test]
    fn partition_key_is_the_chain() {
        assert_eq!(KafkaSink::partition_key(&envelope("base")), "base");
        assert_eq!(KafkaSink::partition_key(&envelope("ethereum")), "ethereum");
    }

    #[test]
    fn config_borrows_what_it_was_built_with() {
        let config = KafkaConfig::new("redpanda:9092", "indexer.events");
        assert_eq!(config.brokers, "redpanda:9092");
        assert_eq!(config.topic, "indexer.events");
    }
}
