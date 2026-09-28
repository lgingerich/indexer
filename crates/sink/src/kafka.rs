//! The Kafka sink: envelopes to a Kafka-protocol topic.
//!
//! This is the Kafka *protocol*, not Kafka the product: Redpanda speaks the same
//! wire protocol, so one sink covers both it and Apache Kafka, and no
//! Redpanda-specific code belongs here.
//!
//! The client is [`rdkafka`](https://docs.rs/rdkafka), the maintained Rust binding
//! to `librdkafka`. It is the production-grade choice: `librdkafka` owns the
//! accumulator, batching, compression, retries, and idempotence.
//!
//! The sink takes a ready [`BaseProducer`] rather than building one, because the
//! producer's settings *are* the runtime's policy — broker addresses, acks,
//! compression, linger, SASL/TLS, the delivery callback. A library cannot guess
//! them; the runtime creates the producer with whatever it needs and hands it in.
//! The cost of that choice is the C build dependency (`rdkafka-sys` compiles
//! `librdkafka` with `cc`), which the crate otherwise avoids (see the `native-tls`
//! note in `Cargo.toml`).
//!
//! Two contracts are settled here, shared with the other sinks:
//!
//! - **Rendering.** Bytes come from `serde_json::to_string(envelope)`, the same
//!   encoding [`StdoutJsonSink`](crate::StdoutJsonSink) writes, so stdout and the
//!   broker never drift apart.
//! - **Key.** Each record is keyed by its chain, not by
//!   [`Event::dedupe_key`](wire::envelope::Event::dedupe_key). `librdkafka`'s
//!   default partitioner places equal keys on one partition, so keying by chain
//!   keeps that chain's stream together and preserves per-chain
//!   [`sequence`](wire::envelope::Envelope::sequence) order across whatever
//!   partition count the topic has. Keying by identity would spread a chain
//!   across partitions and force consumers to buffer and reorder. `ponytail:` one
//!   chain therefore lands on one partition, so a single chain's write throughput
//!   is capped by that partition; shard by chain *and* height, or move to a log
//!   with a total order, if that ceiling is ever hit.
//!
//! `publish` only enqueues — `BaseProducer::send` returns as soon as the record is
//! in `librdkafka`'s accumulator, so the caller is never blocked on a delivery
//! report. [`flush`](EventSink::flush) is the durability point and the drain.
//! `ponytail:` delivery failures between flushes are reported by `librdkafka`'s
//! own callback, not surfaced as a `Result` here; a delivery report carried back
//! through [`EventSink`] is the upgrade path if a lost record must fail the
//! pipeline.

use std::time::Duration;

use anyhow::Context as _;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer as _};
use wire::envelope::Envelope;

use crate::EventSink;

/// How long [`flush`](EventSink::flush) waits for the accumulator to drain.
///
/// Not a config knob: a runtime that wants a different drain can call
/// `Producer::flush` on its own producer after the pipeline stops.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(30);

/// How long to spend serving delivery callbacks per `publish`, and the enqueue
/// timeout once the producer queue is full.
const POLL_INTERVAL: Duration = Duration::from_millis(0);

/// Produces envelopes to a Kafka-protocol topic.
pub struct KafkaSink {
    producer: BaseProducer,
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
    /// Wraps a producer the runtime already built and tuned.
    ///
    /// The runtime owns the producer's settings: `bootstrap.servers`,
    /// `message.timeout.ms`, compression, idempotence, SASL/TLS, and the delivery
    /// callback all come from how it was created here.
    #[must_use]
    pub fn new(producer: BaseProducer, topic: impl Into<String>) -> Self {
        Self {
            producer,
            topic: topic.into(),
        }
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
    async fn publish(&mut self, envelope: &Envelope) -> anyhow::Result<()> {
        // Serve any delivery callbacks that are ready first, so a stuck broker
        // does not accumulate unpolled reports; `POLL_INTERVAL` of zero makes this
        // a non-blocking poll.
        self.producer.poll(POLL_INTERVAL);
        let payload = serde_json::to_string(envelope)?;
        let record = BaseRecord::to(&self.topic)
            .payload(payload.as_bytes())
            .key(Self::partition_key(envelope));
        self.producer
            .send(record)
            .map_err(|(error, _message)| error)
            .context("enqueue to broker")
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.producer.flush(FLUSH_TIMEOUT).context("drain producer")
    }
}

#[cfg(test)]
mod tests {
    use wire::envelope::{ChainId, Envelope, Event, Finalized};

    use crate::kafka::KafkaSink;

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
}
