//! Kafka-protocol connectors: envelopes to and from a topic.
//!
//! This is the Kafka *protocol*, not Kafka the product: Redpanda speaks the same wire
//! protocol, so one module covers both it and Apache Kafka, and no Redpanda-specific
//! code belongs here.
//!
//! The client is [`rdkafka`](https://docs.rs/rdkafka), the maintained Rust binding to
//! `librdkafka`. It carries the crate's one C build dependency (`rdkafka-sys` compiles
//! `librdkafka` with `cc`), which is why it sits behind the `kafka` feature.
//!
//! # Two halves of one boundary
//!
//! [`KafkaSink`] produces and [`KafkaSource`] consumes, so a stage that transforms the
//! stream is a source on one topic and a sink on another. They live together because
//! they share the two contracts that have to agree for that to work:
//!
//! - **Rendering.** Bytes come from `serde_json::to_string(envelope)`, the same
//!   encoding [`StdoutJsonSink`](crate::StdoutJsonSink) writes, so stdout and the
//!   broker never drift apart. [`KafkaSource`] is the inverse.
//! - **Key.** Each record is keyed by its chain, not by
//!   [`Event::dedupe_key`](wire::envelope::Event::dedupe_key). `librdkafka`'s default
//!   partitioner places equal keys on one partition, so keying by chain keeps that
//!   chain's stream together and preserves per-chain
//!   [`sequence`](wire::envelope::Envelope::sequence) order across whatever partition
//!   count the topic has. Keying by identity would spread a chain across partitions and
//!   force consumers to buffer and reorder. `ponytail:` one chain therefore lands on
//!   one partition, so a single chain's write throughput is capped by that partition;
//!   shard by chain *and* height, or move to a log with a total order, if that ceiling
//!   is ever hit.
//!
//! Both take a ready client rather than building one, because the client's settings
//! *are* the runtime's policy — brokers, acks, compression, linger, group id, offset
//! reset, commit mode, rebalance callbacks, the delivery callback. A library cannot
//! guess them; the runtime creates each client with whatever it needs and hands it in.

use std::time::Duration;

use anyhow::Context as _;
use rdkafka::consumer::{CommitMode, Consumer as _, StreamConsumer};
use rdkafka::message::Message as _;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer as _};
use wire::envelope::Envelope;

use crate::{EnvelopeSource, EventSink};

/// How long [`flush`](EventSink::flush) waits for the accumulator to drain.
///
/// Not a config knob: a runtime that wants a different drain can call
/// `Producer::flush` on its own producer after the pipeline stops.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(30);

/// How long [`KafkaSink::publish`] spends serving delivery callbacks, and the enqueue
/// timeout once the producer queue is full.
///
/// Zero makes it a non-blocking poll, which is enough to stop a stuck broker from
/// accumulating unpolled reports without ever stalling the pipeline.
const POLL_INTERVAL: Duration = Duration::from_millis(0);

/// Produces envelopes to a Kafka-protocol topic.
///
/// `publish` only enqueues — `BaseProducer::send` returns as soon as the record is in
/// `librdkafka`'s accumulator, so the caller is never blocked on a delivery report.
/// [`flush`](EventSink::flush) is the durability point and the drain.
/// `ponytail:` delivery failures between flushes are reported by `librdkafka`'s own
/// callback, not surfaced as a `Result` here; a delivery report carried back through
/// [`EventSink`] is the upgrade path if a lost record must fail the pipeline.
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
    /// callback all come from how it was created.
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
        // Serve any delivery callbacks that are ready first, so a stuck broker does
        // not accumulate unpolled reports; `POLL_INTERVAL` of zero makes this a
        // non-blocking poll.
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

/// Yields envelopes from a Kafka-protocol topic.
///
/// Takes a ready [`StreamConsumer`] the runtime built and subscribed, and yields
/// envelopes in the order the broker delivers them. Like the sink, it knows nothing
/// about chains — the key a record was written under is the partitioner's business,
/// not this one's.
///
/// # Why a stream consumer
///
/// [`StreamConsumer`] polls itself as messages are extracted, which lets the caller
/// drive it from an async loop without an explicit poll tick. It also integrates with
/// `librdkafka`'s liveness detection, so group membership is kept alive by the same
/// loop that processes records.
///
/// # Committing offsets
///
/// This source does **not** commit. `librdkafka`'s auto-commit is left to the runtime's
/// configuration, and a caller that wants a deliberate commit point calls
/// [`KafkaSource::commit`] after its own flush. That split is deliberate: the right
/// commit point is a property of how the *caller* delivers, so a source that committed
/// on its own would either commit before a downstream flush — losing records on a crash
/// — or after, with no way to know when.
pub struct KafkaSource {
    consumer: StreamConsumer,
}

impl std::fmt::Debug for KafkaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSource").finish_non_exhaustive()
    }
}

impl KafkaSource {
    /// Wraps a consumer the runtime already built and subscribed.
    ///
    /// The runtime owns `group.id`, `auto.offset.reset`, `enable.auto.commit`, the
    /// rebalance callbacks, and every other setting; a library cannot guess them.
    #[must_use]
    pub const fn new(consumer: StreamConsumer) -> Self {
        Self { consumer }
    }

    /// Commits the offsets of everything consumed so far.
    ///
    /// The caller's durability point, to be called after its own output is flushed.
    /// Committing before a flush risks losing records on a crash; never committing
    /// risks replaying from the last commit, which is safe where the transform applied
    /// between the two is a pure function.
    ///
    /// # Errors
    ///
    /// Returns an error if the commit is rejected by the broker.
    pub fn commit(&self) -> anyhow::Result<()> {
        self.consumer
            .commit_consumer_state(CommitMode::Async)
            .context("commit offsets")
    }
}

impl EnvelopeSource for KafkaSource {
    async fn next(&mut self) -> anyhow::Result<Option<Envelope>> {
        let message = self.consumer.recv().await.context("receive from broker")?;
        let payload = message.payload().context("record has no payload")?;
        // The bytes are whatever the producing sink rendered, so this is the inverse of
        // the one encoding the module docs describe.
        let envelope: Envelope = serde_json::from_slice(payload)
            .context("payload is not an envelope from this indexer")?;
        Ok(Some(envelope))
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use wire::envelope::{ChainId, Envelope, Event, Finalized};

    use super::KafkaSink;

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

    /// The record is keyed by chain, so every event of one chain partitions together
    /// and keeps its `sequence` order.
    #[test]
    fn partition_key_is_the_chain() {
        assert_eq!(KafkaSink::partition_key(&envelope("base")), "base");
        assert_eq!(KafkaSink::partition_key(&envelope("ethereum")), "ethereum");
    }

    /// The sink's rendering and the source's parsing are inverses, which is the whole
    /// contract between the two halves. Pinned here because a change to either alone
    /// would otherwise only show up against a live broker.
    #[test]
    fn the_rendering_round_trips_through_the_parsing() {
        let source = envelope("base");
        let rendered = serde_json::to_string(&source).expect("envelope renders");
        let parsed: Envelope = serde_json::from_str(&rendered).expect("envelope parses");
        assert_eq!(parsed, source);
    }
}
