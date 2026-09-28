//! The Kafka source: envelopes from a Kafka-protocol topic.
//!
//! The mirror of [`KafkaSink`](crate::kafka): it takes a ready
//! [`StreamConsumer`] the runtime built and tuned, and yields envelopes in the order
//! the broker delivers them. Like the sink, it knows nothing about chains — the key a
//! record was written under is the partitioner's business, not this one's.
//!
//! # Why a stream consumer
//!
//! [`StreamConsumer`] polls itself as messages are extracted, which is what lets the
//! caller drive it from an async loop without an explicit poll tick. It also
//! integrates with `librdkafka`'s liveness detection, so the group membership is kept
//! alive by the same loop that processes records.
//!
//! # Committing offsets
//!
//! This source does **not** commit. `librdkafka`'s auto-commit is left to the runtime's
//! configuration, and a caller that wants a deliberate commit point calls
//! [`KafkaSource::commit`] after its own flush. That split is deliberate: the right
//! commit point is a property of how the *caller* delivers, so a source that committed
//! on its own would either commit before a downstream flush — losing records on a
//! crash — or after, with no way to know when.
//!
//! A decode stage that commits per record is at-least-once and idempotent, because the
//! transform is a pure function and a redelivered record decodes to the same output.

use anyhow::Context as _;
use rdkafka::consumer::{Consumer as _, StreamConsumer};
use rdkafka::message::Message as _;
use wire::envelope::Envelope;

use crate::EnvelopeSource;

/// Yields envelopes from a Kafka-protocol topic.
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
    /// risks replaying from the last commit, which is safe here because a redelivered
    /// record transforms to the same output.
    ///
    /// # Errors
    ///
    /// Returns an error if the commit is rejected by the broker.
    pub fn commit(&self) -> anyhow::Result<()> {
        self.consumer
            .commit_consumer_state(rdkafka::consumer::CommitMode::Async)
            .context("commit offsets")
    }
}

impl EnvelopeSource for KafkaSource {
    async fn next(&mut self) -> anyhow::Result<Option<Envelope>> {
        let message = self.consumer.recv().await.context("receive from broker")?;
        let payload = message.payload().context("record has no payload")?;
        // The bytes are whatever the producing sink rendered, which is the same JSON
        // every sink writes, so this is the inverse of that one encoding.
        let envelope: Envelope = serde_json::from_slice(payload)
            .context("payload is not an envelope from this indexer")?;
        Ok(Some(envelope))
    }
}
