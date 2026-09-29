//! In-process topics: envelopes from one stage to another with no broker.
//!
//! The same [`EnvelopeSource`]/[`EnvelopeSink`]
//! boundary Kafka implements, backed by in-memory queues. It carries the pipeline's
//! *architecture* without the broker — the stages, the topics, the publish → flush →
//! commit order are identical — so a single process can run the whole pipeline with
//! `kind = "memory"`.
//!
//! # What it is not
//!
//! It is not durable and not resumable. A crash loses whatever is in flight, and there is
//! no offset to restart from, so a run always begins at the live tip. It is for a
//! single-process run — a local end-to-end, a test, an offline replay — not a deployment.
//! The transport choice *is* the durability choice.
//!
//! # Shape
//!
//! A [`MemoryBus`] holds named topics. A sink publishes to one; a source takes from one;
//! a topic with no subscriber drops what it is given, matching a broker topic nobody
//! reads. Publishing copies the envelope once per subscriber, because the raw topic has
//! two — decode and storage.
//!
//! Queues are **bounded**, so a subscriber that falls behind applies backpressure all
//! the way to ingest rather than losing events. That is deliberately not a broadcast
//! channel, which would overwrite the oldest record on lag: silently dropping a store
//! write is the one failure a store must never have.
//!
//! The ceiling that implies: a subscriber that holds its receiver but never drains it —
//! a stage parked on a downstream that is not moving — blocks the topic's publish loop,
//! and therefore every other publisher and subscriber of that topic, until it drains or
//! is dropped. That is backpressure, not a fault, and it is the deliberate trade for
//! never losing a record. Only a subscriber whose receiver has been *dropped* is retired;
//! a stuck-but-alive one is meant to be felt. `ponytail:` if a deployment needs a stalled
//! consumer to be evicted instead of halting the topic, it needs a broker, whose retention
//! policy is the thing that bounds the wait.
//!
//! `ponytail:` a publisher round-robins across subscribers rather than over per-topic
//! queues, so fairness across topics is approximate; a scheduler over topic queues is the
//! upgrade if a large replay needs it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use crate::connectors::{EnvelopeSink, EnvelopeSource};
use crate::wire::envelope::Envelope;

/// How many envelopes a subscriber may fall behind before the publisher blocks.
///
/// Large enough that a burst absorbs without stalling the publisher, small enough that a
/// stopped consumer is felt rather than growing the process without bound. Not a config
/// knob: a deployment that needs this tuned needs a broker.
const CAPACITY: usize = 1_024;

/// A sender to one subscriber, tagged so the publisher can retire it once it is gone.
type Sender = (usize, mpsc::Sender<Envelope>);

/// The shared topics: one bounded queue per subscriber, keyed by topic name.
type TopicMap = Mutex<HashMap<String, Vec<Sender>>>;

/// Locks `mutex`, recovering from a poisoned one rather than panicking.
///
/// Poisoning only means a previous holder panicked mid-operation; the map it guards is
/// still a valid set of queues, so recovering is right — a panicked stage must not take
/// the bus down with it.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A set of in-process topics the stages share.
///
/// Clone it to hand one handle to each stage; every clone refers to the same topics.
#[derive(Clone, Debug, Default)]
pub struct MemoryBus {
    topics: Arc<TopicMap>,
    next: Arc<Mutex<usize>>,
}

impl MemoryBus {
    /// An empty bus with no topics.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A sink that publishes to `topic`.
    #[must_use]
    pub fn sink(&self, topic: impl Into<String>) -> MemorySink {
        MemorySink {
            bus: self.clone(),
            topic: topic.into(),
        }
    }

    /// A source that reads `topic`.
    ///
    /// Subscribes here rather than on first poll, so a publisher that starts before the
    /// reader still finds the subscriber and does not drop the record.
    #[must_use]
    pub fn source(&self, topic: impl Into<String>) -> MemorySource {
        let (sender, receiver) = mpsc::channel(CAPACITY);
        let topic = topic.into();
        let id = {
            let mut next = lock(&self.next);
            let id = *next;
            *next = next.wrapping_add(1);
            id
        };
        lock(&self.topics)
            .entry(topic)
            .or_default()
            .push((id, sender));
        MemorySource { receiver }
    }
}

/// Publishes envelopes to one in-process topic.
#[derive(Debug)]
pub struct MemorySink {
    bus: MemoryBus,
    topic: String,
}

impl EnvelopeSink for MemorySink {
    async fn publish(&mut self, envelope: &Envelope) -> anyhow::Result<()> {
        // Clone the subscriber list, then release the lock before any `.await`: a slow
        // subscriber blocks on send, and holding the lock across it would block every
        // other publisher and the subscribe path too.
        let senders = lock(&self.bus.topics)
            .get(&self.topic)
            .cloned()
            .unwrap_or_default();
        if senders.is_empty() {
            // No subscriber, like a broker topic nobody reads: the record is dropped
            // rather than buffered for a reader that may never come.
            return Ok(());
        }
        for (id, sender) in &senders {
            if sender.send(envelope.clone()).await.is_err() {
                // The receiver was dropped; retire it and move on. A *parked* subscriber
                // with a live receiver blocks here on purpose — that is backpressure, not
                // a fault, and it is bounded by the queue's capacity. See the module docs.
                if let Some(senders) = lock(&self.bus.topics).get_mut(&self.topic) {
                    senders.retain(|(other, _)| other != id);
                }
            }
        }
        Ok(())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        // Nothing is buffered: each publish is already in the subscriber's queue, so a
        // flush has nothing to make durable.
        Ok(())
    }
}

/// Yields envelopes from one in-process topic.
#[derive(Debug)]
pub struct MemorySource {
    receiver: mpsc::Receiver<Envelope>,
}

impl EnvelopeSource for MemorySource {
    async fn next(&mut self) -> anyhow::Result<Option<Envelope>> {
        // `None` means every sender dropped, which is the topic ending.
        Ok(self.receiver.recv().await)
    }

    async fn commit(&mut self) -> anyhow::Result<()> {
        // There is no offset to advance: in-memory delivery is not resumable. The
        // source still accepts the call, because the publish → flush → commit order is
        // the stage's, and it must not change with the transport.
        Ok(())
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::time::Duration;

    use alloy_primitives::B256;

    use crate::connectors::{EnvelopeSink as _, EnvelopeSource as _};
    use crate::wire::envelope::{ChainId, Envelope, Event, Finalized};

    use super::MemoryBus;

    fn envelope(sequence: u64) -> Envelope {
        Envelope::new(
            ChainId::new("base"),
            sequence,
            Event::Finalized(Finalized {
                height: sequence,
                hash: B256::from([0x11; 32]),
            }),
        )
    }

    /// A sink and a source on one topic round trip in order, and the checkpoint is a
    /// no-op that still succeeds so the stage's ordering is unchanged.
    #[tokio::test]
    async fn a_sink_and_source_round_trip() {
        let bus = MemoryBus::new();
        let mut sink = bus.sink("raw.chain");
        let mut source = bus.source("raw.chain");

        for sequence in 0..8 {
            sink.publish(&envelope(sequence)).await.expect("publish");
        }
        for sequence in 0..8 {
            let got = source.next().await.expect("read").expect("a record");
            assert_eq!(got.sequence, sequence);
        }
        source.commit().await.expect("commit is a no-op");
    }

    /// The raw topic has two subscribers — decode and storage — and each gets a copy, so
    /// choosing an in-memory bus does not silently drop one of them.
    #[tokio::test]
    async fn every_subscriber_gets_a_copy() {
        let bus = MemoryBus::new();
        let mut sink = bus.sink("raw.chain");
        let mut store = bus.source("raw.chain");
        let mut decode = bus.source("raw.chain");

        sink.publish(&envelope(7)).await.expect("publish");

        assert_eq!(
            store
                .next()
                .await
                .expect("read")
                .expect("a record")
                .sequence,
            7
        );
        assert_eq!(
            decode
                .next()
                .await
                .expect("read")
                .expect("a record")
                .sequence,
            7
        );
    }

    /// A topic with no subscriber drops the record, like a broker topic nobody reads;
    /// the publisher is never blocked waiting for a reader that may never come.
    #[tokio::test]
    async fn a_topic_with_no_subscriber_drops() {
        let bus = MemoryBus::new();
        let mut sink = bus.sink("nobody.listens");
        tokio::time::timeout(Duration::from_millis(100), sink.publish(&envelope(1)))
            .await
            .expect("publishing to an unread topic does not block")
            .expect("publish");
    }

    /// A subscriber that stops is retired: the publisher keeps going, which is what lets
    /// one stage end without wedging the others.
    #[tokio::test]
    async fn a_dropped_subscriber_does_not_stall_the_publisher() {
        let bus = MemoryBus::new();
        let mut sink = bus.sink("raw.chain");
        {
            let _gone = bus.source("raw.chain");
        }
        let mut alive = bus.source("raw.chain");

        tokio::time::timeout(Duration::from_millis(200), async {
            sink.publish(&envelope(1)).await.expect("publish");
        })
        .await
        .expect("a retired subscriber does not block the publisher");
        assert_eq!(
            alive.next().await.expect("read").expect("record").sequence,
            1
        );
    }

    /// The other half of that pair, and the deliberate ceiling: a subscriber that holds
    /// its receiver but never reads is not retired, so it blocks the publish once the
    /// queue fills. That is backpressure reaching the publisher — the price of never
    /// dropping a record — so it must be felt, not silently skipped.
    #[tokio::test]
    async fn a_parked_subscriber_applies_backpressure_rather_than_being_retired() {
        let bus = MemoryBus::new();
        let mut sink = bus.sink("raw.chain");
        // Held, never polled: the queue fills and then the publisher waits.
        let _parked = bus.source("raw.chain");

        let published = tokio::time::timeout(Duration::from_secs(2), async {
            let mut count = 0_u64;
            loop {
                sink.publish(&envelope(count)).await.expect("publish");
                count += 1;
            }
        })
        .await;
        assert!(
            published.is_err(),
            "a full queue must block the publisher, not drop or retire the subscriber"
        );
    }
}
