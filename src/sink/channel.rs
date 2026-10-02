//! The channel from decode to storage: blocks of envelopes, in memory, bounded.
//!
//! Ingest and decode run as direct calls in one task; storage runs in another, and this
//! channel is the only hop between them. It exists for one reason: a store stalls (a
//! checkpoint, a slow fsync) and ingest must not stop reading heads while it does. The
//! channel absorbs the stall; nothing else about it is a queue's job.
//!
//! # The unit is a block
//!
//! [`ChannelSink`] collects the envelopes published between two flushes — which the
//! pipeline calls once per block — and sends them as one `Vec<Envelope>`. A block's raw
//! rows and its decoded rows therefore reach the store together and commit together,
//! never split across a crash.
//!
//! # Bounded, with backpressure
//!
//! The channel holds a few dozen blocks (32). When it is full, [`ChannelSink::flush`] waits,
//! which stops ingest from taking the next head until storage catches up. That is
//! deliberate: the alternative is an unbounded buffer that hides lag and loses more on a
//! crash. Capacity times block time is how long a stall can last before ingest itself
//! feels it.
//!
//! # What it is not
//!
//! Not durable and not resumable: a crash loses whatever is in flight. That is safe only
//! because the chain and the store are the record — the store's high-water mark says
//! where to resume, and the node can serve the blocks after it.

use tokio::sync::mpsc;

use crate::sink::{EnvelopeSink, SinkError};
use crate::wire::envelope::Envelope;

/// How many blocks the channel holds before the sender waits.
///
/// A stall of `CAPACITY` block times is absorbed silently. Small on purpose: a deeper
/// buffer only delays the moment the lag is felt, and everything in it is lost on a
/// crash. Not a setting; if a deployment needs more headroom the store is too slow.
const CAPACITY: usize = 32;

/// Opens the channel: the sending half decode publishes into, and the receiving half
/// storage drains.
#[must_use]
pub(crate) fn open() -> (ChannelSink, ChannelReceiver) {
    let (sender, receiver) = mpsc::channel(CAPACITY);
    (
        ChannelSink {
            sender,
            batch: Vec::new(),
        },
        ChannelReceiver { receiver },
    )
}

/// The sending half: buffers a block's envelopes and sends them on flush.
#[derive(Debug)]
pub(crate) struct ChannelSink {
    sender: mpsc::Sender<Vec<Envelope>>,
    batch: Vec<Envelope>,
}

impl EnvelopeSink for ChannelSink {
    async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
        self.batch.push(envelope);
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), SinkError> {
        if self.batch.is_empty() {
            return Ok(());
        }
        // Hand the buffer over rather than copy it, leaving one already sized for the
        // next block: blocks vary in log count, but not enough for this to lose to
        // regrowing from zero.
        let next = Vec::with_capacity(self.batch.len());
        let batch = std::mem::replace(&mut self.batch, next);
        // Storage has stopped. The batch of envelopes inside `SendError` is this
        // layer's own data and nothing else can use it, so the error is translated to
        // `StorageClosed` and `Vec<Envelope>` stops appearing in the sink's error type:
        // a caller matches the variant, and the channel's concrete type stays internal
        // to this module.
        self.sender
            .send(batch)
            .await
            .map_err(|_| SinkError::StorageClosed)?;
        Ok(())
    }
}

/// The receiving half: what storage drains.
#[derive(Debug)]
pub(crate) struct ChannelReceiver {
    receiver: mpsc::Receiver<Vec<Envelope>>,
}

impl ChannelReceiver {
    /// Drains blocks into `sink` until the sending half is dropped, returning how many
    /// envelopes it stored.
    ///
    /// Each pass takes one block, then whatever else is already waiting, up to
    /// `max_records` envelopes, and flushes once. Caught up, that is one block per flush;
    /// behind, the backlog goes in fewer, larger commits, which is how a store recovers
    /// from a stall. A block is never split across flushes, so a commit boundary is
    /// always a block boundary.
    ///
    /// # Errors
    ///
    /// Returns an error when the sink cannot accept an envelope or flush. The blocks
    /// still in the channel are dropped, and the sending half fails on its next flush.
    pub(crate) async fn drain<K: EnvelopeSink>(
        mut self,
        sink: &mut K,
        max_records: usize,
    ) -> Result<u64, SinkError> {
        let mut stored = 0_u64;
        while let Some(first) = self.receiver.recv().await {
            let mut pending = first.len();
            for envelope in first {
                sink.publish(envelope).await?;
            }
            while pending < max_records {
                let Ok(block) = self.receiver.try_recv() else {
                    break;
                };
                pending += block.len();
                for envelope in block {
                    sink.publish(envelope).await?;
                }
            }
            sink.flush().await?;
            stored += pending as u64;
        }
        Ok(stored)
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

    use crate::sink::{EnvelopeSink, SinkError};
    use crate::wire::envelope::{ChainId, Envelope, Event, Finalized};

    use super::open;

    /// A finalized marker at `height`, which stands in for a block's worth of
    /// envelopes — the channel carries no field of its own, so a test needs a value to
    /// tell one envelope from another, and a height is one every dataset has.
    fn envelope(height: u64) -> Envelope {
        Envelope::new(
            ChainId::new("base"),
            Event::Finalized(Finalized {
                height,
                hash: B256::from([0x11; 32]),
            }),
        )
    }

    /// Records what reached it as flushed batches, so a test sees the commit boundaries.
    ///
    /// Keyed on [`Event::dedupe_key`] rather than a field the fixture happens to set, so
    /// publishing some other event kind is recorded rather than panicked on. A fake that
    /// kills the process is a worse failure report than a wrong assertion, and the
    /// assertion is what should say so.
    #[derive(Default)]
    struct Batches {
        open: Vec<String>,
        flushed: Vec<Vec<String>>,
    }

    impl EnvelopeSink for Batches {
        async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
            self.open.push(envelope.event.dedupe_key());
            Ok(())
        }

        async fn flush(&mut self) -> Result<(), SinkError> {
            self.flushed.push(std::mem::take(&mut self.open));
            Ok(())
        }
    }

    /// The keys [`envelope`] produces for these heights, so a test states what it
    /// expects to arrive without the fake having to unwrap the envelope to find out.
    fn keys(heights: impl IntoIterator<Item = u64>) -> Vec<String> {
        heights.into_iter().map(envelope_key).collect()
    }

    fn envelope_key(height: u64) -> String {
        envelope(height).event.dedupe_key()
    }

    /// Publishes one block's worth of markers and flushes, as the pipeline does.
    async fn send_block(sink: &mut super::ChannelSink, heights: std::ops::Range<u64>) {
        for height in heights {
            sink.publish(envelope(height)).await.expect("publish");
        }
        sink.flush().await.expect("flush");
    }

    /// Nothing crosses the channel until the flush, and then the block crosses whole and
    /// in order: the flush is the block boundary.
    #[tokio::test]
    async fn a_block_crosses_whole_and_only_on_flush() {
        let (mut sink, receiver) = open();
        sink.publish(envelope(0)).await.expect("publish");
        sink.publish(envelope(1)).await.expect("publish");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), async {
                let mut store = Batches::default();
                // Sender still alive and nothing flushed, so this waits.
                receiver.drain(&mut store, 100).await
            })
            .await
            .is_err(),
            "an unflushed block must not reach storage"
        );

        let (mut sink, receiver) = open();
        send_block(&mut sink, 0..3).await;
        drop(sink);
        let mut store = Batches::default();
        let stored = receiver.drain(&mut store, 100).await.expect("drain");
        assert_eq!(stored, 3);
        assert_eq!(store.flushed, [keys(0..3)]);
    }

    /// A store that fell behind commits its backlog in one flush, but never splits a
    /// block: the bound is checked between blocks.
    #[tokio::test]
    async fn a_backlog_shares_a_flush_and_blocks_are_never_split() {
        let (mut sink, receiver) = open();
        send_block(&mut sink, 0..3).await;
        send_block(&mut sink, 3..6).await;
        send_block(&mut sink, 6..9).await;
        drop(sink);

        // Bound of 4: the first block (3) is under it, so the second joins (6); the
        // third would start past the bound and gets its own flush.
        let mut store = Batches::default();
        let stored = receiver.drain(&mut store, 4).await.expect("drain");
        assert_eq!(stored, 9);
        assert_eq!(store.flushed, [keys(0..6), keys(6..9)]);
    }

    /// The channel is bounded, so a stalled store is felt as a waiting sender rather
    /// than as unbounded growth or dropped blocks.
    #[tokio::test]
    async fn a_full_channel_blocks_the_sender() {
        let (mut sink, _parked) = open();
        let sent = tokio::time::timeout(Duration::from_secs(2), async {
            let mut sequence = 0;
            loop {
                send_block(&mut sink, sequence..sequence + 1).await;
                sequence += 1;
            }
        })
        .await;
        assert!(sent.is_err(), "a full channel must make the sender wait");
    }

    /// A store that has stopped is an error on the sender's next flush, not a silent
    /// drop: ingest must not keep indexing into nothing.
    ///
    /// The variant is matched, not the message. This used to assert the error was tokio's
    /// `SendError<Vec<Envelope>>` by downcasting, which required the error to be a
    /// type-erased one to downcast at all; matching `StorageClosed` states the contract
    /// directly and keeps the channel's own types out of the assertion.
    #[tokio::test]
    async fn a_stopped_store_fails_the_next_flush() {
        let (mut sink, receiver) = open();
        drop(receiver);
        sink.publish(envelope(0)).await.expect("publish buffers");
        let error = sink.flush().await.expect_err("nobody is reading");
        assert!(
            matches!(error, SinkError::StorageClosed),
            "a dead store must be reported as such, not as a raw channel error: {error}"
        );
    }

    /// A sink error ends the drain with that error, so a failing store stops storage
    /// rather than being read as an ended stream.
    #[tokio::test]
    async fn a_failing_sink_ends_the_drain_with_its_error() {
        /// A sink whose storage is out of space, which is a real store condition and so
        /// carries the same [`SinkError::Write`] a `stdout` failure would.
        struct OutOfSpace;

        impl EnvelopeSink for OutOfSpace {
            async fn publish(&mut self, _envelope: Envelope) -> Result<(), SinkError> {
                Err(SinkError::Write(std::io::Error::other("disk is full")))
            }
        }

        let (mut sink, receiver) = open();
        send_block(&mut sink, 0..1).await;
        let error = receiver
            .drain(&mut OutOfSpace, 100)
            .await
            .expect_err("the sink failed");
        assert!(
            matches!(error, SinkError::Write(_)),
            "the sink's own error must survive the drain, not be reshaped: {error}"
        );
        assert!(error.to_string().contains("disk is full"), "{error}");
    }
}
