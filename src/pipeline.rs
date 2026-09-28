//! The core processing layer, between [`BlockSource`] and [`EventSink`].
//!
//! The three layers split responsibilities like this:
//!
//! - A **source** knows one chain: how to hear about new heads, fetch a block, and
//!   turn it into [`Event`]s. It holds no state about what was published.
//! - A **sink** knows one destination: how to deliver an [`Envelope`]. It knows
//!   nothing about chains or ordering.
//! - The **pipeline** is the only stateful part. It drives the source, turns its
//!   events into a single ordered stream, and hands each envelope to the sink.
//!
//! Turning events into the stream means three things: assigning every envelope a
//! per-chain monotonic sequence number, checking parent-hash linkage so a reorg
//! surfaces as an [`Event::Reorg`] rather than as silently wrong data, and
//! publishing an [`Event::Finalized`] watermark whenever the chain's finalized
//! block advances.
//!
//! Sequence numbers are per event, not per block, so reclaiming them after a reorg
//! requires remembering where each block's run of sequences began. That is what the
//! bounded undo ring stores.
//!
//! What this does not do yet: it reorgs only against blocks it published in this
//! process. Backfill-to-live handoff, checkpoint resume, and rebuilding the ring
//! from durable history are deferred.

use std::collections::VecDeque;

use alloy_primitives::B256;
use anyhow::bail;
use futures_util::StreamExt as _;
use tracing::{error, info, warn};

use crate::envelope::{Envelope, Event};
use crate::sink::EventSink;
use crate::source::{BlockId, BlockSource, RawBlock};

/// The most published blocks the undo ring remembers by default.
///
/// Blocks at or below the finalized height are dropped first, since no reorg can
/// reach them, so on Ethereum the ring rarely holds more than about 64. This cap
/// matters on chains whose finality lags far behind the tip, such as L2s waiting
/// on L1, where it is the ceiling on how deep a reorg can be retracted.
pub const DEFAULT_UNDO_DEPTH: usize = 128;

/// One block's slice of the published stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PublishedBlock {
    height: u64,
    hash: B256,
    first_sequence: u64,
    event_count: u64,
}

/// Drives one source into one sink as an ordered, reorg-aware stream.
#[derive(Debug)]
pub struct Pipeline<S, K> {
    source: S,
    sink: K,
    history: VecDeque<PublishedBlock>,
    undo_depth: usize,
    sequence: u64,
    finalized_height: u64,
}

impl<S: BlockSource, K: EventSink> Pipeline<S, K> {
    /// Builds a pipeline with [`DEFAULT_UNDO_DEPTH`] of history.
    #[must_use]
    pub fn new(source: S, sink: K) -> Self {
        Self::with_undo_depth(source, sink, DEFAULT_UNDO_DEPTH)
    }

    /// Builds a pipeline that can retract reorgs up to `undo_depth` blocks deep.
    ///
    /// A depth of zero disables reorg retraction: linkage is still checked, but no
    /// sequence numbers can be reclaimed.
    #[must_use]
    pub fn with_undo_depth(source: S, sink: K, undo_depth: usize) -> Self {
        Self {
            source,
            sink,
            history: VecDeque::new(),
            undo_depth,
            sequence: 0,
            finalized_height: 0,
        }
    }

    /// The sequence number that the next published envelope will carry.
    #[must_use]
    pub const fn next_sequence(&self) -> u64 {
        self.sequence
    }

    /// Follows the live chain tip until the head subscription ends.
    ///
    /// Starts at whatever head the source reports next; it does not backfill.
    ///
    /// # Errors
    ///
    /// Returns an error when the subscription, a block fetch, or a publish fails,
    /// and when the subscription closes, since a live indexer should never stop.
    pub async fn run(&mut self) -> anyhow::Result<()> {
        let mut heads = self.source.subscribe_heads().await?;
        info!(chain = %self.source.chain(), "subscribed to heads");

        let mut last_height = 0;
        while let Some(head) = heads.next().await {
            let head = head?;
            last_height = head.height;
            let raw = self.source.block_at(head.height).await?;
            self.process_block(raw).await?;
        }
        bail!("head subscription closed after height {last_height}")
    }

    /// Publishes one block's events, emitting a reorg first if linkage broke and a
    /// finality watermark after if finality advanced.
    ///
    /// Returns the number of [`Event::Reorg`] envelopes published, which is `0` or
    /// `1`.
    ///
    /// # Errors
    ///
    /// Returns an error when the source cannot encode the block or the sink cannot
    /// deliver an event. Both are fatal: skipping an event would create a gap that
    /// violates the delivery contract.
    pub async fn process_block(&mut self, raw: RawBlock) -> anyhow::Result<u64> {
        let events = self.source.encode_block(&raw)?;
        let Some((first, _)) = events.split_first() else {
            return Ok(0);
        };

        let Event::Block {
            height,
            hash,
            parent_hash,
            ..
        } = first
        else {
            error!(
                chain = %self.source.chain(),
                first_kind = first.kind(),
                "encode_block must lead with a block marker; skipping block"
            );
            return Ok(0);
        };
        let (height, hash, parent_hash) = (*height, *hash, *parent_hash);

        let mut reorgs = 0;
        if let Some(previous) = self.history.back()
            && parent_hash != previous.hash
        {
            self.publish_reorg(height, hash, parent_hash).await?;
            reorgs = 1;
        }

        let first_sequence = self.sequence;
        let event_count = events.len() as u64;
        for event in events {
            let envelope = Envelope::new(self.source.chain().clone(), self.sequence, event);
            self.sink.publish(&envelope).await?;
            self.sequence += 1;
        }

        self.history.push_back(PublishedBlock {
            height,
            hash,
            first_sequence,
            event_count,
        });
        self.trim_unless_recent(height);
        self.advance_finality(raw.finalized).await?;

        info!(
            chain = %self.source.chain(),
            height,
            finalized_height = self.finalized_height,
            sequence = self.sequence,
            "published block"
        );
        Ok(reorgs)
    }

    /// Publishes a watermark if `finalized` is newer than the last one, and drops
    /// ring entries below it, which no reorg can reach.
    async fn advance_finality(&mut self, finalized: BlockId) -> anyhow::Result<()> {
        if finalized.height <= self.finalized_height {
            return Ok(());
        }
        self.finalized_height = finalized.height;
        self.history
            .retain(|block| block.height >= finalized.height);

        let marker = Envelope::new(
            self.source.chain().clone(),
            self.sequence,
            Event::Finalized {
                height: finalized.height,
                hash: finalized.hash,
            },
        );
        self.sink.publish(&marker).await?;
        self.sequence += 1;
        Ok(())
    }

    /// Retracts the chain from `height` onward and emits the reorg marker.
    async fn publish_reorg(
        &mut self,
        height: u64,
        new_head_hash: B256,
        actual_parent: B256,
    ) -> anyhow::Result<()> {
        let expected_parent = self.history.back().map(|block| block.hash);
        let (orphaned_hashes, reclaimed_from) = self.rewind_to(height);
        self.sequence = reclaimed_from.unwrap_or(self.sequence);
        warn!(
            chain = %self.source.chain(),
            height,
            ?expected_parent,
            %actual_parent,
            orphaned = orphaned_hashes.len(),
            sequence = self.sequence,
            "parent hash mismatch; retracting chain and publishing reorg"
        );

        let reorg = Envelope::new(
            self.source.chain().clone(),
            self.sequence,
            Event::Reorg {
                height,
                new_head_hash,
                orphaned_hashes,
            },
        );
        self.sink.publish(&reorg).await?;
        self.sequence += 1;
        Ok(())
    }

    /// Drops remembered blocks at or above `height`.
    ///
    /// Returns the orphaned block hashes, newest first, and the sequence number to
    /// resume from when a reorg is detected.
    fn rewind_to(&mut self, height: u64) -> (Vec<B256>, Option<u64>) {
        let mut orphaned = Vec::new();
        let mut reclaimed_from = None;
        while let Some(block) = self.history.pop_back() {
            if block.height < height {
                self.history.push_back(block);
                break;
            }
            orphaned.push(block.hash);
            reclaimed_from = Some(block.first_sequence);
        }
        if orphaned.len() == self.undo_depth && self.undo_depth > 0 {
            error!(
                height,
                undo_depth = self.undo_depth,
                "reorg may be deeper than the undo ring; retraction is incomplete"
            );
        }
        (orphaned, reclaimed_from)
    }

    /// Keeps the ring bounded without discarding the block just published.
    fn trim_unless_recent(&mut self, height: u64) {
        if self.history.len() <= self.undo_depth {
            return;
        }
        // The newest entry is only evicted when the source jumps backwards, which
        // is a reorg we cannot retract; swappable in favour of an error once the
        // ring is rebuilt from durable history.
        if self
            .history
            .front()
            .is_some_and(|oldest| oldest.height < height)
        {
            self.history.pop_front();
        }
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use futures_util::stream;

    use super::Pipeline;
    use alloy_primitives::{B256, TxHash};

    use crate::envelope::{ChainId, Envelope, Event};
    use crate::sink::EventSink;
    use crate::source::{BlockId, BlockSource, HeadStream, RawBlock, SourceError};

    fn hash(byte: u8) -> B256 {
        B256::from([byte; 32])
    }

    fn block_event(height: u64, hash: B256, parent_hash: B256) -> Event {
        Event::Block {
            height,
            hash,
            parent_hash,
            timestamp: 1_700_000_000 + height,
            tx_count: 1,
            raw: "{}".to_owned(),
        }
    }

    fn log_event(height: u64, block_hash: B256, item_index: u64) -> Event {
        Event::Log {
            height,
            block_hash,
            tx_id: TxHash::from([0x01; 32]),
            tx_index: 0,
            item_index,
            raw: "{}".to_owned(),
        }
    }

    /// Pops pre-scripted blocks so tests control linkage and depth directly.
    struct FakeSource {
        chain: ChainId,
        blocks: Mutex<VecDeque<Vec<Event>>>,
    }

    impl FakeSource {
        fn new(blocks: Vec<Vec<Event>>) -> Self {
            Self {
                chain: ChainId::new("ethereum"),
                blocks: Mutex::new(blocks.into()),
            }
        }
    }

    impl BlockSource for FakeSource {
        fn chain(&self) -> &ChainId {
            &self.chain
        }

        async fn subscribe_heads(&self) -> Result<HeadStream, SourceError> {
            Ok(Box::pin(stream::empty()))
        }

        async fn block_at(&self, _height: u64) -> Result<RawBlock, SourceError> {
            Err(SourceError::Transport("not scripted".to_owned()))
        }

        fn encode_block(&self, _raw: &RawBlock) -> Result<Vec<Event>, SourceError> {
            self.blocks
                .lock()
                .expect("script lock")
                .pop_front()
                .ok_or_else(|| SourceError::Transport("no scripted block".to_owned()))
        }
    }

    /// Collects published envelopes so tests can assert on order and sequence.
    #[derive(Default)]
    struct CollectSink {
        seen: Mutex<Vec<Envelope>>,
    }

    impl CollectSink {
        fn kinds(&self) -> Vec<&'static str> {
            self.seen
                .lock()
                .expect("sink lock")
                .iter()
                .map(Envelope::kind)
                .collect()
        }

        fn sequences(&self) -> Vec<u64> {
            self.seen
                .lock()
                .expect("sink lock")
                .iter()
                .map(|envelope| envelope.sequence)
                .collect()
        }

        fn reorg_orphans(&self) -> Vec<Vec<B256>> {
            self.seen
                .lock()
                .expect("sink lock")
                .iter()
                .filter_map(|envelope| match &envelope.event {
                    Event::Reorg {
                        orphaned_hashes, ..
                    } => Some(orphaned_hashes.clone()),
                    _ => None,
                })
                .collect()
        }
    }

    impl EventSink for CollectSink {
        async fn publish(&self, envelope: &Envelope) -> anyhow::Result<()> {
            self.seen.lock().expect("sink lock").push(envelope.clone());
            Ok(())
        }
    }

    /// A raw block whose payload is ignored; `FakeSource` scripts the events.
    fn raw_finalized_at(height: u64) -> RawBlock {
        RawBlock {
            raw_block: "{}".to_owned(),
            raw_receipts: "[]".to_owned(),
            finalized: BlockId {
                height,
                hash: hash(0xf0),
            },
        }
    }

    /// Genesis is trivially final, so reporting it publishes no watermark.
    fn raw() -> RawBlock {
        raw_finalized_at(0)
    }

    #[tokio::test]
    async fn publishes_a_linear_chain_with_contiguous_sequences() {
        let source = FakeSource::new(vec![
            vec![block_event(1, hash(1), hash(0)), log_event(1, hash(1), 0)],
            vec![block_event(2, hash(2), hash(1)), log_event(2, hash(2), 0)],
        ]);
        let mut pipeline = Pipeline::new(source, CollectSink::default());

        pipeline
            .process_block(raw())
            .await
            .expect("block 1 publishes");
        pipeline
            .process_block(raw())
            .await
            .expect("block 2 publishes");

        assert_eq!(pipeline.sink.kinds(), ["block", "log", "block", "log"]);
        assert_eq!(pipeline.sink.sequences(), [0, 1, 2, 3]);
        assert_eq!(pipeline.next_sequence(), 4);
    }

    #[tokio::test]
    async fn depth_one_reorg_retracts_and_reuses_the_reclaimed_sequence() {
        let source = FakeSource::new(vec![
            vec![block_event(1, hash(1), hash(0)), log_event(1, hash(1), 0)],
            vec![block_event(2, hash(2), hash(1)), log_event(2, hash(2), 0)],
            // Same height, different hash: block 2 was replaced.
            vec![block_event(2, hash(20), hash(1)), log_event(2, hash(20), 0)],
        ]);
        let mut pipeline = Pipeline::new(source, CollectSink::default());

        for _ in 0..3 {
            pipeline
                .process_block(raw())
                .await
                .expect("block publishes");
        }

        assert_eq!(
            pipeline.sink.kinds(),
            ["block", "log", "block", "log", "reorg", "block", "log"]
        );
        assert_eq!(pipeline.sink.sequences(), [0, 1, 2, 3, 2, 3, 4]);
        assert_eq!(pipeline.sink.reorg_orphans(), vec![vec![hash(2)]]);
    }

    #[tokio::test]
    async fn deep_reorg_retracts_every_block_and_reclaims_their_sequences() {
        let source = FakeSource::new(vec![
            vec![block_event(1, hash(1), hash(0)), log_event(1, hash(1), 0)],
            vec![block_event(2, hash(2), hash(1)), log_event(2, hash(2), 0)],
            vec![block_event(3, hash(3), hash(2)), log_event(3, hash(3), 0)],
            // New chain forks at height 2 and already extends to height 3.
            vec![
                block_event(2, hash(20), hash(1)),
                log_event(2, hash(20), 0),
                log_event(2, hash(20), 1),
            ],
        ]);
        let mut pipeline = Pipeline::new(source, CollectSink::default());

        for _ in 0..4 {
            pipeline
                .process_block(raw())
                .await
                .expect("block publishes");
        }

        // Both old blocks 2 and 3 are retracted, newest first.
        assert_eq!(pipeline.sink.reorg_orphans(), vec![vec![hash(3), hash(2)]]);
        // Block 2's run started at sequence 2, so the reorg and replacement reuse it.
        assert_eq!(pipeline.sink.sequences(), [0, 1, 2, 3, 4, 5, 2, 3, 4, 5]);
        assert_eq!(pipeline.next_sequence(), 6);
    }

    #[tokio::test]
    async fn empty_encode_publishes_nothing_and_holds_the_sequence() {
        let source = FakeSource::new(vec![Vec::new()]);
        let mut pipeline = Pipeline::new(source, CollectSink::default());

        let reorgs = pipeline
            .process_block(raw())
            .await
            .expect("empty block is not an error");

        assert_eq!(reorgs, 0);
        assert!(pipeline.sink.kinds().is_empty());
        assert_eq!(pipeline.next_sequence(), 0);
    }

    #[tokio::test]
    async fn undo_ring_stays_bounded_across_many_blocks() {
        let blocks = (1..=10u8)
            .map(|height| {
                let parent = height - 1;
                vec![
                    block_event(u64::from(height), hash(height), hash(parent)),
                    log_event(u64::from(height), hash(height), 0),
                ]
            })
            .collect();
        let source = FakeSource::new(blocks);
        let mut pipeline = Pipeline::with_undo_depth(source, CollectSink::default(), 2);

        for _ in 0..10 {
            pipeline
                .process_block(raw())
                .await
                .expect("block publishes");
        }

        assert_eq!(pipeline.history.len(), 2);
        assert_eq!(pipeline.next_sequence(), 20);
    }

    #[tokio::test]
    async fn finality_watermark_publishes_once_per_advance_and_trims_the_ring() {
        let blocks = (1..=4u8)
            .map(|height| {
                vec![block_event(
                    u64::from(height),
                    hash(height),
                    hash(height - 1),
                )]
            })
            .collect();
        let mut pipeline = Pipeline::new(FakeSource::new(blocks), CollectSink::default());

        for finalized in [0, 2, 2, 3] {
            pipeline
                .process_block(raw_finalized_at(finalized))
                .await
                .expect("block publishes");
        }

        assert_eq!(
            pipeline.sink.kinds(),
            ["block", "block", "finalized", "block", "block", "finalized"]
        );
        // Blocks below finalized height 3 can never be reorged, so they are dropped.
        let remembered: Vec<u64> = pipeline.history.iter().map(|block| block.height).collect();
        assert_eq!(remembered, [3, 4]);
    }
}
