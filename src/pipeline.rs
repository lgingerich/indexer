//! The core processing layer, between [`BlockSource`] and [`EventSink`].
//!
//! The three layers split responsibilities like this:
//!
//! - A **source** knows one chain: how to hear about new heads, fetch a block, and
//!   turn it into [`Event`]s. It holds no state about what was published.
//! - A **sink** knows one destination: how to deliver an [`Envelope`] to a given
//!   chain's stream. It knows nothing about ordering, and it buffers until
//!   [`flush`](EventSink::flush), which the pipeline calls once per block.
//! - The **pipeline** is the only stateful part. It drives the source, turns its
//!   events into a single ordered stream, and hands each envelope to the sink.
//!
//! Its state is a machine: [`Mode`] names what it is doing, and the linkage rules
//! in [`Pipeline::process_block`] decide the transition. A head that links to the
//! published tip advances the chain; one that does not is a fork, which retracts to
//! the fork point in the same call and is published as an [`Event::Reorg`]. A head
//! that would leave a height gap, a first head above genesis, and a fork deeper
//! than the undo ring all fail loudly rather than publishing across a hole.
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

use crate::envelope::{Envelope, Event, Finalized, Reorg};
use crate::sink::EventSink;
use crate::source::{BlockId, BlockSource, FetchedBlock};

/// The most published blocks the undo ring remembers by default, and therefore the
/// deepest reorg it can retract.
///
/// Blocks at or below the finalized height are dropped first, since no reorg can
/// reach them, so on Ethereum the ring rarely holds more than about 64. This cap
/// matters on chains whose finality lags far behind the tip, such as L2s waiting
/// on L1, where it is the ceiling on how deep a reorg can be retracted.
pub const DEFAULT_UNDO_DEPTH: usize = 128;

/// What the pipeline is doing when it is driven forward.
///
/// The pipeline is a state machine whose state is `(sequence, history, finalized)`;
/// this names the mode that state is in, so the driver's next step is explicit
/// rather than implied by a stack of conditionals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Subscribed to live heads and publishing each one as it arrives.
    Following,
    /// Filling a contiguous height range up to the live tip before trusting heads.
    ///
    /// Not entered yet: backfill-to-live handoff is not built, so a fresh pipeline
    /// starts mid-chain at whatever head arrives next. It also cannot be skipped
    /// out of — a head that would leave a gap is an error, not a silent hole.
    Backfilling,
}

/// One block's slice of the published stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PublishedBlock {
    height: u64,
    hash: B256,
    /// Sequence of the block's first published event; where a rewind resumes.
    first_sequence: u64,
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
    mode: Mode,
}

impl<S: BlockSource, K: EventSink> Pipeline<S, K> {
    /// Builds a pipeline with [`DEFAULT_UNDO_DEPTH`] of history.
    #[must_use]
    pub fn new(source: S, sink: K) -> Self {
        Self::with_undo_depth(source, sink, DEFAULT_UNDO_DEPTH)
    }

    /// Builds a pipeline that retracts up to `undo_depth` published blocks.
    ///
    /// The undo ring holds at most `undo_depth` blocks, so a fork at the ring's
    /// oldest remembered block retracts exactly `undo_depth` of them. A deeper fork
    /// is refused rather than partially retracted.
    ///
    /// A depth of zero disables reorg retraction: linkage is still checked, but no
    /// sequence numbers can be reclaimed.
    #[must_use]
    pub fn with_undo_depth(source: S, sink: K, undo_depth: usize) -> Self {
        Self {
            source,
            sink,
            history: VecDeque::with_capacity(undo_depth.min(256)),
            undo_depth,
            sequence: 0,
            finalized_height: 0,
            mode: Mode::Following,
        }
    }

    /// The sequence number that the next published envelope will carry.
    #[must_use]
    pub const fn next_sequence(&self) -> u64 {
        self.sequence
    }

    /// The mode the pipeline is currently in.
    #[must_use]
    pub const fn mode(&self) -> Mode {
        self.mode
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
            let block = self.source.fetch_block(head.height).await?;
            self.process_block(block).await?;
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
    /// Returns an error when the sink cannot deliver an event — that is fatal,
    /// since skipping an event would leave a sequence gap — and when linkage
    /// cannot be resolved locally: a height gap, a first head above genesis with no
    /// remembered history, or a fork deeper than the undo ring. Those are coverage
    /// breaks rather than reorgs, and neither can be published honestly from here.
    pub async fn process_block(&mut self, block: FetchedBlock) -> anyhow::Result<u64> {
        let FetchedBlock { events, finalized } = block;
        let Some((first, _)) = events.split_first() else {
            return Ok(0);
        };

        let Event::Block(block) = first else {
            error!(
                chain = %self.source.chain(),
                first_kind = first.kind(),
                "fetched block must lead with a block marker; skipping block"
            );
            return Ok(0);
        };
        let (height, hash, parent_hash) = (block.number, block.hash, block.parent_hash);
        let mut reorgs = 0;

        if let Some(previous) = self.history.back() {
            let latest = previous.height;
            if height == latest && hash == previous.hash {
                // The tip was already published and re-announced. Not a fork.
                return Ok(0);
            }
            if parent_hash == previous.hash {
                // Links to the published tip. With no gap this is the next height;
                // a gap means a head was missed, which is a coverage hole rather
                // than a fork, and publishing across it would leave a silent hole.
                if height != latest + 1 {
                    self.mode = Mode::Backfilling;
                    bail!(
                        "height gap: published tip {latest} -> head {height}; \
                         backfill catches this up, but backfill-to-live handoff is not built yet"
                    );
                }
            } else if let Some(fork) = self.find_fork(parent_hash) {
                // A genuine fork: retract to the fork point, not to the head.
                self.publish_reorg(fork, hash, parent_hash).await?;
                reorgs = 1;
            } else {
                return Err(anyhow::anyhow!(
                    "no fork point for head {height} (parent {parent_hash}); the reorg is \
                     deeper than the {} remembered blocks",
                    self.undo_depth
                ));
            }
        } else if height > 1 {
            // A first block above genesis with nothing remembered means history was
            // skipped; publishing it would present a chain with no prior block.
            self.mode = Mode::Backfilling;
            bail!(
                "first head is {height}, above genesis, with no remembered history; \
                 backfill-to-live handoff is not built yet"
            );
        }

        let first_sequence = self.sequence;
        for event in events {
            let envelope = Envelope::new(self.source.chain().clone(), self.sequence, event);
            self.sink.publish(&envelope).await?;
            self.sequence += 1;
        }

        self.history.push_back(PublishedBlock {
            height,
            hash,
            first_sequence,
        });
        self.trim_unless_recent(height);
        self.advance_finality(finalized).await?;
        // One flush per block is the batch boundary: every event published above —
        // a reorg marker, the block's events, and a finality watermark — becomes
        // durable together, so a buffering sink opens its engine once per block
        // instead of once per event.
        self.sink.flush().await?;
        self.mode = Mode::Following;

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
            Event::Finalized(Finalized {
                height: finalized.height,
                hash: finalized.hash,
            }),
        );
        self.sink.publish(&marker).await?;
        self.sequence += 1;
        Ok(())
    }

    /// Retracts the chain to `fork` (the first orphaned height) and emits the reorg marker.
    ///
    /// The fork is the new branch's first block: the first published block that did
    /// not build on `actual_parent`. The marker names that height, so a consumer can
    /// retract every hash at or above it and re-request from there.
    async fn publish_reorg(
        &mut self,
        fork: u64,
        new_head_hash: B256,
        actual_parent: B256,
    ) -> anyhow::Result<()> {
        let expected_parent = self.history.back().map(|block| block.hash);
        let (orphaned_hashes, reclaimed_from) = self.rewind_to(fork);
        // `find_fork` locates `fork` inside the ring, so the rewind must orphan
        // something. Checked on the release path, not with `debug_assert`, because
        // an empty marker here would tell a consumer to retract nothing while the
        // chain has already forked.
        anyhow::ensure!(
            !orphaned_hashes.is_empty(),
            "a reorg must orphan at least the block at {fork}"
        );
        self.sequence = reclaimed_from.unwrap_or(self.sequence);
        warn!(
            chain = %self.source.chain(),
            expected = ?expected_parent,
            %actual_parent,
            fork,
            orphaned = orphaned_hashes.len(),
            sequence = self.sequence,
            "parent hash mismatch; retracted to the fork point and publishing reorg"
        );

        let reorg = Envelope::new(
            self.source.chain().clone(),
            self.sequence,
            Event::Reorg(Reorg {
                height: fork,
                new_head_hash,
                orphaned_hashes,
            }),
        );
        self.sink.publish(&reorg).await?;
        self.sequence += 1;
        Ok(())
    }

    /// The first height at or above which blocks are orphaned by a fork at `parent`.
    ///
    /// `parent` is the new head's parent; the fork point is the first block that did
    /// not build on it, which is the block one above the ring entry whose hash is
    /// `parent`. Returns `None` when the fork is older than the ring.
    fn find_fork(&self, parent: B256) -> Option<u64> {
        self.history
            .iter()
            .rev()
            .find(|block| block.hash == parent)
            .map(|block| block.height + 1)
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
    use alloy_primitives::{B256, TxHash};
    use futures_util::stream;

    use super::{Mode, Pipeline};
    use crate::envelope::{Block, ChainId, Envelope, Event, Log};
    use crate::sink::EventSink;
    use crate::source::{BlockId, BlockSource, FetchedBlock, HeadStream, SourceError};

    fn hash(byte: u8) -> B256 {
        B256::from([byte; 32])
    }

    // Test fixtures use `..Default::default()` so adding a field to a dataset does
    // not churn every test that only cares about identity.
    fn block_event(number: u64, hash: B256, parent_hash: B256) -> Event {
        Event::Block(Box::new(Block {
            number,
            hash,
            parent_hash,
            timestamp: 1_700_000_000 + number,
            ..Block::default()
        }))
    }

    fn log_event(number: u64, block_hash: B256, log_index: u64) -> Event {
        Event::Log(Box::new(Log {
            log_index,
            block_hash,
            block_number: number,
            transaction_hash: TxHash::from([0x01; 32]),
            ..Log::default()
        }))
    }

    /// Tests hand blocks straight to the pipeline, so the source only names a chain.
    struct FakeSource {
        chain: ChainId,
    }

    impl BlockSource for FakeSource {
        fn chain(&self) -> &ChainId {
            &self.chain
        }

        async fn subscribe_heads(&self) -> Result<HeadStream, SourceError> {
            Ok(Box::pin(stream::empty()))
        }

        async fn fetch_block(&self, _height: u64) -> Result<FetchedBlock, SourceError> {
            Err(SourceError::Transport(
                "tests pass blocks directly".to_owned(),
            ))
        }
    }

    /// Collects published envelopes so tests can assert on order and sequence.
    ///
    /// A plain `Vec` and not a mutex: the trait drives the sink through `&mut self`,
    /// so the sink owns its own batch state.
    #[derive(Default)]
    struct CollectSink {
        seen: Vec<Envelope>,
    }

    impl CollectSink {
        fn kinds(&self) -> Vec<&'static str> {
            self.seen.iter().map(Envelope::kind).collect()
        }

        fn sequences(&self) -> Vec<u64> {
            self.seen.iter().map(|envelope| envelope.sequence).collect()
        }

        fn reorg_orphans(&self) -> Vec<Vec<B256>> {
            self.seen
                .iter()
                .filter_map(|envelope| match &envelope.event {
                    Event::Reorg(reorg) => Some(reorg.orphaned_hashes.clone()),
                    _ => None,
                })
                .collect()
        }

        fn reorg_heights(&self) -> Vec<u64> {
            self.seen
                .iter()
                .filter_map(|envelope| match &envelope.event {
                    Event::Reorg(reorg) => Some(reorg.height),
                    _ => None,
                })
                .collect()
        }
    }

    impl EventSink for CollectSink {
        async fn publish(&mut self, envelope: &Envelope) -> anyhow::Result<()> {
            self.seen.push(envelope.clone());
            Ok(())
        }
    }

    fn pipeline(undo_depth: usize) -> Pipeline<FakeSource, CollectSink> {
        let source = FakeSource {
            chain: ChainId::new("ethereum"),
        };
        Pipeline::with_undo_depth(source, CollectSink::default(), undo_depth)
    }

    fn fetched_at(events: Vec<Event>, finalized_height: u64) -> FetchedBlock {
        FetchedBlock {
            events,
            finalized: BlockId {
                height: finalized_height,
                hash: hash(0xf0),
            },
        }
    }

    /// Genesis is trivially final, so reporting it publishes no watermark.
    fn fetched(events: Vec<Event>) -> FetchedBlock {
        fetched_at(events, 0)
    }

    async fn process_all(
        pipeline: &mut Pipeline<FakeSource, CollectSink>,
        blocks: Vec<Vec<Event>>,
    ) {
        for events in blocks {
            pipeline
                .process_block(fetched(events))
                .await
                .expect("block publishes");
        }
    }

    #[tokio::test]
    async fn publishes_a_linear_chain_with_contiguous_sequences() {
        let mut pipeline = pipeline(128);
        process_all(
            &mut pipeline,
            vec![
                vec![block_event(1, hash(1), hash(0)), log_event(1, hash(1), 0)],
                vec![block_event(2, hash(2), hash(1)), log_event(2, hash(2), 0)],
            ],
        )
        .await;

        assert_eq!(pipeline.sink.kinds(), ["block", "log", "block", "log"]);
        assert_eq!(pipeline.sink.sequences(), [0, 1, 2, 3]);
        assert_eq!(pipeline.next_sequence(), 4);
    }

    #[tokio::test]
    async fn depth_one_reorg_retracts_and_reuses_the_reclaimed_sequence() {
        let mut pipeline = pipeline(128);
        process_all(
            &mut pipeline,
            vec![
                vec![block_event(1, hash(1), hash(0)), log_event(1, hash(1), 0)],
                vec![block_event(2, hash(2), hash(1)), log_event(2, hash(2), 0)],
                // Same height, different hash: block 2 was replaced.
                vec![block_event(2, hash(20), hash(1)), log_event(2, hash(20), 0)],
            ],
        )
        .await;

        assert_eq!(
            pipeline.sink.kinds(),
            ["block", "log", "block", "log", "reorg", "block", "log"]
        );
        assert_eq!(pipeline.sink.sequences(), [0, 1, 2, 3, 2, 3, 4]);
        assert_eq!(pipeline.sink.reorg_orphans(), vec![vec![hash(2)]]);
    }

    #[tokio::test]
    async fn deep_reorg_retracts_every_block_and_reclaims_their_sequences() {
        let mut pipeline = pipeline(128);
        process_all(
            &mut pipeline,
            vec![
                vec![block_event(1, hash(1), hash(0)), log_event(1, hash(1), 0)],
                vec![block_event(2, hash(2), hash(1)), log_event(2, hash(2), 0)],
                vec![block_event(3, hash(3), hash(2)), log_event(3, hash(3), 0)],
                // New chain forks at height 2.
                vec![
                    block_event(2, hash(20), hash(1)),
                    log_event(2, hash(20), 0),
                    log_event(2, hash(20), 1),
                ],
            ],
        )
        .await;

        // Both old blocks 2 and 3 are retracted, newest first.
        assert_eq!(pipeline.sink.reorg_orphans(), vec![vec![hash(3), hash(2)]]);
        // Block 2's run started at sequence 2, so the reorg and replacement reuse it.
        assert_eq!(pipeline.sink.sequences(), [0, 1, 2, 3, 4, 5, 2, 3, 4, 5]);
        assert_eq!(pipeline.next_sequence(), 6);
    }

    #[tokio::test]
    async fn empty_block_publishes_nothing_and_holds_the_sequence() {
        let mut pipeline = pipeline(128);
        let reorgs = pipeline
            .process_block(fetched(Vec::new()))
            .await
            .expect("empty block is not an error");

        assert_eq!(reorgs, 0);
        assert!(pipeline.sink.kinds().is_empty());
        assert_eq!(pipeline.next_sequence(), 0);
    }

    #[tokio::test]
    async fn undo_ring_stays_bounded_across_many_blocks() {
        let mut pipeline = pipeline(2);
        let blocks = (1..=10u8)
            .map(|height| {
                vec![
                    block_event(u64::from(height), hash(height), hash(height - 1)),
                    log_event(u64::from(height), hash(height), 0),
                ]
            })
            .collect();
        process_all(&mut pipeline, blocks).await;

        assert_eq!(pipeline.history.len(), 2);
        assert_eq!(pipeline.next_sequence(), 20);
    }

    #[tokio::test]
    async fn a_reannounced_tip_is_not_a_reorg() {
        let mut pipeline = pipeline(128);
        process_all(
            &mut pipeline,
            vec![
                vec![block_event(1, hash(1), hash(0)), log_event(1, hash(1), 0)],
                vec![block_event(2, hash(2), hash(1)), log_event(2, hash(2), 0)],
                // The node resends the current head: same height, same hash.
                vec![block_event(2, hash(2), hash(1)), log_event(2, hash(2), 0)],
            ],
        )
        .await;

        assert_eq!(pipeline.sink.kinds(), ["block", "log", "block", "log"]);
        assert_eq!(pipeline.next_sequence(), 4);
        assert!(pipeline.sink.reorg_orphans().is_empty());
    }

    #[tokio::test]
    async fn a_height_gap_is_a_coverage_error_not_a_silent_hole() {
        let mut pipeline = pipeline(128);
        process_all(&mut pipeline, vec![vec![block_event(1, hash(1), hash(0))]]).await;

        // Head 3 chains from the published block 1, so it links to the tip but skips
        // height 2.
        let error = pipeline
            .process_block(fetched(vec![block_event(3, hash(3), hash(1))]))
            .await
            .expect_err("a gap must fail");
        assert!(error.to_string().contains("height gap"), "{error}");
        assert_eq!(pipeline.mode(), Mode::Backfilling);
    }

    #[tokio::test]
    async fn a_first_head_above_genesis_is_a_coverage_error() {
        let mut pipeline = pipeline(128);
        let error = pipeline
            .process_block(fetched(vec![block_event(5, hash(5), hash(4))]))
            .await
            .expect_err("a fresh pipeline cannot start mid-chain");
        assert!(error.to_string().contains("first head"), "{error}");
        assert_eq!(pipeline.mode(), Mode::Backfilling);
    }

    #[tokio::test]
    async fn a_reorg_deeper_than_the_ring_is_an_error_not_an_empty_marker() {
        // The ring holds only the last two blocks, so a fork below them cannot be
        // located and retracted.
        let mut pipeline = pipeline(2);
        process_all(
            &mut pipeline,
            vec![
                vec![block_event(1, hash(1), hash(0))],
                vec![block_event(2, hash(2), hash(1))],
                vec![block_event(3, hash(3), hash(2))],
            ],
        )
        .await;

        // A new head building on block 1 forks below the remembered range.
        let error = pipeline
            .process_block(fetched(vec![block_event(2, hash(20), hash(1))]))
            .await
            .expect_err("a fork older than the ring must fail");
        assert!(error.to_string().contains("no fork point"), "{error}");
        assert!(pipeline.sink.reorg_orphans().is_empty());
    }

    #[tokio::test]
    async fn a_reorg_names_the_fork_point_as_its_height() {
        let mut pipeline = pipeline(128);
        process_all(
            &mut pipeline,
            vec![
                vec![block_event(1, hash(1), hash(0))],
                vec![block_event(2, hash(2), hash(1)), log_event(2, hash(2), 0)],
                vec![block_event(3, hash(3), hash(2)), log_event(3, hash(3), 0)],
                // A new branch forked at height 2; its head is the first new block.
                vec![block_event(2, hash(20), hash(1)), log_event(2, hash(20), 0)],
            ],
        )
        .await;

        assert_eq!(pipeline.sink.reorg_orphans(), vec![vec![hash(3), hash(2)]]);
        assert_eq!(pipeline.sink.reorg_heights(), [2]);
    }

    #[tokio::test]
    async fn finality_watermark_publishes_once_per_advance_and_trims_the_ring() {
        let mut pipeline = pipeline(128);
        for (height, finalized) in (1..=4u8).zip([0, 2, 2, 3]) {
            let events = vec![block_event(
                u64::from(height),
                hash(height),
                hash(height - 1),
            )];
            pipeline
                .process_block(fetched_at(events, finalized))
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
