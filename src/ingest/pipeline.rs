//! The core processing layer, between [`BlockSource`] and [`EnvelopeSink`].
//!
//! Three layers, three jobs:
//!
//! - A **source** knows one chain: how to hear about heads, fetch a block, and turn it
//!   into [`Event`]s. It holds no state about what was published.
//! - A **sink** knows one destination, with no notion of ordering. It buffers until
//!   [`flush`](EnvelopeSink::flush), which the pipeline calls once per block.
//! - The **pipeline** is the only stateful part: it drives the source, folds its events
//!   into one ordered stream, and hands each envelope to the sink.
//!
//! Its state is a machine: `Mode` names what it is doing, and the linkage rules in
//! `Pipeline::process_block` decide the transition. A head linking to the published tip
//! advances the chain; one that does not is a fork, retracted to the fork point in the
//! same call and published as an [`Event::Reorg`]. A height gap and a fork deeper than
//! the undo ring both fail loudly rather than publishing across a hole. The *first* head
//! of a fresh pipeline is adopted as the start of the stream — wherever the chain happens
//! to be — so a mid-chain start indexes forward from there instead of refusing.
//!
//! Building the stream means three things: a per-chain monotonic sequence number on
//! every envelope, parent-hash linkage so a reorg surfaces as an [`Event::Reorg`] instead
//! of silently wrong data, and an [`Event::Finalized`] watermark whenever finality
//! advances.
//!
//! Sequences are per event, not per block, so reclaiming them after a reorg means
//! remembering where each block's run began — what the bounded undo ring stores.
//!
//! # Coverage
//!
//! It reorgs against blocks published in this process. Backfill-to-live handoff,
//! checkpoint resume, and rebuilding the ring from durable history are not built.

use std::collections::VecDeque;

use alloy_primitives::B256;
use anyhow::bail;
use futures_util::StreamExt as _;
use tracing::{error, info, warn};

use crate::sink::EnvelopeSink;
use crate::wire::envelope::{Envelope, Event, Finalized, Reorg};

use crate::ingest::source::{BlockId, BlockSource, FetchedBlock};

/// The most published blocks the undo ring remembers by default, and so the deepest
/// reorg it can retract.
///
/// Blocks at or below the finalized height are dropped first, since no reorg reaches
/// them, so on Ethereum the ring rarely holds more than ~64. The cap bites on chains
/// whose finality lags far behind the tip, such as L2s waiting on L1, where it is the
/// hard ceiling on retraction depth.
pub(crate) const DEFAULT_UNDO_DEPTH: usize = 128;

/// What the pipeline is doing when it is driven forward.
///
/// The pipeline's state is `(sequence, history, finalized)`; this names the mode it is
/// in, so the driver's next step is explicit rather than implied by a stack of
/// conditionals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    /// Subscribed to live heads and publishing each one as it arrives.
    Following,
    /// A head arrived that does not link to the published tip, so the stream cannot
    /// continue without filling the range first.
    ///
    /// Entered when a head would leave a gap. Backfill is not built, so this mode is
    /// terminal in practice: a gap fails rather than being published as a silent hole.
    /// The first head of a fresh pipeline is *not* a gap — there is nothing before it to
    /// be contiguous with — so it does not enter this mode; see
    /// [`Pipeline::process_block`].
    Backfilling,
}

/// One block's slice of the published stream.
#[derive(Debug)]
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

impl<S: BlockSource, K: EnvelopeSink> Pipeline<S, K> {
    /// Builds a pipeline with `DEFAULT_UNDO_DEPTH` blocks of history.
    #[must_use]
    pub fn new(source: S, sink: K) -> Self {
        Self::with_undo_depth(source, sink, DEFAULT_UNDO_DEPTH)
    }

    /// Builds a pipeline that retracts up to `undo_depth` published blocks.
    ///
    /// The ring holds at most `undo_depth` blocks, so a fork at its oldest retracts
    /// exactly that many; a deeper fork is refused rather than partially retracted. A
    /// depth of zero disables retraction — linkage is still checked, but no sequences
    /// can be reclaimed.
    #[must_use]
    fn with_undo_depth(source: S, sink: K, undo_depth: usize) -> Self {
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

    /// Follows the live chain tip until the head subscription ends.
    ///
    /// Starts at whatever head the source reports next; it does not backfill.
    ///
    /// # Errors
    ///
    /// Returns an error when the subscription, a block fetch, or a publish fails, and
    /// when the subscription closes — a live indexer should never stop.
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
    /// Returns the number of [`Event::Reorg`] envelopes published, `0` or `1`.
    ///
    /// # Errors
    ///
    /// Returns an error when the sink cannot deliver an event — fatal, since skipping
    /// one leaves a sequence gap — and when linkage cannot be resolved locally: a height
    /// gap after the stream has started, or a fork deeper than the undo ring. Those are
    /// coverage breaks, not reorgs, and cannot be published honestly from here. A *first*
    /// head above genesis is not one of them: it is where the stream starts.
    pub(crate) async fn process_block(&mut self, block: FetchedBlock) -> anyhow::Result<u64> {
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
                         backfill catches this up, but backfill-to-live handoff is not built"
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
            // A fresh pipeline's first head is wherever the chain is now, not genesis:
            // there is no backfill, so the stream simply begins at this height. That is
            // a legitimate place to start — the operator gets live data and not the
            // history before it — so it is adopted rather than refused. Parent linkage
            // is still enforced from here on, so the first head is the base of the
            // stream and every block after it links back to it.
            warn!(
                chain = %self.source.chain(),
                height,
                "starting mid-chain: no history before this block is indexed"
            );
        }

        let first_sequence = self.sequence;
        for event in events {
            let envelope = Envelope::new(self.source.chain().clone(), self.sequence, event);
            self.sink.publish(envelope).await?;
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

    /// Publishes a watermark if `finalized` is newer than the current one, and drops
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
        self.sink.publish(marker).await?;
        self.sequence += 1;
        Ok(())
    }

    /// Retracts the chain to `fork` (the first orphaned height) and emits the reorg marker.
    ///
    /// `fork` is the new branch's first block: the first published block that did not
    /// build on `actual_parent`. The marker names that height, so a consumer retracts
    /// every hash at or above it and re-requests from there.
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
        self.sink.publish(reorg).await?;
        self.sequence += 1;
        Ok(())
    }

    /// The first height at or above which blocks are orphaned by a fork at `parent`.
    ///
    /// `parent` is the new head's parent, so the fork point is the block one above the
    /// ring entry whose hash is `parent`. Returns `None` when the fork predates the ring.
    fn find_fork(&self, parent: B256) -> Option<u64> {
        self.history
            .iter()
            .rev()
            .find(|block| block.hash == parent)
            .map(|block| block.height + 1)
    }

    /// Drops remembered blocks at or above `height`.
    ///
    /// Returns the orphaned block hashes, newest first, and the sequence number a reorg
    /// resumes from.
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
        // is a reorg this ring cannot retract; swappable for an error once the ring
        // is rebuilt from durable history.
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
    use crate::wire::envelope::{Block, ChainId, Envelope, Event, Log};

    use crate::sink::EnvelopeSink;

    use crate::ingest::source::{BlockId, BlockSource, FetchedBlock, HeadStream, SourceError};

    fn hash(byte: u8) -> B256 {
        B256::from([byte; 32])
    }

    // Test fixtures use `..Default::default()` so a new field on a dataset does
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

    impl EnvelopeSink for CollectSink {
        async fn publish(&mut self, envelope: Envelope) -> anyhow::Result<()> {
            self.seen.push(envelope);
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
        assert_eq!(pipeline.sequence, 4);
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
        assert_eq!(pipeline.sequence, 6);
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
        assert_eq!(pipeline.sequence, 0);
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
        assert_eq!(pipeline.sequence, 20);
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
        assert_eq!(pipeline.sequence, 4);
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
        assert_eq!(pipeline.mode, Mode::Backfilling);
    }

    /// A fresh pipeline's first head is wherever the chain is, not genesis: there is no
    /// backfill, so the stream starts there. Missing the history before it is the
    /// operator's trade, not an error, so the block is published and the stream follows.
    #[tokio::test]
    async fn a_first_head_above_genesis_starts_the_stream() {
        let mut pipeline = pipeline(128);
        process_all(
            &mut pipeline,
            vec![
                vec![block_event(5, hash(5), hash(4))],
                vec![block_event(6, hash(6), hash(5))],
            ],
        )
        .await;

        assert_eq!(pipeline.sink.kinds(), ["block", "block"]);
        assert_eq!(pipeline.mode, Mode::Following);
        // No earlier height was invented to stand in as the stream's base.
        assert_eq!(pipeline.history.front().map(|block| block.height), Some(5));
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

    /// The live loop: each head the subscription reports is fetched and published in
    /// turn, and the subscription ending is a failure rather than a clean stop — a live
    /// indexer that silently stops is the worst outcome there is.
    #[tokio::test]
    async fn run_drives_each_head_through_fetch_and_publish() {
        /// Emits two heads, then ends; serves each head's block on demand.
        struct HeadingSource {
            chain: ChainId,
        }

        impl BlockSource for HeadingSource {
            fn chain(&self) -> &ChainId {
                &self.chain
            }

            async fn subscribe_heads(&self) -> Result<HeadStream, SourceError> {
                let heads = vec![
                    Ok(BlockId {
                        height: 1,
                        hash: hash(1),
                    }),
                    Ok(BlockId {
                        height: 2,
                        hash: hash(2),
                    }),
                ];
                Ok(Box::pin(stream::iter(heads)))
            }

            async fn fetch_block(&self, height: u64) -> Result<FetchedBlock, SourceError> {
                let (block_hash, parent) = if height == 1 {
                    (hash(1), hash(0))
                } else {
                    (hash(2), hash(1))
                };
                Ok(fetched(vec![block_event(height, block_hash, parent)]))
            }
        }

        let source = HeadingSource {
            chain: ChainId::new("ethereum"),
        };
        let mut pipeline = Pipeline::new(source, CollectSink::default());

        let error = pipeline
            .run()
            .await
            .expect_err("a closed subscription must not be a clean stop");

        // Both heads were followed in order before the subscription ended.
        assert_eq!(pipeline.sink.kinds(), ["block", "block"]);
        assert_eq!(pipeline.sink.sequences(), [0, 1]);
        assert_eq!(pipeline.sequence, 2);
        // The error names the last height it saw, so a stopped live run is diagnosable.
        assert!(error.to_string().contains('2'), "{error}");
    }
}
