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
//! Its state is a machine, and the linkage rules in `Pipeline::process_block` drive it.
//! A head linking to the published tip advances the chain; one that does not is a fork,
//! retracted to the fork point in the same call and published as an [`Event::Reorg`]. A
//! head already in the stream is a re-announcement rather than a fork, and is ignored. A
//! height gap and a fork deeper than the undo ring both fail loudly rather than
//! publishing across a hole. The *first* head of a fresh pipeline is adopted as the start
//! of the stream — wherever the chain happens to be — so a mid-chain start indexes
//! forward from there instead of refusing.
//!
//! Building the stream means two things: parent-hash linkage, so a reorg surfaces as an
//! [`Event::Reorg`] instead of silently wrong data, and an [`Event::Finalized`]
//! watermark whenever finality advances.
//!
//! There is no sequence number on the envelope. A record's position is the dataset's
//! own — `number` for a block, `transaction_index` for a transaction or receipt, then
//! `log_index` for a log — and those tuples are stable across a reorg, where a global
//! counter would have to rewind and would then name two different rows the same thing.
//!
//! # Coverage
//!
//! It reorgs against blocks published in this process. Backfill-to-live handoff,
//! checkpoint resume, and rebuilding the ring from durable history are not built.

use std::collections::VecDeque;

use alloy_primitives::B256;
use futures_util::StreamExt as _;
use thiserror::Error;
use tracing::{debug, error, info, warn};

use crate::sink::{EnvelopeSink, SinkError};
use crate::wire::envelope::{Envelope, Event, Finalized, Reorg};

use crate::ingest::source::{BlockId, BlockSource, FetchedBlock, SourceError};

/// Why the pipeline could not keep publishing.
///
/// Three of these are coverage breaks and the rest are the layers below failing. They are
/// separate variants rather than one message because a caller responds differently: a
/// sink failure is fatal to this run, while a [`PipelineError::HeightGap`] or
/// [`PipelineError::ForkTooDeep`] says the *chain data* skipped — a reindex, not a retry.
///
/// The linkage variants carry both heights so a caller can log the hole without parsing
/// prose, which is the only thing a string error is good for and the one thing a typed
/// one should not make it do.
#[derive(Debug, Error)]
pub enum PipelineError {
    /// The head subscription ended. A live indexer should never stop quietly.
    #[error("head subscription closed after height {last_height}")]
    SubscriptionClosed {
        /// The last height the subscription reported, or 0 if it reported none.
        last_height: u64,
    },
    /// A head left a gap on the canonical branch: the tip is `tip` and the head is
    /// `head`, so heights between them never arrived.
    ///
    /// Publishing across it would leave a silent hole, so it is refused instead.
    #[error("height gap: published tip {tip} -> head {head}")]
    HeightGap {
        /// The published tip the head was measured against.
        tip: u64,
        /// The height of the head that skipped heights.
        head: u64,
    },
    /// A head left a gap on a *forked* branch: the fork point is `fork` and the head is
    /// `head`, so its own branch skipped heights.
    ///
    /// The same hole as [`Self::HeightGap`], reached through the door the canonical
    /// check does not cover.
    #[error("height gap on the forked branch: fork point {fork} -> head {head}")]
    ForkedHeightGap {
        /// The fork point the new branch diverged at.
        fork: u64,
        /// The height of the head that skipped heights.
        head: u64,
    },
    /// A fork reaches below the undo ring, so no marker can honestly say what was
    /// orphaned.
    ///
    /// `undo_depth` is the ring's size, so a caller can see how far past it the fork
    /// reached rather than only that it did.
    #[error("no fork point for head {height} (parent {parent}), deeper than {undo_depth}")]
    ForkTooDeep {
        /// The height of the head that could not be linked.
        height: u64,
        /// Its parent hash, which the ring no longer holds.
        parent: B256,
        /// How many blocks the ring remembered.
        undo_depth: usize,
    },
    /// A reorg retracted nothing, which would publish a marker telling a consumer to
    /// retract no blocks while the chain has already forked.
    ///
    /// A bug rather than a chain condition: `find_fork` located the fork point inside
    /// the ring, so the rewind must orphan at least the block there.
    #[error("a reorg must orphan at least the block at {fork}")]
    EmptyReorg {
        /// The fork point the rewind should have orphaned from.
        fork: u64,
    },
    /// The source could not be read, or its heads could not be fetched.
    #[error(transparent)]
    Source(#[from] SourceError),
    /// The sink could not accept, render, or deliver an envelope.
    #[error(transparent)]
    Sink(#[from] SinkError),
}

/// The most published blocks the undo ring remembers by default, and so the deepest
/// reorg it can retract.
///
/// Blocks at or below the finalized height are dropped first, since no reorg reaches
/// them, so on Ethereum the ring rarely holds more than ~64. The cap bites on chains
/// whose finality lags far behind the tip, such as L2s waiting on L1, where it is the
/// hard ceiling on retraction depth.
pub(crate) const DEFAULT_UNDO_DEPTH: usize = 128;

/// One block the pipeline still remembers, and so can still retract.
#[derive(Debug)]
struct PublishedBlock {
    height: u64,
    hash: B256,
}

/// Drives one source into one sink as an ordered, reorg-aware stream.
#[derive(Debug)]
pub struct Pipeline<S, K> {
    source: S,
    sink: K,
    history: VecDeque<PublishedBlock>,
    undo_depth: usize,
    finalized_height: u64,
}

impl<S: BlockSource, K: EnvelopeSink> Pipeline<S, K> {
    /// Builds a pipeline that retracts up to [`DEFAULT_UNDO_DEPTH`] published blocks.
    ///
    /// The ring holds at most that many blocks, so a fork at its oldest retracts
    /// exactly that many; a deeper fork is refused rather than partially retracted.
    #[must_use]
    pub fn new(source: S, sink: K) -> Self {
        Self {
            source,
            sink,
            history: VecDeque::with_capacity(DEFAULT_UNDO_DEPTH),
            undo_depth: DEFAULT_UNDO_DEPTH,
            finalized_height: 0,
        }
    }

    /// Follows the live chain tip until the head subscription ends.
    ///
    /// Starts at whatever head the source reports next; it does not backfill.
    ///
    /// # Errors
    ///
    /// Returns an error when the subscription, a block fetch, or a publish fails, and
    /// [`PipelineError::SubscriptionClosed`] when the subscription ends — a live indexer
    /// should never stop.
    pub async fn run(&mut self) -> Result<(), PipelineError> {
        let mut heads = self.source.subscribe_heads().await?;
        info!(chain = %self.source.chain(), "subscribed to heads");

        let mut last_height = 0;
        while let Some(head) = heads.next().await {
            let head = head?;
            last_height = head.height;
            let block = self.source.fetch_block(head.height).await?;
            self.process_block(block).await?;
        }
        Err(PipelineError::SubscriptionClosed { last_height })
    }

    /// Publishes one block's events, emitting a reorg first if linkage broke and a
    /// finality watermark after if finality advanced.
    ///
    /// # Errors
    ///
    /// Returns an error when the sink cannot deliver an event — fatal, since skipping
    /// one leaves a sequence gap — and when linkage cannot be resolved locally: a height
    /// gap after the stream has started, on either the canonical or the forked branch, or
    /// a fork deeper than the undo ring. Those are coverage breaks, not reorgs, and
    /// cannot be published honestly from here. A *first* head above genesis is not one of
    /// them: it is where the stream starts.
    pub(crate) async fn process_block(&mut self, block: FetchedBlock) -> Result<(), PipelineError> {
        let FetchedBlock { events, finalized } = block;
        let Some((first, _)) = events.split_first() else {
            return Ok(());
        };

        let Event::Block(block) = first else {
            error!(
                chain = %self.source.chain(),
                first_kind = first.kind(),
                "fetched block must lead with a block marker; skipping block"
            );
            return Ok(());
        };
        let (height, hash, parent_hash) = (block.number, block.hash, block.parent_hash);

        if let Some(previous) = self.history.back() {
            let latest = previous.height;

            // A head already in the stream is a re-announcement, not a fork. A node
            // resends a head after a slow fetch, on reconnect, or from a lagging
            // replica, and such a head's parent is the canonical parent — so linkage
            // alone cannot tell the two apart. The block's own identity can: a
            // re-announcement carries a height and hash already published, and
            // treating it as a fork would retract canonical blocks. Only the tip used
            // to be special-cased, so a head for an *older* height fell through to
            // `find_fork` and destroyed every block above it.
            if self.is_published(height, hash) {
                debug!(
                    chain = %self.source.chain(),
                    height,
                    "head already published; re-announcement, not a fork"
                );
                // Finality still advances. The source re-reads the finalized header
                // per block, so a re-announcement routinely carries a newer watermark
                // than the one published, and dropping it here loses the advance and
                // the ring trim that rides on it.
                self.advance_finality(finalized).await?;
                self.sink.flush().await?;
                return Ok(());
            }

            if parent_hash == previous.hash {
                // Links to the published tip. With no gap this is the next height;
                // a gap means a head was missed, which is a coverage hole rather
                // than a fork, and publishing across it would leave a silent hole.
                if height != latest + 1 {
                    return Err(PipelineError::HeightGap {
                        tip: latest,
                        head: height,
                    });
                }
            } else if let Some(fork) = self.find_fork(parent_hash) {
                // The forked branch is held to the same contiguity as the canonical
                // one. `fork` is the first block of the new branch, so a head above
                // it skips heights of that branch — the same silent hole, reached
                // through a door the check above never covered.
                if height != fork {
                    return Err(PipelineError::ForkedHeightGap { fork, head: height });
                }
                // A genuine fork: retract to the fork point, not to the head.
                self.publish_reorg(fork, hash, parent_hash).await?;
            } else {
                return Err(PipelineError::ForkTooDeep {
                    height,
                    parent: parent_hash,
                    undo_depth: self.undo_depth,
                });
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

        for event in events {
            self.sink
                .publish(Envelope::new(self.source.chain().clone(), event))
                .await?;
        }

        self.history.push_back(PublishedBlock { height, hash });
        self.trim_unless_recent(height);
        self.advance_finality(finalized).await?;
        // One flush per block is the batch boundary: every event published above —
        // a reorg marker, the block's events, and a finality watermark — becomes
        // durable together, so a buffering sink opens its engine once per block
        // instead of once per event.
        self.sink.flush().await?;

        info!(
            chain = %self.source.chain(),
            height,
            finalized_height = self.finalized_height,
            "published block"
        );
        Ok(())
    }

    /// Publishes a watermark if `finalized` is newer than the current one, and drops
    /// ring entries below it, which no reorg can reach.
    async fn advance_finality(&mut self, finalized: BlockId) -> Result<(), PipelineError> {
        if finalized.height <= self.finalized_height {
            return Ok(());
        }
        self.finalized_height = finalized.height;
        self.history
            .retain(|block| block.height >= finalized.height);

        self.sink
            .publish(Envelope::new(
                self.source.chain().clone(),
                Event::Finalized(Finalized {
                    height: finalized.height,
                    hash: finalized.hash,
                }),
            ))
            .await?;
        Ok(())
    }

    /// Retracts the chain to `fork` (the first orphaned height) and emits the reorg marker.
    ///
    /// `fork` is the new branch's first block: the first published block that did not
    /// build on `actual_parent`. The marker names that height, so a consumer retracts
    /// every hash at or above it and re-requests from there.
    ///
    /// # Errors
    ///
    /// Returns [`PipelineError::EmptyReorg`] if the rewind orphaned nothing, and
    /// whatever publishing the marker returns.
    async fn publish_reorg(
        &mut self,
        fork: u64,
        new_head_hash: B256,
        actual_parent: B256,
    ) -> Result<(), PipelineError> {
        let expected_parent = self.history.back().map(|block| block.hash);
        let orphaned_hashes = self.rewind_to(fork);
        // `find_fork` locates `fork` inside the ring, so the rewind must orphan
        // something. Checked on the release path, not with `debug_assert`, because
        // an empty marker here would tell a consumer to retract nothing while the
        // chain has already forked.
        if orphaned_hashes.is_empty() {
            return Err(PipelineError::EmptyReorg { fork });
        }
        warn!(
            chain = %self.source.chain(),
            expected = ?expected_parent,
            %actual_parent,
            fork,
            orphaned = orphaned_hashes.len(),
            "parent hash mismatch; retracted to the fork point and publishing reorg"
        );

        self.sink
            .publish(Envelope::new(
                self.source.chain().clone(),
                Event::Reorg(Reorg {
                    height: fork,
                    new_head_hash,
                    orphaned_hashes,
                }),
            ))
            .await?;
        Ok(())
    }

    /// Whether this exact block is already in the published stream.
    ///
    /// The test for a re-announcement: a head whose height and hash both match a
    /// remembered block carries no new information, whether or not it is the tip.
    /// Only published blocks are remembered, so a fork's replacement block — same
    /// height, different hash — is correctly not a re-announcement.
    fn is_published(&self, height: u64, hash: B256) -> bool {
        self.history
            .iter()
            .any(|block| block.height == height && block.hash == hash)
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
    /// Returns the orphaned block hashes, newest first. Nothing else has to be rewound:
    /// a record's identity is its own natural key, so a retraction is a statement about
    /// which block hashes stopped being canonical rather than a number to hand back.
    fn rewind_to(&mut self, height: u64) -> Vec<B256> {
        let mut orphaned = Vec::new();
        while let Some(block) = self.history.pop_back() {
            if block.height < height {
                self.history.push_back(block);
                break;
            }
            orphaned.push(block.hash);
        }
        orphaned
    }

    /// Keeps the ring bounded without discarding the block just published.
    fn trim_unless_recent(&mut self, height: u64) {
        if self.history.len() <= self.undo_depth {
            return;
        }
        // `ponytail:` the newest entry is only evicted when the source jumps backwards,
        // so a source whose height decreases grows the ring one block per step without
        // limit instead of holding it at `undo_depth`. The ceiling is a peer that
        // walks the tip backwards, which is rarer than a reorg; making it an error
        // here — or evicting the newest entry and losing the ability to retract the
        // block just published — is the upgrade once the ring is rebuilt from durable
        // history.
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

    use super::{DEFAULT_UNDO_DEPTH, Pipeline, PipelineError};
    use crate::wire::envelope::{Block, ChainId, Envelope, Event, Log};

    use crate::sink::{EnvelopeSink, SinkError};

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
            Err(SourceError::Malformed {
                context: "test source".to_owned(),
                detail: "tests pass blocks directly".to_owned(),
            })
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

        /// The block number of each dataset envelope, which is what orders the stream.
        fn block_numbers(&self) -> Vec<u64> {
            self.seen
                .iter()
                .filter_map(|envelope| match &envelope.event {
                    Event::Block(block) => Some(block.number),
                    Event::Log(log) => Some(log.block_number),
                    _ => None,
                })
                .collect()
        }

        /// The block hash of each dataset envelope, in publication order.
        fn block_hashes(&self) -> Vec<&B256> {
            self.seen
                .iter()
                .filter_map(|envelope| match &envelope.event {
                    Event::Block(block) => Some(&block.hash),
                    Event::Log(log) => Some(&log.block_hash),
                    _ => None,
                })
                .collect()
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

        /// The head each reorg marker declares canonical.
        fn reorg_new_heads(&self) -> Vec<B256> {
            self.seen
                .iter()
                .filter_map(|envelope| match &envelope.event {
                    Event::Reorg(reorg) => Some(reorg.new_head_hash),
                    _ => None,
                })
                .collect()
        }
    }

    impl EnvelopeSink for CollectSink {
        async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
            self.seen.push(envelope);
            Ok(())
        }
    }

    fn pipeline() -> Pipeline<FakeSource, CollectSink> {
        let source = FakeSource {
            chain: ChainId::new("ethereum"),
        };
        Pipeline::new(source, CollectSink::default())
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
    async fn publishes_a_linear_chain_in_order() {
        let mut pipeline = pipeline();
        process_all(
            &mut pipeline,
            vec![
                vec![block_event(1, hash(1), hash(0)), log_event(1, hash(1), 0)],
                vec![block_event(2, hash(2), hash(1)), log_event(2, hash(2), 0)],
            ],
        )
        .await;

        assert_eq!(pipeline.sink.kinds(), ["block", "log", "block", "log"]);
        assert_eq!(pipeline.sink.block_numbers(), [1, 1, 2, 2]);
    }

    #[tokio::test]
    async fn depth_one_reorg_retracts_the_replaced_block() {
        let mut pipeline = pipeline();
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
        assert_eq!(pipeline.sink.reorg_orphans(), vec![vec![hash(2)]]);
    }

    #[tokio::test]
    async fn deep_reorg_retracts_every_block_above_the_fork() {
        let mut pipeline = pipeline();
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
        // The replacement carries its own block hash, so a store keying on it writes a
        // new row rather than overwriting the orphaned one.
        assert_eq!(
            pipeline.sink.block_hashes(),
            [
                &hash(1),
                &hash(1),
                &hash(2),
                &hash(2),
                &hash(3),
                &hash(3),
                &hash(20),
                &hash(20),
                &hash(20),
            ]
        );
    }

    #[tokio::test]
    async fn empty_block_publishes_nothing() {
        let mut pipeline = pipeline();
        pipeline
            .process_block(fetched(Vec::new()))
            .await
            .expect("empty block is not an error");

        assert!(pipeline.sink.kinds().is_empty());
    }

    #[tokio::test]
    async fn undo_ring_stays_bounded_across_many_blocks() {
        let mut pipeline = pipeline();
        let past_the_ring =
            u8::try_from(DEFAULT_UNDO_DEPTH + 1).expect("the default depth fits a fixture hash");
        let blocks = (1..=past_the_ring)
            .map(|height| {
                vec![
                    block_event(u64::from(height), hash(height), hash(height - 1)),
                    log_event(u64::from(height), hash(height), 0),
                ]
            })
            .collect();
        process_all(&mut pipeline, blocks).await;

        assert_eq!(pipeline.history.len(), DEFAULT_UNDO_DEPTH);
    }

    #[tokio::test]
    async fn a_reannounced_tip_is_not_a_reorg() {
        let mut pipeline = pipeline();
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
        assert!(pipeline.sink.reorg_orphans().is_empty());
    }

    /// A head for an *older* height is also a re-announcement, not a fork. Only the tip
    /// used to be special-cased, so this one fell through to the fork search: its parent
    /// is the canonical parent, so the fork point resolved to its own height and the
    /// pipeline retracted every block above it. Blocks 4 and 5 were canonical and never
    /// reorged, and the marker named block 3 as canonical *and* as orphaned at once.
    #[tokio::test]
    async fn a_reannounced_head_below_the_tip_is_not_a_reorg() {
        let mut pipeline = pipeline();
        process_all(
            &mut pipeline,
            vec![
                vec![block_event(1, hash(1), hash(0)), log_event(1, hash(1), 0)],
                vec![block_event(2, hash(2), hash(1)), log_event(2, hash(2), 0)],
                vec![block_event(3, hash(3), hash(2)), log_event(3, hash(3), 0)],
                vec![block_event(4, hash(4), hash(3)), log_event(4, hash(4), 0)],
                vec![block_event(5, hash(5), hash(4)), log_event(5, hash(5), 0)],
                // A lagging peer resends head 3, already published, with its canonical
                // parent. Nothing about it is a fork.
                vec![block_event(3, hash(3), hash(2)), log_event(3, hash(3), 0)],
            ],
        )
        .await;

        // No reorg marker at all: the canonical chain is untouched.
        assert!(
            pipeline.sink.reorg_orphans().is_empty(),
            "a re-announcement retracted blocks"
        );
        assert_eq!(
            pipeline.sink.kinds(),
            [
                "block", "log", "block", "log", "block", "log", "block", "log", "block", "log"
            ]
        );
        // Nothing was retracted and nothing was published twice, so the tip is still 5.
        assert_eq!(pipeline.history.back().map(|block| block.height), Some(5));
    }

    /// A marker's new head is never one of the hashes it asks a consumer to retract.
    /// That contradiction is what made the stale-head retraction unrecoverable: a
    /// consumer deleting every orphaned hash would delete the block the marker had just
    /// declared canonical.
    #[tokio::test]
    async fn a_reorg_marker_never_orphans_the_head_it_names() {
        let mut pipeline = pipeline();
        process_all(
            &mut pipeline,
            vec![
                vec![block_event(1, hash(1), hash(0))],
                vec![block_event(2, hash(2), hash(1)), log_event(2, hash(2), 0)],
                vec![block_event(3, hash(3), hash(2)), log_event(3, hash(3), 0)],
                // A genuine replacement of block 3: same height, different hash.
                vec![block_event(3, hash(30), hash(2)), log_event(3, hash(30), 0)],
            ],
        )
        .await;

        let orphans = pipeline.sink.reorg_orphans();
        let new_heads = pipeline.sink.reorg_new_heads();
        assert_eq!(orphans, vec![vec![hash(3)]]);
        assert_eq!(new_heads, vec![hash(30)]);
        assert!(
            !orphans[0].contains(&new_heads[0]),
            "the marker orphaned the head it declared canonical"
        );
    }

    /// A re-announcement is not a no-op for finality. The source re-reads the finalized
    /// header on every block, so a duplicate head routinely carries a watermark newer
    /// than the one published. Returning early without advancing it dropped the advance
    /// and the ring trim that rides on it.
    #[tokio::test]
    async fn a_reannounced_head_still_advances_finality() {
        let mut pipeline = pipeline();
        process_all(
            &mut pipeline,
            vec![
                vec![block_event(1, hash(1), hash(0))],
                vec![block_event(2, hash(2), hash(1))],
            ],
        )
        .await;

        // The same head again, now with a newer finalized height than the run above.
        pipeline
            .process_block(fetched_at(vec![block_event(2, hash(2), hash(1))], 1))
            .await
            .expect("a re-announcement is not an error");

        assert_eq!(pipeline.sink.kinds(), ["block", "block", "finalized"]);
        assert_eq!(pipeline.finalized_height, 1);
    }

    #[tokio::test]
    async fn a_height_gap_is_a_coverage_error_not_a_silent_hole() {
        let mut pipeline = pipeline();
        process_all(&mut pipeline, vec![vec![block_event(1, hash(1), hash(0))]]).await;

        // Head 3 chains from the published block 1, so it links to the tip but skips
        // height 2.
        let error = pipeline
            .process_block(fetched(vec![block_event(3, hash(3), hash(1))]))
            .await
            .expect_err("a gap must fail");
        assert!(
            matches!(error, PipelineError::HeightGap { tip: 1, head: 3 }),
            "a canonical gap must report both heights, not just say 'gap': {error}"
        );
    }

    /// A fork is held to the same contiguity as the canonical branch. Once the fork
    /// search succeeded, the head published with no height check at all, so a new branch
    /// whose head skipped its own heights indexed straight across the hole and the run
    /// ended cleanly — the same silent hole the canonical path refuses, reached through
    /// the one door that skipped the check.
    #[tokio::test]
    async fn a_fork_that_skips_heights_is_a_coverage_error_not_a_silent_hole() {
        let mut pipeline = pipeline();
        process_all(
            &mut pipeline,
            vec![
                vec![block_event(1, hash(1), hash(0))],
                vec![block_event(2, hash(2), hash(1))],
                vec![block_event(3, hash(3), hash(2))],
            ],
        )
        .await;

        // A head at height 5 building on block 2: the fork point is 3, so heights 3 and 4
        // of the new branch never arrive.
        let error = pipeline
            .process_block(fetched(vec![block_event(5, hash(5), hash(2))]))
            .await
            .expect_err("a gap on the forked branch must fail");
        assert!(
            matches!(error, PipelineError::ForkedHeightGap { fork: 3, head: 5 }),
            "a forked gap must be distinguishable from a canonical one: {error}"
        );
        // Nothing was retracted and nothing was published across the hole.
        assert!(pipeline.sink.reorg_orphans().is_empty());
        assert_eq!(pipeline.sink.kinds(), ["block", "block", "block"]);
    }

    /// A fresh pipeline's first head is wherever the chain is, not genesis: there is no
    /// backfill, so the stream starts there. Missing the history before it is the
    /// operator's trade, not an error, so the block is published and the stream follows.
    #[tokio::test]
    async fn a_first_head_above_genesis_starts_the_stream() {
        let mut pipeline = pipeline();
        process_all(
            &mut pipeline,
            vec![
                vec![block_event(5, hash(5), hash(4))],
                vec![block_event(6, hash(6), hash(5))],
            ],
        )
        .await;

        assert_eq!(pipeline.sink.kinds(), ["block", "block"]);
        // No earlier height was invented to stand in as the stream's base.
        assert_eq!(pipeline.history.front().map(|block| block.height), Some(5));
    }

    #[tokio::test]
    async fn a_reorg_deeper_than_the_ring_is_an_error_not_an_empty_marker() {
        // One block past the ring drops the oldest, so a fork under that block cannot
        // be located and retracted.
        let mut pipeline = pipeline();
        let past_the_ring =
            u8::try_from(DEFAULT_UNDO_DEPTH + 1).expect("the default depth fits a fixture hash");
        let blocks = (1..=past_the_ring)
            .map(|height| {
                vec![block_event(
                    u64::from(height),
                    hash(height),
                    hash(height - 1),
                )]
            })
            .collect();
        process_all(&mut pipeline, blocks).await;

        // A new head building on block 1 forks below the remembered range.
        let error = pipeline
            .process_block(fetched(vec![block_event(2, hash(20), hash(1))]))
            .await
            .expect_err("a fork older than the ring must fail");
        // The variant carries the ring's size, so a caller can see how far past it the
        // fork reached instead of only that it did.
        assert!(
            matches!(
                error,
                PipelineError::ForkTooDeep {
                    height: 2,
                    parent: _,
                    undo_depth: DEFAULT_UNDO_DEPTH
                }
            ),
            "a fork below the ring must report the depth it exceeded: {error}"
        );
        assert!(pipeline.sink.reorg_orphans().is_empty());
    }

    #[tokio::test]
    async fn a_reorg_names_the_fork_point_as_its_height() {
        let mut pipeline = pipeline();
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
        let mut pipeline = pipeline();
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
        assert_eq!(pipeline.sink.block_numbers(), [1, 2]);
        // The variant carries the last height it saw, so a stopped live run is
        // diagnosable without reading the formatted message.
        assert!(
            matches!(error, PipelineError::SubscriptionClosed { last_height: 2 }),
            "a closed subscription must report where it stopped: {error}"
        );
    }
}
