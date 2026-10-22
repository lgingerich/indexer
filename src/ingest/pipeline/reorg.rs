//! Heads and forks: catching up to a head, and retracting a branch it orphaned.

use std::sync::Arc;

use tracing::{debug, info};

use super::prefetch::Prefetch;
use super::{Machine, PipelineError};
use crate::ingest::source::{BlockSource, FetchedBlock};
use crate::metrics;
use crate::sink::EnvelopeSink;
use crate::window::MAX_UNFINALIZED_BLOCKS;
use crate::wire::envelope::{BlockMeta, Event, Reorg};

impl<S: BlockSource + 'static, K: EnvelopeSink> Machine<S, K> {
    /// Resolves one observed head against accepted coverage.
    ///
    /// A duplicate accepted head causes no dataset fetch. Ahead of coverage, the gap
    /// is drained by fetching consecutive heights up to the target, reusing the
    /// notification metadata for the target height only. Divergence invokes the
    /// complete reorg operation.
    pub(super) async fn process_head(&mut self, head: BlockMeta) -> Result<(), PipelineError> {
        let tip = self.ring.tip().ok_or(PipelineError::NoTip)?;
        if head == tip {
            return Ok(());
        }
        // An observation already retained at its height needs no work, even if a later
        // block ends up orphaned: only the identity at that height matters.
        if self.accepted(head.height) == Some(head) {
            return Ok(());
        }
        if head.height > tip.height {
            return self.catch_up(tip, head).await;
        }
        // Below every remembered block, the window cannot match the head, but the head
        // may still be an ancestor of what was accepted: a node behind the store, as a
        // load-balanced provider's often is, rather than a fork.
        if self.is_ancestor_below_window(head).await? {
            return Ok(());
        }
        // The head is at or below the tip: a same-height or deeper fork.
        if self.header(head.height).await? != head {
            return Err(PipelineError::UnstableSource);
        }
        self.handle_reorg(head, head).await
    }

    /// Whether `head`, below the oldest remembered block, is that block's ancestor on
    /// the source as it stands: the headers from just under the oldest block down to
    /// `head` link to it, each the parent the one above names, and the one at `head`'s
    /// height is `head`. Then the source is only behind, and there is nothing to do.
    ///
    /// Without this, such a head went to the fork walk, which found no remembered block
    /// below the window and stopped the run with
    /// [`PipelineError::UndoWindowExceeded`], although nothing had forked. A head inside
    /// the window, or one that does not link, is left to the fork walk as before. The
    /// walk is bounded by [`MAX_UNFINALIZED_BLOCKS`] headers; a head further below is too.
    async fn is_ancestor_below_window(&mut self, head: BlockMeta) -> Result<bool, PipelineError> {
        let Some(oldest) = self.ring.oldest() else {
            return Ok(false);
        };
        if head.height >= oldest.height
            || oldest.height - head.height > MAX_UNFINALIZED_BLOCKS as u64
        {
            return Ok(false);
        }
        let mut expected = oldest.parent_hash;
        for height in (head.height..oldest.height).rev() {
            let header = self.header(height).await?;
            if header.hash != expected {
                return Ok(false);
            }
            expected = header.parent_hash;
            if height == head.height {
                return Ok(header == head);
            }
        }
        Ok(false)
    }

    /// Fetches consecutive heights from `tip` to the observed `target`.
    ///
    /// Only a fetch at the target height may reuse the notification metadata, because
    /// that is the only height it describes. A link failure at the next height means
    /// the branch diverged, so the target is re-read and reconciled, the fork walk
    /// starting at the block that did not link.
    ///
    /// The heights after the next are fetched ahead, up to the source's
    /// [`backfill_concurrency`](BlockSource::backfill_concurrency) in flight, each on its
    /// own task, and accepted one at a time in height order with the same link check: a
    /// block costs a round trip, and the reorg window under the head is thousands of them
    /// when a backfill hands over. A divergence drops whatever was fetched ahead, which
    /// stops those fetches, before the reorg is reconciled.
    pub(super) async fn catch_up(
        &mut self,
        mut tip: BlockMeta,
        target: BlockMeta,
    ) -> Result<(), PipelineError> {
        let source = Arc::clone(&self.source);
        let mut fetches = Prefetch::new(Self::next(tip.height)?, source.backfill_concurrency());
        while tip.height < target.height {
            fetches.fill(
                target.height,
                || 1,
                |height, _| {
                    let source = Arc::clone(&source);
                    async move {
                        let head = (height == target.height).then_some(&target);
                        source.fetch_block(height, head).await
                    }
                },
            )?;
            let Some((next, _, block)) = fetches.next().await else {
                break;
            };
            let block = block?;
            let meta = Self::marker(block.meta, next)?;
            if Self::validate_link(tip, meta).is_ok() {
                self.commit(meta, block.events).await?;
                tip = meta;
                continue;
            }
            drop(fetches);
            // Re-read the announced target by height; it must still match before any
            // retraction is emitted.
            if self.header(target.height).await? != target {
                return Err(PipelineError::UnstableSource);
            }
            return self.handle_reorg(target, meta).await;
        }
        Ok(())
    }

    /// Resolves the branch ending at `target` in one bounded walk-and-replay operation.
    ///
    /// Walks back by header from `start`, a block of that branch at or below the target,
    /// to a retained ancestor — or to the first block ever indexed, below which nothing
    /// needs one — and rechecks the target and `start` before output. Retracts only a
    /// nonempty old suffix, then fetches and publishes the replacements ascending
    /// through the target; see [`Self::replay`]. A replay failure leaves the accepted
    /// history at what was delivered, and the first replacement is fetched before the
    /// retraction, so that history is never empty. Delivery can be partial on error, so
    /// public callers consume the instance.
    ///
    /// `start` is the target itself unless the caller knows a lower block of the branch:
    /// catching up, the first block that did not link to the tip. Every block above it is
    /// unaccepted, so walking down to it from the target would read each of their headers
    /// one at a time for nothing, and a fork near the bottom of a long catch-up cost
    /// thousands of sequential reads.
    pub(super) async fn handle_reorg(
        &mut self,
        target: BlockMeta,
        start: BlockMeta,
    ) -> Result<(), PipelineError> {
        // The walk's oldest block so far, and the branch above it, newest first.
        let mut lowest = start;
        let mut above = Vec::new();
        let ancestor = loop {
            let meta = lowest;
            let parent_height =
                meta.height
                    .checked_sub(1)
                    .ok_or(PipelineError::UndoWindowExceeded {
                        floor: Box::new(self.ring.oldest().unwrap_or(meta)),
                        actual: Box::new(meta),
                    })?;
            if let Some(parent) = self.accepted(parent_height)
                && parent.hash == meta.parent_hash
            {
                break parent.height;
            }
            // The branch replaces the first block ever indexed. Nothing was stored below
            // it, so there is no older ancestor to find or retract: adopt the branch from
            // here, as that first block was adopted.
            if self.ring.holds_whole_history()
                && self
                    .ring
                    .oldest()
                    .is_some_and(|oldest| oldest.height == meta.height)
            {
                break parent_height;
            }
            // The common ancestor is below the retained window: stop before any partial
            // retraction rather than guess.
            if !self.ring.retains(parent_height) {
                return Err(PipelineError::UndoWindowExceeded {
                    floor: Box::new(self.ring.oldest().unwrap_or(meta)),
                    actual: Box::new(meta),
                });
            }
            debug!(
                depth = above.len() + 1,
                walked_to = parent_height,
                "reconciling a fork, walking back toward an accepted ancestor"
            );
            let below = self.header(parent_height).await?;
            if below.hash != meta.parent_hash {
                return Err(PipelineError::UnstableSource);
            }
            Self::validate_link(below, meta)?;
            above.push(meta);
            lowest = below;
        };

        // The source may change branches during discovery. Recheck the candidate, and the
        // block the walk started from, at their fixed heights before publishing any
        // retraction: both canonical now puts them on the one branch the walk followed.
        if self.header(target.height).await? != target
            || (start != target && self.header(start.height).await? != start)
        {
            return Err(PipelineError::UnstableSource);
        }
        let block = self.replacement(lowest).await?;

        self.observe_head(target.height);
        let orphaned = self.ring.orphaned(ancestor);
        if !orphaned.is_empty() {
            info!(
                ancestor,
                orphaned = orphaned.len(),
                head = target.height,
                "retracting the blocks a fork orphaned"
            );
            metrics::reorg(orphaned.len());
            self.deliver(vec![Event::Reorg(Reorg {
                height: Self::next(ancestor)?,
                new_head_hash: target.hash,
                orphaned_hashes: orphaned,
            })])
            .await?;
            self.ring.rewind(ancestor);
        }
        self.commit(lowest, block.events).await?;
        above.reverse();
        self.replay(lowest, &above, target).await
    }

    /// Fetches and commits the replacements above `tip` through `target`, fetched ahead
    /// as [`Self::catch_up`] fetches.
    ///
    /// `walked` are the headers a fork walk found above `tip`, ascending: the block at
    /// each of those heights must be the one walked. Above them, up to the target, each
    /// block must link to the one below, and the one at the target must be the target.
    /// Anything else is the source changing branches mid-replay.
    async fn replay(
        &mut self,
        mut tip: BlockMeta,
        walked: &[BlockMeta],
        target: BlockMeta,
    ) -> Result<(), PipelineError> {
        let first = Self::next(tip.height)?;
        let walked_at = |height: u64| walked.get(usize::try_from(height - first).ok()?).copied();
        let source = Arc::clone(&self.source);
        let mut fetches = Prefetch::new(first, source.backfill_concurrency());
        while tip.height < target.height {
            fetches.fill(
                target.height,
                || 1,
                |height, _| {
                    let source = Arc::clone(&source);
                    let head =
                        walked_at(height).or_else(|| (height == target.height).then_some(target));
                    async move { source.fetch_block(height, head.as_ref()).await }
                },
            )?;
            let Some((height, _, block)) = fetches.next().await else {
                break;
            };
            let block = block?;
            let meta = Self::marker(block.meta, height)?;
            let expected = match walked_at(height) {
                Some(walked) => meta == walked,
                None => {
                    Self::validate_link(tip, meta).is_ok()
                        && (height != target.height || meta == target)
                }
            };
            if !expected {
                return Err(PipelineError::UnstableSource);
            }
            self.commit(meta, block.events).await?;
            tip = meta;
        }
        Ok(())
    }

    /// Fetches the block a fork walk found at `meta`, refusing any other.
    async fn replacement(&self, meta: BlockMeta) -> Result<FetchedBlock, PipelineError> {
        let block = self.source.fetch_block(meta.height, Some(&meta)).await?;
        if Self::marker(block.meta, meta.height)? != meta {
            return Err(PipelineError::UnstableSource);
        }
        Ok(block)
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::ingest::pipeline::testing::*;
    use crate::sink::SinkError;
    use crate::window::MAX_UNFINALIZED_BLOCKS;
    use crate::wire::envelope::BlockMeta;

    /// A gap is fetched ahead, as many heights in flight as the source allows, and still
    /// accepted one at a time in height order, each height read once and none taken for
    /// a fork.
    #[tokio::test(start_paused = true)]
    async fn a_gap_is_fetched_ahead_and_accepted_in_order() {
        for concurrency in [1, 4] {
            let mut machine = Machine::new(Source::linear(3), Sink::default(), Vec::new());
            settle(&mut machine, Some(3)).await;
            let source = chain(&mut machine);
            source.data.concurrency = concurrency;
            source.data.slow_blocks = true;
            source
                .data
                .blocks
                .extend((4..=40).map(|h| (h, (hash(h), hash(h - 1)))));
            let (_, before) = reads(&machine);
            observe(&mut machine, 40).await;
            let label = format!("concurrency {concurrency}");
            assert_eq!(heights(&machine), (3..=40).collect::<Vec<_>>(), "{label}");
            assert_eq!(reads(&machine).1 - before, 37, "{label}");
            // The clock is paused, so every fetch requested ahead starts before any ends.
            assert_eq!(
                machine.source.data.blocks_peak.load(Ordering::Relaxed),
                concurrency,
                "{label}: fetches in flight at once"
            );
            assert!(reorgs(&machine).is_empty(), "{label}");
            assert_eq!(machine.tip(), Some(meta(40)), "{label}");
        }
    }

    /// A fork that orphans the tip while heights are fetched ahead drops those fetches,
    /// retracts the tip once, and replays the new branch in order.
    #[tokio::test(start_paused = true)]
    async fn a_fork_under_fetches_made_ahead_is_reconciled() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 5;
        let mut machine = Machine::new(Source::linear(head), Sink::default(), Vec::new());
        settle(&mut machine, Some(0)).await;
        let tip = machine.tip().expect("tip").height;
        let source = chain(&mut machine);
        source.data.concurrency = 4;
        source.data.slow_blocks = true;
        // The tip is replaced, and the new branch runs ten blocks past it.
        fork(source, tip, tip + 10);
        observe(&mut machine, tip + 10).await;
        let markers = reorgs(&machine);
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].height, tip);
        assert_eq!(markers[0].orphaned_hashes, [hash(tip)]);
        assert_eq!(machine.tip().expect("tip").hash, hash(tip + 110));
        let replayed: Vec<u64> = heights(&machine)
            .into_iter()
            .skip_while(|&height| height != tip)
            .skip(1)
            .collect();
        assert_eq!(replayed, (tip..=tip + 10).collect::<Vec<_>>());
    }

    /// A fork near the bottom of a long catch-up is walked from the block that did not
    /// link, not from the head: a handful of header reads rather than one per height of
    /// the tail, and the new branch is replayed with fetches made ahead, not one block
    /// at a time.
    #[tokio::test(start_paused = true)]
    async fn a_fork_under_a_long_catch_up_is_walked_from_where_it_diverged() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 5;
        let mut machine = Machine::new(Source::linear(head), Sink::default(), Vec::new());
        settle(&mut machine, Some(0)).await;
        let tip = machine.tip().expect("tip").height;
        let source = chain(&mut machine);
        source.data.concurrency = 4;
        source.data.slow_blocks = true;
        // The tip is replaced, and the new branch runs 300 blocks past it.
        let tail = 300;
        fork(source, tip, tip + tail);
        let started = tokio::time::Instant::now();
        observe(&mut machine, tip + tail).await;
        let markers = reorgs(&machine);
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].height, tip);
        assert_eq!(markers[0].orphaned_hashes, [hash(tip)]);
        assert_eq!(markers[0].new_head_hash, hash(tip + tail + 100));
        let replayed: Vec<u64> = heights(&machine)
            .into_iter()
            .skip_while(|&height| height != tip)
            .skip(1)
            .collect();
        assert_eq!(replayed, (tip..=tip + tail).collect::<Vec<_>>());
        assert_eq!(
            machine.source.data.header_calls.load(Ordering::Relaxed),
            4,
            "the target, before the walk and again before the retraction, the block the \
             walk started from, and the one header it walked"
        );
        // The clock is paused, so the time taken is the fetches' waits: one at a time,
        // the tail's replay alone would take `tail` of them.
        let one = std::time::Duration::from_millis(5);
        assert!(
            started.elapsed() < one * u32::try_from(tail).expect("tail") / 2,
            "took {:?}",
            started.elapsed()
        );
    }

    /// A restart whose node is behind the store, below every block the store handed
    /// back, is not a fork when that head is the oldest block's ancestor: nothing is
    /// retracted or published, and the run carries on. DST-002: found by the simulation
    /// as a stop with `UndoWindowExceeded` on seed 236; see `docs/dst-changelog.md`.
    #[tokio::test]
    async fn a_head_behind_the_store_below_the_window_is_not_a_fork() {
        let mut source = Source::linear(13);
        // A fresh run stored 12 and 13; the node now reports 11 as its head.
        source.data.head = 11;
        let mut machine = Machine::new(source, Sink::default(), ledger(12..=13));
        machine
            .sync(None, None)
            .await
            .expect("a lagging head is not a fork");
        assert!(machine.sink.events.is_empty());
        assert_eq!(machine.tip(), Some(meta(13)));
    }

    /// A head below the window that does not lead to the oldest remembered block is a
    /// fork the window cannot reach, and still stops the run.
    #[tokio::test]
    async fn a_forked_head_below_the_window_still_stops() {
        let mut source = Source::linear(13);
        source.data.head = 11;
        source.data.blocks.insert(11, (hash(111), hash(10)));
        let mut machine = Machine::new(source, Sink::default(), ledger(12..=13));
        fails_with(
            machine.sync(None, None).await,
            &PipelineError::UndoWindowExceeded {
                floor: Box::new(meta(12)),
                actual: Box::new(BlockMeta {
                    hash: hash(111),
                    ..meta(11)
                }),
            },
        );
        assert!(machine.sink.events.is_empty());
        assert_eq!(machine.tip(), Some(meta(13)));
    }

    /// A head already accepted, at the tip or below it in the window, is neither fetched
    /// again nor published again.
    #[tokio::test]
    async fn a_duplicate_head_is_not_fetched_again() {
        let mut machine = Machine::new(Source::linear(4), Sink::default(), Vec::new());
        settle(&mut machine, Some(2)).await;
        let events = machine.sink.events.clone();
        let before = reads(&machine);
        for head in [meta(4), meta(3), meta(2)] {
            machine.process_head(head).await.expect("duplicate");
            assert_eq!(
                reads(&machine),
                before,
                "head {}: nothing read",
                head.height
            );
            assert_eq!(
                machine.sink.events, events,
                "head {}: nothing published",
                head.height
            );
        }
        assert_eq!(machine.tip(), Some(meta(4)));
    }

    #[tokio::test]
    async fn a_same_height_fork_retracts_the_tail() {
        // Leave room in the window: the fork's common ancestor must stay retained.
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 5;
        let mut machine = Machine::new(Source::linear(head), Sink::default(), Vec::new());
        settle(&mut machine, Some(0)).await;
        // Replace the branch above the tip's predecessor and announce the new head.
        chain(&mut machine)
            .data
            .blocks
            .insert(head, (hash(head + 100), hash(head - 1)));
        observe(&mut machine, head).await;
        assert_eq!(machine.tip().expect("tip").hash, hash(head + 100));
        let markers = reorgs(&machine);
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].height, head);
        assert_eq!(markers[0].orphaned_hashes, [hash(head)]);
        assert_eq!(
            heights(&machine)[usize::try_from(head).expect("index")..],
            [head, head],
            "the tip, then its replacement"
        );
    }

    /// A fresh run's first block has nothing stored below it, so a fork that replaces
    /// it is retracted and the new branch adopted from the start height, whatever the
    /// branch's parent. Found by the simulation: see `docs/dst-changelog.md`.
    #[tokio::test]
    async fn a_fork_replacing_the_first_indexed_block_adopts_the_new_branch() {
        let mut machine = Machine::new(Source::linear(3), Sink::default(), Vec::new());
        settle(&mut machine, Some(3)).await;
        // The window holds only the first block; its replacement descends from height 1.
        chain(&mut machine)
            .data
            .blocks
            .insert(3, (hash(103), hash(1)));
        // The replacement is fetched before the first block is retracted, so a failure
        // cannot leave the window empty.
        machine.source.data.fail_once.store(true, Ordering::Relaxed);
        fails_with(
            machine.process_head(machine.source.head_meta()).await,
            &malformed("injected source failure"),
        );
        assert!(reorgs(&machine).is_empty());
        assert_eq!(machine.tip(), Some(meta(3)));
        machine
            .process_head(machine.source.head_meta())
            .await
            .expect("the fork is resolved");
        let markers = reorgs(&machine);
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].height, 3);
        assert_eq!(markers[0].orphaned_hashes, [hash(3)]);
        assert_eq!(
            heights(&machine),
            [3, 3],
            "the first block, then its replacement"
        );
        assert_eq!(machine.tip().expect("tip").hash, hash(103));
    }

    #[tokio::test]
    async fn reorg_publication_failure_preserves_old_history() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 5;
        let mut machine = Machine::new(Source::linear(head), Sink::default(), Vec::new());
        settle(&mut machine, Some(0)).await;
        chain(&mut machine)
            .data
            .blocks
            .insert(head, (hash(head + 100), hash(head - 1)));
        machine.sink.fail = true;
        fails_with(
            machine.process_head(machine.source.head_meta()).await,
            &PipelineError::Sink(SinkError::StorageClosed),
        );
        // The failed retraction leaves the accepted branch and its tip untouched, and
        // the reorg record itself never reached the sink.
        assert_eq!(machine.tip(), Some(meta(head)));
        assert_eq!(heights(&machine), (0..=head).collect::<Vec<_>>());
        assert!(reorgs(&machine).is_empty());
    }

    #[tokio::test]
    async fn source_failure_during_discovery_leaves_old_history_unchanged() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 5;
        let mut machine = Machine::new(Source::linear(head), Sink::default(), Vec::new());
        settle(&mut machine, Some(0)).await;
        chain(&mut machine)
            .data
            .blocks
            .insert(head, (hash(head + 100), hash(head - 1)));
        let candidate = machine
            .source
            .fetch_header(Some(head))
            .await
            .expect("candidate");
        // Injection fires on the replacement's fetch, which precedes any retraction, so the
        // accepted history must survive intact.
        machine.source.data.fail_once.store(true, Ordering::Relaxed);
        fails_with(
            machine.handle_reorg(candidate, candidate).await,
            &malformed("injected source failure"),
        );
        assert_eq!(machine.tip(), Some(meta(head)));
        assert_eq!(heights(&machine), (0..=head).collect::<Vec<_>>());
        assert!(reorgs(&machine).is_empty());
    }
}
