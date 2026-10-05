//! Finality-anchored ingestion using the existing source and sink contracts.
//!
//! TODO: Resume from the last storage-committed finalized `BlockId`, validate it against
//! the source, and reconcile or replace the stored unfinalized suffix before replay.
//! Couple finality checkpoints to atomic storage commits, not channel acceptance.
//! Decide whether undo history should cover the full finality lag or a smaller sliding
//! reorg window with explicit deep-fork recovery; a larger memory limit alone cannot
//! recover prior-run orphaned rows or make replay idempotent.
//!
//! History through a captured finalized block is backfilled; the remaining tail is
//! always reorg-aware. Progress is process-local. Restarting does not retract a prior
//! run's stored branch, and sink flushing means acceptance, not storage durability.
//! The source fetches by height: changing views are detected, not hash-pinned.

use crate::ingest::source::{BlockId, BlockSource, FetchedBlock, SourceError};
use crate::sink::{EnvelopeSink, SinkError};
use crate::wire::envelope::{Envelope, Event, Finalized, Reorg};
use alloy_primitives::B256;
use futures_util::StreamExt as _;
use std::collections::VecDeque;
use thiserror::Error;

/// Hardcoded budget for the complete published unfinalized tail.
pub const MAX_UNFINALIZED_BLOCKS: usize = 4096;

/// Why ingestion cannot continue safely.
#[derive(Debug, Error)]
pub enum PipelineError {
    /// A source returned no leading block marker.
    #[error("block {height} has no leading block marker")]
    MissingBlockMarker {
        /// Requested height.
        height: u64,
    },
    /// Returned block identity disagrees with the request.
    #[error("requested block {expected:?}, received {actual:?}")]
    IdentityMismatch {
        /// Expected identity.
        expected: BlockId,
        /// Returned identity.
        actual: BlockId,
    },
    /// Heights or parent hashes do not form a contiguous chain.
    #[error("block {actual:?} does not extend {previous:?}")]
    BrokenLink {
        /// Expected predecessor.
        previous: BlockId,
        /// Returned successor.
        actual: BlockId,
    },
    /// A source conflicts with irrevocable history.
    #[error("finality conflict: expected {expected:?}, received {actual:?}")]
    FinalityViolation {
        /// Finalized identity.
        expected: BlockId,
        /// Conflicting identity.
        actual: BlockId,
    },
    /// An explicit source observation regressed.
    #[error("finality regressed from {from:?} to {to:?}")]
    FinalityRegressed {
        /// Previous observation.
        from: BlockId,
        /// New observation.
        to: BlockId,
    },
    /// The full unfinalized tail does not fit the fixed budget.
    #[error("undo budget {capacity} cannot cover tail {tail}")]
    UndoCapacity {
        /// Fixed budget.
        capacity: usize,
        /// Required number of identities.
        tail: u64,
    },
    /// Historical start must not skip the finalized anchor.
    #[error("historical start {start} is above finalized block {finalized}")]
    StartAfterFinalized {
        /// Requested start.
        start: u64,
        /// Captured finalized height.
        finalized: u64,
    },
    /// A height-only source changed views during reconciliation.
    #[error("source changed canonical views during reconciliation")]
    UnstableSource,
    /// Height progression would overflow.
    #[error("cannot advance beyond height {height}")]
    HeightOverflow {
        /// Last height.
        height: u64,
    },
    /// A live subscription ended without an error item.
    #[error("head subscription closed")]
    SubscriptionClosed,
    /// Source failure.
    #[error(transparent)]
    Source(#[from] SourceError),
    /// Sink failure.
    #[error(transparent)]
    Sink(#[from] SinkError),
}

/// Scheduling state. Reorg discovery and replay are one bounded operation.
#[derive(Debug)]
pub enum State {
    /// Capture finality and choose the first block.
    Starting,
    /// Index immutable history through the captured anchor.
    Backfilling {
        /// Next historical height.
        next: u64,
        /// Exact captured finalized target.
        target: BlockId,
    },
    /// Reconcile the unfinalized tail, including ordinary catch-up.
    Syncing,
    /// Resolve and replay a replacement branch.
    Reorg,
}

/// Accepted chain identities: one finalized anchor and an ordered unfinalized tail.
/// During historical startup, only the previous block is retained for linkage.
#[derive(Debug, Default)]
struct UndoRing {
    // One immutable linkage identity, not an undoable block.
    anchor: Option<BlockId>,
    // Published unfinalized tail. During historical startup, this holds only the
    // previous block for linkage until the captured finalized anchor is published.
    entries: VecDeque<BlockId>,
}

impl UndoRing {
    /// Returns the newest accepted block, or the anchor if the tail is empty.
    /// Returns `None` before any block has been accepted.
    fn tip(&self) -> Option<BlockId> {
        self.entries.back().copied().or(self.anchor)
    }

    /// Looks up a remembered identity, including the finalized anchor.
    /// Absence means this height is not retained, not that it is noncanonical.
    fn at(&self, height: u64) -> Option<BlockId> {
        self.anchor
            .filter(|id| id.height == height)
            .or_else(|| self.entries.iter().find(|id| id.height == height).copied())
    }

    /// Checks an observation against applied finality without updating the ring.
    /// Rejects regression or a different hash at the anchor's height. Observations
    /// ahead of indexed coverage are allowed; they are not retained here.
    fn check_finality(&self, next: BlockId) -> Result<(), PipelineError> {
        if let Some(old) = self.anchor {
            if next.height < old.height {
                return Err(PipelineError::FinalityRegressed {
                    from: old,
                    to: next,
                });
            }
            if next.height == old.height && next != old {
                return Err(PipelineError::FinalityViolation {
                    expected: old,
                    actual: next,
                });
            }
        }
        Ok(())
    }

    /// Collects hashes above a previously verified ancestor, newest first.
    /// Does not remove them: the sink must accept their retraction before rewind.
    fn orphaned(&self, ancestor: BlockId) -> Vec<B256> {
        self.entries
            .iter()
            .rev()
            .take_while(|id| id.height > ancestor.height)
            .map(|id| id.hash)
            .collect()
    }

    /// Removes the suffix above a verified ancestor after retraction is accepted.
    /// Keeps the ancestor and finalized anchor available for replacement linkage.
    fn rewind(&mut self, ancestor: BlockId) {
        self.entries.retain(|id| id.height <= ancestor.height);
    }

    /// Advances the anchor and discards identities at or below it.
    /// The caller validates the identity and delivers its finality marker first.
    fn finalize(&mut self, finalized: BlockId) {
        self.entries.retain(|id| id.height > finalized.height);
        self.anchor = Some(finalized);
    }
}

/// One process-local driver, consumed by its public asynchronous operations.
#[derive(Debug)]
pub struct Machine<S, K> {
    source: S,
    sink: K,
    state: State,
    ring: UndoRing,
}

impl<S, K> Machine<S, K> {
    /// Starts at the source-finalized anchor with a hardcoded undo budget.
    #[must_use]
    pub fn new(source: S, sink: K) -> Self {
        Self {
            source,
            sink,
            state: State::Starting,
            ring: UndoRing::default(),
        }
    }

    /// Current scheduling state.
    #[must_use]
    pub fn state(&self) -> &State {
        &self.state
    }

    /// Highest block accepted by the sink.
    #[must_use]
    pub fn tip(&self) -> Option<BlockId> {
        self.ring.tip()
    }

    /// Accepted finality identity, distinct from source observations ahead of coverage.
    #[must_use]
    pub fn emitted_finality(&self) -> Option<BlockId> {
        self.ring.anchor
    }

    /// Number of fully undoable unfinalized blocks, excluding the finalized anchor.
    #[must_use]
    pub fn undo_depth(&self) -> usize {
        self.ring.entries.len()
    }

    /// Finds a sink-accepted identity used for duplicate and ancestor checks.
    /// Only the anchor and retained tail can be matched; older history is absent.
    fn accepted(&self, height: u64) -> Option<BlockId> {
        self.ring.at(height)
    }

    /// Reads the leading block's identity and parent without copying its events.
    /// Rejects a missing marker or a number different from the requested height.
    /// Does not establish canonicality or validate linkage to another block.
    fn marker(block: &FetchedBlock, height: u64) -> Result<(BlockId, B256), PipelineError> {
        let Some(Event::Block(marker)) = block.events.first() else {
            return Err(PipelineError::MissingBlockMarker { height });
        };
        let id = BlockId {
            height: marker.number,
            hash: marker.hash,
        };
        if id.height != height {
            return Err(PipelineError::IdentityMismatch {
                expected: BlockId {
                    height,
                    hash: id.hash,
                },
                actual: id,
            });
        }
        Ok((id, marker.parent_hash))
    }

    /// Requires a successor at exactly the next height with the previous hash as parent.
    /// Used both during backward discovery and before ascending publication.
    fn validate_link(previous: BlockId, id: BlockId, parent: B256) -> Result<(), PipelineError> {
        if previous.height.checked_add(1) != Some(id.height) || previous.hash != parent {
            return Err(PipelineError::BrokenLink {
                previous,
                actual: id,
            });
        }
        Ok(())
    }

    /// Computes the next height, refusing overflow rather than wrapping or stalling.
    fn next(height: u64) -> Result<u64, PipelineError> {
        height
            .checked_add(1)
            .ok_or(PipelineError::HeightOverflow { height })
    }

    /// Checks that the sampled head is compatible with finality and its full tail fits.
    /// Measures `head - finalized`, not the length of historical backfill. The fixed
    /// budget must be strictly larger than that distance; no identities are evicted.
    fn check_budget(head: BlockId, finalized: BlockId) -> Result<(), PipelineError> {
        let Some(tail) = head.height.checked_sub(finalized.height) else {
            return Err(PipelineError::FinalityViolation {
                expected: finalized,
                actual: head,
            });
        };
        if head.height == finalized.height && head != finalized {
            return Err(PipelineError::FinalityViolation {
                expected: finalized,
                actual: head,
            });
        }
        if tail >= MAX_UNFINALIZED_BLOCKS as u64 {
            return Err(PipelineError::UndoCapacity {
                capacity: MAX_UNFINALIZED_BLOCKS,
                tail,
            });
        }
        Ok(())
    }
}

impl<S: BlockSource, K: EnvelopeSink> Machine<S, K> {
    /// Drives finalized startup, catch-up, and live indexing.
    ///
    /// Catches up sequentially before waiting for the next head hint. Source errors
    /// propagate without retries. The socket is not polled during fetches or delivery;
    /// prolonged backpressure may disconnect it.
    ///
    /// # Errors
    /// Consumes this instance. An error or cancellation drops its source, sink, and
    /// history, so partially delivered output cannot be followed by reuse.
    /// Returns permanent source, invariant, or sink failures.
    pub async fn run(mut self) -> Result<(), PipelineError> {
        let mut heads = self.source.subscribe_heads().await?;
        loop {
            while self.step().await? {}
            match heads.next().await {
                Some(head) => {
                    head?;
                }
                None => return Err(PipelineError::SubscriptionClosed),
            }
        }
    }

    /// Consumes this instance to index earlier immutable history.
    ///
    /// Returns ownership only on success, allowing the result to be passed to `run`.
    /// Failure or cancellation drops the instance and any buffered partial output.
    ///
    /// # Errors
    /// Returns an error if called after startup, if `from` skips finality, or if delivery
    /// fails. This does not restore a previous run's stored history.
    pub async fn backfill(mut self, from: u64) -> Result<Self, PipelineError> {
        if !matches!(self.state, State::Starting) {
            return Err(PipelineError::UnstableSource);
        }
        self.start(Some(from)).await?;
        while matches!(self.state, State::Backfilling { .. }) {
            self.step().await?;
        }
        Ok(self)
    }

    /// Performs the next operation for the current state.
    /// Returns `true` when the driver should continue immediately, and `false` only
    /// when syncing matches the sampled head and may wait for a notification.
    async fn step(&mut self) -> Result<bool, PipelineError> {
        match self.state {
            State::Starting => self.start(None).await?,
            State::Backfilling { next, target } => self.backfill_step(next, target).await?,
            State::Syncing | State::Reorg => return self.sync_step().await,
        }
        Ok(true)
    }

    /// Samples the head and its fetch-time finality, then captures the backfill target.
    /// `None` starts at the finalized block itself; `Some` requests earlier history.
    /// Validates identity and capacity before output, then enters `Backfilling`.
    async fn start(&mut self, from: Option<u64>) -> Result<(), PipelineError> {
        let head = self.source.current_head().await?;
        let probe = self.source.fetch_block(head.height).await?;
        let finalized = probe.finalized;
        let (id, _) = Self::marker(&probe, head.height)?;
        if id != head {
            return Err(PipelineError::IdentityMismatch {
                expected: head,
                actual: id,
            });
        }
        Self::check_budget(head, finalized)?;
        let next = from.unwrap_or(finalized.height);
        if next > finalized.height {
            return Err(PipelineError::StartAfterFinalized {
                start: next,
                finalized: finalized.height,
            });
        }
        self.ring.check_finality(finalized)?;
        self.state = State::Backfilling {
            next,
            target: finalized,
        };
        Ok(())
    }

    /// Fetches and publishes one historical block, validating linkage incrementally.
    /// Retains only that block for the next append. At the captured target, checks its
    /// exact hash, publishes finality, and transitions to reorg-aware `Syncing`.
    async fn backfill_step(&mut self, next: u64, target: BlockId) -> Result<(), PipelineError> {
        let fetched = self.source.fetch_block(next).await?;
        let (id, _) = Self::marker(&fetched, next)?;
        if next == target.height && id != target {
            return Err(PipelineError::FinalityViolation {
                expected: target,
                actual: id,
            });
        }
        self.append(fetched, next, false).await?;
        if next == target.height {
            // The captured anchor remains available even when newer observations are
            // ahead of indexed coverage; publish that exact identity first.
            self.publish_finality(target).await?;
            self.state = State::Syncing;
        } else {
            self.state = State::Backfilling {
                next: Self::next(next)?,
                target,
            };
        }
        Ok(())
    }

    /// Refreshes the HTTP canonical head and finality, then resolves indexed coverage.
    /// A duplicate may still advance finality. A gap publishes the next linked block;
    /// divergence invokes the complete reorg operation. Returns `false` for a duplicate
    /// and `true` after publication/reconciliation so the driver probes again.
    async fn sync_step(&mut self) -> Result<bool, PipelineError> {
        let head = self.source.current_head().await?;
        let fetched = self.source.fetch_block(head.height).await?;
        let finalized = fetched.finalized;
        let (id, _) = Self::marker(&fetched, head.height)?;
        if id != head {
            return Err(PipelineError::UnstableSource);
        }
        self.ring.check_finality(finalized)?;
        self.emit_finality(finalized).await?;
        Self::check_budget(head, finalized)?;
        if self.accepted(head.height) == Some(head) {
            return Ok(false);
        }
        let Some(tip) = self.ring.tip() else {
            return Err(PipelineError::UnstableSource);
        };
        if head.height > tip.height {
            let next = Self::next(tip.height)?;
            let block = if next == head.height {
                fetched
            } else {
                self.source.fetch_block(next).await?
            };
            let (id, parent) = Self::marker(&block, next)?;
            if Self::validate_link(tip, id, parent).is_ok() {
                if id.height == finalized.height && id != finalized {
                    return Err(PipelineError::FinalityViolation {
                        expected: finalized,
                        actual: id,
                    });
                }
                self.append(block, next, true).await?;
                self.emit_finality(finalized).await?;
                return Ok(true);
            }
            // Re-read the target by height only when needed; the existing source cannot
            // pin a hash. Every edge is checked before any retraction is emitted.
            let target = self.source.fetch_block(head.height).await?;
            if Self::marker(&target, head.height)?.0 != head {
                return Err(PipelineError::UnstableSource);
            }
            self.handle_reorg(target, head.height).await?;
            return Ok(true);
        }
        self.handle_reorg(fetched, head.height).await?;
        Ok(true)
    }

    /// Resolves a candidate branch in one bounded walk-and-replay operation.
    /// Validates backward edges, finds a remembered ancestor, and rechecks the target
    /// before output. Retracts only a nonempty old suffix, then publishes replacements
    /// ascending and returns to `Syncing`. Discovery failures leave history untouched;
    /// delivery can be partial on error, so public callers consume the instance.
    async fn handle_reorg(&mut self, head: FetchedBlock, height: u64) -> Result<(), PipelineError> {
        self.state = State::Reorg;
        let target = Self::marker(&head, height)?.0;
        // One fresh observation applies to this operation; reverse-fetched payload
        // snapshots are not treated as new finality observations during replay.
        let finalized = head.finalized;
        self.ring.check_finality(finalized)?;
        // Newest first while walking; replay consumes this vector in reverse.
        let mut branch = vec![(height, head)];
        let ancestor = loop {
            let (at, first) = branch.last().ok_or(PipelineError::UnstableSource)?;
            let (id, parent_hash) = Self::marker(first, *at)?;
            let height = id
                .height
                .checked_sub(1)
                .ok_or(PipelineError::FinalityViolation {
                    expected: self.ring.anchor.unwrap_or(id),
                    actual: id,
                })?;
            let parent = BlockId {
                height,
                hash: parent_hash,
            };
            if parent.height == finalized.height && parent != finalized {
                return Err(PipelineError::FinalityViolation {
                    expected: finalized,
                    actual: parent,
                });
            }
            if self.accepted(height) == Some(parent) {
                break parent;
            }
            if let Some(floor) = self.ring.anchor
                && height <= floor.height
            {
                return Err(PipelineError::FinalityViolation {
                    expected: floor,
                    actual: parent,
                });
            }
            if branch.len() >= MAX_UNFINALIZED_BLOCKS {
                return Err(PipelineError::UndoCapacity {
                    capacity: MAX_UNFINALIZED_BLOCKS,
                    tail: branch.len() as u64 + 1,
                });
            }
            let below = self.source.fetch_block(height).await?;
            let below_id = Self::marker(&below, height)?.0;
            if below_id != parent {
                return Err(PipelineError::UnstableSource);
            }
            Self::validate_link(below_id, id, parent_hash)?;
            branch.push((height, below));
        };

        // The height-only source may change branches during discovery. Verify the
        // candidate and finalized identity before publishing any retraction.
        let current = self.source.fetch_block(target.height).await?;
        if Self::marker(&current, target.height)?.0 != target {
            return Err(PipelineError::UnstableSource);
        }
        if finalized.height <= target.height
            && finalized.height > ancestor.height
            && branch
                .iter()
                .find(|(height, _)| *height == finalized.height)
                .is_none_or(|(height, block)| {
                    !Self::marker(block, *height).is_ok_and(|(id, _)| id == finalized)
                })
        {
            return Err(PipelineError::FinalityViolation {
                expected: finalized,
                actual: target,
            });
        }

        let orphaned = self.ring.orphaned(ancestor);
        if !orphaned.is_empty() {
            self.deliver(vec![Event::Reorg(Reorg {
                height: Self::next(ancestor.height)?,
                new_head_hash: target.hash,
                orphaned_hashes: orphaned,
            })])
            .await?;
            self.ring.rewind(ancestor);
            // No source reads remain. Dropping during any replay delivery is terminal.
        }
        for (height, block) in branch.into_iter().rev() {
            self.append(block, height, true).await?;
            self.emit_finality(finalized).await?;
        }
        self.state = State::Syncing;
        Ok(())
    }

    /// Validates and delivers one block before recording its accepted identity.
    /// `unfinalized = true` retains the undoable tail and enforces its capacity;
    /// `false` keeps only the previous historical block for the next linkage check.
    /// This method does not change scheduling state or apply fetch-time finality.
    async fn append(
        &mut self,
        block: FetchedBlock,
        height: u64,
        unfinalized: bool,
    ) -> Result<(), PipelineError> {
        let (id, parent) = Self::marker(&block, height)?;
        if let Some(previous) = self.ring.tip() {
            Self::validate_link(previous, id, parent)?;
        }
        if let Some(finalized) = self.ring.anchor
            && id.height <= finalized.height
        {
            return Err(PipelineError::FinalityViolation {
                expected: finalized,
                actual: id,
            });
        }
        if unfinalized && self.ring.entries.len() >= MAX_UNFINALIZED_BLOCKS {
            return Err(PipelineError::UndoCapacity {
                capacity: MAX_UNFINALIZED_BLOCKS,
                tail: self.ring.entries.len() as u64 + 1,
            });
        }
        self.deliver(block.events).await?;
        if !unfinalized {
            // Before the captured anchor is reached, only the previous historical
            // block is needed for incremental linkage; no finality is emitted yet.
            self.ring.entries.clear();
        }
        self.ring.entries.push_back(id);
        Ok(())
    }

    /// Announces an observation only when its exact identity is already accepted.
    /// Rejects regression against the applied anchor; an ahead-of-coverage or different
    /// branch identity is deferred without storing another watermark. Duplicates emit
    /// nothing. Successful announcements flush even when no new block was published.
    async fn emit_finality(&mut self, observed: BlockId) -> Result<(), PipelineError> {
        self.ring.check_finality(observed)?;
        if self.ring.anchor == Some(observed) {
            return Ok(());
        }
        let Some(accepted) = self.accepted(observed.height) else {
            return Ok(());
        };
        if accepted != observed {
            // New finality can certify a replacement not yet reconciled. Do not
            // finalize the old branch; validate the candidate before retracting it.
            return Ok(());
        }
        self.publish_finality(observed).await
    }

    /// Delivers a finality marker, then advances the anchor and drops finalized entries.
    /// The caller must establish that this identity belongs to accepted coverage.
    /// Sink failure leaves the ring unchanged; acceptance is not a durable checkpoint.
    async fn publish_finality(&mut self, finalized: BlockId) -> Result<(), PipelineError> {
        self.ring.check_finality(finalized)?;
        self.deliver(vec![Event::Finalized(Finalized {
            height: finalized.height,
            hash: finalized.hash,
        })])
        .await?;
        self.ring.finalize(finalized);
        Ok(())
    }

    /// Moves events to the sink in order and flushes their batch, including control-only
    /// batches. Does not update chain history. Failure can follow partial acceptance;
    /// there is no rollback, retry, or guarantee of storage durability here.
    async fn deliver(&mut self, events: Vec<Event>) -> Result<(), PipelineError> {
        for event in events {
            self.sink
                .publish(Envelope::new(self.source.chain().clone(), event))
                .await?;
        }
        self.sink.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    use futures_util::stream;

    use super::*;
    use crate::ingest::source::HeadStream;
    use crate::wire::envelope::{Block, ChainId};

    /// Produces deterministic distinct identities for test heights and replacement blocks.
    fn hash(height: u64) -> B256 {
        B256::from(alloy_primitives::U256::from(height).to_be_bytes())
    }

    struct Source {
        chain: ChainId,
        data: Data,
    }

    struct Data {
        blocks: HashMap<u64, (B256, B256)>,
        head: u64,
        finalized: u64,
        fail_once: AtomicBool,
    }

    impl Source {
        /// Builds a canonical chain from genesis through `head` with chosen finality.
        fn linear(head: u64, finalized: u64) -> Self {
            Self {
                chain: ChainId::new("ethereum"),
                data: Data {
                    blocks: (0..=head)
                        .map(|h| (h, (hash(h), hash(h.saturating_sub(1)))))
                        .collect(),
                    head,
                    finalized,
                    fail_once: AtomicBool::new(false),
                },
            }
        }

        /// Replaces a contiguous suffix with new hashes while preserving parent linkage.
        fn fork(&mut self, from: u64, through: u64) {
            let data = &mut self.data;
            for height in from..=through {
                let parent = data.blocks.get(&(height - 1)).expect("parent").0;
                data.blocks.insert(height, (hash(height + 100), parent));
            }
            data.head = through;
        }
    }

    impl BlockSource for Source {
        fn chain(&self) -> &ChainId {
            &self.chain
        }
        async fn subscribe_heads(&self) -> Result<HeadStream, SourceError> {
            Ok(Box::pin(stream::pending()))
        }
        async fn current_head(&self) -> Result<BlockId, SourceError> {
            let data = &self.data;
            Ok(BlockId {
                height: data.head,
                hash: data.blocks[&data.head].0,
            })
        }
        async fn fetch_block(&self, height: u64) -> Result<FetchedBlock, SourceError> {
            let data = &self.data;
            if data.fail_once.swap(false, Ordering::Relaxed) {
                return Err(SourceError::Closed {
                    code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Away,
                });
            }
            let Some(&(id, parent)) = data.blocks.get(&height) else {
                return Err(SourceError::Malformed {
                    context: "test".into(),
                    detail: "missing block".into(),
                });
            };
            Ok(FetchedBlock {
                events: vec![Event::Block(Box::new(Block {
                    number: height,
                    hash: id,
                    parent_hash: parent,
                    ..Block::default()
                }))],
                finalized: BlockId {
                    height: data.finalized,
                    hash: data.blocks[&data.finalized].0,
                },
            })
        }
    }

    #[derive(Default)]
    struct Sink {
        events: Vec<Event>,
        flushes: usize,
        fail: bool,
        stall: bool,
        dropped: Option<tokio::sync::oneshot::Sender<()>>,
    }

    impl Drop for Sink {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.take() {
                let _ = dropped.send(());
            }
        }
    }

    impl EnvelopeSink for Sink {
        async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
            if self.fail {
                return Err(SinkError::StorageClosed);
            }
            self.events.push(envelope.event);
            Ok(())
        }
        async fn flush(&mut self) -> Result<(), SinkError> {
            if self.stall {
                std::future::pending::<()>().await;
            }
            if self.fail {
                return Err(SinkError::StorageClosed);
            }
            self.flushes += 1;
            Ok(())
        }
    }

    /// Drives the private state operations to convergence without waiting for a live
    /// subscription. The bounded loop fails the test if indexing stops making progress.
    async fn catch_up(machine: &mut Machine<Source, Sink>) {
        for _ in 0..100 {
            if !machine.step().await.expect("step") {
                return;
            }
        }
        panic!("catch-up did not converge");
    }

    #[tokio::test]
    async fn startup_indexes_anchor_and_full_tail() {
        let mut machine = Machine::new(Source::linear(5, 2), Sink::default());
        catch_up(&mut machine).await;
        let heights: Vec<_> = machine
            .sink
            .events
            .iter()
            .filter_map(|e| match e {
                Event::Block(b) => Some(b.number),
                _ => None,
            })
            .collect();
        assert_eq!(heights, [2, 3, 4, 5]);
        assert_eq!(machine.undo_depth(), 3);
        assert_eq!(
            machine.emitted_finality(),
            Some(BlockId {
                height: 2,
                hash: hash(2)
            })
        );
    }

    #[tokio::test]
    async fn historical_finality_ahead_of_coverage_is_deferred() {
        let mut machine = Machine::new(Source::linear(5, 3), Sink::default())
            .backfill(0)
            .await
            .expect("historical fill");
        assert_eq!(machine.tip().expect("tip").height, 3);
        assert_eq!(machine.undo_depth(), 0);
        catch_up(&mut machine).await;
        assert_eq!(machine.tip().expect("tip").height, 5);
    }

    #[tokio::test]
    async fn missed_heads_extend_without_reorg() {
        let mut source = Source::linear(6, 1);
        source.data.head = 2;
        let mut machine = Machine::new(source, Sink::default());
        catch_up(&mut machine).await;
        machine.source.data.head = 6;
        catch_up(&mut machine).await;
        assert_eq!(machine.tip().expect("tip").height, 6);
        assert!(
            !machine
                .sink
                .events
                .iter()
                .any(|e| matches!(e, Event::Reorg(_)))
        );
    }

    #[tokio::test]
    async fn full_tail_reorg_resolves_against_anchor() {
        let source = Source::linear(5, 2);
        let mut machine = Machine::new(source, Sink::default());
        catch_up(&mut machine).await;
        machine.source.fork(3, 5);
        machine
            .step()
            .await
            .expect("one operation resolves and replays the fork");
        assert!(matches!(machine.state(), State::Syncing));
        let replacements: Vec<_> = machine
            .sink
            .events
            .iter()
            .filter_map(|event| match event {
                Event::Block(block) if block.hash == hash(block.number + 100) => Some(block.number),
                _ => None,
            })
            .collect();
        assert_eq!(replacements, [3, 4, 5]);
        let markers: Vec<_> = machine
            .sink
            .events
            .iter()
            .filter_map(|e| match e {
                Event::Reorg(r) => Some(r),
                _ => None,
            })
            .collect();
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].height, 3);
        assert_eq!(markers[0].orphaned_hashes, [hash(5), hash(4), hash(3)]);
        assert_eq!(machine.tip().expect("tip").hash, hash(105));
    }

    #[tokio::test]
    async fn duplicate_finality_is_flushed_without_another_block() {
        let source = Source::linear(5, 2);
        let mut machine = Machine::new(source, Sink::default());
        catch_up(&mut machine).await;
        let before = machine.sink.flushes;
        machine.source.data.finalized = 5;
        assert!(!machine.step().await.expect("duplicate step"));
        assert_eq!(machine.sink.flushes, before + 1);
        assert_eq!(machine.undo_depth(), 0);
        assert_eq!(machine.emitted_finality().expect("finality").height, 5);
    }

    #[tokio::test]
    async fn source_failure_retains_backfill_cursor() {
        let source = Source::linear(4, 3);
        let mut machine = Machine::new(source, Sink::default());
        machine.start(Some(0)).await.expect("start");
        machine.step().await.expect("block zero");
        machine.source.data.fail_once.store(true, Ordering::Relaxed);
        assert!(matches!(
            machine.step().await,
            Err(PipelineError::Source(_))
        ));
        assert!(matches!(
            machine.state(),
            State::Backfilling { next: 1, .. }
        ));
        catch_up(&mut machine).await;
        assert_eq!(machine.tip().expect("tip").height, 4);
    }

    #[tokio::test]
    async fn sink_failure_does_not_advance_tip() {
        let mut machine = Machine::new(Source::linear(3, 1), Sink::default());
        machine.step().await.expect("start");
        machine.sink.fail = true;
        assert!(matches!(machine.step().await, Err(PipelineError::Sink(_))));
        assert_eq!(machine.tip(), None);
    }

    #[tokio::test]
    async fn failed_run_drops_its_sink() {
        let (dropped, mut notification) = tokio::sync::oneshot::channel();
        let mut sink = Sink::default();
        sink.fail = true;
        sink.dropped = Some(dropped);
        assert!(matches!(
            Machine::new(Source::linear(3, 1), sink).run().await,
            Err(PipelineError::Sink(SinkError::StorageClosed))
        ));
        assert_eq!(notification.try_recv(), Ok(()));
    }

    #[tokio::test]
    async fn canceled_run_drops_its_sink() {
        let (dropped, mut notification) = tokio::sync::oneshot::channel();
        let mut sink = Sink::default();
        sink.stall = true;
        sink.dropped = Some(dropped);
        {
            let run = Machine::new(Source::linear(3, 1), sink).run();
            tokio::pin!(run);
            assert!(futures_util::poll!(&mut run).is_pending());
            assert_eq!(
                notification.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            );
        }
        assert_eq!(notification.try_recv(), Ok(()));
    }

    #[tokio::test]
    async fn capacity_is_checked_before_startup_output() {
        let mut machine = Machine::new(
            Source::linear(MAX_UNFINALIZED_BLOCKS as u64, 0),
            Sink::default(),
        );
        assert!(matches!(
            machine.step().await,
            Err(PipelineError::UndoCapacity { .. })
        ));
        assert!(machine.sink.events.is_empty());
    }

    #[tokio::test]
    async fn new_finality_can_certify_a_branch_not_yet_replayed() {
        let source = Source::linear(5, 2);
        let mut machine = Machine::new(source, Sink::default());
        catch_up(&mut machine).await;
        machine.source.fork(3, 5);
        machine.source.data.finalized = 4;
        catch_up(&mut machine).await;
        assert_eq!(
            machine.emitted_finality(),
            Some(BlockId {
                height: 4,
                hash: hash(104)
            })
        );
        assert_eq!(machine.undo_depth(), 1);
    }

    #[tokio::test]
    async fn a_branch_cannot_replace_the_emitted_anchor() {
        let source = Source::linear(5, 2);
        let mut machine = Machine::new(source, Sink::default());
        catch_up(&mut machine).await;
        machine.source.fork(2, 5);
        assert!(matches!(
            machine.step().await,
            Err(PipelineError::FinalityViolation { .. })
        ));
        assert!(
            !machine
                .sink
                .events
                .iter()
                .any(|event| matches!(event, Event::Reorg(_)))
        );
    }

    #[tokio::test]
    async fn reorg_publication_failure_preserves_old_history() {
        let source = Source::linear(5, 2);
        let mut machine = Machine::new(source, Sink::default());
        catch_up(&mut machine).await;
        machine.source.fork(3, 5);
        machine.sink.fail = true;
        assert!(matches!(machine.step().await, Err(PipelineError::Sink(_))));
        assert_eq!(
            machine.tip(),
            Some(BlockId {
                height: 5,
                hash: hash(5)
            })
        );
        assert_eq!(machine.undo_depth(), 3);
    }

    #[tokio::test]
    async fn source_failure_during_discovery_leaves_old_history_unchanged() {
        let source = Source::linear(5, 2);
        let mut machine = Machine::new(source, Sink::default());
        catch_up(&mut machine).await;
        machine.source.fork(3, 5);
        let head = machine.source.fetch_block(5).await.expect("candidate");
        machine.source.data.fail_once.store(true, Ordering::Relaxed);
        assert!(matches!(
            machine.handle_reorg(head, 5).await,
            Err(PipelineError::Source(_))
        ));
        assert!(matches!(machine.state(), State::Reorg));
        assert_eq!(
            machine.tip(),
            Some(BlockId {
                height: 5,
                hash: hash(5)
            })
        );
        assert_eq!(machine.undo_depth(), 3);
        catch_up(&mut machine).await;
        assert_eq!(
            machine
                .sink
                .events
                .iter()
                .filter(|event| matches!(event, Event::Reorg(_)))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn live_driver_returns_source_errors_without_retrying() {
        let source = Source::linear(3, 1);
        source.data.fail_once.store(true, Ordering::Relaxed);
        assert!(matches!(
            Machine::new(source, Sink::default()).run().await,
            Err(PipelineError::Source(SourceError::Closed { .. }))
        ));
    }

    #[tokio::test]
    async fn ahead_of_coverage_finality_is_not_retained() {
        let mut machine = Machine::new(Source::linear(5, 2), Sink::default());
        catch_up(&mut machine).await;
        let anchor = machine.emitted_finality();
        machine
            .emit_finality(BlockId {
                height: 6,
                hash: hash(6),
            })
            .await
            .expect("defer");
        assert_eq!(machine.emitted_finality(), anchor);
        // Only regression below applied finality is invalid; an unapplied observation
        // does not become a second persistent watermark.
        machine
            .emit_finality(BlockId {
                height: 4,
                hash: hash(4),
            })
            .await
            .expect("fresh observation");
        assert_eq!(
            machine.emitted_finality(),
            Some(BlockId {
                height: 4,
                hash: hash(4)
            })
        );
        assert_eq!(
            machine.tip(),
            Some(BlockId {
                height: 5,
                hash: hash(5)
            })
        );
    }

    #[test]
    fn empty_unfinalized_tail_still_has_a_tip_and_anchor() {
        let mut ring = UndoRing::default();
        ring.entries.extend((2..=4).map(|height| BlockId {
            height,
            hash: hash(height),
        }));
        let finalized = BlockId {
            height: 4,
            hash: hash(4),
        };
        ring.finalize(finalized);
        assert!(ring.entries.is_empty());
        assert_eq!(ring.tip(), Some(finalized));
        assert_eq!(ring.at(4), Some(finalized));
        assert_eq!(ring.at(3), None);
    }

    #[test]
    fn finality_regression_and_same_height_conflict_are_typed_errors() {
        let mut ring = UndoRing::default();
        ring.finalize(BlockId {
            height: 3,
            hash: hash(3),
        });
        assert!(matches!(
            ring.check_finality(BlockId {
                height: 2,
                hash: hash(2)
            }),
            Err(PipelineError::FinalityRegressed { .. })
        ));
        assert!(matches!(
            ring.check_finality(BlockId {
                height: 3,
                hash: hash(103)
            }),
            Err(PipelineError::FinalityViolation { .. })
        ));
    }

    #[test]
    fn wrong_height_and_noncontiguous_parent_are_rejected() {
        let fetched = FetchedBlock {
            events: vec![Event::Block(Box::new(Block {
                number: 4,
                hash: hash(4),
                parent_hash: hash(1),
                ..Block::default()
            }))],
            finalized: BlockId {
                height: 0,
                hash: hash(0),
            },
        };
        assert!(matches!(
            Machine::<Source, Sink>::marker(&fetched, 3),
            Err(PipelineError::IdentityMismatch { .. })
        ));
        assert!(matches!(
            Machine::<Source, Sink>::validate_link(
                BlockId {
                    height: 3,
                    hash: hash(3)
                },
                BlockId {
                    height: 4,
                    hash: hash(4)
                },
                hash(1),
            ),
            Err(PipelineError::BrokenLink { .. })
        ));
    }
}
