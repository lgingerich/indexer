//! Reorg-aware, block-at-a-time ingestion that resumes from what the store committed.
//!
//! Startup samples the head once and splits the work at the reorg window. Heights at or
//! below `head - window` are buried: they sit a full window behind the sampled head, so
//! no fork reaches them and backfill indexes them straight through without asking the
//! source where the chain is. On a chain shorter than the window the bound clamps to
//! genesis, which is equally unreorgable. Everything above that boundary is the
//! reorgable tail, and startup owns it too — from the buried bound it fills to the
//! sampled head through the reorg-aware path before any live head is consumed, so a run
//! never leaves a gap under its start. A start already inside the window makes that fill
//! the whole run. The live phase then follows `newHeads`, reuses the announced metadata,
//! and reconciles gaps and forks over HTTP. Every accepted block's identity is retained
//! in a sliding window so a fork within it is retracted; deeper forks stop rather than
//! guess.
//!
//! # Resume
//!
//! Every block's batch ends with an [`AcceptedBlock`] marker, so a store commits a
//! block's identity in the same transaction as its rows — empty blocks included, and
//! whatever datasets are selected. Sink flushing is acceptance, not durability, so a
//! restart does not trust a previous process's memory: it reads the newest window of
//! markers back from the store and hands it to [`Machine::new`], which keeps the newest
//! parent-linked suffix as its undo window.
//!
//! Startup then re-reads the restored tip's height. A match continues from the next
//! height through the same buried/tail split a fresh start uses. A mismatch means the
//! stored suffix was orphaned while the process was down: the ordinary fork walk finds
//! the common ancestor inside the restored window, retracts the stored suffix with one
//! [`Reorg`], and replays the replacements. Either way the first new block must link to
//! what the store committed, so coverage from the restored tip forward is contiguous. A
//! fork deeper than the restored window stops with
//! [`PipelineError::UndoWindowExceeded`]: recovering from it is an operator's decision.

use crate::ingest::source::{BlockMeta, BlockSource, FetchedBlock, SourceError};
use crate::sink::{EnvelopeSink, SinkError};
use crate::wire::envelope::{AcceptedBlock, Envelope, Event, Reorg};
use alloy_primitives::B256;
use futures_util::StreamExt as _;
use std::collections::VecDeque;
use thiserror::Error;
use tracing::{info, warn};

/// Hardcoded budget for the recovered undo window.
///
/// The window is a recovery floor, not a finality claim: a fork deeper than this
/// stops with [`PipelineError::UndoWindowExceeded`] rather than retracting a suffix
/// the pipeline can no longer identify. Identities cost nothing beside the payloads
/// they describe, so the value is a bounded-memory choice, not a tuning knob.
pub const MAX_UNFINALIZED_BLOCKS: usize = 4096;

/// Why ingestion cannot continue safely.
///
/// The identity-bearing variants box their [`BlockMeta`] fields: an unboxed pair is 160
/// bytes, which would make every `Result` in the pipeline carry that on the error path.
/// The allocations happen only when a run is already stopping.
#[derive(Debug, Error)]
pub enum PipelineError {
    /// Returned block identity disagrees with the request.
    #[error("requested block {expected:?}, received {actual:?}")]
    IdentityMismatch {
        /// Expected identity.
        expected: Box<BlockMeta>,
        /// Returned identity.
        actual: Box<BlockMeta>,
    },
    /// Heights or parent hashes do not form a contiguous chain.
    #[error("block {actual:?} does not extend {previous:?}")]
    BrokenLink {
        /// Expected predecessor.
        previous: Box<BlockMeta>,
        /// Returned successor.
        actual: Box<BlockMeta>,
    },
    /// A fork reaches below the oldest identity the window still remembers.
    #[error("fork at {actual:?} is below the recovered floor {floor:?}")]
    UndoWindowExceeded {
        /// Oldest retained identity.
        floor: Box<BlockMeta>,
        /// The unretained replacement.
        actual: Box<BlockMeta>,
    },
    /// A requested historical start is above the sampled head.
    #[error("historical start {start} is above the sampled head {head}")]
    StartAboveHead {
        /// Requested start height.
        start: u64,
        /// Sampled head height.
        head: u64,
    },
    /// A start height was requested, but the store already holds accepted history.
    ///
    /// Resuming from the store and starting elsewhere would leave a gap or orphaned rows,
    /// so the run refuses to guess which was meant.
    #[error(
        "a start at height {start} was requested, but the store already holds blocks through \
         {tip}; remove ingest.start_block to resume from the store"
    )]
    StartWithHistory {
        /// Requested start height.
        start: u64,
        /// The restored tip's height.
        tip: u64,
    },
    /// A range fetch returned no blocks, or blocks past the end of the range.
    #[error("source returned {returned} blocks for heights {from}..={to}")]
    InvalidRange {
        /// First requested height.
        from: u64,
        /// Last height the fetch could return.
        to: u64,
        /// How many blocks it returned.
        returned: usize,
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
    /// Sample the head once and choose the first height.
    Starting,
    /// Index consecutive buried heights, none of which can reorg, then converge on the
    /// sampled head through the reorg-aware path.
    Backfilling {
        /// Next historical height.
        next: u64,
        /// Last height backfill indexes directly; above it the live path takes over.
        end: u64,
        /// The head sampled at startup, which startup converges to before going live.
        head: BlockMeta,
    },
    /// Reconcile the live tail, including ordinary catch-up.
    Syncing,
}

/// Accepted chain identities: a recovery predecessor and an ordered sliding tail.
#[derive(Debug, Default)]
struct UndoRing {
    /// Oldest recoverable identity, kept only as a linkage floor. The last entry
    /// evicted from the tail lands here.
    floor: Option<BlockMeta>,
    /// Published tail, oldest first, at most [`MAX_UNFINALIZED_BLOCKS`] identities.
    entries: VecDeque<BlockMeta>,
}

impl UndoRing {
    /// Returns the newest accepted block, or `None` before any block is accepted.
    fn tip(&self) -> Option<BlockMeta> {
        self.entries.back().copied()
    }

    /// The oldest identity the window still remembers: the floor, or the oldest tail
    /// entry when nothing has been evicted yet. `None` before any block is accepted.
    fn oldest(&self) -> Option<BlockMeta> {
        self.floor.or_else(|| self.entries.front().copied())
    }

    /// Looks up a remembered identity: the tail, or the floor boundary.
    /// Absence means this height is no longer retained, not that it is noncanonical.
    ///
    /// Scans newest first: lookups are for live heads and fork walks, both at or near
    /// the tip, so they stop after a few entries rather than crossing the window.
    fn at(&self, height: u64) -> Option<BlockMeta> {
        if let Some(floor) = self.floor
            && floor.height == height
        {
            return Some(floor);
        }
        self.entries
            .iter()
            .rev()
            .find(|meta| meta.height == height)
            .copied()
    }

    /// Whether `height` is still inside the retained window, so a backward walk may
    /// still find an ancestor there.
    fn retains(&self, height: u64) -> bool {
        self.oldest().is_some_and(|oldest| height >= oldest.height)
    }

    /// Collects hashes above a previously verified ancestor, newest first.
    /// Does not remove them: the sink must accept their retraction before rewind.
    ///
    /// ponytail: `take_while` stops at the first entry at or below the ancestor, so it
    /// assumes `entries` is height-ordered ascending. That holds because `push` appends
    /// and the driver only ever accepts the next height, but nothing here enforces it —
    /// unordered input yields a partial orphan list. Upgrade path: a `debug_assert!` in
    /// `push`, or a sort, if a future caller ever pushes out of order.
    fn orphaned(&self, ancestor: BlockMeta) -> Vec<B256> {
        self.entries
            .iter()
            .rev()
            .take_while(|meta| meta.height > ancestor.height)
            .map(|meta| meta.hash)
            .collect()
    }

    /// Removes the suffix above a verified ancestor after retraction is accepted.
    /// Order-independent, unlike [`Self::orphaned`]: every entry above the height goes.
    fn rewind(&mut self, ancestor: BlockMeta) {
        self.entries.retain(|meta| meta.height <= ancestor.height);
    }

    /// Appends an accepted identity, sliding the window: the oldest tail entry
    /// becomes the floor when the window is full.
    fn push(&mut self, meta: BlockMeta) {
        if self.entries.len() >= MAX_UNFINALIZED_BLOCKS
            && let Some(evicted) = self.entries.pop_front()
        {
            self.floor = Some(evicted);
        }
        self.entries.push_back(meta);
    }
}

/// The newest parent-linked run of `ledger`, both oldest first.
///
/// A stored ledger is contiguous unless something outside the pipeline broke it — an
/// older run that did not resume, or a hand-edited store. Linking across that gap would
/// let a fork walk "find" an ancestor that never preceded the tip, so the window starts
/// above it instead, and the operator is told.
fn linked_suffix(mut ledger: Vec<BlockMeta>) -> Vec<BlockMeta> {
    let Some(mut newer) = ledger.last().copied() else {
        return ledger;
    };
    let mut start = ledger.len() - 1;
    for (index, meta) in ledger.iter().enumerate().rev().skip(1) {
        if meta.height.checked_add(1) != Some(newer.height) || meta.hash != newer.parent_hash {
            break;
        }
        newer = *meta;
        start = index;
    }
    if start > 0 {
        warn!(
            dropped = start,
            kept = ledger.len() - start,
            from = newer.height,
            "the stored ledger is not contiguous; resuming with its newest linked blocks"
        );
    }
    ledger.drain(..start);
    ledger
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
    /// Builds a driver whose undo window starts as `ledger`: the accepted identities a
    /// store read back, oldest first.
    ///
    /// An empty ledger starts fresh, at the sampled head or a requested height. Otherwise
    /// the run resumes after the ledger's tip; see the module docs. Only the newest
    /// parent-linked suffix is kept, and only the newest [`MAX_UNFINALIZED_BLOCKS`] of it
    /// plus one floor are remembered.
    #[must_use]
    pub fn new(source: S, sink: K, ledger: Vec<BlockMeta>) -> Self {
        let mut ring = UndoRing::default();
        for meta in linked_suffix(ledger) {
            ring.push(meta);
        }
        Self {
            source,
            sink,
            state: State::Starting,
            ring,
        }
    }

    /// Current scheduling state.
    #[must_use]
    pub fn state(&self) -> &State {
        &self.state
    }

    /// Highest block accepted by the sink.
    #[must_use]
    pub fn tip(&self) -> Option<BlockMeta> {
        self.ring.tip()
    }

    /// Finds a sink-accepted identity used for duplicate and ancestor checks.
    /// Only the floor and retained tail can be matched; older history is absent.
    fn accepted(&self, height: u64) -> Option<BlockMeta> {
        self.ring.at(height)
    }

    /// Reads a fetched block's identity and parent from its metadata.
    /// Rejects a number different from the requested height.
    /// Does not establish canonicality or validate linkage to another block.
    fn marker(block: &FetchedBlock, height: u64) -> Result<BlockMeta, PipelineError> {
        let meta = block.meta;
        if meta.height != height {
            return Err(PipelineError::IdentityMismatch {
                expected: Box::new(BlockMeta { height, ..meta }),
                actual: Box::new(meta),
            });
        }
        Ok(meta)
    }

    /// Requires a successor at exactly the next height with the previous hash as parent.
    /// Used both during backward discovery and before ascending publication.
    fn validate_link(previous: BlockMeta, next: BlockMeta) -> Result<(), PipelineError> {
        if previous.height.checked_add(1) != Some(next.height) || previous.hash != next.parent_hash
        {
            return Err(PipelineError::BrokenLink {
                previous: Box::new(previous),
                actual: Box::new(next),
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
}

impl<S: BlockSource, K: EnvelopeSink> Machine<S, K> {
    /// Drives startup, backfill, and live indexing.
    ///
    /// Backfill indexes only buried heights, which cannot reorg; once it reaches the
    /// reorgable window the live phase owns the tail. Notifications are hints, not a
    /// replay log: HTTP reconciliation fills gaps and resolves reorgs. Source errors
    /// still propagate without retries; the source owns connection maintenance.
    ///
    /// # Errors
    /// Consumes this instance. An error or cancellation drops its source, sink, and
    /// history, so partially delivered output cannot be followed by reuse.
    /// Returns permanent source, invariant, or sink failures.
    pub async fn run(mut self) -> Result<(), PipelineError> {
        // Backfill is headless: it captures its target once in `start` and reads
        // concrete heights only, so the subscription is opened after it converges.
        // A notification consumed during backfill would only be a hint anyway. A run
        // already backfilled through `backfill` is live, so it does not re-sample.
        if matches!(self.state, State::Starting) {
            self.start(None).await?;
        }
        while self.step().await? {}
        let mut heads = self.source.subscribe_heads().await?;
        loop {
            tokio::select! {
                head = heads.next() => match head {
                    Some(head) => { self.process_head(head?).await?; }
                    None => return Err(PipelineError::SubscriptionClosed),
                },
                () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    self.reconcile().await?;
                }
            }
        }
    }

    /// Consumes this instance to index history from `from` through the sampled head.
    ///
    /// Returns ownership only on success, allowing the result to be passed to `run`.
    /// Failure or cancellation drops the instance and any buffered partial output.
    /// Makes no head-discovery call: the head is sampled once, and a fork reconciles
    /// against the same capture while the live phase processes it.
    ///
    /// # Errors
    /// Returns an error if called after startup, if `from` is above the sampled head,
    /// or if delivery fails, and [`PipelineError::StartWithHistory`] when the machine was
    /// built with a stored ledger, which `from` would contradict.
    pub async fn backfill(mut self, from: u64) -> Result<Self, PipelineError> {
        if !matches!(self.state, State::Starting) {
            return Err(PipelineError::UnstableSource);
        }
        self.start(Some(from)).await?;
        while self.step().await? {}
        Ok(self)
    }

    /// Performs the next buried-backfill operation, returning whether it made progress.
    /// `false` once the live phase should wait for a notification or timer.
    async fn step(&mut self) -> Result<bool, PipelineError> {
        match self.state {
            State::Backfilling { next, end, head } => {
                self.backfill_step(next, end, head).await?;
                Ok(true)
            }
            State::Starting | State::Syncing => Ok(false),
        }
    }

    /// Samples the head once and chooses what backfill should cover.
    ///
    /// Backfill indexes `[start, head - window]`: the buried heights a reorg cannot
    /// reach. Everything above that boundary is the reorgable tail, and startup owns it
    /// too — it adopts the first block and fills to the sampled head through the
    /// reorg-aware path before going live, so a run never leaves a gap under its start.
    /// When the start is already above the boundary that fill is the whole run. `from`
    /// names the first height; `None` starts at the sampled head, or after the restored
    /// tip when the machine was built with a ledger. A start above the sampled head is a
    /// mistake, not something to clamp.
    ///
    /// # Errors
    /// Returns [`PipelineError::StartAboveHead`] for a start above the sampled head, and
    /// [`PipelineError::StartWithHistory`] for any start when a ledger was restored.
    async fn start(&mut self, from: Option<u64>) -> Result<(), PipelineError> {
        let restored = self.ring.tip();
        if let (Some(start), Some(tip)) = (from, restored) {
            return Err(PipelineError::StartWithHistory {
                start,
                tip: tip.height,
            });
        }
        let head = self.source.fetch_header(None).await?;
        self.sink.observe_head(head.height);
        if let Some(tip) = restored {
            return self.resume(tip, head).await;
        }
        let next = from.unwrap_or(head.height);
        if next > head.height {
            return Err(PipelineError::StartAboveHead {
                start: next,
                head: head.height,
            });
        }
        // Saturating: when the head is below the window there is nothing buried, and the
        // `next > end` check below takes the live path instead.
        let end = head.height.saturating_sub(MAX_UNFINALIZED_BLOCKS as u64);
        if next > end {
            let fetched = self.source.fetch_block(next, None).await?;
            self.accept(next, fetched).await?;
            self.settle_tail(head).await?;
        } else {
            self.state = State::Backfilling { next, end, head };
        }
        Ok(())
    }

    /// Continues after a restored tip, first reconciling it against the source.
    ///
    /// The tip's header is re-read — the header alone, since on the usual path only its
    /// hash is compared. A different hash there means the stored suffix was orphaned
    /// while no process was running: only then is the full block fetched, and the
    /// ordinary fork walk retracts the suffix before anything new is published. From the reconciled tip, the remaining heights
    /// take the same buried/tail split as a fresh start, and every one of them links to
    /// the tip before it is accepted. A source whose head is below the stored tip is
    /// behind the store, not ahead of it, so live reconciliation takes over directly.
    async fn resume(&mut self, tip: BlockMeta, head: BlockMeta) -> Result<(), PipelineError> {
        info!(
            height = tip.height,
            hash = %tip.hash,
            head = head.height,
            "resuming from the stored tip"
        );
        if head.height < tip.height {
            self.state = State::Syncing;
            return self.process_head(head).await;
        }
        if self.source.fetch_header(Some(tip.height)).await?.hash != tip.hash {
            let current = self.source.fetch_block(tip.height, None).await?;
            self.handle_reorg(current, tip.height).await?;
        }
        let tip = self.ring.tip().ok_or(PipelineError::UnstableSource)?;
        let next = Self::next(tip.height)?;
        let end = head.height.saturating_sub(MAX_UNFINALIZED_BLOCKS as u64);
        if next > end {
            self.settle_tail(head).await?;
        } else {
            self.state = State::Backfilling { next, end, head };
        }
        Ok(())
    }

    /// Delivers a block fetched for `height` and records its identity.
    ///
    /// The link to the accepted tip is checked first and the identity is recorded after
    /// delivery succeeds, so a failure leaves the accepted history untouched. The first
    /// block of a fresh run has no predecessor to link onto and is accepted as-is; a
    /// resumed run links its first block to the restored tip. A link
    /// failure here is a source fault: this is the buried path, where no fork is
    /// reachable.
    async fn accept(&mut self, height: u64, fetched: FetchedBlock) -> Result<(), PipelineError> {
        let meta = Self::marker(&fetched, height)?;
        if let Some(tip) = self.ring.tip() {
            Self::validate_link(tip, meta)?;
        }
        self.commit(meta, fetched.events).await
    }

    /// Delivers a block's events, then records its identity.
    ///
    /// The batch ends with the block's [`AcceptedBlock`] marker, so a store commits the
    /// identity a restart resumes from in the same transaction as the block's rows, and
    /// an empty block still reaches the store. The order is the whole point: a delivery
    /// that fails leaves the identity unrecorded, so the accepted history never claims a
    /// block the sink did not take.
    async fn commit(
        &mut self,
        meta: BlockMeta,
        mut events: Vec<Event>,
    ) -> Result<(), PipelineError> {
        events.push(Event::AcceptedBlock(AcceptedBlock::from(meta)));
        self.deliver(events).await?;
        self.ring.push(meta);
        Ok(())
    }

    /// Fills from the accepted tip to the sampled head, then returns to `Syncing`.
    ///
    /// The tail is reorgable, so this is [`Self::catch_up`] rather than [`Self::accept`]:
    /// a link failure here is a fork to reconcile.
    async fn settle_tail(&mut self, head: BlockMeta) -> Result<(), PipelineError> {
        let tip = self.ring.tip().ok_or(PipelineError::UnstableSource)?;
        self.catch_up(tip, head).await?;
        self.state = State::Syncing;
        Ok(())
    }

    /// Fetches and publishes the buried heights the source returns from `next`, as many
    /// as it chooses, in order. No reorg handling: no fork this deep is reachable, and
    /// invalid linkage is a source fault rather than a fork to replay. The cursor moves
    /// past each block as it is accepted, so a failure partway through leaves it at the
    /// first block not taken. At the buried bound, the remaining reorgable tail is filled
    /// through [`Self::settle_tail`] before the run goes live.
    ///
    /// # Errors
    /// Returns [`PipelineError::InvalidRange`] when the source returns no blocks or blocks
    /// past `end`, before any of them is published.
    async fn backfill_step(
        &mut self,
        next: u64,
        end: u64,
        head: BlockMeta,
    ) -> Result<(), PipelineError> {
        let blocks = self.source.fetch_blocks(next, end).await?;
        let last = u64::try_from(blocks.len())
            .ok()
            .and_then(|count| count.checked_sub(1))
            .and_then(|extra| next.checked_add(extra))
            .filter(|last| *last <= end)
            .ok_or(PipelineError::InvalidRange {
                from: next,
                to: end,
                returned: blocks.len(),
            })?;
        for (height, fetched) in (next..=last).zip(blocks) {
            self.accept(height, fetched).await?;
            if height < end {
                self.state = State::Backfilling {
                    next: Self::next(height)?,
                    end,
                    head,
                };
            }
        }
        if last >= end {
            self.settle_tail(head).await?;
        }
        Ok(())
    }

    /// Reconciles the live tail: duplicate, gap, or fork against the sampled head.
    ///
    /// # Errors
    /// Returns [`PipelineError::UnstableSource`] when a re-read candidate disagrees
    /// with the sampled head.
    async fn reconcile(&mut self) -> Result<(), PipelineError> {
        let head = self.source.fetch_header(None).await?;
        self.sink.observe_head(head.height);
        self.process_head(head).await
    }

    /// Resolves one observed head against accepted coverage.
    ///
    /// A duplicate accepted head causes no dataset fetch. Ahead of coverage, the gap
    /// is drained by fetching consecutive heights up to the target, reusing the
    /// notification metadata for the target height only. Divergence invokes the
    /// complete reorg operation.
    async fn process_head(&mut self, head: BlockMeta) -> Result<(), PipelineError> {
        let tip = self.ring.tip().ok_or(PipelineError::UnstableSource)?;
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
        // The head is at or below the tip: a same-height or deeper fork.
        let candidate = self.source.fetch_block(head.height, Some(&head)).await?;
        if Self::marker(&candidate, head.height)? != head {
            return Err(PipelineError::UnstableSource);
        }
        self.handle_reorg(candidate, head.height).await
    }

    /// Fetches consecutive heights from `tip` to the observed `target`.
    ///
    /// Only a fetch at the target height may reuse the notification metadata, because
    /// that is the only height it describes. A link failure at the next height means
    /// the branch diverged, so the target is re-read and reconciled.
    async fn catch_up(
        &mut self,
        mut tip: BlockMeta,
        target: BlockMeta,
    ) -> Result<(), PipelineError> {
        while tip.height < target.height {
            let next = Self::next(tip.height)?;
            let fetch_head = (next == target.height).then_some(&target);
            let block = self.source.fetch_block(next, fetch_head).await?;
            let meta = Self::marker(&block, next)?;
            if Self::validate_link(tip, meta).is_ok() {
                self.commit(meta, block.events).await?;
                tip = meta;
                continue;
            }
            // Re-read the announced target by height; it must still match before any
            // retraction is emitted.
            let recheck = self.source.fetch_block(target.height, None).await?;
            if Self::marker(&recheck, target.height)? != target {
                return Err(PipelineError::UnstableSource);
            }
            return self.handle_reorg(recheck, target.height).await;
        }
        Ok(())
    }

    /// Resolves a candidate branch in one bounded walk-and-replay operation.
    ///
    /// Validates backward edges, finds a retained ancestor, and rechecks the target
    /// before output. Retracts only a nonempty old suffix, then publishes replacements
    /// ascending and returns to `Syncing`. Discovery failures leave history untouched;
    /// delivery can be partial on error, so public callers consume the instance.
    async fn handle_reorg(&mut self, head: FetchedBlock, height: u64) -> Result<(), PipelineError> {
        let target = Self::marker(&head, height)?;
        // Newest first while walking; replay consumes this vector in reverse.
        let mut branch = vec![(height, head)];
        let ancestor = loop {
            let (at, first) = branch.last().ok_or(PipelineError::UnstableSource)?;
            let meta = Self::marker(first, *at)?;
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
                break parent;
            }
            // The common ancestor is below the retained window: stop before any partial
            // retraction rather than guess.
            if !self.ring.retains(parent_height) {
                return Err(PipelineError::UndoWindowExceeded {
                    floor: Box::new(self.ring.oldest().unwrap_or(meta)),
                    actual: Box::new(meta),
                });
            }
            info!(
                depth = branch.len(),
                walked_to = parent_height,
                "reconciling a fork, walking back toward an accepted ancestor"
            );
            let below = self.source.fetch_block(parent_height, None).await?;
            let below_meta = Self::marker(&below, parent_height)?;
            if below_meta.hash != meta.parent_hash {
                return Err(PipelineError::UnstableSource);
            }
            Self::validate_link(below_meta, meta)?;
            branch.push((parent_height, below));
        };

        // The source may change branches during discovery. Recheck the candidate at its
        // fixed height before publishing any retraction.
        let current = self.source.fetch_block(target.height, None).await?;
        if Self::marker(&current, target.height)? != target {
            return Err(PipelineError::UnstableSource);
        }

        self.sink.observe_head(target.height);
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
            let meta = Self::marker(&block, height)?;
            self.commit(meta, block.events).await?;
        }
        self.state = State::Syncing;
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use futures_util::stream;

    use super::*;
    use crate::ingest::source::HeadStream;
    use crate::wire::envelope::{Block, ChainId};

    /// Produces deterministic distinct identities for test heights and replacement blocks.
    fn hash(height: u64) -> B256 {
        B256::from(alloy_primitives::U256::from(height).to_be_bytes())
    }

    /// A deterministic chain source. `head` moves when `fork` or a test edits it,
    /// `head_calls` counts discovery so a test can prove backfill makes none, and
    /// `block_calls` counts full block fetches.
    struct Source {
        chain: ChainId,
        data: Data,
    }

    struct Data {
        blocks: HashMap<u64, (B256, B256)>,
        head: u64,
        fail_once: AtomicBool,
        head_calls: AtomicUsize,
        block_calls: AtomicUsize,
        /// How many heights one `fetch_blocks` returns; zero answers with none.
        range: u64,
    }

    impl Source {
        /// Builds a canonical chain from genesis through `head`.
        fn linear(head: u64) -> Self {
            Self {
                chain: ChainId::new("ethereum"),
                data: Data {
                    blocks: (0..=head)
                        .map(|h| (h, (hash(h), hash(h.saturating_sub(1)))))
                        .collect(),
                    head,
                    fail_once: AtomicBool::new(false),
                    head_calls: AtomicUsize::new(0),
                    block_calls: AtomicUsize::new(0),
                    range: 1,
                },
            }
        }

        /// The metadata a source would announce for its current head.
        fn head_meta(&self) -> BlockMeta {
            let height = self.data.head;
            let (hash, parent) = self.data.blocks[&height];
            BlockMeta {
                height,
                hash,
                parent_hash: parent,
                timestamp: height,
            }
        }
    }

    impl BlockSource for Source {
        fn chain(&self) -> &ChainId {
            &self.chain
        }
        async fn subscribe_heads(&self) -> Result<HeadStream, SourceError> {
            Ok(Box::pin(stream::pending()))
        }
        async fn fetch_header(&self, height: Option<u64>) -> Result<BlockMeta, SourceError> {
            let Some(height) = height else {
                self.data.head_calls.fetch_add(1, Ordering::Relaxed);
                return Ok(self.head_meta());
            };
            let Some(&(hash, parent)) = self.data.blocks.get(&height) else {
                return Err(SourceError::Malformed {
                    context: "test".into(),
                    detail: "missing block".into(),
                });
            };
            Ok(BlockMeta {
                height,
                hash,
                parent_hash: parent,
                timestamp: height,
            })
        }
        async fn fetch_blocks(&self, from: u64, to: u64) -> Result<Vec<FetchedBlock>, SourceError> {
            let mut blocks = Vec::new();
            if self.data.range == 0 {
                return Ok(blocks);
            }
            for height in from..=to.min(from + self.data.range - 1) {
                blocks.push(self.fetch_block(height, None).await?);
            }
            Ok(blocks)
        }
        async fn fetch_block(
            &self,
            height: u64,
            _head: Option<&BlockMeta>,
        ) -> Result<FetchedBlock, SourceError> {
            let data = &self.data;
            data.block_calls.fetch_add(1, Ordering::Relaxed);
            if data.fail_once.swap(false, Ordering::Relaxed) {
                return Err(SourceError::Malformed {
                    context: "test".into(),
                    detail: "injected source failure".into(),
                });
            }
            let Some(&(hash, parent)) = data.blocks.get(&height) else {
                return Err(SourceError::Malformed {
                    context: "test".into(),
                    detail: "missing block".into(),
                });
            };
            Ok(FetchedBlock {
                meta: BlockMeta {
                    height,
                    hash,
                    parent_hash: parent,
                    timestamp: height,
                },
                events: vec![Event::Block(Box::new(Block {
                    number: height,
                    hash,
                    parent_hash: parent,
                    ..Block::default()
                }))],
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
        for _ in 0..(MAX_UNFINALIZED_BLOCKS * 2 + 16) {
            if !machine.step().await.expect("step") {
                return;
            }
        }
        panic!("catch-up did not converge");
    }

    /// Starts a machine at `from` and drives it to a live, reconciled state. A run whose
    /// start is already inside the window seeds and fills to the sampled head
    /// synchronously, so nothing is left pending afterward.
    async fn settle(machine: &mut Machine<Source, Sink>, from: Option<u64>) {
        machine.start(from).await.expect("start");
        catch_up(machine).await;
        while !matches!(machine.state(), State::Syncing) {
            let head = machine.source.head_meta();
            machine.process_head(head).await.expect("observe");
            catch_up(machine).await;
        }
    }

    /// Moves the fake chain to `height` and feeds that head to the live phase, as a
    /// notification or heartbeat would.
    async fn observe(machine: &mut Machine<Source, Sink>, height: u64) {
        machine.source.data.head = height;
        let head = machine.source.head_meta();
        machine.process_head(head).await.expect("observe");
    }

    /// Collects the heights of block events the sink accepted, in order.
    fn heights(machine: &Machine<Source, Sink>) -> Vec<u64> {
        machine
            .sink
            .events
            .iter()
            .filter_map(|event| match event {
                Event::Block(block) => Some(block.number),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn a_near_head_start_seeds_then_extends_one_height_at_a_time() {
        let mut machine = Machine::new(Source::linear(5), Sink::default(), Vec::new());
        machine.start(Some(2)).await.expect("start");
        // The head is inside the window, but startup still fills through the sampled
        // head, so the run is already live and reconciled above the start.
        assert!(matches!(machine.state(), State::Syncing));
        assert_eq!(heights(&machine), [2, 3, 4, 5]);
        assert_eq!(machine.tip().expect("tip").height, 5);
    }

    #[tokio::test]
    async fn a_start_above_the_head_is_rejected_before_output() {
        let mut machine = Machine::new(Source::linear(5), Sink::default(), Vec::new());
        assert!(matches!(
            machine.start(Some(6)).await,
            Err(PipelineError::StartAboveHead { start: 6, head: 5 })
        ));
        assert!(machine.sink.events.is_empty());
    }

    #[tokio::test]
    async fn backfill_drives_through_the_start_and_returns_the_machine() {
        let machine = Machine::new(Source::linear(5), Sink::default(), Vec::new());
        let machine = machine.backfill(2).await.expect("backfill");
        assert!(matches!(machine.state(), State::Syncing));
        assert_eq!(heights(&machine), [2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn backfill_refuses_a_second_start() {
        let machine = Machine::new(Source::linear(5), Sink::default(), Vec::new());
        let machine = machine.backfill(2).await.expect("backfill");
        assert!(matches!(
            machine.backfill(2).await,
            Err(PipelineError::UnstableSource)
        ));
    }

    #[tokio::test]
    async fn backfill_covers_only_buried_heights_and_makes_one_discovery_call() {
        // A head beyond the window leaves buried history for backfill to index.
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 5;
        let mut machine = Machine::new(Source::linear(head), Sink::default(), Vec::new());
        machine.start(Some(0)).await.expect("start");
        assert!(matches!(machine.state(), State::Backfilling { .. }));
        // The chain advances while backfilling; the captured head must not move, so
        // startup converges on the originally sampled head rather than the moved one.
        machine.source.data.head = head + 10;
        catch_up(&mut machine).await;
        assert!(matches!(machine.state(), State::Syncing));
        assert_eq!(machine.tip().expect("tip").height, head);
        assert_eq!(
            machine.source.data.head_calls.load(Ordering::Relaxed),
            1,
            "backfill must discover exactly once, at startup"
        );
    }

    #[tokio::test]
    async fn gap_catch_up_reads_each_missing_height_once() {
        let mut machine = Machine::new(Source::linear(3), Sink::default(), Vec::new());
        settle(&mut machine, Some(3)).await;
        machine.source.data.head = 6;
        machine
            .source
            .data
            .blocks
            .extend((4..=6).map(|h| (h, (hash(h), hash(h.saturating_sub(1))))));
        machine
            .process_head(machine.source.head_meta())
            .await
            .expect("gap");
        assert_eq!(machine.tip().expect("tip").height, 6);
        assert_eq!(heights(&machine), [3, 4, 5, 6]);
        assert!(
            !machine
                .sink
                .events
                .iter()
                .any(|event| matches!(event, Event::Reorg(_)))
        );
    }

    #[tokio::test]
    async fn a_duplicate_head_is_not_fetched_again() {
        let mut machine = Machine::new(Source::linear(4), Sink::default(), Vec::new());
        settle(&mut machine, Some(4)).await;
        let rows = machine.sink.events.len();
        let head = machine.source.head_meta();
        assert_eq!(head, machine.tip().expect("tip"));
        machine.process_head(head).await.expect("duplicate");
        assert_eq!(machine.sink.events.len(), rows);
    }

    #[tokio::test]
    async fn a_same_height_fork_retracts_the_tail() {
        // Leave room in the window: the fork's common ancestor must stay retained.
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 5;
        let mut machine = Machine::new(Source::linear(head), Sink::default(), Vec::new());
        settle(&mut machine, Some(0)).await;
        // Replace the branch above the tip's predecessor and announce the new head.
        machine
            .source
            .data
            .blocks
            .insert(head, (hash(head + 100), hash(head - 1)));
        observe(&mut machine, head).await;
        assert!(matches!(machine.state(), State::Syncing));
        assert_eq!(machine.tip().expect("tip").hash, hash(head + 100));
        let markers: Vec<_> = machine
            .sink
            .events
            .iter()
            .filter_map(|event| match event {
                Event::Reorg(reorg) => Some(reorg),
                _ => None,
            })
            .collect();
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].height, head);
        assert_eq!(markers[0].orphaned_hashes, [hash(head)]);
    }

    #[tokio::test]
    async fn a_fork_below_the_window_stops_without_retraction() {
        let mut machine = Machine::new(Source::linear(3), Sink::default(), Vec::new());
        settle(&mut machine, Some(3)).await;
        // The window keeps only the tip; a replacement at 3 whose parent is below it.
        machine.source.data.blocks.insert(3, (hash(103), hash(1)));
        assert!(matches!(
            machine.process_head(machine.source.head_meta()).await,
            Err(PipelineError::UndoWindowExceeded { .. })
        ));
        assert!(
            !machine
                .sink
                .events
                .iter()
                .any(|event| matches!(event, Event::Reorg(_)))
        );
    }

    /// A source returning several heights per call still has every buried height accepted
    /// once, in order, with the last range cut at the buried bound and the tail filled
    /// after it.
    #[tokio::test]
    async fn ranged_backfill_accepts_every_buried_height_once_in_order() {
        // The buried bound is 17: ranges of seven are 0..=6, 7..=13, then 14..=17.
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 17;
        let mut source = Source::linear(head);
        source.data.range = 7;
        let mut machine = Machine::new(source, Sink::default(), Vec::new());
        machine.start(Some(0)).await.expect("start");
        catch_up(&mut machine).await;
        assert!(matches!(machine.state(), State::Syncing));
        assert_eq!(heights(&machine), (0..=head).collect::<Vec<_>>());
        assert_eq!(
            machine.source.data.block_calls.load(Ordering::Relaxed),
            usize::try_from(head + 1).expect("count"),
            "each height is fetched exactly once"
        );
    }

    #[tokio::test]
    async fn a_range_answered_with_no_blocks_is_rejected_before_output() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 5;
        let mut source = Source::linear(head);
        source.data.range = 0;
        let mut machine = Machine::new(source, Sink::default(), Vec::new());
        machine.start(Some(0)).await.expect("start");
        assert!(matches!(
            machine.step().await,
            Err(PipelineError::InvalidRange {
                from: 0,
                to: 5,
                returned: 0
            })
        ));
        assert!(machine.sink.events.is_empty());
        assert!(machine.tip().is_none());
    }

    #[tokio::test]
    async fn source_failure_retains_backfill_cursor() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 5;
        let mut machine = Machine::new(Source::linear(head), Sink::default(), Vec::new());
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
        assert_eq!(machine.tip().expect("tip").height, head);
    }

    #[tokio::test]
    async fn sink_failure_does_not_advance_tip() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 5;
        let mut machine = Machine::new(Source::linear(head), Sink::default(), Vec::new());
        machine.start(Some(0)).await.expect("start");
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
            Machine::new(Source::linear(3), sink, Vec::new())
                .run()
                .await,
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
            let run = Machine::new(Source::linear(3), sink, Vec::new()).run();
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
    async fn reorg_publication_failure_preserves_old_history() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 5;
        let mut machine = Machine::new(Source::linear(head), Sink::default(), Vec::new());
        settle(&mut machine, Some(0)).await;
        machine
            .source
            .data
            .blocks
            .insert(head, (hash(head + 100), hash(head - 1)));
        machine.sink.fail = true;
        assert!(matches!(
            machine.process_head(machine.source.head_meta()).await,
            Err(PipelineError::Sink(_))
        ));
        // The failed retraction leaves the accepted branch and its tip untouched, and
        // the reorg record itself never reached the sink.
        assert_eq!(machine.tip().expect("tip").hash, hash(head));
        assert!(
            !machine
                .sink
                .events
                .iter()
                .any(|event| matches!(event, Event::Reorg(_)))
        );
    }

    #[tokio::test]
    async fn source_failure_during_discovery_leaves_old_history_unchanged() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 5;
        let mut machine = Machine::new(Source::linear(head), Sink::default(), Vec::new());
        settle(&mut machine, Some(0)).await;
        machine
            .source
            .data
            .blocks
            .insert(head, (hash(head + 100), hash(head - 1)));
        let candidate = machine
            .source
            .fetch_block(head, None)
            .await
            .expect("candidate");
        // Injection fires on the backward walk's read of the tip's predecessor, before
        // any retraction, so the accepted history must survive intact.
        machine.source.data.fail_once.store(true, Ordering::Relaxed);
        assert!(matches!(
            machine.handle_reorg(candidate, head).await,
            Err(PipelineError::Source(_))
        ));
        assert_eq!(machine.tip().expect("tip").hash, hash(head));
        assert!(
            !machine
                .sink
                .events
                .iter()
                .any(|event| matches!(event, Event::Reorg(_)))
        );
    }

    #[tokio::test]
    async fn live_driver_returns_source_errors_without_retrying() {
        let source = Source::linear(3);
        source.data.fail_once.store(true, Ordering::Relaxed);
        assert!(matches!(
            Machine::new(source, Sink::default(), Vec::new())
                .run()
                .await,
            Err(PipelineError::Source(SourceError::Malformed { .. }))
        ));
    }

    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    struct LiveSource {
        source: Source,
        head: Arc<AtomicU64>,
        forked: Arc<AtomicBool>,
        hints: std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<BlockMeta>>>,
    }
    impl BlockSource for LiveSource {
        fn chain(&self) -> &ChainId {
            self.source.chain()
        }
        async fn subscribe_heads(&self) -> Result<HeadStream, SourceError> {
            let hints = self
                .hints
                .lock()
                .expect("hints lock")
                .take()
                .expect("subscribe once");
            Ok(Box::pin(stream::unfold(hints, |mut hints| async move {
                hints.recv().await.map(|hint| (Ok(hint), hints))
            })))
        }
        async fn fetch_header(&self, height: Option<u64>) -> Result<BlockMeta, SourceError> {
            if let Some(height) = height {
                return Ok(self.fetch_block(height, None).await?.meta);
            }
            let height = self.head.load(Ordering::Relaxed);
            let replacement = self.forked.load(Ordering::Relaxed) && height >= 3;
            let tip_hash = hash(height + if replacement { 100 } else { 0 });
            let parent = if height == 0 {
                hash(0)
            } else if replacement && height >= 3 {
                hash(height + 99)
            } else {
                hash(height - 1)
            };
            Ok(BlockMeta {
                height,
                hash: tip_hash,
                parent_hash: parent,
                timestamp: height,
            })
        }
        async fn fetch_block(
            &self,
            height: u64,
            head: Option<&BlockMeta>,
        ) -> Result<FetchedBlock, SourceError> {
            let mut block = self.source.fetch_block(height, head).await?;
            if self.forked.load(Ordering::Relaxed) && height >= 3 {
                block.meta.hash = hash(height + 100);
                block.meta.parent_hash = hash(if height == 3 { 2 } else { height + 99 });
                let Event::Block(marker) = &mut block.events[0] else {
                    unreachable!()
                };
                marker.hash = block.meta.hash;
                marker.parent_hash = block.meta.parent_hash;
            }
            Ok(block)
        }
    }
    /// Forwards every event but the per-block ledger markers, which the live test does
    /// not assert on: it is about which blocks and reorgs reach the sink, and in what order.
    struct LiveSink(tokio::sync::mpsc::UnboundedSender<Event>);
    impl EnvelopeSink for LiveSink {
        async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
            if matches!(envelope.event, Event::AcceptedBlock(_)) {
                return Ok(());
            }
            self.0
                .send(envelope.event)
                .map_err(|_| SinkError::StorageClosed)
        }
        async fn flush(&mut self) -> Result<(), SinkError> {
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn silent_subscription_reconciles_gaps_and_reorgs_without_duplicate_stale_hints() {
        let recovery = async {
            let head = Arc::new(AtomicU64::new(1));
            let forked = Arc::new(AtomicBool::new(false));
            let (hints, hint_stream) = tokio::sync::mpsc::unbounded_channel();
            let (events, mut received) = tokio::sync::mpsc::unbounded_channel();
            let source = LiveSource {
                source: Source::linear(6),
                head: Arc::clone(&head),
                forked: Arc::clone(&forked),
                hints: std::sync::Mutex::new(Some(hint_stream)),
            };
            // The source's head is at 1, so the default start lands there — the live-only
            // case: startup seeds block 1 and the timer fills the rest, leaving a tallied
            // window for the later fork.
            let machine = Machine::new(source, LiveSink(events), Vec::new());
            let run = tokio::spawn(machine.run());
            loop {
                if let Event::Block(block) = received.recv().await.expect("startup event") {
                    assert_eq!(block.number, 1);
                    break;
                }
            }
            // No new head hint arrives during the outage: the timer must discover the gap.
            head.store(6, Ordering::Relaxed);
            tokio::time::advance(std::time::Duration::from_secs(30)).await;
            for expected in 2..=6 {
                let Event::Block(block) = received.recv().await.expect("catch-up event") else {
                    panic!("ordinary gap must not emit a reorg");
                };
                assert_eq!(block.number, expected);
            }
            // A second silent period spans a fork, not just a height gap.
            forked.store(true, Ordering::Relaxed);
            tokio::time::advance(std::time::Duration::from_secs(30)).await;
            let Event::Reorg(reorg) = received.recv().await.expect("reorg marker") else {
                panic!("the old suffix must be retracted before replacement blocks");
            };
            assert_eq!(reorg.orphaned_hashes, [hash(6), hash(5), hash(4), hash(3)]);
            for expected in 3..=6 {
                let Event::Block(block) = received.recv().await.expect("replacement event") else {
                    panic!("replacement blocks must follow the reorg marker");
                };
                assert_eq!(block.number, expected);
                assert_eq!(block.hash, hash(expected + 100));
            }
            // Stale, already-accepted hints must not re-fetch or duplicate output.
            for _ in 0..2 {
                hints
                    .send(BlockMeta {
                        height: 2,
                        hash: hash(2),
                        parent_hash: hash(1),
                        timestamp: 2,
                    })
                    .expect("stale hint");
            }
            drop(hints);
            assert!(matches!(
                run.await.expect("driver task"),
                Err(PipelineError::SubscriptionClosed)
            ));
            assert!(
                received.try_recv().is_err(),
                "stale hints must not duplicate output"
            );
        };
        tokio::time::timeout(std::time::Duration::from_mins(2), recovery)
            .await
            .expect("silent recovery must converge within four fallback intervals");
    }

    #[test]
    fn wrong_height_and_noncontiguous_parent_are_rejected() {
        let marker = BlockMeta {
            height: 4,
            hash: hash(4),
            parent_hash: hash(1),
            timestamp: 0,
        };
        let fetched = FetchedBlock {
            meta: marker,
            events: vec![],
        };
        assert!(matches!(
            Machine::<Source, Sink>::marker(&fetched, 3),
            Err(PipelineError::IdentityMismatch { .. })
        ));
        assert!(matches!(
            Machine::<Source, Sink>::validate_link(
                BlockMeta {
                    height: 3,
                    hash: hash(3),
                    parent_hash: hash(2),
                    timestamp: 3,
                },
                marker,
            ),
            Err(PipelineError::BrokenLink { .. })
        ));
    }

    /// The identities a store would hand back after committing `heights` of the linear
    /// chain [`Source::linear`] builds.
    fn ledger(heights: std::ops::RangeInclusive<u64>) -> Vec<BlockMeta> {
        heights
            .map(|height| BlockMeta {
                height,
                hash: hash(height),
                parent_hash: hash(height.saturating_sub(1)),
                timestamp: height,
            })
            .collect()
    }

    /// Replaces the fake chain from `from` upward with a branch whose hashes are offset
    /// by 100, linked to the canonical block below `from`.
    fn fork(source: &mut Source, from: u64, through: u64) {
        for height in from..=through {
            let parent = if height == from {
                hash(height - 1)
            } else {
                hash(height + 99)
            };
            source
                .data
                .blocks
                .insert(height, (hash(height + 100), parent));
        }
    }

    fn reorgs(machine: &Machine<Source, Sink>) -> Vec<&Reorg> {
        machine
            .sink
            .events
            .iter()
            .filter_map(|event| match event {
                Event::Reorg(reorg) => Some(reorg),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn every_block_batch_ends_with_its_ledger_marker() {
        let mut machine = Machine::new(Source::linear(3), Sink::default(), Vec::new());
        settle(&mut machine, Some(2)).await;
        let markers: Vec<_> = machine
            .sink
            .events
            .iter()
            .map(|event| match event {
                Event::Block(block) => ("block", block.number),
                Event::AcceptedBlock(accepted) => ("accepted", accepted.height),
                other => panic!("unexpected {}", other.kind()),
            })
            .collect();
        assert_eq!(
            markers,
            [("block", 2), ("accepted", 2), ("block", 3), ("accepted", 3)]
        );
    }

    #[tokio::test]
    async fn an_empty_block_still_reaches_the_sink_as_its_ledger_marker() {
        let mut machine = Machine::new(Source::linear(3), Sink::default(), Vec::new());
        let meta = machine.source.head_meta();
        machine.commit(meta, Vec::new()).await.expect("commit");
        assert_eq!(
            machine.sink.events,
            [Event::AcceptedBlock(AcceptedBlock::from(meta))]
        );
        assert_eq!(machine.sink.flushes, 1, "the empty block is its own batch");
        assert_eq!(machine.tip(), Some(meta));
    }

    #[tokio::test]
    async fn a_resumed_run_continues_after_a_matching_stored_tip() {
        let mut machine = Machine::new(Source::linear(8), Sink::default(), ledger(0..=5));
        assert_eq!(machine.tip().expect("restored tip").height, 5);
        machine.start(None).await.expect("start");
        assert!(matches!(machine.state(), State::Syncing));
        assert_eq!(
            heights(&machine),
            [6, 7, 8],
            "nothing below the tip is replayed"
        );
        assert!(reorgs(&machine).is_empty());
    }

    #[tokio::test]
    async fn a_resumed_run_far_behind_the_head_backfills_from_the_stored_tip() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 20;
        let mut machine = Machine::new(Source::linear(head), Sink::default(), ledger(0..=5));
        machine.start(None).await.expect("start");
        assert!(matches!(
            machine.state(),
            State::Backfilling { next: 6, .. }
        ));
        catch_up(&mut machine).await;
        assert_eq!(heights(&machine), (6..=head).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_fork_while_down_is_retracted_once_then_replayed() {
        let mut source = Source::linear(7);
        fork(&mut source, 4, 7);
        let mut machine = Machine::new(source, Sink::default(), ledger(0..=5));
        machine.start(None).await.expect("start");
        assert!(matches!(machine.state(), State::Syncing));
        let markers = reorgs(&machine);
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].height, 4);
        assert_eq!(markers[0].orphaned_hashes, [hash(5), hash(4)]);
        assert!(
            matches!(machine.sink.events[0], Event::Reorg(_)),
            "the retraction comes before any replacement"
        );
        assert_eq!(heights(&machine), [4, 5, 6, 7]);
        assert_eq!(machine.tip().expect("tip").hash, hash(107));
    }

    #[tokio::test]
    async fn a_fork_while_down_deeper_than_the_restored_window_stops() {
        let mut source = Source::linear(7);
        fork(&mut source, 3, 7);
        let mut machine = Machine::new(source, Sink::default(), ledger(4..=5));
        assert!(matches!(
            machine.start(None).await,
            Err(PipelineError::UndoWindowExceeded { .. })
        ));
        assert!(
            machine.sink.events.is_empty(),
            "nothing is retracted or replayed"
        );
    }

    #[tokio::test]
    async fn the_first_resumed_block_must_link_to_the_stored_tip() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 20;
        let mut source = Source::linear(head);
        source.data.blocks.insert(6, (hash(6), hash(99)));
        let mut machine = Machine::new(source, Sink::default(), ledger(0..=5));
        machine.start(None).await.expect("start");
        assert!(matches!(
            machine.step().await,
            Err(PipelineError::BrokenLink { .. })
        ));
        assert!(machine.sink.events.is_empty());
        assert_eq!(machine.tip().expect("tip").height, 5);
    }

    #[tokio::test]
    async fn a_source_behind_the_stored_tip_waits_for_it() {
        let mut machine = Machine::new(Source::linear(8), Sink::default(), ledger(0..=5));
        machine.source.data.head = 3;
        machine.start(None).await.expect("start");
        assert!(matches!(machine.state(), State::Syncing));
        assert!(machine.sink.events.is_empty());
        assert_eq!(machine.tip().expect("tip").height, 5);
    }

    #[tokio::test]
    async fn a_start_height_with_stored_history_is_refused_before_any_read() {
        let machine = Machine::new(Source::linear(8), Sink::default(), ledger(0..=5));
        let Err(error) = machine.backfill(2).await else {
            panic!("a start height contradicts the stored tip");
        };
        assert!(matches!(
            error,
            PipelineError::StartWithHistory { start: 2, tip: 5 }
        ));
    }

    #[test]
    fn only_the_newest_linked_suffix_of_a_stored_ledger_is_kept() {
        let mut stored = ledger(0..=2);
        stored.extend(ledger(5..=8));
        let kept: Vec<_> = linked_suffix(stored)
            .iter()
            .map(|meta| meta.height)
            .collect();
        assert_eq!(kept, [5, 6, 7, 8]);
        assert!(linked_suffix(Vec::new()).is_empty());

        // A window's worth plus one: the oldest becomes the floor, not a tail entry.
        let window = MAX_UNFINALIZED_BLOCKS as u64;
        let machine = Machine::new(Source::linear(0), Sink::default(), ledger(0..=window));
        assert_eq!(machine.ring.entries.len(), MAX_UNFINALIZED_BLOCKS);
        assert_eq!(machine.ring.floor.expect("floor").height, 0);
        assert_eq!(machine.tip().expect("tip").height, window);
    }

    #[test]
    fn the_ring_slides_and_keeps_a_recoverable_floor() {
        let mut ring = UndoRing::default();
        for height in 0..=(MAX_UNFINALIZED_BLOCKS as u64) {
            ring.push(BlockMeta {
                height,
                hash: hash(height),
                parent_hash: hash(height.saturating_sub(1)),
                timestamp: height,
            });
        }
        assert_eq!(ring.entries.len(), MAX_UNFINALIZED_BLOCKS);
        assert_eq!(ring.oldest().expect("floor").height, 0);
        assert_eq!(
            ring.tip().expect("tip").height,
            MAX_UNFINALIZED_BLOCKS as u64
        );
        // The floor and the whole tail are still addressable.
        assert!(ring.retains(0));
        assert!(ring.at(0).is_some());
        assert!(ring.at(1).is_some());
    }

    #[test]
    fn the_window_retains_only_from_its_oldest_identity() {
        // A seeded ring whose floor sits above the last backfilled height: the fork
        // walk must stop rather than read below what is remembered.
        let mut ring = UndoRing {
            floor: Some(BlockMeta {
                height: 2,
                hash: hash(2),
                parent_hash: hash(1),
                timestamp: 2,
            }),
            ..UndoRing::default()
        };
        for height in 3..=5 {
            ring.push(BlockMeta {
                height,
                hash: hash(height),
                parent_hash: hash(height - 1),
                timestamp: height,
            });
        }
        assert_eq!(ring.oldest().expect("oldest").height, 2);
        assert!(ring.retains(2));
        assert!(ring.retains(5));
        assert!(!ring.retains(1));
        assert_eq!(ring.at(2).expect("floor").hash, hash(2));
    }
}
