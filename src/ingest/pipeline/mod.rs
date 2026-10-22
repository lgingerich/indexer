//! Reorg-aware, block-at-a-time ingestion that resumes from what the store committed.
//!
//! Startup samples the head and splits the work at the reorg window. Heights at or
//! below `head - window` are buried: they sit a full window behind the sampled head, so
//! no fork reaches them and backfill indexes them straight through without asking the
//! source where the chain is. On a chain shorter than the window the bound clamps to
//! genesis, which is equally unreorgable. Reaching that bound, backfill samples the head
//! again and continues while the chain has buried more. Everything above the bound is the
//! reorgable tail, and startup owns it too — it fills to the last sampled head through
//! the reorg-aware path before any live head is consumed, so a run never leaves a gap
//! under its start. A start already inside the window makes that fill the whole run. The live phase then follows `newHeads`, reuses the announced metadata,
//! and reconciles gaps and forks over HTTP. Every accepted block's identity is retained
//! in a sliding window so a fork within it is retracted; deeper forks stop rather than
//! guess.
//!
//! A stop height caps startup instead: backfill and the tail fill end at it, and the run
//! returns without going live. It must be at or below the sampled head.
//!
//! # Resume
//!
//! Every block's batch ends with an [`Event::AcceptedBlock`] marker, so a store commits a
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
//! [`Reorg`](crate::wire::envelope::Reorg), and replays the replacements. Either way the
//! first new block must link to what the store committed, so coverage from the restored
//! tip forward is contiguous. A fork deeper than the restored window stops with
//! [`PipelineError::UndoWindowExceeded`]: recovering from it is an operator's decision.
//!
//! The first block a run ever indexed is the exception. Nothing below it was stored, so
//! a fork that replaces it has nothing older to retract: while the window still holds
//! that block, the walk stops there, retracts everything above it, and adopts the new
//! branch from the start height, as the first block itself was adopted.

mod backfill;
mod error;
mod reorg;
mod ring;
mod startup;
#[cfg(test)]
mod testing;

use std::sync::Arc;

use futures_util::StreamExt as _;
use tracing::{debug, info, warn};

pub use error::PipelineError;
pub use ring::Ledger;
use ring::UndoRing;

use crate::ingest::source::{BlockSource, SourceError};
use crate::metrics;
use crate::sink::EnvelopeSink;
use crate::wire::envelope::{BlockMeta, Envelope, Event};

/// Hardcoded budget for the recovered undo window.
///
/// The window is a recovery floor, not a finality claim: a fork deeper than this
/// stops with [`PipelineError::UndoWindowExceeded`] rather than retracting a suffix
/// the pipeline can no longer identify. Identities cost nothing beside the payloads
/// they describe, so the value is a bounded-memory choice, not a tuning knob.
pub const MAX_UNFINALIZED_BLOCKS: usize = 4096;

/// How soon a live head the source answered inconsistently is asked about again, and the
/// longest wait it doubles to while the source keeps answering so. A head arriving first
/// covers it instead, so on a fast chain the wait seldom matters.
const RETRY_FIRST: std::time::Duration = std::time::Duration::from_millis(100);
const RETRY_LONGEST: std::time::Duration = std::time::Duration::from_secs(2);

/// The retry after a failed attempt at `attempted`, given the one pending, `retry`.
///
/// Asking about the same head again doubles its wait, up to [`RETRY_LONGEST`]; a newly
/// announced head starts at [`RETRY_FIRST`], since a source a block behind its own
/// announcements fails most heads once, and on a fast chain doubling across them reached
/// the longest wait within a second. A failed reconcile, which names no head, keeps
/// asking about the pending one.
fn next_retry(
    retry: Option<(BlockMeta, std::time::Duration)>,
    attempted: Option<BlockMeta>,
) -> Option<(BlockMeta, std::time::Duration)> {
    let longer = |delay: std::time::Duration| delay.saturating_mul(2).min(RETRY_LONGEST);
    match (retry, attempted) {
        (Some((pending, delay)), None) => Some((pending, longer(delay))),
        (Some((pending, delay)), Some(head)) if head == pending => Some((pending, longer(delay))),
        (_, Some(head)) => Some((head, RETRY_FIRST)),
        (None, None) => None,
    }
}

/// Resolves to the head in `retry` once its wait is over; never, with none to retry.
async fn retry_after(retry: Option<(BlockMeta, std::time::Duration)>) -> BlockMeta {
    match retry {
        Some((head, delay)) => {
            tokio::time::sleep(delay).await;
            head
        }
        None => std::future::pending().await,
    }
}

/// How many accepted blocks a store hands back to a restart: the undo window and the
/// block below it, which the oldest unfinalized block's parent hash is checked against.
pub const LEDGER_WINDOW: usize = MAX_UNFINALIZED_BLOCKS + 1;

/// One process-local driver, consumed by its public asynchronous operations.
#[derive(Debug)]
pub struct Machine<S, K> {
    /// Shared so backfill's fetches in flight can borrow it while blocks are accepted.
    source: Arc<S>,
    sink: K,
    ring: UndoRing,
}

impl<S: BlockSource, K> Machine<S, K> {
    /// The source's finality depth, never deeper than a run can retract.
    fn finality_depth(&self) -> u64 {
        self.source
            .finality_depth()
            .clamp(1, MAX_UNFINALIZED_BLOCKS as u64)
    }
}

impl<S, K> Machine<S, K> {
    /// Builds a driver whose undo window starts as `ledger`: the accepted identities the
    /// stores read back, oldest first.
    ///
    /// An empty ledger starts fresh, at the sampled head or a requested height. Otherwise
    /// the run resumes after the ledger's tip; see the module docs. Only the newest
    /// parent-linked suffix is kept, and only the newest [`MAX_UNFINALIZED_BLOCKS`] of it
    /// plus one floor are remembered.
    #[must_use]
    pub fn new(source: S, sink: K, ledger: impl Into<Ledger>) -> Self {
        Self {
            source: Arc::new(source),
            sink,
            ring: UndoRing::from(ledger.into()),
        }
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

    /// Checks that a block read for `height` is at that height.
    /// Does not establish canonicality or validate linkage to another block.
    fn marker(meta: BlockMeta, height: u64) -> Result<BlockMeta, PipelineError> {
        if meta.height != height {
            return Err(PipelineError::WrongHeight {
                requested: height,
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

    /// The highest height a fork cannot reach while `head` is the head, `depth` blocks
    /// under it (see [`Self::finality_depth`]).
    fn buried(head: BlockMeta, depth: u64) -> u64 {
        head.height.saturating_sub(depth)
    }
}

impl<S: BlockSource + 'static, K: EnvelopeSink> Machine<S, K> {
    /// Indexes from `start` through the sampled head, then follows live heads; or, given a
    /// `stop`, indexes through `stop` and returns. A source that does not
    /// [follow heads](BlockSource::follows_heads) only backfills: the run indexes the
    /// buried heights, no further than `stop`, and returns, leaving the rest to a run on a
    /// source that does, which resumes after it.
    ///
    /// `start` is a fresh run's first height; `None` starts at the sampled head. A machine
    /// built with a ledger resumes after its tip instead: a `start` the stored history
    /// already reaches is ignored, and one above the tip's successor is refused. A `stop`
    /// must be at or below the sampled head and at or above where the run starts; a
    /// ledger whose tip is already at or past it returns at once.
    /// Backfill indexes only buried heights, which cannot reorg; once it reaches the
    /// reorgable window the live phase owns the tail. Notifications are hints, not a
    /// replay log: HTTP reconciliation fills gaps and resolves reorgs. A live head the
    /// source answers inconsistently is skipped; every other error propagates. The source
    /// owns connection maintenance.
    ///
    /// # Errors
    /// Consumes this instance. An error or cancellation drops its source, sink, and
    /// history, so partially delivered output cannot be followed by reuse.
    /// Returns [`PipelineError::StartAboveHistory`] for a `start` past a ledger's tip,
    /// [`PipelineError::StartAboveHead`] for one above the sampled head,
    /// [`PipelineError::BackfillWithoutStart`] for neither on a source that only
    /// backfills, [`PipelineError::StartNotBuried`] for one there above the buried
    /// heights, [`PipelineError::StopAboveHead`] and [`PipelineError::StopBelowStart`]
    /// for a `stop` outside those bounds, [`PipelineError::IdentityMismatch`] when a source
    /// that only backfills holds another block at the stored tip,
    /// [`PipelineError::SubscriptionClosed`] when the subscription ends, and permanent
    /// source, invariant, or sink failures.
    pub async fn run(mut self, start: Option<u64>, stop: Option<u64>) -> Result<(), PipelineError> {
        info!(chain = %self.source.chain(), start_block = ?start, stop_block = ?stop,
            "ingest started");
        // Backfill is headless: it samples its target itself and reads concrete heights
        // only, so the subscription is opened after it converges.
        if let Some(stop) = self.sync_settled(start, stop).await? {
            info!(chain = %self.source.chain(), stop_block = stop, "ingest reached the stop block");
            return Ok(());
        }
        let mut heads = self.source.subscribe_heads().await?;
        // The last head the source answered inconsistently, to ask about again, and how
        // long to wait first.
        let mut retry: Option<(BlockMeta, std::time::Duration)> = None;
        loop {
            let (attempted, result) = tokio::select! {
                // A head, a retry, and the reconcile timer can be ready together; checking
                // them in this order makes the choice the same every time instead of
                // tokio's random pick. Any order is correct: each brings coverage up to a
                // head, and a newer one covers an older.
                biased;
                head = heads.next() => match head {
                    Some(head) => {
                        let head = head?;
                        // The newest head, as a sample is: on a live chain a head arrives
                        // well within the reconcile interval, so the timer never fires.
                        self.observe_head(head.height);
                        (Some(head), self.process_head(head).await)
                    }
                    None => return Err(PipelineError::SubscriptionClosed),
                },
                head = retry_after(retry) => (Some(head), self.process_head(head).await),
                () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    (None, self.reconcile().await)
                }
            };
            match result {
                // Accepted history matches what was delivered, so asking again is safe. The
                // head is asked about again shortly rather than at the next one: a backend
                // behind the one that announced it catches up in well under a second, and
                // on a slow chain the next head is seconds away.
                Err(
                    error @ (PipelineError::UnstableSource
                    | PipelineError::Source(SourceError::Inconsistent { .. })),
                ) => {
                    retry = next_retry(retry, attempted);
                    if let Some((head, delay)) = retry {
                        debug!(%error, height = head.height, delay_ms = delay.as_millis(),
                            "the source does not serve this head yet; asking again");
                        if delay == RETRY_LONGEST {
                            warn!(%error, height = head.height,
                                "the source still does not serve this head; asking again");
                        }
                    }
                }
                Err(error) => return Err(error),
                // Coverage reached a head, which covers any older one waiting to be asked
                // about again.
                Ok(()) => retry = None,
            }
        }
    }

    /// Delivers a block's events, then records its identity.
    ///
    /// The batch ends with the block's [`Event::AcceptedBlock`] marker, so a store commits the
    /// identity a restart resumes from in the same transaction as the block's rows, and
    /// an empty block still reaches the store. The order is the whole point: a delivery
    /// that fails leaves the identity unrecorded, so the accepted history never claims a
    /// block the sink did not take.
    async fn commit(
        &mut self,
        meta: BlockMeta,
        mut events: Vec<Event>,
    ) -> Result<(), PipelineError> {
        events.push(Event::AcceptedBlock(meta));
        self.deliver(events).await?;
        self.ring.push(meta);
        metrics::ingested(meta.height);
        Ok(())
    }

    /// Samples the source's head and reports it to the sink.
    async fn sample_head(&mut self) -> Result<BlockMeta, PipelineError> {
        let head = self.source.fetch_header(None).await?;
        self.observe_head(head.height);
        Ok(head)
    }

    /// Reports the source's newest head to the sink, which measures each commit's lag
    /// against it, and to the metrics.
    fn observe_head(&mut self, height: u64) {
        self.sink.observe_head(height);
        metrics::chain_head(height);
    }

    /// Reconciles the live tail: duplicate, gap, or fork against the sampled head.
    ///
    /// # Errors
    /// Returns [`PipelineError::UnstableSource`] when a re-read candidate disagrees
    /// with the sampled head.
    async fn reconcile(&mut self) -> Result<(), PipelineError> {
        let head = self.sample_head().await?;
        self.process_head(head).await
    }

    /// Reads the header at `height`, checking it is at that height.
    async fn header(&self, height: u64) -> Result<BlockMeta, PipelineError> {
        Self::marker(self.source.fetch_header(Some(height)).await?, height)
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use futures_util::stream;
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

    use super::testing::*;
    use super::*;
    use crate::ingest::source::{FetchedBlock, HeadStream};
    use crate::sink::SinkError;
    use crate::wire::envelope::{Block, ChainId};

    struct LiveSource {
        source: Source,
        head: Arc<AtomicU64>,
        forked: Arc<AtomicBool>,
        /// Set to answer the next block fetch inconsistently.
        inconsistent: Arc<AtomicBool>,
        hints: std::sync::Mutex<Option<UnboundedReceiver<BlockMeta>>>,
    }

    impl LiveSource {
        /// A live chain over [`Source::linear`]`(6)` whose head starts at 1, and the
        /// sender of the heads its subscription announces.
        fn new() -> (Self, UnboundedSender<BlockMeta>) {
            let (hints, hint_stream) = unbounded_channel();
            let source = Self {
                source: Source::linear(6),
                head: Arc::new(AtomicU64::new(1)),
                forked: Arc::default(),
                inconsistent: Arc::default(),
                hints: std::sync::Mutex::new(Some(hint_stream)),
            };
            (source, hints)
        }
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
            if self.inconsistent.swap(false, Ordering::Relaxed) {
                return Err(SourceError::Inconsistent {
                    context: "test".into(),
                    detail: "injected inconsistency".into(),
                });
            }
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
    struct LiveSink(UnboundedSender<Event>);

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

    /// Forwards as [`LiveSink`] does, and keeps the newest head it was told of.
    struct HeadSink(LiveSink, Arc<AtomicU64>);

    impl EnvelopeSink for HeadSink {
        async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
            self.0.publish(envelope).await
        }
        async fn flush(&mut self) -> Result<(), SinkError> {
            self.0.flush().await
        }
        fn observe_head(&mut self, height: u64) {
            self.1.store(height, Ordering::Relaxed);
        }
    }

    /// The next event a live run forwarded, which must be a block; `what` names it in
    /// the failure.
    async fn next_block(received: &mut UnboundedReceiver<Event>, what: &str) -> Box<Block> {
        match received.recv().await {
            Some(Event::Block(block)) => block,
            other => panic!("expected {what}, received {other:?}"),
        }
    }

    /// Ends a live run by closing its subscription, and checks that is why it ended.
    async fn close(
        hints: UnboundedSender<BlockMeta>,
        run: tokio::task::JoinHandle<Result<(), PipelineError>>,
    ) {
        drop(hints);
        fails_with(
            run.await.expect("driver task"),
            &PipelineError::SubscriptionClosed,
        );
    }

    #[tokio::test]
    async fn failed_run_drops_its_sink() {
        let (dropped, mut notification) = tokio::sync::oneshot::channel();
        let mut sink = Sink::default();
        sink.fail = true;
        sink.dropped = Some(dropped);
        fails_with(
            Machine::new(Source::linear(3), sink, Vec::new())
                .run(None, None)
                .await,
            &PipelineError::Sink(SinkError::StorageClosed),
        );
        assert_eq!(notification.try_recv(), Ok(()));
    }

    #[tokio::test]
    async fn canceled_run_drops_its_sink() {
        let (dropped, mut notification) = tokio::sync::oneshot::channel();
        let mut sink = Sink::default();
        sink.stall = true;
        sink.dropped = Some(dropped);
        {
            let run = Machine::new(Source::linear(3), sink, Vec::new()).run(None, None);
            tokio::pin!(run);
            assert!(futures_util::poll!(&mut run).is_pending());
            assert_eq!(
                notification.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            );
        }
        assert_eq!(notification.try_recv(), Ok(()));
    }

    /// A head the subscription announces is observed as a sampled one is. On a live
    /// chain the reconcile timer never fires, so without it every commit's lag was
    /// measured against the head sampled at startup, and read zero.
    #[tokio::test(start_paused = true)]
    async fn an_announced_head_is_observed() {
        let observed = Arc::new(AtomicU64::new(0));
        let (events, mut received) = unbounded_channel();
        let (source, hints) = LiveSource::new();
        let sink = HeadSink(LiveSink(events), Arc::clone(&observed));
        let run = tokio::spawn(Machine::new(source, sink, Vec::new()).run(None, None));
        assert_eq!(next_block(&mut received, "block 1").await.number, 1);
        assert_eq!(observed.load(Ordering::Relaxed), 1);
        hints.send(meta(3)).expect("hint");
        for expected in [2, 3] {
            let block = next_block(&mut received, "the announced head caught up to").await;
            assert_eq!(block.number, expected);
        }
        assert_eq!(observed.load(Ordering::Relaxed), 3);
        close(hints, run).await;
    }

    #[tokio::test(start_paused = true)]
    async fn silent_subscription_reconciles_gaps_and_reorgs_without_duplicate_stale_hints() {
        let (events, mut received) = unbounded_channel();
        let (source, hints) = LiveSource::new();
        let head = Arc::clone(&source.head);
        let forked = Arc::clone(&source.forked);
        // The source's head is at 1, so the default start lands there — the live-only
        // case: startup seeds block 1 and the timer fills the rest, leaving a tallied
        // window for the later fork.
        let run = tokio::spawn(Machine::new(source, LiveSink(events), Vec::new()).run(None, None));
        assert_eq!(next_block(&mut received, "block 1").await.number, 1);
        // No new head hint arrives during the outage: the timer must discover the gap.
        head.store(6, Ordering::Relaxed);
        tokio::time::advance(std::time::Duration::from_secs(30)).await;
        for expected in 2..=6 {
            let block = next_block(&mut received, "a catch-up block, not a reorg").await;
            assert_eq!(block.number, expected);
        }
        // A second silent period spans a fork, not just a height gap.
        forked.store(true, Ordering::Relaxed);
        tokio::time::advance(std::time::Duration::from_secs(30)).await;
        let Some(Event::Reorg(reorg)) = received.recv().await else {
            panic!("the old suffix must be retracted before replacement blocks");
        };
        assert_eq!(reorg.height, 3);
        assert_eq!(reorg.orphaned_hashes, [hash(6), hash(5), hash(4), hash(3)]);
        for expected in 3..=6 {
            let block = next_block(&mut received, "a replacement after the reorg").await;
            assert_eq!(block.number, expected);
            assert_eq!(block.hash, hash(expected + 100));
        }
        // Stale, already-accepted hints must not re-fetch or duplicate output.
        for _ in 0..2 {
            hints.send(meta(2)).expect("stale hint");
        }
        close(hints, run).await;
        assert_eq!(
            received.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected),
            "stale hints must not duplicate output"
        );
    }

    /// A live head the source answers inconsistently is skipped, not fatal: nothing was
    /// delivered for it, so the next head catches up from the same tip.
    #[tokio::test(start_paused = true)]
    async fn an_inconsistent_live_answer_skips_the_head() {
        let (events, mut received) = unbounded_channel();
        let (source, hints) = LiveSource::new();
        let inconsistent = Arc::clone(&source.inconsistent);
        let run = tokio::spawn(Machine::new(source, LiveSink(events), Vec::new()).run(None, None));
        assert_eq!(next_block(&mut received, "block 1").await.number, 1);
        inconsistent.store(true, Ordering::Relaxed);
        for height in [2, 3] {
            hints.send(meta(height)).expect("hint");
        }
        for expected in [2, 3] {
            let block = next_block(&mut received, "the next head caught up").await;
            assert_eq!(block.number, expected);
        }
        close(hints, run).await;
    }

    /// A live head the source answers inconsistently is asked about again within a
    /// fraction of a second, without waiting for the next head or the reconcile timer:
    /// on a slow chain those are seconds away, and the head's rows with them.
    #[tokio::test(start_paused = true)]
    async fn an_inconsistent_live_head_is_asked_about_again_soon() {
        let (events, mut received) = unbounded_channel();
        let (source, hints) = LiveSource::new();
        let inconsistent = Arc::clone(&source.inconsistent);
        let run = tokio::spawn(Machine::new(source, LiveSink(events), Vec::new()).run(None, None));
        assert_eq!(next_block(&mut received, "block 1").await.number, 1);
        inconsistent.store(true, Ordering::Relaxed);
        let announced = tokio::time::Instant::now();
        hints.send(meta(2)).expect("hint");
        // No other head is announced: only the retry can deliver block 2, and well before
        // the reconcile timer, which would not either, since the source's head is still 1.
        // The clock is paused, so the time it took is exactly the wait before the retry.
        let block = next_block(&mut received, "block 2, asked about again").await;
        assert_eq!(block.number, 2);
        let waited = announced.elapsed();
        assert!(
            (RETRY_FIRST..RETRY_FIRST * 2).contains(&waited),
            "asked again after {waited:?}, not after the first retry wait"
        );
        close(hints, run).await;
    }

    /// A head asked about again waits twice as long each time, up to the longest wait;
    /// a newly announced head starts over, so a source that misses most heads once is
    /// not taken for one stuck on a head.
    #[test]
    fn retry_waits_double_per_head_and_start_over_for_a_new_one() {
        let mut retry = next_retry(None, Some(meta(5)));
        assert_eq!(retry, Some((meta(5), RETRY_FIRST)));
        retry = next_retry(retry, Some(meta(5)));
        assert_eq!(
            retry,
            Some((meta(5), RETRY_FIRST * 2)),
            "the same head again waits twice as long"
        );
        for _ in 0..10 {
            retry = next_retry(retry, Some(meta(5)));
        }
        assert_eq!(retry, Some((meta(5), RETRY_LONGEST)));
        assert_eq!(
            next_retry(retry, None),
            Some((meta(5), RETRY_LONGEST)),
            "a failed reconcile keeps asking about the pending head"
        );
        assert_eq!(
            next_retry(retry, Some(meta(6))),
            Some((meta(6), RETRY_FIRST)),
            "a new head starts over"
        );
        assert_eq!(next_retry(None, None), None);
    }

    /// A block read for another height, or one that does not extend its predecessor by
    /// height and parent hash, is refused; the matching ones are not.
    #[test]
    fn wrong_height_and_noncontiguous_parent_are_rejected() {
        type Driver = Machine<Source, Sink>;
        let marker = BlockMeta {
            height: 4,
            hash: hash(4),
            parent_hash: hash(1),
            timestamp: 0,
        };
        fails_with(
            Driver::marker(marker, 3),
            &PipelineError::WrongHeight {
                requested: 3,
                actual: Box::new(marker),
            },
        );
        assert_eq!(Driver::marker(marker, 4).expect("at its height"), marker);
        fails_with(
            Driver::validate_link(meta(3), marker),
            &PipelineError::BrokenLink {
                previous: Box::new(meta(3)),
                actual: Box::new(marker),
            },
        );
        // The right parent does not excuse a skipped height.
        let skipped = BlockMeta {
            height: 5,
            ..meta(4)
        };
        fails_with(
            Driver::validate_link(meta(3), skipped),
            &PipelineError::BrokenLink {
                previous: Box::new(meta(3)),
                actual: Box::new(skipped),
            },
        );
        Driver::validate_link(meta(3), meta(4)).expect("linked");
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
        assert_eq!(machine.sink.flushes, 2, "each block is its own batch");
    }

    #[tokio::test]
    async fn an_empty_block_still_reaches_the_sink_as_its_ledger_marker() {
        let mut machine = Machine::new(Source::linear(3), Sink::default(), Vec::new());
        let meta = machine.source.head_meta();
        machine.commit(meta, Vec::new()).await.expect("commit");
        assert_eq!(machine.sink.events, [Event::AcceptedBlock(meta)]);
        assert_eq!(machine.sink.flushes, 1, "the empty block is its own batch");
        assert_eq!(machine.tip(), Some(meta));
    }
}
