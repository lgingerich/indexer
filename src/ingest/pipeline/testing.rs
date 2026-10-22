//! The fake chains and sinks the pipeline's tests drive a machine with.

#![expect(clippy::expect_used)]

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use alloy_primitives::B256;
use futures_util::stream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;

use super::{Machine, PipelineError};
use crate::ingest::source::{BlockSource, FetchedBlock, HeadStream, SourceError};
use crate::sink::{EnvelopeSink, SinkError};
use crate::window::MAX_UNFINALIZED_BLOCKS;
use crate::wire::datasets::evm::Block;
use crate::wire::envelope::{BlockMeta, ChainId, Envelope, Event, Reorg};

/// Produces deterministic distinct identities for test heights and replacement blocks.
pub(super) fn hash(height: u64) -> B256 {
    B256::from(alloy_primitives::U256::from(height).to_be_bytes())
}

/// The identity [`Source::linear`] gives `height`.
pub(super) fn meta(height: u64) -> BlockMeta {
    BlockMeta {
        height,
        hash: hash(height),
        parent_hash: hash(height.saturating_sub(1)),
        timestamp: height,
    }
}

/// How a block read [`Source::fail`] injected answers instead of the block.
#[derive(Clone, Copy)]
pub(super) enum Fault {
    /// As a backend behind the head does, which startup and the live phase ask again.
    Inconsistent,
    /// As [`malformed`]`("injected source failure")`, which nothing asks again.
    Failed,
}

/// A deterministic chain source. `head` moves when `fork` or a test edits it,
/// `head_calls` counts discovery so a test can bound what backfill makes, and
/// `block_calls` counts block reads.
pub(super) struct Source {
    pub(super) chain: ChainId,
    /// Each height's hash and parent hash.
    pub(super) blocks: HashMap<u64, (B256, B256)>,
    pub(super) head: u64,
    /// Heads the next samples return, in order, before `head`.
    pub(super) samples: Mutex<VecDeque<u64>>,
    /// Per height, how the next block reads there fail, and how many more do.
    faults: Mutex<HashMap<u64, (Fault, u32)>>,
    pub(super) head_calls: AtomicUsize,
    pub(super) block_calls: AtomicUsize,
    /// Header reads at a height, as the fork walk and its rechecks make.
    pub(super) header_calls: AtomicUsize,
    /// The heights backfill asks for in one `fetch_blocks`.
    pub(super) range: u64,
    /// The reads backfill and fetches made ahead keep in flight.
    pub(super) concurrency: usize,
    /// Answer each `fetch_blocks` one block short.
    pub(super) short: bool,
    /// Whether reads wait, so concurrent ones overlap: each `fetch_block` a moment, each
    /// `fetch_blocks` a second, and the range from 0 two, so the ranges above it answer
    /// first.
    pub(super) slow: bool,
    /// Slow reads in flight now, and the most there ever were.
    in_flight: AtomicUsize,
    pub(super) peak: AtomicUsize,
    /// Whether the source follows heads, or only backfills.
    pub(super) follows: bool,
    /// The source's finality depth.
    pub(super) finality: u64,
}

impl Source {
    /// Builds a canonical chain from genesis through `head`.
    pub(super) fn linear(head: u64) -> Self {
        Self {
            chain: ChainId::new("ethereum"),
            blocks: (0..=head)
                .map(|h| (h, (hash(h), hash(h.saturating_sub(1)))))
                .collect(),
            head,
            samples: Mutex::default(),
            faults: Mutex::default(),
            head_calls: AtomicUsize::new(0),
            block_calls: AtomicUsize::new(0),
            header_calls: AtomicUsize::new(0),
            range: 1,
            concurrency: 1,
            short: false,
            slow: false,
            in_flight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            follows: true,
            finality: MAX_UNFINALIZED_BLOCKS as u64,
        }
    }

    /// [`Self::linear`], as a source that only backfills.
    pub(super) fn backfilling(head: u64) -> Self {
        Self {
            follows: false,
            ..Self::linear(head)
        }
    }

    /// Makes the next `times` block reads at `height` answer `fault`.
    pub(super) fn fail(&self, height: u64, fault: Fault, times: u32) {
        self.faults
            .lock()
            .expect("faults")
            .insert(height, (fault, times));
    }

    /// The metadata a source would announce for its current head.
    pub(super) fn head_meta(&self) -> BlockMeta {
        self.meta_at(self.head).expect("the head is on the chain")
    }

    /// The identity the chain holds at `height`.
    fn meta_at(&self, height: u64) -> Result<BlockMeta, SourceError> {
        let &(hash, parent_hash) = self
            .blocks
            .get(&height)
            .ok_or_else(|| source_malformed("missing block"))?;
        Ok(BlockMeta {
            height,
            hash,
            parent_hash,
            timestamp: height,
        })
    }

    /// Reads the block at `height`, counted, answering an injected fault first.
    fn read(&self, height: u64) -> Result<FetchedBlock, SourceError> {
        self.block_calls.fetch_add(1, Ordering::Relaxed);
        if let Some((fault, left)) = self
            .faults
            .lock()
            .expect("faults")
            .get_mut(&height)
            .filter(|(_, left)| *left > 0)
        {
            *left -= 1;
            return Err(match fault {
                Fault::Inconsistent => SourceError::Inconsistent {
                    context: "test".into(),
                    detail: "a backend behind the head".into(),
                },
                Fault::Failed => source_malformed("injected source failure"),
            });
        }
        let meta = self.meta_at(height)?;
        Ok(FetchedBlock {
            meta,
            events: vec![Event::Block(Box::new(Block {
                number: height,
                hash: meta.hash,
                parent_hash: meta.parent_hash,
                ..Block::default()
            }))],
        })
    }

    /// Reads `from..=to`, stopping at the first failure, one block short if `short`.
    fn read_range(&self, from: u64, to: u64) -> Result<Vec<FetchedBlock>, SourceError> {
        let mut blocks = (from..=to)
            .map(|height| self.read(height))
            .collect::<Result<Vec<_>, _>>()?;
        if self.short {
            blocks.pop();
        }
        Ok(blocks)
    }

    /// Waits `duration`, counted in flight, if the source is slow.
    async fn wait(&self, duration: Duration) {
        if !self.slow {
            return;
        }
        let now = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(now, Ordering::Relaxed);
        tokio::time::sleep(duration).await;
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

impl BlockSource for Source {
    fn chain(&self) -> &ChainId {
        &self.chain
    }
    fn follows_heads(&self) -> bool {
        self.follows
    }
    async fn subscribe_heads(&self) -> Result<HeadStream, SourceError> {
        Ok(Box::pin(stream::pending()))
    }
    async fn fetch_header(&self, height: Option<u64>) -> Result<BlockMeta, SourceError> {
        if height.is_some() {
            self.header_calls.fetch_add(1, Ordering::Relaxed);
        }
        let height = height.unwrap_or_else(|| {
            self.head_calls.fetch_add(1, Ordering::Relaxed);
            let sample = self.samples.lock().expect("samples").pop_front();
            sample.unwrap_or(self.head)
        });
        self.meta_at(height)
    }
    async fn fetch_blocks(&self, from: u64, to: u64) -> Result<Vec<FetchedBlock>, SourceError> {
        self.wait(Duration::from_secs(if from == 0 { 2 } else { 1 }))
            .await;
        self.read_range(from, to)
    }
    fn backfill_span(&self) -> u64 {
        self.range
    }
    fn backfill_concurrency(&self) -> usize {
        self.concurrency
    }
    fn finality_depth(&self) -> u64 {
        self.finality
    }
    async fn fetch_block(
        &self,
        height: u64,
        _head: Option<&BlockMeta>,
    ) -> Result<FetchedBlock, SourceError> {
        self.wait(Duration::from_millis(5)).await;
        self.read(height)
    }
}

#[derive(Default)]
pub(super) struct Sink {
    pub(super) events: Vec<Event>,
    pub(super) flushes: usize,
    pub(super) fail: bool,
    pub(super) stall: bool,
    pub(super) dropped: Option<tokio::sync::oneshot::Sender<()>>,
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

/// A [`Source`] a spawned run reads while the test moves it through the same lock, with
/// a subscription announcing the heads the test sends. Headers are read as blocks are,
/// so an injected fault meets whichever read of its height comes first.
struct LiveSource {
    id: ChainId,
    chain: Arc<Mutex<Source>>,
    hints: Mutex<Option<UnboundedReceiver<BlockMeta>>>,
}

impl LiveSource {
    fn lock(&self) -> MutexGuard<'_, Source> {
        self.chain.lock().expect("chain")
    }
}

impl BlockSource for LiveSource {
    fn chain(&self) -> &ChainId {
        &self.id
    }
    fn follows_heads(&self) -> bool {
        self.lock().follows
    }
    async fn fetch_blocks(&self, from: u64, to: u64) -> Result<Vec<FetchedBlock>, SourceError> {
        self.lock().read_range(from, to)
    }
    fn backfill_span(&self) -> u64 {
        self.lock().range
    }
    fn finality_depth(&self) -> u64 {
        self.lock().finality
    }
    fn backfill_concurrency(&self) -> usize {
        self.lock().concurrency
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
        let chain = self.lock();
        match height {
            Some(height) => Ok(chain.read(height)?.meta),
            None => Ok(chain.head_meta()),
        }
    }
    async fn fetch_block(
        &self,
        height: u64,
        _head: Option<&BlockMeta>,
    ) -> Result<FetchedBlock, SourceError> {
        self.lock().read(height)
    }
}

/// Forwards every event but the per-block ledger markers, which the live tests do not
/// assert on: they are about which blocks and reorgs reach the sink, and in what order.
/// Keeps the newest head it was told of.
struct LiveSink {
    events: UnboundedSender<Event>,
    head: Arc<AtomicU64>,
}

impl EnvelopeSink for LiveSink {
    async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
        if matches!(envelope.event, Event::AcceptedBlock(_)) {
            return Ok(());
        }
        self.events
            .send(envelope.event)
            .map_err(|_| SinkError::StorageClosed)
    }
    async fn flush(&mut self) -> Result<(), SinkError> {
        Ok(())
    }
    fn observe_head(&mut self, height: u64) {
        self.head.store(height, Ordering::Relaxed);
    }
}

/// A spawned live run over [`Source::linear`]`(6)` whose head starts at 1, so the
/// default start lands there: the live-only case, where startup seeds block 1 and the
/// run follows from it. [`Self::start`] returns once block 1 is forwarded.
pub(super) struct Live {
    chain: Arc<Mutex<Source>>,
    hints: UnboundedSender<BlockMeta>,
    pub(super) received: UnboundedReceiver<Event>,
    /// The newest head the sink was told of.
    pub(super) observed: Arc<AtomicU64>,
    run: JoinHandle<Result<(), PipelineError>>,
}

impl Live {
    pub(super) async fn start() -> Self {
        let (hints, hint_stream) = unbounded_channel();
        let (events, received) = unbounded_channel();
        let chain = Arc::new(Mutex::new(Source {
            head: 1,
            ..Source::linear(6)
        }));
        let observed = Arc::new(AtomicU64::new(0));
        let source = LiveSource {
            id: ChainId::new("ethereum"),
            chain: Arc::clone(&chain),
            hints: Mutex::new(Some(hint_stream)),
        };
        let sink = LiveSink {
            events,
            head: Arc::clone(&observed),
        };
        let run = tokio::spawn(Machine::new(source, sink, Vec::new()).run(None, None));
        let mut live = Self {
            chain,
            hints,
            received,
            observed,
            run,
        };
        assert_eq!(live.next_block("block 1").await.number, 1);
        live
    }

    /// The chain the run reads, to move between its reads.
    pub(super) fn chain(&self) -> MutexGuard<'_, Source> {
        self.chain.lock().expect("chain")
    }

    /// Announces the head at `height` on the subscription.
    pub(super) fn announce(&self, height: u64) {
        self.hints.send(meta(height)).expect("hint");
    }

    /// The next event the run forwarded, which must be a block; `what` names it in the
    /// failure.
    pub(super) async fn next_block(&mut self, what: &str) -> Box<Block> {
        match self.received.recv().await {
            Some(Event::Block(block)) => block,
            other => panic!("expected {what}, received {other:?}"),
        }
    }

    /// Ends the run by closing its subscription, checks that is why it ended, and hands
    /// back what it forwarded that was not read yet.
    pub(super) async fn close(self) -> UnboundedReceiver<Event> {
        drop(self.hints);
        fails_with(
            self.run.await.expect("driver task"),
            &PipelineError::SubscriptionClosed,
        );
        self.received
    }
}

/// Starts a machine at `from` and syncs it to the sampled head, as `run` does before
/// going live.
pub(super) async fn settle(machine: &mut Machine<Source, Sink>, from: Option<u64>) {
    machine.sync(from, None).await.expect("sync");
}

/// Moves the fake chain to `height` and feeds that head to the live phase, as a
/// notification or heartbeat would.
pub(super) async fn observe(machine: &mut Machine<Source, Sink>, height: u64) {
    chain(machine).head = height;
    let head = machine.source.head_meta();
    machine.process_head(head).await.expect("observe");
}

/// The fake chain, to edit between operations, when no fetch holds it.
pub(super) fn chain(machine: &mut Machine<Source, Sink>) -> &mut Source {
    Arc::get_mut(&mut machine.source).expect("no fetch in flight")
}

/// How often the fake chain was asked so far: head samples, then block reads.
pub(super) fn reads(machine: &Machine<Source, Sink>) -> (usize, usize) {
    let source = &machine.source;
    (
        source.head_calls.load(Ordering::Relaxed),
        source.block_calls.load(Ordering::Relaxed),
    )
}

/// Collects the heights of block events the sink accepted, in order.
pub(super) fn heights(machine: &Machine<Source, Sink>) -> Vec<u64> {
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

/// Asserts `result` failed with exactly `expected`, every field compared.
#[track_caller]
pub(super) fn fails_with<T: std::fmt::Debug>(
    result: Result<T, PipelineError>,
    expected: &PipelineError,
) {
    let error = result.expect_err("the operation fails");
    assert_eq!(format!("{error:?}"), format!("{expected:?}"));
}

fn source_malformed(detail: &str) -> SourceError {
    SourceError::Malformed {
        context: "test".into(),
        detail: detail.into(),
    }
}

/// The error the fake chain answers with, as the pipeline surfaces it.
pub(super) fn malformed(detail: &str) -> PipelineError {
    PipelineError::Source(source_malformed(detail))
}

/// The identities a store would hand back after committing `heights` of the linear
/// chain [`Source::linear`] builds.
pub(super) fn ledger(heights: std::ops::RangeInclusive<u64>) -> Vec<BlockMeta> {
    heights.map(meta).collect()
}

/// Replaces the fake chain from `from` upward with a branch whose hashes are offset
/// by 100, linked to the canonical block below `from`.
pub(super) fn fork(source: &mut Source, from: u64, through: u64) {
    for height in from..=through {
        let parent = if height == from {
            hash(height - 1)
        } else {
            hash(height + 99)
        };
        source.blocks.insert(height, (hash(height + 100), parent));
    }
}

pub(super) fn reorgs(machine: &Machine<Source, Sink>) -> Vec<&Reorg> {
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
