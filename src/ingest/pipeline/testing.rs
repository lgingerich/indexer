//! The fake chain and sink the pipeline's tests drive a machine with.

#![expect(clippy::expect_used)]

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use alloy_primitives::B256;
use futures_util::stream;

use super::{MAX_UNFINALIZED_BLOCKS, Machine, PipelineError};
use crate::ingest::source::{BlockSource, FetchedBlock, HeadStream, SourceError};
use crate::sink::{EnvelopeSink, SinkError};
use crate::wire::envelope::{Block, BlockMeta, ChainId, Envelope, Event, Reorg};

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

/// A deterministic chain source. `head` moves when `fork` or a test edits it,
/// `head_calls` counts discovery so a test can bound what backfill makes, and
/// `block_calls` counts full block fetches.
pub(super) struct Source {
    pub(super) chain: ChainId,
    pub(super) data: Data,
}

pub(super) struct Data {
    pub(super) blocks: HashMap<u64, (B256, B256)>,
    pub(super) head: u64,
    pub(super) fail_once: AtomicBool,
    /// Heads the next samples return, in order, before `head`.
    pub(super) samples: std::sync::Mutex<VecDeque<u64>>,
    pub(super) head_calls: AtomicUsize,
    pub(super) block_calls: AtomicUsize,
    /// Header reads at a height, as the fork walk and its rechecks make.
    pub(super) header_calls: AtomicUsize,
    /// The heights backfill asks for in one `fetch_blocks`.
    pub(super) range: u64,
    /// The `fetch_blocks` calls backfill keeps in flight.
    pub(super) concurrency: usize,
    /// Answer each `fetch_blocks` one block short.
    pub(super) short: bool,
    /// Every range answers after a second, and the one starting at this height after
    /// two, so the ranges above it answer first.
    pub(super) slow: Option<u64>,
    /// `fetch_blocks` calls in flight now, and the most there ever were.
    pub(super) in_flight: AtomicUsize,
    pub(super) peak: AtomicUsize,
    /// Whether the source follows heads, or only backfills.
    pub(super) follows: bool,
    /// The source's finality depth.
    pub(super) finality: u64,
    /// Whether each `fetch_block` waits a moment, so fetches made ahead overlap.
    pub(super) slow_blocks: bool,
    /// How many more times the block at each height is answered inconsistently.
    pub(super) inconsistent: std::sync::Mutex<HashMap<u64, u32>>,
    /// `fetch_block` calls in flight now, and the most there ever were.
    pub(super) blocks_in_flight: AtomicUsize,
    pub(super) blocks_peak: AtomicUsize,
}

impl Source {
    /// Builds a canonical chain from genesis through `head`.
    pub(super) fn linear(head: u64) -> Self {
        Self {
            chain: ChainId::new("ethereum"),
            data: Data {
                blocks: (0..=head)
                    .map(|h| (h, (hash(h), hash(h.saturating_sub(1)))))
                    .collect(),
                head,
                fail_once: AtomicBool::new(false),
                samples: std::sync::Mutex::default(),
                head_calls: AtomicUsize::new(0),
                block_calls: AtomicUsize::new(0),
                header_calls: AtomicUsize::new(0),
                range: 1,
                concurrency: 1,
                short: false,
                slow: None,
                in_flight: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                follows: true,
                finality: MAX_UNFINALIZED_BLOCKS as u64,
                slow_blocks: false,
                inconsistent: std::sync::Mutex::default(),
                blocks_in_flight: AtomicUsize::new(0),
                blocks_peak: AtomicUsize::new(0),
            },
        }
    }

    /// [`Self::linear`], as a source that only backfills.
    pub(super) fn backfilling(head: u64) -> Self {
        let mut source = Self::linear(head);
        source.data.follows = false;
        source
    }

    /// The metadata a source would announce for its current head.
    pub(super) fn head_meta(&self) -> BlockMeta {
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
    fn follows_heads(&self) -> bool {
        self.data.follows
    }
    async fn subscribe_heads(&self) -> Result<HeadStream, SourceError> {
        Ok(Box::pin(stream::pending()))
    }
    async fn fetch_header(&self, height: Option<u64>) -> Result<BlockMeta, SourceError> {
        if height.is_some() {
            self.data.header_calls.fetch_add(1, Ordering::Relaxed);
        }
        let height = height.unwrap_or_else(|| {
            self.data.head_calls.fetch_add(1, Ordering::Relaxed);
            let sample = self.data.samples.lock().expect("samples").pop_front();
            sample.unwrap_or(self.data.head)
        });
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
        let data = &self.data;
        let now = data.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        data.peak.fetch_max(now, Ordering::Relaxed);
        if let Some(slow) = data.slow {
            let seconds = if from == slow { 2 } else { 1 };
            tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
        }
        let mut blocks = Vec::new();
        let mut result = Ok(());
        for height in from..=to {
            match self.fetch_block(height, None).await {
                Ok(block) => blocks.push(block),
                Err(error) => {
                    result = Err(error);
                    break;
                }
            }
        }
        data.in_flight.fetch_sub(1, Ordering::Relaxed);
        result?;
        if data.short {
            blocks.pop();
        }
        Ok(blocks)
    }
    fn backfill_span(&self) -> u64 {
        self.data.range
    }
    fn backfill_concurrency(&self) -> usize {
        self.data.concurrency
    }
    fn finality_depth(&self) -> u64 {
        self.data.finality
    }
    async fn fetch_block(
        &self,
        height: u64,
        _head: Option<&BlockMeta>,
    ) -> Result<FetchedBlock, SourceError> {
        let data = &self.data;
        data.block_calls.fetch_add(1, Ordering::Relaxed);
        if data.slow_blocks {
            let now = data.blocks_in_flight.fetch_add(1, Ordering::Relaxed) + 1;
            data.blocks_peak.fetch_max(now, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            data.blocks_in_flight.fetch_sub(1, Ordering::Relaxed);
        }
        if let Some(left) = data
            .inconsistent
            .lock()
            .expect("inconsistent")
            .get_mut(&height)
            .filter(|left| **left > 0)
        {
            *left -= 1;
            return Err(SourceError::Inconsistent {
                context: "test".into(),
                detail: "a backend behind the head".into(),
            });
        }
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

/// Starts a machine at `from` and syncs it to the sampled head, as `run` does before
/// going live.
pub(super) async fn settle(machine: &mut Machine<Source, Sink>, from: Option<u64>) {
    machine.sync(from, None).await.expect("sync");
}

/// Moves the fake chain to `height` and feeds that head to the live phase, as a
/// notification or heartbeat would.
pub(super) async fn observe(machine: &mut Machine<Source, Sink>, height: u64) {
    chain(machine).data.head = height;
    let head = machine.source.head_meta();
    machine.process_head(head).await.expect("observe");
}

/// The fake chain, to edit between operations, when no fetch holds it.
pub(super) fn chain(machine: &mut Machine<Source, Sink>) -> &mut Source {
    Arc::get_mut(&mut machine.source).expect("no fetch in flight")
}

/// How often the fake chain was asked so far: head samples, then block fetches.
pub(super) fn reads(machine: &Machine<Source, Sink>) -> (usize, usize) {
    let data = &machine.source.data;
    (
        data.head_calls.load(Ordering::Relaxed),
        data.block_calls.load(Ordering::Relaxed),
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

/// The error the fake chain answers with, as the pipeline surfaces it.
pub(super) fn malformed(detail: &str) -> PipelineError {
    PipelineError::Source(SourceError::Malformed {
        context: "test".into(),
        detail: detail.into(),
    })
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
        source
            .data
            .blocks
            .insert(height, (hash(height + 100), parent));
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
