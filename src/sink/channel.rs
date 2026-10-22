//! The channel from decode to storage: blocks of envelopes, in memory, bounded.
//!
//! Ingest and decode run as direct calls in one task; storage runs in another, and this
//! channel is the only hop between them. It exists for one reason: a store stalls (a
//! checkpoint, a slow fsync) and ingest must not stop reading heads while it does. The
//! channel absorbs the stall; nothing else about it is a queue's job.
//!
//! # The unit is a block
//!
//! [`ChannelSink`] collects the envelopes published between two flushes — which the
//! pipeline calls once per block — and sends them as one `Vec<Envelope>`. A block's raw
//! rows and its decoded rows therefore reach the store together and commit together,
//! never split across a crash.
//!
//! # Bounded, with backpressure
//!
//! The channel holds up to 1024 blocks and, whatever their count, about 131 thousand
//! envelopes. When either is full, [`ChannelSink::flush`] waits,
//! which stops ingest from taking the next head until storage catches up. That is
//! deliberate: the alternative is an unbounded buffer that hides lag and loses more on a
//! crash. Capacity times block time is how long a stall can last before ingest itself
//! feels it.
//!
//! # What it is not
//!
//! Not durable and not resumable: a crash loses whatever is in flight. That is safe only
//! because the chain and the store are the record — every block's message ends with its
//! `accepted_block` marker, which commits with the block's rows, so the store's ledger
//! says where to resume, and the node can serve the blocks after it. See
//! `crate::ingest::pipeline`.
//!
//! # Stopping
//!
//! A [`Stop`] ends the drain between commits: a commit under way finishes, then what is
//! gathered is committed too, and what the store's commits left running (a lake's last
//! ledger append) is waited for, the two given [`STOP_FLUSH`] together, and what is still
//! queued is dropped. A lake gathers up to its commit interval of blocks, so committing
//! them on a deploy saves fetching them all again. A commit that runs out of time is
//! abandoned, which every store survives as it survives a crash: nothing it wrote counts
//! until its ledger entry does, and the blocks are fetched again from the ledger's tip.
//!
//! Only a commit that awaits can be abandoned: the timeout takes effect when the commit
//! next yields. `PostgreSQL`'s and the lake's commits await the network throughout;
//! `DuckDB`'s, a development store, runs its statements without yielding, so its last
//! commit always runs to the end.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::time::Instant;
use tracing::{info, warn};

use super::progress::{self, HEAD_UNKNOWN, Progress};
use super::stop::Stop;
use crate::metrics;
use crate::sink::route::Backend;
use crate::sink::{EnvelopeSink, SinkError};
use crate::wire::envelope::Envelope;

/// How many blocks the channel holds before the sender waits.
///
/// A stall of `CAPACITY` block times is absorbed silently. Sized for backfill, where a
/// lake commit takes seconds and blocks arrive at a hundred or more a second: 32 filled
/// in a quarter second and held ingest for the rest of every commit. At the tip it
/// delays the moment a slow store's lag is felt, and a crash loses what is in it, which
/// the ledger refetches. Not a setting.
const CAPACITY: usize = 1024;

/// How many envelopes the channel holds before the sender waits, whatever the block
/// count.
///
/// Blocks differ a hundredfold: an early Robinhood block is about a hundred envelopes, an
/// Ethereum block over a thousand. Counted in blocks alone, [`CAPACITY`] of the large
/// ones held gigabytes. An envelope averages about 2 KB once stored, on both, so this is
/// about 256 MB. A block larger than this on its own still passes, alone.
const ENVELOPES: usize = 1 << 17;

/// The most blocks one flush gathers, whatever a store's own limits allow: half the
/// window of accepted blocks a store hands back to a restart.
///
/// A stop abandons what a store has gathered when its commit does not finish within
/// [`STOP_FLUSH`], and the stores commit on their own schedules, so one can be this far
/// behind another after a stop.
/// Resume needs a block both hand back, within that window; the lake's byte limit alone
/// let a commit gather over ten thousand of Robinhood's small blocks, and a stop left it
/// further behind the database than resume could reach.
const MAX_BLOCKS: usize = crate::ingest::pipeline::LEDGER_WINDOW / 2;

/// How long a stop gives the drain to commit what it has gathered and wait for what its
/// commits left running.
///
/// The host stops a container with `docker stop -t 30` (`stop_timeout` in the infra
/// repository's indexer stack) and kills it after that. This is half of it, counted from
/// when the drain sees the stop, which is after any commit under way: the rest covers
/// that commit and exiting. A lake's commit of its interval at the tip takes a few
/// seconds; a commit of a full buffer while catching up may not fit, and is then
/// abandoned, as is a last ledger append still running at the deadline.
const STOP_FLUSH: Duration = Duration::from_secs(15);

/// A block on its way to storage, holding its share of the envelope budget until the
/// drain takes it.
#[derive(Debug)]
struct Parcel {
    envelopes: Vec<Envelope>,
    /// When ingest handed the block over, which its [`wait`](Batching::wait) counts from:
    /// a block queued behind a slow commit has waited already.
    sent: Instant,
    _room: OwnedSemaphorePermit,
}

/// When the drain flushes: how many blocks it gathers into one commit.
///
/// A flush happens once the blocks gathered reach [`records`](Self::records) envelopes or
/// the store [reports](EnvelopeSink::buffered_bytes) [`bytes`](Self::bytes) buffered,
/// once the oldest of them has waited [`wait`](Self::wait), or when the channel closes.
/// A block is never split, so a flush may pass either limit by one block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Batching {
    /// The most envelopes one flush gathers.
    pub(crate) records: usize,
    /// The most bytes the store may hold unflushed, as it reports them.
    pub(crate) bytes: usize,
    /// The longest a gathered block waits for its flush, counted from when ingest handed
    /// it over, so time spent queued counts. Zero flushes as soon as what is already
    /// waiting is gathered.
    pub(crate) wait: Duration,
}

impl Batching {
    /// Flushes as soon as the blocks already waiting are gathered, up to `records`
    /// envelopes: one block per flush at the tip, the backlog in larger ones behind it.
    /// What the SQL stores commit by.
    #[cfg(any(feature = "duckdb", feature = "postgres", test))]
    pub(crate) const fn eager(records: usize) -> Self {
        Self {
            records,
            bytes: usize::MAX,
            wait: Duration::ZERO,
        }
    }
}

/// The sending half: buffers a block's envelopes and sends them on flush.
#[derive(Debug)]
pub(crate) struct ChannelSink {
    sender: mpsc::Sender<Parcel>,
    /// The envelope budget. A block's share travels with it and is freed when the drain
    /// has taken it.
    room: Arc<Semaphore>,
    batch: Vec<Envelope>,
    /// Newest canonical head ingest has sampled. Storage reads it when a commit lands.
    head: Arc<AtomicU64>,
    /// The store this sends to, as its lag is reported.
    store: Backend,
    /// The newest block the store has committed, or [`UNCOMMITTED`]: what a head sampled
    /// between commits measures the store's lag against.
    committed: Arc<AtomicU64>,
}

/// The store has committed no block since the channel was made.
const UNCOMMITTED: u64 = u64::MAX;

impl ChannelSink {
    /// Creates the channel: this sending half, which decode publishes into, and the
    /// receiving half `store` drains, whose commits it reports under that name.
    #[must_use]
    pub(crate) fn new(store: Backend) -> (Self, ChannelReceiver) {
        let (sender, receiver) = mpsc::channel(CAPACITY);
        let room = Arc::new(Semaphore::new(ENVELOPES));
        let head = Arc::new(AtomicU64::new(HEAD_UNKNOWN));
        let committed = Arc::new(AtomicU64::new(UNCOMMITTED));
        (
            Self {
                sender,
                room,
                batch: Vec::new(),
                head: Arc::clone(&head),
                store,
                committed: Arc::clone(&committed),
            },
            ChannelReceiver {
                receiver,
                store,
                head,
                committed,
                progress: Progress::default(),
            },
        )
    }
}

impl EnvelopeSink for ChannelSink {
    async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
        self.batch.push(envelope);
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), SinkError> {
        if self.batch.is_empty() {
            return Ok(());
        }
        // Hand the buffer over rather than copy it, leaving one already sized for the
        // next block: blocks vary in log count, but not enough for this to lose to
        // regrowing from zero.
        let sent = Instant::now();
        let next = Vec::with_capacity(self.batch.len());
        let batch = std::mem::replace(&mut self.batch, next);
        // A block over the whole budget takes all of it, and goes alone.
        let share = u32::try_from(batch.len().min(ENVELOPES)).unwrap_or(u32::MAX);
        // The semaphore is never closed, so this fails only if that changes; either way
        // storage cannot take the block.
        let room = Arc::clone(&self.room)
            .acquire_many_owned(share)
            .await
            .map_err(|_| SinkError::StorageClosed)?;
        // Storage has stopped. The batch of envelopes inside `SendError` is this
        // layer's own data and nothing else can use it, so the error is translated to
        // `StorageClosed` and `Vec<Envelope>` stops appearing in the sink's error type:
        // a caller matches the variant, and the channel's concrete type stays internal
        // to this module.
        self.sender
            .send(Parcel {
                envelopes: batch,
                sent,
                _room: room,
            })
            .await
            .map_err(|_| SinkError::StorageClosed)?;
        Ok(())
    }

    /// Also reports the store's lag against the new head: a commit reports it too, but a
    /// store that has stopped committing would otherwise go on showing its last, small,
    /// lag. Ingest samples heads whether or not storage keeps up, until the channel fills.
    fn observe_head(&mut self, height: u64) {
        self.head.store(height, Ordering::Release);
        let committed = self.committed.load(Ordering::Acquire);
        if committed != UNCOMMITTED {
            metrics::store_lag(self.store.name(), height.saturating_sub(committed));
        }
    }
}

/// The receiving half: what storage drains.
#[derive(Debug)]
pub(crate) struct ChannelReceiver {
    receiver: mpsc::Receiver<Parcel>,
    /// The store this drains into, as its commits are reported.
    store: Backend,
    head: Arc<AtomicU64>,
    committed: Arc<AtomicU64>,
    progress: Progress,
}

impl ChannelReceiver {
    /// Drains blocks into `sink` until the sending half is dropped or `stop` is
    /// requested, returning how many envelopes it stored.
    ///
    /// Blocks are gathered into one flush as [`Batching`] says. Each block arrives whole
    /// and is published at once, then whatever else is already waiting joins it while
    /// the flush is under its limits. Waiting for the next block is bounded by the
    /// oldest gathered block's deadline, counted from when ingest sent it, so neither a
    /// stalled chain nor a slow commit before it holds the block past its wait; with
    /// nothing gathered, it waits for as long as the chain does. When the sending half is
    /// dropped, whatever is gathered is flushed before the drain ends. When `stop` is
    /// requested, a flush under way finishes, what is gathered is flushed and what the
    /// flushes left running is waited for within [`STOP_FLUSH`] or abandoned, and what is
    /// queued is dropped (see the module docs).
    ///
    /// A successful flush is reported by the progress log: one line per commit near the
    /// sampled head, and one line per second while catching up.
    ///
    /// # Errors
    ///
    /// Returns an error when the sink cannot accept an envelope or flush. The blocks
    /// still in the channel are dropped, and the sending half fails on its next flush.
    pub(crate) async fn drain<K: EnvelopeSink>(
        mut self,
        sink: &mut K,
        batching: Batching,
        mut stop: Stop,
    ) -> Result<u64, SinkError> {
        let mut stored = 0_u64;
        let mut pending = Pending::default();
        // When the oldest gathered block must be flushed; `None` with nothing gathered.
        let mut deadline: Option<Instant> = None;
        loop {
            // A stop is checked first, so once it is requested neither a backlog nor a
            // due flush is worked off.
            let next = tokio::select! {
                biased;
                () = stop.wait() => break,
                next = recv_until(&mut self.receiver, deadline) => next,
            };
            let block = match next {
                Received::Block(block) => block,
                Received::Closed => break,
                Received::Due => {
                    stored += self.flush(sink, &mut pending).await?;
                    deadline = None;
                    continue;
                }
            };
            // The first block gathered is the oldest: the channel is in order.
            let due = *deadline.get_or_insert_with(|| block.sent + batching.wait);
            self.publish(sink, block, &mut pending).await?;
            while !full(sink, &pending, batching) {
                let Ok(block) = self.receiver.try_recv() else {
                    break;
                };
                self.publish(sink, block, &mut pending).await?;
            }
            if stop.requested() {
                break;
            }
            if full(sink, &pending, batching) || Instant::now() >= due {
                stored += self.flush(sink, &mut pending).await?;
                deadline = None;
            }
        }
        if stop.requested() {
            // The last commit and what it leaves running share one deadline, so a store
            // that hangs in either still lets the process exit before the host kills it.
            let deadline = Instant::now() + STOP_FLUSH;
            let gathered = pending.envelopes;
            if pending.blocks > 0 {
                if let Ok(flushed) =
                    tokio::time::timeout_at(deadline, self.flush(sink, &mut pending)).await
                {
                    stored += flushed?;
                } else {
                    info!(
                        uncommitted = gathered,
                        seconds = STOP_FLUSH.as_secs(),
                        "storage stopping on request; the last commit did not finish in time, and its blocks are fetched again on the next start"
                    );
                }
            }
            info!(
                committed = gathered - pending.envelopes,
                "storage stopping on request; blocks still queued are fetched again on the next start"
            );
            if let Ok(finished) = tokio::time::timeout_at(deadline, sink.finish()).await {
                finished?;
            } else {
                warn!(
                    seconds = STOP_FLUSH.as_secs(),
                    "storage stopping on request; what the last commit left running did not finish in time, and its blocks are fetched again on the next start"
                );
            }
        } else {
            if pending.blocks > 0 {
                stored += self.flush(sink, &mut pending).await?;
            }
            sink.finish().await?;
        }
        self.progress.finish(
            progress::sampled_head(self.head.load(Ordering::Acquire)),
            Instant::now(),
        );
        Ok(stored)
    }

    /// Hands one block's envelopes to `sink`, which buffers them against its own limits;
    /// the block's share of the channel's budget is freed once they are handed over.
    async fn publish<K: EnvelopeSink>(
        &mut self,
        sink: &mut K,
        block: Parcel,
        pending: &mut Pending,
    ) -> Result<(), SinkError> {
        pending.envelopes += block.envelopes.len();
        pending.blocks += 1;
        for envelope in block.envelopes {
            self.progress.note(&envelope);
            sink.publish(envelope).await?;
        }
        Ok(())
    }

    /// Flushes what is gathered and reports the commit, returning how many envelopes it
    /// held.
    async fn flush<K: EnvelopeSink>(
        &mut self,
        sink: &mut K,
        pending: &mut Pending,
    ) -> Result<u64, SinkError> {
        let started = Instant::now();
        sink.flush().await?;
        let elapsed = started.elapsed();
        let head = progress::sampled_head(self.head.load(Ordering::Acquire));
        let records = std::mem::take(pending).envelopes as u64;
        if let Some(height) =
            self.progress
                .committed(self.store, records, elapsed, head, Instant::now())
        {
            self.committed.store(height, Ordering::Release);
        }
        Ok(records)
    }
}

/// What waiting on the channel ended with.
enum Received {
    /// The next block.
    Block(Parcel),
    /// The sending half was dropped, and every block it sent was taken.
    Closed,
    /// The oldest gathered block's deadline passed first.
    Due,
}

/// The next block from `receiver`, waiting no later than `deadline` when there is one.
async fn recv_until(receiver: &mut mpsc::Receiver<Parcel>, deadline: Option<Instant>) -> Received {
    let next = match deadline {
        None => receiver.recv().await,
        Some(at) => match tokio::time::timeout_at(at, receiver.recv()).await {
            Ok(next) => next,
            Err(_) => return Received::Due,
        },
    };
    next.map_or(Received::Closed, Received::Block)
}

/// Whether what is gathered has reached a limit of `batching`.
fn full<K: EnvelopeSink>(sink: &K, pending: &Pending, batching: Batching) -> bool {
    pending.envelopes >= batching.records
        || pending.blocks >= MAX_BLOCKS
        || sink.buffered_bytes() >= batching.bytes
}

/// What the drain has gathered since its last flush.
#[derive(Debug, Default)]
struct Pending {
    /// Envelopes, as [`Batching::records`] counts them.
    envelopes: usize,
    /// Blocks, each arriving whole.
    blocks: usize,
}

#[cfg(test)]
mod tests;
