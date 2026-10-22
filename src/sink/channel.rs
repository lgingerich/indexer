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
            let next = match next {
                Received::Block(next) => next,
                Received::Due => {
                    stored += self.flush(sink, &mut pending).await?;
                    deadline = None;
                    continue;
                }
            };
            let Some(block) = next else {
                break;
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
    /// The next block, or `None` once the sending half is dropped.
    Block(Option<Parcel>),
    /// The oldest gathered block's deadline passed first.
    Due,
}

/// The next block from `receiver`, waiting no later than `deadline` when there is one.
async fn recv_until(receiver: &mut mpsc::Receiver<Parcel>, deadline: Option<Instant>) -> Received {
    match deadline {
        None => Received::Block(receiver.recv().await),
        Some(at) => tokio::time::timeout_at(at, receiver.recv())
            .await
            .map_or(Received::Due, Received::Block),
    }
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
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use alloy_primitives::B256;
    use tokio::sync::Notify;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::task::JoinHandle;

    use crate::sink::route::Backend;
    use crate::sink::{EnvelopeSink, SinkError};
    use crate::wire::envelope::{Block, ChainId, Envelope, Event};

    use super::{Batching, CAPACITY, ChannelReceiver, ChannelSink, ENVELOPES, MAX_BLOCKS};
    use crate::sink::stop::Stop;

    /// A block at `height`, which stands in for a block's worth of envelopes. The
    /// channel carries no field of its own, so a test needs a value to tell one
    /// envelope from another.
    fn envelope(height: u64) -> Envelope {
        Envelope::new(
            ChainId::new("base"),
            Event::Block(Box::new(Block {
                number: height,
                hash: B256::from(alloy_primitives::U256::from(height).to_be_bytes()),
                ..Block::default()
            })),
        )
    }

    /// Records what reached it as flushed batches, so a test sees the commit boundaries.
    ///
    /// Keyed on [`Event::dedupe_key`] rather than a field the fixture happens to set, so
    /// publishing some other event kind is recorded rather than panicked on. A fake that
    /// kills the process is a worse failure report than a wrong assertion, and the
    /// assertion is what should say so.
    #[derive(Default)]
    struct Batches {
        open: Vec<String>,
        flushed: Vec<Vec<String>>,
    }

    impl EnvelopeSink for Batches {
        async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
            self.open.push(envelope.event.dedupe_key());
            Ok(())
        }

        async fn flush(&mut self) -> Result<(), SinkError> {
            self.flushed.push(std::mem::take(&mut self.open));
            Ok(())
        }

        /// Ten bytes an envelope, so a test can state a byte limit in envelopes.
        fn buffered_bytes(&self) -> usize {
            self.open.len() * 10
        }
    }

    /// The keys [`envelope`] produces for these heights, so a test states what it
    /// expects to arrive without the fake having to unwrap the envelope to find out.
    fn keys(heights: impl IntoIterator<Item = u64>) -> Vec<String> {
        heights.into_iter().map(envelope_key).collect()
    }

    fn envelope_key(height: u64) -> String {
        envelope(height).event.dedupe_key()
    }

    /// Publishes one block's worth of markers and flushes, as the pipeline does.
    async fn send_block(sink: &mut ChannelSink, heights: std::ops::Range<u64>) {
        for height in heights {
            sink.publish(envelope(height)).await.expect("publish");
        }
        sink.flush().await.expect("flush");
    }

    /// Batching bounded only by `wait`: no record or byte limit gathers a flush early.
    const fn waiting(wait: Duration) -> Batching {
        Batching {
            records: usize::MAX,
            bytes: usize::MAX,
            wait,
        }
    }

    /// Drains `receiver` into a [`Batches`] on a task of its own, returning how many
    /// envelopes it stored and the batches it flushed.
    fn spawn_drain(
        receiver: ChannelReceiver,
        batching: Batching,
        stop: Stop,
    ) -> JoinHandle<(u64, Vec<Vec<String>>)> {
        tokio::spawn(async move {
            let mut store = Batches::default();
            let stored = receiver
                .drain(&mut store, batching, stop)
                .await
                .expect("drain");
            (stored, store.flushed)
        })
    }

    /// Whether `send` is still waiting once every task that could let it finish has run.
    ///
    /// Under a paused clock the runtime advances time only when no task can make
    /// progress, so a timeout that fires means `send` was waiting on something that would
    /// never come by itself, and one that does not means it finished without waiting on
    /// time at all. Neither depends on how fast the machine is.
    async fn waits(send: impl Future<Output = ()>) -> bool {
        tokio::time::timeout(Duration::from_secs(1), send)
            .await
            .is_err()
    }

    /// Nothing crosses the channel until the flush, and then the block crosses whole and
    /// in order: the flush is the block boundary.
    #[tokio::test]
    async fn a_block_crosses_whole_and_only_on_flush() {
        let (mut sink, mut receiver) = ChannelSink::new(Backend::DuckDb);
        sink.publish(envelope(0)).await.expect("publish");
        sink.publish(envelope(1)).await.expect("publish");
        assert!(
            matches!(receiver.receiver.try_recv(), Err(TryRecvError::Empty)),
            "an unflushed block must not reach storage"
        );
        sink.publish(envelope(2)).await.expect("publish");
        sink.flush().await.expect("flush");
        sink.flush().await.expect("an empty flush");
        drop(sink);

        let mut store = Batches::default();
        let stored = receiver
            .drain(&mut store, Batching::eager(100), Stop::never())
            .await
            .expect("drain");
        assert_eq!(stored, 3);
        assert_eq!(
            store.flushed,
            [keys(0..3)],
            "one block, and nothing for the empty flush"
        );
    }

    /// Large blocks wait on the envelope budget long before the block count is reached,
    /// get room back as storage takes them, and one larger than the whole budget still
    /// goes, alone.
    #[tokio::test(start_paused = true)]
    async fn the_envelope_budget_holds_back_large_blocks() {
        let half = u64::try_from(ENVELOPES / 2 + 1).expect("fits");
        let (mut sink, mut receiver) = ChannelSink::new(Backend::DuckDb);
        send_block(&mut sink, 0..half).await;
        assert!(
            waits(send_block(&mut sink, half..2 * half)).await,
            "a second block over the budget must wait"
        );
        let first = receiver.receiver.recv().await.expect("the first block");
        assert_eq!(first.envelopes.len(), ENVELOPES / 2 + 1);
        drop(first);
        assert!(
            !waits(send_block(&mut sink, half..2 * half)).await,
            "room again once storage took the first"
        );

        let (mut sink, mut receiver) = ChannelSink::new(Backend::DuckDb);
        let whole = u64::try_from(ENVELOPES + 1).expect("fits");
        assert!(
            !waits(send_block(&mut sink, 0..whole)).await,
            "a block over the whole budget goes"
        );
        assert!(
            waits(send_block(&mut sink, whole..whole + 1)).await,
            "and goes alone: it holds the whole budget"
        );
        let large = receiver.receiver.recv().await.expect("the large block");
        assert_eq!(large.envelopes.len(), ENVELOPES + 1, "whole, never split");
        drop(large);
        assert!(!waits(send_block(&mut sink, whole..whole + 1)).await);
    }

    /// A store that fell behind commits its backlog in one flush, but never splits a
    /// block: the bound is checked between blocks.
    #[tokio::test]
    async fn a_backlog_shares_a_flush_and_blocks_are_never_split() {
        let (mut sink, receiver) = ChannelSink::new(Backend::DuckDb);
        send_block(&mut sink, 0..3).await;
        send_block(&mut sink, 3..6).await;
        send_block(&mut sink, 6..9).await;
        drop(sink);

        // Bound of 4: the first block (3) is under it, so the second joins (6); the
        // third would start past the bound and gets its own flush.
        let mut store = Batches::default();
        let stored = receiver
            .drain(&mut store, Batching::eager(4), Stop::never())
            .await
            .expect("drain");
        assert_eq!(stored, 9);
        assert_eq!(store.flushed, [keys(0..6), keys(6..9)]);
    }

    /// With a wait, blocks are gathered until the oldest has waited that long, even when
    /// no block follows to notice it: a stalled chain still flushes on time. What is
    /// gathered when the channel closes is flushed too.
    #[tokio::test(start_paused = true)]
    async fn a_gathered_block_is_flushed_by_its_deadline_in_a_stall() {
        let (mut sink, receiver) = ChannelSink::new(Backend::DuckDb);
        let storage = spawn_drain(receiver, waiting(Duration::from_secs(30)), Stop::never());
        send_block(&mut sink, 0..1).await;
        tokio::time::sleep(Duration::from_secs(10)).await;
        send_block(&mut sink, 1..2).await;
        // The chain stalls past the first block's deadline.
        tokio::time::sleep(Duration::from_mins(1)).await;
        send_block(&mut sink, 2..3).await;
        drop(sink);

        let (stored, flushed) = storage.await.expect("storage ends");
        assert_eq!(stored, 3);
        assert_eq!(flushed, [keys(0..2), keys(2..3)]);
    }

    /// A block's wait counts from when ingest sent it, so one queued behind a slow commit
    /// is flushed as soon as that commit ends, rather than waiting its whole wait again.
    #[tokio::test(start_paused = true)]
    async fn a_block_queued_behind_a_slow_commit_has_already_waited() {
        /// Takes five seconds a flush, and records when each began, after `start`.
        struct Slow {
            start: tokio::time::Instant,
            began: Vec<Duration>,
        }

        impl EnvelopeSink for Slow {
            async fn publish(&mut self, _envelope: Envelope) -> Result<(), SinkError> {
                Ok(())
            }

            async fn flush(&mut self) -> Result<(), SinkError> {
                self.began.push(self.start.elapsed());
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok(())
            }
        }

        let (mut sink, receiver) = ChannelSink::new(Backend::DuckDb);
        let mut store = Slow {
            start: tokio::time::Instant::now(),
            began: Vec::new(),
        };
        let storage = tokio::spawn(async move {
            receiver
                .drain(&mut store, waiting(Duration::from_secs(1)), Stop::never())
                .await
                .expect("drain");
            store.began
        });
        send_block(&mut sink, 0..1).await;
        // Sent while the first block's commit runs, from 1 s to 6 s.
        tokio::time::sleep(Duration::from_secs(2)).await;
        send_block(&mut sink, 1..2).await;
        tokio::time::sleep(Duration::from_secs(10)).await;
        drop(sink);

        assert_eq!(
            storage.await.expect("storage ends"),
            [Duration::from_secs(1), Duration::from_secs(6)],
            "the second block was due at 3 s, so it commits once the first commit ends"
        );
    }

    /// Past the store's byte limit, what is gathered flushes at once, long before its
    /// wait; a block is never split to stay under it.
    #[tokio::test]
    async fn a_store_past_its_byte_limit_flushes_early() {
        let (mut sink, receiver) = ChannelSink::new(Backend::DuckDb);
        for height in 0..4 {
            send_block(&mut sink, height..height + 1).await;
        }
        drop(sink);

        // Ten bytes an envelope: the third crosses 25.
        let batching = Batching {
            bytes: 25,
            ..waiting(Duration::from_hours(1))
        };
        let mut store = Batches::default();
        receiver
            .drain(&mut store, batching, Stop::never())
            .await
            .expect("drain");
        assert_eq!(store.flushed, [keys(0..3), keys(3..4)]);
    }

    /// However much a store's own limits would gather, one flush holds at most
    /// [`MAX_BLOCKS`] blocks, so a stop can leave the stores no further apart than a
    /// restart reaches back.
    #[tokio::test]
    async fn a_flush_holds_at_most_half_a_window_of_blocks() {
        let max = MAX_BLOCKS as u64;
        let (mut sink, receiver) = ChannelSink::new(Backend::DuckDb);
        let storage = spawn_drain(receiver, waiting(Duration::from_hours(1)), Stop::never());
        for height in 0..max + 10 {
            send_block(&mut sink, height..height + 1).await;
        }
        drop(sink);
        let (_, flushed) = storage.await.expect("storage");
        assert_eq!(flushed, [keys(0..max), keys(max..max + 10)]);
    }

    /// The channel is bounded, so a stalled store is felt as a waiting sender rather
    /// than as unbounded growth or dropped blocks: it holds [`CAPACITY`] blocks, the next
    /// waits until storage takes one, and none is lost.
    #[tokio::test(start_paused = true)]
    async fn a_full_channel_blocks_the_sender() {
        let capacity = CAPACITY as u64;
        let (mut sink, mut receiver) = ChannelSink::new(Backend::DuckDb);
        for height in 0..capacity {
            assert!(
                !waits(send_block(&mut sink, height..height + 1)).await,
                "block {height} fits in the channel"
            );
        }
        assert!(
            waits(send_block(&mut sink, capacity..capacity + 1)).await,
            "a full channel must make the sender wait"
        );
        let oldest = receiver.receiver.recv().await.expect("the oldest block");
        assert_eq!(oldest.envelopes.len(), 1);
        assert_eq!(oldest.envelopes[0].event.dedupe_key(), envelope_key(0));
        assert!(
            !waits(send_block(&mut sink, capacity..capacity + 1)).await,
            "room again once storage took one"
        );
        drop(sink);

        let mut store = Batches::default();
        receiver
            .drain(&mut store, Batching::eager(usize::MAX), Stop::never())
            .await
            .expect("drain");
        assert_eq!(
            store.flushed.concat(),
            keys(1..=capacity),
            "every other block arrives, in order"
        );
    }

    /// A store's lag is measured against each head ingest samples, not only on commit, so
    /// a store that stops committing reports a lag that grows.
    #[tokio::test]
    async fn a_store_that_stops_committing_reports_a_growing_lag() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let scrape = recorder.handle();
        // Thread-local, and the test runtime runs the drain on this thread.
        let _recorder = metrics::set_default_local_recorder(&recorder);
        let lag = || {
            scrape
                .render()
                .lines()
                .find_map(|line| line.strip_prefix("indexer_store_lag_blocks{store=\"duckdb\"} "))
                .map(str::to_owned)
        };
        let (mut sink, receiver) = ChannelSink::new(Backend::DuckDb);
        sink.observe_head(10);
        assert_eq!(lag(), None, "no lag before the store's first commit");

        let storage = spawn_drain(receiver, Batching::eager(1), Stop::never());
        send_block(&mut sink, 5..6).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while lag().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the block commits");
        assert_eq!(lag().as_deref(), Some("5"), "the commit measures it");
        sink.observe_head(30);
        assert_eq!(
            lag().as_deref(),
            Some("25"),
            "a head sampled with no commit since measures it"
        );
        drop(sink);
        storage.await.expect("storage ends");
    }

    /// A store that has stopped is an error on the sender's next flush, not a silent
    /// drop: ingest must not keep indexing into nothing.
    ///
    /// The variant is matched, not the message. This used to assert the error was tokio's
    /// `SendError<Vec<Envelope>>` by downcasting, which required the error to be a
    /// type-erased one to downcast at all; matching `StorageClosed` states the contract
    /// directly and keeps the channel's own types out of the assertion.
    #[tokio::test]
    async fn a_stopped_store_fails_the_next_flush() {
        let (mut sink, receiver) = ChannelSink::new(Backend::DuckDb);
        drop(receiver);
        sink.publish(envelope(0)).await.expect("publish buffers");
        let error = sink.flush().await.expect_err("nobody is reading");
        assert!(
            matches!(error, SinkError::StorageClosed),
            "a dead store must be reported as such, not as a raw channel error: {error}"
        );
    }

    /// A stop requested while a commit is under way lets it finish, then ends the drain
    /// without flushing the block queued behind it, and still waits on what the commit
    /// left running.
    #[tokio::test]
    async fn a_stop_finishes_the_commit_under_way_and_drops_the_rest() {
        /// Records like [`Batches`], but holds each flush until released.
        struct Gated {
            inner: Batches,
            entered: Arc<Notify>,
            release: Arc<Notify>,
            finished: bool,
        }

        impl EnvelopeSink for Gated {
            async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
                self.inner.publish(envelope).await
            }

            async fn flush(&mut self) -> Result<(), SinkError> {
                self.entered.notify_one();
                self.release.notified().await;
                self.inner.flush().await
            }

            async fn finish(&mut self) -> Result<(), SinkError> {
                self.finished = true;
                Ok(())
            }
        }

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (mut sink, receiver) = ChannelSink::new(Backend::DuckDb);
        let (request, stop) = Stop::new();
        let mut store = Gated {
            inner: Batches::default(),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            finished: false,
        };
        let storage = tokio::spawn(async move {
            let stored = receiver
                .drain(&mut store, Batching::eager(1), stop)
                .await
                .expect("drain");
            (stored, store)
        });
        send_block(&mut sink, 0..1).await;
        entered.notified().await;
        send_block(&mut sink, 1..2).await;
        request.send(true).expect("the drain is listening");
        release.notify_one();

        let (stored, store) = storage.await.expect("storage ends");
        assert_eq!(stored, 1);
        assert_eq!(store.inner.flushed, [keys(0..1)]);
        assert!(
            store.inner.open.is_empty(),
            "the queued block is dropped, not published"
        );
        assert!(
            store.finished,
            "what the last commit left running is waited on"
        );
    }

    /// A stop commits what a store with a commit interval has gathered, rather than
    /// leaving it to be fetched again, and drops nothing it had in hand.
    #[tokio::test(start_paused = true)]
    async fn a_stop_commits_what_is_gathered() {
        let (mut sink, receiver) = ChannelSink::new(Backend::DuckDb);
        let (request, stop) = Stop::new();
        let storage = spawn_drain(receiver, waiting(Duration::from_hours(1)), stop);
        send_block(&mut sink, 0..1).await;
        send_block(&mut sink, 1..2).await;
        // The clock is paused, so this returns once the drain has gathered both and is
        // waiting again, far short of their wait.
        tokio::time::sleep(Duration::from_secs(1)).await;
        request.send(true).expect("the drain is listening");

        let (stored, flushed) = storage.await.expect("storage ends");
        assert_eq!(stored, 2);
        assert_eq!(flushed, [keys(0..2)]);
    }

    /// A stop's commit that outlasts [`STOP_FLUSH`](super::STOP_FLUSH) is abandoned, so
    /// the process exits before the supervisor kills it, and what the commits before it
    /// left running is still waited on.
    #[tokio::test(start_paused = true)]
    async fn a_stop_abandons_a_commit_that_runs_out_of_time() {
        /// Never finishes a flush.
        #[derive(Default)]
        struct Stuck {
            finished: bool,
        }

        impl EnvelopeSink for Stuck {
            async fn publish(&mut self, _envelope: Envelope) -> Result<(), SinkError> {
                Ok(())
            }

            async fn flush(&mut self) -> Result<(), SinkError> {
                std::future::pending().await
            }

            async fn finish(&mut self) -> Result<(), SinkError> {
                self.finished = true;
                Ok(())
            }
        }

        let (mut sink, receiver) = ChannelSink::new(Backend::DuckDb);
        let (request, stop) = Stop::new();
        let storage = tokio::spawn(async move {
            let mut store = Stuck::default();
            let stored = receiver
                .drain(&mut store, waiting(Duration::from_hours(1)), stop)
                .await
                .expect("an abandoned commit is not an error");
            (stored, store.finished)
        });
        send_block(&mut sink, 0..1).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        request.send(true).expect("the drain is listening");

        let started = tokio::time::Instant::now();
        let (stored, finished) = storage.await.expect("storage ends");
        assert_eq!(stored, 0);
        assert!(finished, "what earlier commits left running is waited on");
        assert_eq!(started.elapsed(), super::STOP_FLUSH);
    }

    /// On a stop, waiting for what the commits left running, such as a lake's last ledger
    /// append on a hung connection, is bounded by [`STOP_FLUSH`](super::STOP_FLUSH) too,
    /// and abandoning it is not an error.
    #[tokio::test(start_paused = true)]
    async fn a_stop_abandons_what_a_commit_left_running_once_out_of_time() {
        /// Commits at once, but never finishes what it left running.
        struct Hung(Batches);

        impl EnvelopeSink for Hung {
            async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
                self.0.publish(envelope).await
            }

            async fn flush(&mut self) -> Result<(), SinkError> {
                self.0.flush().await
            }

            async fn finish(&mut self) -> Result<(), SinkError> {
                std::future::pending().await
            }
        }

        let (mut sink, receiver) = ChannelSink::new(Backend::DuckDb);
        let (request, stop) = Stop::new();
        let storage = tokio::spawn(async move {
            let mut store = Hung(Batches::default());
            let stored = receiver
                .drain(&mut store, Batching::eager(1), stop)
                .await
                .expect("an abandoned wait is not an error");
            (stored, store.0.flushed)
        });
        send_block(&mut sink, 0..1).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        request.send(true).expect("the drain is listening");

        let started = tokio::time::Instant::now();
        let (stored, flushed) = storage.await.expect("storage ends");
        assert_eq!((stored, flushed), (1, vec![keys(0..1)]));
        assert_eq!(started.elapsed(), super::STOP_FLUSH);
    }

    /// A sink error ends the drain with that error, so a failing store stops storage
    /// rather than being read as an ended stream, and the sender fails on its next flush.
    #[tokio::test]
    async fn a_failing_sink_ends_the_drain_with_its_error() {
        /// A sink whose storage is out of space, which is a real store condition and so
        /// carries the same [`SinkError::Write`] a `stdout` failure would.
        struct OutOfSpace;

        impl EnvelopeSink for OutOfSpace {
            async fn publish(&mut self, _envelope: Envelope) -> Result<(), SinkError> {
                Err(SinkError::Write(std::io::Error::other("disk is full")))
            }
        }

        let (mut sink, receiver) = ChannelSink::new(Backend::DuckDb);
        send_block(&mut sink, 0..1).await;
        let error = receiver
            .drain(&mut OutOfSpace, Batching::eager(100), Stop::never())
            .await
            .expect_err("the sink failed");
        let SinkError::Write(io) = error else {
            panic!("the sink's own error must survive the drain, not be reshaped: {error}");
        };
        assert_eq!(
            (io.kind(), io.to_string()),
            (std::io::ErrorKind::Other, "disk is full".to_owned())
        );
        sink.publish(envelope(1)).await.expect("publish buffers");
        assert!(
            matches!(sink.flush().await, Err(SinkError::StorageClosed)),
            "the sender fails on its next flush once the drain has ended"
        );
    }
}
