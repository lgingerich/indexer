// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#![expect(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::B256;
use tokio::sync::Notify;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinHandle;

use crate::sink::route::Backend;
use crate::sink::{EnvelopeSink, SinkError};
use crate::wire::datasets::evm::Block;
use crate::wire::envelope::{ChainId, Envelope, Event};

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
