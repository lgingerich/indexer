//! Fetching ahead: requests in flight while the blocks below them are accepted.

use std::pin::Pin;
use std::task::{Context, Poll, ready};

use futures_util::StreamExt as _;
use futures_util::stream::FuturesOrdered;

use super::PipelineError;

/// Consecutive ranges of heights, requested ahead and answered in height order.
///
/// Backfill, catching up to a head, and replaying a fork all accept blocks one height
/// after another, and each block costs the source a round trip, so they keep up to
/// `ahead` requests in flight while they accept the answers below them. Each request
/// is its own task, so responses are read and parsed on every worker while the caller
/// accepts blocks, not only while it waits on the next one. Dropping the prefetcher
/// aborts what it still has in flight: a fetch whose answer will not be accepted stops
/// reading the source rather than finishing for nothing.
pub(super) struct Prefetch<T> {
    fetches: FuturesOrdered<Spawned<(u64, u64, T)>>,
    /// The first height not yet requested.
    cut: u64,
    /// The most requests in flight at once; at least one.
    ahead: usize,
}

impl<T: Send + 'static> Prefetch<T> {
    /// A prefetcher whose first request starts at `from`, with up to `ahead` in flight.
    pub(super) fn new(from: u64, ahead: usize) -> Self {
        Self {
            fetches: FuturesOrdered::new(),
            cut: from,
            ahead: ahead.max(1),
        }
    }

    /// Requests ranges from the first height not yet requested through `end`, until
    /// `ahead` are in flight. Each covers `span()` heights, at least one, read as it is
    /// cut so a source can change it as it goes; `fetch(from, to)` makes the request for
    /// `from ..= to`.
    ///
    /// # Errors
    /// Returns [`PipelineError::HeightOverflow`] when a range ends at the last height.
    pub(super) fn fill<F>(
        &mut self,
        end: u64,
        span: impl Fn() -> u64,
        mut fetch: impl FnMut(u64, u64) -> F,
    ) -> Result<(), PipelineError>
    where
        F: Future<Output = T> + Send + 'static,
    {
        while self.fetches.len() < self.ahead && self.cut <= end {
            let from = self.cut;
            let to = end.min(from.saturating_add(span().max(1) - 1));
            let request = fetch(from, to);
            self.fetches.push_back(Spawned(tokio::spawn(
                async move { (from, to, request.await) },
            )));
            self.cut = to
                .checked_add(1)
                .ok_or(PipelineError::HeightOverflow { height: to })?;
        }
        Ok(())
    }

    /// The lowest range in flight, `(from, to, answer)`, once it is answered; `None` with
    /// nothing in flight.
    pub(super) async fn next(&mut self) -> Option<(u64, u64, T)> {
        self.fetches.next().await
    }
}

/// A spawned task, aborted when dropped.
struct Spawned<T>(tokio::task::JoinHandle<T>);

impl<T> Future for Spawned<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        match ready!(Pin::new(&mut self.0).poll(cx)) {
            Ok(output) => Poll::Ready(output),
            // Only a drop aborts the task, and a dropped one is never polled, so this is
            // a panic inside it: it surfaces here, as it would have inline.
            Err(error) => std::panic::resume_unwind(error.into_panic()),
        }
    }
}

impl<T> Drop for Spawned<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
