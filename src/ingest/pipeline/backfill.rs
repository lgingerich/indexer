//! Backfill: the buried heights, fetched in ranges ahead and accepted in order.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use futures_util::StreamExt as _;
use futures_util::stream::FuturesOrdered;

use super::{Machine, PipelineError};
use crate::ingest::source::{BlockSource, FetchedBlock};
use crate::sink::EnvelopeSink;
use crate::wire::envelope::BlockMeta;

/// A spawned task, aborted when dropped: a backfill range that will not be accepted
/// stops reading the source rather than finishing for nothing.
pub(super) struct Spawned<T>(pub(super) tokio::task::JoinHandle<T>);

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

impl<S: BlockSource + 'static, K: EnvelopeSink> Machine<S, K> {
    /// Delivers a block fetched for `height` and records its identity.
    ///
    /// The link to the accepted tip is checked first and the identity is recorded after
    /// delivery succeeds, so a failure leaves the accepted history untouched. The first
    /// block of a fresh run has no predecessor to link onto and is accepted as-is; a
    /// resumed run links its first block to the restored tip. A link
    /// failure here is a source fault: this is the buried path, where no fork is
    /// reachable.
    async fn accept(&mut self, height: u64, fetched: FetchedBlock) -> Result<(), PipelineError> {
        let meta = Self::marker(fetched.meta, height)?;
        if let Some(tip) = self.ring.tip() {
            Self::validate_link(tip, meta)?;
        }
        self.commit(meta, fetched.events).await
    }

    /// Fetches and publishes the buried heights from `next`, and returns the height after
    /// the last with the head last sampled. No reorg handling: no fork this deep is
    /// reachable, and invalid linkage is a source fault rather than a fork to replay.
    ///
    /// Buried means at or below `head - window`, and at or below `stop`. Reaching that
    /// bound, the head is sampled again, and backfill continues while the chain has
    /// buried more; a bound at the stop has nothing more to wait for.
    ///
    /// Ranges of [`BlockSource::backfill_span`] heights are requested ahead, up to
    /// [`BlockSource::backfill_concurrency`] at once, and accepted strictly in order, so
    /// the source is read while the sink writes. A failed range surfaces only after every
    /// range below it is accepted; the ranges above it are dropped unpublished, and the
    /// accepted history ends where the failure began.
    ///
    /// # Errors
    /// Returns [`PipelineError::InvalidRange`] when the source returns other than one
    /// block per requested height, before any of that range is published.
    pub(super) async fn backfill(
        &mut self,
        mut next: u64,
        mut head: BlockMeta,
        stop: Option<u64>,
    ) -> Result<(u64, BlockMeta), PipelineError> {
        let depth = self.finality_depth();
        let bound = |head| {
            stop.map_or_else(
                || Self::buried(head, depth),
                |stop| Self::buried(head, depth).min(stop),
            )
        };
        let mut end = bound(head);
        if next > end {
            return Ok((next, head));
        }
        let source = Arc::clone(&self.source);
        let concurrency = source.backfill_concurrency().max(1);
        let mut fetches = FuturesOrdered::new();
        // The first height not yet requested; `next` is the first not yet accepted.
        let mut cut = next;
        loop {
            while fetches.len() < concurrency && cut <= end {
                let span = source.backfill_span().max(1);
                let last = end.min(cut.saturating_add(span - 1));
                // Its own task, so responses are read and parsed on every worker while
                // this one accepts blocks, not only while it waits on the next range.
                let source = Arc::clone(&source);
                let fetch = Spawned(tokio::spawn(
                    async move { source.fetch_blocks(cut, last).await },
                ));
                fetches.push_back(async move { (cut, last, fetch.await) });
                cut = Self::next(last)?;
            }
            let Some((from, to, answer)) = fetches.next().await else {
                // Everything requested is accepted, through `end`.
                if stop == Some(end) {
                    return Ok((next, head));
                }
                head = self.sample_head().await?;
                end = bound(head);
                if next > end {
                    return Ok((next, head));
                }
                continue;
            };
            let blocks = answer?;
            if u64::try_from(blocks.len()).ok() != Some(to - from + 1) {
                return Err(PipelineError::InvalidRange {
                    from,
                    to,
                    returned: blocks.len(),
                });
            }
            for (height, block) in (from..=to).zip(blocks) {
                self.accept(height, block).await?;
            }
            next = Self::next(to)?;
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::ingest::pipeline::MAX_UNFINALIZED_BLOCKS;
    use crate::ingest::pipeline::testing::*;
    use crate::sink::SinkError;

    #[tokio::test]
    async fn backfill_covers_buried_heights_and_samples_the_head_again_at_the_bound() {
        // A head beyond the window leaves buried history for backfill to index.
        // The chain advances by ten while backfilling: the first sample buries 0..=5, the
        // second 0..=15, and the third finds nothing new. The heights buried meanwhile are
        // backfilled too, rather than left to the reorg-aware path a height at a time.
        let moved = MAX_UNFINALIZED_BLOCKS as u64 + 15;
        let source = Source::linear(moved);
        source
            .data
            .samples
            .lock()
            .expect("samples")
            .push_back(moved - 10);
        let mut machine = Machine::new(source, Sink::default(), Vec::new());
        settle(&mut machine, Some(0)).await;
        assert_eq!(heights(&machine), (0..=moved).collect::<Vec<_>>());
        assert_eq!(
            reads(&machine),
            (3, usize::try_from(moved + 1).expect("count")),
            "backfill discovers at startup and at each buried bound only, and fetches each \
             height once"
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
        settle(&mut machine, Some(0)).await;
        assert_eq!(heights(&machine), (0..=head).collect::<Vec<_>>());
        assert_eq!(
            reads(&machine).1,
            usize::try_from(head + 1).expect("count"),
            "each height is fetched exactly once"
        );
    }

    #[tokio::test]
    async fn a_range_answered_short_is_rejected_before_output() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 5;
        let mut source = Source::linear(head);
        source.data.range = 4;
        source.data.short = true;
        let mut machine = Machine::new(source, Sink::default(), Vec::new());
        fails_with(
            machine.sync(Some(0), None).await,
            &PipelineError::InvalidRange {
                from: 0,
                to: 3,
                returned: 3,
            },
        );
        assert!(machine.sink.events.is_empty());
        assert!(machine.tip().is_none());
    }

    #[tokio::test]
    async fn a_backfill_source_failure_keeps_what_was_accepted() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 5;
        let mut source = Source::linear(head);
        source.data.blocks.remove(&3);
        let mut machine = Machine::new(source, Sink::default(), Vec::new());
        fails_with(
            machine.sync(Some(0), None).await,
            &malformed("missing block"),
        );
        assert_eq!(heights(&machine), [0, 1, 2]);
        assert_eq!(machine.tip(), Some(meta(2)));
    }

    /// Ranges are requested ahead, up to the source's concurrency, and accepted in order
    /// even when a lower range answers after the ones above it.
    #[tokio::test(start_paused = true)]
    async fn concurrent_ranges_are_accepted_in_order() {
        // The buried bound is 17: ranges of four, three in flight, the first answering last.
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 17;
        let mut source = Source::linear(head);
        source.data.range = 4;
        source.data.concurrency = 3;
        source.data.slow = Some(0);
        let mut machine = Machine::new(source, Sink::default(), Vec::new());
        settle(&mut machine, Some(0)).await;
        assert_eq!(heights(&machine), (0..=head).collect::<Vec<_>>());
        assert_eq!(machine.source.data.peak.load(Ordering::Relaxed), 3);
        assert_eq!(
            reads(&machine).1,
            usize::try_from(head + 1).expect("count"),
            "each height is fetched exactly once"
        );
    }

    /// A failed range surfaces after every range below it is accepted, and the ranges
    /// fetched above it are not published.
    #[tokio::test]
    async fn a_failed_range_in_flight_keeps_the_ranges_below_it() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 17;
        let mut source = Source::linear(head);
        source.data.range = 2;
        source.data.concurrency = 3;
        source.data.blocks.remove(&3);
        let mut machine = Machine::new(source, Sink::default(), Vec::new());
        fails_with(
            machine.sync(Some(0), None).await,
            &malformed("missing block"),
        );
        assert_eq!(heights(&machine), [0, 1]);
        assert_eq!(machine.tip(), Some(meta(1)));
    }

    #[tokio::test]
    async fn sink_failure_does_not_advance_tip() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 5;
        let mut machine = Machine::new(Source::linear(head), Sink::default(), Vec::new());
        machine.sink.fail = true;
        fails_with(
            machine.sync(Some(0), None).await,
            &PipelineError::Sink(SinkError::StorageClosed),
        );
        assert_eq!(machine.tip(), None);
    }
}
