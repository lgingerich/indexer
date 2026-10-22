//! Startup: resume or start, backfill the buried heights, and fill the reorgable tail.

use tracing::{info, warn};

use super::{Machine, PipelineError};
use crate::ingest::source::{BlockSource, SourceError};
use crate::sink::EnvelopeSink;
use crate::wire::envelope::BlockMeta;

/// How many times startup asks a source that answered inconsistently, and how long it
/// waits between: about a minute, ample for a lagging backend to catch up.
const STARTUP_ATTEMPTS: u32 = 30;
const STARTUP_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

impl<S: BlockSource + 'static, K: EnvelopeSink> Machine<S, K> {
    /// [`Self::sync`], asked again when the source answered inconsistently: a backend
    /// behind the one that announced the head, as a load-balanced provider's often is,
    /// catches up within seconds, and stopping the run for it cost a restart. Accepted
    /// history matches what was delivered, so the next attempt resumes from the tip.
    /// After [`STARTUP_ATTEMPTS`] attempts in a row that accept nothing, the error stops
    /// the run as any other does. An attempt that moved the tip starts the count over: a
    /// long backfill on a provider that lags now and then meets many such answers, each
    /// soon past, and counting them all stopped it after the thirtieth.
    pub(super) async fn sync_settled(
        &mut self,
        start: Option<u64>,
        stop: Option<u64>,
    ) -> Result<Option<u64>, PipelineError> {
        let mut attempt = 1;
        let mut reached = self.ring.tip();
        loop {
            let result = self.sync(start, stop).await;
            if self.ring.tip() != reached {
                reached = self.ring.tip();
                attempt = 1;
            }
            match result {
                Err(
                    error @ (PipelineError::UnstableSource
                    | PipelineError::Source(SourceError::Inconsistent { .. })),
                ) if attempt < STARTUP_ATTEMPTS => {
                    warn!(%error, attempt, "the source answered inconsistently at startup; asking again");
                    attempt += 1;
                    tokio::time::sleep(STARTUP_RETRY_DELAY).await;
                }
                result => return result,
            }
        }
    }

    /// Brings accepted history up to a sampled head, leaving at least one block accepted.
    ///
    /// Backfill indexes `[start, head - window]`: the buried heights a reorg cannot reach.
    /// Reaching that bound, it samples the head again and continues while the chain has
    /// buried more. Above the bound is the reorgable tail, filled through
    /// [`Self::catch_up`], where a link failure is a fork to reconcile, so a run never
    /// leaves a gap under its start. A fresh run's first block has no predecessor and is
    /// adopted as-is; a resumed run continues after its reconciled tip. A start above the
    /// sampled head is a mistake, not something to clamp.
    ///
    /// A `stop` caps both phases: backfill ends at it when it is buried, and the tail is
    /// filled to it rather than to the head. It is checked against the first sampled head,
    /// before any output; a later sample only moves the head further from it. A source
    /// that does not [follow heads](BlockSource::follows_heads) always has one: its
    /// [`Self::buried_stop`].
    ///
    /// Returns the stop the run ended at, if it had one.
    pub(super) async fn sync(
        &mut self,
        start: Option<u64>,
        stop: Option<u64>,
    ) -> Result<Option<u64>, PipelineError> {
        if self.source.follows_heads() {
            self.sync_through(start, stop, None).await?;
            return Ok(stop);
        }
        let (stop, head) = self.buried_stop(start, stop).await?;
        self.sync_through(start, Some(stop), Some(head)).await?;
        Ok(Some(stop))
    }

    /// [`Self::sync`], once the stop is known: from `head` when it was already sampled
    /// and the stop checked against it, and from a head sampled here otherwise.
    async fn sync_through(
        &mut self,
        start: Option<u64>,
        stop: Option<u64>,
        head: Option<BlockMeta>,
    ) -> Result<(), PipelineError> {
        let restored = self.ring.tip();
        if let (Some(start), Some(tip)) = (start, restored) {
            if start > tip.height.saturating_add(1) {
                return Err(PipelineError::StartAboveHistory {
                    start,
                    tip: tip.height,
                });
            }
            info!(
                start,
                tip = tip.height,
                "the store already holds blocks past the start; resuming from the stored tip"
            );
        }
        if let (Some(tip), Some(stop)) = (restored, stop)
            && tip.height >= stop
        {
            info!(
                tip = tip.height,
                stop, "the store already holds the stop block; nothing to index"
            );
            return Ok(());
        }
        let mut head = if let Some(head) = head {
            head
        } else {
            let head = self.sample_head().await?;
            Self::check_stop(stop, head)?;
            head
        };
        let mut next = if let Some(tip) = restored {
            info!(height = tip.height, hash = %tip.hash, head = head.height,
                "resuming from the stored tip");
            // A source behind the store is not ahead of it: live reconciliation takes
            // over directly.
            if head.height < tip.height {
                return self.process_head(head).await;
            }
            Self::next(self.resume(tip).await?.height)?
        } else {
            let next = start.unwrap_or(head.height);
            if next > head.height {
                return Err(PipelineError::StartAboveHead {
                    start: next,
                    head: head.height,
                });
            }
            if let Some(stop) = stop
                && stop < next
            {
                return Err(PipelineError::StopBelowStart { stop, start: next });
            }
            next
        };
        (next, head) = self.backfill(next, head, stop).await?;
        let tip = if let Some(tip) = self.ring.tip() {
            tip
        } else {
            let block = self.source.fetch_block(next, None).await?;
            let meta = Self::marker(block.meta, next)?;
            self.commit(meta, block.events).await?;
            meta
        };
        let target = match stop {
            Some(stop) if tip.height >= stop => return Ok(()),
            Some(stop) if stop < head.height => self.header(stop).await?,
            _ => head,
        };
        self.catch_up(tip, target).await
    }

    /// Refuses a `stop` above `head`, the head a run's bounds are checked against.
    fn check_stop(stop: Option<u64>, head: BlockMeta) -> Result<(), PipelineError> {
        match stop {
            Some(stop) if stop > head.height => Err(PipelineError::StopAboveHead {
                stop,
                head: head.height,
            }),
            _ => Ok(()),
        }
    }

    /// The stop of a run on a source that does not [follow heads](BlockSource::follows_heads):
    /// the buried bound under the head it samples now, or `stop` when that is lower, with
    /// that head. The run is then an ordinary one with a stop, and ends there.
    ///
    /// It is fixed once, not resampled as backfill reaches it: no live phase takes over
    /// after it, and on a fast chain the head moves on during every pass, so a run that
    /// waited for it to stand still would never end.
    ///
    /// # Errors
    /// Returns [`PipelineError::BackfillWithoutStart`] for a fresh run with no `start`,
    /// which at the head would have nothing buried to index,
    /// [`PipelineError::StopAboveHead`] for a `stop` above the sampled head, and, for a
    /// fresh run, [`PipelineError::StopBelowStart`] for a `stop` below its `start`,
    /// [`PipelineError::StartAboveHead`] for a `start` above the head, and
    /// [`PipelineError::StartNotBuried`] for one above the buried heights.
    async fn buried_stop(
        &mut self,
        start: Option<u64>,
        stop: Option<u64>,
    ) -> Result<(u64, BlockMeta), PipelineError> {
        let fresh = self.ring.tip().is_none();
        if start.is_none() && fresh {
            return Err(PipelineError::BackfillWithoutStart);
        }
        let head = self.sample_head().await?;
        Self::check_stop(stop, head)?;
        let buried = Self::buried(head, self.finality_depth());
        // A fresh start above the buried heights would otherwise surface as the buried
        // bound below the start, a stop nobody set.
        if fresh && let Some(start) = start {
            match stop {
                Some(stop) if stop < start => {
                    return Err(PipelineError::StopBelowStart { stop, start });
                }
                _ if start > head.height => {
                    return Err(PipelineError::StartAboveHead {
                        start,
                        head: head.height,
                    });
                }
                _ if start > buried => {
                    return Err(PipelineError::StartNotBuried { start, buried });
                }
                _ => {}
            }
        }
        let buried = stop.map_or(buried, |stop| stop.min(buried));
        info!(chain = %self.source.chain(), through = buried,
            "this source does not follow the chain: indexing the buried heights, then \
             stopping; restart on one that does to continue");
        Ok((buried, head))
    }

    /// Reconciles a restored tip against the source, returning the tip to continue from.
    ///
    /// Only the tip's header is re-read, since on the usual path only its hash is
    /// compared. A different hash means the stored suffix was orphaned while no process
    /// was running, and the ordinary fork walk retracts it before anything new is
    /// published. A source that does not follow heads has no view of the tail to walk,
    /// so on one the difference is an error.
    async fn resume(&mut self, tip: BlockMeta) -> Result<BlockMeta, PipelineError> {
        let current = self.header(tip.height).await?;
        if current.hash == tip.hash {
            return Ok(tip);
        }
        if !self.source.follows_heads() {
            return Err(PipelineError::IdentityMismatch {
                expected: Box::new(tip),
                actual: Box::new(current),
            });
        }
        self.handle_reorg(current, current).await?;
        Ok(current)
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {

    use super::*;
    use crate::ingest::pipeline::testing::*;
    use crate::ingest::source::SourceError;
    use crate::window::Ledger;
    use crate::window::MAX_UNFINALIZED_BLOCKS;
    use crate::wire::envelope::{BlockMeta, Event};

    #[tokio::test]
    async fn a_near_head_start_seeds_then_extends_one_height_at_a_time() {
        let mut machine = Machine::new(Source::linear(5), Sink::default(), Vec::new());
        // The head is inside the window, but startup still fills through the sampled
        // head, so the run is already reconciled above the start.
        settle(&mut machine, Some(2)).await;
        assert_eq!(heights(&machine), [2, 3, 4, 5]);
        assert_eq!(machine.tip(), Some(meta(5)));
    }

    /// A start or stop outside the sampled head, or a stop below the start, is refused
    /// before any block is fetched; a start and stop at the head itself are not.
    #[tokio::test]
    async fn a_start_or_stop_out_of_bounds_is_rejected_before_output() {
        let cases = [
            (
                "start above the head",
                Some(6),
                None,
                PipelineError::StartAboveHead { start: 6, head: 5 },
            ),
            (
                "stop above the head",
                Some(2),
                Some(6),
                PipelineError::StopAboveHead { stop: 6, head: 5 },
            ),
            (
                "stop below the start",
                Some(3),
                Some(2),
                PipelineError::StopBelowStart { stop: 2, start: 3 },
            ),
            // Without a start the run starts at the head, so a lower stop is below it too.
            (
                "stop below the head without a start",
                None,
                Some(4),
                PipelineError::StopBelowStart { stop: 4, start: 5 },
            ),
        ];
        for (label, start, stop, expected) in cases {
            let mut machine = Machine::new(Source::linear(5), Sink::default(), Vec::new());
            let error = machine
                .sync(start, stop)
                .await
                .expect_err(&format!("{label} is refused"));
            assert_eq!(format!("{error:?}"), format!("{expected:?}"), "{label}");
            assert_eq!(reads(&machine).1, 0, "{label}: no block is fetched");
            assert!(machine.sink.events.is_empty(), "{label}");
        }
        let mut machine = Machine::new(Source::linear(5), Sink::default(), Vec::new());
        machine
            .sync(Some(5), Some(5))
            .await
            .expect("bounds at the head");
        assert_eq!(heights(&machine), [5]);
    }

    /// A buried stop ends backfill there: the head is not sampled again, nothing above
    /// the stop is fetched, and the run returns rather than subscribing, which the test
    /// source would never answer.
    #[tokio::test(start_paused = true)]
    async fn a_buried_stop_ends_the_run_there() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 20;
        let mut source = Source::linear(head);
        source.range = 4;
        let mut machine = Machine::new(source, Sink::default(), Vec::new());
        assert_eq!(machine.sync(Some(0), Some(9)).await.expect("sync"), Some(9));
        assert_eq!(heights(&machine), (0..=9).collect::<Vec<_>>());
        assert_eq!(reads(&machine), (1, 10));
        // The clock is paused, so the timeout fires only if the run went on to wait for
        // heads: the reconcile timer is the only thing that would advance it.
        let machine = Machine::new(Source::linear(head), Sink::default(), Vec::new());
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            machine.run(Some(0), Some(9)),
        )
        .await
        .expect("a stopped run returns without following heads")
        .expect("run");
    }

    #[tokio::test]
    async fn a_stop_inside_the_window_fills_the_tail_to_it() {
        let mut machine = Machine::new(Source::linear(5), Sink::default(), Vec::new());
        machine.sync(Some(2), Some(4)).await.expect("sync");
        assert_eq!(heights(&machine), [2, 3, 4]);
        assert_eq!(machine.tip(), Some(meta(4)));
        assert_eq!(reads(&machine).1, 3, "nothing above the stop is fetched");
    }

    #[tokio::test]
    async fn a_resumed_run_continues_to_the_stop() {
        let mut machine = Machine::new(Source::linear(8), Sink::default(), ledger(0..=3));
        machine.sync(None, Some(6)).await.expect("sync");
        assert_eq!(heights(&machine), [4, 5, 6]);
        assert_eq!(machine.tip(), Some(meta(6)));
    }

    #[tokio::test]
    async fn a_store_holding_the_stop_has_nothing_to_do() {
        let mut machine = Machine::new(Source::linear(8), Sink::default(), ledger(0..=6));
        machine.sync(None, Some(6)).await.expect("sync");
        machine.sync(None, Some(4)).await.expect("sync");
        assert!(machine.sink.events.is_empty());
        assert_eq!(reads(&machine), (0, 0), "the chain is not asked at all");
    }

    /// A source that answers inconsistently at startup is asked again, from the tip it
    /// reached, rather than stopping the run.
    #[tokio::test(start_paused = true)]
    async fn startup_asks_an_inconsistent_source_again() {
        let source = Source::linear(6);
        source.fail(3, Fault::Inconsistent, 1);
        let mut machine = Machine::new(source, Sink::default(), ledger(0..=2));
        assert_eq!(
            machine
                .sync_settled(None, Some(6))
                .await
                .expect("asked again"),
            Some(6)
        );
        assert_eq!(heights(&machine), [3, 4, 5, 6]);
        assert_eq!(
            reads(&machine),
            (2, 5),
            "the head is sampled again, and only the failed height fetched twice"
        );
    }

    /// Startup gives up on an inconsistent source only after that many attempts in a row
    /// accept nothing: a long backfill whose provider lags at many heights, each soon
    /// caught up, runs through, and one stuck at a height still stops the run.
    #[tokio::test(start_paused = true)]
    async fn startup_gives_up_only_after_attempts_in_a_row_without_progress() {
        let attempts = u64::from(STARTUP_ATTEMPTS);
        let head = 2 * attempts;
        let source = Source::linear(head);
        for height in 1..=head {
            source.fail(height, Fault::Inconsistent, 1);
        }
        let mut machine = Machine::new(source, Sink::default(), ledger(0..=0));
        machine
            .sync_settled(None, Some(head))
            .await
            .expect("every lagging height is caught up");
        assert_eq!(heights(&machine), (1..=head).collect::<Vec<_>>());

        let source = Source::linear(6);
        source.fail(3, Fault::Inconsistent, u32::MAX);
        let mut machine = Machine::new(source, Sink::default(), ledger(0..=2));
        let error = machine
            .sync_settled(None, Some(6))
            .await
            .expect_err("a height that never settles stops the run");
        assert!(
            matches!(
                error,
                PipelineError::Source(SourceError::Inconsistent { .. })
            ),
            "{error:?}"
        );
        assert_eq!(
            reads(&machine).1,
            usize::try_from(attempts).expect("count"),
            "the stuck height is asked once per attempt"
        );
    }

    /// A source failure other than an inconsistent answer stops startup at once: the
    /// head is sampled once and the failing block fetched once, not asked again.
    #[tokio::test(start_paused = true)]
    async fn startup_returns_source_errors_without_retrying() {
        let source = Source::linear(3);
        source.fail(3, Fault::Failed, 1);
        let mut machine = Machine::new(source, Sink::default(), Vec::new());
        // The failure is injected once, so a retry would succeed and return `Ok`.
        fails_with(
            machine.sync_settled(None, None).await,
            &malformed("injected source failure"),
        );
        assert_eq!(reads(&machine), (1, 1));
        assert!(machine.sink.events.is_empty());
    }

    /// A run on a source that only backfills indexes every buried height from its start and stops at the
    /// reorg window, leaving the window to the run that follows.
    #[tokio::test]
    async fn a_backfill_stops_at_the_reorg_window() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 5;
        let mut machine = Machine::new(Source::backfilling(head), Sink::default(), Vec::new());
        assert_eq!(
            machine.sync(Some(2), None).await.expect("backfill"),
            Some(5)
        );
        assert_eq!(heights(&machine), (2..=5).collect::<Vec<_>>());
    }

    /// A chain that finalizes sooner hands its backfill over sooner: the run stops its
    /// finality depth under the head, not the whole undo window under it, and a depth
    /// deeper than a run can retract is held to the undo window.
    #[tokio::test]
    async fn a_backfill_stops_at_the_chains_finality_depth() {
        let mut source = Source::backfilling(100);
        source.finality = 10;
        let mut machine = Machine::new(source, Sink::default(), Vec::new());
        machine.sync(Some(2), None).await.expect("backfill");
        assert_eq!(heights(&machine), (2..=90).collect::<Vec<_>>());

        let head = MAX_UNFINALIZED_BLOCKS as u64 + 5;
        let mut source = Source::backfilling(head);
        source.finality = u64::MAX;
        let mut machine = Machine::new(source, Sink::default(), Vec::new());
        machine.sync(Some(2), None).await.expect("backfill");
        assert_eq!(heights(&machine), (2..=5).collect::<Vec<_>>());
    }

    /// A stop below the reorg window ends a backfill there; one already stored
    /// leaves nothing to do.
    #[tokio::test]
    async fn a_backfill_ends_at_its_stop_block() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 9;
        let mut machine = Machine::new(Source::backfilling(head), Sink::default(), Vec::new());
        machine.sync(Some(2), Some(4)).await.expect("backfill");
        assert_eq!(heights(&machine), (2..=4).collect::<Vec<_>>());

        let mut machine = Machine::new(Source::backfilling(head), Sink::default(), ledger(0..=5));
        machine.sync(None, Some(5)).await.expect("backfill");
        assert!(machine.sink.events.is_empty());
        assert_eq!(reads(&machine).1, 0, "no block is fetched");
    }

    /// A backfill resumes after a stored tip that the source agrees with, and refuses one
    /// it does not: that is a fork it cannot retract.
    #[tokio::test]
    async fn a_backfill_resumes_after_the_stored_tip_or_refuses_a_different_one() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 9;
        let mut machine = Machine::new(Source::backfilling(head), Sink::default(), ledger(0..=5));
        machine.sync(None, None).await.expect("backfill");
        assert_eq!(heights(&machine), (6..=9).collect::<Vec<_>>());

        let mut stored = ledger(0..=5);
        stored[5].hash = hash(105);
        let tip = stored[5];
        let mut machine = Machine::new(Source::backfilling(head), Sink::default(), stored);
        fails_with(
            machine.sync(None, None).await,
            &PipelineError::IdentityMismatch {
                expected: Box::new(tip),
                actual: Box::new(meta(5)),
            },
        );
        assert!(machine.sink.events.is_empty());
    }

    /// With no stored blocks and no start, a backfill would start at the head, where
    /// nothing is buried: that is a missing setting, not an empty run.
    #[tokio::test]
    async fn a_backfill_needs_a_start_or_stored_blocks() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 5;
        let mut machine = Machine::new(Source::backfilling(head), Sink::default(), Vec::new());
        fails_with(
            machine.sync(None, None).await,
            &PipelineError::BackfillWithoutStart,
        );
        assert_eq!(reads(&machine).1, 0, "no block is fetched");
    }

    /// A fresh backfill asked to start inside the finality window, where it indexes
    /// nothing, is told so, rather than refused for a stop below its start that nobody
    /// set. One that starts below the window samples the head once and backfills to it.
    #[tokio::test]
    async fn a_backfill_starting_above_the_buried_heights_is_refused_plainly() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 5;
        let mut machine = Machine::new(Source::backfilling(head), Sink::default(), Vec::new());
        fails_with(
            machine.sync(Some(7), None).await,
            &PipelineError::StartNotBuried {
                start: 7,
                buried: 5,
            },
        );
        assert_eq!(reads(&machine), (1, 0), "one head sample, no block");

        let mut machine = Machine::new(Source::backfilling(head), Sink::default(), Vec::new());
        assert_eq!(
            machine.sync(Some(2), None).await.expect("backfill"),
            Some(5)
        );
        assert_eq!(heights(&machine), [2, 3, 4, 5]);
        assert_eq!(reads(&machine).0, 1, "the head is sampled once");
    }

    /// A start the stored history already reaches is ignored, as a followed run ignores
    /// it; one past the tip's successor would leave a gap, and is refused.
    #[tokio::test]
    async fn a_backfill_resumes_past_a_start_the_store_reaches_and_refuses_a_gap() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 9;
        for start in [0, 5, 6] {
            let mut machine =
                Machine::new(Source::backfilling(head), Sink::default(), ledger(0..=5));
            machine.sync(Some(start), None).await.expect("backfill");
            assert_eq!(
                heights(&machine),
                (6..=9).collect::<Vec<_>>(),
                "start {start}"
            );
        }
        let mut machine = Machine::new(Source::backfilling(head), Sink::default(), ledger(0..=5));
        fails_with(
            machine.sync(Some(7), None).await,
            &PipelineError::StartAboveHistory { start: 7, tip: 5 },
        );
        assert!(machine.sink.events.is_empty());
    }

    /// A source that only backfills stops at the reorg window under the head it sampled
    /// first, rather than chasing a head that keeps moving.
    #[tokio::test]
    async fn a_backfill_stops_under_the_head_it_sampled_first() {
        let window = MAX_UNFINALIZED_BLOCKS as u64;
        let source = Source::backfilling(window + 20);
        // The head moves at every sample; the run takes one, for its stop.
        source
            .samples
            .lock()
            .expect("samples")
            .extend([window + 5, window + 10, window + 15]);
        let mut machine = Machine::new(source, Sink::default(), Vec::new());
        let stop = machine.sync(Some(2), None).await.expect("backfill");
        assert_eq!(stop, Some(5));
        assert_eq!(heights(&machine), (2..=5).collect::<Vec<_>>());
        assert_eq!(reads(&machine).0, 1, "the head is sampled once");
    }

    #[tokio::test]
    async fn a_resumed_run_continues_after_a_matching_stored_tip() {
        let mut machine = Machine::new(Source::linear(8), Sink::default(), ledger(0..=5));
        assert_eq!(machine.tip(), Some(meta(5)), "the restored tip");
        settle(&mut machine, None).await;
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
        settle(&mut machine, None).await;
        assert_eq!(heights(&machine), (6..=head).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_fork_while_down_is_retracted_once_then_replayed() {
        let mut source = Source::linear(7);
        fork(&mut source, 4, 7);
        let mut machine = Machine::new(source, Sink::default(), ledger(0..=5));
        settle(&mut machine, None).await;
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

    /// A store whose ledger starts at the run's first block resumes through a fork that
    /// replaced it while down — here its parent too — by adopting the new branch.
    #[tokio::test]
    async fn a_fork_while_down_replacing_the_first_stored_block_is_adopted() {
        let mut source = Source::linear(7);
        fork(&mut source, 3, 7);
        let mut machine = Machine::new(source, Sink::default(), ledger(4..=5));
        settle(&mut machine, None).await;
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

    /// A restored ledger that lost an older, unlinked part may have rows below its oldest
    /// block, so a fork below that block stops rather than adopt over them.
    #[tokio::test]
    async fn a_fork_while_down_below_a_truncated_ledger_stops() {
        let mut source = Source::linear(7);
        fork(&mut source, 3, 7);
        let mut stored = ledger(0..=1);
        stored.extend(ledger(4..=5));
        let mut machine = Machine::new(source, Sink::default(), stored);
        fails_with(
            machine.sync(None, None).await,
            &PipelineError::UndoWindowExceeded {
                floor: Box::new(meta(4)),
                actual: Box::new(BlockMeta {
                    hash: hash(104),
                    parent_hash: hash(103),
                    ..meta(4)
                }),
            },
        );
        assert!(
            machine.sink.events.is_empty(),
            "nothing is retracted or replayed"
        );
        assert_eq!(machine.tip(), Some(meta(5)));
    }

    /// A ledger cut from the top, as resuming several stores cuts the slowest one's, is
    /// as short as a whole history, but says the stores hold blocks below it. A fork
    /// replacing its oldest block then stops instead of being adopted over them.
    #[tokio::test]
    async fn a_fork_while_down_replacing_the_oldest_block_of_a_cut_ledger_stops() {
        let mut source = Source::linear(7);
        fork(&mut source, 3, 7);
        let stored = Ledger {
            blocks: ledger(4..=5),
            history_below: true,
        };
        let mut machine = Machine::new(source, Sink::default(), stored);
        fails_with(
            machine.sync(None, None).await,
            &PipelineError::UndoWindowExceeded {
                floor: Box::new(meta(4)),
                actual: Box::new(BlockMeta {
                    hash: hash(104),
                    parent_hash: hash(103),
                    ..meta(4)
                }),
            },
        );
        assert!(
            machine.sink.events.is_empty(),
            "nothing is retracted or replayed"
        );
        assert_eq!(machine.tip(), Some(meta(5)));
    }

    #[tokio::test]
    async fn the_first_resumed_block_must_link_to_the_stored_tip() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 20;
        let mut source = Source::linear(head);
        source.blocks.insert(6, (hash(6), hash(99)));
        let mut machine = Machine::new(source, Sink::default(), ledger(0..=5));
        fails_with(
            machine.sync(None, None).await,
            &PipelineError::BrokenLink {
                previous: Box::new(meta(5)),
                actual: Box::new(BlockMeta {
                    parent_hash: hash(99),
                    ..meta(6)
                }),
            },
        );
        assert!(machine.sink.events.is_empty());
        assert_eq!(machine.tip(), Some(meta(5)));
    }

    #[tokio::test]
    async fn a_source_behind_the_stored_tip_waits_for_it() {
        let mut machine = Machine::new(Source::linear(8), Sink::default(), ledger(0..=5));
        chain(&mut machine).head = 3;
        settle(&mut machine, None).await;
        assert!(machine.sink.events.is_empty());
        assert_eq!(machine.tip(), Some(meta(5)));
    }

    #[tokio::test]
    async fn a_start_height_above_stored_history_is_refused_before_any_read() {
        let mut machine = Machine::new(Source::linear(8), Sink::default(), ledger(0..=5));
        fails_with(
            machine.sync(Some(7), None).await,
            &PipelineError::StartAboveHistory { start: 7, tip: 5 },
        );
        assert_eq!(reads(&machine), (0, 0), "the chain is not asked at all");
    }

    /// A start the stored history already reaches, or the block right after it, is
    /// ignored: the run resumes from the stored tip.
    #[tokio::test]
    async fn a_start_height_within_stored_history_resumes_from_the_tip() {
        for start in [2, 5, 6] {
            let mut machine = Machine::new(Source::linear(8), Sink::default(), ledger(0..=5));
            machine.sync(Some(start), Some(8)).await.expect("sync");
            assert_eq!(heights(&machine), [6, 7, 8], "start {start}");
        }
    }
}
