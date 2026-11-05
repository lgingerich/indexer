//! Startup: resume or start, backfill the buried heights, and fill the reorgable tail.
//!
//! What a run does is resolved once, before its first attempt, into a [`Plan`]: where a
//! fresh run begins and where startup ends ([`End`]), from the requested start and stop,
//! whether the source follows heads, the undo window, and the first head sampled. Each
//! attempt then follows the plan from wherever the attempts before it left the window.

use tracing::{info, warn};

use super::{Machine, PipelineError};
use crate::ingest::source::{BlockSource, SourceError};
use crate::sink::EnvelopeSink;
use crate::wire::envelope::BlockMeta;

/// How many times startup asks a source that answered inconsistently, and how long it
/// waits between: about a minute, ample for a lagging backend to catch up.
const STARTUP_ATTEMPTS: u32 = 30;
const STARTUP_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

/// Where startup ends.
#[derive(Debug, Clone, Copy)]
enum End {
    /// At the head each attempt samples; the run then follows live heads. Only a run
    /// without a stop, on a source that [follows heads](BlockSource::follows_heads), ends
    /// so.
    Follow,
    /// At this height; the run then returns. A run with a stop ends at it. One on a
    /// source that only backfills always ends here: at the buried bound under the head
    /// it samples first, or at its stop when that is lower. The bound is fixed once, not
    /// resampled as backfill reaches it: no live phase takes over after it, and on a fast
    /// chain the head moves on during every pass, so a run that waited for it to stand
    /// still would never end.
    Stop(u64),
}

impl End {
    /// The height startup ends at, for a run that returns there.
    const fn stop(self) -> Option<u64> {
        match self {
            Self::Follow => None,
            Self::Stop(stop) => Some(stop),
        }
    }
}

/// What startup runs, resolved once against the first sampled head, so a retry neither
/// moves the run's end nor its first height.
#[derive(Debug, Clone, Copy)]
struct Plan {
    /// The first height to index while the window is empty: the requested start, or the
    /// first sampled head without one, adopted as-is. Once the window holds a block, an
    /// attempt resumes after its tip instead.
    first: u64,
    end: End,
}

/// How many attempts in a row startup has made without the tip advancing.
///
/// Advancing means reaching a height above any reached before. A tip that merely changed
/// is not progress: a source flapping between two forks moves it back and forth at one
/// height, and one rewinding moves it down, and either would otherwise be asked forever.
#[derive(Debug)]
struct Attempts {
    /// Failed attempts since the tip last advanced.
    stalled: u32,
    /// The highest tip reached, `None` before any block is accepted.
    highest: Option<u64>,
}

impl Attempts {
    /// No attempts yet, from the window's tip `tip`.
    fn new(tip: Option<BlockMeta>) -> Self {
        Self {
            stalled: 0,
            highest: tip.map(|tip| tip.height),
        }
    }

    /// Counts an attempt that failed with the window at `tip`, and says whether another
    /// may follow: one that advanced the tip starts the count over, and after
    /// [`STARTUP_ATTEMPTS`] in a row that did not, none may.
    fn retry(&mut self, tip: Option<BlockMeta>) -> bool {
        let reached = tip.map(|tip| tip.height);
        if reached > self.highest {
            self.highest = reached;
            self.stalled = 0;
        }
        self.stalled += 1;
        self.stalled < STARTUP_ATTEMPTS
    }
}

impl<S: BlockSource + 'static, K: EnvelopeSink> Machine<S, K> {
    /// Brings accepted history up to where startup ends for `start` and `stop`, leaving
    /// at least one block accepted, and returns the stop it ended at, if the run has one.
    ///
    /// What is knowable without the chain is checked first: a `start` past the stored
    /// tip's successor, a stored tip already at the stop, and a fresh backfill with no
    /// start. The head is then sampled once and the run's [`Plan`] resolved against it.
    ///
    /// An attempt the source answered inconsistently is made again: a backend behind the
    /// one that announced the head, as a load-balanced provider's often is, catches up
    /// within seconds, and stopping the run for it cost a restart. Accepted history
    /// matches what was delivered, so the next attempt resumes from the tip. After
    /// [`STARTUP_ATTEMPTS`] attempts in a row that do not advance the tip (see
    /// [`Attempts`]), the error stops the run as any other does. An attempt that advanced
    /// it starts the count over: a long backfill on a provider that lags now and then
    /// meets many such answers, each soon past, and counting them all stopped it after
    /// the thirtieth.
    ///
    /// # Errors
    /// Returns [`PipelineError::StartAboveHistory`] and
    /// [`PipelineError::BackfillWithoutStart`] before any read, the errors of
    /// [`Self::plan`] before any block is fetched, and whatever the last attempt failed
    /// with.
    pub(super) async fn sync_settled(
        &mut self,
        start: Option<u64>,
        stop: Option<u64>,
    ) -> Result<Option<u64>, PipelineError> {
        match (self.ring.tip(), start) {
            (Some(tip), Some(start)) if start > tip.height.saturating_add(1) => {
                return Err(PipelineError::StartAboveHistory {
                    start,
                    tip: tip.height,
                });
            }
            (Some(tip), Some(start)) => info!(
                start,
                tip = tip.height,
                "the store already holds blocks past the start; resuming from the stored tip"
            ),
            (None, None) if !self.source.follows_heads() => {
                return Err(PipelineError::BackfillWithoutStart);
            }
            _ => {}
        }
        if self.holds(stop) {
            return Ok(stop);
        }
        let head = self.sample_head().await?;
        let plan = self.plan(start, stop, head)?;
        let mut attempts = Attempts::new(self.ring.tip());
        let mut result = self.sync(plan, head).await;
        loop {
            match result {
                Err(
                    error @ (PipelineError::UnstableSource
                    | PipelineError::Source(SourceError::Inconsistent { .. })),
                ) if attempts.retry(self.ring.tip()) => {
                    warn!(%error, attempt = attempts.stalled,
                        "the source answered inconsistently at startup; asking again");
                    tokio::time::sleep(STARTUP_RETRY_DELAY).await;
                    result = match self.sample_head().await {
                        Ok(head) => self.sync(plan, head).await,
                        Err(error) => Err(error),
                    };
                }
                result => return result.map(|()| plan.end.stop()),
            }
        }
    }

    /// Whether the window's tip is already at or past `stop`, which leaves startup
    /// nothing to index; says so when it is.
    fn holds(&self, stop: Option<u64>) -> bool {
        let (Some(tip), Some(stop)) = (self.ring.tip(), stop) else {
            return false;
        };
        let held = tip.height >= stop;
        if held {
            info!(
                tip = tip.height,
                stop, "the store already holds the stop block; nothing to index"
            );
        }
        held
    }

    /// Resolves what startup runs from the requested `start` and `stop`, whether the
    /// source follows heads, the undo window, and `head`, the first head sampled.
    ///
    /// A fresh run begins at `start`, or at `head` without one; a run with accepted
    /// history resumes after its tip, and its `start` was checked already. On a source
    /// that follows heads, startup ends at the head each attempt samples without a stop,
    /// and at the stop with one; on a source that only backfills, at the buried bound
    /// under `head`, or at the stop when that is lower.
    ///
    /// # Errors
    /// Returns [`PipelineError::StopAboveHead`] for a stop above `head`, and, for a fresh
    /// run, [`PipelineError::StopBelowStart`] for a stop below its first height,
    /// [`PipelineError::StartAboveHead`] for a first height above `head`, and
    /// [`PipelineError::StartNotBuried`] for one above the buried heights of a run that
    /// only backfills, which there would surface as a stop below the start nobody set.
    fn plan(
        &self,
        start: Option<u64>,
        stop: Option<u64>,
        head: BlockMeta,
    ) -> Result<Plan, PipelineError> {
        if let Some(stop) = stop
            && stop > head.height
        {
            return Err(PipelineError::StopAboveHead {
                stop,
                head: head.height,
            });
        }
        let follows = self.source.follows_heads();
        let buried = Self::buried(head, self.finality_depth());
        let end = match stop {
            None if follows => End::Follow,
            Some(stop) if follows => End::Stop(stop),
            stop => End::Stop(stop.map_or(buried, |stop| stop.min(buried))),
        };
        let first = start.unwrap_or(head.height);
        if self.ring.tip().is_none() {
            match stop {
                Some(stop) if stop < first => {
                    return Err(PipelineError::StopBelowStart { stop, start: first });
                }
                _ if first > head.height => {
                    return Err(PipelineError::StartAboveHead {
                        start: first,
                        head: head.height,
                    });
                }
                _ if !follows && first > buried => {
                    return Err(PipelineError::StartNotBuried {
                        start: first,
                        buried,
                    });
                }
                _ => {}
            }
        }
        if let (false, End::Stop(through)) = (follows, end) {
            info!(chain = %self.source.chain(), through,
                "this source does not follow the chain: indexing the buried heights, then \
                 stopping; restart on one that does to continue");
        }
        Ok(Plan { first, end })
    }

    /// One attempt at `plan`, from `head`, the head sampled for it.
    ///
    /// Backfill indexes `[next, head - window]`: the buried heights a reorg cannot reach.
    /// Reaching that bound, it samples the head again and continues while the chain has
    /// buried more. Above the bound is the reorgable tail, filled through
    /// [`Self::catch_up`], where a link failure is a fork to reconcile, so a run never
    /// leaves a gap under its start. A fresh run's first block has no predecessor and is
    /// adopted as-is; a resumed run continues after its reconciled tip.
    ///
    /// A stop caps both phases: backfill ends at it when it is buried, and the tail is
    /// filled to the block there rather than to the head.
    async fn sync(&mut self, plan: Plan, head: BlockMeta) -> Result<(), PipelineError> {
        if self.holds(plan.end.stop()) {
            return Ok(());
        }
        // An attempt after one that accepted blocks resumes after them. Once the window
        // holds a block it holds one until a fork's first replacement is accepted, and a
        // failure there is the sink's, which is not retried.
        let next = if let Some(tip) = self.ring.tip() {
            info!(height = tip.height, hash = %tip.hash, head = head.height,
                "resuming from the stored tip");
            // A source behind the store is not ahead of it: live reconciliation takes
            // over directly. A run with a stop goes on to read the blocks it lacks, which
            // a backend that lags fails as an inconsistent answer.
            if matches!(plan.end, End::Follow) && head.height < tip.height {
                return self.process_head(head).await;
            }
            Self::next(self.resume(tip).await?.height)?
        } else {
            plan.first
        };
        let (next, head) = self.backfill(next, head, plan.end.stop()).await?;
        let tip = if let Some(tip) = self.ring.tip() {
            tip
        } else {
            let block = self.source.fetch_block(next, None).await?;
            let meta = Self::marker(block.meta, next)?;
            self.commit(meta, block.events).await?;
            meta
        };
        let target = match plan.end {
            End::Stop(stop) if tip.height >= stop => return Ok(()),
            // Read by height even when the head sampled for this attempt is lower: the
            // stop was at or below the first one, and a retry's may lag it.
            End::Stop(stop) if stop != head.height => self.header(stop).await?,
            _ => head,
        };
        self.catch_up(tip, target).await
    }

    /// Reconciles a restored tip against the source, returning the tip to continue from.
    ///
    /// Only the tip's header is re-read, since on the usual path only its hash is
    /// compared. A different hash means the stored suffix was orphaned while no process
    /// was running, and the ordinary fork walk retracts it before anything new is
    /// published. A source that only backfills has no view of the tail to walk, so on
    /// one the difference is an error.
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
                .sync_settled(start, stop)
                .await
                .expect_err(&format!("{label} is refused"));
            assert_eq!(format!("{error:?}"), format!("{expected:?}"), "{label}");
            assert_eq!(reads(&machine).1, 0, "{label}: no block is fetched");
            assert!(machine.sink.events.is_empty(), "{label}");
        }
        let mut machine = Machine::new(Source::linear(5), Sink::default(), Vec::new());
        machine
            .sync_settled(Some(5), Some(5))
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
        assert_eq!(
            machine.sync_settled(Some(0), Some(9)).await.expect("sync"),
            Some(9)
        );
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
        machine.sync_settled(Some(2), Some(4)).await.expect("sync");
        assert_eq!(heights(&machine), [2, 3, 4]);
        assert_eq!(machine.tip(), Some(meta(4)));
        assert_eq!(reads(&machine).1, 3, "nothing above the stop is fetched");
    }

    #[tokio::test]
    async fn a_resumed_run_continues_to_the_stop() {
        let mut machine = Machine::new(Source::linear(8), Sink::default(), ledger(0..=3));
        machine.sync_settled(None, Some(6)).await.expect("sync");
        assert_eq!(heights(&machine), [4, 5, 6]);
        assert_eq!(machine.tip(), Some(meta(6)));
    }

    #[tokio::test]
    async fn a_store_holding_the_stop_has_nothing_to_do() {
        let mut machine = Machine::new(Source::linear(8), Sink::default(), ledger(0..=6));
        machine.sync_settled(None, Some(6)).await.expect("sync");
        machine.sync_settled(None, Some(4)).await.expect("sync");
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

    /// Only a tip above every one reached before is progress. A source flapping between
    /// two forks at one height, or rewinding the tip and refilling it, changes the tip on
    /// every attempt without advancing it, and startup still gives up on it.
    #[test]
    fn only_a_higher_tip_starts_the_attempts_over() {
        let forked = BlockMeta {
            hash: hash(105),
            ..meta(5)
        };
        for (label, tips) in [
            ("flapping", [forked, meta(5)]),
            ("rewinding", [meta(3), meta(5)]),
        ] {
            let mut attempts = Attempts::new(Some(meta(5)));
            let allowed = tips
                .into_iter()
                .cycle()
                .take(2 * usize::try_from(STARTUP_ATTEMPTS).expect("count"))
                .take_while(|&tip| attempts.retry(Some(tip)))
                .count();
            assert_eq!(
                allowed,
                usize::try_from(STARTUP_ATTEMPTS - 1).expect("count"),
                "{label}: the attempts in a row without progress run out as for a stuck tip"
            );
        }

        let mut attempts = Attempts::new(None);
        for height in 0..u64::from(STARTUP_ATTEMPTS) * 2 {
            assert!(
                attempts.retry(Some(meta(height))),
                "height {height} advanced"
            );
        }
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
            machine.sync_settled(Some(2), None).await.expect("backfill"),
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
        machine.sync_settled(Some(2), None).await.expect("backfill");
        assert_eq!(heights(&machine), (2..=90).collect::<Vec<_>>());

        let head = MAX_UNFINALIZED_BLOCKS as u64 + 5;
        let mut source = Source::backfilling(head);
        source.finality = u64::MAX;
        let mut machine = Machine::new(source, Sink::default(), Vec::new());
        machine.sync_settled(Some(2), None).await.expect("backfill");
        assert_eq!(heights(&machine), (2..=5).collect::<Vec<_>>());
    }

    /// A stop below the reorg window ends a backfill there; one already stored
    /// leaves nothing to do.
    #[tokio::test]
    async fn a_backfill_ends_at_its_stop_block() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 9;
        let mut machine = Machine::new(Source::backfilling(head), Sink::default(), Vec::new());
        machine
            .sync_settled(Some(2), Some(4))
            .await
            .expect("backfill");
        assert_eq!(heights(&machine), (2..=4).collect::<Vec<_>>());

        let mut machine = Machine::new(Source::backfilling(head), Sink::default(), ledger(0..=5));
        machine.sync_settled(None, Some(5)).await.expect("backfill");
        assert!(machine.sink.events.is_empty());
        assert_eq!(reads(&machine).1, 0, "no block is fetched");
    }

    /// A backfill resumes after a stored tip that the source agrees with, and refuses one
    /// it does not: that is a fork it cannot retract.
    #[tokio::test]
    async fn a_backfill_resumes_after_the_stored_tip_or_refuses_a_different_one() {
        let head = MAX_UNFINALIZED_BLOCKS as u64 + 9;
        let mut machine = Machine::new(Source::backfilling(head), Sink::default(), ledger(0..=5));
        machine.sync_settled(None, None).await.expect("backfill");
        assert_eq!(heights(&machine), (6..=9).collect::<Vec<_>>());

        let mut stored = ledger(0..=5);
        stored[5].hash = hash(105);
        let tip = stored[5];
        let mut machine = Machine::new(Source::backfilling(head), Sink::default(), stored);
        fails_with(
            machine.sync_settled(None, None).await,
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
            machine.sync_settled(None, None).await,
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
            machine.sync_settled(Some(7), None).await,
            &PipelineError::StartNotBuried {
                start: 7,
                buried: 5,
            },
        );
        assert_eq!(reads(&machine), (1, 0), "one head sample, no block");

        let mut machine = Machine::new(Source::backfilling(head), Sink::default(), Vec::new());
        assert_eq!(
            machine.sync_settled(Some(2), None).await.expect("backfill"),
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
            machine
                .sync_settled(Some(start), None)
                .await
                .expect("backfill");
            assert_eq!(
                heights(&machine),
                (6..=9).collect::<Vec<_>>(),
                "start {start}"
            );
        }
        let mut machine = Machine::new(Source::backfilling(head), Sink::default(), ledger(0..=5));
        fails_with(
            machine.sync_settled(Some(7), None).await,
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
        let stop = machine.sync_settled(Some(2), None).await.expect("backfill");
        assert_eq!(stop, Some(5));
        assert_eq!(heights(&machine), (2..=5).collect::<Vec<_>>());
        assert_eq!(reads(&machine).0, 1, "the head is sampled once");
    }

    /// A backfill asked again after an inconsistent answer still stops where its first
    /// sample put it: the retry samples a newer head for its own reads, but the stop is
    /// fixed once, not moved with it.
    #[tokio::test(start_paused = true)]
    async fn a_backfill_asked_again_keeps_the_stop_its_first_sample_set() {
        let window = MAX_UNFINALIZED_BLOCKS as u64;
        let source = Source::backfilling(window + 20);
        source
            .samples
            .lock()
            .expect("samples")
            .extend([window + 5, window + 15]);
        source.fail(3, Fault::Inconsistent, 1);
        let mut machine = Machine::new(source, Sink::default(), Vec::new());
        let stop = machine.sync_settled(Some(2), None).await.expect("backfill");
        assert_eq!(stop, Some(5));
        assert_eq!(heights(&machine), (2..=5).collect::<Vec<_>>());
        assert_eq!(reads(&machine).0, 2, "the retry samples the head again");
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
            machine.sync_settled(None, None).await,
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
        let stored = Ledger::new(ledger(4..=5), true, crate::window::UndoWindow::DEFAULT);
        let mut machine = Machine::new(source, Sink::default(), stored);
        fails_with(
            machine.sync_settled(None, None).await,
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
            machine.sync_settled(None, None).await,
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
            machine.sync_settled(Some(7), None).await,
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
            machine
                .sync_settled(Some(start), Some(8))
                .await
                .expect("sync");
            assert_eq!(heights(&machine), [6, 7, 8], "start {start}");
        }
    }
}
