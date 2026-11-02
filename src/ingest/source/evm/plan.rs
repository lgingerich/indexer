//! What a fetch asks the node for, decided once from the datasets.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::wire::datasets::Datasets;

/// The most calls one JSON-RPC batch carries.
///
/// Providers limit requests per second and count each call in a batch against that
/// limit. Over it, the batch comes back with its first calls answered and the rest
/// refused, so a batch larger than the limit fails the same way however often it is
/// retried. `mainnet.base.org` and a Chainstack plan both refused every call past the
/// 25th in one batch; ten leaves room for pacing and the requests around it. The
/// per-transaction receipt fallback requests hashes in chunks this size.
pub(super) const BATCH_LIMIT: usize = 10;

/// The most heights a ranged backfill fetches at once: their headers, with one log
/// query, in one batch.
///
/// A ceiling, not a target: [`Span`] starts here and shrinks to what the provider
/// serves. Larger than [`BATCH_LIMIT`], since a range that is refused is fetched in
/// halves until it fits, where a receipt chunk is not. Envio's `HyperRPC` serves a
/// hundred headers and a log query in one batch; `mainnet.base.org` and a Chainstack
/// plan refuse it, and the span settles at what they serve.
pub(super) const RANGE_LIMIT: u64 = 100;

/// Successful range fetches in a row after which [`Span`] doubles.
///
/// Growing probes the provider: against a fixed batch cap, roughly one fetch in this
/// many is refused and halved again. Larger probes less often and recovers from a busy
/// stretch more slowly.
pub(super) const GROW_AFTER: u32 = 20;

/// How many heights a ranged backfill fetch covers, learned from the provider.
///
/// One ladder, fed once by each failure: a range the provider fails on is
/// [`refused`](Self::refused), which drops the span to half the range, and the range is
/// fetched again in pieces of the span, so later ranges do not fail the same way first.
/// [`GROW_AFTER`] successes in a row double it, up to [`RANGE_LIMIT`]. That keeps both
/// causes of a refusal cheap: a provider's fixed batch cap is found once and held to
/// within a factor of two, and a stretch of chain too busy to serve whole shrinks the
/// span only until quieter heights let it grow back. Every fetch in flight shares it, so
/// one refusal sizes the others; a range in flight from before that refusal, wider than
/// the span, fails for the reason already learned, so it is cut to the span without
/// shrinking it again or counting against its growth. It is not kept across restarts; a
/// new process finds it again in a few failures.
#[derive(Debug)]
pub(super) struct Span {
    /// The heights the next range covers; at least one.
    heights: AtomicU64,
    /// Successful fetches since the last change.
    streak: AtomicU32,
}

impl Span {
    pub(super) const fn new() -> Self {
        Self {
            heights: AtomicU64::new(RANGE_LIMIT),
            streak: AtomicU32::new(0),
        }
    }

    /// The heights the next range should cover; at least one.
    pub(super) fn get(&self) -> u64 {
        self.heights.load(Ordering::Relaxed)
    }

    /// Records a failed range of `heights`, at least two, and returns how many to fetch
    /// it in at once, which is fewer.
    ///
    /// A range no wider than the span shrinks it to the range's first half, rounded up.
    /// A wider one was cut before a refusal that shrank the span already, so it teaches
    /// nothing new: the span is kept, streak and all, and the range is fetched in pieces
    /// of it.
    pub(super) fn refused(&self, heights: u64) -> u64 {
        let span = self.get();
        if heights > span {
            return span;
        }
        let half = heights.div_ceil(2);
        self.streak.store(0, Ordering::Relaxed);
        self.heights.fetch_min(half, Ordering::Relaxed).min(half)
    }

    /// Records a served range, doubling the span after [`GROW_AFTER`] in a row.
    pub(super) fn served(&self) {
        if self.streak.fetch_add(1, Ordering::Relaxed) + 1 >= GROW_AFTER {
            self.streak.store(0, Ordering::Relaxed);
            // Doubled in one step, so a shrink racing it is not overwritten.
            let _ = self
                .heights
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |heights| {
                    Some(heights.saturating_mul(2).min(RANGE_LIMIT))
                });
        }
    }
}

/// Where the selected logs come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LogSource {
    /// Logs are not selected.
    Skip,
    /// The receipts fetched for transactions, which already carry every log.
    Receipts,
    /// A separate `eth_getLogs` for the height.
    GetLogs,
}

/// The RPC calls one height needs, decided once from the dataset selection.
///
/// This is the whole fetch policy; the fetch path and the projection read it rather
/// than the selection, so the table below is the one place the choice is made:
///
/// | Selected | Block read | Receipts | Logs from | Buried backfill |
/// | --- | --- | --- | --- | --- |
/// | none | hashes only | — | — | ranged |
/// | `logs` | none when a live head matches, else hashes only | — | `eth_getLogs` | ranged |
/// | `blocks`, with or without `logs` | hashes only | — | `eth_getLogs`, if selected | ranged |
/// | `transactions`, with any others | full | `eth_getBlockReceipts` | receipts | per height |
///
/// No dataset is selected when every store writes only the bookkeeping tables, which a
/// block's metadata fills: its headers are still read, for linkage and the ledger.
///
/// Receipts are fetched only for transactions, because each transaction row is joined
/// to its receipt. A node without `eth_getBlockReceipts` is answered per transaction,
/// which is a fallback inside the receipts step rather than another plan.
///
/// A plan is ranged when every selected dataset can be served for a range of heights at
/// once: headers as one batch, logs as one `eth_getLogs` over the range. Receipts are per
/// block, so `transactions` is never ranged.
///
/// Its fields are set only by [`Self::new`], so no plan disagrees with the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FetchPlan {
    block_row: bool,
    transactions: bool,
    logs: LogSource,
    reuse_head: bool,
}

impl FetchPlan {
    pub(super) const fn new(datasets: Datasets) -> Self {
        let Datasets {
            blocks,
            transactions,
            logs,
        } = datasets;
        let logs_from = match (logs, transactions) {
            (false, _) => LogSource::Skip,
            (true, true) => LogSource::Receipts,
            (true, false) => LogSource::GetLogs,
        };
        Self {
            block_row: blocks,
            transactions,
            logs: logs_from,
            reuse_head: logs && !blocks && !transactions,
        }
    }

    /// Whether the block row is projected. Its header is read whenever the plan reads a
    /// block.
    pub(super) const fn block_row(self) -> bool {
        self.block_row
    }

    /// Whether full transaction objects and the block's receipts are read, and each
    /// transaction projected joined to its receipt.
    pub(super) const fn transactions(self) -> bool {
        self.transactions
    }

    /// Where the logs come from, if they are selected.
    pub(super) const fn logs(self) -> LogSource {
        self.logs
    }

    /// Whether a live notification for the height stands in for the block read: only
    /// when nothing selected reads the body but logs, which a hash-pinned query fetches.
    pub(super) const fn reuse_head(self) -> bool {
        self.reuse_head
    }

    /// Whether buried backfill fetches heights a range at a time: whenever transactions,
    /// whose receipts are per block, are not selected.
    pub(super) const fn ranged(self) -> bool {
        !self.transactions
    }
}

#[cfg(test)]
mod tests {
    use super::{FetchPlan, GROW_AFTER, LogSource, RANGE_LIMIT, Span};
    use crate::wire::datasets::Datasets;

    /// The span grows back after enough ranges in a row are served, up to the limit.
    #[test]
    fn the_span_doubles_after_a_streak_and_stops_at_the_limit() {
        let span = Span::new();
        assert_eq!(span.refused(25), 13, "a refused range is fetched in halves");
        assert_eq!(span.refused(40), 13, "a refusal never grows the span");
        assert_eq!(span.get(), 13);
        for _ in 1..GROW_AFTER {
            span.served();
        }
        assert_eq!(span.get(), 13);
        span.served();
        assert_eq!(span.get(), 26);
        for _ in 0..GROW_AFTER * 4 {
            span.served();
        }
        assert_eq!(span.get(), RANGE_LIMIT);
    }

    /// A range cut before the span shrank fails for the reason already learned: it is
    /// fetched in pieces of the span, which neither shrinks again nor loses its streak.
    #[test]
    fn a_failure_wider_than_the_span_is_counted_once() {
        let span = Span::new();
        assert_eq!(span.refused(RANGE_LIMIT), RANGE_LIMIT / 2);
        for _ in 1..GROW_AFTER {
            span.served();
        }
        assert_eq!(span.refused(RANGE_LIMIT), RANGE_LIMIT / 2);
        span.served();
        assert_eq!(
            span.get(),
            RANGE_LIMIT,
            "the streak survived the stale failure"
        );

        assert_eq!(span.refused(2), 1);
        assert_eq!(span.get(), 1);
    }

    /// Each selection lands on its row of the plan's table, and the row answers for
    /// every call the fetch path makes.
    #[test]
    fn every_selection_has_its_row() {
        use LogSource::{GetLogs, Receipts, Skip};
        // Blocks, transactions, and logs selected; then whether the plan projects the
        // block row, reads transactions, where its logs come from, whether it reuses a
        // live head, and whether it is ranged.
        let cases = [
            ((false, false, false), (false, false, Skip, false, true)),
            ((false, false, true), (false, false, GetLogs, true, true)),
            ((true, false, false), (true, false, Skip, false, true)),
            ((true, false, true), (true, false, GetLogs, false, true)),
            ((false, true, false), (false, true, Skip, false, false)),
            ((false, true, true), (false, true, Receipts, false, false)),
            ((true, true, false), (true, true, Skip, false, false)),
            ((true, true, true), (true, true, Receipts, false, false)),
        ];
        for ((blocks, transactions, logs), expected) in cases {
            let plan = FetchPlan::new(Datasets {
                blocks,
                transactions,
                logs,
            });
            let answers = (
                plan.block_row(),
                plan.transactions(),
                plan.logs(),
                plan.reuse_head(),
                plan.ranged(),
            );
            assert_eq!(answers, expected, "{plan:?}");
        }
    }
}
