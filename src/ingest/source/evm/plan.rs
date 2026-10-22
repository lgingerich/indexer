//! What a fetch asks the node for, decided once from the datasets.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::ingest::source::Datasets;

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
/// A range the provider fails on is fetched in halves, and the span drops to the half's
/// size, so later ranges do not fail the same way first. [`GROW_AFTER`] successes in a
/// row double it, up to [`RANGE_LIMIT`]. That keeps both causes of a refusal cheap: a
/// provider's fixed batch cap is found once and held to within a factor of two, and a
/// stretch of chain too busy to serve whole shrinks the span only until quieter heights
/// let it grow back. Every fetch in flight shares it, so one refusal sizes the others.
/// It is not kept across restarts; a new process finds it again in a few failures.
#[derive(Debug)]
pub(super) struct Span {
    pub(super) heights: AtomicU64,
    /// Successful fetches since the last change.
    pub(super) streak: AtomicU32,
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

    /// Records a failed range, whose first half of `heights` is fetched next.
    pub(super) fn shrink(&self, heights: u64) {
        self.streak.store(0, Ordering::Relaxed);
        self.heights.fetch_min(heights.max(1), Ordering::Relaxed);
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
/// | `logs` | none when a live head matches, else hashes only | — | `eth_getLogs` | ranged |
/// | `blocks` | hashes only | — | — | ranged |
/// | `blocks`, `logs` | hashes only | — | `eth_getLogs` | ranged |
/// | `transactions`, with any others | full | `eth_getBlockReceipts` | receipts | per height |
///
/// Receipts are fetched only for transactions, because each transaction row is joined
/// to its receipt. A node without `eth_getBlockReceipts` is answered per transaction,
/// which is a fallback inside the receipts step rather than another plan.
///
/// A plan is ranged when every selected dataset can be served for a range of heights at
/// once: headers as one batch, logs as one `eth_getLogs` over the range. Receipts are per
/// block, so `transactions` is never ranged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FetchPlan {
    /// Project the block row. Its header is read whenever the plan reads a block.
    pub(super) block_row: bool,
    /// Read full transaction objects and the block's receipts, and project each
    /// transaction joined to its receipt.
    pub(super) transactions: bool,
    /// Where the logs come from, if they are selected.
    pub(super) logs: LogSource,
    /// A live notification for the height stands in for the block read, because
    /// nothing selected reads the body.
    pub(super) reuse_head: bool,
}

impl FetchPlan {
    pub(super) const fn new(datasets: Datasets) -> Self {
        let logs = match (datasets.logs, datasets.transactions) {
            (false, _) => LogSource::Skip,
            (true, true) => LogSource::Receipts,
            (true, false) => LogSource::GetLogs,
        };
        Self {
            block_row: datasets.blocks,
            transactions: datasets.transactions,
            logs,
            reuse_head: datasets.logs && !datasets.blocks && !datasets.transactions,
        }
    }

    /// Whether buried backfill fetches heights a range at a time: whenever transactions,
    /// whose receipts are per block, are not selected.
    pub(super) const fn ranged(self) -> bool {
        !self.transactions
    }
}

#[cfg(test)]
mod tests {
    /// The span grows back after enough ranges in a row are served, up to the limit.
    #[test]
    fn the_span_doubles_after_a_streak_and_stops_at_the_limit() {
        let span = super::Span::new();
        span.shrink(13);
        span.shrink(40);
        assert_eq!(span.get(), 13, "a shrink never grows the span");
        for _ in 1..super::GROW_AFTER {
            span.served();
        }
        assert_eq!(span.get(), 13);
        span.served();
        assert_eq!(span.get(), 26);
        for _ in 0..super::GROW_AFTER * 4 {
            span.served();
        }
        assert_eq!(span.get(), super::RANGE_LIMIT);
    }
}
