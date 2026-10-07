//! Live progress for storage commits.
//!
//! The channel reports each envelope and each successful flush. This module decides
//! whether that flush is a line of its own, part of a one-second catch-up summary, or
//! quiet, and it logs a reorg on its own. Finality-only batches are not progress.

use std::time::{Duration, Instant};

use alloy_primitives::B256;
use tracing::info;

use crate::wire::envelope::{ChainId, Envelope, Event};

/// How far behind the sampled head still counts as caught up.
///
/// At or under this, every store commit is logged. Past it, commits collapse into one
/// line per [`SUMMARY_EVERY`].
const NEAR_HEAD_BLOCKS: u64 = 8;

/// The most often a catch-up summary is logged.
const SUMMARY_EVERY: Duration = Duration::from_secs(1);

/// No canonical head has been sampled yet.
pub(crate) const HEAD_UNKNOWN: u64 = u64::MAX;

/// The head height ingest stored, or `None` before the first sample.
pub(crate) fn sampled_head(raw: u64) -> Option<u64> {
    (raw != HEAD_UNKNOWN).then_some(raw)
}

/// Commit progress for one storage task.
#[derive(Debug, Default)]
pub(crate) struct Progress {
    open: CommitStats,
    window: Window,
}

impl Progress {
    /// Notes one envelope of the commit currently being written.
    pub(crate) fn note(&mut self, envelope: &Envelope) {
        self.open.note(envelope);
    }

    /// The commit landed. Logs any reorg in it, then the progress line if one is due.
    pub(crate) fn committed(
        &mut self,
        rows: u64,
        elapsed_ms: u64,
        head: Option<u64>,
        now: Instant,
    ) {
        let stats = std::mem::take(&mut self.open);
        log_reorgs(&stats);
        if let Some(commit) = stats.into_commit(rows, elapsed_ms) {
            for line in self.window.observe(commit, head, now).into_iter().flatten() {
                line.log();
            }
        }
    }

    /// Logs a catch-up summary still held when storage stops.
    pub(crate) fn finish(&mut self, head: Option<u64>, now: Instant) {
        if let Some(line) = self.window.take(head, true, now) {
            line.log();
        }
    }
}

/// One block's identity inside a commit, plus a reorg that committed with it.
#[derive(Debug, Default)]
struct CommitStats {
    chain: Option<ChainId>,
    from: Option<u64>,
    to: Option<u64>,
    tip_hash: B256,
    blocks: u64,
    reorgs: Vec<LoggedReorg>,
}

#[derive(Debug)]
struct LoggedReorg {
    height: u64,
    orphaned: usize,
    new_head: B256,
}

/// A commit that contained at least one block.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Commit {
    chain: ChainId,
    from: u64,
    to: u64,
    tip_hash: B256,
    blocks: u64,
    rows: u64,
    elapsed_ms: u64,
}

impl CommitStats {
    fn note(&mut self, envelope: &Envelope) {
        if self.chain.is_none() {
            self.chain = Some(envelope.chain.clone());
        }
        match &envelope.event {
            Event::Block(block) => self.observe_block(block.number, block.hash),
            Event::Transaction(transaction) => {
                self.observe_block(transaction.block_number, transaction.block_hash);
            }
            Event::Receipt(receipt) => {
                self.observe_block(receipt.block_number, receipt.block_hash);
            }
            Event::Log(log) => self.observe_block(log.block_number, log.block_hash),
            Event::Decoded(decoded) => self.observe_block(decoded.block_number, decoded.block_hash),
            Event::Contract(contract) => {
                self.observe_block(contract.block_number, contract.block_hash);
            }
            Event::AcceptedBlock(accepted) => self.observe_block(accepted.height, accepted.hash),
            Event::Reorg(reorg) => self.reorgs.push(LoggedReorg {
                height: reorg.height,
                orphaned: reorg.orphaned_hashes.len(),
                new_head: reorg.new_head_hash,
            }),
        }
    }

    /// Records one block inside the commit. Further rows from the same block do not
    /// count as another block, so a logs-only commit still reports a single height.
    fn observe_block(&mut self, number: u64, hash: B256) {
        let same = self.to == Some(number) && self.tip_hash == hash;
        if !same {
            self.blocks += 1;
        }
        self.from = Some(self.from.map_or(number, |from| from.min(number)));
        if self.to.is_none_or(|to| number >= to) {
            self.to = Some(number);
            self.tip_hash = hash;
        }
    }

    /// A block commit. Finality-only batches return `None`: they are not progress.
    fn into_commit(self, rows: u64, elapsed_ms: u64) -> Option<Commit> {
        Some(Commit {
            chain: self.chain?,
            from: self.from?,
            to: self.to?,
            tip_hash: self.tip_hash,
            blocks: self.blocks,
            rows,
            elapsed_ms,
        })
    }
}

fn log_reorgs(stats: &CommitStats) {
    let Some(chain) = &stats.chain else {
        return;
    };
    for reorg in &stats.reorgs {
        info!(
            chain = %chain,
            height = reorg.height,
            orphaned = reorg.orphaned,
            new_head = %reorg.new_head,
            "reorg"
        );
    }
}

/// Progress lines waiting to be logged. Catch-up totals sit here until a second has passed.
#[derive(Debug, Default)]
struct Window {
    last_emit: Option<Instant>,
    chain: Option<ChainId>,
    from: u64,
    to: u64,
    tip_hash: B256,
    blocks: u64,
    rows: u64,
    elapsed_ms: u64,
}

/// One progress line.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ProgressLine {
    summary: bool,
    chain: ChainId,
    from: u64,
    to: u64,
    tip_hash: B256,
    blocks: u64,
    rows: u64,
    elapsed_ms: u64,
    lag: Option<u64>,
}

impl Window {
    /// Folds `commit` into the cadence and returns the lines that should be logged now.
    ///
    /// The first line, when present, is a catch-up summary. The second is this commit
    /// on its own, which happens only when the tip is within [`NEAR_HEAD_BLOCKS`].
    fn observe(
        &mut self,
        commit: Commit,
        head: Option<u64>,
        now: Instant,
    ) -> [Option<ProgressLine>; 2] {
        let near = head.is_some_and(|head| head.saturating_sub(commit.to) <= NEAR_HEAD_BLOCKS);
        if near {
            let summary = self.take(head, true, now);
            self.last_emit = Some(now);
            return [summary, Some(ProgressLine::commit(commit, head))];
        }
        self.absorb(&commit);
        let due = self
            .last_emit
            .is_none_or(|then| now.saturating_duration_since(then) >= SUMMARY_EVERY);
        if due {
            [self.take(head, true, now), None]
        } else {
            [None, None]
        }
    }

    fn absorb(&mut self, commit: &Commit) {
        if self.blocks == 0 {
            self.chain = Some(commit.chain.clone());
            self.from = commit.from;
        } else {
            self.from = self.from.min(commit.from);
        }
        self.to = commit.to;
        self.tip_hash = commit.tip_hash;
        self.blocks += commit.blocks;
        self.rows += commit.rows;
        self.elapsed_ms = self.elapsed_ms.saturating_add(commit.elapsed_ms);
    }

    fn take(&mut self, head: Option<u64>, summary: bool, now: Instant) -> Option<ProgressLine> {
        if self.blocks == 0 {
            return None;
        }
        let chain = self.chain.take()?;
        let line = ProgressLine {
            summary,
            chain,
            from: self.from,
            to: self.to,
            tip_hash: self.tip_hash,
            blocks: self.blocks,
            rows: self.rows,
            elapsed_ms: self.elapsed_ms,
            lag: head.map(|head| head.saturating_sub(self.to)),
        };
        self.blocks = 0;
        self.rows = 0;
        self.elapsed_ms = 0;
        self.last_emit = Some(now);
        Some(line)
    }
}

impl ProgressLine {
    fn commit(commit: Commit, head: Option<u64>) -> Self {
        Self {
            summary: false,
            lag: head.map(|head| head.saturating_sub(commit.to)),
            chain: commit.chain,
            from: commit.from,
            to: commit.to,
            tip_hash: commit.tip_hash,
            blocks: commit.blocks,
            rows: commit.rows,
            elapsed_ms: commit.elapsed_ms,
        }
    }

    fn log(&self) {
        if self.summary {
            self.emit("catching up");
        } else {
            self.emit("committed");
        }
    }

    fn emit(&self, message: &'static str) {
        if let Some(lag) = self.lag {
            info!(
                chain = %self.chain,
                from = self.from,
                to = self.to,
                blocks = self.blocks,
                block_hash = %self.tip_hash,
                rows = self.rows,
                commit_ms = self.elapsed_ms,
                lag,
                "{message}"
            );
        } else {
            info!(
                chain = %self.chain,
                from = self.from,
                to = self.to,
                blocks = self.blocks,
                block_hash = %self.tip_hash,
                rows = self.rows,
                commit_ms = self.elapsed_ms,
                "{message}"
            );
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::time::{Duration, Instant};

    use alloy_primitives::B256;

    use crate::wire::envelope::{Block, ChainId, Envelope, Event, Log, Reorg};

    use super::{Commit, CommitStats, Window};

    fn commit_at(height: u64, rows: u64) -> Commit {
        Commit {
            chain: ChainId::new("base"),
            from: height,
            to: height,
            tip_hash: B256::from([0xab; 32]),
            blocks: 1,
            rows,
            elapsed_ms: 10,
        }
    }

    /// Near the head every commit is a line. Far behind, commits inside one second
    /// collapse, and the held line is emitted when the tip arrives or the second elapses.
    #[test]
    fn progress_is_per_commit_near_the_head_and_summarized_while_behind() {
        let start = Instant::now();
        let mut progress = Window::default();
        let head = Some(10_000);

        let first = progress.observe(commit_at(100, 10), head, start);
        let summary = first[0]
            .as_ref()
            .expect("the first catch-up commit is visible");
        assert!(summary.summary);
        assert_eq!(
            (summary.from, summary.to, summary.blocks, summary.rows),
            (100, 100, 1, 10)
        );
        assert_eq!(summary.lag, Some(9_900));
        assert!(first[1].is_none());

        let quiet = progress.observe(commit_at(101, 12), head, start + Duration::from_millis(10));
        assert_eq!(quiet, [None, None]);

        let later = progress.observe(commit_at(140, 8), head, start + Duration::from_secs(1));
        let summary = later[0]
            .as_ref()
            .expect("a second later the held commits flush");
        assert_eq!(
            (
                summary.from,
                summary.to,
                summary.blocks,
                summary.rows,
                summary.elapsed_ms
            ),
            (101, 140, 2, 20, 20)
        );
        assert!(later[1].is_none());

        let mut progress = Window::default();
        let _ = progress.observe(commit_at(100, 5), Some(1_000), start);
        let quiet = progress.observe(
            commit_at(101, 5),
            Some(1_000),
            start + Duration::from_millis(10),
        );
        assert_eq!(quiet, [None, None]);
        let near = progress.observe(
            commit_at(998, 7),
            Some(1_000),
            start + Duration::from_millis(20),
        );
        let flushed = near[0]
            .as_ref()
            .expect("pending catch-up flushes at the tip");
        assert_eq!(flushed.to, 101);
        let commit = near[1].as_ref().expect("a near commit is its own line");
        assert!(!commit.summary);
        assert_eq!((commit.to, commit.lag), (998, Some(2)));

        let again = progress.observe(
            commit_at(999, 7),
            Some(1_000),
            start + Duration::from_millis(30),
        );
        assert!(again[0].is_none());
        assert_eq!(again[1].as_ref().map(|line| line.to), Some(999));
    }

    /// An empty commit is not progress, and a reorg is reported by its orphan count
    /// rather than as a progress line.
    #[test]
    fn control_batches_are_not_block_progress() {
        let stats = CommitStats::default();
        assert!(stats.into_commit(0, 1).is_none(), "nothing is not progress");

        let block = Block {
            number: 8,
            hash: B256::from([0xcd; 32]),
            ..Block::default()
        };
        let mut stats = CommitStats::default();
        stats.note(&Envelope::new(
            ChainId::new("base"),
            Event::Reorg(Reorg {
                height: 7,
                new_head_hash: B256::from([0xcd; 32]),
                orphaned_hashes: vec![B256::from([0x01; 32]), B256::from([0x02; 32])],
            }),
        ));
        stats.note(&Envelope::new(
            ChainId::new("base"),
            Event::Block(Box::new(block)),
        ));
        assert_eq!(stats.reorgs.len(), 1);
        assert_eq!(stats.reorgs[0].orphaned, 2);
        let commit = stats.into_commit(3, 4).expect("the block is progress");
        assert_eq!(
            (commit.from, commit.to, commit.rows, commit.tip_hash),
            (8, 8, 3, B256::from([0xcd; 32]),)
        );

        let mut stats = CommitStats::default();
        let hash = B256::from([0xcd; 32]);
        for _ in 0..2 {
            stats.note(&Envelope::new(
                ChainId::new("base"),
                Event::Log(Box::new(Log {
                    block_number: 8,
                    block_hash: hash,
                    ..Log::default()
                })),
            ));
        }
        let commit = stats.into_commit(2, 4).expect("logs are progress");
        assert_eq!(
            (commit.from, commit.to, commit.blocks, commit.tip_hash),
            (8, 8, 1, hash)
        );
    }
}
