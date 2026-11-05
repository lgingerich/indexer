//! The undo window: how many unfinalized blocks a run can retract, and the ledger of
//! accepted blocks a restart rebuilds it from.
//!
//! Its own module because both sides of the stream size themselves by it. Ingest keeps
//! the window and retracts forks within it; every store keeps the ledger a restart reads
//! back and prunes it to this length, and the stores' resume hands ingest the
//! [`Ledger`] it starts from. Defined here, below both, neither depends on the other for
//! it. Each chain sets its own [`UndoWindow`], in `[ingest] undo_window`.

use crate::wire::envelope::BlockMeta;

/// The largest undo window a chain may set, and the one it gets by setting none.
pub const MAX_UNFINALIZED_BLOCKS: usize = 4096;

/// The ledger of the largest undo window (see [`UndoWindow::ledger`]).
pub const LEDGER_WINDOW: usize = MAX_UNFINALIZED_BLOCKS + 1;

/// How many unfinalized blocks a run can retract, and everything sized by that.
///
/// The window is a recovery floor, not a finality claim: a fork deeper than this stops
/// with [`PipelineError::UndoWindowExceeded`](crate::ingest::PipelineError::UndoWindowExceeded)
/// rather than retracting a suffix the pipeline can no longer identify.
///
/// A restart reads back every block of it from each store, so a narrower window opens a
/// lake faster, the more so on a slow chain, whose window spans more commits. It must
/// stay deeper than any fork the chain makes, and is at least twice the blocks one lake
/// commit gathers or that commit is cut short (see [`Self::flush_blocks`]).
///
/// Narrowing it is safe: each store prunes its ledger to the new length. Widening it
/// is not, at once: a ledger shorter than the window reads as a store's whole history,
/// so until the ledgers have grown back a fork replacing the oldest block would be
/// adopted rather than stop the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UndoWindow(usize);

impl UndoWindow {
    /// [`MAX_UNFINALIZED_BLOCKS`] blocks, which suits any chain.
    pub const DEFAULT: Self = Self(MAX_UNFINALIZED_BLOCKS);

    /// A window of `blocks`; `None` for zero or more than [`MAX_UNFINALIZED_BLOCKS`].
    #[must_use]
    pub const fn new(blocks: usize) -> Option<Self> {
        if blocks == 0 || blocks > MAX_UNFINALIZED_BLOCKS {
            None
        } else {
            Some(Self(blocks))
        }
    }

    /// How many blocks a run can retract.
    #[must_use]
    pub const fn blocks(self) -> usize {
        self.0
    }

    /// How many accepted blocks a store hands back to a restart: the undo window and the
    /// block below it, which the oldest unfinalized block's parent hash is checked
    /// against.
    #[must_use]
    pub const fn ledger(self) -> usize {
        self.0 + 1
    }

    /// The most blocks one flush gathers, so a stop leaves stores at most this far apart
    /// and resume always finds a block both hand back: half the ledger.
    #[must_use]
    pub const fn flush_blocks(self) -> usize {
        self.ledger() / 2
    }

    /// How far past the slowest store's tip a store whose whole ledger lies above that
    /// tip may be, and still be rewound to it: four ledgers of blocks. A stop leaves
    /// stores at most [`Self::flush_blocks`] apart; this allows for more, and refuses a
    /// store further ahead. No delete of a running store reaches deeper than this, which
    /// is what bounds a backfill.
    #[must_use]
    pub const fn resume_lag(self) -> u64 {
        4 * self.ledger() as u64
    }
}

impl Default for UndoWindow {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// The accepted blocks a restart resumes from, and whether the stores hold blocks below
/// the oldest of them.
///
/// Which it is decides what a fork replacing the oldest block means: with nothing stored
/// below, it replaces the first block ever indexed and is adopted; with history below,
/// it reaches past what the run can retract and stops it. An empty ledger has no oldest
/// block for history to sit below, so the fields are private and [`Ledger::new`] never
/// claims history below one.
#[derive(Debug, Default)]
pub struct Ledger {
    /// The accepted blocks, oldest first.
    blocks: Vec<BlockMeta>,
    /// Whether some store holds blocks below the oldest of [`Self::blocks`]. Never set
    /// when there are none.
    history_below: bool,
    /// The window the run resuming from it keeps.
    window: UndoWindow,
}

impl Ledger {
    /// A ledger of `blocks`, oldest first, for a run keeping `window`, which some store
    /// holds history below when `history_below` is set. With no blocks there is nothing
    /// to sit below, so the flag is dropped: the ledger is a fresh start either way.
    #[must_use]
    pub fn new(blocks: Vec<BlockMeta>, history_below: bool, window: UndoWindow) -> Self {
        Self {
            history_below: history_below && !blocks.is_empty(),
            blocks,
            window,
        }
    }

    /// The window the run resuming from this ledger keeps.
    #[must_use]
    pub const fn window(&self) -> UndoWindow {
        self.window
    }

    /// The accepted blocks, oldest first.
    #[must_use]
    pub fn blocks(&self) -> &[BlockMeta] {
        &self.blocks
    }

    /// Whether some store holds blocks below the oldest of [`Self::blocks`]. Always
    /// `false` for an empty ledger.
    #[must_use]
    pub const fn history_below(&self) -> bool {
        self.history_below
    }

    /// The accepted blocks, oldest first, taking them out of the ledger.
    #[must_use]
    pub fn into_blocks(self) -> Vec<BlockMeta> {
        self.blocks
    }
}

/// A ledger as one store's restore reads it back under the default window: its newest
/// [`LEDGER_WINDOW`] blocks, so one that long may have history below it, and a shorter
/// one is all the store holds. A ledger cut any other way — as resuming several stores
/// cuts one — says so itself, through [`Ledger::new`].
impl From<Vec<BlockMeta>> for Ledger {
    fn from(blocks: Vec<BlockMeta>) -> Self {
        let history_below = blocks.len() >= LEDGER_WINDOW;
        Self::new(blocks, history_below, UndoWindow::DEFAULT)
    }
}

#[cfg(test)]
// `expect` is allowed here per the repository test style: a failed one means the fixture
// is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use super::{MAX_UNFINALIZED_BLOCKS, UndoWindow};

    /// Everything sized by the window follows it, and only a size from one block to the
    /// most allowed makes one.
    #[test]
    fn a_window_sizes_the_ledger_flushes_and_resume() {
        let window = UndoWindow::new(512).expect("in range");
        assert_eq!(window.blocks(), 512);
        assert_eq!(window.ledger(), 513);
        assert_eq!(window.flush_blocks(), 256);
        assert_eq!(window.resume_lag(), 2_052);

        assert_eq!(UndoWindow::DEFAULT.resume_lag(), 16_388);
        assert_eq!(UndoWindow::default(), UndoWindow::DEFAULT);
        assert!(UndoWindow::new(0).is_none());
        assert!(UndoWindow::new(MAX_UNFINALIZED_BLOCKS).is_some());
        assert!(UndoWindow::new(MAX_UNFINALIZED_BLOCKS + 1).is_none());
    }
}
