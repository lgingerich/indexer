//! The undo window: how many unfinalized blocks a run can retract, and the ledger of
//! accepted blocks a restart rebuilds it from.
//!
//! Its own module because both sides of the stream size themselves by it. Ingest keeps
//! the window and retracts forks within it; every store keeps the ledger a restart reads
//! back and prunes it to this length, and the stores' resume hands ingest the
//! [`Ledger`] it starts from. Defined here, below both, neither depends on the other for
//! it.

use crate::wire::envelope::BlockMeta;

/// Hardcoded budget for the recovered undo window.
///
/// The window is a recovery floor, not a finality claim: a fork deeper than this
/// stops with [`PipelineError::UndoWindowExceeded`](crate::ingest::PipelineError::UndoWindowExceeded)
/// rather than retracting a suffix the pipeline can no longer identify. Identities cost
/// nothing beside the payloads they describe, so the value is a bounded-memory choice,
/// not a tuning knob.
pub const MAX_UNFINALIZED_BLOCKS: usize = 4096;

/// How many accepted blocks a store hands back to a restart: the undo window and the
/// block below it, which the oldest unfinalized block's parent hash is checked against.
pub const LEDGER_WINDOW: usize = MAX_UNFINALIZED_BLOCKS + 1;

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
}

impl Ledger {
    /// A ledger of `blocks`, oldest first, which some store holds history below when
    /// `history_below` is set. With no blocks there is nothing to sit below, so the flag
    /// is dropped: the ledger is a fresh start either way.
    #[must_use]
    pub fn new(blocks: Vec<BlockMeta>, history_below: bool) -> Self {
        Self {
            history_below: history_below && !blocks.is_empty(),
            blocks,
        }
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

/// A ledger as one store's restore reads it back: its newest [`LEDGER_WINDOW`] blocks,
/// so one that long may have history below it, and a shorter one is all the store holds.
/// A ledger cut any other way — as resuming several stores cuts one — says so itself,
/// through [`Ledger::new`].
impl From<Vec<BlockMeta>> for Ledger {
    fn from(blocks: Vec<BlockMeta>) -> Self {
        let history_below = blocks.len() >= LEDGER_WINDOW;
        Self::new(blocks, history_below)
    }
}
