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
/// it reaches past what the run can retract and stops it.
#[derive(Debug, Default)]
pub struct Ledger {
    /// The accepted blocks, oldest first.
    pub blocks: Vec<BlockMeta>,
    /// Whether some store holds blocks below the oldest of [`Self::blocks`].
    pub history_below: bool,
}

/// A ledger as one store's restore reads it back: its newest [`LEDGER_WINDOW`] blocks,
/// so one that long may have history below it, and a shorter one is all the store holds.
/// A ledger cut any other way — as resuming several stores cuts one — says so itself.
impl From<Vec<BlockMeta>> for Ledger {
    fn from(blocks: Vec<BlockMeta>) -> Self {
        Self {
            history_below: blocks.len() >= LEDGER_WINDOW,
            blocks,
        }
    }
}
