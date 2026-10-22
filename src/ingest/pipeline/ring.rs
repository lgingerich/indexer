//! The undo window a fork is retracted against, and the ledger a restart rebuilds it
//! from.

use std::collections::VecDeque;

use alloy_primitives::B256;
use tracing::warn;

use crate::window::{Ledger, MAX_UNFINALIZED_BLOCKS};
use crate::wire::envelope::BlockMeta;

/// Accepted chain identities: a recovery predecessor and an ordered sliding tail.
#[derive(Debug, Default)]
pub(super) struct UndoRing {
    /// Oldest recoverable identity, kept only as a linkage floor. The last entry
    /// evicted from the tail lands here.
    floor: Option<BlockMeta>,
    /// Published tail, oldest first, at most [`MAX_UNFINALIZED_BLOCKS`] identities.
    entries: VecDeque<BlockMeta>,
    /// Whether stored rows may sit below the oldest entry: the restored ledger said so, or
    /// lost older blocks that did not link to the rest.
    truncated: bool,
}

impl UndoRing {
    /// Returns the newest accepted block, or `None` before any block is accepted.
    pub(super) fn tip(&self) -> Option<BlockMeta> {
        self.entries.back().copied()
    }

    /// The oldest identity the window still remembers: the floor, or the oldest tail
    /// entry when nothing has been evicted yet. `None` before any block is accepted.
    pub(super) fn oldest(&self) -> Option<BlockMeta> {
        self.floor.or_else(|| self.entries.front().copied())
    }

    /// Looks up a remembered identity: the tail, or the floor boundary.
    /// Absence means this height is no longer retained, not that it is noncanonical.
    ///
    /// Scans newest first: lookups are for live heads and fork walks, both at or near
    /// the tip, so they stop after a few entries rather than crossing the window.
    pub(super) fn at(&self, height: u64) -> Option<BlockMeta> {
        if let Some(floor) = self.floor
            && floor.height == height
        {
            return Some(floor);
        }
        self.entries
            .iter()
            .rev()
            .find(|meta| meta.height == height)
            .copied()
    }

    /// Whether the window still holds the first block this run's history ever indexed:
    /// nothing has slid into the floor, the restored ledger had no history below it, and
    /// no older part of it was dropped. Then nothing is stored below the oldest entry.
    pub(super) fn holds_whole_history(&self) -> bool {
        self.floor.is_none() && !self.truncated
    }

    /// Whether `height` is still inside the retained window, so a backward walk may
    /// still find an ancestor there.
    pub(super) fn retains(&self, height: u64) -> bool {
        self.oldest().is_some_and(|oldest| height >= oldest.height)
    }

    /// Collects hashes above the ancestor at height `ancestor`, newest first.
    /// Does not remove them: the sink must accept their retraction before rewind.
    ///
    /// Stops at the first entry at or below the ancestor, which relies on `entries`
    /// ascending by height: `push` appends, and every block is accepted only after it
    /// links to the tip, one height above it.
    pub(super) fn orphaned(&self, ancestor: u64) -> Vec<B256> {
        self.entries
            .iter()
            .rev()
            .take_while(|meta| meta.height > ancestor)
            .map(|meta| meta.hash)
            .collect()
    }

    /// Removes the suffix above the ancestor at height `ancestor` after retraction is
    /// accepted. Order-independent, unlike [`Self::orphaned`]: every entry above the
    /// height goes.
    pub(super) fn rewind(&mut self, ancestor: u64) {
        self.entries.retain(|meta| meta.height <= ancestor);
    }

    /// Appends an accepted identity, sliding the window: the oldest tail entry
    /// becomes the floor when the window is full.
    pub(super) fn push(&mut self, meta: BlockMeta) {
        if self.entries.len() >= MAX_UNFINALIZED_BLOCKS
            && let Some(evicted) = self.entries.pop_front()
        {
            self.floor = Some(evicted);
        }
        self.entries.push_back(meta);
    }
}

/// The window a restart starts from: the newest parent-linked suffix of `ledger`,
/// marked truncated when stored rows may sit below its oldest block.
impl From<Ledger> for UndoRing {
    fn from(ledger: Ledger) -> Self {
        let Ledger {
            blocks,
            history_below,
        } = ledger;
        let stored = blocks.len();
        let linked = linked_suffix(blocks);
        let mut ring = Self {
            truncated: history_below || linked.len() < stored,
            ..Self::default()
        };
        for meta in linked {
            ring.push(meta);
        }
        ring
    }
}

/// The newest parent-linked run of `ledger`, both oldest first.
///
/// A stored ledger is contiguous unless something outside the pipeline broke it — an
/// older run that did not resume, or a hand-edited store. Linking across that gap would
/// let a fork walk "find" an ancestor that never preceded the tip, so the window starts
/// above it instead, and the operator is told.
fn linked_suffix(mut ledger: Vec<BlockMeta>) -> Vec<BlockMeta> {
    let Some(mut newer) = ledger.last().copied() else {
        return ledger;
    };
    let mut start = ledger.len() - 1;
    for (index, meta) in ledger.iter().enumerate().rev().skip(1) {
        if meta.height.checked_add(1) != Some(newer.height) || meta.hash != newer.parent_hash {
            break;
        }
        newer = *meta;
        start = index;
    }
    if start > 0 {
        warn!(
            dropped = start,
            kept = ledger.len() - start,
            from = newer.height,
            "the stored ledger is not contiguous; resuming with its newest linked blocks"
        );
    }
    ledger.drain(..start);
    ledger
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::ingest::pipeline::Machine;
    use crate::ingest::pipeline::testing::*;
    use crate::wire::envelope::BlockMeta;

    /// Only the newest run of a stored ledger whose heights and parent hashes link is
    /// kept, and a window built from a ledger that lost its older part is marked
    /// truncated. A whole stored window's worth is kept whole, its oldest as the floor,
    /// and is marked as having history below too: a restore reads no more than that.
    #[test]
    fn only_the_newest_linked_suffix_of_a_stored_ledger_is_kept() {
        let kept = |stored: Vec<BlockMeta>| {
            linked_suffix(stored)
                .iter()
                .map(|meta| meta.height)
                .collect::<Vec<_>>()
        };
        let mut gapped = ledger(0..=2);
        gapped.extend(ledger(5..=8));
        assert_eq!(kept(gapped.clone()), [5, 6, 7, 8], "a height gap");
        let mut relinked = ledger(0..=8);
        relinked[5].parent_hash = hash(99);
        assert_eq!(
            kept(relinked),
            [5, 6, 7, 8],
            "a parent that is not the block below"
        );
        assert_eq!(kept(ledger(3..=3)), [3], "a single block");
        assert!(linked_suffix(Vec::new()).is_empty());

        let machine = Machine::new(Source::linear(0), Sink::default(), gapped);
        assert!(machine.ring.truncated);
        assert_eq!(machine.ring.oldest(), Some(meta(5)));

        let window = MAX_UNFINALIZED_BLOCKS as u64;
        let machine = Machine::new(Source::linear(0), Sink::default(), ledger(0..=window));
        assert!(machine.ring.truncated);
        assert_eq!(machine.ring.entries.len(), MAX_UNFINALIZED_BLOCKS);
        assert_eq!(machine.ring.floor, Some(meta(0)));
        assert_eq!(machine.tip(), Some(meta(window)));
    }

    /// The window holds the whole history until it is full; the next block slides the
    /// oldest into the floor, still addressable for linkage, and the one after that
    /// moves the floor up past it.
    #[test]
    fn the_ring_slides_and_keeps_a_recoverable_floor() {
        let window = MAX_UNFINALIZED_BLOCKS as u64;
        let mut ring = UndoRing::default();
        for height in 0..window {
            ring.push(meta(height));
        }
        assert!(ring.holds_whole_history());
        assert_eq!(ring.floor, None);
        assert_eq!(ring.oldest(), Some(meta(0)));

        ring.push(meta(window));
        assert!(
            !ring.holds_whole_history(),
            "the first block slid into the floor"
        );
        assert_eq!(ring.entries.len(), MAX_UNFINALIZED_BLOCKS);
        assert_eq!(ring.floor, Some(meta(0)));
        assert_eq!(ring.entries.front(), Some(&meta(1)));
        assert_eq!(ring.tip(), Some(meta(window)));
        // The floor and the whole tail are still addressable.
        assert!(ring.retains(0));
        assert_eq!(ring.at(0), Some(meta(0)));
        assert_eq!(ring.at(1), Some(meta(1)));

        ring.push(meta(window + 1));
        assert_eq!(ring.floor, Some(meta(1)));
        assert!(!ring.retains(0));
        assert_eq!(ring.at(0), None);
    }

    #[test]
    fn the_window_retains_only_from_its_oldest_identity() {
        // A seeded ring whose floor sits above the last backfilled height: the fork
        // walk must stop rather than read below what is remembered.
        let mut ring = UndoRing {
            floor: Some(meta(2)),
            ..UndoRing::default()
        };
        for height in 3..=5 {
            ring.push(meta(height));
        }
        assert_eq!(ring.oldest(), Some(meta(2)));
        assert!(ring.retains(2));
        assert!(ring.retains(5));
        assert!(!ring.retains(1));
        assert_eq!(ring.at(2), Some(meta(2)));
        assert_eq!(ring.at(1), None);
    }
}
