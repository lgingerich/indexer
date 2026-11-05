//! The undo window a fork is retracted against, and the ledger a restart rebuilds it
//! from.

use std::collections::VecDeque;

use alloy_primitives::B256;
use tracing::warn;

use crate::window::{Ledger, UndoWindow};
use crate::wire::envelope::BlockMeta;

/// Accepted chain identities: an ordered sliding tail, and what lies below it.
///
/// Its tip and its oldest identity are absent together, and only before any block is
/// accepted: a rewind to the floor leaves the floor as the tip, so no window remembers
/// an oldest block without a newest one.
#[derive(Debug, Default)]
pub(super) struct UndoRing {
    /// What the window knows of the history below [`Self::entries`].
    window: Window,
    /// Published tail above the floor, if there is one, oldest first, at most
    /// [`Self::capacity`] identities.
    entries: VecDeque<BlockMeta>,
    /// The undo window's size.
    capacity: UndoWindow,
}

/// How much of a run's history the undo window holds.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Window {
    /// Every block this run's history ever indexed, and none before the first is
    /// accepted: nothing is stored below the oldest entry, so a fork replacing it is
    /// adopted.
    #[default]
    Whole,
    /// History may be stored below `floor`, which is kept only as a linkage floor and is
    /// never retracted. A full tail slides its oldest entry here, and a restore whose
    /// ledger had history below, or lost an older part that did not link, starts with
    /// its oldest kept block here.
    Bounded {
        /// The oldest identity remembered, an accepted ancestor of every entry.
        floor: BlockMeta,
    },
}

impl UndoRing {
    /// How many blocks the window retracts at most.
    pub(super) const fn capacity(&self) -> UndoWindow {
        self.capacity
    }

    /// Returns the newest accepted block: the newest entry, or the floor when a rewind
    /// to it left nothing above. `None` only before any block is accepted, when
    /// [`Self::oldest`] is `None` too.
    pub(super) fn tip(&self) -> Option<BlockMeta> {
        self.entries.back().copied().or_else(|| self.floor())
    }

    /// The oldest identity the window still remembers: the floor, or the oldest tail
    /// entry when the window holds the whole history. `None` before any block is
    /// accepted.
    pub(super) fn oldest(&self) -> Option<BlockMeta> {
        self.floor().or_else(|| self.entries.front().copied())
    }

    /// The floor of a bounded window; a whole one has none.
    const fn floor(&self) -> Option<BlockMeta> {
        match self.window {
            Window::Whole => None,
            Window::Bounded { floor } => Some(floor),
        }
    }

    /// Looks up a remembered identity: the tail, or the floor boundary.
    /// Absence means this height is no longer retained, not that it is noncanonical.
    ///
    /// Scans newest first: lookups are for live heads and fork walks, both at or near
    /// the tip, so they stop after a few entries rather than crossing the window.
    pub(super) fn at(&self, height: u64) -> Option<BlockMeta> {
        if let Some(floor) = self.floor()
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
    /// nothing has slid into a floor, the restored ledger had no history below it, and
    /// no older part of it was dropped. Then nothing is stored below the oldest entry.
    pub(super) fn holds_whole_history(&self) -> bool {
        self.window == Window::Whole
    }

    /// Whether `height` is still inside the retained window, so a backward walk may
    /// still find an ancestor there.
    pub(super) fn retains(&self, height: u64) -> bool {
        self.oldest().is_some_and(|oldest| height >= oldest.height)
    }

    /// Collects the hashes at and above height `from`, the first height a fork replaces,
    /// newest first. Does not remove them: the sink must accept their retraction before
    /// [`Self::rewind`]. The floor is never among them.
    ///
    /// Stops at the first entry below `from`, which relies on `entries` ascending by
    /// height: `push` appends, and every block is accepted only after it links to the
    /// tip, one height above it.
    pub(super) fn orphaned(&self, from: u64) -> Vec<B256> {
        self.entries
            .iter()
            .rev()
            .take_while(|meta| meta.height >= from)
            .map(|meta| meta.hash)
            .collect()
    }

    /// Removes the entries at and above height `from` after their retraction is
    /// accepted. Order-independent, unlike [`Self::orphaned`]: every entry at or above
    /// the height goes. The floor stays, and is the tip when nothing is left above it;
    /// a whole window rewound to its oldest entry holds nothing, as before its first
    /// block.
    pub(super) fn rewind(&mut self, from: u64) {
        self.entries.retain(|meta| meta.height < from);
    }

    /// Appends an accepted identity, sliding the window: the oldest tail entry
    /// becomes the floor when the window is full.
    pub(super) fn push(&mut self, meta: BlockMeta) {
        if self.entries.len() >= self.capacity.blocks()
            && let Some(evicted) = self.entries.pop_front()
        {
            self.window = Window::Bounded { floor: evicted };
        }
        self.entries.push_back(meta);
    }
}

/// The window a restart starts from: the newest parent-linked suffix of `ledger`,
/// bounded at its oldest block when stored rows may sit below it.
impl From<Ledger> for UndoRing {
    fn from(ledger: Ledger) -> Self {
        let history_below = ledger.history_below();
        let capacity = ledger.window();
        let blocks = ledger.into_blocks();
        let stored = blocks.len();
        let linked = linked_suffix(blocks);
        let bounded = history_below || linked.len() < stored;
        let mut linked = linked.into_iter();
        let mut ring = Self {
            capacity,
            ..Self::default()
        };
        // Rows may sit below the oldest kept block, so it is the floor: no fork replacing
        // it is adopted, and none is retracted past it.
        if bounded && let Some(oldest) = linked.next() {
            ring.window = Window::Bounded { floor: oldest };
        }
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
    /// kept, and a window built from a ledger that lost its older part is bounded at its
    /// oldest kept block. A whole stored window's worth is kept whole, its oldest as the
    /// floor, and is bounded too: a restore reads no more than that. A ledger of one
    /// block with history below is bounded at it, which is both its floor and its tip.
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
        assert_eq!(machine.ring.window, Window::Bounded { floor: meta(5) });
        assert_eq!(machine.ring.oldest(), Some(meta(5)));
        assert_eq!(machine.tip(), Some(meta(8)));

        let window = crate::window::MAX_UNFINALIZED_BLOCKS as u64;
        let machine = Machine::new(Source::linear(0), Sink::default(), ledger(0..=window));
        assert_eq!(machine.ring.window, Window::Bounded { floor: meta(0) });
        assert_eq!(
            machine.ring.entries.len(),
            crate::window::MAX_UNFINALIZED_BLOCKS
        );
        assert_eq!(machine.tip(), Some(meta(window)));

        let machine = Machine::new(
            Source::linear(0),
            Sink::default(),
            Ledger::new(ledger(3..=3), true, UndoWindow::DEFAULT),
        );
        assert_eq!(machine.ring.window, Window::Bounded { floor: meta(3) });
        assert!(machine.ring.entries.is_empty());
        assert_eq!(machine.tip(), Some(meta(3)));
        assert_eq!(machine.ring.oldest(), Some(meta(3)));
    }

    /// An empty ledger is a fresh start whatever it was built with: there is no oldest
    /// block for history to sit below.
    #[test]
    fn an_empty_ledger_never_has_history_below() {
        let ledger = Ledger::new(Vec::new(), true, UndoWindow::DEFAULT);
        assert!(!ledger.history_below());
        let ring = UndoRing::from(ledger);
        assert!(ring.holds_whole_history());
        assert_eq!((ring.tip(), ring.oldest()), (None, None));
    }

    /// A narrower window slides at its own size, and a ledger carries it to the ring.
    #[test]
    #[expect(
        clippy::expect_used,
        reason = "a failed one means the fixture is wrong"
    )]
    fn a_ring_slides_at_the_window_its_ledger_names() {
        let window = UndoWindow::new(3).expect("in range");
        let mut ring = UndoRing::from(Ledger::new(ledger(1..=3), false, window));
        assert!(ring.holds_whole_history());
        ring.push(meta(4));
        assert_eq!(ring.window, Window::Bounded { floor: meta(1) });
        assert_eq!(ring.capacity(), window);
        assert_eq!(ring.entries.len(), 3);
    }

    /// The window holds the whole history until it is full; the next block slides the
    /// oldest into the floor, still addressable for linkage, and the one after that
    /// moves the floor up past it.
    #[test]
    fn the_ring_slides_and_keeps_a_recoverable_floor() {
        let window = crate::window::MAX_UNFINALIZED_BLOCKS as u64;
        let mut ring = UndoRing::default();
        for height in 0..window {
            ring.push(meta(height));
        }
        assert!(ring.holds_whole_history());
        assert_eq!(ring.oldest(), Some(meta(0)));

        ring.push(meta(window));
        assert!(
            !ring.holds_whole_history(),
            "the first block slid into the floor"
        );
        assert_eq!(ring.entries.len(), crate::window::MAX_UNFINALIZED_BLOCKS);
        assert_eq!(ring.window, Window::Bounded { floor: meta(0) });
        assert_eq!(ring.entries.front(), Some(&meta(1)));
        assert_eq!(ring.tip(), Some(meta(window)));
        // The floor and the whole tail are still addressable.
        assert!(ring.retains(0));
        assert_eq!(ring.at(0), Some(meta(0)));
        assert_eq!(ring.at(1), Some(meta(1)));

        ring.push(meta(window + 1));
        assert_eq!(ring.window, Window::Bounded { floor: meta(1) });
        assert!(!ring.retains(0));
        assert_eq!(ring.at(0), None);
    }

    #[test]
    fn the_window_retains_only_from_its_oldest_identity() {
        // A seeded ring whose floor sits above the last backfilled height: the fork
        // walk must stop rather than read below what is remembered.
        let mut ring = UndoRing {
            window: Window::Bounded { floor: meta(2) },
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

    /// Rewinding to the floor leaves the floor as the tip, never a window that remembers
    /// an oldest block but no newest; rewinding a whole window to its oldest entry
    /// leaves nothing, as before its first block.
    #[test]
    fn a_rewind_keeps_the_tip_and_the_oldest_identity_together() {
        let mut bounded = UndoRing {
            window: Window::Bounded { floor: meta(2) },
            ..UndoRing::default()
        };
        for height in 3..=5 {
            bounded.push(meta(height));
        }
        assert_eq!(bounded.orphaned(3), [hash(5), hash(4), hash(3)]);
        bounded.rewind(3);
        assert_eq!(bounded.tip(), Some(meta(2)), "the floor is the tip");
        assert_eq!(bounded.oldest(), Some(meta(2)));
        bounded.push(meta(3));
        assert_eq!(bounded.tip(), Some(meta(3)));

        let mut whole = UndoRing::default();
        for height in 0..=2 {
            whole.push(meta(height));
        }
        assert_eq!(whole.orphaned(0), [hash(2), hash(1), hash(0)]);
        whole.rewind(0);
        assert_eq!((whole.tip(), whole.oldest()), (None, None));
        assert!(whole.holds_whole_history());
    }
}
