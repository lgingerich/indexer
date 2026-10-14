//! Bringing every store of a run to one block before ingest resumes.
//!
//! One ingest feeds every store, but each commits on its own schedule — the lake every
//! half minute, a database every block — and a process can stop between any two
//! commits. So a restart finds the stores at different heights, and after a reorg the
//! process did not live to finish, on different branches. Ingest resumes from one
//! ledger, and a store must never be handed a block it already holds: the lake appends
//! rather than upserts, and a store that is ahead on an orphaned branch would keep the
//! orphan, since ingest retracts only what its ledger names.
//!
//! [`resume`] settles that before the first envelope. It finds the newest block every
//! store's ledger holds with the same hash, [rewinds](Store::rewind) every store that
//! committed past it, and resumes ingest from it. Every store then holds the same chain,
//! and ingest re-fetches the blocks the rewind took — at most the slowest store's commit
//! interval of them.
//!
//! A store with an empty ledger — new to the run, or never committed — has nothing to
//! agree on. It joins where the others resume; the blocks before that are not
//! backfilled into it. Nor does a store whose whole history lies above a block: one
//! that joined a running deployment and committed ahead of a slower store before the
//! process stopped. Its ledger reaches back to its first block, so it holds nothing at or
//! below that block, and rewinding it there deletes only what it wrote since joining. A
//! ledger as long as the window a restore reads may have history below it, so it always
//! has a say: a store that far ahead is a startup error rather than a rewind that could
//! delete days of rows.
//!
//! A rewind is not a retraction, so it writes no `reorgs` row. A store rewound past
//! blocks a reorg orphaned, which a faster store already recorded, never records that
//! reorg; see `docs/multi-store-open-items.md`.

use std::collections::HashSet;
use std::time::Instant;

use alloy_primitives::B256;
use tracing::{info, warn};

use crate::ingest::pipeline::LEDGER_WINDOW;
use crate::sink::channel::Batching;
use crate::sink::route::Backend;
use crate::sink::table::TableId;
use crate::sink::{Restored, Store, StoreError};
use crate::wire::envelope::BlockMeta;

/// A store, opened, with what the run routes to it.
#[derive(Debug)]
pub(crate) struct Opened<S> {
    /// Which store it is.
    pub(crate) backend: Backend,
    /// The store.
    pub(crate) store: S,
    /// When its drain commits.
    pub(crate) batching: Batching,
    /// The tables routed to it.
    pub(crate) tables: HashSet<TableId>,
}

/// Why the stores could not be brought to one block.
#[derive(Debug, thiserror::Error)]
pub enum ResumeError {
    /// A store's ledger or contracts could not be read back.
    #[error("{store} could not be read back: {source}")]
    Restore {
        /// The store.
        store: Backend,
        /// What failed.
        source: StoreError,
    },
    /// A store could not be rewound to the block the others hold.
    #[error("{store} could not be rewound to height {height}: {source}")]
    Rewind {
        /// The store.
        store: Backend,
        /// The height it was being rewound to.
        height: u64,
        /// What failed.
        source: StoreError,
    },
    /// The stores' ledgers share no block, so no height is safe to resume every one
    /// from: one store is further behind than the others' ledgers reach back, or they
    /// hold different chains.
    #[error("the stores hold no block in common ({})", describe_tips(.tips))]
    NoCommonBlock {
        /// Each store with a ledger, and the height of its tip.
        tips: Vec<(Backend, u64)>,
    },
}

fn describe_tips(tips: &[(Backend, u64)]) -> String {
    tips.iter()
        .map(|(store, height)| format!("{store} at {height}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Brings every store to the newest block they all hold, and returns what ingest
/// resumes from: that block's ledger, and every contract any store discovered.
///
/// # Errors
///
/// Returns [`ResumeError::Restore`] or [`ResumeError::Rewind`] naming the store that
/// failed, and [`ResumeError::NoCommonBlock`] when no block is held by them all.
pub(crate) async fn resume<S: Store>(stores: &mut [Opened<S>]) -> Result<Restored, ResumeError> {
    let mut restored = Vec::with_capacity(stores.len());
    for opened in stores.iter_mut() {
        restored.push(restore(opened).await?);
    }
    let ledgers: Vec<&[BlockMeta]> = restored.iter().map(|r| r.ledger.as_slice()).collect();
    let common = common_block(&ledgers).map_err(|Disjoint| ResumeError::NoCommonBlock {
        tips: stores
            .iter()
            .zip(&ledgers)
            .filter_map(|(opened, ledger)| Some((opened.backend, ledger.last()?.height)))
            .collect(),
    })?;
    let Some((common, slowest)) = common else {
        // Every ledger is empty: a fresh run, with nothing to agree on.
        return Ok(Restored {
            contracts: restored.into_iter().flat_map(|r| r.contracts).collect(),
            ledger: Vec::new(),
        });
    };
    let ledger: Vec<BlockMeta> = restored[slowest]
        .ledger
        .iter()
        .copied()
        .filter(|block| block.height <= common.height)
        .collect();
    for (opened, restored) in stores.iter_mut().zip(&mut restored) {
        let Some(tip) = restored.ledger.last() else {
            info!(store = %opened.backend, from = common.height + 1,
                "store has no accepted blocks; it joins where the others resume");
            continue;
        };
        if tip.height == common.height {
            continue;
        }
        warn!(store = %opened.backend, from = tip.height, to = common.height,
            "rewinding a store that committed past the others");
        let started = Instant::now();
        opened
            .store
            .rewind(common.height)
            .await
            .map_err(|source| ResumeError::Rewind {
                store: opened.backend,
                height: common.height,
                source,
            })?;
        info!(store = %opened.backend, to = common.height, elapsed_ms = millis(started),
            "store rewound");
        // The contracts it discovered above the block went with their blocks.
        *restored = restore(opened).await?;
    }
    Ok(Restored {
        contracts: restored.into_iter().flat_map(|r| r.contracts).collect(),
        ledger,
    })
}

/// Reads back what `opened` holds, and logs how long it took: a lake's read is a scan of
/// object storage, and one of the slower parts of a start.
async fn restore<S: Store>(opened: &mut Opened<S>) -> Result<Restored, ResumeError> {
    let started = Instant::now();
    let restored = opened
        .store
        .restore()
        .await
        .map_err(|source| ResumeError::Restore {
            store: opened.backend,
            source,
        })?;
    info!(
        store = %opened.backend,
        blocks = restored.ledger.len(),
        tip = ?restored.ledger.last().map(|block| block.height),
        contracts = restored.contracts.len(),
        elapsed_ms = millis(started),
        "store read back"
    );
    Ok(restored)
}

/// Milliseconds since `started`, as the startup logs report them.
fn millis(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Non-empty ledgers that share no block.
#[derive(Debug)]
struct Disjoint;

/// The newest block every ledger with a say holds with the same hash, and the position
/// of the ledger with the lowest tip, which holds it; `None` when every ledger is empty.
///
/// A ledger has no say about a block below its oldest one when it is shorter than
/// [`LEDGER_WINDOW`], since a restore reads up to that many: it is the store's whole
/// history, so the store holds nothing at or below the block. See the module docs.
fn common_block(ledgers: &[&[BlockMeta]]) -> Result<Option<(BlockMeta, usize)>, Disjoint> {
    let held: Vec<HashSet<(u64, B256)>> = ledgers
        .iter()
        .map(|ledger| {
            ledger
                .iter()
                .map(|block| (block.height, block.hash))
                .collect()
        })
        .collect();
    let Some((slowest, ledger)) = ledgers
        .iter()
        .enumerate()
        .filter_map(|(position, ledger)| Some((position, ledger.last()?.height)))
        .min_by_key(|&(_, tip)| tip)
        .map(|(position, _)| (position, ledgers[position]))
    else {
        return Ok(None);
    };
    let has_say = |ledger: &[BlockMeta], block: &BlockMeta| {
        ledger.len() >= LEDGER_WINDOW
            || ledger
                .first()
                .is_some_and(|oldest| oldest.height <= block.height)
    };
    let common = ledger.iter().rev().find(|block| {
        held.iter().zip(ledgers).all(|(held, ledger)| {
            !has_say(ledger, block) || held.contains(&(block.height, block.hash))
        })
    });
    common.map(|block| Some((*block, slowest))).ok_or(Disjoint)
}

#[cfg(test)]
mod tests {
    use alloy_primitives::B256;

    use crate::wire::envelope::BlockMeta;

    use super::{Disjoint, LEDGER_WINDOW, common_block};

    /// Blocks `heights`, each hashed `tag:height` and linked to the one below.
    fn ledger(tag: u8, heights: std::ops::RangeInclusive<u64>) -> Vec<BlockMeta> {
        heights
            .map(|height| BlockMeta {
                height,
                hash: hash(tag, height),
                parent_hash: hash(tag, height.saturating_sub(1)),
                timestamp: height,
            })
            .collect()
    }

    fn hash(tag: u8, height: u64) -> B256 {
        let mut bytes = [0; 32];
        bytes[0] = tag;
        bytes[24..].copy_from_slice(&height.to_be_bytes());
        B256::from(bytes)
    }

    fn common(ledgers: &[&[BlockMeta]]) -> Option<(u64, usize)> {
        common_block(ledgers)
            .ok()
            .flatten()
            .map(|(block, slowest)| (block.height, slowest))
    }

    #[test]
    fn the_common_block_is_the_slowest_tip_when_the_chains_agree() {
        let slow = ledger(0xa0, 1..=5);
        let fast = ledger(0xa0, 1..=9);
        assert_eq!(common(&[&fast, &slow]), Some((5, 1)));
        assert_eq!(common(&[&slow, &slow]), Some((5, 0)));
    }

    /// The slow store still holds a block the fast one saw orphaned: they agree only
    /// below it.
    #[test]
    fn the_common_block_is_below_where_the_branches_part() {
        let slow = ledger(0xa0, 1..=6);
        let mut fast = ledger(0xa0, 1..=4);
        fast.extend(ledger(0xb0, 5..=8));
        assert_eq!(common(&[&slow, &fast]), Some((4, 0)));
    }

    #[test]
    fn an_empty_ledger_has_no_say() {
        let stored = ledger(0xa0, 1..=5);
        assert_eq!(common(&[&[], &stored]), Some((5, 1)));
        assert_eq!(common(&[&[], &[]]), None);
    }

    /// A store that joined at 6 and committed ahead of the lake, still at 5, before the
    /// process stopped: it holds nothing at or below 5, so 5 is the common block and it is
    /// rewound to nothing.
    #[test]
    fn a_store_whose_whole_history_is_above_the_slowest_tip_has_no_say() {
        let slow = ledger(0xa0, 1..=5);
        let joined = ledger(0xa0, 6..=9);
        assert_eq!(common(&[&slow, &joined]), Some((5, 0)));
    }

    /// A full window may have history below it, so a store that far ahead is refused
    /// rather than rewound past all of it.
    #[test]
    fn a_full_ledger_above_the_slowest_tip_is_refused() {
        let behind = ledger(0xa0, 1..=5);
        let ahead = ledger(0xa0, 100..=99 + LEDGER_WINDOW as u64);
        assert!(matches!(common_block(&[&behind, &ahead]), Err(Disjoint)));
    }
}
