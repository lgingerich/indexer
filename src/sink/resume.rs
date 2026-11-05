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
//! store's ledger holds with the same hash, and resumes ingest from it.
//!
//! # A store ahead keeps its blocks
//!
//! A store that committed past that block keeps what it holds above it: deleting it would
//! take rows away from a database an app is reading, only for ingest to write them back
//! moments later. Instead the store is told which blocks it holds there
//! ([`Store::reconcile`]), and ingest, resuming from the common block, hands it each of
//! them again. One with the same hash is not written a second time — the lake, which
//! appends, would otherwise hold it twice — and one with another hash was orphaned while
//! the store was ahead, so the store retracts it as a reorg at the tip would, `reorgs`
//! row included. Every store then holds the same chain, and the only rows deleted are
//! those of blocks that really left it.
//!
//! That needs the store's ledger to name every block it holds above the common block. A
//! restore reads the newest [`UndoWindow::ledger`](crate::window::UndoWindow::ledger)
//! blocks, so a full ledger whose oldest block is above the one after the common block
//! may leave blocks in between unnamed. Such a store is [rewound](Store::rewind) to the
//! common block instead, and ingest re-fetches its rows above. A rewind is not a retraction, so it writes no `reorgs` row; see
//! `docs/multi-store-open-items.md`.
//!
//! # Stores with no say
//!
//! A store with an empty ledger — new to the run, or never committed — has nothing to
//! agree on. It joins where the others resume; the blocks before that are not
//! backfilled into it. Nor does a store whose whole history lies above a block: one
//! that joined a running deployment and committed ahead of a slower store before the
//! process stopped. Its ledger reaches back to its first block, so it holds nothing at or
//! below that block, and its ledger names all it holds. A ledger as long as the window a
//! restore reads may have history below it. When even its oldest block is above the
//! slowest store's tip, the stores show no conflict, only a lag: a lake whose commit, of
//! thousands of small blocks, was dropped by a stop the database's commits outlived.
//! Within [`UndoWindow::resume_lag`](crate::window::UndoWindow::resume_lag) blocks of that tip it has no say, and is rewound to it, its
//! rows above re-fetched; further ahead is a startup error rather than a rewind that
//! could delete days of rows.

use std::collections::HashSet;
use std::time::Instant;

use alloy_primitives::B256;
use futures_util::FutureExt as _;
use futures_util::future::{BoxFuture, try_join_all};
use tracing::{info, warn};

use crate::sink::channel::Batching;
use crate::sink::route::StoreBackend;
use crate::sink::store_runtime::StoreRuntime;
use crate::sink::table::TableId;
use crate::sink::{Restored, Store, StoreError, StoredContract, millis};
use crate::window::{Ledger, UndoWindow};
use crate::wire::envelope::{BlockMeta, Handoff};

/// A store, opened, with what the run routes to it and the runtime its work runs on.
#[derive(Debug)]
pub(crate) struct Opened<S> {
    /// Which store it is.
    pub(crate) backend: StoreBackend,
    /// The store.
    pub(crate) store: S,
    /// When its drain commits.
    pub(crate) batching: Batching,
    /// The tables routed to it.
    pub(crate) tables: HashSet<TableId>,
    /// Where the store's async calls run: its startup reads and rewind here, and its
    /// drain once ingest starts.
    pub(crate) runtime: StoreRuntime,
}

impl<S: Store> Opened<S> {
    /// Runs `call` on the store's own runtime, so what it spawns lands there too, and
    /// takes the store back with what it returns.
    async fn on_runtime<T: Send + 'static>(
        self,
        call: impl for<'a> FnOnce(&'a mut S) -> BoxFuture<'a, T> + Send + 'static,
    ) -> (Self, T) {
        let mut store = self.store;
        let (store, out) = self
            .runtime
            .run(async move {
                let out = call(&mut store).await;
                (store, out)
            })
            .await;
        (Self { store, ..self }, out)
    }
}

/// Why the stores could not be brought to one block.
#[derive(Debug, thiserror::Error)]
pub enum ResumeError {
    /// A store's ledger or contracts could not be read back.
    #[error("{store} could not be read back: {source}")]
    Restore {
        /// The store.
        store: StoreBackend,
        /// What failed.
        source: StoreError,
    },
    /// A store could not be rewound to the block the others hold.
    #[error("{store} could not be rewound to height {height}: {source}")]
    Rewind {
        /// The store.
        store: StoreBackend,
        /// The height it was being rewound to.
        height: u64,
        /// What failed.
        source: StoreError,
    },
    /// A store could not be told which of its blocks ingest hands it again.
    #[error("{store} could not reconcile the blocks it holds: {source}")]
    Reconcile {
        /// The store.
        store: StoreBackend,
        /// What failed.
        source: StoreError,
    },
    /// The stores' ledgers share no block, so no height is safe to resume every one
    /// from: one store is further behind than the others' ledgers reach back, or they
    /// hold different chains.
    #[error("the stores hold no block in common ({})", describe_tips(.tips))]
    NoCommonBlock {
        /// Each store with a ledger, and the height of its tip.
        tips: Vec<(StoreBackend, u64)>,
    },
}

impl ResumeError {
    /// Whether starting again against the same stores fails the same way: they hold no
    /// block in common, which no restart changes, or a store's error is permanent (see
    /// [`StoreError::is_permanent`]).
    #[must_use]
    pub fn is_permanent(&self) -> bool {
        match self {
            Self::NoCommonBlock { .. } => true,
            Self::Restore { source, .. }
            | Self::Rewind { source, .. }
            | Self::Reconcile { source, .. } => source.is_permanent(),
        }
    }
}

fn describe_tips(tips: &[(StoreBackend, u64)]) -> String {
    tips.iter()
        .map(|(store, height)| format!("{store} at {height}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// What ingest resumes from once the stores hold one block.
#[derive(Debug)]
pub(crate) struct Resumed {
    /// Every contract any store discovered, each from a block ingest will not hand back.
    pub(crate) contracts: Vec<StoredContract>,
    /// Every contract a backfill handed over that any store recorded, each from a block
    /// ingest will not hand back.
    pub(crate) handoffs: Vec<Handoff>,
    /// The blocks stores hold above the common block, which ingest hands them again: a
    /// store that holds one drops its rows as it arrives, so a record published with it
    /// would not land there.
    pub(crate) held: HashSet<B256>,
    /// The common block's ledger, and whether a store holds blocks below it.
    pub(crate) ledger: Ledger,
}

/// Brings every store to the newest block they all hold, and returns what ingest
/// resumes from: that block's ledger, and every contract any store discovered.
///
/// The ledger is the slowest store's, cut to the common block. Cut from the top, it can
/// be shorter than a restore reads while that store, or another, holds blocks below its
/// oldest one, so it carries whether any does (see [`history_below`]).
///
/// # Errors
///
/// Returns [`ResumeError::Restore`] or [`ResumeError::Rewind`] naming the store that
/// failed, and [`ResumeError::NoCommonBlock`] when no block is held by them all.
pub(crate) async fn resume<S: Store>(
    stores: Vec<Opened<S>>,
    window: UndoWindow,
) -> Result<(Vec<Opened<S>>, Resumed), ResumeError> {
    // Each store reads back on its own runtime, so they read at once rather than each
    // waiting out the last: a lake's read is a scan of object storage.
    let read = try_join_all(stores.into_iter().map(restore)).await?;
    let (stores, mut restored): (Vec<_>, Vec<_>) = read.into_iter().unzip();
    let ledgers: Vec<&[BlockMeta]> = restored.iter().map(|r| r.ledger.as_slice()).collect();
    let common = common_block(&ledgers, window).map_err(|Disjoint| ResumeError::NoCommonBlock {
        tips: stores
            .iter()
            .zip(&ledgers)
            .filter_map(|(opened, ledger)| Some((opened.backend, ledger.last()?.height)))
            .collect(),
    })?;
    let Some((common, slowest)) = common else {
        // Every ledger is empty: a fresh run, with nothing to agree on.
        return Ok((
            stores,
            gathered(
                restored,
                HashSet::new(),
                Ledger::new(Vec::new(), false, window),
            ),
        ));
    };
    let blocks: Vec<BlockMeta> = restored[slowest]
        .ledger
        .iter()
        .copied()
        .filter(|block| block.height <= common.height)
        .collect();
    // The blocks stores kept above the common block, which ingest hands them again.
    let mut held = HashSet::new();
    let mut settled = Vec::with_capacity(stores.len());
    for (opened, restored) in stores.into_iter().zip(&mut restored) {
        settled.push(settle(opened, restored, common.height, window, &mut held).await?);
    }
    let history_below = history_below(&restored, &blocks, window);
    let resumed = gathered(restored, held, Ledger::new(blocks, history_below, window));
    Ok((settled, resumed))
}

/// What every store read back, but what it holds of the `held` blocks, which ingest
/// hands back: a contract or record from one is discovered or recorded again then, and
/// only if the block is still canonical. Resumed from `ledger`.
fn gathered(restored: Vec<Restored>, held: HashSet<B256>, ledger: Ledger) -> Resumed {
    let mut resumed = Resumed {
        contracts: Vec::new(),
        handoffs: Vec::new(),
        held: HashSet::new(),
        ledger,
    };
    let canonical = |hash: &B256| !held.contains(hash);
    for r in restored {
        let contracts = r.contracts.into_iter();
        let handoffs = r.handoffs.into_iter();
        resumed
            .contracts
            .extend(contracts.filter(|contract| canonical(&contract.block_hash)));
        resumed
            .handoffs
            .extend(handoffs.filter(|handoff| canonical(&handoff.block_hash)));
    }
    resumed.held = held;
    resumed
}

/// Whether any store, as settled, may hold a block below the oldest of `blocks`: its
/// ledger starts lower, or is as long as a restore reads, so it may have more below.
///
/// Without it, a ledger cut to the common block looked like the slowest store's whole
/// history, and a fork replacing its oldest block was adopted as if it replaced the first
/// block ever indexed, where it reaches past what the run can retract.
fn history_below(restored: &[Restored], blocks: &[BlockMeta], window: UndoWindow) -> bool {
    let Some(floor) = blocks.first() else {
        return false;
    };
    restored.iter().any(|restored| {
        restored.ledger.first().is_some_and(|oldest| {
            restored.ledger.len() >= window.ledger() || oldest.height < floor.height
        })
    })
}

/// Brings `opened`, which read back `restored`, to the block every store holds at
/// `common`, and returns it: a store at that block or with nothing stored is left as it
/// is, one ahead whose ledger names all it holds above reconciles those blocks (adding
/// them to `held`), and any other is rewound and read back again into `restored`.
async fn settle<S: Store>(
    mut opened: Opened<S>,
    restored: &mut Restored,
    common: u64,
    window: UndoWindow,
    held: &mut HashSet<B256>,
) -> Result<Opened<S>, ResumeError> {
    let Some(tip) = restored.ledger.last().map(|block| block.height) else {
        info!(store = %opened.backend, from = common + 1,
            "store has no accepted blocks; it joins where the others resume");
        return Ok(opened);
    };
    if tip == common {
        return Ok(opened);
    }
    if names_all_above(&restored.ledger, common, window) {
        let above: Vec<BlockMeta> = restored
            .ledger
            .iter()
            .copied()
            .filter(|block| block.height > common)
            .collect();
        info!(store = %opened.backend, from = common + 1, to = tip,
            "store committed past the others; it keeps its blocks and is handed each again");
        held.extend(above.iter().map(|block| block.hash));
        opened
            .store
            .reconcile(&above)
            .map_err(|source| ResumeError::Reconcile {
                store: opened.backend,
                source,
            })?;
        return Ok(opened);
    }
    warn!(store = %opened.backend, from = tip, to = common,
        "rewinding a store that committed past the others, whose ledger does not reach back to them");
    let started = Instant::now();
    let (opened, rewound) = opened
        .on_runtime(move |store| store.rewind(common).boxed())
        .await;
    rewound.map_err(|source| ResumeError::Rewind {
        store: opened.backend,
        height: common,
        source,
    })?;
    info!(store = %opened.backend, to = common, elapsed_ms = millis(started.elapsed()),
        "store rewound");
    // The contracts it discovered above the block went with their blocks.
    let (opened, back) = restore(opened).await?;
    *restored = back;
    Ok(opened)
}

/// Whether `ledger` names every block its store holds above `common`: it is the store's
/// whole history, being shorter than the window a restore reads, or it reaches down to
/// the block after `common`.
fn names_all_above(ledger: &[BlockMeta], common: u64, window: UndoWindow) -> bool {
    ledger.len() < window.ledger()
        || ledger
            .first()
            .is_some_and(|oldest| oldest.height <= common.saturating_add(1))
}

/// Reads back what `opened` holds, and logs how long it took: a lake's read is a scan of
/// object storage, and one of the slower parts of a start.
async fn restore<S: Store>(opened: Opened<S>) -> Result<(Opened<S>, Restored), ResumeError> {
    let started = Instant::now();
    let (opened, result) = opened.on_runtime(|store| store.restore().boxed()).await;
    let restored = result.map_err(|source| ResumeError::Restore {
        store: opened.backend,
        source,
    })?;
    info!(
        store = %opened.backend,
        blocks = restored.ledger.len(),
        tip = ?restored.ledger.last().map(|block| block.height),
        contracts = restored.contracts.len(),
        elapsed_ms = millis(started.elapsed()),
        "store read back"
    );
    Ok((opened, restored))
}

/// Non-empty ledgers that share no block.
#[derive(Debug)]
struct Disjoint;

/// The newest block every ledger with a say holds with the same hash, and the position
/// of the ledger with the lowest tip, which holds it; `None` when every ledger is empty.
///
/// A ledger has no say about a block below its oldest one when it is shorter than
/// [`UndoWindow::ledger`], since a restore reads up to that many: it is the store's whole
/// history, so the store holds nothing at or below the block. A full one has none
/// either while its tip is within [`UndoWindow::resume_lag`] of the block: no hash disagrees,
/// so the store is only ahead. See the module docs.
fn common_block(
    ledgers: &[&[BlockMeta]],
    window: UndoWindow,
) -> Result<Option<(BlockMeta, usize)>, Disjoint> {
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
        let (Some(oldest), Some(tip)) = (ledger.first(), ledger.last()) else {
            return false;
        };
        oldest.height <= block.height
            || (ledger.len() >= window.ledger()
                && tip.height.saturating_sub(block.height) > window.resume_lag())
    };
    let common = ledger.iter().rev().find(|block| {
        held.iter().zip(ledgers).all(|(held, ledger)| {
            !has_say(ledger, block) || held.contains(&(block.height, block.hash))
        })
    });
    common.map(|block| Some((*block, slowest))).ok_or(Disjoint)
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::sync::{Arc, Mutex};

    use alloy_primitives::B256;

    use crate::sink::channel::Batching;
    use crate::sink::route::StoreBackend;
    use crate::sink::store_runtime::StoreRuntime;
    use crate::sink::{EnvelopeSink, Restored, SinkError, Store, StoreError};
    use crate::wire::envelope::{BlockMeta, Envelope};

    use super::{Disjoint, Opened, UndoWindow, common_block, names_all_above, resume};
    use crate::window::LEDGER_WINDOW;

    /// A store that holds `ledger` and notes which thread each startup call ran on.
    struct Probe {
        ledger: Vec<BlockMeta>,
        threads: Arc<Mutex<Vec<String>>>,
    }

    impl Probe {
        fn note(&self, call: &str) {
            let thread = std::thread::current().name().unwrap_or("-").to_owned();
            self.threads
                .lock()
                .expect("not poisoned")
                .push(format!("{call} on {thread}"));
        }
    }

    impl EnvelopeSink for Probe {
        async fn publish(&mut self, _envelope: Envelope) -> Result<(), SinkError> {
            Ok(())
        }
    }

    impl Store for Probe {
        async fn restore(&mut self) -> Result<Restored, StoreError> {
            self.note("restore");
            Ok(Restored {
                contracts: Vec::new(),
                handoffs: Vec::new(),
                ledger: self.ledger.clone(),
            })
        }

        fn reconcile(&mut self, _held: &[BlockMeta]) -> Result<(), StoreError> {
            Ok(())
        }

        async fn rewind(&mut self, height: u64) -> Result<(), StoreError> {
            self.note("rewind");
            self.ledger.retain(|block| block.height <= height);
            Ok(())
        }
    }

    /// The startup calls `resume` makes run on each store's own runtime, so what they
    /// spawn lands there: the lake's reads read back, and its rewind, on the lake's
    /// threads, and the database's on its own.
    #[tokio::test]
    async fn every_startup_call_runs_on_the_stores_own_runtime() {
        let threads = Arc::new(Mutex::new(Vec::new()));
        let opened = |backend, ledger| Opened {
            backend,
            store: Probe {
                ledger,
                threads: Arc::clone(&threads),
            },
            batching: Batching::eager(1),
            tables: std::collections::HashSet::new(),
            runtime: StoreRuntime::dedicated(backend).expect("a runtime"),
        };
        // The lake is ahead with a full window that starts above the database's tip, so
        // it is rewound rather than reconciled, and read back again.
        let stores = vec![
            opened(StoreBackend::Postgres, ledger(0xa0, 1..=5)),
            opened(
                StoreBackend::Delta,
                ledger(0xa0, 7..=6 + LEDGER_WINDOW as u64),
            ),
        ];
        let (stores, restored) = resume(stores, UndoWindow::DEFAULT).await.expect("resumed");
        assert_eq!(
            restored.ledger.blocks(),
            ledger(0xa0, 1..=5),
            "the slowest store's ledger"
        );
        assert!(
            !restored.ledger.history_below(),
            "the rewound store holds nothing below it"
        );
        assert_eq!(
            stores
                .iter()
                .map(|opened| opened.backend)
                .collect::<Vec<_>>(),
            [StoreBackend::Postgres, StoreBackend::Delta],
            "every store comes back, in order"
        );
        // The two stores read back at once, so in either order; the rewind and the read
        // after it follow.
        let mut calls = threads.lock().expect("not poisoned").clone();
        calls[..2].sort_unstable();
        assert_eq!(
            calls,
            [
                "restore on store-delta",
                "restore on store-postgres",
                "rewind on store-delta",
                "restore on store-delta",
            ]
        );
    }

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

    /// What [`common_block`] decided, by height rather than whole block.
    #[derive(Debug, PartialEq, Eq)]
    enum Outcome {
        /// Resume from this height, held by the ledger at this position, the slowest.
        Resume(u64, usize),
        /// Every ledger is empty.
        Fresh,
        /// No block is safe to resume from.
        Disjoint,
    }

    fn outcome(ledgers: &[&[BlockMeta]]) -> Outcome {
        match common_block(ledgers, UndoWindow::DEFAULT) {
            Ok(Some((block, slowest))) => Outcome::Resume(block.height, slowest),
            Ok(None) => Outcome::Fresh,
            Err(Disjoint) => Outcome::Disjoint,
        }
    }

    /// Where the stores resume from, for each shape their ledgers can be in after a stop:
    ///
    /// - The chains agree: the slowest tip.
    /// - The slow store still holds a block the fast one saw orphaned: they agree only
    ///   below it.
    /// - Different chains throughout: nothing is safe.
    /// - An empty ledger has no say, and with every ledger empty the run is fresh.
    /// - A store that joined at 6 and committed ahead of the lake, still at 5, before the
    ///   process stopped: it holds nothing at or below 5, so 5 is the common block and it
    ///   is rewound to nothing.
    /// - A full window whose oldest block is above the slowest tip, its tip within
    ///   `MAX_RESUME_LAG` of it: a database that outran a lake whose commit a stop
    ///   dropped. No hash disagrees, so the slowest tip is the common block, and the store
    ///   ahead is rewound to it.
    /// - Such a window one block further ahead may have history below it, so the store is
    ///   refused rather than rewound past all of it.
    #[test]
    fn the_common_block_is_the_newest_every_ledger_with_a_say_holds() {
        let window = LEDGER_WINDOW as u64;
        let lag = UndoWindow::DEFAULT.resume_lag();
        let slow = ledger(0xa0, 1..=5);
        let fast = ledger(0xa0, 1..=9);
        let orphan_held = ledger(0xa0, 1..=6);
        let mut forked = ledger(0xa0, 1..=4);
        forked.extend(ledger(0xb0, 5..=8));
        let other_chain = ledger(0xb0, 1..=9);
        let joined = ledger(0xa0, 6..=9);
        let just_above = ledger(0xa0, 5_000..=4_999 + window);
        // Full windows above the slow store's tip of 5, ending `lag` and `lag + 1` past it.
        let within = ledger(0xa0, 6 + lag - window..=5 + lag);
        let beyond = ledger(0xa0, 7 + lag - window..=6 + lag);
        let empty: &[BlockMeta] = &[];
        let cases: [(&str, [&[BlockMeta]; 2], Outcome); 10] = [
            ("agreeing chains", [&fast, &slow], Outcome::Resume(5, 1)),
            ("one ledger twice", [&slow, &slow], Outcome::Resume(5, 0)),
            (
                "branches that part",
                [&orphan_held, &forked],
                Outcome::Resume(4, 0),
            ),
            ("different chains", [&slow, &other_chain], Outcome::Disjoint),
            ("an empty ledger", [empty, &slow], Outcome::Resume(5, 1)),
            ("every ledger empty", [empty, empty], Outcome::Fresh),
            (
                "a whole history above the slowest tip",
                [&slow, &joined],
                Outcome::Resume(5, 0),
            ),
            (
                "a full window just above the slowest tip",
                [&slow, &just_above],
                Outcome::Resume(5, 0),
            ),
            (
                "a full window ending MAX_RESUME_LAG past the slowest tip",
                [&slow, &within],
                Outcome::Resume(5, 0),
            ),
            (
                "a full window ending further past the slowest tip",
                [&slow, &beyond],
                Outcome::Disjoint,
            ),
        ];
        for (label, ledgers, expected) in cases {
            assert_eq!(outcome(&ledgers), expected, "{label}");
        }
    }

    /// The ledger ingest resumes from is the slowest store's, cut to the common block,
    /// and says whether a store holds blocks below it: a full window cut from the top is
    /// shorter than one but still has history below, as does another store's ledger
    /// starting lower. Only whole histories starting at or above it have none.
    #[tokio::test]
    async fn a_cut_ledger_says_whether_a_store_holds_blocks_below_it() {
        let window = LEDGER_WINDOW as u64;
        let forked_tip = |heights: std::ops::RangeInclusive<u64>| {
            let tip = *heights.end();
            let mut forked = ledger(0xa0, *heights.start()..=tip - 1);
            forked.extend(ledger(0xb0, tip..=tip));
            forked
        };
        for (label, slow, fast, common, below) in [
            (
                "a full window, its tip orphaned",
                forked_tip(1..=window),
                ledger(0xa0, window - 5..=window + 1),
                window - 1,
                true,
            ),
            (
                "whole histories, the slow one's tip orphaned",
                forked_tip(3..=5),
                ledger(0xa0, 3..=9),
                4,
                false,
            ),
            (
                "another store's history starting lower",
                forked_tip(3..=5),
                ledger(0xa0, 1..=9),
                4,
                true,
            ),
        ] {
            let opened = |backend, ledger| Opened {
                backend,
                store: Probe {
                    ledger,
                    threads: Arc::default(),
                },
                batching: Batching::eager(1),
                tables: std::collections::HashSet::new(),
                runtime: StoreRuntime::dedicated(backend).expect("a runtime"),
            };
            let stores = vec![
                opened(StoreBackend::Postgres, slow.clone()),
                opened(StoreBackend::Delta, fast),
            ];
            let (_, resumed) = resume(stores, UndoWindow::DEFAULT).await.expect(label);
            let mut expected = slow;
            expected.retain(|block| block.height <= common);
            assert_eq!(resumed.ledger.blocks(), expected, "{label}");
            assert_eq!(resumed.ledger.history_below(), below, "{label}");
        }
    }

    /// A narrower window decides as the default does, at its own lengths: a ledger of its
    /// size may have history below, a store ahead is rewound only within its lag, and
    /// one a default window would still rewind is refused.
    #[test]
    fn resuming_follows_the_chains_undo_window() {
        let window = UndoWindow::new(8).expect("in range");
        let (len, lag) = (window.ledger() as u64, window.resume_lag());
        assert_eq!((len, lag), (9, 36));
        let slow = ledger(0xa0, 1..=5);
        let within = ledger(0xa0, 6 + lag - len..=5 + lag);
        let beyond = ledger(0xa0, 7 + lag - len..=6 + lag);
        assert_eq!(
            common_block(&[&slow, &within], window)
                .expect("a store ahead within the lag")
                .map(|(block, _)| block.height),
            Some(5)
        );
        assert!(common_block(&[&slow, &beyond], window).is_err());
        assert!(common_block(&[&slow, &beyond], UndoWindow::DEFAULT).is_ok());

        assert!(
            names_all_above(&ledger(0xa0, 7..=14), 5, window),
            "short of a ledger"
        );
        assert!(
            !names_all_above(&ledger(0xa0, 7..=15), 5, window),
            "a full ledger"
        );
    }

    /// A store ahead keeps its blocks only when its ledger names all of them: its whole
    /// history, or a window reaching down to the block after the common one. A full
    /// window starting higher may hide blocks in between, so that store is rewound.
    #[test]
    fn a_store_ahead_reconciles_only_what_its_ledger_names() {
        let full = |from: u64| ledger(0xa0, from..=from + LEDGER_WINDOW as u64 - 1);
        for (label, ledger, names_all) in [
            ("reaches below", ledger(0xa0, 1..=9), true),
            ("its whole history", ledger(0xa0, 6..=9), true),
            ("a full window from the next block", full(6), true),
            ("a full window with a gap below", full(7), false),
        ] {
            assert_eq!(
                names_all_above(&ledger, 5, UndoWindow::DEFAULT),
                names_all,
                "{label}"
            );
        }
    }
}
