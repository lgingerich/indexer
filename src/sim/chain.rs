//! The simulated chain: a tree of blocks, one path of which is canonical.
//!
//! Blocks are real alloy headers with real hashes, so parent linkage is what a node
//! would serve. Each carries a few logs, and its `logsBloom` covers them, so the
//! source's bloom check holds. The world outlives every simulated process: a crash
//! drops the indexer, never the chain.
//!
//! Every step of the chain is kept as a *view*: the canonical chain as it stood then.
//! A node backend that lags serves an older view — an older head, or the far side of a
//! reorg it has not seen yet — which is how a load-balanced provider disagrees with
//! itself.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use alloy_primitives::{Address, B256, Bloom, Bytes};
use tokio::sync::{Notify, broadcast};

use super::Rng;

/// The world every simulated process shares.
pub(super) type Shared = Arc<Mutex<World>>;

/// Locks the world. Nothing panics while holding it, so it is never poisoned.
pub(super) fn lock(world: &Shared) -> MutexGuard<'_, World> {
    world
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// How often each fault fires, per mille, drawn once per seed. Most seeds leave some
/// faults off entirely, so a run is dominated by a few kinds rather than a uniform blur
/// ("swarm testing").
#[derive(Debug, Clone, Copy)]
pub(super) struct Profile {
    /// A reorg instead of a new block.
    pub(super) reorg: u64,
    /// An HTTP request refused with `503`.
    pub(super) refuse: u64,
    /// An HTTP request that never answers, failing at the client's timeout.
    pub(super) timeout: u64,
    /// An HTTP call served by a backend behind the newest view.
    pub(super) lag: u64,
    /// A batch whose calls are served by different backends, not one.
    pub(super) split: u64,
    /// The head subscription dropped.
    pub(super) drop_heads: u64,
    /// A new head not announced.
    pub(super) skip_head: u64,
    /// An old head announced again: the previous one, or one from a stale view.
    pub(super) stale_head: u64,
    /// A store statement failing before it runs.
    pub(super) store_fail: u64,
    /// A commit that lands but reports failure.
    pub(super) commit_lost: u64,
    /// The process dying at a store statement, before or after it runs.
    pub(super) store_crash: u64,
}

impl Profile {
    fn draw(rng: &mut Rng) -> Self {
        let mut rate = |max: u64| {
            if rng.chance(60) {
                rng.below(max + 1)
            } else {
                0
            }
        };
        Self {
            reorg: rate(250),
            refuse: rate(80),
            timeout: rate(30),
            lag: rate(300),
            split: rate(300),
            drop_heads: rate(40),
            skip_head: rate(200),
            stale_head: rate(150),
            store_fail: rate(10),
            commit_lost: rate(60),
            store_crash: rate(10),
        }
    }
}

/// One log, as the node serves it.
#[derive(Debug, Clone)]
pub(super) struct SimLog {
    pub(super) address: Address,
    pub(super) topics: Vec<B256>,
    pub(super) data: Bytes,
    pub(super) transaction_hash: B256,
}

/// One block: its header, its logs in block order, and the view it first appeared in.
#[derive(Debug, Clone)]
pub(super) struct SimBlock {
    pub(super) header: alloy_rpc_types_eth::Header,
    pub(super) logs: Vec<SimLog>,
    view: usize,
}

impl SimBlock {
    pub(super) const fn hash(&self) -> B256 {
        self.header.hash
    }

    pub(super) const fn number(&self) -> u64 {
        self.header.inner.number
    }

    /// The block's transaction hashes, each once, in order.
    pub(super) fn transactions(&self) -> Vec<B256> {
        let mut hashes: Vec<B256> = Vec::new();
        for log in &self.logs {
            if hashes.last() != Some(&log.transaction_hash) {
                hashes.push(log.transaction_hash);
            }
        }
        hashes
    }
}

/// The chain, the randomness everything draws from, and what happened.
#[derive(Debug)]
pub(super) struct World {
    pub(super) rng: Rng,
    pub(super) profile: Profile,
    /// Whether anything misbehaves. Off for the final, fault-free phase.
    pub(super) faults: bool,
    /// Every block ever made, on any branch.
    blocks: BTreeMap<B256, SimBlock>,
    /// The canonical chain by height after each step; the last is the newest.
    views: Vec<Vec<B256>>,
    /// New canonical blocks, for `newHeads`.
    heads: broadcast::Sender<B256>,
    /// Woken when a store statement kills the process; the driver drops the runtime.
    pub(super) crash: Arc<Notify>,
    /// Whether a store fault fired in this process lifetime.
    pub(super) store_faulted: bool,
    /// Distinguishes blocks that would otherwise share a hash across branches.
    salt: u64,
    /// Everything that happened, in order: the determinism check compares it.
    pub(super) trace: Vec<String>,
    /// How often each scenario was reached.
    pub(super) coverage: BTreeMap<&'static str, u64>,
}

impl World {
    /// A chain of `length` blocks after genesis.
    pub(super) fn new(mut rng: Rng, length: u64) -> Self {
        let profile = Profile::draw(&mut rng);
        let mut world = Self {
            rng,
            profile,
            faults: true,
            blocks: BTreeMap::new(),
            views: Vec::new(),
            heads: broadcast::channel(64).0,
            crash: Arc::new(Notify::new()),
            store_faulted: false,
            salt: 0,
            trace: vec![format!("{profile:?}")],
            coverage: BTreeMap::new(),
        };
        let genesis = world.make(None);
        world.views.push(vec![genesis]);
        for _ in 0..length {
            world.extend();
        }
        world
    }

    /// Notes that `scenario` happened.
    pub(super) fn reached(&mut self, scenario: &'static str) {
        *self.coverage.entry(scenario).or_default() += 1;
        self.trace.push(scenario.to_owned());
    }

    /// Whether the fault firing at the profile's `rate`, per mille, fires now.
    pub(super) fn fault(&mut self, rate: fn(&Profile) -> u64) -> bool {
        self.faults && self.rng.below(1000) < rate(&self.profile)
    }

    fn canonical(&self) -> &[B256] {
        self.views.last().map_or(&[], Vec::as_slice)
    }

    pub(super) fn tip(&self) -> u64 {
        (self.canonical().len() as u64).saturating_sub(1)
    }

    /// The canonical block at `height`, if the chain is that long.
    pub(super) fn at(&self, height: u64) -> Option<&SimBlock> {
        self.view(self.newest()).at(height)
    }

    pub(super) fn block(&self, hash: &B256) -> Option<&SimBlock> {
        self.blocks.get(hash)
    }

    /// The newest view's index.
    pub(super) const fn newest(&self) -> usize {
        self.views.len().saturating_sub(1)
    }

    /// The view a backend serves: the newest, or with a lag fault one a few steps
    /// behind it.
    pub(super) fn pick_view(&mut self) -> usize {
        let newest = self.newest();
        if self.fault(|p| p.lag) {
            self.reached("backend lags");
            newest.saturating_sub(1 + self.index_below(4))
        } else {
            newest
        }
    }

    pub(super) fn view(&self, index: usize) -> View<'_> {
        View {
            world: self,
            index,
            chain: self.views.get(index).map_or(&[], Vec::as_slice),
        }
    }

    pub(super) fn subscribe(&self) -> broadcast::Receiver<B256> {
        self.heads.subscribe()
    }

    /// A number in `0..n`, as an index.
    pub(super) fn index_below(&mut self, n: usize) -> usize {
        usize::try_from(self.rng.below(n as u64)).unwrap_or(0)
    }

    /// Advances the chain one step while the indexer runs: usually a new block,
    /// sometimes a short reorg.
    pub(super) fn step(&mut self) {
        if self.tip() > 4 && self.fault(|p| p.reorg) {
            let depth = 1 + self.index_below(3);
            self.reached("reorg");
            self.reorg(depth);
        } else {
            self.extend();
        }
    }

    /// Advances the chain while the indexer is down: a few blocks, and sometimes a reorg
    /// deep enough to fork below what the indexer last stored.
    pub(super) fn downtime(&mut self) {
        for _ in 0..self.rng.below(6) {
            self.extend();
        }
        if self.tip() > 12 && self.fault(|p| p.reorg * 2) {
            let depth = 1 + self.index_below(10);
            self.reached("reorg while down");
            self.reorg(depth);
        }
    }

    fn extend(&mut self) {
        let mut chain = self.canonical().to_vec();
        chain.push(self.make(chain.last().copied()));
        let hash = chain[chain.len() - 1];
        self.views.push(chain);
        // No receiver is fine: nothing is subscribed between processes.
        let _ = self.heads.send(hash);
    }

    /// Replaces the newest `depth` blocks with a branch one block longer. The branch
    /// point is a view of its own, as a node mid-reorg would serve it.
    fn reorg(&mut self, depth: usize) {
        let mut chain = self.canonical().to_vec();
        chain.truncate(chain.len().saturating_sub(depth).max(1));
        self.trace
            .push(format!("reorg depth {depth} at {}", chain.len() - 1));
        self.views.push(chain);
        for _ in 0..=depth {
            self.extend();
        }
    }

    /// Makes a block on `parent`, first seen in the view about to be pushed, and
    /// returns its hash.
    fn make(&mut self, parent: Option<B256>) -> B256 {
        let gap = 1 + self.rng.below(4);
        let (number, parent_hash, timestamp) = parent
            .and_then(|hash| self.blocks.get(&hash))
            .map_or((0, B256::ZERO, 1_700_000_000), |block| {
                (
                    block.number() + 1,
                    block.hash(),
                    block.header.inner.timestamp + gap,
                )
            });
        let mut logs: Vec<SimLog> = Vec::new();
        let mut bloom = Bloom::ZERO;
        if number > 0 {
            for _ in 0..self.rng.below(5) {
                let transaction_hash = match logs.last() {
                    Some(last) if self.rng.chance(50) => last.transaction_hash,
                    _ => self.rng.b256(),
                };
                let address = Address::repeat_byte(match self.rng.below(3) {
                    0 => 1,
                    1 => 2,
                    _ => 3,
                });
                let topics = (0..self.rng.below(5)).map(|_| self.rng.b256()).collect();
                let length = self.index_below(33);
                let data = Bytes::copy_from_slice(&self.rng.b256()[..length]);
                let log = SimLog {
                    address,
                    topics,
                    data,
                    transaction_hash,
                };
                bloom.accrue_raw_log(log.address, &log.topics);
                logs.push(log);
            }
        }
        self.salt += 1;
        let header = alloy_rpc_types_eth::Header::new(alloy_consensus::Header {
            parent_hash,
            number,
            timestamp,
            logs_bloom: bloom,
            extra_data: Bytes::from(self.salt.to_be_bytes().to_vec()),
            ..alloy_consensus::Header::default()
        });
        let hash = header.hash;
        self.trace.push(format!("block {number} {hash}"));
        let view = self.views.len();
        self.blocks.insert(hash, SimBlock { header, logs, view });
        hash
    }
}

/// The chain as one backend sees it.
pub(super) struct View<'a> {
    world: &'a World,
    index: usize,
    chain: &'a [B256],
}

impl<'a> View<'a> {
    pub(super) fn tip(&self) -> u64 {
        (self.chain.len() as u64).saturating_sub(1)
    }

    pub(super) fn at(&self, height: u64) -> Option<&'a SimBlock> {
        let hash = self.chain.get(usize::try_from(height).ok()?)?;
        self.world.blocks.get(hash)
    }

    /// A block this backend has seen, on any branch.
    pub(super) fn known(&self, hash: &B256) -> Option<&'a SimBlock> {
        self.world
            .blocks
            .get(hash)
            .filter(|block| block.view <= self.index)
    }
}
