//! Matching other protocols' pools to the token pairs Metric's pools trade, so a store
//! can keep only the pools compared with Metric's while the lake keeps every pool.
//!
//! A Metric `PoolCreated` names its pair. Every Uniswap pool on that pair is then
//! *matched*: Uniswap V3's by its address, from its factory's `PoolCreated`, and Uniswap
//! V4's by its pool id, from its `PoolManager`'s `Initialize`. A store routing a table as
//! `{ matched = "<glob>" }` takes only the matched pools' rows of it (see [`Kept`]).
//!
//! # Finding the pools
//!
//! A pool created once its pair is known is matched from its creation in the stream. One
//! created earlier, before the pair, or before the lake's first block, is found by
//! searching the chain's history for its creation log over `HyperSync`: both events name
//! their two tokens as topics, so one search per protocol finds every pool on the pairs
//! seen since the last. At startup the Metric pools themselves are searched for the same
//! way, so no pair waits for a new Metric pool to be known. Each search runs off the
//! ingest path: the decode stage sends [`Query`]s and takes the logs they find at a block
//! boundary. A node's `eth_getLogs` cannot do this: most plans take a few hundred heights
//! a request.
//!
//! A pair is a pair of tokens, lower address first, with the native coin counted as the
//! chain's wrapped one: Metric's pools hold WETH, where Uniswap V4's often hold ETH.
//!
//! # Recording them
//!
//! A pool found by a fetch joins after a block's marker, as a contract a backfill hands
//! over does, and the next block records it: a `matched_pools` row, with the `contracts`
//! row of a Uniswap V3 pool the run did not decode yet. Its rows are kept from that block
//! on, and its earlier ones, back to the lookback before the first Metric pool on its
//! pair, are filled from the lake (a [`Fill`]); once they are, a second record says so.
//! Every record belongs to the block it is published with, so a reorg that orphans the
//! block deletes it and the next block records it again, and a restart restores every
//! pool from them.

// A build without a store matches nothing: it has nowhere to record a match, and the
// router keeps no table to the matched pools without one.
#![cfg_attr(
    not(feature = "store"),
    expect(dead_code, reason = "only a run that stores matches pools")
)]

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, RwLock};

use alloy_primitives::{Address, B256, address};

use super::abi::EventKey;
use super::catalog::{Catalog, EntryId};
use super::decoder::DecodedAs;
use crate::wire::datasets::evm::{Contract, Decoded};
use crate::wire::envelope::{BlockMeta, ChainId, Event, Matched};
use crate::wire::typed::TypedValue;

/// Two tokens, lower address first, the native coin as the chain's wrapped one.
pub(crate) type Pair = (Address, Address);

/// The wrapped native coin on each chain the indexer runs, which a pool holding the native
/// coin itself, as the zero address, is matched as.
const WRAPPED_NATIVE: [(&str, Address); 4] = [
    (
        "base",
        address!("0x4200000000000000000000000000000000000006"),
    ),
    (
        "bsc",
        address!("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"),
    ),
    (
        "ethereum",
        address!("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
    ),
    (
        "robinhood",
        address!("0x0bd7d308f8e1639fab988df18a8011f41eacad73"),
    ),
];

/// The event that names a pair to match: Metric's factory's `PoolCreated`.
const REFERENCE: Rule = Rule {
    protocol: "metric_v1",
    contract: "MetricOmmPoolFactory",
    event: "PoolCreated",
    tokens: ["token0", "token1"],
    token_topics: [2, 3],
    pool: Pool::Contract("poolAddress"),
    fee: None,
    child: None,
};

/// The events that announce a pool to match, each with where its arguments are.
const CANDIDATES: [Rule; 2] = [
    Rule {
        protocol: "uniswap_v3",
        contract: "UniswapV3Factory",
        event: "PoolCreated",
        tokens: ["token0", "token1"],
        token_topics: [1, 2],
        pool: Pool::Contract("pool"),
        fee: Some("fee"),
        child: Some("UniswapV3Pool"),
    },
    Rule {
        protocol: "uniswap_v4",
        contract: "PoolManager",
        event: "Initialize",
        tokens: ["currency0", "currency1"],
        token_topics: [2, 3],
        pool: Pool::Id {
            id: "id",
            id_topic: 1,
            hooks: "hooks",
        },
        fee: Some("fee"),
        child: None,
    },
];

/// One event that names a pool, and its pair.
#[derive(Debug, Clone, Copy)]
struct Rule {
    protocol: &'static str,
    contract: &'static str,
    event: &'static str,
    /// The arguments naming the pair's two tokens.
    tokens: [&'static str; 2],
    /// The topics those arguments are, which a fetch filters on.
    token_topics: [usize; 2],
    pool: Pool,
    /// The argument holding the pool's fee, if it has one.
    fee: Option<&'static str>,
    /// The contract a pool that is a contract of its own decodes as.
    child: Option<&'static str>,
}

/// How an event names its pool.
#[derive(Debug, Clone, Copy)]
enum Pool {
    /// By an `address` argument: the pool is a contract of its own.
    Contract(&'static str),
    /// By a `bytes32` argument, within the contract emitting the event, with the
    /// argument naming its hooks. Every event of the contract that concerns one pool
    /// names it by that argument, the topic `id_topic` of its log.
    Id {
        id: &'static str,
        id_topic: usize,
        hooks: &'static str,
    },
}

/// How the pools of a contract holding many are told apart in its logs and records: the
/// `bytes32` argument naming the pool, and the topic it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PoolIds {
    /// The argument.
    pub(crate) argument: &'static str,
    /// The topic, so a reader can keep a pool's logs before decoding them.
    pub(crate) topic: usize,
}

impl PoolIds {
    /// The pools of `protocol`'s contract holding many, if its pools are kept by id.
    pub(crate) fn of(protocol: &str) -> Option<Self> {
        CANDIDATES
            .iter()
            .find(|rule| rule.protocol == protocol)
            .and_then(|rule| match rule.pool {
                Pool::Id { id, id_topic, .. } => Some(Self {
                    argument: id,
                    topic: id_topic,
                }),
                Pool::Contract(_) => None,
            })
    }

    /// The pool `decoded` names.
    pub(crate) fn read(self, decoded: &Decoded) -> Option<B256> {
        pool_id(decoded, self.argument)
    }

    /// The pool `log` names, read from its topic.
    pub(crate) fn of_log(self, log: &crate::wire::datasets::evm::Log) -> Option<B256> {
        match self.topic {
            1 => log.topic1,
            2 => log.topic2,
            3 => log.topic3,
            _ => None,
        }
    }
}

/// A [`Rule`] bound to the catalog: the entry emitting its event, the event's keys, and
/// the entry's listed addresses on the chain, which a fetch asks for.
#[derive(Debug, Clone)]
struct Bound {
    rule: Rule,
    entry: EntryId,
    keys: Vec<EventKey>,
    addresses: Vec<Address>,
}

impl Bound {
    fn new(catalog: &Catalog, rule: Rule) -> Option<Self> {
        let entry = catalog.entry(rule.protocol, rule.contract)?;
        let keys = catalog.entries[entry].abi.events_named(rule.event);
        let addresses: Vec<Address> = catalog
            .seeds
            .iter()
            .filter(|&&(_, seeded)| seeded == entry)
            .map(|&(address, _)| address)
            .collect();
        (!keys.is_empty() && !addresses.is_empty()).then_some(Self {
            rule,
            entry,
            keys,
            addresses,
        })
    }

    fn binds(&self, (entry, key): DecodedAs) -> bool {
        self.entry == entry && self.keys.contains(&key)
    }

    /// The fetch of every log of this event naming one of `tokens[0]` and one of
    /// `tokens[1]`, in the order the event names them, or of every one when `tokens` is
    /// `None`, up to block `through`.
    fn query(&self, tokens: Option<[&BTreeSet<Address>; 2]>, through: u64) -> Query {
        let mut topics: [Vec<B256>; 4] = Default::default();
        topics[0] = self.keys.iter().map(|key| key.0).collect();
        if let Some(tokens) = tokens {
            for (topic, tokens) in self.rule.token_topics.into_iter().zip(tokens) {
                topics[topic] = tokens.iter().map(Address::into_word).collect();
            }
        }
        Query {
            addresses: self.addresses.clone(),
            topics,
            through,
        }
    }
}

/// A search of a chain's history for logs: those the listed `addresses` emitted with each
/// topic one of its list, an empty list matching any, from the first block through
/// `through`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Query {
    /// The contracts.
    pub(crate) addresses: Vec<Address>,
    /// `topic0` through `topic3`.
    pub(crate) topics: [Vec<B256>; 4],
    /// The last block.
    pub(crate) through: u64,
}

/// The pools a store keeping tables to the matched pools takes rows of: Uniswap V3
/// pools by address, and the pools of a contract holding many by id. Shared between
/// the decode stage, which changes it at block boundaries, and the router, which reads
/// it for every envelope.
#[derive(Debug, Default)]
pub(crate) struct Kept {
    /// Pools that are contracts of their own.
    contracts: HashSet<Address>,
    /// Of each contract holding many pools, how it names a pool and the ids of those kept.
    pools: HashMap<Address, (PoolIds, HashSet<B256>)>,
    /// The factories creating pools that are contracts of their own, each with the
    /// argument of its creation event naming the pool: a kept pool's creation record is
    /// kept too.
    creators: HashMap<Address, &'static str>,
}

/// [`Kept`], as the decode stage and the router share it.
pub(crate) type SharedKept = Arc<RwLock<Kept>>;

impl Kept {
    /// Whether `event` is a kept pool's: a pool contract's log, decoded record, or derived
    /// row; its factory's record creating it; or a decoded record of a contract holding
    /// many naming a kept pool. An event no contract emits is everyone's; a contract
    /// holding many keeps none of its raw logs, which name no pool a store can read.
    pub(crate) fn keeps(&self, event: &Event) -> bool {
        match event {
            Event::Log(log) => self.contracts.contains(&log.address),
            Event::Derived(derived) => self.contracts.contains(&derived.address),
            Event::Decoded(decoded) => {
                if let Some((ids, kept)) = self.pools.get(&decoded.address) {
                    return ids.read(decoded).is_some_and(|id| kept.contains(&id));
                }
                if let Some(argument) = self.creators.get(&decoded.address) {
                    return argument_address(decoded, argument)
                        .is_some_and(|pool| self.contracts.contains(&pool));
                }
                self.contracts.contains(&decoded.address)
            }
            Event::Block(_)
            | Event::Transaction(_)
            | Event::Contract(_)
            | Event::Handoff(_)
            | Event::Matched(_)
            | Event::Reorg(_)
            | Event::AcceptedBlock(_) => true,
        }
    }

    /// Adds `pool`.
    fn keep(&mut self, pool: &Matched) {
        match (pool.pool_id, PoolIds::of(&pool.protocol)) {
            (Some(id), Some(ids)) => {
                self.pools
                    .entry(pool.contract)
                    .or_insert_with(|| (ids, HashSet::new()))
                    .1
                    .insert(id);
            }
            (Some(_), None) => {}
            (None, _) => {
                self.contracts.insert(pool.contract);
            }
        }
    }

    /// Takes `pool` out, its creation orphaned.
    fn forget(&mut self, pool: &Matched) {
        match pool.pool_id {
            Some(id) => {
                if let Some((_, kept)) = self.pools.get_mut(&pool.contract) {
                    kept.remove(&id);
                }
            }
            None => {
                self.contracts.remove(&pool.contract);
            }
        }
    }

    /// The kept pools of `contracts` holding many and the kept pool contracts, as a test
    /// sets them.
    #[cfg(test)]
    #[expect(clippy::expect_used)]
    pub(crate) fn of(contracts: &[Address], pools: &[(Address, B256)]) -> Self {
        let ids = PoolIds::of("uniswap_v4").expect("Uniswap V4 keeps pools by id");
        let mut kept = Self {
            contracts: contracts.iter().copied().collect(),
            pools: HashMap::new(),
            creators: HashMap::new(),
        };
        for &(contract, id) in pools {
            kept.pools
                .entry(contract)
                .or_insert_with(|| (ids, HashSet::new()))
                .1
                .insert(id);
        }
        kept
    }
}

/// The key a pool is matched under: the contract its logs are, and its id within it.
type PoolKey = (Address, Option<B256>);

/// Pools' history to fill from the lake: their rows through block `through`, the one
/// before the run kept them from, into the stores keeping matched pools from each pool's
/// `from_timestamp`, and a pool contract's into the lake's decoded tables from the lake's
/// first block, since the run decoded none of them before the protocol was deployed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Fill {
    /// The pools, all recorded in one block.
    pub(crate) pools: Vec<Matched>,
    /// The last block to fill.
    pub(crate) through: u64,
}

/// The contract a pool of `protocol` that is a contract of its own decodes as, to add it
/// to a decoder that does not know it: `UniswapV3Pool` for `uniswap_v3`.
pub(crate) fn pool_contract(protocol: &str) -> Option<&'static str> {
    CANDIDATES
        .iter()
        .find(|rule| rule.protocol == protocol)
        .and_then(|rule| rule.child)
}

/// What a decoded record tells the matching.
#[derive(Debug)]
enum Seen {
    /// A Metric pool on `pair`, created at `timestamp`.
    Pair { pair: Pair, timestamp: u64 },
    /// A pool another protocol created.
    Pool(Box<Matched>, Pair),
}

/// The matching's state in the decode stage: the pairs, the pools matched, and those
/// joining at the next block.
#[derive(Debug)]
pub(crate) struct Matching {
    reference: Bound,
    candidates: Vec<Bound>,
    wrapped: Address,
    /// How far before the first Metric pool on its pair a pool's rows are kept from.
    lookback: u64,
    /// Every pair, with the creation time of its first Metric pool.
    pairs: HashMap<Pair, u64>,
    /// Every pool matched, by key, with its latest record.
    matched: HashMap<PoolKey, Matched>,
    /// Pools found since the last marker, which the next block records: each with its
    /// `contracts` row when it is a contract the run did not decode yet. A record not yet
    /// `filled` has earlier rows to fill.
    joining: Vec<(Matched, Option<Contract>)>,
    /// The pairs seen since the last [`Self::queries`], to search for the pools on them.
    asking: BTreeSet<Pair>,
    /// The pools whose earlier rows were sent to fill, so a record written again after a
    /// reorg sends none again.
    filling: HashSet<PoolKey>,
    /// Whether the startup fetch of Metric's pools has been asked for.
    started: bool,
    kept: SharedKept,
}

impl Matching {
    /// The matching `catalog` supports on `chain`, keeping `lookback` seconds before the
    /// first Metric pool on a pair, or `None` when the catalog has no Metric factory or no
    /// protocol to match against it, or the chain has no wrapped native coin listed.
    pub(crate) fn new(catalog: &Catalog, chain: &ChainId, lookback: u64) -> Option<Self> {
        let reference = Bound::new(catalog, REFERENCE)?;
        let candidates: Vec<Bound> = CANDIDATES
            .into_iter()
            .filter_map(|rule| Bound::new(catalog, rule))
            .collect();
        let wrapped = WRAPPED_NATIVE
            .iter()
            .find(|(name, _)| *name == chain.as_str())?
            .1;
        let creators = candidates
            .iter()
            .filter_map(|bound| match bound.rule.pool {
                Pool::Contract(argument) => Some((bound, argument)),
                Pool::Id { .. } => None,
            })
            .flat_map(|(bound, argument)| bound.addresses.iter().map(move |&a| (a, argument)))
            .collect();
        let kept = Kept {
            creators,
            ..Kept::default()
        };
        (!candidates.is_empty()).then(|| Self {
            reference,
            candidates,
            wrapped,
            lookback,
            pairs: HashMap::new(),
            matched: HashMap::new(),
            joining: Vec::new(),
            filling: HashSet::new(),
            asking: BTreeSet::new(),
            started: false,
            kept: Arc::new(RwLock::new(kept)),
        })
    }

    /// The pools kept, as the router reads them.
    pub(crate) fn kept(&self) -> SharedKept {
        Arc::clone(&self.kept)
    }

    /// Takes back the pools the stores record, `restored`, so none is matched twice,
    /// and returns the fills a run stopped before finishing.
    pub(crate) fn restore(&mut self, restored: Vec<Matched>) -> Vec<Fill> {
        // Of each pool, its newest record, so a reorg of the block holding it is seen, and
        // the first that is not filled, whose block its fill runs up to, until a filled one
        // says it is done.
        let mut unfilled: HashMap<PoolKey, Matched> = HashMap::new();
        let mut filled: HashSet<PoolKey> = HashSet::new();
        for pool in restored {
            let key = (pool.contract, pool.pool_id);
            if pool.filled {
                filled.insert(key);
            } else if unfilled
                .get(&key)
                .is_none_or(|first| first.block_number > pool.block_number)
            {
                unfilled.insert(key, pool.clone());
            }
            let newer = self.matched.get(&key).is_none_or(|kept| {
                (kept.filled, kept.block_number) < (pool.filled, pool.block_number)
            });
            if newer {
                self.matched.insert(key, pool);
            }
        }
        let mut kept = self
            .kept
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (key, pool) in &self.matched {
            kept.keep(pool);
            self.filling.insert(*key);
        }
        let mut fills: HashMap<u64, Vec<Matched>> = HashMap::new();
        for (key, pool) in unfilled {
            if !filled.contains(&key) {
                fills.entry(pool.block_number).or_default().push(pool);
            }
        }
        fills
            .into_iter()
            .map(|(block, pools)| Fill {
                pools,
                through: block.saturating_sub(1),
            })
            .collect()
    }

    /// The fetch of every Metric pool through `through`, asked for once, at the first
    /// block the run writes.
    pub(crate) fn start(&mut self, through: u64) -> Option<Query> {
        if self.started {
            return None;
        }
        self.started = true;
        Some(self.reference.query(None, through))
    }

    /// What `decoded`, decoded as `event`, tells the matching, if anything.
    fn seen(&self, decoded: &Decoded, event: DecodedAs) -> Option<Seen> {
        if self.reference.binds(event) {
            let pair = self.pair(&self.reference.rule, decoded)?;
            return Some(Seen::Pair {
                pair,
                timestamp: decoded.block_timestamp,
            });
        }
        let bound = self.candidates.iter().find(|bound| bound.binds(event))?;
        let rule = &bound.rule;
        let pair = self.pair(rule, decoded)?;
        let [token0, token1] = rule.tokens.map(|name| argument_address(decoded, name));
        let (contract, pool_id, hooks) = match rule.pool {
            Pool::Contract(name) => (argument_address(decoded, name)?, None, None),
            Pool::Id { id, hooks, .. } => (
                decoded.address,
                Some(pool_id(decoded, id)?),
                argument_address(decoded, hooks).filter(|hooks| !hooks.is_zero()),
            ),
        };
        Some(Seen::Pool(
            Box::new(Matched {
                protocol: rule.protocol.to_owned(),
                contract,
                pool_id,
                token0: token0?,
                token1: token1?,
                fee: rule
                    .fee
                    .and_then(|name| argument_uint(decoded, name))
                    .unwrap_or_default(),
                hooks,
                created_block: decoded.block_number,
                from_timestamp: decoded.block_timestamp,
                filled: true,
                block_number: 0,
                block_hash: B256::ZERO,
                block_timestamp: 0,
            }),
            pair,
        ))
    }

    /// The pair `rule`'s event names in `decoded`.
    fn pair(&self, rule: &Rule, decoded: &Decoded) -> Option<Pair> {
        let [a, b] = rule.tokens.map(|name| argument_address(decoded, name));
        let native = |token: Address| if token.is_zero() { self.wrapped } else { token };
        let (a, b) = (native(a?), native(b?));
        (a != b).then(|| if a < b { (a, b) } else { (b, a) })
    }

    /// The searches that find every pool created through `through` on the pairs seen
    /// since the last call, which they are then taken off: one per protocol over all the
    /// pairs' tokens at once, and for Uniswap V4 one more with the native coin in place of
    /// the wrapped one, which its pools may hold instead. They find every pool on those
    /// pairs and some on pairs of their tokens crossed, which no Metric pool trades and
    /// [`Self::fetched`] passes over.
    pub(crate) fn queries(&mut self, through: u64) -> Vec<Query> {
        if self.asking.is_empty() {
            return Vec::new();
        }
        let pairs = std::mem::take(&mut self.asking);
        let lower: BTreeSet<Address> = pairs.iter().map(|pair| pair.0).collect();
        let upper: BTreeSet<Address> = pairs.iter().map(|pair| pair.1).collect();
        let native = BTreeSet::from([Address::ZERO]);
        let beside_wrapped: BTreeSet<Address> = pairs
            .iter()
            .filter_map(|&(a, b)| match (a == self.wrapped, b == self.wrapped) {
                (true, _) => Some(b),
                (_, true) => Some(a),
                _ => None,
            })
            .collect();
        let mut queries = Vec::new();
        for bound in &self.candidates {
            queries.push(bound.query(Some([&lower, &upper]), through));
            if matches!(bound.rule.pool, Pool::Id { .. }) && !beside_wrapped.is_empty() {
                queries.push(bound.query(Some([&native, &beside_wrapped]), through));
            }
        }
        queries
    }

    /// Takes a record the stream decoded, as `event`: a Metric pool on a new pair is to
    /// be searched for the pools already on it (see [`Self::queries`]), and a pool created
    /// on a known pair is kept from its creation, recorded with its block.
    pub(crate) fn decoded(&mut self, decoded: &Decoded, event: DecodedAs) {
        match self.seen(decoded, event) {
            Some(Seen::Pair { pair, timestamp }) => self.pair_seen(pair, timestamp),
            Some(Seen::Pool(pool, pair)) if self.pairs.contains_key(&pair) => {
                self.join(*pool, None);
            }
            Some(Seen::Pool(..)) | None => {}
        }
    }

    /// A Metric pool on `pair` at `timestamp`: a new pair is to be searched for the pools
    /// already on it, and a known one now dates from the earlier of its Metric pools.
    fn pair_seen(&mut self, pair: Pair, timestamp: u64) {
        match self.pairs.get(&pair) {
            Some(&first) if first <= timestamp => {}
            Some(_) => {
                self.pairs.insert(pair, timestamp);
            }
            None => {
                self.pairs.insert(pair, timestamp);
                self.asking.insert(pair);
            }
        }
    }

    /// Takes a creation log a search found, decoded as `event`, with the contract it
    /// creates when it is a pool the run does not decode yet, and returns whether a pool
    /// joined, which the caller then admits `contract` for. A Metric pool on a new pair
    /// is to be searched for the pools on it (see [`Self::queries`]). A pool joins after
    /// the marker it was taken at, so the next block's rows are kept and the next block
    /// records it; its rows before that are filled.
    pub(crate) fn fetched(
        &mut self,
        decoded: &Decoded,
        event: DecodedAs,
        contract: Option<Contract>,
    ) -> bool {
        match self.seen(decoded, event) {
            Some(Seen::Pair { pair, timestamp }) => {
                self.pair_seen(pair, timestamp);
                false
            }
            Some(Seen::Pool(mut pool, pair)) => {
                let Some(&first) = self.pairs.get(&pair) else {
                    return false;
                };
                pool.filled = false;
                // A pool created after the lookback starts has nothing before its creation.
                pool.from_timestamp = first.saturating_sub(self.lookback).max(pool.from_timestamp);
                self.join(*pool, contract)
            }
            None => false,
        }
    }

    /// Adds `pool` to the kept pools and the next block's records, unless it is matched
    /// already, returning whether it joined.
    fn join(&mut self, pool: Matched, contract: Option<Contract>) -> bool {
        let key = (pool.contract, pool.pool_id);
        if self.matched.contains_key(&key)
            || self
                .joining
                .iter()
                .any(|(joining, _)| (joining.contract, joining.pool_id) == key)
        {
            return false;
        }
        self.kept
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keep(&pool);
        self.joining.push((pool, contract));
        true
    }

    /// Takes `fill` as done: its pools are recorded filled with the next block.
    pub(crate) fn filled(&mut self, fill: &Fill) {
        for pool in &fill.pools {
            let key = (pool.contract, pool.pool_id);
            let Some(matched) = self.matched.get(&key) else {
                continue;
            };
            if matched.filled {
                continue;
            }
            let filled = Matched {
                filled: true,
                ..matched.clone()
            };
            self.joining.push((filled, None));
        }
    }

    /// The records the next block, `block`, holds: each joining pool's `contracts` row
    /// when it has one, then its record. They stay joining until [`Self::recorded`], once
    /// published.
    pub(crate) fn records(&self, block: &BlockMeta) -> Vec<Event> {
        let mut events = Vec::with_capacity(2 * self.joining.len());
        for (pool, contract) in &self.joining {
            if let Some(contract) = contract {
                events.push(Event::Contract(Box::new(contract.clone())));
            }
            events.push(Event::Matched(Box::new(at(pool, block))));
        }
        events
    }

    /// Takes the joining pools as recorded in `block`, once [`Self::records`] are
    /// published, and returns the fills their records call for: the earlier rows of each
    /// pool that existed before it was matched, through the block before this one.
    pub(crate) fn recorded(&mut self, block: &BlockMeta) -> Option<Fill> {
        let mut fill = Fill {
            pools: Vec::new(),
            through: block.height.saturating_sub(1),
        };
        for (pool, _) in std::mem::take(&mut self.joining) {
            let pool = at(&pool, block);
            tracing::info!(protocol = %pool.protocol, contract = %pool.contract,
                pool_id = ?pool.pool_id, filled = pool.filled, block = block.height,
                "pool matched to a Metric pair recorded");
            let key = (pool.contract, pool.pool_id);
            if !pool.filled && self.filling.insert(key) {
                fill.pools.push(pool.clone());
            }
            let replaces = self
                .matched
                .get(&key)
                .is_none_or(|kept| !kept.filled || pool.filled);
            if replaces {
                self.matched.insert(key, pool);
            }
        }
        (!fill.pools.is_empty()).then_some(fill)
    }

    /// Forgets the records `orphaned` deleted, so the next block records each again,
    /// unless the pool was created in the same block, the stream having matched it from
    /// its creation: then it was never created, and is no longer kept.
    pub(crate) fn rejoin(&mut self, orphaned: &[B256]) {
        let lost: Vec<PoolKey> = self
            .matched
            .iter()
            .filter(|(_, pool)| orphaned.contains(&pool.block_hash))
            .map(|(&key, _)| key)
            .collect();
        for key in lost {
            let Some(pool) = self.matched.remove(&key) else {
                continue;
            };
            if pool.filled && pool.created_block == pool.block_number {
                self.kept
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .forget(&pool);
                self.filling.remove(&key);
            } else {
                self.joining.push((pool, None));
            }
        }
    }
}

/// `pool` as recorded in `block`.
fn at(pool: &Matched, block: &BlockMeta) -> Matched {
    Matched {
        block_number: block.height,
        block_hash: block.hash,
        block_timestamp: block.timestamp,
        ..pool.clone()
    }
}

/// The argument of `decoded` named `name`, indexed or not.
fn argument<'a>(decoded: &'a Decoded, name: &str) -> Option<&'a TypedValue> {
    decoded
        .indexed
        .iter()
        .chain(&decoded.body)
        .find(|argument| argument.name == name)
        .map(|argument| &argument.value)
}

/// The `address` argument of `decoded` named `name`.
fn argument_address(decoded: &Decoded, name: &str) -> Option<Address> {
    match argument(decoded, name)? {
        TypedValue::Address { value } => Some(*value),
        _ => None,
    }
}

/// The unsigned integer argument of `decoded` named `name`, when it fits in 64 bits.
fn argument_uint(decoded: &Decoded, name: &str) -> Option<u64> {
    match argument(decoded, name)? {
        TypedValue::Uint { value, .. } => u64::try_from(*value).ok(),
        _ => None,
    }
}

/// The `bytes32` argument of `decoded` named `name`.
fn pool_id(decoded: &Decoded, name: &str) -> Option<B256> {
    match argument(decoded, name)? {
        TypedValue::FixedBytes { value, size: 32 } => B256::try_from(value.as_ref()).ok(),
        _ => None,
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
pub(super) mod tests {
    use alloy_dyn_abi::DynSolValue;
    use alloy_primitives::{Address, B256, I256, U256, address, keccak256};

    use super::{Kept, Matching};
    use crate::decode::fixtures::shipped;
    use crate::decode::{Decoder, Record};
    use crate::wire::datasets::evm::{Contract, Decoded, Log};
    use crate::wire::envelope::{BlockMeta, ChainId, Event, Matched};

    pub(in crate::decode) const USDC: Address =
        address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
    pub(in crate::decode) const WETH: Address =
        address!("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
    const V3_FACTORY: Address = address!("0x1F98431c8aD98523631AE4a59f267346ea31F984");
    const POOL_MANAGER: Address = address!("0x000000000004444c5dc75cB358380D2e3dE08A90");
    const DAY: u64 = 86_400;
    /// When the first Metric pool on the pair opened: after the V3 pool was created, and
    /// within 30 days after the V4 pool was.
    const METRIC_OPENED: u64 = 1_700_000_020 + 10 * DAY;

    /// The matching over the shipped catalog on Ethereum, keeping 30 days, and a decoder
    /// over the same catalog.
    pub(in crate::decode) fn ethereum() -> (Matching, Decoder) {
        let chain = ChainId::new("ethereum");
        let matching = Matching::new(&shipped("ethereum"), &chain, 30 * DAY).expect("matches");
        (matching, Decoder::new(shipped("ethereum"), []))
    }

    /// `log` stamped as of `block` at `timestamp`.
    fn at(log: Log, block: u64, timestamp: u64) -> Log {
        Log {
            block_number: block,
            block_hash: B256::with_last_byte(u8::try_from(block % 256).expect("small")),
            block_timestamp: timestamp,
            ..log
        }
    }

    /// Metric's `PoolCreated` on `token0`/`token1`, at `block` and `timestamp`.
    pub(in crate::decode) fn metric(
        token0: Address,
        token1: Address,
        block: u64,
        timestamp: u64,
    ) -> Log {
        at(
            crate::sink::fixtures::metric_pool_created(token0, token1),
            block,
            timestamp,
        )
    }

    /// Uniswap V3's factory creating `pool` on `token0`/`token1` at fee 500.
    pub(in crate::decode) fn v3_created(
        token0: Address,
        token1: Address,
        pool: Address,
        block: u64,
    ) -> Log {
        at(
            Log {
                address: V3_FACTORY,
                topic0: Some(keccak256(
                    "PoolCreated(address,address,uint24,int24,address)",
                )),
                topic1: Some(token0.into_word()),
                topic2: Some(token1.into_word()),
                topic3: Some(B256::left_padding_from(&500_u16.to_be_bytes())),
                data: DynSolValue::Tuple(vec![
                    DynSolValue::Int(I256::try_from(10).expect("fits"), 24),
                    DynSolValue::Address(pool),
                ])
                .abi_encode_params()
                .into(),
                log_index: 3,
                ..Log::default()
            },
            block,
            1_600_000_000 + block,
        )
    }

    /// Uniswap V4's `PoolManager` initializing pool `id` on `currency0`/`currency1` at
    /// fee 3000, with no hooks.
    pub(in crate::decode) fn v4_initialized(
        id: B256,
        currency0: Address,
        currency1: Address,
        block: u64,
    ) -> Log {
        at(
            Log {
                address: POOL_MANAGER,
                topic0: Some(keccak256(
                    "Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)",
                )),
                topic1: Some(id),
                topic2: Some(currency0.into_word()),
                topic3: Some(currency1.into_word()),
                data: DynSolValue::Tuple(vec![
                    DynSolValue::Uint(U256::from(3000), 24),
                    DynSolValue::Int(I256::try_from(60).expect("fits"), 24),
                    DynSolValue::Address(Address::ZERO),
                    DynSolValue::Uint(U256::from(1) << 96, 160),
                    DynSolValue::Int(I256::ZERO, 24),
                ])
                .abi_encode_params()
                .into(),
                log_index: 5,
                ..Log::default()
            },
            block,
            1_700_000_000 + block,
        )
    }

    /// `log` decoded, with the contract its claims name.
    pub(in crate::decode) fn decode(
        decoder: &Decoder,
        log: &Log,
    ) -> (Decoded, super::DecodedAs, Option<Contract>) {
        let (record, claims) = decoder.decode(log).expect("a listed contract's event");
        let Record { decoded, event } = record.expect("decodes");
        let contract = claims.contracts().next().cloned();
        (decoded, event, contract)
    }

    fn block(height: u64) -> BlockMeta {
        BlockMeta {
            height,
            hash: B256::with_last_byte(u8::try_from(height % 256).expect("small")),
            parent_hash: B256::ZERO,
            timestamp: 1_800_000_000 + height,
        }
    }

    fn matched(events: &[Event]) -> Vec<&Matched> {
        events
            .iter()
            .filter_map(|event| match event {
                Event::Matched(matched) => Some(matched.as_ref()),
                _ => None,
            })
            .collect()
    }

    /// The startup search asks for every Metric pool once; the pairs Metric pools name
    /// are searched together for every Uniswap V3 pool and every V4 pool on them, the V4
    /// ones holding the wrapped coin or the native one, and a known pair is not searched
    /// again.
    #[test]
    fn new_pairs_are_searched_for_every_pool_on_them() {
        let (mut matching, decoder) = ethereum();
        let start = matching.start(100).expect("the first block asks");
        assert_eq!(
            start.addresses,
            [address!("0x2a53833cc95548cf52c7b159110e22d3a9018f32")]
        );
        assert_eq!(start.through, 100);
        assert!(matching.start(101).is_none(), "and only once");

        let dai = address!("0x6B175474E89094C44Da98b954EedeAC495271d0F");
        for (token0, token1) in [(USDC, WETH), (dai, USDC)] {
            let (record, event, _) = decode(&decoder, &metric(token0, token1, 50, 30 * DAY));
            assert!(!matching.fetched(&record, event, None));
        }
        let queries = matching.queries(100);
        let word = |address: Address| address.into_word();
        let mut asked: Vec<_> = queries
            .iter()
            .map(|query| (query.addresses.clone(), query.topics[1..].to_vec()))
            .collect();
        asked.sort();
        let mut lower = vec![word(USDC), word(dai)];
        lower.sort();
        let mut upper = vec![word(USDC), word(WETH)];
        upper.sort();
        let mut expected = vec![
            (
                vec![V3_FACTORY],
                vec![lower.clone(), upper.clone(), Vec::new()],
            ),
            (vec![POOL_MANAGER], vec![Vec::new(), lower, upper]),
            (
                vec![POOL_MANAGER],
                vec![Vec::new(), vec![word(Address::ZERO)], vec![word(USDC)]],
            ),
        ];
        expected.sort();
        assert_eq!(asked, expected);
        assert!(queries.iter().all(|query| query.through == 100));
        assert!(matching.queries(101).is_empty(), "searched once");

        let (record, event, _) = decode(&decoder, &metric(USDC, WETH, 60, 31 * DAY));
        assert!(!matching.fetched(&record, event, None));
        assert!(
            matching.queries(102).is_empty(),
            "a known pair is not searched again"
        );
    }

    /// Pools a fetch finds on a known pair join at the next block: a V3 pool with its
    /// `contracts` row, a V4 pool by id, each kept from then on and filled before it, from
    /// the lookback before the pair's first Metric pool. A pool on another pair is not
    /// matched, and one created in the stream is kept from its creation, filled already.
    #[test]
    fn found_pools_join_at_the_next_block_and_are_filled_before_it() {
        let (mut matching, mut decoder) = ethereum();
        let (record, event, _) = decode(&decoder, &metric(USDC, WETH, 50, METRIC_OPENED));
        let _ = matching.fetched(&record, event, None);

        let pool = Address::repeat_byte(0x31);
        let id = B256::repeat_byte(0x41);
        let (record, event, contract) = decode(&decoder, &v3_created(USDC, WETH, pool, 10));
        assert!(contract.is_some(), "a pool the run does not decode yet");
        assert!(matching.fetched(&record, event, contract));
        let (record, event, _) = decode(&decoder, &v4_initialized(id, Address::ZERO, USDC, 20));
        assert!(matching.fetched(&record, event, None));
        let other = B256::repeat_byte(0x42);
        let (record, event, _) = decode(
            &decoder,
            &v4_initialized(other, USDC, Address::repeat_byte(0x99), 20),
        );
        assert!(!matching.fetched(&record, event, None), "another pair");

        let kept = matching.kept();
        let swap = |address| {
            Event::Log(Box::new(Log {
                address,
                ..Log::default()
            }))
        };
        assert!(kept.read().expect("readable").keeps(&swap(pool)));
        assert!(
            !kept
                .read()
                .expect("readable")
                .keeps(&swap(Address::repeat_byte(0x32)))
        );

        let records = matching.records(&block(101));
        assert!(
            matches!(records[0], Event::Contract(_)),
            "the pool's row first"
        );
        let pools = matched(&records);
        assert_eq!(pools.len(), 2);
        let v3 = pools
            .iter()
            .find(|pool| pool.pool_id.is_none())
            .expect("v3");
        assert_eq!(
            (v3.contract, v3.token0, v3.token1, v3.fee),
            (pool, USDC, WETH, 500)
        );
        assert_eq!(
            v3.from_timestamp,
            METRIC_OPENED - 30 * DAY,
            "30 days before the pair's first pool"
        );
        assert!(!v3.filled);
        let v4 = pools
            .iter()
            .find(|pool| pool.pool_id.is_some())
            .expect("v4");
        assert_eq!(
            (v4.contract, v4.pool_id, v4.token0, v4.fee),
            (POOL_MANAGER, Some(id), Address::ZERO, 3000)
        );
        assert_eq!(v4.block_number, 101);
        assert_eq!(
            v4.from_timestamp, 1_700_000_020,
            "created within the lookback, so kept from its creation"
        );

        let fill = matching.recorded(&block(101)).expect("a fill");
        assert_eq!(fill.through, 100);
        assert_eq!(fill.pools.len(), 2);
        assert!(matching.records(&block(102)).is_empty(), "recorded once");

        // A pool created in the stream on the pair is kept from its creation.
        let created = Address::repeat_byte(0x33);
        let (record, event, contract) = decode(&decoder, &v3_created(WETH, USDC, created, 102));
        if let Some(contract) = contract {
            let (_, claims) = decoder
                .decode(&v3_created(WETH, USDC, created, 102))
                .expect("decodes");
            decoder.admit(claims);
            let _ = contract;
        }
        matching.decoded(&record, event);
        let records = matching.records(&block(102));
        assert_eq!(matched(&records).len(), 1);
        assert!(matched(&records)[0].filled, "nothing before its creation");
        assert!(
            matching.recorded(&block(102)).is_none(),
            "and nothing to fill"
        );

        // Once filled, each is recorded so.
        matching.filled(&fill);
        let records = matching.records(&block(103));
        assert!(matched(&records).iter().all(|pool| pool.filled));
        assert_eq!(matched(&records).len(), 2);
        assert!(matching.recorded(&block(103)).is_none());
    }

    /// A reorg of the block recording a pool records it again with the next, without
    /// filling it again; a restart takes the records back, keeps their pools, and fills
    /// what was not filled.
    #[test]
    fn records_survive_a_reorg_and_a_restart() {
        let (mut matching, decoder) = ethereum();
        let (record, event, _) = decode(&decoder, &metric(USDC, WETH, 50, 40 * DAY));
        let _ = matching.fetched(&record, event, None);
        let id = B256::repeat_byte(0x41);
        let (record, event, _) = decode(&decoder, &v4_initialized(id, USDC, WETH, 20));
        assert!(matching.fetched(&record, event, None));
        let records = matching.records(&block(101));
        assert!(matching.recorded(&block(101)).is_some());

        matching.rejoin(&[block(101).hash]);
        let again = matching.records(&block(102));
        assert_eq!(matched(&again).len(), 1);
        assert!(matching.recorded(&block(102)).is_none(), "filled only once");

        let (mut restarted, _) = ethereum();
        let stored: Vec<Matched> = matched(&again).into_iter().cloned().collect();
        let fills = restarted.restore(stored);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].through, 101);
        let manager = Event::Decoded(Box::new(record));
        assert!(restarted.kept().read().expect("readable").keeps(&manager));
        let _ = records;
    }

    /// A V3 pool created in the stream on a known pair is kept from its creation, its
    /// factory's record creating it too; a reorg orphaning the block that created it
    /// forgets it rather than recording it again.
    #[test]
    fn a_pool_created_in_an_orphaned_block_is_forgotten() {
        let (mut matching, decoder) = ethereum();
        let (record, event, _) = decode(&decoder, &metric(USDC, WETH, 50, METRIC_OPENED));
        let _ = matching.fetched(&record, event, None);
        let pool = Address::repeat_byte(0x35);
        let created = v3_created(USDC, WETH, pool, 102);
        let (record, event, _) = decode(&decoder, &created);
        matching.decoded(&record, event);
        let kept = matching.kept();
        assert!(
            kept.read()
                .expect("readable")
                .keeps(&Event::Decoded(Box::new(record))),
            "the factory's record creating a kept pool"
        );
        assert_eq!(matched(&matching.records(&block(102))).len(), 1);
        assert!(matching.recorded(&block(102)).is_none());
        matching.rejoin(&[block(102).hash]);
        assert!(
            matching.records(&block(103)).is_empty(),
            "not recorded again"
        );
        let swap = Event::Log(Box::new(Log {
            address: pool,
            ..Log::default()
        }));
        assert!(
            !kept.read().expect("readable").keeps(&swap),
            "and no longer kept"
        );
    }

    /// A restart takes each pool's newest record, so a reorg of the block holding its
    /// `filled` record is seen, and fills only a pool with no `filled` record, up to the
    /// block of its first.
    #[test]
    fn a_restart_takes_the_newest_record_and_fills_only_what_is_unfilled() {
        let (mut matching, _) = ethereum();
        let record = |id: u8, height: u64, filled: bool| Matched {
            protocol: "uniswap_v4".to_owned(),
            contract: POOL_MANAGER,
            pool_id: Some(B256::repeat_byte(id)),
            token0: USDC,
            token1: WETH,
            fee: 3000,
            hooks: None,
            created_block: 1,
            from_timestamp: 0,
            filled,
            block_number: height,
            block_hash: block(height).hash,
            block_timestamp: block(height).timestamp,
        };
        let fills = matching.restore(vec![
            record(1, 10, true),
            record(1, 5, false),
            record(2, 7, false),
        ]);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].through, 6);
        assert_eq!(fills[0].pools[0].pool_id, Some(B256::repeat_byte(2)));
        matching.rejoin(&[block(10).hash]);
        let again = matching.records(&block(11));
        assert_eq!(matched(&again).len(), 1);
        assert!(matched(&again)[0].filled, "the filled record written again");
    }

    /// A contract holding many pools keeps the decoded records naming a kept pool's
    /// `id`, none of its raw logs, and no record naming another pool or a token's `id`.
    #[test]
    fn a_pool_manager_keeps_only_its_kept_pools_records() {
        let (_, decoder) = ethereum();
        let id = B256::repeat_byte(0x41);
        let kept = Kept::of(&[], &[(POOL_MANAGER, id)]);
        let ours = v4_initialized(id, USDC, WETH, 20);
        let theirs = v4_initialized(B256::repeat_byte(0x42), USDC, WETH, 20);
        let (record, _, _) = decode(&decoder, &ours);
        assert!(kept.keeps(&Event::Decoded(Box::new(record))));
        let (record, _, _) = decode(&decoder, &theirs);
        assert!(!kept.keeps(&Event::Decoded(Box::new(record))));
        assert!(!kept.keeps(&Event::Log(Box::new(ours))), "no raw log");
        let transfer = Log {
            address: POOL_MANAGER,
            topic0: Some(keccak256(
                "Transfer(address,address,address,uint256,uint256)",
            )),
            topic1: Some(Address::repeat_byte(5).into_word()),
            topic2: Some(Address::repeat_byte(6).into_word()),
            topic3: Some(id),
            data: vec![0; 64].into(),
            ..Log::default()
        };
        let (record, _, _) = decode(&decoder, &transfer);
        assert!(
            !kept.keeps(&Event::Decoded(Box::new(record))),
            "a token's id is not a pool's"
        );
    }
}
