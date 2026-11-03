//! The records the decode stage publishes with a block rather than with a log: a
//! contract a backfill handed over ([`Handoff`]) and a pool matched to a Metric pair
//! ([`Matched`]), each naming the block from which the run decodes or keeps it.
//!
//! Both follow one course, which [`BlockRecords`] keeps. A record *joins* between two
//! blocks, after a marker; the next block's batch publishes it, with the `contracts` row
//! of a contract the run did not decode before, just before that block's own marker, so
//! every store commits it with the block. It is *recorded* only once published, so a
//! publish that fails publishes it again. A reorg that orphans the block deletes the
//! record with the block's rows, so the record *joins again* and the next block publishes
//! it once more.

use std::collections::HashMap;
use std::hash::Hash;

use alloy_primitives::{Address, B256};

use crate::wire::datasets::evm::Contract;
use crate::wire::envelope::{BlockMeta, Event, Handoff, Matched};

/// A record published with the block that holds it.
pub(crate) trait BlockRecord: Clone {
    /// What identifies the thing it records, of which the decode stage keeps one record.
    type Key: Copy + Eq + Hash;
    /// The record's key.
    fn key(&self) -> Self::Key;
    /// The block it is published with.
    fn block_hash(&self) -> B256;
    /// The record as published with `block`.
    #[must_use]
    fn stamped(&self, block: &BlockMeta) -> Self;
    /// The record as an envelope's event.
    fn event(self) -> Event;
}

/// [`BlockRecord`] for a record of `block_number`, `block_hash`, and `block_timestamp`
/// fields, keyed by `$key`, published as `Event::$variant`.
macro_rules! block_record {
    ($record:ty, $variant:ident, $key:ty, |$this:ident| $read:expr) => {
        impl BlockRecord for $record {
            type Key = $key;

            fn key(&self) -> $key {
                let $this = self;
                $read
            }

            fn block_hash(&self) -> B256 {
                self.block_hash
            }

            fn stamped(&self, block: &BlockMeta) -> Self {
                Self {
                    block_number: block.height,
                    block_hash: block.hash,
                    block_timestamp: block.timestamp,
                    ..self.clone()
                }
            }

            fn event(self) -> Event {
                Event::$variant(Box::new(self))
            }
        }
    };
}

block_record!(Handoff, Handoff, Address, |handoff| handoff.address);
// A pool is the contract its logs are, and its id within it.
block_record!(Matched, Matched, (Address, Option<B256>), |pool| (
    pool.contract,
    pool.pool_id
));

/// The records of one kind: those joining at the next block, and the latest recorded of
/// each key.
#[derive(Debug)]
pub(crate) struct BlockRecords<R: BlockRecord> {
    /// Records that joined since the last marker, each with the `contracts` row to
    /// publish before it, if any.
    joining: Vec<(R, Option<Contract>)>,
    /// The latest record of each key, as the stores hold it or this run published it.
    recorded: HashMap<R::Key, R>,
}

impl<R: BlockRecord> Default for BlockRecords<R> {
    fn default() -> Self {
        Self {
            joining: Vec::new(),
            recorded: HashMap::new(),
        }
    }
}

impl<R: BlockRecord> BlockRecords<R> {
    /// Takes `record` back as the stores hold it, the latest of its key.
    pub(crate) fn restore(&mut self, record: R) {
        self.recorded.insert(record.key(), record);
    }

    /// The latest record of `key`.
    pub(crate) fn get(&self, key: &R::Key) -> Option<&R> {
        self.recorded.get(key)
    }

    /// Every key's latest record.
    pub(crate) fn latest(&self) -> impl Iterator<Item = &R> {
        self.recorded.values()
    }

    /// Whether `key` is recorded, or joining at the next block.
    pub(crate) fn knows(&self, key: &R::Key) -> bool {
        self.recorded.contains_key(key)
            || self
                .joining
                .iter()
                .any(|(joining, _)| joining.key() == *key)
    }

    /// Adds `record` to the next block's, after `contract`'s row when there is one.
    pub(crate) fn join(&mut self, record: R, contract: Option<Contract>) {
        self.joining.push((record, contract));
    }

    /// The events the next block, `block`, publishes before its marker: each joining
    /// record's `contracts` row when it has one, then the record. They stay joining until
    /// [`Self::recorded`], once published.
    pub(crate) fn events(&self, block: &BlockMeta) -> Vec<Event> {
        let mut events = Vec::with_capacity(2 * self.joining.len());
        for (record, contract) in &self.joining {
            if let Some(contract) = contract {
                events.push(Event::Contract(Box::new(contract.clone())));
            }
            events.push(record.stamped(block).event());
        }
        events
    }

    /// Takes the joining records as published with `block`, once [`Self::events`] are,
    /// and returns them as recorded.
    pub(crate) fn recorded(&mut self, block: &BlockMeta) -> Vec<R> {
        let recorded: Vec<R> = std::mem::take(&mut self.joining)
            .into_iter()
            .map(|(record, _)| record.stamped(block))
            .collect();
        for record in &recorded {
            self.recorded.insert(record.key(), record.clone());
        }
        recorded
    }

    /// Takes out and returns the records `orphaned` deleted, for the caller to join again
    /// or let go.
    pub(crate) fn orphaned(&mut self, orphaned: &[B256]) -> Vec<R> {
        let lost: Vec<R::Key> = self
            .recorded
            .iter()
            .filter(|(_, record)| orphaned.contains(&record.block_hash()))
            .map(|(&key, _)| key)
            .collect();
        lost.into_iter()
            .filter_map(|key| self.recorded.remove(&key))
            .collect()
    }
}
