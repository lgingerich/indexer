//! The contracts backfills hand the running indexer: each discovered below the run's
//! restart, adopted by the decoder at a block boundary and recorded with the next block,
//! so the backfill decodes its logs below that block and the run from it.
//!
//! A record *joins* between two blocks, after a marker; the next block's batch publishes
//! it, with the `contracts` row of a contract the run did not decode before, just before
//! that block's own marker, so every store commits it with the block. It is *recorded*
//! only once published, so a publish that fails publishes it again. A reorg that orphans
//! the block deletes the record with the block's rows, so the record *joins again* and
//! the next block publishes it once more.

use std::collections::{HashMap, HashSet, VecDeque};

use alloy_primitives::{Address, B256};
use tokio::sync::mpsc;
use tracing::info;

use super::Decoder;
use super::decoder::Adopted;
use crate::wire::datasets::evm::Contract;
use crate::wire::envelope::{BlockMeta, Event, Handoff};

/// The most handed-over contracts joining at once, so a backfill handing over hundreds
/// of thousands spreads their rows over as many blocks as it takes rather than writing
/// them all with one, however long the blocks a store already holds keep them waiting.
pub(crate) const PER_BLOCK: usize = 10_000;

/// The decode stage's handed-over contracts: those the run's watch reads from the lake,
/// those waiting their turn, those joining at the next block, and their records.
#[derive(Debug)]
pub(crate) struct Handoffs {
    /// The contracts backfills hand over, as the watch reads them.
    handed: mpsc::Receiver<Vec<Contract>>,
    /// Those taken from `handed` that wait until fewer than [`PER_BLOCK`] are joining.
    waiting: VecDeque<Contract>,
    /// The records joining at the next block, each with the `contracts` row to publish
    /// before it when its contract is new to the set.
    joining: Vec<(Handoff, Option<Contract>)>,
    /// The contracts of `joining`, so a check for one does not walk it.
    joining_contracts: HashSet<Address>,
    /// The latest record of each contract, as the stores hold it or this run published it.
    recorded: HashMap<Address, Handoff>,
}

impl Handoffs {
    /// Takes contracts from `handed`, and back the records of those the stores hold,
    /// `restored`, so none is recorded twice.
    pub(crate) fn new(restored: Vec<Handoff>, handed: mpsc::Receiver<Vec<Contract>>) -> Self {
        Self {
            handed,
            waiting: VecDeque::new(),
            joining: Vec::new(),
            joining_contracts: HashSet::new(),
            recorded: restored
                .into_iter()
                .map(|handoff| (handoff.address, handoff))
                .collect(),
        }
    }

    /// Adds to `decoder` the contracts handed over that are neither recorded nor joining,
    /// until [`PER_BLOCK`] are joining, for the next block to record: each with its
    /// `contracts` row when it is new to the set. The rest wait for the blocks after.
    pub(crate) fn adopt(&mut self, decoder: &mut Decoder) {
        while let Ok(contracts) = self.handed.try_recv() {
            self.waiting.extend(contracts);
        }
        while self.joining.len() < PER_BLOCK {
            let Some(contract) = self.waiting.pop_front() else {
                break;
            };
            if self.recorded.contains_key(&contract.address)
                || self.joining_contracts.contains(&contract.address)
            {
                continue;
            }
            let row = match decoder.adopt(&contract) {
                Adopted::Added => Some(contract.clone()),
                Adopted::Known => None,
                Adopted::Refused => continue,
            };
            let handoff = Handoff {
                protocol: contract.protocol,
                name: contract.name,
                address: contract.address,
                block_number: 0,
                block_hash: B256::ZERO,
                block_timestamp: 0,
            };
            self.join(handoff, row);
        }
    }

    /// The events the next block, `block`, publishes before its marker: each joining
    /// record's `contracts` row when it has one, then the record. They stay joining until
    /// [`Self::recorded`], once published.
    pub(crate) fn events(&self, block: &BlockMeta) -> Vec<Event> {
        let mut events = Vec::with_capacity(2 * self.joining.len());
        for (handoff, contract) in &self.joining {
            if let Some(contract) = contract {
                events.push(Event::Contract(Box::new(contract.clone())));
            }
            events.push(Event::Handoff(Box::new(stamped(handoff, block))));
        }
        events
    }

    /// Takes the joining records as published with `block`, once [`Self::events`] are.
    pub(crate) fn recorded(&mut self, block: &BlockMeta) {
        if self.joining.is_empty() {
            return;
        }
        let recorded = self.joining.len();
        self.joining_contracts.clear();
        for (handoff, _) in std::mem::take(&mut self.joining) {
            self.recorded
                .insert(handoff.address, stamped(&handoff, block));
        }
        info!(
            contracts = recorded,
            waiting = self.waiting.len(),
            block = block.height,
            "handed-over contracts decoded from this block on"
        );
    }

    /// Joins again the records `orphaned` deleted, so the next block records each again.
    pub(crate) fn rejoin(&mut self, orphaned: &[B256]) {
        let lost: Vec<Handoff> = self
            .recorded
            .extract_if(|_, handoff| orphaned.contains(&handoff.block_hash))
            .map(|(_, handoff)| handoff)
            .collect();
        for handoff in lost {
            self.join(handoff, None);
        }
    }

    /// Adds `handoff` to the next block's, after `contract`'s row when there is one.
    fn join(&mut self, handoff: Handoff, contract: Option<Contract>) {
        self.joining_contracts.insert(handoff.address);
        self.joining.push((handoff, contract));
    }
}

/// `handoff` as published with `block`.
fn stamped(handoff: &Handoff, block: &BlockMeta) -> Handoff {
    Handoff {
        block_number: block.height,
        block_hash: block.hash,
        block_timestamp: block.timestamp,
        ..handoff.clone()
    }
}
