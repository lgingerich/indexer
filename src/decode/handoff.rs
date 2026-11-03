//! The contracts backfills hand the running indexer: each discovered below the run's
//! restart, adopted by the decoder at a block boundary and recorded with the next block
//! (see [`super::records`]), so the backfill decodes its logs below that block and the
//! run from it.

use alloy_primitives::B256;
use tokio::sync::mpsc;
use tracing::info;

use super::Decoder;
use super::decoder::Adopted;
use super::records::BlockRecords;
use crate::wire::datasets::evm::Contract;
use crate::wire::envelope::{BlockMeta, Event, Handoff};

/// The decode stage's handed-over contracts: those the run's watch reads from the lake,
/// and their records.
#[derive(Debug)]
pub(crate) struct Handoffs {
    /// The contracts backfills hand over, as the watch reads them.
    handed: mpsc::Receiver<Vec<Contract>>,
    records: BlockRecords<Handoff>,
}

impl Handoffs {
    /// Takes contracts from `handed`, and back the records of those the stores hold,
    /// `restored`, so none is recorded twice.
    pub(crate) fn new(restored: Vec<Handoff>, handed: mpsc::Receiver<Vec<Contract>>) -> Self {
        let mut records = BlockRecords::default();
        for handoff in restored {
            records.restore(handoff);
        }
        Self { handed, records }
    }

    /// Adds to `decoder` the contracts handed over since the last marker that are neither
    /// recorded nor joining, for the next block to record: each with its `contracts` row
    /// when it is new to the set.
    pub(crate) fn adopt(&mut self, decoder: &mut Decoder) {
        while let Ok(contracts) = self.handed.try_recv() {
            for contract in contracts {
                if self.records.knows(&contract.address) {
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
                self.records.join(handoff, row);
            }
        }
    }

    /// The events the next block, `block`, publishes before its marker (see
    /// [`BlockRecords::events`]).
    pub(crate) fn events(&self, block: &BlockMeta) -> Vec<Event> {
        self.records.events(block)
    }

    /// Takes the joining records as published with `block`.
    pub(crate) fn recorded(&mut self, block: &BlockMeta) {
        for handoff in self.records.recorded(block) {
            info!(address = %handoff.address, protocol = %handoff.protocol,
                name = %handoff.name, block = block.height,
                "handed-over contract decoded from this block on");
        }
    }

    /// Joins again the records `orphaned` deleted, so the next block records each again.
    pub(crate) fn rejoin(&mut self, orphaned: &[B256]) {
        for handoff in self.records.orphaned(orphaned) {
            self.records.join(handoff, None);
        }
    }
}
