//! Immutable log decoding for ingestion.

use crate::decode::{ContractRegistry, DecodeError};
use crate::wire::envelope::{ChainId, Decoded, Log};

/// A log decoder backed by immutable, predefined registrations.
#[derive(Debug)]
pub struct Decoder {
    registry: ContractRegistry,
}

impl Decoder {
    /// Uses the validated registry for every live or historical lookup.
    #[must_use]
    pub const fn new(registry: ContractRegistry) -> Self {
        Self { registry }
    }

    /// Number of distinct configured chain/address pairs.
    #[must_use]
    pub(crate) fn registrations(&self) -> usize {
        self.registry.len()
    }

    /// ABI identity applicable to a log, for caller-owned diagnostics.
    #[must_use]
    pub fn abi_id(&self, chain: &ChainId, log: &Log) -> Option<alloy_primitives::B256> {
        self.registry
            .contract(chain, log.address, log.block_number)
            .map(|contract| contract.abi.id())
    }

    /// Decodes a log using its original block's registration.
    ///
    /// `None` means no applicable registration or no matching event selector.
    ///
    /// # Errors
    /// Returns the concrete decoding or schema invariant failure. Callers may skip bad
    /// logs while preserving raw records, but must stop on internal invariant failures.
    pub fn decode(&self, chain: &ChainId, log: &Log) -> Result<Option<Decoded>, DecodeError> {
        let Some(contract) = self.registry.contract(chain, log.address, log.block_number) else {
            return Ok(None);
        };
        let Some(event) = contract.abi.decode_log(log)? else {
            return Ok(None);
        };
        Ok(Some(Decoded {
            name: event.name,
            address: log.address,
            protocol: contract.protocol.clone(),
            abi_id: contract.abi.id(),
            selector: event.selector,
            signature: event.signature,
            anonymous: false,
            transaction_hash: log.transaction_hash,
            transaction_index: log.transaction_index,
            log_index: log.log_index,
            indexed: event.indexed,
            body: event.body,
            block_number: log.block_number,
            block_hash: log.block_hash,
            block_timestamp: log.block_timestamp,
        }))
    }
}
