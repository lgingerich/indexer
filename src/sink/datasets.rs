//! Which on-chain datasets a deployment fetches and stores.
//!
//! A block's metadata is reported separately from its events, so the selection is
//! enforced where the events are projected: `src/ingest/source/evm.rs` reads only what
//! the selected datasets need, which is what makes the logs-only fetch skip its block
//! read. [`Datasets::keeps`] is the same answer as a predicate, for a consumer that
//! decodes a whole batch itself and has to filter. Control signals always pass.

use serde::de::{self, Unexpected};
use serde::{Deserialize, Deserializer};

use crate::wire::envelope::Event;

/// Which on-chain datasets a deployment fetches and stores.
///
/// Absent from the settings file means all four. An empty list is rejected:
/// a run that stores nothing is not a deployment. The flags are independent:
/// any combination is valid, so this is not a state machine.
#[expect(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Datasets {
    /// Store block headers.
    pub block: bool,
    /// Store transactions. The node is asked for full transaction objects.
    pub transaction: bool,
    /// Store receipts. Logs selected alongside receipts are taken from them.
    pub receipt: bool,
    /// Store logs.
    pub log: bool,
}

impl Default for Datasets {
    fn default() -> Self {
        Self::all()
    }
}

impl Datasets {
    /// Every dataset. This is the fetch the source used before selection existed.
    #[must_use]
    pub const fn all() -> Self {
        Self {
            block: true,
            transaction: true,
            receipt: true,
            log: true,
        }
    }

    /// Whether this event is one of the selected datasets.
    ///
    /// The ingest source already projects only the selected datasets, so a pipeline
    /// consumer has nothing to drop. This is the filter for a caller that decodes
    /// through [`decode_block`](crate::ingest::source::evm::decode_block), which emits
    /// every row in its batch regardless of selection. A decoded record or discovered
    /// contract is kept only when logs are, because each is produced from a log. A
    /// reorg or accepted-block marker always passes: they are control signals, not
    /// datasets.
    #[must_use]
    pub const fn keeps(self, event: &Event) -> bool {
        match event {
            Event::Block(_) => self.block,
            Event::Transaction(_) => self.transaction,
            Event::Receipt(_) => self.receipt,
            Event::Log(_) | Event::Decoded(_) | Event::Contract(_) => self.log,
            Event::Reorg(_) | Event::AcceptedBlock(_) => true,
        }
    }

    /// Whether logs are the only thing fetched: nothing selected reads the block body.
    ///
    /// This is the condition under which a live notification's metadata stands in for
    /// the header and the block read is skipped. `block` and `transaction` are projected
    /// from the body, and `receipt` needs the body's transaction identities, so each
    /// rules the reuse out.
    #[must_use]
    pub const fn logs_only(self) -> bool {
        self.log && !self.block && !self.transaction && !self.receipt
    }
}

impl std::fmt::Display for Datasets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut names = Vec::new();
        if self.block {
            names.push("block");
        }
        if self.transaction {
            names.push("transaction");
        }
        if self.receipt {
            names.push("receipt");
        }
        if self.log {
            names.push("log");
        }
        write!(f, "{}", names.join(","))
    }
}

impl<'de> Deserialize<'de> for Datasets {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let names = Vec::<String>::deserialize(deserializer)?;
        if names.is_empty() {
            return Err(de::Error::invalid_value(
                Unexpected::Seq,
                &"one or more of `block`, `transaction`, `receipt`, `log`",
            ));
        }
        let mut datasets = Self {
            block: false,
            transaction: false,
            receipt: false,
            log: false,
        };
        for name in &names {
            let slot = match name.as_str() {
                "block" => &mut datasets.block,
                "transaction" => &mut datasets.transaction,
                "receipt" => &mut datasets.receipt,
                "log" => &mut datasets.log,
                other => {
                    return Err(de::Error::unknown_variant(
                        other,
                        &["block", "transaction", "receipt", "log"],
                    ));
                }
            };
            if *slot {
                return Err(de::Error::custom(format!("duplicate dataset `{name}`")));
            }
            *slot = true;
        }
        Ok(datasets)
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{Address, B256, TxHash};

    use super::Datasets;
    use crate::wire::envelope::{Event, Log, Reorg};

    /// `keeps` is the filter for a `decode_block` consumer, so its answer is the
    /// contract: the selected datasets' events, decoded records with logs, and reorgs.
    #[test]
    fn keeps_selects_only_the_named_datasets() {
        let datasets: Datasets = serde_json::from_str(r#"["log"]"#).expect("datasets");
        let block = Event::Block(Box::default());
        let log = Event::Log(Box::new(Log {
            block_number: 1,
            block_hash: B256::ZERO,
            ..Log::default()
        }));
        let decoded = Event::Decoded(Box::new(crate::wire::envelope::Decoded {
            event_id: B256::ZERO,
            name: "Transfer".to_owned(),
            address: Address::ZERO,
            protocol: "erc20".to_owned(),
            selector: B256::ZERO,
            signature: "Transfer(address,address,uint256)".to_owned(),
            anonymous: false,
            transaction_hash: TxHash::ZERO,
            transaction_index: 0,
            log_index: 0,
            indexed: Vec::new(),
            body: Vec::new(),
            block_number: 1,
            block_hash: B256::ZERO,
            block_timestamp: 0,
        }));
        let reorg = Event::Reorg(Reorg {
            height: 1,
            new_head_hash: B256::ZERO,
            orphaned_hashes: vec![],
        });

        assert!(!datasets.keeps(&block), "block is not selected");
        assert!(datasets.keeps(&log), "log is selected");
        assert!(datasets.keeps(&decoded), "a decoded record follows its log");
        assert!(datasets.keeps(&reorg), "a reorg always passes");
    }
}
