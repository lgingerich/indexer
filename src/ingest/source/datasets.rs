//! Which on-chain datasets a deployment fetches and stores.
//!
//! A block's metadata is reported separately from its events, so the selection is
//! enforced where the events are projected: `src/ingest/source/evm/` reads only what
//! the selected datasets need, which is what makes the logs-only fetch skip its block
//! read.

use serde::de::{self, Unexpected};
use serde::{Deserialize, Deserializer};

/// Which on-chain datasets a deployment fetches and stores.
///
/// Absent from the settings file means all three. An empty list is rejected:
/// a run that stores nothing is not a deployment. The flags are independent:
/// any combination is valid, so this is not a state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Datasets {
    /// Store block headers.
    pub blocks: bool,
    /// Store transactions, each joined to its receipt. The node is asked for full
    /// transaction objects and the block's receipts, and logs selected alongside are
    /// taken from those receipts rather than fetched again.
    pub transactions: bool,
    /// Store logs.
    pub logs: bool,
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
            blocks: true,
            transactions: true,
            logs: true,
        }
    }
}

impl std::fmt::Display for Datasets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut names = Vec::new();
        if self.blocks {
            names.push("blocks");
        }
        if self.transactions {
            names.push("transactions");
        }
        if self.logs {
            names.push("logs");
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
                &"one or more of `blocks`, `transactions`, `logs`",
            ));
        }
        let mut datasets = Self {
            blocks: false,
            transactions: false,
            logs: false,
        };
        for name in &names {
            let slot = match name.as_str() {
                "blocks" => &mut datasets.blocks,
                "transactions" => &mut datasets.transactions,
                "logs" => &mut datasets.logs,
                other => {
                    return Err(de::Error::unknown_variant(
                        other,
                        &["blocks", "transactions", "logs"],
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
