//! The chain-agnostic event payload, which is the wire contract consumers depend on.
//!
//! [`Event`] is the union of everything the indexer publishes, and it holds two
//! kinds of thing:
//!
//! - **Datasets** ([`Event::Block`], [`Event::Transaction`], [`Event::Receipt`],
//!   [`Event::Log`]): durable on-chain records, re-exported from [`crate::datasets`].
//!   Each carries its own identity fields and dedupe key. Their shape is per-chain,
//!   so the EVM records live in [`crate::datasets::evm`]; Solana's would be a sibling
//!   module and a variant here.
//! - **Control** ([`Reorg`], [`Finalized`]): signals about the indexer's own state,
//!   not records of a chain. They carry no verbatim payload and exist to drive a
//!   consumer's state machine, so they are defined here.
//!
//! Raw chain payloads are carried verbatim in each dataset's `raw` field. Decoding is
//! deliberately out of scope, so a consumer decodes with whatever ABI or IDL it
//! trusts.
//!
//! An [`Envelope`] carries the [`Event`], the `sequence` the pipeline assigned, and
//! the [`ChainId`] it came from. The schema version is a property of the serialized
//! form, so the sink that serializes stamps it.
//!
//! # Identity types
//!
//! Dataset identity uses [`B256`] and [`TxHash`](alloy_primitives::TxHash) from
//! `alloy-primitives`. They are
//! not hand-rolled here because alloy already models exactly this: a fixed 32-byte
//! identity whose JSON form is lowercase `0x` hex. The encoding is a wire contract,
//! so it is pinned by a test rather than assumed.

use std::fmt;

use alloy_primitives::B256;
use serde::{Deserialize, Serialize};

pub use crate::datasets::evm::{Block, Log, Receipt, Transaction};

/// Identifies the chain an event came from, for example `ethereum` or `solana`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChainId(String);

impl ChainId {
    /// Builds a chain identifier from any string-like value.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Borrows the identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ChainId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for ChainId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for ChainId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// A discontinuity: previously published blocks are no longer canonical.
///
/// A control signal rather than a dataset: it describes the indexer's view of the
/// chain changing, and has no verbatim payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reorg {
    /// Height of the new head at which the discontinuity was detected.
    pub height: u64,
    /// Hash of the new head.
    pub new_head_hash: B256,
    /// Hashes of the blocks that are no longer canonical, newest first.
    pub orphaned_hashes: Vec<B256>,
}

/// A finality watermark: this block and everything below it are permanent.
///
/// Taken from the chain's own definition of finality rather than a block count, and
/// published only when it advances. A later [`Reorg`] never retracts it, because a
/// reorg cannot reach a finalized block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Finalized {
    /// Height of the newest finalized block.
    pub height: u64,
    /// Hash of the newest finalized block.
    pub hash: B256,
}

/// Everything the indexer publishes, tagged by `type` on the wire.
///
/// Dataset payloads are boxed: a [`Block`] or [`Receipt`] carries a 256-byte bloom
/// filter, and without boxing every [`Log`] event — the overwhelming majority on a
/// busy block — would be padded to that size in memory and on the stack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    /// A block, from `eth_getBlockByNumber`.
    Block(Box<Block>),
    /// A transaction, from its block's `transactions` array.
    Transaction(Box<Transaction>),
    /// A transaction receipt, from `eth_getTransactionReceipt`.
    Receipt(Box<Receipt>),
    /// A log, from its receipt's `logs` array.
    Log(Box<Log>),
    /// A discontinuity in the published chain.
    Reorg(Reorg),
    /// A finality watermark.
    Finalized(Finalized),
}

impl Event {
    /// Names the event kind, for logs and metrics.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Block(_) => "block",
            Self::Transaction(_) => "transaction",
            Self::Receipt(_) => "receipt",
            Self::Log(_) => "log",
            Self::Reorg(_) => "reorg",
            Self::Finalized(_) => "finalized",
        }
    }

    /// Whether this event is a dataset record rather than a control signal.
    ///
    /// Datasets are durable on-chain records a consumer can store and deduplicate;
    /// control signals only drive the stream's state machine.
    #[must_use]
    pub const fn is_dataset(&self) -> bool {
        match self {
            Self::Block(_) | Self::Transaction(_) | Self::Receipt(_) | Self::Log(_) => true,
            Self::Reorg(_) | Self::Finalized(_) => false,
        }
    }

    /// A key that is stable across redelivery and unique per event.
    ///
    /// Consumer groups deliver at least once, so consumers deduplicate on this. For
    /// datasets the key is the record's on-chain identity; for control signals it is
    /// the identity of the change they announce. It is scoped to the stream, so it
    /// excludes the chain, and it deliberately excludes `sequence`, which changes if
    /// the indexer restarts and replays from a different point.
    #[must_use]
    pub fn dedupe_key(&self) -> String {
        match self {
            Self::Block(block) => block.dedupe_key(),
            Self::Transaction(transaction) => transaction.dedupe_key(),
            Self::Receipt(receipt) => receipt.dedupe_key(),
            Self::Log(log) => log.dedupe_key(),
            Self::Reorg(reorg) => format!("{}:{}:reorg", reorg.height, reorg.new_head_hash),
            Self::Finalized(finalized) => {
                format!("{}:{}:finalized", finalized.height, finalized.hash)
            }
        }
    }
}

/// A single event plus the metadata the pipeline assigns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    /// The chain this event came from.
    pub chain: ChainId,
    /// Per-chain monotonic position in the published stream. Consumers order by
    /// this; a reorg rewinds it.
    pub sequence: u64,
    /// The event itself.
    #[serde(flatten)]
    pub event: Event,
}

impl Envelope {
    /// Builds an envelope from the chain it came from and the sequence the pipeline
    /// assigned.
    #[must_use]
    pub const fn new(chain: ChainId, sequence: u64, event: Event) -> Self {
        Self {
            chain,
            sequence,
            event,
        }
    }

    /// Names the event kind, for logs and metrics.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        self.event.kind()
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::str::FromStr as _;

    use alloy_primitives::B256;

    use super::{Block, ChainId, Envelope, Event, Finalized};

    fn chain() -> ChainId {
        ChainId::new("ethereum")
    }

    fn hash(byte: u8) -> B256 {
        B256::from([byte; 32])
    }

    #[test]
    fn b256_wire_format_is_lowercase_0x_hex() {
        // This is the envelope's serialized identity format, so it is a contract:
        // if an alloy upgrade changed it, consumers would break silently.
        let value = hash(0xab);
        let encoded = serde_json::to_string(&value).expect("hash serializes");
        assert_eq!(
            encoded,
            format!("\"0x{}\"", "ab".repeat(32)),
            "block hash wire format changed"
        );
        let decoded: B256 = serde_json::from_str(&encoded).expect("hash deserializes");
        assert_eq!(decoded, value);
    }

    #[test]
    fn b256_rejects_wrong_length_hex() {
        assert!(B256::from_str("0xdeadbeef").is_err());
        assert!(B256::from_str("not hex").is_err());
    }

    #[test]
    fn datasets_are_flagged_and_control_signals_are_not() {
        assert!(
            Event::Block(Box::new(Block {
                number: 1,
                hash: hash(1),
                parent_hash: hash(0),
                timestamp: 1,
                ..Block::default()
            }))
            .is_dataset()
        );
        assert!(
            !Event::Finalized(Finalized {
                height: 1,
                hash: hash(1),
            })
            .is_dataset()
        );
    }

    #[test]
    fn envelope_serializes_event_fields_flat_with_a_type_tag() {
        let envelope = Envelope::new(
            chain(),
            4,
            Event::Block(Box::new(Block {
                number: 5,
                hash: hash(9),
                parent_hash: hash(8),
                timestamp: 1_700_000_000,
                ..Block::default()
            })),
        );
        let value = serde_json::to_value(&envelope).expect("envelope serializes");
        assert_eq!(value["type"], "block");
        assert_eq!(value["sequence"], 4);
        assert_eq!(value["number"], 5);
        assert_eq!(value["hash"], format!("0x{}", "09".repeat(32)));
        assert_eq!(value["chain"], "ethereum");
    }
}
