//! The chain-agnostic event envelope, which is the wire contract consumers depend on.
//!
//! Raw chain payloads are carried verbatim in [`Event`]'s `raw` field. This layer
//! only adds identity, ordering, and finality. Decoding is deliberately out of
//! scope, so a consumer can decode with whatever ABI or IDL it trusts. Every
//! envelope carries [`SCHEMA_VERSION`] so consumers can detect a shape change.
//!
//! # Chain identity types
//!
//! Block and transaction identity use [`B256`] and [`TxHash`] from
//! `alloy-primitives`. They are not hand-rolled here because alloy already models
//! exactly this: a fixed 32-byte identity whose JSON form is lowercase `0x` hex.
//! The encoding is a wire contract, so it is pinned by a test rather than assumed.
//!
//! When a chain arrives whose identity does not fit that shape, the extension is a
//! per-chain type plus a tagged union at this boundary, not a wider shared type.
//! Solana's base58 blockhash and 64-byte signature are the expected first case.

use std::fmt;

use alloy_primitives::{B256, TxHash};
use serde::{Deserialize, Serialize};

/// The envelope shape version stamped on every [`Envelope`].
///
/// Bump this whenever the envelope's shape changes so consumers can detect the
/// change rather than silently misreading events.
pub const SCHEMA_VERSION: u16 = 3;

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

/// The payload of an [`Envelope`], tagged by `type` on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    /// A block boundary marker, emitted before that block's events.
    Block {
        /// Block height, or slot on chains that use slots.
        height: u64,
        /// Canonical hash of this block.
        hash: B256,
        /// Hash of the block this one builds on.
        parent_hash: B256,
        /// Block timestamp in seconds since the Unix epoch.
        timestamp: u64,
        /// Number of transactions in the block.
        tx_count: u64,
        /// The block payload as the source returned it, minus its transactions,
        /// which are published as [`Event::Transaction`]s.
        raw: String,
    },
    /// One transaction and its receipt.
    Transaction {
        /// Height of the block containing this transaction.
        height: u64,
        /// Hash of the block containing this transaction.
        block_hash: B256,
        /// Identity of this transaction.
        tx_id: TxHash,
        /// Position of this transaction within its block.
        tx_index: u64,
        /// Verbatim transaction payload as the source returned it.
        raw: String,
        /// The receipt payload as the source returned it, minus its logs, which
        /// are published as [`Event::Log`]s.
        receipt: String,
    },
    /// One log or event within a block.
    Log {
        /// Height of the block containing this event.
        height: u64,
        /// Hash of the block containing this event.
        block_hash: B256,
        /// Identity of the transaction that produced this event.
        tx_id: TxHash,
        /// Position of the transaction within its block.
        tx_index: u64,
        /// Position of this event within its transaction; the EVM log index, and
        /// an instruction index on chains without logs.
        item_index: u64,
        /// Verbatim log payload as the source returned it.
        raw: String,
    },
    /// A discontinuity: previously published blocks are no longer canonical.
    Reorg {
        /// Height of the new head at which the discontinuity was detected.
        height: u64,
        /// Hash of the new head.
        new_head_hash: B256,
        /// Hashes of the blocks that are no longer canonical, newest first.
        orphaned_hashes: Vec<B256>,
    },
    /// A finality watermark: this block and everything below it are permanent.
    ///
    /// Taken from the chain's own definition of finality rather than a block
    /// count, and published only when it advances. A later [`Event::Reorg`] never
    /// retracts it, because a reorg cannot reach a finalized block.
    Finalized {
        /// Height of the newest finalized block.
        height: u64,
        /// Hash of the newest finalized block.
        hash: B256,
    },
}

impl Event {
    /// Names the event kind, for logs and metrics.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Block { .. } => "block",
            Self::Transaction { .. } => "transaction",
            Self::Log { .. } => "log",
            Self::Reorg { .. } => "reorg",
            Self::Finalized { .. } => "finalized",
        }
    }
}

/// A single event plus the metadata needed to order, deduplicate, and trust it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    /// The chain this event came from.
    pub chain: ChainId,
    /// Per-chain monotonic position in the published stream. Consumers order by
    /// this; a reorg rewinds it.
    pub sequence: u64,
    /// Shape version of this envelope; always [`SCHEMA_VERSION`] today.
    pub schema_version: u16,
    /// The event itself.
    #[serde(flatten)]
    pub event: Event,
}

impl Envelope {
    /// Builds an envelope at the current schema version.
    #[must_use]
    pub const fn new(chain: ChainId, sequence: u64, event: Event) -> Self {
        Self {
            chain,
            sequence,
            schema_version: SCHEMA_VERSION,
            event,
        }
    }

    /// A key that is stable across redelivery and unique per on-chain event.
    ///
    /// Consumer groups deliver at least once, so consumers deduplicate on this.
    /// It deliberately excludes `sequence`, which changes if the indexer restarts
    /// and replays from a different point.
    #[must_use]
    pub fn dedupe_key(&self) -> String {
        match &self.event {
            Event::Block { height, hash, .. } => {
                format!("{}:{height}:{hash}:block", self.chain)
            }
            Event::Transaction { height, tx_id, .. } => {
                format!("{}:{height}:{tx_id}:tx", self.chain)
            }
            Event::Log {
                height,
                tx_id,
                item_index,
                ..
            } => format!("{}:{height}:{tx_id}:{item_index}", self.chain),
            Event::Reorg {
                height,
                new_head_hash,
                ..
            } => format!("{}:{height}:{new_head_hash}:reorg", self.chain),
            Event::Finalized { height, hash } => {
                format!("{}:{height}:{hash}:finalized", self.chain)
            }
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

    use alloy_primitives::{B256, TxHash};

    use super::{ChainId, Envelope, Event};

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
    fn log_dedupe_key_is_stable_across_sequence_changes() {
        let tx_id = TxHash::from([0xab; 32]);
        let event = Event::Log {
            height: 21_000_000,
            block_hash: hash(1),
            tx_id,
            tx_index: 3,
            item_index: 7,
            raw: "{}".to_owned(),
        };
        let first = Envelope::new(chain(), 10, event.clone());
        let replay = Envelope::new(chain(), 99, event);
        assert_eq!(first.dedupe_key(), replay.dedupe_key());
        assert_eq!(first.dedupe_key(), format!("ethereum:21000000:{tx_id}:7"));
    }

    #[test]
    fn distinct_events_get_distinct_dedupe_keys() {
        let log = |item_index| {
            Envelope::new(
                chain(),
                0,
                Event::Log {
                    height: 1,
                    block_hash: hash(1),
                    tx_id: TxHash::from([0xaa; 32]),
                    tx_index: 0,
                    item_index,
                    raw: "{}".to_owned(),
                },
            )
        };
        assert_ne!(log(0).dedupe_key(), log(1).dedupe_key());
    }

    #[test]
    fn envelope_serializes_event_fields_flat_with_a_type_tag() {
        let envelope = Envelope::new(
            chain(),
            4,
            Event::Block {
                height: 5,
                hash: hash(9),
                parent_hash: hash(8),
                timestamp: 1_700_000_000,
                tx_count: 12,
                raw: "{}".to_owned(),
            },
        );
        let value = serde_json::to_value(&envelope).expect("envelope serializes");
        assert_eq!(value["type"], "block");
        assert_eq!(value["sequence"], 4);
        assert_eq!(value["height"], 5);
        assert_eq!(value["schema_version"], 3);
        assert_eq!(value["hash"], format!("0x{}", "09".repeat(32)));
    }
}
