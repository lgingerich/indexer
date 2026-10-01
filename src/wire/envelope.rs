//! The chain-agnostic event payload, which is the wire contract consumers depend on.
//!
//! [`Event`] is the union of everything the indexer publishes, and it holds two
//! kinds of thing:
//!
//! - **Datasets** ([`Event::Block`], [`Event::Transaction`], [`Event::Receipt`],
//!   [`Event::Log`]): durable on-chain records, re-exported from [`crate::wire::datasets`].
//!   Each carries its own identity fields and dedupe key. Their shape is per-chain,
//!   so the EVM records live in [`crate::wire::datasets::evm`]; Solana's would be a sibling
//!   module and a variant here.
//! - **Derived** ([`Event::Decoded`]): a record the decode stage produces from a
//!   dataset, carrying its typed arguments under their ABI names. It is a dataset,
//!   not a control signal, and it always follows the log it was decoded from.
//! - **Control** ([`Reorg`], [`Finalized`]): signals about the indexer's own state,
//!   not records of a chain. They carry no verbatim payload and exist to drive a
//!   consumer's state machine, so they are defined here.
//!
//! Decoding is deliberately out of scope here, so a consumer decodes with whatever
//! ABI or IDL it trusts. Every dataset field is present and typed, though, so a
//! consumer that only persists does not have to decode anything.
//!
//! An [`Envelope`] carries the [`Event`], the [`ChainId`] it came from, the
//! [`Envelope::sequence`] the pipeline assigned, and the [`SCHEMA_VERSION`] the
//! encoder wrote. The version is a field on the envelope rather than a property of
//! a sink's framing because one of the sinks is a local database: a transport header
//! survives no hop into `DuckDB`, a file, or a pipe, so a consumer reading those
//! could not tell two shapes apart.
//!
//! # Compatibility policy
//!
//! [`SCHEMA_VERSION`] is bumped only for a **breaking** change: a field's type or
//! meaning changing, a field or variant being removed, or a required field being
//! added. **Additive** changes — a new optional field, a new [`Event`] variant —
//! do not bump it; consumers must skip an unknown `type` and ignore unknown fields,
//! which is what keeps an additive change safe without a version bump.
//!
//! A breaking change needs a coexistence window in the store, because a reader
//! reading across the change sees both shapes interleaved. That is the point of
//! the number: it lets a consumer reading a stream tell which shape it has.
//!
//! The line every sink writes is pinned by `every_variant_round_trips_through_json`
//! and `the_wire_object_carries_only_the_envelope_and_event_fields`.
//!
//! # Identity types
//!
//! Dataset identity uses [`B256`] and [`TxHash`] from
//! `alloy-primitives`. They are
//! not hand-rolled here because alloy already models exactly this: a fixed 32-byte
//! identity whose JSON form is lowercase `0x` hex. The encoding is a wire contract,
//! so it is pinned by a test rather than assumed.

use std::fmt;

use alloy_primitives::{Address, B256, TxHash};
use serde::{Deserialize, Serialize};

pub use crate::wire::datasets::evm::{Block, DecodedArg, Log, Receipt, Transaction};
pub use crate::wire::typed::TypedValue;

/// The version of the envelope's wire shape.
///
/// Stamped on every serialized [`Envelope`] as `v`, so a consumer can tell which
/// shape it is reading without out-of-band knowledge. Bumped only for a breaking
/// change; see the compatibility policy in the module docs.
///
/// It is 1 because no shape has been published yet, so a consumer's first number is
/// the first shape it can read.
pub const SCHEMA_VERSION: u16 = 1;

/// Identifies the chain an event came from, for example `ethereum` or `solana`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

/// A discontinuity: published blocks are no longer canonical.
///
/// A control signal rather than a dataset: it describes the indexer's view of the
/// chain changing, and has no verbatim payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reorg {
    /// Height of the new head at which the discontinuity was detected.
    #[serde(with = "alloy_serde::quantity")]
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
    #[serde(with = "alloy_serde::quantity")]
    pub height: u64,
    /// Hash of the newest finalized block.
    pub hash: B256,
}

/// One log decoded against a contract ABI, as typed arguments.
///
/// Produced by the decode layer, not ingest: nothing in the ingest path knows an ABI,
/// so this record is only ever added after the raw log it came from. It is a *dataset*
/// rather than a control signal — a durable on-chain record a consumer can store —
/// and it carries everything needed to identify the row it becomes without a second
/// lookup.
///
/// The raw log it came from is referenced by [`Log::dedupe_key`], not embedded:
/// the stored raw log is the archive, so re-decoding with a later ABI is a replay of
/// that record rather than a re-fetch from a node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Decoded {
    /// The event name from the ABI, for example `Transfer`.
    pub name: String,
    /// The contract that emitted the log.
    pub address: Address,
    /// What the contract is, from the registry, for example `uniswap_v3`.
    ///
    /// Not derivable from the ABI: an ABI is a list of signatures and says nothing
    /// about which protocol an address implements. It comes from the registry entry
    /// that matched, so a consumer can group a record by protocol without knowing any
    /// address.
    ///
    /// Deliberately *not* accompanied by a dataset name. Which table an event's rows
    /// belong to depends on context this stage does not have — which token a pool
    /// trades, how many decimals it has — and on modeling choices that change for
    /// reasons the decoder should not care about. The decoder supplies the protocol as
    /// a fact; the projection supplies the dataset.
    pub protocol: String,
    /// The event selector, `keccak256` of its signature.
    ///
    /// [`B256::ZERO`] for an anonymous event, which has no selector in `topic0`. A
    /// zero selector is not a valid `keccak256` of a signature, so it cannot collide
    /// with a real one.
    pub selector: B256,
    /// The event's human-readable signature, for example
    /// `Transfer(address,address,uint256)`.
    ///
    /// Carried because a selector alone is not a readable name, and a consumer
    /// debugging a decode should not have to compute one back from the selector.
    pub signature: String,
    /// Whether the ABI declares this event anonymous.
    ///
    /// An anonymous event's `topic0` is its first indexed argument rather than a
    /// selector, so a consumer that wants to reconstruct the log's topics needs to
    /// know not to prefix them with [`selector`](Self::selector).
    pub anonymous: bool,
    /// The transaction that emitted the log.
    pub transaction_hash: TxHash,
    /// The emitting transaction's position in its block.
    #[serde(with = "alloy_serde::quantity")]
    pub transaction_index: u64,
    /// The log's position in its block.
    #[serde(with = "alloy_serde::quantity")]
    pub log_index: u64,
    /// The indexed arguments, in ABI order, each carrying its name.
    pub indexed: Vec<DecodedArg>,
    /// The non-indexed arguments, in ABI order, each carrying its name.
    pub body: Vec<DecodedArg>,
    /// Height of the block containing this log.
    #[serde(with = "alloy_serde::quantity")]
    pub block_number: u64,
    /// Hash of the block containing this log.
    pub block_hash: B256,
    /// Timestamp of the block containing this log, denormalized from its header.
    #[serde(with = "alloy_serde::quantity")]
    pub block_timestamp: u64,
}

impl Decoded {
    /// A key that is stable across redelivery and unique per decoded record.
    ///
    /// Built from the raw log's natural key plus this event's selector, so a re-decode
    /// of the same log produces the same key and a store upserts rather than
    /// duplicates. It excludes `sequence` for the same reason every other dataset
    /// does: a sequence is reassigned by a reorg or a restart.
    #[must_use]
    pub fn dedupe_key(&self) -> String {
        format!("{}:{}:decoded", self.source_key(), self.selector)
    }

    /// The raw log's natural key, in the same shape
    /// [`Log::dedupe_key`] produces: `block:transaction:log_index`.
    ///
    /// This is the link back to the record the decode read, so a decoded row traces to
    /// the exact raw log it came from.
    #[must_use]
    pub fn source_key(&self) -> String {
        format!(
            "{}:{}:{}",
            self.block_number, self.transaction_hash, self.log_index
        )
    }
}

/// Everything the indexer publishes, tagged by `type` on the wire.
///
/// The dataset payloads are boxed: a [`Block`] or [`Receipt`] carries a 256-byte bloom
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
    /// A log decoded against a contract ABI. Only the decode stage produces this.
    Decoded(Box<Decoded>),
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
            Self::Decoded(_) => "decoded",
            Self::Reorg(_) => "reorg",
            Self::Finalized(_) => "finalized",
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
            Self::Decoded(decoded) => decoded.dedupe_key(),
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
    /// The wire shape's version, written by [`Envelope::new`].
    ///
    /// Defaulted on deserialize, so a line carrying no `v` reads as the current
    /// shape rather than failing.
    #[serde(default = "schema_version", rename = "v")]
    pub schema_version: u16,
    /// The event itself.
    #[serde(flatten)]
    pub event: Event,
}

/// The [`SCHEMA_VERSION`] as a serde default, for a line carrying no `v` field.
const fn schema_version() -> u16 {
    SCHEMA_VERSION
}

impl Envelope {
    /// Builds an envelope from the chain it came from and the sequence the pipeline
    /// assigned.
    #[must_use]
    pub const fn new(chain: ChainId, sequence: u64, event: Event) -> Self {
        Self {
            chain,
            sequence,
            schema_version: SCHEMA_VERSION,
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
    use alloy_primitives::{Address, B256, TxHash, U256};

    use super::{
        Block, ChainId, Decoded, DecodedArg, Envelope, Event, Finalized, Log, Receipt, Reorg,
        SCHEMA_VERSION, Transaction, TypedValue,
    };

    fn chain() -> ChainId {
        ChainId::new("ethereum")
    }

    fn hash(byte: u8) -> B256 {
        B256::from([byte; 32])
    }

    /// A decoded record's `source_key` must stay byte-for-byte the raw log's
    /// `dedupe_key`. The two are built by separate `format!` calls — one here, one on
    /// [`Log`] — so nothing but this test stops them drifting; if they do, every
    /// decoded record silently stops joining back to the log it came from.
    #[test]
    fn a_decoded_records_source_key_is_the_raw_logs_dedupe_key() {
        let log = Log {
            log_index: 767,
            transaction_hash: TxHash::from([0x2a; 32]),
            block_number: 51_913_794,
            ..Log::default()
        };
        let decoded = Decoded {
            name: "Swap".to_owned(),
            address: Address::from([0xd0; 20]),
            protocol: "uniswap_v3".to_owned(),
            selector: hash(0x07),
            signature: "Swap(address,address,int256,int256,uint160,uint128,int24)".to_owned(),
            anonymous: false,
            transaction_hash: log.transaction_hash,
            transaction_index: 12,
            log_index: log.log_index,
            indexed: Vec::new(),
            body: Vec::new(),
            block_number: log.block_number,
            block_hash: hash(0xd4),
            block_timestamp: 1_700_000_000,
        };
        assert_eq!(decoded.source_key(), log.dedupe_key());
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
        assert_eq!(value["number"], "0x5");
        assert_eq!(value["hash"], format!("0x{}", "09".repeat(32)));
        assert_eq!(value["chain"], "ethereum");
        assert_eq!(value["v"], SCHEMA_VERSION);
    }

    /// The wire object carries exactly the envelope's keys (`chain`, `sequence`)
    /// plus the event's (`type` and its fields) — nothing else rides along.
    /// Pinned on `Finalized`, whose two fields make the count exact.
    #[test]
    fn the_wire_object_carries_only_the_envelope_and_event_fields() {
        let envelope = Envelope::new(
            chain(),
            7,
            Event::Finalized(Finalized {
                height: 42,
                hash: hash(0x11),
            }),
        );
        let value = serde_json::to_value(&envelope).expect("envelope serializes");
        assert_eq!(value["type"], "finalized");
        assert_eq!(value["height"], "0x2a");
        assert_eq!(value["hash"], format!("0x{}", "11".repeat(32)));
        assert_eq!(value["sequence"], 7);
        assert_eq!(value["chain"], "ethereum");
        assert_eq!(value["v"], SCHEMA_VERSION);
        assert_eq!(value.as_object().expect("object").len(), 6);
    }

    /// The version is stamped on every rendered line, and a line carrying no `v`
    /// reads back as the current shape rather than failing.
    #[test]
    fn the_schema_version_is_stamped_and_absent_v_parses() {
        let envelope = Envelope::new(
            chain(),
            7,
            Event::Finalized(Finalized {
                height: 42,
                hash: hash(0x11),
            }),
        );
        let mut value = serde_json::to_value(&envelope).expect("envelope serializes");
        assert_eq!(value["v"], SCHEMA_VERSION);

        // A line from a producer that predates the `v` field: no `v`, everything else
        // as it is.
        value.as_object_mut().expect("object").remove("v");
        let decoded: Envelope = serde_json::from_value(value).expect("line without `v` parses");
        assert_eq!(decoded.schema_version, SCHEMA_VERSION);
        assert_eq!(decoded, envelope);
    }

    /// Integer fields render as the node's own "quantity" form, not as JSON
    /// numbers. A live `eth_getBlockByNumber` returns `number`, `gas`, `nonce`,
    /// `gasPrice`, `maxFeePerGas`, `chainId`, `gasUsed`, `logIndex` — every
    /// integer — as `0x` hex, so matching that is what keeps a consumer from
    /// having to special-case our encoding against the node's.
    #[test]
    fn integers_render_as_quantities_not_json_numbers() {
        let envelope = Envelope::new(
            chain(),
            120,
            Event::Transaction(Box::new(Transaction {
                hash: TxHash::from([0x11; 32]),
                nonce: 130_000,
                gas: 454_000,
                gas_price: Some(7),
                max_fee_per_gas: 8,
                chain_id: Some(1),
                block_number: 26_000_000,
                ..Transaction::default()
            })),
        );
        let value = serde_json::to_value(&envelope).expect("envelope serializes");
        assert_eq!(value["nonce"], "0x1fbd0");
        assert_eq!(value["gas"], "0x6ed70");
        assert_eq!(value["gas_price"], "0x7");
        assert_eq!(value["max_fee_per_gas"], "0x8");
        assert_eq!(value["chain_id"], "0x1");
        assert_eq!(value["block_number"], "0x18cba80");
        // The envelope's own sequence is a plain JSON number, not a chain quantity.
        assert_eq!(value["sequence"], 120);
    }

    /// The round trip an envelope takes through a sink and back. `Transaction` and
    /// `Receipt` carry `u128` fee fields, which serde's `flatten` buffering cannot
    /// deserialize unless those fields use the quantity encoding — this test is what
    /// pins that, and it is the only test here that is not tied to one event shape.
    #[test]
    fn every_variant_round_trips_through_json() {
        let variants = [
            Event::Block(Box::new(Block {
                number: 5,
                hash: hash(9),
                parent_hash: hash(8),
                timestamp: 1_700_000_000,
                ..Block::default()
            })),
            Event::Transaction(Box::new(Transaction {
                hash: TxHash::from([0x11; 32]),
                from: Address::from([0x22; 20]),
                gas_price: Some(u128::MAX),
                max_fee_per_gas: u128::MAX,
                max_priority_fee_per_gas: Some(u128::MAX),
                max_fee_per_blob_gas: Some(u128::MAX),
                block_number: 5,
                ..Transaction::default()
            })),
            Event::Receipt(Box::new(Receipt {
                transaction_hash: TxHash::from([0x11; 32]),
                effective_gas_price: u128::MAX,
                blob_gas_price: Some(u128::MAX),
                block_number: 5,
                ..Receipt::default()
            })),
            Event::Log(Box::new(Log {
                log_index: 1,
                transaction_hash: TxHash::from([0x11; 32]),
                block_number: 5,
                topic0: Some(hash(0x07)),
                ..Log::default()
            })),
            Event::Decoded(Box::new(Decoded {
                name: "Transfer".to_owned(),
                address: Address::from([0x22; 20]),
                protocol: "erc20".to_owned(),
                selector: hash(0x07),
                signature: "Transfer(address,address,uint256)".to_owned(),
                anonymous: false,
                transaction_hash: TxHash::from([0x11; 32]),
                transaction_index: 0,
                log_index: 1,
                indexed: vec![DecodedArg {
                    name: "from".to_owned(),
                    value: TypedValue::Address {
                        value: Address::from([0x22; 20]),
                    },
                }],
                body: vec![DecodedArg {
                    name: "value".to_owned(),
                    value: TypedValue::Uint {
                        value: U256::from(1),
                        bits: 256,
                    },
                }],
                block_number: 5,
                block_hash: hash(5),
                block_timestamp: 1_700_000_000,
            })),
            Event::Reorg(Reorg {
                height: 1,
                new_head_hash: hash(3),
                orphaned_hashes: vec![hash(2)],
            }),
            Event::Finalized(Finalized {
                height: 1,
                hash: hash(3),
            }),
        ];
        for event in variants {
            let kind = event.kind();
            let envelope = Envelope::new(chain(), 0, event);
            let encoded = serde_json::to_string(&envelope).expect("envelope serializes");
            let decoded: Envelope = serde_json::from_str(&encoded)
                .unwrap_or_else(|error| panic!("{kind} does not round-trip: {error}\n{encoded}"));
            assert_eq!(decoded, envelope, "{kind} changed across the round trip");
        }
    }
}
