//! The chain-agnostic event payload, which is the wire contract consumers depend on.
//!
//! [`Event`] is the union of everything the indexer publishes, and it holds two
//! kinds of thing:
//!
//! - **Datasets** ([`Event::Block`], [`Event::Transaction`], [`Event::Log`]): durable on-chain records, re-exported from [`crate::wire::datasets`].
//!   Each carries its own identity fields and dedupe key. Their shape is per-chain,
//!   so the EVM records live in [`crate::wire::datasets::evm`]; Solana's would be a sibling
//!   module and a variant here.
//! - **Derived** ([`Event::Decoded`], [`Event::Contract`]): records the decode stage
//!   produces from a log — its typed arguments under their ABI names, or the contract a
//!   factory's creation event names. They are datasets, not control signals, and each
//!   always follows the log it came from.
//! - **Control** ([`Reorg`], [`Event::AcceptedBlock`]): signals about the indexer's own state,
//!   not records of a chain. They carry no verbatim payload and exist to drive a
//!   consumer's state machine, so they are defined here.
//!
//! The decode stage can publish typed interpretations alongside raw logs. Consumers
//! may also replay the raw datasets against an ABI or IDL they trust. Every dataset
//! field is present and typed, so a consumer that only persists need not decode.
//!
//! An [`Envelope`] carries the [`Event`], the [`ChainId`] it came from, and the
//! [`SCHEMA_VERSION`] the
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

pub use crate::wire::datasets::evm::{Block, DecodedArg, Log, Transaction, log_key};
pub use crate::wire::typed::{AbiComponent, AbiType, TypedValue};

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
    /// Height of the lowest orphaned block, the first height the new branch replaces;
    /// no orphan is below it. Stores bound their deletes by it, so a smaller value only
    /// costs a wider delete, and a larger one would leave orphans behind.
    #[serde(with = "alloy_serde::quantity")]
    pub height: u64,
    /// Hash of the new head.
    pub new_head_hash: B256,
    /// Hashes of the blocks that are no longer canonical, newest first.
    pub orphaned_hashes: Vec<B256>,
}

/// A block's identity, parent hash, and timestamp: what a `newHeads` notification
/// carries, what a fetched block is anchored to, and, as [`Event::AcceptedBlock`], the
/// marker that ends every block's batch.
///
/// As that marker it is a control signal, published for every block whatever datasets
/// are selected, so a store keeps a contiguous, parent-linked record of what it committed,
/// in the same transaction as the block's rows. A restart resumes from it; see
/// `ingest::pipeline`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockMeta {
    /// Block height, or slot on slot-based chains.
    #[serde(with = "alloy_serde::quantity")]
    pub height: u64,
    /// Block hash.
    pub hash: B256,
    /// The block's parent hash.
    pub parent_hash: B256,
    /// The block's timestamp.
    #[serde(with = "alloy_serde::quantity")]
    pub timestamp: u64,
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
    /// The identity of the event definition this log was decoded with: a hash of the
    /// event's name, parameter names, types, and indexed flags.
    ///
    /// Scoped to the one event, not the ABI file it came from, so editing an unrelated
    /// event in the same file leaves every existing row's key alone. A changed definition
    /// of *this* event produces a separate append-only decoded row even when the raw log
    /// and event selector are unchanged.
    pub event_id: B256,
    /// The event name from the ABI, for example `Transfer`.
    pub name: String,
    /// The contract that emitted the log.
    pub address: Address,
    /// What the contract is, from its protocol manifest, for example `uniswap_v3`.
    ///
    /// Not derivable from the ABI: an ABI is a list of signatures and says nothing
    /// about which protocol an address implements. It comes from the manifest that
    /// listed or discovered the address, so a consumer can group a record by protocol
    /// without knowing any address.
    ///
    /// Deliberately *not* accompanied by a dataset name. Which table an event's rows
    /// belong to depends on context this stage does not have — which token a pool
    /// trades, how many decimals it has — and on modeling choices that change for
    /// reasons the decoder should not care about. The decoder supplies the protocol as
    /// a fact; the projection supplies the dataset.
    pub protocol: String,
    /// Which of the protocol's contracts emitted it, from its manifest, for example
    /// `UniswapV3Pool`.
    ///
    /// With the protocol and the event, this names the one typed table the record is
    /// also stored in: two contracts of a protocol may emit the same event, and each
    /// gets its own table.
    pub contract: String,
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
    /// The indexed arguments, in ABI order, with original positions and declared types.
    pub indexed: Vec<DecodedArg>,
    /// The non-indexed arguments, in ABI order, with original positions and declared types.
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
    /// Built from the raw log's natural key, event selector, and event identity:
    /// `block_hash:transaction_hash:log_index:selector:event_id:decoded`.
    /// Redelivery of the same interpretation deduplicates, while a changed event
    /// definition yields a separate append-only interpretation. The block hash also keeps an orphaned
    /// log's interpretation separate from its replacement's.
    #[must_use]
    pub fn dedupe_key(&self) -> String {
        format!(
            "{}:{}:{}:decoded",
            self.source_key(),
            self.selector,
            self.event_id
        )
    }

    /// The raw log's natural key, in the same shape [`Log::dedupe_key`] produces:
    /// `block_hash:transaction_hash:log_index`.
    ///
    /// This is the link back to the record the decode read, so a decoded row traces to
    /// the exact raw log it came from.
    #[must_use]
    pub fn source_key(&self) -> String {
        log_key(self.block_hash, self.transaction_hash, self.log_index)
    }
}

/// A contract the decode stage discovered from its creation event.
///
/// Modeled on the provenance columns of Allium's `dex.pools`: the contract, the
/// factory that created it, and the creation log's position. A factory names each child
/// in an event — Uniswap V3's `PoolCreated.pool` — and a protocol manifest says which
/// argument; see [`crate::decode`]. Contracts listed by address in a manifest are seeds
/// and are not published here.
///
/// Natural key is `(block_hash, transaction_hash, log_index, address)`: the creation log
/// plus the child, so a creation in an orphaned block and its replay in the replacement
/// are different rows, and a `reorg` deletes the orphaned one like any other row.
/// The store holding these rows is how discovered contracts survive a restart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Contract {
    /// The protocol the contract belongs to, from its manifest, for example `uniswap_v3`.
    pub protocol: String,
    /// The contract's name within its protocol, from its ABI file, for example
    /// `UniswapV3Pool`.
    pub name: String,
    /// The discovered contract.
    pub address: Address,
    /// The contract that emitted the creation event.
    pub factory_address: Address,
    /// The transaction that emitted the creation event.
    pub transaction_hash: TxHash,
    /// The creating transaction's position in its block.
    #[serde(with = "alloy_serde::quantity")]
    pub transaction_index: u64,
    /// The creation log's position in its block.
    #[serde(with = "alloy_serde::quantity")]
    pub log_index: u64,
    /// Height of the block containing the creation log.
    #[serde(with = "alloy_serde::quantity")]
    pub block_number: u64,
    /// Hash of the block containing the creation log.
    pub block_hash: B256,
    /// Timestamp of the block containing the creation log, denormalized from its header.
    #[serde(with = "alloy_serde::quantity")]
    pub block_timestamp: u64,
}

/// A contract a previous run discovered, as a store reads it back: only what decode
/// needs to decode it again. Not published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredContract {
    /// The protocol its manifest declares.
    pub protocol: String,
    /// Its contract name within that protocol, for example `UniswapV3Pool`.
    pub name: String,
    /// The contract.
    pub address: Address,
    /// The hash of the block whose creation log discovered it, so a reorg that orphans
    /// that block after a restart still retracts it.
    pub block_hash: B256,
}

impl Contract {
    /// A key that is stable across redelivery and unique per discovery:
    /// `block_hash:transaction_hash:log_index:address:contract`.
    #[must_use]
    pub fn dedupe_key(&self) -> String {
        // `{:#x}`, not `Display`: an address displays checksummed, and every key is
        // lowercase hex like the node's own encoding.
        format!(
            "{}:{:#x}:contract",
            log_key(self.block_hash, self.transaction_hash, self.log_index),
            self.address
        )
    }
}

/// Everything the indexer publishes, tagged by `type` on the wire.
///
/// The dataset payloads are boxed: a [`Block`] or [`Transaction`] carries a 256-byte bloom
/// filter, and without boxing every [`Log`] event — the overwhelming majority on a
/// busy block — would be padded to that size in memory and on the stack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    /// A block, from `eth_getBlockByNumber`.
    Block(Box<Block>),
    /// A transaction, from its block's `transactions` array, joined to its receipt.
    Transaction(Box<Transaction>),
    /// A log, from its receipt's `logs` array.
    Log(Box<Log>),
    /// A log decoded against a contract ABI. Only the decode stage produces this.
    Decoded(Box<Decoded>),
    /// A contract discovered from its creation event. Only the decode stage produces this.
    Contract(Box<Contract>),
    /// A discontinuity in the published chain.
    Reorg(Reorg),
    /// A block the pipeline accepted, published last in its batch.
    AcceptedBlock(BlockMeta),
}

impl Event {
    /// Names the event kind, for logs and metrics.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Block(_) => "block",
            Self::Transaction(_) => "transaction",
            Self::Log(_) => "log",
            Self::Decoded(_) => "decoded",
            Self::Contract(_) => "contract",
            Self::Reorg(_) => "reorg",
            Self::AcceptedBlock(_) => "accepted_block",
        }
    }

    /// A key that is stable across redelivery and unique per event.
    ///
    /// Consumer groups deliver at least once, so consumers deduplicate on this. For
    /// raw datasets the key is the record's on-chain identity; decoded records also
    /// include the ABI interpretation identity. For control signals it is
    /// the identity of the change they announce. It is scoped to the stream, so it
    /// excludes the chain, and it deliberately excludes `sequence`, which changes if
    /// the indexer restarts and replays from a different point.
    ///
    /// A later copy of a key supersedes an earlier one: the stores upsert, keeping the
    /// last row a batch holds for a key. A consumer that deduplicates by dropping repeats
    /// must therefore keep the newest copy, not the first, or it can disagree with what
    /// the store holds.
    #[must_use]
    pub fn dedupe_key(&self) -> String {
        match self {
            Self::Block(block) => block.dedupe_key(),
            Self::Transaction(transaction) => transaction.dedupe_key(),
            Self::Log(log) => log.dedupe_key(),
            Self::Decoded(decoded) => decoded.dedupe_key(),
            Self::Contract(contract) => contract.dedupe_key(),
            Self::Reorg(reorg) => format!("{}:reorg", reorg.new_head_hash),
            Self::AcceptedBlock(accepted) => format!("{}:accepted_block", accepted.hash),
        }
    }
}

/// A single event plus the metadata the pipeline assigns.
///
/// No sequence number: a record's position in the stream is the dataset's own, and
/// those tuples survive a reorg where a global counter does not. See
/// [`Event::dedupe_key`] for the identity a consumer deduplicates on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    /// The chain this event came from.
    pub chain: ChainId,
    /// The wire shape's version, written by [`Envelope::new`].
    ///
    /// Defaulted on deserialize to the first shape, so a line carrying no `v` reads as
    /// the shape that predates the field rather than failing or claiming to be current.
    #[serde(default = "schema_version", rename = "v")]
    pub schema_version: u16,
    /// The event itself.
    #[serde(flatten)]
    pub event: Event,
}

/// The version an envelope with no `v` field is read as.
///
/// A line with no `v` predates the field, so it is the first shape: `1`. Defaulting to
/// [`SCHEMA_VERSION`] instead would read an unversioned line as the current shape, which
/// is the case the field exists to distinguish — a reader must not mistake an old line
/// for a new one after the number is bumped.
const fn schema_version() -> u16 {
    1
}

impl Envelope {
    /// Builds an envelope from the chain it came from.
    #[must_use]
    pub const fn new(chain: ChainId, event: Event) -> Self {
        Self {
            chain,
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
mod tests {
    use alloy_primitives::{Address, B256, TxHash};

    use super::{Block, BlockMeta, Contract, Decoded, Event, Log, Reorg, Transaction};

    fn contract(block_hash: B256) -> Contract {
        Contract {
            protocol: "uniswap_v3".to_owned(),
            name: "UniswapV3Pool".to_owned(),
            address: Address::from([0xd0; 20]),
            factory_address: Address::from([0xfa; 20]),
            transaction_hash: TxHash::from([0xbb; 32]),
            transaction_index: 1,
            log_index: 3,
            block_number: 100,
            block_hash,
            block_timestamp: 1_700_000_000,
        }
    }

    fn hash(byte: u8) -> B256 {
        B256::from([byte; 32])
    }

    /// A decoded record's `source_key` must stay byte-for-byte the raw log's
    /// `dedupe_key`. Both use the shared `log_key` helper; this test protects the
    /// join contract if either caller changes.
    #[test]
    fn a_decoded_records_source_key_is_the_raw_logs_dedupe_key() {
        let log = Log {
            log_index: 767,
            transaction_hash: TxHash::from([0x2a; 32]),
            block_number: 51_913_794,
            block_hash: hash(0xd4),
            ..Log::default()
        };
        let decoded = Decoded {
            name: "Swap".to_owned(),
            address: Address::from([0xd0; 20]),
            protocol: "uniswap_v3".to_owned(),
            contract: "UniswapV3Pool".to_owned(),
            event_id: hash(0x08),
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
        let mut later_interpretation = decoded.clone();
        later_interpretation.event_id = hash(0x09);
        assert_ne!(decoded.dedupe_key(), later_interpretation.dedupe_key());
        assert_eq!(decoded.source_key(), later_interpretation.source_key());
    }

    /// The key is what a store deduplicates on, so its exact shape is a published
    /// contract. These pin it as a literal, since a test that compares two functions
    /// against each other cannot catch both drifting the same way.
    #[test]
    fn every_dedupe_key_is_the_documented_shape() {
        let block_hex = "aa".repeat(32);
        let tx_hex = "bb".repeat(32);
        let selector_hex = "cc".repeat(32);
        let block_hash = hash(0xaa);
        let tx = TxHash::from([0xbb; 32]);
        let selector = hash(0xcc);

        assert_eq!(
            Block {
                number: 100,
                hash: block_hash,
                ..Block::default()
            }
            .dedupe_key(),
            format!("0x{block_hex}:block")
        );
        assert_eq!(
            Transaction {
                block_number: 100,
                block_hash,
                hash: tx,
                ..Transaction::default()
            }
            .dedupe_key(),
            format!("0x{block_hex}:0x{tx_hex}:tx")
        );
        assert_eq!(
            Log {
                block_number: 100,
                block_hash,
                transaction_hash: tx,
                log_index: 3,
                ..Log::default()
            }
            .dedupe_key(),
            format!("0x{block_hex}:0x{tx_hex}:3")
        );
        assert_eq!(
            Event::Decoded(Box::new(Decoded {
                name: "Swap".to_owned(),
                address: Address::from([0xd0; 20]),
                protocol: "uniswap_v3".to_owned(),
                contract: "UniswapV3Pool".to_owned(),
                event_id: hash(0x08),
                selector,
                signature: "Swap(address)".to_owned(),
                anonymous: false,
                transaction_hash: tx,
                transaction_index: 0,
                log_index: 3,
                indexed: Vec::new(),
                body: Vec::new(),
                block_number: 100,
                block_hash,
                block_timestamp: 1_700_000_000,
            }))
            .dedupe_key(),
            format!(
                "0x{block_hex}:0x{tx_hex}:3:0x{selector_hex}:0x{}:decoded",
                "08".repeat(32)
            )
        );
        assert_eq!(
            Event::Contract(Box::new(contract(block_hash))).dedupe_key(),
            format!("0x{block_hex}:0x{tx_hex}:3:0x{}:contract", "d0".repeat(20))
        );
        assert_eq!(
            Event::Reorg(Reorg {
                height: 100,
                new_head_hash: block_hash,
                orphaned_hashes: vec![hash(0xdd)],
            })
            .dedupe_key(),
            format!("0x{block_hex}:reorg")
        );
        assert_eq!(
            Event::AcceptedBlock(BlockMeta {
                height: 100,
                hash: block_hash,
                parent_hash: hash(0xdd),
                timestamp: 1_700_000_000,
            })
            .dedupe_key(),
            format!("0x{block_hex}:accepted_block")
        );
    }
}
