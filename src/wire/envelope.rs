//! The chain-agnostic event payload, which is the wire contract consumers depend on.
//!
//! [`Event`] is the union of everything the indexer publishes, and it holds three
//! kinds of thing:
//!
//! - **Datasets** ([`Event::Block`], [`Event::Transaction`], [`Event::Log`]): durable on-chain records, re-exported from [`crate::wire::datasets`].
//!   Each carries its own identity fields and dedupe key. Their shape is per-chain,
//!   so the EVM records live in [`crate::wire::datasets::evm`]; Solana's would be a sibling
//!   module and a variant here.
//! - **Produced from a log** ([`Event::Decoded`], [`Event::Contract`],
//!   [`Event::Derived`]): records the decode stage produces from a log — its typed
//!   arguments under their ABI names, or the contract a factory's creation event names —
//!   and rows its derivations compute from one decoded record. They are datasets, not
//!   control signals, and each always follows the record it came from.
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
//! [`SCHEMA_VERSION`] is bumped for a **breaking** change: a field's type or meaning
//! changing, a field or variant being removed, a required field being added — or a new
//! [`Event`] variant. A reader built on this crate fails on a `type` it does not know
//! (`unknown variant`), so a new variant breaks it like any other change it cannot
//! read. A new optional field is **additive** and does not bump it: a reader ignores
//! unknown fields, and a missing one reads as absent.
//!
//! A breaking change needs a coexistence window in the store, because a reader
//! reading across the change sees both shapes interleaved. That is the point of
//! the number: it lets a consumer reading a stream tell which shape it has.
//!
//! The line every sink writes is pinned by
//! `every_variant_round_trips_through_json_with_unique_keys`: every variant, with every
//! optional field set, reads back unchanged, and no key appears twice on the line.
//!
//! # Identity types
//!
//! Dataset identity uses [`B256`] and [`TxHash`] from
//! `alloy-primitives`. They are
//! not hand-rolled here because alloy already models exactly this: a fixed 32-byte
//! identity whose JSON form is lowercase `0x` hex. The encoding is a wire contract,
//! so it is pinned by tests rather than assumed: the round trip above, and the
//! lowercase hex every key carries by `every_dedupe_key_is_the_documented_shape`.

use std::fmt;

use alloy_primitives::{Address, B256, TxHash};
use serde::{Deserialize, Serialize};

pub use crate::wire::datasets::evm::{Block, DecodedArg, Log, Transaction, log_key};
pub use crate::wire::typed::{AbiComponent, AbiType, TypedValue};

/// The version of the envelope's wire shape.
///
/// Stamped on every serialized [`Envelope`] as `schema_version`, so a consumer can tell
/// which shape it is reading without out-of-band knowledge. Bumped only for a breaking
/// change; see the compatibility policy in the module docs.
///
/// It is 1, the first shape stdout and the lake published, so a consumer's first
/// number is the first shape it can read.
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

/// One row a derivation computed from a single decoded log.
///
/// Produced by the decode stage's derivations, after the decoded record it came from. A derivation
/// names its table, and the record carries that table's values beside the log's own
/// identity, so a store renders it like an event table's row and a `reorg` deletes it by
/// its block like any other.
///
/// Natural key is `(block_hash, transaction_hash, log_index, table, row)`: a derivation may
/// compute several rows from one log, numbered in the order it computes them, and the
/// block hash keeps an orphaned log's rows apart from its replacement's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Derived {
    /// The derived table, for example `metric_v1_swaps`.
    pub table: String,
    /// The contract that emitted the log.
    pub address: Address,
    /// The transaction that emitted the log.
    pub transaction_hash: TxHash,
    /// The emitting transaction's position in its block.
    #[serde(with = "alloy_serde::quantity")]
    pub transaction_index: u64,
    /// The log's position in its block.
    #[serde(with = "alloy_serde::quantity")]
    pub log_index: u64,
    /// This row's position among the rows its derivation computed from the log, from 0.
    #[serde(with = "alloy_serde::quantity")]
    pub row: u64,
    /// The account that sent the transaction, denormalized from it as Allium's
    /// `dex.trades` carries it. `None` only when the transaction was not fetched.
    pub transaction_from_address: Option<Address>,
    /// The transaction's recipient. `None` when it deployed a contract, or was not
    /// fetched.
    pub transaction_to_address: Option<Address>,
    /// The table's own values, in its column order.
    pub fields: Vec<DerivedField>,
    /// Height of the block containing the log.
    #[serde(with = "alloy_serde::quantity")]
    pub block_number: u64,
    /// Hash of the block containing the log.
    pub block_hash: B256,
    /// Timestamp of the block containing the log, denormalized from its header.
    #[serde(with = "alloy_serde::quantity")]
    pub block_timestamp: u64,
}

impl Derived {
    /// A key that is stable across redelivery and unique per derived row:
    /// `block_hash:transaction_hash:log_index:table:row`.
    #[must_use]
    pub fn dedupe_key(&self) -> String {
        format!(
            "{}:{}:{}",
            log_key(self.block_hash, self.transaction_hash, self.log_index),
            self.table,
            self.row
        )
    }
}

/// One value of a [`Derived`] row, under its column's name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedField {
    /// The column, for example `amount0_raw`.
    pub name: String,
    /// The value, typed as the decoded arguments are.
    pub value: TypedValue,
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
    /// A block, from `eth_getBlockByNumber` or `HyperSync`'s blocks table.
    Block(Box<Block>),
    /// A transaction, from its block's `transactions` array, joined to its receipt.
    Transaction(Box<Transaction>),
    /// A log, from its receipt's `logs` array.
    Log(Box<Log>),
    /// A log decoded against a contract ABI. Only the decode stage produces this.
    Decoded(Box<Decoded>),
    /// A contract discovered from its creation event. Only the decode stage produces this.
    Contract(Box<Contract>),
    /// A row computed from one decoded log. Only the decode stage produces this.
    Derived(Box<Derived>),
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
            Self::Derived(_) => "derived",
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
    /// excludes the chain.
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
            Self::Derived(derived) => derived.dedupe_key(),
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
    /// Defaulted on deserialize to the first shape, so a line carrying no
    /// `schema_version` reads as the shape that predates the field rather than failing or
    /// claiming to be current.
    ///
    /// Not shortened to `v`: the event's fields are flattened beside it, and a
    /// transaction's signature `v` would then be a second key of the same name on one
    /// object.
    #[serde(default = "schema_version")]
    pub schema_version: u16,
    /// The event itself.
    #[serde(flatten)]
    pub event: Event,
}

/// The version an envelope with no `schema_version` field is read as.
///
/// A line with no `schema_version` predates the field, so it is the first shape: `1`.
/// Defaulting to [`SCHEMA_VERSION`] instead would read an unversioned line as the current
/// shape, which is the case the field exists to distinguish — a reader must not mistake
/// an old line for a new one after the number is bumped.
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
// The crate denies `expect`; a test may use it, since a failed expectation there means
// the fixture is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{Address, B64, B256, Bloom, Bytes, Function, I256, TxHash, U256};
    use alloy_rpc_types_eth::{
        AccessList, AccessListItem, Authorization, SignedAuthorization, Withdrawal,
    };
    use serde::de::{IgnoredAny, MapAccess};
    use serde::{Deserialize, Deserializer};

    use super::{
        AbiComponent, AbiType, Block, BlockMeta, ChainId, Contract, Decoded, DecodedArg, Derived,
        DerivedField, Envelope, Event, Log, Reorg, Transaction, TypedValue,
    };

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

    /// Every variant, with every optional field set, as the line a sink writes.
    ///
    /// The dataset literals name every field rather than spreading a default, so a new
    /// field fails to compile here until it is set, and so is covered by the test below.
    #[expect(
        clippy::too_many_lines,
        reason = "one literal per variant is the point"
    )]
    fn every_variant_fully_populated() -> Vec<Event> {
        let (block_hash, tx) = (hash(0xaa), TxHash::from([0xbb; 32]));
        let address = Address::from([0xd0; 20]);
        let typed = |value: TypedValue| DecodedArg {
            position: 0,
            name: "value".to_owned(),
            abi_type: AbiType {
                kind: "tuple".to_owned(),
                components: vec![AbiComponent {
                    name: "inner".to_owned(),
                    abi_type: AbiType {
                        kind: "uint256".to_owned(),
                        components: Vec::new(),
                    },
                }],
            },
            value,
        };
        let block = Block {
            number: 100,
            hash: block_hash,
            parent_hash: hash(0x01),
            timestamp: 1_700_000_000,
            nonce: B64::from([0x02; 8]),
            ommers_hash: hash(0x03),
            transactions_root: hash(0x04),
            state_root: hash(0x05),
            receipts_root: hash(0x06),
            withdrawals_root: Some(hash(0x07)),
            logs_bloom: Bloom::from([0x08; 256]),
            miner: address,
            difficulty: U256::from(9),
            total_difficulty: Some(U256::from(10)),
            size: Some(U256::from(11)),
            extra_data: Bytes::from_static(b"extra"),
            gas_limit: 12,
            gas_used: 13,
            transaction_count: 1,
            base_fee_per_gas: Some(14),
            blob_gas_used: Some(15),
            excess_blob_gas: Some(16),
            parent_beacon_block_root: Some(hash(0x11)),
            mix_hash: Some(hash(0x12)),
            withdrawals: Some(vec![Withdrawal {
                index: 1,
                validator_index: 2,
                address,
                amount: 3,
            }]),
            requests_hash: Some(hash(0x13)),
            l1_block_number: Some(17),
            send_count: Some(18),
            send_root: Some(hash(0x14)),
            ommers: vec![hash(0x15)],
            transaction_hashes: vec![tx],
        };
        let transaction = Transaction {
            hash: tx,
            nonce: 1,
            transaction_index: 2,
            from: address,
            to: Some(Address::from([0xd1; 20])),
            value: U256::from(3),
            gas: 4,
            gas_price: Some(5),
            max_fee_per_gas: Some(6),
            max_priority_fee_per_gas: Some(7),
            max_fee_per_blob_gas: Some(8),
            input: Bytes::from_static(&[0x01, 0x02]),
            transaction_type: 4,
            chain_id: Some(1),
            access_list: Some(AccessList(vec![AccessListItem {
                address,
                storage_keys: vec![hash(0x16)],
            }])),
            blob_versioned_hashes: Some(vec![hash(0x17)]),
            authorization_list: Some(vec![SignedAuthorization::new_unchecked(
                Authorization {
                    chain_id: U256::from(1),
                    address,
                    nonce: 9,
                },
                1,
                U256::from(10),
                U256::from(11),
            )]),
            r: Some(U256::from(12)),
            s: Some(U256::from(13)),
            // An EIP-155 legacy `v`: the field whose name the envelope must not share.
            v: Some(0x25),
            y_parity: Some(1),
            receipt_status: true,
            receipt_gas_used: 14,
            receipt_cumulative_gas_used: 15,
            receipt_effective_gas_price: 16,
            receipt_contract_address: Some(Address::from([0xd2; 20])),
            receipt_logs_bloom: Bloom::from([0x09; 256]),
            receipt_blob_gas_used: Some(17),
            receipt_blob_gas_price: Some(18),
            receipt_gas_used_for_l1: Some(19),
            receipt_l1_block_number: Some(20),
            log_count: 1,
            block_timestamp: 1_700_000_000,
            block_number: 100,
            block_hash,
        };
        let log = Log {
            log_index: 3,
            transaction_hash: tx,
            transaction_index: 2,
            address,
            topic0: Some(hash(0x18)),
            topic1: Some(hash(0x19)),
            topic2: Some(hash(0x1a)),
            topic3: Some(hash(0x1b)),
            data: Bytes::from_static(&[0xde, 0xad]),
            removed: false,
            block_number: 100,
            block_hash,
            block_timestamp: 1_700_000_000,
        };
        let decoded = Decoded {
            event_id: hash(0x08),
            name: "Swap".to_owned(),
            address,
            protocol: "uniswap_v3".to_owned(),
            contract: "UniswapV3Pool".to_owned(),
            selector: hash(0x18),
            signature: "Swap(address)".to_owned(),
            anonymous: false,
            transaction_hash: tx,
            transaction_index: 2,
            log_index: 3,
            indexed: vec![typed(TypedValue::IndexedHash { value: hash(0x1c) })],
            body: vec![typed(TypedValue::Tuple {
                value: vec![
                    TypedValue::Bool { value: true },
                    TypedValue::Int {
                        value: I256::MINUS_ONE,
                        bits: 24,
                    },
                    TypedValue::Uint {
                        value: U256::MAX,
                        bits: 256,
                    },
                    TypedValue::FixedBytes {
                        value: Bytes::from_static(&[0xab; 4]),
                        size: 4,
                    },
                    TypedValue::Address { value: address },
                    TypedValue::Bytes {
                        value: Bytes::from_static(&[0xcd]),
                    },
                    TypedValue::String {
                        value: Bytes::from_static(b"text"),
                        text: Some("text".to_owned()),
                    },
                    TypedValue::Function {
                        value: Function::from([0xef; 24]),
                    },
                    TypedValue::Array {
                        value: vec![TypedValue::Bool { value: false }],
                    },
                    TypedValue::FixedArray {
                        value: vec![TypedValue::Bool { value: true }],
                        size: 1,
                    },
                ],
            })],
            block_number: 100,
            block_hash,
            block_timestamp: 1_700_000_000,
        };
        let derived = Derived {
            table: "metric_v1_swaps".to_owned(),
            address,
            transaction_hash: tx,
            transaction_index: 2,
            log_index: 3,
            row: 0,
            transaction_from_address: Some(Address::from([0xd3; 20])),
            transaction_to_address: Some(Address::from([0xd4; 20])),
            fields: vec![DerivedField {
                name: "amount0_raw".to_owned(),
                value: TypedValue::Int {
                    value: I256::MINUS_ONE,
                    bits: 256,
                },
            }],
            block_number: 100,
            block_hash,
            block_timestamp: 1_700_000_000,
        };
        vec![
            Event::Block(Box::new(block)),
            Event::Transaction(Box::new(transaction)),
            Event::Log(Box::new(log)),
            Event::Decoded(Box::new(decoded)),
            Event::Contract(Box::new(contract(block_hash))),
            Event::Derived(Box::new(derived)),
            Event::Reorg(Reorg {
                height: 100,
                new_head_hash: block_hash,
                orphaned_hashes: vec![hash(0xdd)],
            }),
            Event::AcceptedBlock(BlockMeta {
                height: 100,
                hash: block_hash,
                parent_hash: hash(0x01),
                timestamp: 1_700_000_000,
            }),
        ]
    }

    /// An object's top-level keys in the order they appear, repeats kept.
    ///
    /// A `serde_json::Value` cannot be used to find a repeat: its map keeps the last
    /// value for a key and drops the earlier one without error.
    struct Keys(Vec<String>);

    impl<'de> Deserialize<'de> for Keys {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            struct Visitor;
            impl<'de> serde::de::Visitor<'de> for Visitor {
                type Value = Keys;
                fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("a JSON object")
                }
                fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Keys, A::Error> {
                    let mut keys = Vec::new();
                    while let Some(key) = map.next_key::<String>()? {
                        map.next_value::<IgnoredAny>()?;
                        keys.push(key);
                    }
                    Ok(Keys(keys))
                }
            }
            deserializer.deserialize_map(Visitor)
        }
    }

    /// The line every sink writes is the wire contract: every variant, with every
    /// optional field set, reads back as the envelope that wrote it, and no key appears
    /// twice on the line. A repeated key is the failure a flattened event invites — a
    /// strict reader rejects the line, and a last-wins one such as `jq` silently reads
    /// the wrong value.
    #[test]
    fn every_variant_round_trips_through_json_with_unique_keys() {
        for event in every_variant_fully_populated() {
            let envelope = Envelope::new(ChainId::new("ethereum"), event);
            let line = serde_json::to_string(&envelope).expect("serializes");

            let Keys(mut keys) = serde_json::from_str(&line).expect("an object");
            let count = keys.len();
            keys.sort_unstable();
            keys.dedup();
            assert_eq!(keys.len(), count, "a key repeats on the line {line}");
            for envelope_key in ["chain", "schema_version", "type"] {
                assert!(keys.iter().any(|key| key == envelope_key), "{line}");
            }

            let read: Envelope = serde_json::from_str(&line)
                .unwrap_or_else(|error| panic!("{line} reads back: {error}"));
            assert_eq!(read, envelope);
        }
    }

    /// An absent value is written as `null` whatever its encoding, a quantity or not, so
    /// a consumer meets one kind of absence; and the line reads back as `None`.
    #[test]
    fn an_absent_value_is_null_and_not_omitted() {
        let envelope = Envelope::new(ChainId::new("ethereum"), Event::Transaction(Box::default()));
        let line = serde_json::to_string(&envelope).expect("serializes");
        let object: serde_json::Value = serde_json::from_str(&line).expect("an object");
        for key in [
            "v",
            "gas_price",
            "receipt_l1_block_number",
            "r",
            "to",
            "access_list",
        ] {
            assert_eq!(object.get(key), Some(&serde_json::Value::Null), "{key}");
        }
        let read: Envelope = serde_json::from_str(&line).expect("reads back");
        assert_eq!(read, envelope);
    }

    /// A derived row's key is its log's, its table, and its position among the log's
    /// rows, pinned as a literal like every other key.
    #[test]
    fn a_derived_rows_dedupe_key_is_the_documented_shape() {
        let block_hash = hash(0xaa);
        let tx = TxHash::from([0xbb; 32]);
        let (block_hex, tx_hex) = ("aa".repeat(32), "bb".repeat(32));
        assert_eq!(
            Event::Derived(Box::new(Derived {
                table: "metric_v1_swaps".to_owned(),
                address: Address::from([0xd0; 20]),
                transaction_hash: tx,
                transaction_index: 0,
                log_index: 3,
                row: 2,
                transaction_from_address: None,
                transaction_to_address: None,
                fields: Vec::new(),
                block_number: 100,
                block_hash,
                block_timestamp: 1_700_000_000,
            }))
            .dedupe_key(),
            format!("0x{block_hex}:0x{tx_hex}:3:metric_v1_swaps:2")
        );
    }
}
