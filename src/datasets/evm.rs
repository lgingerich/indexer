//! EVM dataset records.
//!
//! Each struct is one normalized dataset table with a natural key, so a row maps
//! to a persistence row without further flattening:
//!
//! | Dataset | RPC source | Natural key |
//! | --- | --- | --- |
//! | [`Block`] | `eth_getBlockByNumber` | `(number, hash)` |
//! | [`Transaction`] | the block's `transactions` array | `hash` |
//! | [`Receipt`] | `eth_getBlockReceipts` / `eth_getTransactionReceipt` | `transaction_hash` |
//! | [`Log`] | a receipt's `logs` array | `(transaction_hash, log_index)` |
//!
//! Datasets reference each other by scalar key, never by embedding: a block holds
//! its transactions' hashes, not the transactions. That keeps a row a fixed shape
//! and lets a store load children by key.
//!
//! Every field is present and typed; nothing is kept as opaque JSON, so a consumer
//! can persist a row without decoding. Field types come from `alloy-primitives`
//! ([`Address`], [`B256`], [`U256`], `Bytes`) and `alloy-rpc-types-eth`
//! ([`AccessList`], [`TxType`]), which serialize to the canonical Ethereum JSON
//! forms — lowercase `0x` hex for bytes, `0x` quantity for numbers — pinned by a
//! test in `crate::envelope`.

use alloy_consensus::TxType;
use alloy_primitives::{Address, B64, B256, BlockHash, Bloom, Bytes, TxHash, U256};
use alloy_rpc_types_eth::{AccessList, SignedAuthorization};
use serde::{Deserialize, Serialize};

/// Block header and metadata, from `eth_getBlockByNumber`.
///
/// Natural key is `(number, hash)`: the number locates the block in the chain, the
/// hash pins which block that number held, so a reorg yields a new row at the same
/// number. `transaction_hashes` is the block's transactions in order, referenced by
/// key rather than embedded.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Block {
    /// Height of the chain, in blocks.
    pub number: u64,
    /// Unique identifier of this block.
    pub hash: BlockHash,
    /// Identifier of the parent block.
    pub parent_hash: BlockHash,
    /// Timestamp for when the block was collated.
    pub timestamp: u64,
    /// Hash of the generated proof-of-work; `B64::ZERO` post-merge.
    pub nonce: B64,
    /// Keccak hash of the ommers (uncle) list.
    pub ommers_hash: B256,
    /// Root of the transaction trie.
    pub transactions_root: B256,
    /// Root of the final state trie.
    pub state_root: B256,
    /// Root of the receipts trie.
    pub receipts_root: B256,
    /// Root of the withdrawals list; `None` before EIP-4895.
    pub withdrawals_root: Option<B256>,
    /// Bloom filter for the block's logs.
    pub logs_bloom: Bloom,
    /// Address of the block's beneficiary.
    pub miner: Address,
    /// Difficulty of this block.
    pub difficulty: U256,
    /// Total difficulty of the chain up to this block; deprecated.
    pub total_difficulty: Option<U256>,
    /// Size of this block in bytes.
    pub size: Option<U256>,
    /// Arbitrary data relevant to this block.
    pub extra_data: Bytes,
    /// Gas limit of this block.
    pub gas_limit: u64,
    /// Total gas used by this block's transactions.
    pub gas_used: u64,
    /// Number of transactions in this block.
    pub transaction_count: u64,
    /// Minimum gas price required for inclusion, in wei; `None` before EIP-1559.
    pub base_fee_per_gas: Option<u64>,
    /// Total blob gas used by this block's blob transactions; EIP-4844.
    pub blob_gas_used: Option<u64>,
    /// Excess blob gas carried from the parent; EIP-4844.
    pub excess_blob_gas: Option<u64>,
    /// Root of the parent beacon block; EIP-4788.
    pub parent_beacon_block_root: Option<B256>,
    /// Hashes of the ommers (uncles) this block includes.
    pub ommers: Vec<BlockHash>,
    /// Hashes of this block's transactions, in index order.
    pub transaction_hashes: Vec<TxHash>,
}

impl Block {
    /// A key that is stable across redelivery and unique per canonical block.
    #[must_use]
    pub fn dedupe_key(&self) -> String {
        format!("{}:{}:block", self.number, self.hash)
    }
}

/// One transaction, from its block's `transactions` array.
///
/// Natural key is `hash`. Receipt-only fields are denormalized onto this row where
/// a store wants one table per transaction instead of joining the [`Receipt`].
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Transaction {
    /// Unique identifier of this transaction.
    pub hash: TxHash,
    /// Nonce of the sending account.
    pub nonce: u64,
    /// Position of this transaction within its block.
    pub transaction_index: u64,
    /// Address of the sending party.
    pub from: Address,
    /// Address of the receiving party; `None` when this transaction deploys a contract.
    pub to: Option<Address>,
    /// Value transferred, in wei.
    pub value: U256,
    /// Gas allocated to this transaction.
    pub gas: u64,
    /// Gas price, in wei; `Some` for legacy and EIP-2930 transactions.
    pub gas_price: Option<u128>,
    /// Maximum fee per gas, in wei; EIP-1559.
    pub max_fee_per_gas: u128,
    /// Maximum priority fee per gas, in wei; EIP-1559.
    pub max_priority_fee_per_gas: Option<u128>,
    /// Maximum fee per blob gas, in wei; EIP-4844.
    pub max_fee_per_blob_gas: Option<u128>,
    /// Calldata sent with this transaction.
    pub input: Bytes,
    /// Transaction type: 0 legacy, 1 access list, 2 dynamic fee, 3 blob, 4 set-code.
    pub transaction_type: TxType,
    /// Chain this transaction is valid on, from EIP-155.
    pub chain_id: Option<u64>,
    /// Access list; EIP-2930.
    pub access_list: Option<AccessList>,
    /// Versioned blob hashes carried by this transaction; EIP-4844.
    pub blob_versioned_hashes: Option<Vec<B256>>,
    /// Authorization list; EIP-7702.
    pub authorization_list: Option<Vec<SignedAuthorization>>,
    /// Block timestamp, denormalized for time-based partitioning.
    pub block_timestamp: u64,
    /// Height of the block containing this transaction.
    pub block_number: u64,
    /// Hash of the block containing this transaction.
    pub block_hash: BlockHash,
}

impl Transaction {
    /// A key that is stable across redelivery and unique per transaction.
    #[must_use]
    pub fn dedupe_key(&self) -> String {
        format!("{}:{}:tx", self.block_number, self.hash)
    }
}

/// A transaction receipt, from `eth_getBlockReceipts` or `eth_getTransactionReceipt`.
///
/// Natural key is `transaction_hash`: exactly one receipt exists per transaction, so
/// a receipt is keyed the same way as its [`Transaction`] and separated by dataset
/// kind. `logs` are published as their own [`Log`] dataset, so a receipt holds only
/// the receipt-scalar fields.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Receipt {
    /// Hash of the transaction this receipt belongs to.
    pub transaction_hash: TxHash,
    /// Position of that transaction within its block.
    pub transaction_index: u64,
    /// Address of the sending party.
    pub from: Address,
    /// Address of the receiving party; `None` for a contract creation.
    pub to: Option<Address>,
    /// Success status of the transaction.
    pub status: bool,
    /// Transaction type, mirrored from the transaction.
    pub transaction_type: TxType,
    /// Gas used by this transaction alone.
    pub gas_used: u64,
    /// Total gas used in the block when this transaction was executed.
    pub cumulative_gas_used: u64,
    /// Effective gas price paid, in wei.
    pub effective_gas_price: u128,
    /// Address of the created contract, if this was a deployment.
    pub contract_address: Option<Address>,
    /// Bloom filter for this receipt's logs.
    pub logs_bloom: Bloom,
    /// Blob gas used; EIP-4844.
    pub blob_gas_used: Option<u64>,
    /// Blob gas price; EIP-4844.
    pub blob_gas_price: Option<u128>,
    /// Number of logs emitted by this transaction.
    pub log_count: u64,
    /// Height of the block containing this receipt's transaction.
    pub block_number: u64,
    /// Hash of the block containing this receipt's transaction.
    pub block_hash: BlockHash,
}

impl Receipt {
    /// A key that is stable across redelivery and unique per transaction's receipt.
    #[must_use]
    pub fn dedupe_key(&self) -> String {
        format!("{}:{}:receipt", self.block_number, self.transaction_hash)
    }
}

/// One log, from a receipt's `logs` array.
///
/// Natural key is `(transaction_hash, log_index)`. Topics are flattened into
/// `topic0`..`topic3` so a row is fixed-shape; `topic0` is the event signature.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Log {
    /// Position of this log within its block.
    pub log_index: u64,
    /// Hash of the transaction that emitted this log.
    pub transaction_hash: TxHash,
    /// Position of the emitting transaction within its block.
    pub transaction_index: u64,
    /// Address of the contract that emitted this log.
    pub address: Address,
    /// The event signature hash, `topics[0]`.
    pub topic0: Option<B256>,
    /// First indexed topic.
    pub topic1: Option<B256>,
    /// Second indexed topic.
    pub topic2: Option<B256>,
    /// Third indexed topic.
    pub topic3: Option<B256>,
    /// Unindexed data, encoded per the event ABI.
    pub data: Bytes,
    /// Whether this log was removed by a reorg; the indexer reports reorgs
    /// separately, so this is carried as the source returned it.
    pub removed: bool,
    /// Height of the block containing this log.
    pub block_number: u64,
    /// Hash of the block containing this log.
    pub block_hash: BlockHash,
}

impl Log {
    /// A key that is stable across redelivery and unique per log.
    #[must_use]
    pub fn dedupe_key(&self) -> String {
        format!(
            "{}:{}:{}",
            self.block_number, self.transaction_hash, self.log_index
        )
    }
}
