//! The query `HyperSync` is sent, and the rows it answers with.

use std::fmt;

use alloy_primitives::{Address, B64, B256, Bloom, Bytes, TxHash, U256};
use serde::de::{self};
use serde::{Deserialize, Deserializer, Serialize};

use crate::ingest::source::{SourceError, malformed};
use crate::wire::datasets::evm::{Block, Log, Transaction};
use crate::wire::envelope::BlockMeta;

use super::CONTEXT;
use super::bincode::{access_list, authorization_list, hashes, withdrawals};

/// One query's body. See the `HyperSync` query reference for every key.
#[derive(Debug, Serialize)]
pub(super) struct Query {
    pub(super) from_block: u64,
    /// Exclusive.
    pub(super) to_block: u64,
    /// Every block in the range, matched or not, so every height has its identity.
    pub(super) include_all_blocks: bool,
    /// Only what the selections match, so logs do not pull in their transactions.
    pub(super) join_mode: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) transactions: Option<[All; 1]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) logs: Option<[All; 1]>,
    pub(super) field_selection: FieldSelection,
}

/// A selection with no filter, which matches every row.
#[derive(Debug, Serialize)]
pub(super) struct All {}

/// The columns a query returns, per table.
#[derive(Debug, Clone, Copy, Serialize)]
pub(super) struct FieldSelection {
    pub(super) block: &'static [&'static str],
    pub(super) transaction: &'static [&'static str],
    pub(super) log: &'static [&'static str],
}

/// The answer to `GET /height`.
#[derive(Debug, Deserialize)]
pub(super) struct Height {
    pub(super) height: u64,
}

/// One query's answer: rows in batches, and where the next query should start.
#[derive(Debug, Deserialize)]
pub(super) struct Response {
    pub(super) data: Vec<Batch>,
    /// The height after the last one this response covers.
    pub(super) next_block: u64,
}

/// One batch of rows. A response's batches are in height order.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(super) struct Batch {
    pub(super) blocks: Vec<RawBlock>,
    pub(super) transactions: Vec<RawTransaction>,
    pub(super) logs: Vec<RawLog>,
}

/// A block as `HyperSync` returns it. Everything but identity is optional, because a
/// query selects only some columns.
#[derive(Debug, Deserialize)]
pub(super) struct RawBlock {
    pub(super) number: Quantity,
    pub(super) hash: B256,
    pub(super) parent_hash: B256,
    pub(super) timestamp: Quantity,
    pub(super) logs_bloom: Bloom,
    pub(super) gas_used: Quantity,
    #[serde(default)]
    pub(super) nonce: Option<B64>,
    #[serde(default)]
    pub(super) sha3_uncles: Option<B256>,
    #[serde(default)]
    pub(super) transactions_root: Option<B256>,
    #[serde(default)]
    pub(super) state_root: Option<B256>,
    #[serde(default)]
    pub(super) receipts_root: Option<B256>,
    #[serde(default)]
    pub(super) withdrawals_root: Option<B256>,
    #[serde(default)]
    pub(super) miner: Option<Address>,
    #[serde(default)]
    pub(super) difficulty: Option<Quantity>,
    #[serde(default)]
    pub(super) total_difficulty: Option<Quantity>,
    #[serde(default)]
    pub(super) size: Option<Quantity>,
    #[serde(default)]
    pub(super) extra_data: Option<Bytes>,
    #[serde(default)]
    pub(super) gas_limit: Option<Quantity>,
    #[serde(default)]
    pub(super) base_fee_per_gas: Option<Quantity>,
    #[serde(default)]
    pub(super) blob_gas_used: Option<Quantity>,
    #[serde(default)]
    pub(super) excess_blob_gas: Option<Quantity>,
    #[serde(default)]
    pub(super) parent_beacon_block_root: Option<B256>,
    #[serde(default)]
    pub(super) mix_hash: Option<B256>,
    /// Bincode; see [`withdrawals`].
    #[serde(default)]
    pub(super) withdrawals: Option<Bytes>,
    #[serde(default)]
    pub(super) l1_block_number: Option<Quantity>,
    #[serde(default)]
    pub(super) send_count: Option<Quantity>,
    #[serde(default)]
    pub(super) send_root: Option<B256>,
    /// The ommers' hashes, concatenated.
    #[serde(default)]
    pub(super) uncles: Option<Bytes>,
}

impl RawBlock {
    pub(super) fn meta(&self) -> Result<BlockMeta, SourceError> {
        Ok(BlockMeta {
            height: self.number.to_u64("block number")?,
            hash: self.hash,
            parent_hash: self.parent_hash,
            timestamp: self.timestamp.to_u64("block timestamp")?,
        })
    }

    /// The `blocks` row, with the block's transaction hashes in index order.
    pub(super) fn into_block(
        self,
        meta: BlockMeta,
        transaction_hashes: Vec<TxHash>,
    ) -> Result<Block, SourceError> {
        Ok(Block {
            number: meta.height,
            hash: meta.hash,
            parent_hash: meta.parent_hash,
            timestamp: meta.timestamp,
            // As the RPC source: a block without a nonce has a zero one.
            nonce: self.nonce.unwrap_or_default(),
            ommers_hash: required(self.sha3_uncles, "sha3_uncles")?,
            transactions_root: required(self.transactions_root, "transactions_root")?,
            state_root: required(self.state_root, "state_root")?,
            receipts_root: required(self.receipts_root, "receipts_root")?,
            withdrawals_root: self.withdrawals_root,
            logs_bloom: self.logs_bloom,
            miner: required(self.miner, "miner")?,
            difficulty: self
                .difficulty
                .map_or(U256::ZERO, |difficulty| difficulty.0),
            total_difficulty: self.total_difficulty.map(|difficulty| difficulty.0),
            size: self.size.map(|size| size.0),
            extra_data: required(self.extra_data, "extra_data")?,
            gas_limit: required(self.gas_limit, "gas_limit")?.to_u64("gas_limit")?,
            gas_used: self.gas_used.to_u64("gas_used")?,
            transaction_count: transaction_hashes.len() as u64,
            base_fee_per_gas: optional_u64(self.base_fee_per_gas, "base_fee_per_gas")?,
            blob_gas_used: optional_u64(self.blob_gas_used, "blob_gas_used")?,
            excess_blob_gas: optional_u64(self.excess_blob_gas, "excess_blob_gas")?,
            parent_beacon_block_root: self.parent_beacon_block_root,
            mix_hash: self.mix_hash,
            withdrawals: self
                .withdrawals
                .as_ref()
                .map(|bytes| withdrawals(bytes))
                .transpose()?,
            // `HyperSync` serves no `requestsHash`, so a backfilled Ethereum block has none.
            requests_hash: None,
            l1_block_number: optional_u64(self.l1_block_number, "l1_block_number")?,
            send_count: optional_u64(self.send_count, "send_count")?,
            send_root: self.send_root,
            ommers: hashes(self.uncles.as_ref(), "uncles")?,
            transaction_hashes,
        })
    }
}

/// A transaction joined to its receipt, as `HyperSync` returns it.
#[derive(Debug, Deserialize)]
pub(super) struct RawTransaction {
    pub(super) block_hash: B256,
    pub(super) block_number: Quantity,
    pub(super) transaction_index: Quantity,
    pub(super) hash: TxHash,
    #[serde(default)]
    pub(super) nonce: Option<Quantity>,
    #[serde(default)]
    pub(super) from: Option<Address>,
    #[serde(default)]
    pub(super) to: Option<Address>,
    #[serde(default)]
    pub(super) value: Option<Quantity>,
    #[serde(default)]
    pub(super) gas: Option<Quantity>,
    #[serde(default)]
    pub(super) gas_price: Option<Quantity>,
    #[serde(default)]
    pub(super) max_fee_per_gas: Option<Quantity>,
    #[serde(default)]
    pub(super) max_priority_fee_per_gas: Option<Quantity>,
    #[serde(default)]
    pub(super) max_fee_per_blob_gas: Option<Quantity>,
    #[serde(default)]
    pub(super) input: Option<Bytes>,
    #[serde(default, rename = "type")]
    pub(super) transaction_type: Option<Quantity>,
    #[serde(default)]
    pub(super) chain_id: Option<Quantity>,
    /// Bincode; see [`access_list`].
    #[serde(default)]
    pub(super) access_list: Option<Bytes>,
    /// Concatenated hashes.
    #[serde(default)]
    pub(super) blob_versioned_hashes: Option<Bytes>,
    /// Bincode; see [`authorization_list`].
    #[serde(default)]
    pub(super) authorization_list: Option<Bytes>,
    #[serde(default)]
    pub(super) r: Option<Quantity>,
    #[serde(default)]
    pub(super) s: Option<Quantity>,
    /// As the node reports it: a legacy transaction's EIP-155 value, a typed one's parity.
    #[serde(default)]
    pub(super) v: Option<Quantity>,
    #[serde(default)]
    pub(super) y_parity: Option<Quantity>,
    #[serde(default)]
    pub(super) status: Option<Quantity>,
    /// The pre-Byzantium post-state root a receipt carries instead of a status.
    #[serde(default)]
    pub(super) root: Option<B256>,
    #[serde(default)]
    pub(super) gas_used: Option<Quantity>,
    #[serde(default)]
    pub(super) cumulative_gas_used: Option<Quantity>,
    #[serde(default)]
    pub(super) effective_gas_price: Option<Quantity>,
    #[serde(default)]
    pub(super) contract_address: Option<Address>,
    #[serde(default)]
    pub(super) logs_bloom: Option<Bloom>,
    #[serde(default)]
    pub(super) blob_gas_used: Option<Quantity>,
    #[serde(default)]
    pub(super) blob_gas_price: Option<Quantity>,
    #[serde(default)]
    pub(super) gas_used_for_l1: Option<Quantity>,
    #[serde(default)]
    pub(super) l1_block_number: Option<Quantity>,
}

impl RawTransaction {
    /// The `transactions` row.
    pub(super) fn into_transaction(
        self,
        meta: BlockMeta,
        transaction_index: u64,
        log_count: u64,
    ) -> Result<Transaction, SourceError> {
        Ok(Transaction {
            hash: self.hash,
            nonce: required(self.nonce, "nonce")?.to_u64("nonce")?,
            transaction_index,
            from: required(self.from, "from")?,
            to: self.to,
            value: required(self.value, "value")?.0,
            gas: required(self.gas, "gas")?.to_u64("gas")?,
            gas_price: optional_u128(self.gas_price, "gas_price")?,
            max_fee_per_gas: optional_u128(self.max_fee_per_gas, "max_fee_per_gas")?,
            max_priority_fee_per_gas: optional_u128(
                self.max_priority_fee_per_gas,
                "max_priority_fee_per_gas",
            )?,
            max_fee_per_blob_gas: optional_u128(self.max_fee_per_blob_gas, "max_fee_per_blob_gas")?,
            input: required(self.input, "input")?,
            transaction_type: u8::try_from(
                required(self.transaction_type, "type")?.to_u64("type")?,
            )
            .map_err(|_| malformed(CONTEXT, "transaction type is above 255"))?,
            chain_id: optional_u64(self.chain_id, "chain_id")?,
            access_list: self
                .access_list
                .as_ref()
                .map(|bytes| access_list(bytes))
                .transpose()?,
            blob_versioned_hashes: self
                .blob_versioned_hashes
                .as_ref()
                .map(|bytes| hashes(Some(bytes), "blob_versioned_hashes"))
                .transpose()?,
            authorization_list: self
                .authorization_list
                .as_ref()
                .map(|bytes| authorization_list(bytes))
                .transpose()?,
            r: self.r.map(|r| r.0),
            s: self.s.map(|s| s.0),
            v: optional_u128(self.v, "v")?,
            y_parity: optional_u64(self.y_parity, "y_parity")?
                .map(|parity| {
                    u8::try_from(parity).map_err(|_| malformed(CONTEXT, "y_parity is above 255"))
                })
                .transpose()?,
            // As alloy reads an RPC receipt: a status when there is one, and success for a
            // pre-Byzantium receipt, which has a state root instead.
            receipt_status: match (self.status, self.root) {
                (Some(status), _) => status.to_u64("status")? == 1,
                (None, Some(_)) => true,
                (None, None) => return Err(missing("status")),
            },
            receipt_gas_used: required(self.gas_used, "gas_used")?.to_u64("gas_used")?,
            receipt_cumulative_gas_used: required(self.cumulative_gas_used, "cumulative_gas_used")?
                .to_u64("cumulative_gas_used")?,
            receipt_effective_gas_price: required(self.effective_gas_price, "effective_gas_price")?
                .to_u128("effective_gas_price")?,
            receipt_contract_address: self.contract_address,
            receipt_logs_bloom: required(self.logs_bloom, "logs_bloom")?,
            receipt_blob_gas_used: optional_u64(self.blob_gas_used, "blob_gas_used")?,
            receipt_blob_gas_price: optional_u128(self.blob_gas_price, "blob_gas_price")?,
            receipt_gas_used_for_l1: optional_u64(self.gas_used_for_l1, "gas_used_for_l1")?,
            receipt_l1_block_number: optional_u64(self.l1_block_number, "l1_block_number")?,
            log_count,
            block_timestamp: meta.timestamp,
            block_number: meta.height,
            block_hash: meta.hash,
        })
    }
}

/// A log as `HyperSync` returns it, its topics already flattened.
#[derive(Debug, Deserialize)]
pub(super) struct RawLog {
    pub(super) block_hash: B256,
    pub(super) block_number: Quantity,
    pub(super) transaction_index: Quantity,
    pub(super) log_index: Quantity,
    #[serde(default)]
    pub(super) transaction_hash: Option<TxHash>,
    #[serde(default)]
    pub(super) address: Option<Address>,
    #[serde(default)]
    pub(super) data: Option<Bytes>,
    #[serde(default)]
    pub(super) topic0: Option<B256>,
    #[serde(default)]
    pub(super) topic1: Option<B256>,
    #[serde(default)]
    pub(super) topic2: Option<B256>,
    #[serde(default)]
    pub(super) topic3: Option<B256>,
    #[serde(default)]
    pub(super) removed: Option<bool>,
}

impl RawLog {
    /// The `logs` row.
    pub(super) fn into_log(
        self,
        meta: BlockMeta,
        transaction_index: u64,
        log_index: u64,
    ) -> Result<Log, SourceError> {
        Ok(Log {
            log_index,
            transaction_hash: required(self.transaction_hash, "transaction_hash")?,
            transaction_index,
            address: required(self.address, "address")?,
            topic0: self.topic0,
            topic1: self.topic1,
            topic2: self.topic2,
            topic3: self.topic3,
            data: required(self.data, "data")?,
            removed: self.removed.unwrap_or(false),
            block_number: meta.height,
            block_hash: meta.hash,
            block_timestamp: meta.timestamp,
        })
    }
}

/// An unsigned integer as `HyperSync` writes it: a JSON number, or `0x` hex that may carry
/// leading zeros (`"0x01"`), which an RPC quantity may not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Quantity(pub(super) U256);

impl Quantity {
    pub(super) fn to_u64(self, field: &str) -> Result<u64, SourceError> {
        u64::try_from(self.0)
            .map_err(|_| malformed(CONTEXT, format!("{field} {} overflows u64", self.0)))
    }

    pub(super) fn to_u128(self, field: &str) -> Result<u128, SourceError> {
        u128::try_from(self.0)
            .map_err(|_| malformed(CONTEXT, format!("{field} {} overflows u128", self.0)))
    }
}

impl<'de> Deserialize<'de> for Quantity {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl de::Visitor<'_> for Visitor {
            type Value = Quantity;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an unsigned integer, or `0x` hex")
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Quantity, E> {
                Ok(Quantity(U256::from(value)))
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Quantity, E> {
                parse_quantity(value).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

/// Parses `0x` hex, leading zeros allowed.
pub(super) fn parse_quantity(text: &str) -> Result<Quantity, String> {
    let digits = text
        .strip_prefix("0x")
        .filter(|digits| !digits.is_empty())
        .ok_or_else(|| format!("{text:?} is not 0x hex"))?;
    U256::from_str_radix(digits, 16)
        .map(Quantity)
        .map_err(|error| format!("{text:?} is not a 256-bit quantity: {error}"))
}

/// A selected column the response left out.
fn missing(field: &str) -> SourceError {
    malformed(CONTEXT, format!("missing {field}"))
}

pub(super) fn required<T>(value: Option<T>, field: &str) -> Result<T, SourceError> {
    value.ok_or_else(|| missing(field))
}

fn optional_u64(value: Option<Quantity>, field: &str) -> Result<Option<u64>, SourceError> {
    value.map(|value| value.to_u64(field)).transpose()
}

fn optional_u128(value: Option<Quantity>, field: &str) -> Result<Option<u128>, SourceError> {
    value.map(|value| value.to_u128(field)).transpose()
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn a_quantity_takes_leading_zeros_but_not_bare_digits() {
        assert_eq!(parse_quantity("0x01").expect("hex").0, U256::from(1));
        assert_eq!(parse_quantity("0x0000").expect("zero").0, U256::ZERO);
        for bare in ["0x", "12"] {
            assert_eq!(
                parse_quantity(bare).err(),
                Some(format!("{bare:?} is not 0x hex"))
            );
        }
    }
}
