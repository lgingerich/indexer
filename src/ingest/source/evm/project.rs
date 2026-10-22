//! Projecting the node's answers into dataset rows.

use alloy_consensus::Transaction as ConsensusTransaction;
use alloy_consensus::transaction::to_eip155_value;
use alloy_network::TransactionResponse;
use alloy_network::any::{AnyRpcBlock, AnyRpcTransaction, AnyTransactionReceipt, AnyTxEnvelope};
use alloy_network::eip2718::Typed2718 as _;
use alloy_primitives::{B256, U8, U64, U128, U256};
use alloy_rpc_types_eth::Log as RpcLog;
use alloy_serde::OtherFields;

use super::plan::{FetchPlan, LogSource};
use super::{BLOCK, LOGS, RECEIPTS};
use crate::ingest::source::assemble::{BlockRows, check_logs_present};
use crate::ingest::source::{Datasets, FetchedBlock, SourceError, inconsistent, malformed};
use crate::wire::datasets::evm::{Block, Log, Transaction};
use crate::wire::envelope::{BlockMeta, Event};

/// The identity, parent hash, and timestamp a fetched block is anchored to — the same
/// fields [`From<RpcBlockMeta>`] reads off a notification, so one type answers both.
impl From<&AnyRpcBlock> for BlockMeta {
    fn from(block: &AnyRpcBlock) -> Self {
        let header = &block.0.inner.header;
        Self {
            height: header.number,
            hash: header.hash,
            parent_hash: header.inner.parent_hash,
            timestamp: header.inner.timestamp,
        }
    }
}

/// The results one batched block request carries, still in alloy's RPC types
/// and not yet projected into [`crate::wire::datasets`] records.
///
/// The block and receipts use alloy's *catch-all* (`any`) types, so a chain's
/// non-Ethereum transaction types — an OP-stack deposit (`0x7e`), an Arbitrum
/// retry (`0x6a`) — decode instead of failing the whole block. Everything this
/// source reads is reachable through the standard `Transaction`/`BlockHeader`
/// accessors; only the transaction type falls back to a raw `u8`.
///
/// Only for `benches/hot_path.rs`, which measures [`decode_block`] without a node; the
/// source itself fetches and projects inside [`EvmSource`](super::EvmSource). Hidden
/// from the docs, since it is not an API for anything else.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct RpcBatch {
    /// The block metadata the projection is anchored to.
    ///
    /// Its identity is checked against the fetched block when one was fetched, so a
    /// reused live notification and the HTTP response cannot silently disagree.
    pub meta: BlockMeta,
    /// The block, from `eth_getBlockByNumber` with full transactions.
    ///
    /// `None` when a validated live notification already supplied the metadata and
    /// the selected datasets need nothing from the body — the logs-only case.
    pub block: Option<AnyRpcBlock>,
    /// The block's receipts, from `eth_getBlockReceipts`.
    ///
    /// `None` when the node does not serve that method; the caller then has to
    /// fetch each receipt by transaction hash. Some nodes answer the method with
    /// a [`METHOD_NOT_FOUND`](crate::ingest::source::METHOD_NOT_FOUND) error and others
    /// with a `null` result, so both are treated the same way.
    pub receipts: Option<Vec<AnyTransactionReceipt>>,
    /// Logs from `eth_getLogs` for this height.
    ///
    /// `None` when logs are taken from [`Self::receipts`] instead, which is the
    /// case whenever transactions, and so receipts, were fetched.
    pub logs: Option<Vec<RpcLog>>,
}

/// Turns a parsed batch into ordered dataset events and its metadata.
///
/// Projects every row in the batch, as a source built for every dataset does. Order is
/// the block row, then each transaction followed by its logs, all in index order, so a
/// consumer sees every transaction before its logs. A body is only required for the
/// rows that read one, so a logs-only block with no fetched block still decodes.
///
/// Only for `benches/hot_path.rs` and the projection's tests, like [`RpcBatch`].
///
/// # Errors
///
/// Returns [`SourceError::Malformed`] when a body the rows read is absent, or the
/// receipts do not line up with its transactions, and [`SourceError::Inconsistent`]
/// when the receipts belong to another block or are missing as a lagging node's are.
#[doc(hidden)]
pub fn decode_block(batch: RpcBatch) -> Result<FetchedBlock, SourceError> {
    let RpcBatch {
        meta,
        block,
        receipts,
        logs,
    } = batch;
    Ok(FetchedBlock {
        meta,
        events: project(
            meta,
            block.as_ref(),
            receipts.as_deref(),
            logs.as_deref(),
            FetchPlan::new(Datasets::all()),
        )?,
    })
}

/// Projects a fetched block into dataset events, anchored to `meta`.
///
/// The block row is pushed only when the `blocks` dataset is selected; metadata is
/// reported separately, so the pipeline keeps linkage and timestamps whether or not
/// the row is stored. The order is each transaction, then its logs, all in block
/// index order, with `eth_getLogs` rows appended after.
/// A selected dataset whose body is absent is an error; a logs-only fetch that reused
/// a notification has no body and appends only its hash-pinned logs.
///
/// `eth_getLogs` answering nothing for a header whose `logsBloom` is set is an error
/// too: the block has logs, and a backend that has not indexed it can answer an empty
/// list rather than fail.
pub(super) fn project(
    meta: BlockMeta,
    block: Option<&AnyRpcBlock>,
    receipts: Option<&[AnyTransactionReceipt]>,
    logs: Option<&[RpcLog]>,
    plan: FetchPlan,
) -> Result<Vec<Event>, SourceError> {
    // Anything but logs is projected from the block body, and receipts are validated
    // against its transaction identities, so a missing body is an error rather than a
    // silently empty projection. Logs alone need no body.
    if (plan.block_row || plan.transactions) && block.is_none() {
        return Err(malformed(BLOCK, "block was not fetched"));
    }
    if plan.transactions && receipts.is_none() {
        return Err(malformed(RECEIPTS, "receipts were not fetched"));
    }
    if plan.logs == LogSource::GetLogs && logs.is_none() {
        return Err(malformed(LOGS, "logs were not fetched"));
    }

    let mut rows = BlockRows::default();
    if let Some(block) = block {
        let header = &block.0.inner.header;
        if header.number != meta.height || header.hash != meta.hash {
            return Err(malformed(
                BLOCK,
                format!("block {} at {} is not {meta:?}", header.hash, header.number),
            ));
        }
        if plan.block_row {
            rows.block = Some(decode_header(block));
        }
        append_body(&mut rows, block, receipts, plan, meta)?;
    }
    if let Some(logs) = logs {
        if let Some(block) = block {
            check_logs_present(
                LOGS,
                meta.height,
                !logs.is_empty(),
                &block.0.inner.header.inner.logs_bloom,
            )?;
        }
        for log in logs {
            rows.logs.push(get_logs_record(
                log,
                meta.height,
                meta.hash,
                meta.timestamp,
            )?);
        }
    }
    Ok(rows.into_events())
}

/// Appends the transaction and log rows a fetched body and its receipts carry.
///
/// Both come from the receipts, which are fetched only alongside transactions, so a
/// body read for the block row alone appends nothing here.
fn append_body(
    rows: &mut BlockRows,
    block: &AnyRpcBlock,
    receipts: Option<&[AnyTransactionReceipt]>,
    plan: FetchPlan,
    meta: BlockMeta,
) -> Result<(), SourceError> {
    if !plan.transactions {
        return Ok(());
    }
    let transactions = block
        .0
        .inner
        .transactions
        .as_transactions()
        .ok_or_else(|| malformed(BLOCK, "block returned transaction hashes only"))?;
    let receipts = receipts.ok_or_else(|| malformed(RECEIPTS, "receipts were not fetched"))?;
    check_receipts_answer(receipts, transactions.len(), meta.hash)?;
    for (position, (transaction, receipt)) in transactions.iter().zip(receipts).enumerate() {
        let tx_index = position as u64;
        if receipt.transaction_index != Some(tx_index)
            || receipt.transaction_hash != transaction.tx_hash()
        {
            return Err(malformed(
                RECEIPTS,
                format!(
                    "receipt {position} has transactionIndex {:?}, not {tx_index}",
                    receipt.transaction_index
                ),
            ));
        }
        let mut own = Vec::new();
        if plan.logs == LogSource::Receipts {
            for log in receipt.logs() {
                // Logs nested in this receipt, as opposed to a separate `eth_getLogs`
                // query; the label names the boundary an error at this site reports.
                own.push(log_record(
                    log,
                    tx_index,
                    meta.height,
                    meta.hash,
                    meta.timestamp,
                    RECEIPTS,
                )?);
            }
        }
        rows.transactions.push((
            decode_transaction(transaction, receipt, tx_index, meta),
            own,
        ));
    }
    Ok(())
}

/// Checks that `receipts` answer the block `hash` and its `transactions`, and says
/// whether asking again can help when they do not.
///
/// - A receipt with no `blockHash` is [`malformed`]: a receipt of a mined block always
///   has one, and a node that leaves it out will leave it out again. Asking again forever
///   would only hide that.
/// - A receipt from another block is [`inconsistent`]: the block and its receipts are
///   separate calls, so the chain moved between them, or a load-balanced provider sent
///   them to backends on different forks. This is checked before the count, since the
///   other block's receipts need not number the same.
/// - No receipts for a block with transactions is [`inconsistent`] too: a backend that
///   has not processed the block answers `[]` as readily as `null` (see [`NOT_SERVED`]).
/// - Any other count is [`malformed`]: a node that places receipts in this block and
///   still answers short or long has the block, and has it wrong.
fn check_receipts_answer(
    receipts: &[AnyTransactionReceipt],
    transactions: usize,
    hash: B256,
) -> Result<(), SourceError> {
    for receipt in receipts {
        let block = receipt
            .block_hash
            .ok_or_else(|| malformed(RECEIPTS, "receipt has no blockHash"))?;
        if block != hash {
            return Err(inconsistent(
                RECEIPTS,
                format!(
                    "receipt belongs to block {block}, not {hash}; the chain reorganised mid-request"
                ),
            ));
        }
    }
    if receipts.is_empty() && transactions > 0 {
        return Err(inconsistent(
            RECEIPTS,
            format!(
                "no receipts for {transactions} transactions: the node does not serve them yet"
            ),
        ));
    }
    if receipts.len() != transactions {
        return Err(malformed(
            RECEIPTS,
            format!(
                "{} receipts for {transactions} transactions",
                receipts.len()
            ),
        ));
    }
    Ok(())
}

/// One `eth_getLogs` row, checked against the header it was requested for.
///
/// The `eth_getLogs` counterpart of the receipt path's [`log_record`] call: that call
/// runs separately from the block read, so the row's block identity is checked here.
fn get_logs_record(
    log: &RpcLog,
    block_number: u64,
    block_hash: B256,
    block_timestamp: u64,
) -> Result<Log, SourceError> {
    let got = log
        .block_hash
        .ok_or_else(|| malformed(LOGS, "log has no blockHash"))?;
    if got != block_hash {
        return Err(inconsistent(
            LOGS,
            format!("log belongs to block {got}, not {block_hash}"),
        ));
    }
    if log.block_number != Some(block_number) {
        return Err(malformed(
            LOGS,
            format!("log blockNumber {:?}, not {block_number}", log.block_number),
        ));
    }
    let tx_index = log
        .transaction_index
        .ok_or_else(|| malformed(LOGS, "log has no transactionIndex"))?;
    log_record(
        log,
        tx_index,
        block_number,
        block_hash,
        block_timestamp,
        LOGS,
    )
}

/// Flattens a block header and its transaction hashes into the `blocks` dataset.
///
/// A chain's own header fields — Arbitrum's `l1BlockNumber`, `sendCount`, and
/// `sendRoot` — are not in alloy's header, so they are read from the fields the
/// catch-all captured, and are `None` on a chain without them. One field a receipt may
/// carry is not: the pre-Byzantium `stateRoot` in place of a `status` (alloy's
/// `coerce_status` maps that variant to `true`, so a pre-Byzantium failure would read
/// as a success). A consumer needing it reads the receipt.
fn decode_header(block: &AnyRpcBlock) -> Block {
    let header = &block.0.inner.header;
    let transactions = &block.0.inner.transactions;
    let extra = &block.0.other;
    Block {
        number: header.number,
        hash: header.hash,
        parent_hash: header.inner.parent_hash,
        timestamp: header.inner.timestamp,
        nonce: header.inner.nonce.unwrap_or_default(),
        ommers_hash: header.inner.ommers_hash,
        transactions_root: header.inner.transactions_root,
        state_root: header.inner.state_root,
        receipts_root: header.inner.receipts_root,
        withdrawals_root: header.inner.withdrawals_root,
        logs_bloom: header.inner.logs_bloom,
        miner: header.inner.beneficiary,
        difficulty: header.inner.difficulty,
        total_difficulty: header.total_difficulty,
        size: header.size,
        extra_data: header.inner.extra_data.clone(),
        gas_limit: header.inner.gas_limit,
        gas_used: header.inner.gas_used,
        transaction_count: transactions.len() as u64,
        base_fee_per_gas: header.inner.base_fee_per_gas,
        blob_gas_used: header.inner.blob_gas_used,
        excess_blob_gas: header.inner.excess_blob_gas,
        parent_beacon_block_root: header.inner.parent_beacon_block_root,
        mix_hash: header.inner.mix_hash,
        withdrawals: block
            .0
            .inner
            .withdrawals
            .as_ref()
            .map(|withdrawals| withdrawals.to_vec()),
        requests_hash: header.inner.requests_hash,
        l1_block_number: quantity(extra, "l1BlockNumber"),
        send_count: quantity(extra, "sendCount"),
        send_root: extra.get_deserialized("sendRoot").and_then(Result::ok),
        ommers: block.0.inner.uncles.clone(),
        transaction_hashes: transactions.hashes().collect(),
    }
}

/// Flattens one RPC transaction and its receipt into the `transactions` dataset.
///
/// Every transaction field comes from the standard `alloy_consensus::Transaction`
/// accessors, which work for Ethereum and non-Ethereum types alike (an OP deposit reads
/// its `gasPrice` from the unknown transaction's captured fields). The type is kept as
/// the raw `u8` so a non-Ethereum type — `0x7e`, `0x6a` — survives. The receipt's
/// copies of the transaction's own fields are not read; its logs become [`Log`] rows.
fn decode_transaction(
    transaction: &AnyRpcTransaction,
    receipt: &AnyTransactionReceipt,
    tx_index: u64,
    meta: BlockMeta,
) -> Transaction {
    let signature = signature(transaction);
    Transaction {
        hash: transaction.tx_hash(),
        nonce: ConsensusTransaction::nonce(transaction),
        transaction_index: tx_index,
        from: transaction.from(),
        to: ConsensusTransaction::to(transaction),
        value: ConsensusTransaction::value(transaction),
        gas: ConsensusTransaction::gas_limit(transaction),
        // `gas_price` is the node's reported paid price, not the consensus accessor,
        // which is `None` for a dynamic-fee type. `max_fee_per_gas` is the cap the
        // transaction actually carries, if any; the consensus accessor is not used for
        // it because it fabricates one for a type that has none.
        gas_price: TransactionResponse::gas_price(transaction),
        max_fee_per_gas: known_max_fee_per_gas(transaction),
        max_priority_fee_per_gas: ConsensusTransaction::max_priority_fee_per_gas(transaction),
        max_fee_per_blob_gas: ConsensusTransaction::max_fee_per_blob_gas(transaction),
        input: ConsensusTransaction::input(transaction).clone(),
        transaction_type: transaction.ty(),
        chain_id: ConsensusTransaction::chain_id(transaction),
        access_list: ConsensusTransaction::access_list(transaction).cloned(),
        blob_versioned_hashes: ConsensusTransaction::blob_versioned_hashes(transaction)
            .map(<[B256]>::to_vec),
        authorization_list: ConsensusTransaction::authorization_list(transaction)
            .map(<[_]>::to_vec),
        r: signature.r,
        s: signature.s,
        v: signature.v,
        y_parity: signature.y_parity,
        receipt_status: receipt.inner.inner.status(),
        receipt_gas_used: receipt.gas_used,
        receipt_cumulative_gas_used: receipt.inner.inner.cumulative_gas_used(),
        receipt_effective_gas_price: receipt.effective_gas_price,
        receipt_contract_address: receipt.contract_address,
        receipt_logs_bloom: receipt.inner.inner.bloom(),
        receipt_blob_gas_used: receipt.blob_gas_used,
        receipt_blob_gas_price: receipt.blob_gas_price,
        receipt_gas_used_for_l1: quantity(&receipt.other, "gasUsedForL1"),
        receipt_l1_block_number: quantity(&receipt.other, "l1BlockNumber"),
        log_count: receipt.logs().len() as u64,
        block_timestamp: meta.timestamp,
        block_number: meta.height,
        block_hash: meta.hash,
    }
}

/// A transaction's signature, as the node reports it.
struct SignatureFields {
    r: Option<U256>,
    s: Option<U256>,
    v: Option<u128>,
    y_parity: Option<u8>,
}

/// Reads a transaction's signature.
///
/// A known envelope keeps only the parity, so a legacy transaction's `v` is rebuilt
/// from it and the chain id as EIP-155 defines it, which is the value the node sent; a
/// typed one's `v` is the parity itself, and only a typed one has a `yParity`. An
/// unknown type is read from the fields the catch-all captured, each `None` when the
/// node gave none.
fn signature(transaction: &AnyRpcTransaction) -> SignatureFields {
    match transaction.0.inner.inner.inner() {
        AnyTxEnvelope::Ethereum(envelope) => {
            let signature = envelope.signature();
            let parity = signature.v();
            let legacy = envelope.is_legacy();
            SignatureFields {
                r: Some(signature.r()),
                s: Some(signature.s()),
                v: Some(if legacy {
                    to_eip155_value(parity, ConsensusTransaction::chain_id(transaction))
                } else {
                    u128::from(parity)
                }),
                y_parity: (!legacy).then_some(u8::from(parity)),
            }
        }
        AnyTxEnvelope::Unknown(unknown) => {
            let fields = &unknown.inner.fields;
            SignatureFields {
                r: fields.get_deserialized("r").and_then(Result::ok),
                s: fields.get_deserialized("s").and_then(Result::ok),
                v: fields
                    .get_deserialized::<U128>("v")
                    .and_then(Result::ok)
                    .map(|v| v.to()),
                y_parity: fields
                    .get_deserialized::<U8>("yParity")
                    .and_then(Result::ok)
                    .map(|parity| parity.to()),
            }
        }
    }
}

/// A chain-specific quantity the catch-all captured under `key`, when the node sent one
/// that fits.
fn quantity(fields: &OtherFields, key: &str) -> Option<u64> {
    fields
        .get_deserialized::<U64>(key)
        .and_then(Result::ok)
        .map(|value| value.to())
}

/// The maximum fee per gas a transaction actually carries, if it carries one.
///
/// `None` for a legacy or EIP-2930 transaction, which has no cap, and for a
/// chain-specific type the node gives none — an OP-stack deposit (`0x7e`), say. Both
/// distinctions matter, so neither is defaulted: the consensus `max_fee_per_gas`
/// accessor saturates to the gas price for a legacy transaction and to zero for an
/// unknown envelope, and storing either would be a cap the chain never had.
///
/// A known envelope answers through the RPC accessor, which already returns `None` for
/// a type below 2. An unknown one is read from the fields the catch-all captured, the
/// same place [`AnyRpcTransaction`] keeps an OP deposit's `gasPrice`.
fn known_max_fee_per_gas(transaction: &AnyRpcTransaction) -> Option<u128> {
    if transaction.0.inner.inner.is_unknown() {
        return transaction
            .0
            .other
            .get_deserialized::<U128>("maxFeePerGas")
            .and_then(Result::ok)
            .map(|fee| fee.to());
    }
    TransactionResponse::max_fee_per_gas(transaction)
}

/// Flattens one RPC log into the `logs` dataset.
///
/// A log's identity is its transaction hash and its index in the block, and both are
/// required for the dataset's `dedupe_key` to mean anything: two logs in the same
/// transaction at the same index would be one row. So both are **rejected** when absent
/// rather than defaulted.
///
/// alloy models `logIndex` and `transactionIndex` as `Option` because an `eth_getLogs`
/// filter result can lack block context. That is not this path — every log here is nested
/// in a receipt fetched for a specific block, where both are present — so the `None` arms
/// are unreachable in practice, and a node that sent one is describing a log we cannot
/// place in the chain. Defaulting would be worse than failing: `log_index` in particular
/// would be filled from the log's position *within its receipt*, which is not the block
/// index the field means, and the wrong value silently collides on dedupe with a different
/// log in the same transaction.
///
/// `transaction_index` is the *transaction's* position in the block, which the caller
/// already knows from the receipt loop, so it is the same value the log would carry.
///
/// # Errors
///
/// Returns [`SourceError::Malformed`] when the log has no transaction hash, or no index
/// within the block.
fn log_record(
    log: &RpcLog,
    transaction_index: u64,
    block_number: u64,
    block_hash: B256,
    block_timestamp: u64,
    context: &str,
) -> Result<Log, SourceError> {
    let transaction_hash = log
        .transaction_hash
        .ok_or_else(|| malformed(context, "log has no transactionHash"))?;
    let log_index = log.log_index.ok_or_else(|| {
        malformed(
            context,
            "log has no logIndex; it cannot be placed in the block",
        )
    })?;
    let topics = log.topics();
    Ok(Log {
        log_index,
        transaction_hash,
        transaction_index,
        address: log.address(),
        topic0: topics.first().copied(),
        topic1: topics.get(1).copied(),
        topic2: topics.get(2).copied(),
        topic3: topics.get(3).copied(),
        data: log.data().data.clone(),
        removed: log.removed,
        block_number,
        block_hash,
        block_timestamp,
    })
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use alloy_consensus::TxType;
    use alloy_network::any::AnyRpcBlock;
    use alloy_primitives::{Address, B256, U256};
    use serde_json::{Value, json};

    use super::*;

    use crate::ingest::source::evm::testing::*;
    use crate::wire::envelope::{BlockMeta, Event};

    /// The typed projection input; RPC behavior is tested through the source below.
    ///
    /// The metadata is derived from the block fixture, so a fetched body and its
    /// metadata agree, which is what [`project`] checks.
    fn batch(block: &Value, receipts: &Value) -> RpcBatch {
        let parsed: AnyRpcBlock = serde_json::from_value(block.clone()).expect("block fixture");
        let meta = BlockMeta::from(&parsed);
        RpcBatch {
            meta,
            block: Some(parsed),
            receipts: Some(serde_json::from_value(receipts.clone()).expect("receipt fixture")),
            logs: None,
        }
    }

    #[test]
    fn datasets_carry_their_fields_and_reference_children_by_key() {
        let fetched = decode_block(batch(&block(), &receipts())).expect("batch decodes");

        let Event::Block(block) = &fetched.events[0] else {
            panic!("first event must be the block marker");
        };
        // Header fields are lifted, and children are referenced by key, not embedded.
        assert_eq!(
            block.miner,
            address(0x11).parse::<Address>().expect("miner parses")
        );
        assert_eq!(block.extra_data.len(), 2);
        assert_eq!(
            block.transaction_hashes,
            vec![
                hash(0x11).parse::<B256>().expect("hash"),
                hash(0x22).parse::<B256>().expect("hash"),
            ]
        );

        let Event::Transaction(transaction) = &fetched.events[1] else {
            panic!("second event must be a transaction");
        };
        assert_eq!(
            transaction.hash,
            hash(0x11).parse::<B256>().expect("tx id parses")
        );
        assert_eq!(transaction.transaction_index, 0);
        assert_eq!(transaction.nonce, 1);
        assert_eq!(
            transaction.to,
            Some(address(0xf1).parse().expect("to parses"))
        );
        assert_eq!(transaction.input.len(), 4);
        assert_eq!(transaction.transaction_type, TxType::Eip1559 as u8);
        assert_eq!(transaction.block_number, 18_000_000);
        // The receipt's fields are joined onto the transaction row.
        assert!(transaction.receipt_status);
        assert_eq!(transaction.receipt_gas_used, 21_000);
        assert_eq!(transaction.receipt_cumulative_gas_used, 21_000);
        assert_eq!(transaction.receipt_effective_gas_price, 0x004c_4b40);
        assert!(transaction.receipt_contract_address.is_none());
        // Logs are their own dataset; the transaction carries only their count.
        assert_eq!(transaction.log_count, 2);

        let Event::Transaction(failed) = &fetched.events[4] else {
            panic!("fifth event must be the second transaction");
        };
        assert!(
            !failed.receipt_status,
            "the fixture's second receipt reverted"
        );
        assert_eq!(failed.receipt_cumulative_gas_used, 0xa410);
        assert_eq!(failed.log_count, 0);

        let Event::Log(log) = &fetched.events[3] else {
            panic!("fourth event must be a log");
        };
        assert_eq!(log.log_index, 1);
        assert_eq!(log.transaction_hash, transaction.hash);
        assert_eq!(
            log.address,
            address(0xbe).parse::<Address>().expect("address parses")
        );
        assert_eq!(log.topic0, Some(hash(0x11).parse().expect("topic0")));
        assert!(log.topic1.is_none());
    }

    /// What a chain adds to the standard fields is kept: Ethereum's withdrawals and
    /// requests hash, Arbitrum's L1 anchoring on the block and its receipts, and every
    /// signature, including one on a transaction type alloy does not know.
    #[test]
    fn chain_fields_and_signatures_are_kept() {
        let mut block = block();
        block["withdrawals"] = json!([{
            "index": "0x8b872fb", "validatorIndex": "0x2328f2",
            "address": address(0x52), "amount": "0xd0f510",
        }]);
        block["requestsHash"] = json!(hash(0x0a));
        block["l1BlockNumber"] = json!("0x16ff3c1");
        block["sendCount"] = json!("0x2a");
        block["sendRoot"] = json!(hash(0x0b));
        block["transactions"]
            .as_array_mut()
            .expect("transactions")
            .push(json!({
                "type": "0x6a", "chainId": "0x1237", "nonce": "0x0",
                "hash": hash(0x33), "blockHash": hash(0xab),
                "blockNumber": "0x112a880", "transactionIndex": "0x2",
                "from": address(0xa4), "to": address(0xa5), "value": "0x0",
                "gas": "0x0", "gasPrice": "0x0", "input": "0x6bf6a42d",
                "v": "0x0", "r": "0x0", "s": "0x0",
            }));
        let mut receipts = receipts();
        receipts[0]["gasUsedForL1"] = json!("0x1f4");
        receipts[0]["l1BlockNumber"] = json!("0x16ff3c1");
        let mut arbitrum = receipts[1].clone();
        arbitrum["transactionHash"] = json!(hash(0x33));
        arbitrum["transactionIndex"] = json!("0x2");
        arbitrum["type"] = json!("0x6a");
        arbitrum["status"] = json!("0x1");
        receipts.as_array_mut().expect("receipts").push(arbitrum);

        let fetched = decode_block(batch(&block, &receipts)).expect("batch decodes");
        let Event::Block(header) = &fetched.events[0] else {
            panic!("first event must be the block marker");
        };
        assert_eq!(header.mix_hash, Some(B256::ZERO));
        let withdrawals = header.withdrawals.as_ref().expect("withdrawals");
        assert_eq!(withdrawals.len(), 1);
        assert_eq!(withdrawals[0].validator_index, 0x0023_28f2);
        assert_eq!(
            header.requests_hash,
            Some(hash(0x0a).parse().expect("hash"))
        );
        assert_eq!(header.l1_block_number, Some(0x016f_f3c1));
        assert_eq!(header.send_count, Some(42));
        assert_eq!(header.send_root, Some(hash(0x0b).parse().expect("hash")));

        let transactions: Vec<_> = fetched
            .events
            .iter()
            .filter_map(|event| match event {
                Event::Transaction(transaction) => Some(transaction),
                _ => None,
            })
            .collect();
        let [typed, legacy, unknown] = transactions[..] else {
            panic!("three transactions");
        };
        // A typed transaction's `v` is its parity, which it also carries as `yParity`.
        assert_eq!(
            typed.r,
            Some(U256::from_be_bytes(B256::repeat_byte(0x05).0))
        );
        assert_eq!(
            typed.s,
            Some(U256::from_be_bytes(B256::repeat_byte(0x06).0))
        );
        assert_eq!((typed.v, typed.y_parity), (Some(1), Some(1)));
        assert_eq!(typed.receipt_gas_used_for_l1, Some(500));
        assert_eq!(typed.receipt_l1_block_number, Some(0x016f_f3c1));
        // A legacy one's `v` is the EIP-155 value the node sent, and it has no `yParity`.
        assert_eq!((legacy.v, legacy.y_parity), (Some(0x25), None));
        assert_eq!(legacy.receipt_gas_used_for_l1, None);
        // An unknown type's signature is read from the fields the node sent.
        assert_eq!(unknown.transaction_type, 0x6a);
        assert_eq!(
            (unknown.r, unknown.s, unknown.v, unknown.y_parity),
            (Some(U256::ZERO), Some(U256::ZERO), Some(0), None)
        );
    }

    /// A chain without the extras leaves them null rather than zero.
    #[test]
    fn missing_chain_fields_are_none() {
        let fetched = decode_block(batch(&block(), &receipts())).expect("batch decodes");
        let Event::Block(header) = &fetched.events[0] else {
            panic!("first event must be the block marker");
        };
        assert_eq!(
            (header.withdrawals.as_ref(), header.requests_hash),
            (None, None)
        );
        assert_eq!(
            (header.l1_block_number, header.send_count, header.send_root),
            (None, None, None)
        );
        let transactions: Vec<_> = fetched
            .events
            .iter()
            .filter_map(|event| match event {
                Event::Transaction(transaction) => Some(transaction),
                _ => None,
            })
            .collect();
        assert_eq!(transactions.len(), 2);
        for transaction in transactions {
            assert_eq!(
                (
                    transaction.receipt_gas_used_for_l1,
                    transaction.receipt_l1_block_number
                ),
                (None, None),
                "transaction {}",
                transaction.transaction_index
            );
        }
    }

    #[test]
    fn topics_past_three_are_dropped_and_data_is_kept_verbatim() {
        let fetched = decode_block(batch(&block(), &receipts())).expect("batch decodes");
        let Event::Log(first) = &fetched.events[2] else {
            panic!("third event must be a log");
        };
        // The source log carries five topics; only topic0..topic3 are kept, because
        // four is the maximum an EVM log can index.
        assert_eq!(first.topic0, Some(hash(0x01).parse().expect("topic0")));
        assert_eq!(first.topic1, Some(hash(0x02).parse().expect("topic1")));
        assert_eq!(first.topic2, Some(hash(0x03).parse().expect("topic2")));
        assert_eq!(first.topic3, Some(hash(0x04).parse().expect("topic3")));
        assert_eq!(
            first.data,
            alloy_primitives::Bytes::from_static(&[0x00, 0x01])
        );
    }

    /// Receipts that do not answer the block's transactions are refused. One from
    /// another block means the chain moved mid-request, however many there are, and none
    /// at all is a node that has not processed the block: asking again can help. A
    /// missing, misplaced, or unplaced one in a block's own answer cannot be right.
    #[test]
    fn receipts_that_do_not_match_the_block_are_rejected() {
        let other_block = format!(
            "inconsistent data from eth_getBlockReceipts: receipt belongs to block {}, not {}; the chain reorganised mid-request",
            hash(0xcd),
            hash(0xab)
        );
        let cases: [(&str, fn(&mut Value), String); 6] = [
            (
                "a receipt from another block",
                |receipts| receipts[1]["blockHash"] = json!(hash(0xcd)),
                other_block.clone(),
            ),
            (
                "fewer receipts, from another block",
                |receipts| {
                    receipts.as_array_mut().expect("receipts").pop();
                    receipts[0]["blockHash"] = json!(hash(0xcd));
                },
                other_block,
            ),
            (
                "no receipts",
                |receipts| *receipts = json!([]),
                "inconsistent data from eth_getBlockReceipts: no receipts for 2 transactions: the node does not serve them yet"
                    .to_owned(),
            ),
            (
                "a receipt without a blockHash",
                |receipts| {
                    receipts[1]
                        .as_object_mut()
                        .expect("receipt")
                        .remove("blockHash");
                },
                "malformed data from eth_getBlockReceipts: receipt has no blockHash".to_owned(),
            ),
            (
                "a missing receipt",
                |receipts| {
                    receipts.as_array_mut().expect("receipts").pop();
                },
                "malformed data from eth_getBlockReceipts: 1 receipts for 2 transactions"
                    .to_owned(),
            ),
            (
                "a receipt at another index",
                |receipts| receipts[1]["transactionIndex"] = json!("0x5"),
                "malformed data from eth_getBlockReceipts: receipt 1 has transactionIndex Some(5), not 1"
                    .to_owned(),
            ),
        ];
        for (case, change, expected) in cases {
            let mut receipts = receipts();
            change(&mut receipts);
            let error = decode_block(batch(&block(), &receipts)).expect_err(case);
            assert_eq!(error.to_string(), expected, "{case}");
        }
    }
}
