//! An EVM block source: live heads over WebSocket, blocks over JSON-RPC.
//!
//! The two transports map onto the two roles in [`BlockSource`]. `newHeads` over
//! WebSocket is the live path: a notification carries the announced block's
//! metadata, which the fetch path reuses instead of reading the same header again.
//! JSON-RPC over HTTP is the pull path, and also fills gaps and backs off the live
//! path. Requirements worth stating because they are easy to get wrong:
//!
//! - A block's metadata (identity, parent hash, timestamp) is reported separately
//!   from its events, so parent linkage and dataset timestamps are available even
//!   when the block row itself is not stored. The block row is emitted only when the
//!   `blocks` dataset is selected.
//! - The selected datasets decide the calls, once, as a `FetchPlan`. Logs without
//!   transactions use `eth_getLogs` for that one height, pinned to the announced hash
//!   when the notification supplied it; a validated notification header then stands
//!   in for the header read. Transactions require full transaction objects and their
//!   receipts, from `eth_getBlockReceipts`; each transaction row is joined to its
//!   receipt, and selected logs are taken from those receipts. Every
//!   field the selected datasets declare is carried across; a few the node also
//!   returns are not, and each omission is named at the projection that makes it.
//!   Some nodes lack `eth_getBlockReceipts` (some L2s, pre-Cancun Ethereum); it is
//!   answered with `-32601` or null, and the source then fetches each receipt with
//!   `eth_getTransactionReceipt` in batches of `BATCH_LIMIT`.
//! - The calls are sent as one batch, so a block costs one round trip. They
//!   still execute separately on the node, so a reorg between them can pair a
//!   block with another fork's receipts or logs; every `blockHash` is checked.
//! - Without transactions, buried backfill widens that batch to a range of up to
//!   `BATCH_LIMIT` heights — their headers and one `eth_getLogs` over the range — and
//!   retries a range the provider fails on in halves.
//! - The batch is parsed into alloy's RPC types, then projected field by field into
//!   the [`crate::wire::datasets`] records; nothing is kept as opaque JSON.
//! - Parsing uses alloy's *catch-all* (`any`) types, so a chain's non-Ethereum
//!   transaction types — an OP-stack deposit (`0x7e`), an Arbitrum retry (`0x6a`) —
//!   decode instead of failing the block, and chain-specific extras are captured.
//!   Everything read from them goes through the standard `Transaction` accessors,
//!   except the transaction type, which is kept as its raw `u8`.

use std::fmt;

use alloy_consensus::Transaction as ConsensusTransaction;
use alloy_json_rpc::RpcError;
use alloy_network::any::{AnyRpcBlock, AnyRpcTransaction, AnyTransactionReceipt};
use alloy_network::eip2718::Typed2718 as _;
use alloy_network::{AnyNetwork, TransactionResponse};
use alloy_primitives::{B256, Bloom};
use alloy_provider::{Provider, RootProvider, WsConnect};
use alloy_pubsub::PubSubConnect;
use alloy_rpc_client::{ClientBuilder, RpcClient};
use alloy_rpc_types_eth::Filter;
use alloy_rpc_types_eth::Log as RpcLog;
use alloy_transport::{TransportError, TransportErrorKind};
use futures_util::StreamExt;
use serde::Deserialize;
use tracing::warn;

use super::retry::RetryLayer;
use super::{BlockSource, Datasets, FetchedBlock, HeadStream, METHOD_NOT_FOUND, SourceError};
use crate::wire::datasets::evm::{Block, Log, Transaction};
use crate::wire::envelope::{BlockMeta, ChainId, Event};

/// The most calls one JSON-RPC batch carries.
///
/// Providers limit requests per second and count each call in a batch against that
/// limit. Over it, the batch comes back with its first calls answered and the rest
/// refused, so a batch larger than the limit fails the same way however often it is
/// retried. `mainnet.base.org` and a Chainstack plan both refused every call past the
/// 25th in one batch; ten leaves room for pacing and the requests around it. The
/// per-transaction receipt fallback requests hashes in chunks this size, and a ranged
/// backfill fetches at most this many heights at once: their headers, with one log query.
const BATCH_LIMIT: usize = 10;

/// The method labels these calls are reported under, shared by the fetch path and
/// the projection so an error names the same boundary wherever it is raised.
const BLOCK: &str = "eth_getBlockByNumber";
const RECEIPTS: &str = "eth_getBlockReceipts";
const LOGS: &str = "eth_getLogs";

/// Where the selected logs come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogSource {
    /// Logs are not selected.
    Skip,
    /// The receipts fetched for transactions, which already carry every log.
    Receipts,
    /// A separate `eth_getLogs` for the height.
    GetLogs,
}

/// The RPC calls one height needs, decided once from the dataset selection.
///
/// This is the whole fetch policy; the fetch path and the projection read it rather
/// than the selection, so the table below is the one place the choice is made:
///
/// | Selected | Block read | Receipts | Logs from | Buried backfill |
/// | --- | --- | --- | --- | --- |
/// | `logs` | none when a live head matches, else hashes only | — | `eth_getLogs` | ranged |
/// | `blocks` | hashes only | — | — | ranged |
/// | `blocks`, `logs` | hashes only | — | `eth_getLogs` | ranged |
/// | `transactions`, with any others | full | `eth_getBlockReceipts` | receipts | per height |
///
/// Receipts are fetched only for transactions, because each transaction row is joined
/// to its receipt. A node without `eth_getBlockReceipts` is answered per transaction,
/// which is a fallback inside the receipts step rather than another plan.
///
/// A plan is ranged when every selected dataset can be served for a range of heights at
/// once: headers as one batch, logs as one `eth_getLogs` over the range. Receipts are per
/// block, so `transactions` is never ranged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FetchPlan {
    /// Project the block row. Its header is read whenever the plan reads a block.
    block_row: bool,
    /// Read full transaction objects and the block's receipts, and project each
    /// transaction joined to its receipt.
    transactions: bool,
    /// Where the logs come from, if they are selected.
    logs: LogSource,
    /// A live notification for the height stands in for the block read, because
    /// nothing selected reads the body.
    reuse_head: bool,
}

impl FetchPlan {
    const fn new(datasets: Datasets) -> Self {
        let logs = match (datasets.logs, datasets.transactions) {
            (false, _) => LogSource::Skip,
            (true, true) => LogSource::Receipts,
            (true, false) => LogSource::GetLogs,
        };
        Self {
            block_row: datasets.blocks,
            transactions: datasets.transactions,
            logs,
            reuse_head: datasets.logs && !datasets.blocks && !datasets.transactions,
        }
    }

    /// Whether buried backfill fetches heights a range at a time: whenever transactions,
    /// whose receipts are per block, are not selected.
    const fn ranged(self) -> bool {
        !self.transactions
    }
}

/// A source that talks to one EVM chain: blocks over a JSON-RPC client, live heads
/// over a subscription connection `C`.
///
/// A deployment reads over HTTP ([`http_client`]) and subscribes over WebSocket
/// ([`ws_connect`]); a simulation passes in-memory ones instead.
#[derive(Debug)]
pub struct EvmSource<C = WsConnect> {
    chain: ChainId,
    heads: C,
    provider: RootProvider<AnyNetwork>,
    plan: FetchPlan,
}

impl<C> EvmSource<C> {
    /// Builds a source for `chain` that fetches `datasets` over `http` and subscribes
    /// to heads through `heads`, which is connected afresh for each subscription.
    ///
    /// A live notification's metadata stands in for the header only on the logs-only
    /// live path; every other dataset still reads the block body, and the header is
    /// what supplies linkage when no notification did.
    pub fn new(chain: impl Into<ChainId>, http: RpcClient, heads: C, datasets: Datasets) -> Self {
        Self {
            chain: chain.into(),
            heads,
            provider: RootProvider::new(http),
            plan: FetchPlan::new(datasets),
        }
    }
}

/// The JSON-RPC client a deployment reads blocks over: HTTP to `url`.
///
/// Requests are paced and retried to fit the provider's rate limit, which is learned
/// from its refusals rather than configured.
///
/// # Errors
///
/// Returns a typed error if the HTTP endpoint or TLS client cannot be built.
pub fn http_client(url: &str) -> Result<RpcClient, SourceError> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;
    let url = url.parse().map_err(|source| SourceError::Transport {
        context: "HTTP endpoint",
        source: TransportErrorKind::custom(source),
    })?;
    // Jitter only has to differ between processes, so the OS-keyed hasher seeds it.
    let seed = std::hash::BuildHasher::hash_one(&std::hash::RandomState::new(), ());
    Ok(ClientBuilder::default()
        .layer(RetryLayer::new(seed))
        .http_with_client(client, url))
}

/// The connection a deployment subscribes to heads over: WebSocket to `url`.
///
/// Alloy reconnects and reissues active subscriptions; retries are bounded per
/// disconnect, rather than hiding an unavailable endpoint forever.
#[must_use]
pub fn ws_connect(url: &str) -> WsConnect {
    WsConnect::new(url)
        .with_max_retries(3)
        .with_retry_interval(std::time::Duration::from_secs(1))
}

/// A block's header fields a `newHeads` notification carries: identity, parent
/// linkage, and timestamp. Every other header field is ignored here.
#[derive(Debug, Deserialize)]
struct RpcBlockMeta {
    // A quantity on the wire; alloy's `quantity` handles the `0x` hex form and
    // rejects anything else. `B256`'s own `Deserialize` parses the hash.
    #[serde(with = "alloy_serde::quantity")]
    number: u64,
    hash: B256,
    #[serde(rename = "parentHash")]
    parent_hash: B256,
    #[serde(with = "alloy_serde::quantity")]
    timestamp: u64,
}

impl From<RpcBlockMeta> for BlockMeta {
    fn from(block: RpcBlockMeta) -> Self {
        Self {
            height: block.number,
            hash: block.hash,
            parent_hash: block.parent_hash,
            timestamp: block.timestamp,
        }
    }
}

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

fn malformed(context: &str, detail: impl fmt::Display) -> SourceError {
    SourceError::Malformed {
        context: context.to_owned(),
        detail: detail.to_string(),
    }
}

fn inconsistent(context: &str, detail: impl fmt::Display) -> SourceError {
    SourceError::Inconsistent {
        context: context.to_owned(),
        detail: detail.to_string(),
    }
}

fn invalid_json(context: &'static str, source: serde_json::Error) -> SourceError {
    SourceError::Json { context, source }
}

/// The error for a failed call at `context`, without the request URL a reqwest error
/// carries: a provider's URL usually holds its API key, and these errors are logged.
fn transport(context: &'static str, source: TransportError) -> SourceError {
    let source = match source {
        RpcError::Transport(TransportErrorKind::Custom(error)) => RpcError::Transport(
            TransportErrorKind::Custom(match error.downcast::<reqwest::Error>() {
                Ok(error) => Box::new(error.without_url()),
                Err(error) => error,
            }),
        ),
        other => other,
    };
    SourceError::Transport { context, source }
}

/// The results one batched block request carries, still in alloy's RPC types
/// and not yet projected into [`crate::wire::datasets`] records.
///
/// The block and receipts use alloy's *catch-all* (`any`) types, so a chain's
/// non-Ethereum transaction types — an OP-stack deposit (`0x7e`), an Arbitrum
/// retry (`0x6a`) — decode instead of failing the whole block. Everything this
/// source reads is reachable through the standard `Transaction`/`BlockHeader`
/// accessors; only the transaction type falls back to a raw `u8`.
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
    /// a [`METHOD_NOT_FOUND`] error and others with a `null` result, so both are
    /// treated the same way.
    pub receipts: Option<Vec<AnyTransactionReceipt>>,
    /// Logs from `eth_getLogs` for this height.
    ///
    /// `None` when logs are taken from [`Self::receipts`] instead, which is the
    /// case whenever transactions, and so receipts, were fetched.
    pub logs: Option<Vec<RpcLog>>,
}

/// Turns a parsed batch into ordered dataset events and its metadata.
///
/// Projects every row in the batch, whatever a deployment would have selected: the
/// caller filters with [`Datasets::keeps`] if it wants
/// less. Order is the block row, then each transaction followed by its logs, all in
/// index order, so a consumer sees every transaction before its logs. A body is only
/// required for the rows that read one, so a logs-only block with no fetched block
/// still decodes.
///
/// # Errors
///
/// Returns [`SourceError::Malformed`] when a body the rows read is absent, the
/// receipts do not belong to the block, or they do not line up with its transactions.
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
fn project(
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

    let mut events = Vec::new();
    if let Some(block) = block {
        let header = &block.0.inner.header;
        if header.number != meta.height || header.hash != meta.hash {
            return Err(malformed(
                BLOCK,
                format!("block {} at {} is not {meta:?}", header.hash, header.number),
            ));
        }
        if plan.block_row {
            events.push(Event::Block(Box::new(decode_header(block))));
        }
        append_body(&mut events, block, receipts, plan, meta)?;
    }
    if let Some(logs) = logs {
        if logs.is_empty()
            && let Some(block) = block
            && block.0.inner.header.inner.logs_bloom != Bloom::ZERO
        {
            return Err(inconsistent(
                LOGS,
                "no logs returned for a block whose logsBloom is set",
            ));
        }
        for log in logs {
            events.push(Event::Log(Box::new(get_logs_record(
                log,
                meta.height,
                meta.hash,
                meta.timestamp,
            )?)));
        }
    }
    Ok(events)
}

/// Appends the transaction and log rows a fetched body and its receipts carry.
///
/// Both come from the receipts, which are fetched only alongside transactions, so a
/// body read for the block row alone appends nothing here.
fn append_body(
    events: &mut Vec<Event>,
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
    if receipts.len() != transactions.len() {
        return Err(malformed(
            RECEIPTS,
            format!(
                "{} receipts for {} transactions",
                receipts.len(),
                transactions.len()
            ),
        ));
    }
    for (position, (transaction, receipt)) in transactions.iter().zip(receipts).enumerate() {
        let tx_index = position as u64;
        let receipt_block = receipt.block_hash.unwrap_or_default();
        if receipt_block != meta.hash {
            return Err(inconsistent(
                RECEIPTS,
                format!(
                    "receipt belongs to block {receipt_block}, not {}; the chain reorganised mid-request",
                    meta.hash
                ),
            ));
        }
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
        events.push(Event::Transaction(Box::new(decode_transaction(
            transaction,
            receipt,
            tx_index,
            meta,
        ))));
        if plan.logs == LogSource::Receipts {
            for log in receipt.logs() {
                // Logs nested in this receipt, as opposed to a separate `eth_getLogs`
                // query; the label names the boundary an error at this site reports.
                events.push(Event::Log(Box::new(log_record(
                    log,
                    tx_index,
                    meta.height,
                    meta.hash,
                    meta.timestamp,
                    RECEIPTS,
                )?)));
            }
        }
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

/// Splits one range's headers and `eth_getLogs` rows into a block per height, in order.
///
/// `headers` are the range's headers in height order, starting at `from`. Each height is
/// projected exactly as a single block would be, so the same checks apply: each log's
/// block hash against its header, and an empty answer against the header's bloom. The
/// range adds its own: every log belongs to a requested height, in ascending
/// `(blockNumber, logIndex)` order. Logs out of order are rejected rather than sorted,
/// since a node that misorders them is not one to trust with the rest of the answer.
fn split_range(
    from: u64,
    headers: &[AnyRpcBlock],
    logs: Option<&[RpcLog]>,
    plan: FetchPlan,
) -> Result<Vec<FetchedBlock>, SourceError> {
    let mut rest = logs;
    let mut blocks = Vec::with_capacity(headers.len());
    for (height, header) in (from..).zip(headers) {
        let meta = BlockMeta::from(header);
        if meta.height != height {
            return Err(malformed(
                BLOCK,
                format!("requested block {height}, received {}", meta.height),
            ));
        }
        // In order, this height's logs are the next run carrying its number.
        let own = match rest {
            Some(logs) => {
                let count = logs
                    .iter()
                    .take_while(|log| log.block_number == Some(height))
                    .count();
                let (own, after) = logs.split_at(count);
                if own
                    .windows(2)
                    .any(|pair| pair[0].log_index >= pair[1].log_index)
                {
                    return Err(malformed(
                        LOGS,
                        format!("logs of block {height} are not in ascending logIndex order"),
                    ));
                }
                rest = Some(after);
                Some(own)
            }
            None => None,
        };
        blocks.push(FetchedBlock {
            meta,
            events: project(meta, Some(header), None, own, plan)?,
        });
    }
    if let Some(log) = rest.and_then(<[RpcLog]>::first) {
        return Err(malformed(
            LOGS,
            format!(
                "log at block {:?} is outside the range or out of order",
                log.block_number
            ),
        ));
    }
    Ok(blocks)
}

/// Flattens a block header and its transaction hashes into the `blocks` dataset.
///
/// Two header fields the node returns are not carried, because the dataset has no column
/// for either and a half-present field is worse than an absent one: `mix_hash`, which
/// post-merge is EIP-4788's `prevRandao`, and the pre-Byzantium `stateRoot` a receipt
/// may carry instead of a `status` (alloy's `coerce_status` maps that variant to
/// `true`, so a pre-Byzantium failure would read as a success). A consumer needing them
/// reads the header and the receipt.
fn decode_header(block: &AnyRpcBlock) -> Block {
    let header = &block.0.inner.header;
    let transactions = &block.0.inner.transactions;
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
        receipt_status: receipt.inner.inner.status(),
        receipt_gas_used: receipt.gas_used,
        receipt_cumulative_gas_used: receipt.inner.inner.cumulative_gas_used(),
        receipt_effective_gas_price: receipt.effective_gas_price,
        receipt_contract_address: receipt.contract_address,
        receipt_logs_bloom: receipt.inner.inner.bloom(),
        receipt_blob_gas_used: receipt.blob_gas_used,
        receipt_blob_gas_price: receipt.blob_gas_price,
        log_count: receipt.logs().len() as u64,
        block_timestamp: meta.timestamp,
        block_number: meta.height,
        block_hash: meta.hash,
    }
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
            .get_deserialized::<alloy_primitives::U128>("maxFeePerGas")
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

impl<C: PubSubConnect + Clone> BlockSource for EvmSource<C> {
    fn chain(&self) -> &ChainId {
        &self.chain
    }

    async fn subscribe_heads(&self) -> Result<HeadStream, SourceError> {
        let client = ClientBuilder::default()
            .pubsub(self.heads.clone())
            .await
            .map_err(|source| transport("websocket connect", source))?;
        let provider = RootProvider::<AnyNetwork>::new(client);
        // Identity, parent hash, and timestamp, so a fetch for this height can reuse
        // them instead of reading the same header again over HTTP. Subscribe through
        // Alloy's client so the typed result stream retains malformed notifications
        // rather than dropping them.
        let subscription = provider
            .subscribe::<_, RpcBlockMeta>(("newHeads",))
            .await
            .map_err(|source| transport("eth_subscribe", source))?;
        let heads = subscription.into_result_stream().map(move |head| {
            // Keep the client's frontend alive for as long as the stream lives.
            let _keep_alive = &provider;
            head.map(BlockMeta::from)
                .map_err(|source| invalid_json("newHeads", source))
        });
        Ok(Box::pin(heads) as HeadStream)
    }

    async fn fetch_block(
        &self,
        height: u64,
        head: Option<&BlockMeta>,
    ) -> Result<FetchedBlock, SourceError> {
        // A live notification for this height already carries the metadata, and when
        // the plan reads nothing else from the block body it stands in for the block
        // read entirely. A head for a different height is no use, so it is ignored
        // rather than mistrusted.
        // An empty answer is not trusted without the header's bloom, so it falls through.
        if let Some(meta) = head.filter(|meta| meta.height == height && self.plan.reuse_head) {
            let logs = self.fetch_logs(meta.hash).await?;
            if !logs.is_empty() {
                return Ok(FetchedBlock {
                    meta: *meta,
                    events: project(*meta, None, None, Some(&logs), self.plan)?,
                });
            }
        }
        if self.plan.ranged() {
            return self
                .fetch_range(height, height)
                .await?
                .pop()
                .ok_or_else(|| malformed(BLOCK, "block was not fetched"));
        }
        let (block, receipts) = self.fetch_batch(height).await?;
        // Nodes that do not serve `eth_getBlockReceipts` (some L2s, and pre-Cancun
        // Ethereum) answer it with `METHOD_NOT_FOUND` or null. Fetch the receipts
        // by transaction hash instead, so the block still becomes events.
        let receipts = match receipts {
            Some(receipts) => receipts,
            None => self.fetch_receipts(&block).await?,
        };
        let meta = BlockMeta::from(&block);
        Ok(FetchedBlock {
            meta,
            events: project(meta, Some(&block), Some(&receipts), None, self.plan)?,
        })
    }

    /// Fetches up to `BATCH_LIMIT` heights as one range when the plan is ranged, and
    /// one height otherwise.
    ///
    /// Logs per block vary widely, so a range a provider cannot serve is not the end: a
    /// transport failure — a refusal of the range's size, a timeout, or an error that
    /// outlasted the retry layer — is retried for the first half of the range, rounded
    /// up, down to one height, and only a single height's failure is returned. Each
    /// call starts again at the full size, since the next stretch of the chain may be
    /// quieter. A range that comes back but fails its checks is returned as is: a
    /// smaller range would not fix data the node got wrong.
    async fn fetch_blocks(&self, from: u64, to: u64) -> Result<Vec<FetchedBlock>, SourceError> {
        if !self.plan.ranged() {
            return Ok(vec![self.fetch_block(from, None).await?]);
        }
        let mut last = to.min(from.saturating_add(BATCH_LIMIT as u64 - 1));
        loop {
            match self.fetch_range(from, last).await {
                Err(error @ SourceError::Transport { .. }) if last > from => {
                    warn!(from, to = last, %error, "range fetch failed; retrying its first half");
                    last = from + (last - from) / 2;
                }
                result => return result,
            }
        }
    }

    async fn fetch_header(&self, height: Option<u64>) -> Result<BlockMeta, SourceError> {
        const CONTEXT: &str = "eth_getBlockByNumber(header)";
        let tag = height.map_or(
            alloy_rpc_types_eth::BlockNumberOrTag::Latest,
            alloy_rpc_types_eth::BlockNumberOrTag::Number,
        );
        // Hashes only, not full transactions: callers want an identity and the metadata
        // a backfill target needs, and a full-transaction response is tens of KB on a
        // busy chain for nothing.
        let header: Option<RpcBlockMeta> = self
            .provider
            .client()
            .request(BLOCK, (tag, false))
            .await
            .map_err(|source| transport(CONTEXT, source))?;
        header
            .map(BlockMeta::from)
            .ok_or_else(|| malformed(CONTEXT, format!("no block at {tag}")))
    }
}

impl<C> EvmSource<C> {
    /// Fetches the block's receipts one transaction at a time, in index order.
    ///
    /// The fallback for nodes without `eth_getBlockReceipts`. The hashes are requested
    /// in chunks of [`BATCH_LIMIT`], so no batch outruns a provider's per-second limit.
    async fn fetch_receipts(
        &self,
        block: &AnyRpcBlock,
    ) -> Result<Vec<AnyTransactionReceipt>, SourceError> {
        const RECEIPT: &str = "eth_getTransactionReceipt";
        let hashes: Vec<B256> = block.0.inner.transactions.hashes().collect();

        let mut receipts = Vec::with_capacity(hashes.len());
        for chunk in hashes.chunks(BATCH_LIMIT) {
            let mut batch = alloy_rpc_client::BatchRequest::new(self.provider.client());
            let mut calls = Vec::with_capacity(chunk.len());
            for hash in chunk {
                calls.push(
                    batch
                        .add_call::<_, Option<AnyTransactionReceipt>>(RECEIPT, &(*hash,))
                        .map_err(|source| transport(RECEIPT, source))?,
                );
            }
            batch
                .send()
                .await
                .map_err(|source| transport(RECEIPT, source))?;
            // Alloy correlates IDs; awaiting in request order preserves block order.
            for call in calls {
                receipts.push(
                    call.await
                        .map_err(|source| transport(RECEIPT, source))?
                        .ok_or_else(|| malformed(RECEIPT, "result was null"))?,
                );
            }
        }
        Ok(receipts)
    }

    /// Fetches one height's full block and its receipts in one batch: the per-height
    /// fetch of a plan with transactions.
    ///
    /// The receipts are `None` when the node does not serve `eth_getBlockReceipts`. Some
    /// nodes answer the method with a [`METHOD_NOT_FOUND`] error and others with a `null`
    /// result, so both are treated the same way.
    async fn fetch_batch(
        &self,
        height: u64,
    ) -> Result<(AnyRpcBlock, Option<Vec<AnyTransactionReceipt>>), SourceError> {
        let tag = alloy_rpc_types_eth::BlockNumberOrTag::Number(height);
        // In alloy 2.5.0 Provider::client() returns RpcClientInner; new_batch() is
        // only on RpcClient. This is its identical BatchRequest constructor.
        let mut batch = alloy_rpc_client::BatchRequest::new(self.provider.client());
        let block = batch
            .add_call::<_, Option<AnyRpcBlock>>(BLOCK, &(tag, true))
            .map_err(|source| transport(BLOCK, source))?;
        let receipts = batch
            .add_call::<_, Option<Vec<AnyTransactionReceipt>>>(RECEIPTS, &(tag,))
            .map_err(|source| transport(RECEIPTS, source))?;
        batch
            .send()
            .await
            .map_err(|source| transport("rpc batch", source))?;
        let block = block
            .await
            .map_err(|source| transport(BLOCK, source))?
            .ok_or_else(|| malformed(BLOCK, "result was null"))?;
        let receipts = match receipts.await {
            Ok(receipts) => receipts,
            Err(RpcError::ErrorResp(error)) if error.code == METHOD_NOT_FOUND => None,
            Err(source) => return Err(transport(RECEIPTS, source)),
        };
        Ok((block, receipts))
    }

    /// Fetches one height's logs, pinned to `hash` so a reorg cannot silently answer
    /// with the replacement branch: the query returns that block's logs or nothing.
    async fn fetch_logs(&self, hash: B256) -> Result<Vec<RpcLog>, SourceError> {
        let filter = Filter::new().at_block_hash(hash);
        self.provider
            .client()
            .request(LOGS, (filter,))
            .await
            .map_err(|source| transport(LOGS, source))
    }

    /// Fetches the heights `from ..= to` in one batch: each height's hashes-only header
    /// and, when logs are selected, one `eth_getLogs` over the range. One height is a
    /// range of one, so this is also the per-height fetch of a plan without
    /// transactions.
    async fn fetch_range(&self, from: u64, to: u64) -> Result<Vec<FetchedBlock>, SourceError> {
        let mut batch = alloy_rpc_client::BatchRequest::new(self.provider.client());
        let headers = (from..=to)
            .map(|height| {
                let tag = alloy_rpc_types_eth::BlockNumberOrTag::Number(height);
                batch.add_call::<_, Option<AnyRpcBlock>>(BLOCK, &(tag, false))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| transport(BLOCK, source))?;
        let logs = (self.plan.logs == LogSource::GetLogs)
            .then(|| {
                let filter = Filter::new().from_block(from).to_block(to);
                batch.add_call::<_, Vec<RpcLog>>(LOGS, &(filter,))
            })
            .transpose()
            .map_err(|source| transport(LOGS, source))?;
        batch
            .send()
            .await
            .map_err(|source| transport("rpc batch", source))?;
        let mut blocks = Vec::with_capacity(headers.len());
        for header in headers {
            blocks.push(
                header
                    .await
                    .map_err(|source| transport(BLOCK, source))?
                    .ok_or_else(|| malformed(BLOCK, "result was null"))?,
            );
        }
        let logs = match logs {
            Some(logs) => Some(logs.await.map_err(|source| transport(LOGS, source))?),
            None => None,
        };
        split_range(from, &blocks, logs.as_deref(), self.plan)
    }
}

/// Renders the source as its chain, for log fields.
impl<C> fmt::Display for EvmSource<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "evm[{}]", self.chain)
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use alloy_consensus::TxType;
    use alloy_network::any::AnyRpcBlock;
    use alloy_primitives::{Address, B256};
    use serde_json::{Value, json};

    use super::{EvmSource, RpcBatch, decode_block, http_client, ws_connect};
    use crate::ingest::source::Datasets;
    use crate::ingest::source::{BlockSource, SourceError};
    use crate::wire::envelope::BlockMeta;
    use crate::wire::envelope::Event;

    /// A source over real HTTP and WebSocket endpoints, as a deployment builds one.
    fn from_urls(
        chain: &str,
        http_url: impl AsRef<str>,
        ws_url: impl AsRef<str>,
        datasets: Datasets,
    ) -> Result<EvmSource, SourceError> {
        Ok(EvmSource::new(
            chain,
            http_client(http_url.as_ref())?,
            ws_connect(ws_url.as_ref()),
            datasets,
        ))
    }

    /// Matches the `block()` fixture's `timestamp`, so metadata and body agree.
    const TIMESTAMP: u64 = 0x6530_a1b0;

    /// A real HTTP boundary: inspect requests and return replies in reverse order.
    fn rpc_server(
        requests: usize,
        mut reply: impl FnMut(&Value) -> Value + Send + 'static,
    ) -> (String, std::thread::JoinHandle<Vec<Value>>) {
        use std::io::{Read as _, Write as _};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind RPC server");
        let url = format!("http://{}", listener.local_addr().expect("server address"));
        let task = std::thread::spawn(move || {
            let mut observed = Vec::new();
            for _ in 0..requests {
                let (mut socket, _) = listener.accept().expect("accept RPC");
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .expect("read deadline");
                let mut bytes = Vec::new();
                let mut byte = [0];
                while !bytes.ends_with(b"\r\n\r\n") {
                    socket.read_exact(&mut byte).expect("HTTP header");
                    bytes.push(byte[0]);
                }
                let headers = std::str::from_utf8(&bytes).expect("HTTP headers");
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().expect("length"))
                    })
                    .expect("content length");
                let mut body = vec![0; length];
                socket.read_exact(&mut body).expect("RPC body");
                let request: Value = serde_json::from_slice(&body).expect("RPC JSON");
                let response = reply(&request).to_string();
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).expect("RPC response");
                observed.push(request);
            }
            observed
        });
        (url, task)
    }

    fn primary_reply(request: &Value, block: &Value, receipts: &Value) -> Value {
        let calls = request.as_array().expect("primary request must be a batch");
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0]["method"], "eth_getBlockByNumber");
        assert_eq!(calls[0]["params"], json!(["0x112a880", true]));
        assert_eq!(calls[1]["method"], "eth_getBlockReceipts");
        assert_eq!(calls[1]["params"], json!(["0x112a880"]));
        json!([
            {"jsonrpc": "2.0", "id": calls[0]["id"], "result": block},
            {"jsonrpc": "2.0", "id": calls[1]["id"], "result": receipts},
        ])
    }

    #[tokio::test]
    async fn primary_fetch_is_one_two_call_batch_with_reversed_responses() {
        let (url, server) = rpc_server(1, |request| primary_reply(request, &block(), &receipts()));
        let source = from_urls("ethereum", url, "ws://unused", Datasets::all()).expect("source");
        let fetched = source.fetch_block(18_000_000, None).await.expect("fetch");
        assert_eq!(fetched.events.len(), 5);
        // The header is reported as metadata, and no finalized call is made.
        assert_eq!(fetched.meta.height, 18_000_000);
        assert_eq!(fetched.meta.timestamp, TIMESTAMP);
        assert_eq!(server.join().expect("server").len(), 1);
    }

    /// Reads a hex quantity out of a request.
    fn quantity(value: &Value) -> u64 {
        let digits = value.as_str().expect("quantity").trim_start_matches("0x");
        u64::from_str_radix(digits, 16).expect("quantity")
    }

    /// A hashes-only header at `height`, on a chain whose block at height `h` hashes to
    /// `hash(h)`, with an empty bloom. Heights stay below 256 so each has its own hash.
    fn header_at(height: u8) -> Value {
        let mut header = block();
        header["number"] = json!(format!("0x{height:x}"));
        header["hash"] = json!(hash(height));
        header["parentHash"] = json!(hash(height - 1));
        header["transactions"] = json!([]);
        header
    }

    /// Log `index` of the block at `height`, as `eth_getLogs` returns it.
    fn log_at(height: u8, index: u8) -> Value {
        let mut log = receipts()[0]["logs"][0].clone();
        log["blockNumber"] = json!(format!("0x{height:x}"));
        log["blockHash"] = json!(hash(height));
        log["logIndex"] = json!(format!("0x{index:x}"));
        log
    }

    /// The `eth_getLogs` call in a range's batch.
    fn log_query(request: &Value) -> &Value {
        let calls = request.as_array().expect("a range is one batch");
        calls
            .iter()
            .find(|call| call["method"] == "eth_getLogs")
            .expect("a log query")
    }

    /// How many heights a range's log query spans.
    fn span(request: &Value) -> u64 {
        let filter = &log_query(request)["params"][0];
        quantity(&filter["toBlock"]) - quantity(&filter["fromBlock"]) + 1
    }

    /// Answers a range's batch: each requested header, its bloom set at `bloom_at`, and
    /// `logs` as given, so a test controls exactly what the node got wrong.
    fn range_reply(request: &Value, logs: &[Value], bloom_at: Option<u8>) -> Value {
        let calls = request.as_array().expect("a range is one batch");
        Value::Array(
            calls
                .iter()
                .map(|call| {
                    let result = if call["method"] == "eth_getLogs" {
                        json!(logs)
                    } else {
                        assert_eq!(call["method"], "eth_getBlockByNumber");
                        assert_eq!(call["params"][1], false, "a range reads hashes only");
                        let height = u8::try_from(quantity(&call["params"][0])).expect("height");
                        let mut header = header_at(height);
                        if bloom_at == Some(height) {
                            header["logsBloom"] = json!(format!("0x80{}", "00".repeat(255)));
                        }
                        header
                    };
                    json!({"jsonrpc": "2.0", "id": call["id"], "result": result})
                })
                .collect(),
        )
    }

    fn logs_only(url: String) -> EvmSource {
        from_urls(
            "base",
            url,
            "ws://unused",
            serde_json::from_str(r#"["logs"]"#).expect("datasets"),
        )
        .expect("source")
    }

    /// A range is one batch — a header per height and one `eth_getLogs` — split back into
    /// a block per height, an empty height included, with each block's events in the
    /// order a single height's fetch emits them.
    #[tokio::test]
    async fn a_range_is_one_batch_split_per_height() {
        for (selection, expected) in [
            (r#"["logs"]"#, [&["log", "log"][..], &[], &["log"]]),
            (
                r#"["blocks", "logs"]"#,
                [&["block", "log", "log"][..], &["block"], &["block", "log"]],
            ),
        ] {
            let logs = [log_at(100, 0), log_at(100, 1), log_at(102, 0)];
            let (url, server) = rpc_server(1, move |request| range_reply(request, &logs, None));
            let source = from_urls(
                "base",
                url,
                "ws://unused",
                serde_json::from_str(selection).expect("datasets"),
            )
            .expect("source");
            let blocks = source.fetch_blocks(100, 102).await.expect("range");
            let observed = server.join().expect("server");
            assert_eq!(observed[0].as_array().expect("batch").len(), 4);
            assert_eq!(span(&observed[0]), 3);
            assert_eq!(blocks.len(), 3);
            for ((height, block), kinds) in (100_u8..).zip(&blocks).zip(expected) {
                assert_eq!(block.meta.height, u64::from(height));
                assert_eq!(block.meta.hash, hash(height).parse::<B256>().expect("hash"));
                let got: Vec<&str> = block.events.iter().map(Event::kind).collect();
                assert_eq!(got, kinds, "{selection} at {height}");
            }
        }
    }

    /// Without transactions, one height is a range of one: the tail and live paths send
    /// the same single batch.
    #[tokio::test]
    async fn one_height_without_transactions_is_a_range_of_one() {
        let logs = [log_at(100, 0)];
        let (url, server) = rpc_server(1, move |request| range_reply(request, &logs, None));
        let fetched = logs_only(url).fetch_block(100, None).await.expect("fetch");
        let observed = server.join().expect("server");
        assert_eq!(observed[0].as_array().expect("batch").len(), 2);
        assert_eq!(span(&observed[0]), 1);
        assert_eq!(fetched.meta.height, 100);
        assert_eq!(fetched.events.len(), 1);
    }

    /// A reused head's empty `eth_getLogs` answer falls through to the header read, which
    /// checks it against the bloom.
    #[tokio::test]
    async fn a_reused_head_with_no_logs_reads_the_header() {
        let head = BlockMeta {
            height: 100,
            hash: hash(100).parse().expect("hash"),
            parent_hash: hash(99).parse().expect("hash"),
            timestamp: TIMESTAMP,
        };
        for (logs, bloom_at, requests) in [
            (vec![log_at(100, 0)], None, 1),
            (vec![], None, 2),
            (vec![], Some(100), 2),
        ] {
            let (url, server) = rpc_server(requests, move |request| {
                if request.is_array() {
                    return range_reply(request, &logs, bloom_at);
                }
                assert_eq!(request["params"][0]["blockHash"], json!(hash(100)));
                json!({"jsonrpc": "2.0", "id": request["id"], "result": logs})
            });
            let fetched = logs_only(url).fetch_block(100, Some(&head)).await;
            assert_eq!(server.join().expect("server").len(), requests);
            match bloom_at {
                None => assert_eq!(fetched.expect("fetch").meta, head),
                Some(_) => assert!(matches!(fetched, Err(SourceError::Inconsistent { .. }))),
            }
        }
    }

    /// A range the provider refuses is retried for its first half, rounded up, down to
    /// one height, and only a single height's failure is returned. The first attempt is
    /// capped at the batch limit, however far the caller allows.
    #[tokio::test]
    async fn a_refused_range_is_retried_in_halves_down_to_one_height() {
        for (largest, spans, served) in [
            (2, vec![10, 5, 3, 2], Some(2)),
            (0, vec![10, 5, 3, 2, 1], None),
        ] {
            let (url, server) = rpc_server(spans.len(), move |request| {
                let mut reply = range_reply(request, &[], None);
                if span(request) > largest {
                    let id = &log_query(request)["id"];
                    for response in reply.as_array_mut().expect("batch") {
                        if response["id"] == *id {
                            *response = json!({"jsonrpc": "2.0", "id": id, "error":
                                {"code": -32602, "message": "query exceeds the maximum range"}});
                        }
                    }
                }
                reply
            });
            let result = logs_only(url).fetch_blocks(100, 200).await;
            let observed = server.join().expect("server");
            assert_eq!(observed.iter().map(span).collect::<Vec<_>>(), spans);
            match served {
                Some(count) => assert_eq!(result.expect("a smaller range").len(), count),
                None => assert!(matches!(
                    result,
                    Err(SourceError::Transport {
                        context: "eth_getLogs",
                        ..
                    })
                )),
            }
        }
    }

    /// A range that comes back but breaks a range check fails as it is: a smaller range
    /// would not fix data the node got wrong.
    #[tokio::test]
    async fn a_range_that_fails_its_checks_is_not_retried_smaller() {
        let mut wrong_hash = log_at(101, 0);
        wrong_hash["blockHash"] = json!(hash(0xee));
        for (case, logs, bloom_at) in [
            ("a log from another block", vec![wrong_hash], None),
            (
                "logs out of order",
                vec![log_at(101, 1), log_at(101, 0)],
                None,
            ),
            (
                "a duplicate log",
                vec![log_at(101, 0), log_at(101, 0)],
                None,
            ),
            ("a log below the range", vec![log_at(99, 0)], None),
            ("a log above the range", vec![log_at(103, 0)], None),
            ("no logs for a header whose bloom is set", vec![], Some(101)),
        ] {
            let (url, server) = rpc_server(1, move |request| range_reply(request, &logs, bloom_at));
            let error = logs_only(url).fetch_blocks(100, 102).await.expect_err(case);
            assert!(
                matches!(
                    error,
                    SourceError::Malformed { .. } | SourceError::Inconsistent { .. }
                ),
                "{case}: {error}"
            );
            assert_eq!(server.join().expect("server").len(), 1, "{case}");
        }
    }

    /// Transactions come with their receipts, which are per block, so a transaction
    /// dataset backfills a height at a time.
    #[tokio::test]
    async fn a_transaction_dataset_backfills_one_height_at_a_time() {
        let (url, server) = rpc_server(1, |request| primary_reply(request, &block(), &receipts()));
        let source = from_urls("ethereum", url, "ws://unused", Datasets::all()).expect("source");
        let blocks = source
            .fetch_blocks(18_000_000, 18_000_009)
            .await
            .expect("fetch");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].meta.height, 18_000_000);
        server.join().expect("server");
    }

    /// A reusable head stands in for the header only when logs are the only thing
    /// fetched. Anything else reads the block body, so a `transactions` dataset still
    /// fetches it and still emits its rows — silently dropping them would index
    /// transactions for history and not for live blocks.
    #[tokio::test]
    async fn a_reusable_head_still_reads_the_body_for_a_transaction_dataset() {
        // One batch, two calls: the body is read even though a head was announced.
        let (url, server) = rpc_server(1, |request| primary_reply(request, &block(), &receipts()));
        let source = from_urls(
            "ethereum",
            url,
            "ws://unused",
            serde_json::from_str(r#"["transactions", "logs"]"#).expect("datasets"),
        )
        .expect("source");
        let head = BlockMeta {
            height: 18_000_000,
            hash: hash(0xab).parse().expect("hash"),
            parent_hash: hash(0xaa).parse().expect("hash"),
            timestamp: TIMESTAMP,
        };
        let fetched = source
            .fetch_block(18_000_000, Some(&head))
            .await
            .expect("fetch");
        let kinds: Vec<&str> = fetched.events.iter().map(Event::kind).collect();
        assert_eq!(
            kinds,
            ["transaction", "log", "log", "transaction"],
            "the body rows survive a reusable head"
        );
        server.join().expect("server");
    }

    #[tokio::test]
    async fn receipt_fallback_batches_21_transactions_as_10_10_1() {
        for unsupported in [false, true] {
            let mut block = block();
            let template = block["transactions"][0].clone();
            let mut receipt = receipts()[0].clone();
            receipt["logs"] = json!([]);
            block["transactions"] = Value::Array(
                (0..21_u8)
                    .map(|index| {
                        let mut transaction = template.clone();
                        transaction["hash"] = json!(hash(index));
                        transaction["transactionIndex"] = json!(format!("0x{index:x}"));
                        transaction
                    })
                    .collect(),
            );
            let (url, server) = rpc_server(4, move |request| {
                let calls = request.as_array().expect("batch");
                if calls[0]["method"] == "eth_getBlockByNumber" {
                    let mut response = primary_reply(request, &block, &Value::Null);
                    if unsupported {
                        response[1]
                            .as_object_mut()
                            .expect("response")
                            .remove("result");
                        response[1]["error"] = json!({"code": -32601, "message": "unsupported"});
                    }
                    response
                } else {
                    Value::Array(
                        calls
                            .iter()
                            .rev()
                            .map(|call| {
                                assert_eq!(call["method"], "eth_getTransactionReceipt");
                                let hash = call["params"][0].as_str().expect("hash");
                                let index = u8::from_str_radix(&hash[2..4], 16).expect("index");
                                let mut receipt = receipt.clone();
                                receipt["transactionHash"] = json!(hash);
                                receipt["transactionIndex"] = json!(format!("0x{index:x}"));
                                json!({"jsonrpc": "2.0", "id": call["id"], "result": receipt})
                            })
                            .collect(),
                    )
                }
            });
            let source = from_urls("base", url, "ws://unused", Datasets::all()).expect("source");
            let fetched = source
                .fetch_block(18_000_000, None)
                .await
                .expect("fallback fetch");
            assert_eq!(fetched.events.len(), 22);
            let observed = server.join().expect("server");
            assert_eq!(
                observed
                    .iter()
                    .map(|batch| batch.as_array().expect("batch").len())
                    .collect::<Vec<_>>(),
                [2, 10, 10, 1]
            );
            for (index, event) in fetched.events.iter().skip(1).enumerate() {
                let Event::Transaction(transaction) = event else {
                    panic!("transaction");
                };
                assert_eq!(transaction.transaction_index, index as u64);
                assert_eq!(
                    transaction.hash,
                    B256::repeat_byte(u8::try_from(index).expect("index"))
                );
            }
        }
    }

    #[tokio::test]
    async fn rpc_and_decode_errors_remain_typed_and_do_not_trigger_fallback() {
        for malformed in [false, true] {
            let (url, server) = rpc_server(1, move |request| {
                let mut response = primary_reply(request, &block(), &receipts());
                if malformed {
                    response[0]["result"]["number"] = json!("not a quantity");
                } else {
                    response[1]
                        .as_object_mut()
                        .expect("response")
                        .remove("result");
                    response[1]["error"] = json!({"code": -32000, "message": "internal"});
                }
                response
            });
            let source =
                from_urls("ethereum", url, "ws://unused", Datasets::all()).expect("source");
            let error = source
                .fetch_block(18_000_000, None)
                .await
                .expect_err("must fail");
            if malformed {
                assert!(matches!(
                    error,
                    SourceError::Transport {
                        context: "eth_getBlockByNumber",
                        source: alloy_json_rpc::RpcError::DeserError { .. }
                    }
                ));
            } else {
                assert!(
                    matches!(error, SourceError::Transport { context: "eth_getBlockReceipts", source: alloy_json_rpc::RpcError::ErrorResp(ref payload) } if payload.code == -32000)
                );
            }
            server.join().expect("server");
        }
    }

    #[tokio::test]
    async fn head_stream_retains_provider_decode_errors_and_resubscribes() {
        use futures_util::{SinkExt as _, StreamExt as _};
        use tokio_tungstenite::tungstenite::Message;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("WS bind");
        let url = format!("ws://{}", listener.local_addr().expect("WS address"));
        let (release, done) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            for index in 0..2 {
                let (socket, _) = listener.accept().await.expect("WS accept");
                let mut socket = tokio_tungstenite::accept_async(socket)
                    .await
                    .expect("WS handshake");
                let request = socket
                    .next()
                    .await
                    .expect("subscribe frame")
                    .expect("subscribe read");
                let request: Value =
                    serde_json::from_str(request.to_text().expect("text")).expect("subscribe JSON");
                assert_eq!(request["method"], "eth_subscribe");
                assert_eq!(request["params"], json!(["newHeads"]));
                let id = format!("0x{:064x}", index + 1);
                socket
                    .send(Message::text(
                        json!({"jsonrpc": "2.0", "id": request["id"], "result": id}).to_string(),
                    ))
                    .await
                    .expect("subscribe reply");
                if index == 0 {
                    socket.send(Message::text(json!({"jsonrpc": "2.0", "method": "eth_subscription", "params": {"subscription": id, "result": {"number": "invalid", "hash": hash(1), "parentHash": hash(0), "timestamp": "0x1"}}}).to_string())).await.expect("malformed head");
                }
                socket.send(Message::text(json!({"jsonrpc": "2.0", "method": "eth_subscription", "params": {"subscription": id, "result": {"number": format!("0x{:x}", index + 16), "hash": hash(1), "parentHash": hash(0), "timestamp": format!("0x{:x}", index + 1)}}}).to_string())).await.expect("head");
                if index == 0 {
                    socket.close(None).await.expect("close for reconnect");
                } else {
                    done.await.expect("hold connection until consumed");
                    break;
                }
            }
        });
        let source = from_urls("ethereum", "http://unused", url, Datasets::all()).expect("source");
        let mut heads = source.subscribe_heads().await.expect("subscribe");
        drop(source);
        let consume = async {
            assert!(matches!(
                heads.next().await.expect("decode error"),
                Err(SourceError::Json {
                    context: "newHeads",
                    ..
                })
            ));
            assert_eq!(
                heads
                    .next()
                    .await
                    .expect("head")
                    .expect("valid head")
                    .height,
                16
            );
            assert_eq!(
                heads
                    .next()
                    .await
                    .expect("resubscribed head")
                    .expect("valid head")
                    .height,
                17
            );
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), consume)
            .await
            .expect("head/reconnect deadline");
        release.send(()).expect("release server");
        server.await.expect("server");
    }

    fn hash(byte: u8) -> String {
        format!("0x{}", format!("{byte:02x}").repeat(32))
    }

    fn address(byte: u8) -> String {
        format!("0x{}", format!("{byte:02x}").repeat(20))
    }

    fn bloom() -> String {
        format!("0x{}", "00".repeat(256))
    }

    /// A block as `eth_getBlockByNumber` returns it with full transactions, so it
    /// carries every header field the `blocks` dataset reads.
    fn block() -> Value {
        json!({
            "number": "0x112a880",
            "hash": hash(0xab),
            "parentHash": hash(0xaa),
            "timestamp": "0x6530a1b0",
            "nonce": "0x0000000000000042",
            "sha3Uncles": hash(0x00),
            "logsBloom": bloom(),
            "transactionsRoot": hash(0x01),
            "stateRoot": hash(0x02),
            "receiptsRoot": hash(0x03),
            "miner": address(0x11),
            "difficulty": "0x0",
            "totalDifficulty": "0x0",
            "extraData": "0xbeef",
            "size": "0x220",
            "gasLimit": "0x1c9c380",
            "gasUsed": "0x5208",
            "mixHash": hash(0x00),
            "baseFeePerGas": "0x4c4b40",
            "withdrawalsRoot": hash(0x00),
            "blobGasUsed": "0x0",
            "excessBlobGas": "0x0",
            "parentBeaconBlockRoot": hash(0x04),
            "uncles": [hash(0x99)],
            "transactions": [
                {
                    "type": "0x2", "chainId": "0x2105", "nonce": "0x1",
                    "hash": hash(0x11), "blockHash": hash(0xab),
                    "blockNumber": "0x112a880", "transactionIndex": "0x0",
                    "from": address(0xf0), "to": address(0xf1), "value": "0x0",
                    "gas": "0x5208", "gasPrice": "0x4c4b40",
                    "maxFeePerGas": "0x989680", "maxPriorityFeePerGas": "0x0",
                    "input": "0xdeadbeef", "accessList": [],
                    "v": "0x1", "yParity": "0x1", "r": hash(0x05), "s": hash(0x06),
                },
                {
                    "type": "0x0", "nonce": "0x2",
                    "hash": hash(0x22), "blockHash": hash(0xab),
                    "blockNumber": "0x112a880", "transactionIndex": "0x1",
                    "from": address(0xf2), "to": address(0xf3), "value": "0x1",
                    "gas": "0x5208", "gasPrice": "0x4c4b40", "input": "0x",
                    "v": "0x25", "r": hash(0x07), "s": hash(0x08),
                },
            ],
        })
    }

    /// Receipts as `eth_getBlockReceipts` returns them, with logs nested.
    fn receipts() -> Value {
        json!([
            {
                "blockHash": hash(0xab), "blockNumber": "0x112a880",
                "transactionHash": hash(0x11), "transactionIndex": "0x0",
                "from": address(0xf0), "to": address(0xf1),
                "cumulativeGasUsed": "0x5208", "gasUsed": "0x5208",
                "effectiveGasPrice": "0x4c4b40", "contractAddress": null,
                "status": "0x1", "type": "0x2", "logsBloom": bloom(),
                "logs": [
                    {
                        "address": address(0xde), "data": "0x0001",
                        "topics": [hash(0x01), hash(0x02), hash(0x03), hash(0x04), hash(0x05)],
                        "blockNumber": "0x112a880", "blockHash": hash(0xab),
                        "transactionHash": hash(0x11), "transactionIndex": "0x0",
                        "logIndex": "0x0", "removed": false,
                    },
                    {
                        "address": address(0xbe), "data": "0x",
                        "topics": [hash(0x11)],
                        "blockNumber": "0x112a880", "blockHash": hash(0xab),
                        "transactionHash": hash(0x11), "transactionIndex": "0x0",
                        "logIndex": "0x1", "removed": false,
                    },
                ],
            },
            {
                "blockHash": hash(0xab), "blockNumber": "0x112a880",
                "transactionHash": hash(0x22), "transactionIndex": "0x1",
                "from": address(0xf2), "to": address(0xf3),
                "cumulativeGasUsed": "0xa410", "gasUsed": "0x5208",
                "effectiveGasPrice": "0x4c4b40", "contractAddress": null,
                "status": "0x0", "type": "0x0", "logsBloom": bloom(),
                "logs": [],
            },
        ])
    }

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

    #[test]
    fn receipts_from_another_block_are_rejected() {
        let mut receipts = receipts();
        receipts[1]["blockHash"] = json!(hash(0xcd));
        let error = decode_block(batch(&block(), &receipts)).expect_err("fork mismatch must fail");
        assert!(error.to_string().contains("reorganised"), "{error}");
    }

    #[test]
    fn receipt_count_must_match_transactions() {
        let mut receipts = receipts();
        receipts
            .as_array_mut()
            .expect("receipts are an array")
            .pop();
        let error = decode_block(batch(&block(), &receipts)).expect_err("count mismatch must fail");
        assert!(
            error.to_string().contains("1 receipts for 2 transactions"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_missing_log_index_is_rejected() {
        // Alloy rejects an absent logIndex at the RPC decode boundary.
        let mut receipts = receipts();
        for log in receipts[0]["logs"]
            .as_array_mut()
            .expect("logs are an array")
        {
            log.as_object_mut()
                .expect("log is an object")
                .remove("logIndex");
        }
        let (url, server) = rpc_server(1, move |request| {
            primary_reply(request, &block(), &receipts)
        });
        let source = from_urls("ethereum", url, "ws://unused", Datasets::all()).expect("source");
        let error = source
            .fetch_block(18_000_000, None)
            .await
            .expect_err("missing logIndex");
        assert!(matches!(
            error,
            SourceError::Transport {
                context: "eth_getBlockReceipts",
                source: alloy_json_rpc::RpcError::DeserError { .. }
            }
        ));
        server.join().expect("server");
    }

    /// A provider's URL holds its API key, and a failed request's error is logged with
    /// `{:?}`, so neither rendering may carry the URL reqwest attaches. A refused
    /// connection is retried with backoff first; paused time skips those waits.
    #[tokio::test(start_paused = true)]
    async fn a_transport_error_does_not_carry_the_endpoint_url() {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .expect("a free port")
            .port();
        let url = format!("http://127.0.0.1:{port}/v2/secret-api-key");
        let source = from_urls("ethereum", url, "ws://unused", Datasets::all()).expect("source");
        let error = source
            .fetch_block(1, None)
            .await
            .expect_err("nothing listens on the port");

        assert!(matches!(error, SourceError::Transport { .. }), "{error:?}");
        let printed = format!("{error} {error:?}");
        assert!(!printed.contains("secret-api-key"), "{printed}");
    }

    /// An explicit `null` `logIndex` is the case the old fallback got wrong. alloy
    /// accepts it and hands over `None`, so the fallback was reachable — and it supplied
    /// the log's position *within its receipt*, which is not the block index the field
    /// means. On a block whose second transaction emits a log, the two differ, and the
    /// wrong value collides on dedupe with a different log.
    #[test]
    fn a_null_log_index_is_rejected_rather_than_filled_from_the_receipt() {
        let mut receipts = receipts();
        for log in receipts[0]["logs"]
            .as_array_mut()
            .expect("logs are an array")
        {
            log["logIndex"] = Value::Null;
        }
        let error =
            decode_block(batch(&block(), &receipts)).expect_err("a null logIndex must fail");
        let message = error.to_string();
        assert!(
            message.contains("eth_getBlockReceipts") && message.contains("logIndex"),
            "a log that cannot be placed in the block must not be filed under a guess: {message}"
        );
    }

    /// An OP-stack block carrying a deposit transaction (`type: 0x7e`), which
    /// Ethereum-only types reject outright; the fixture is trimmed from a real Base
    /// block.
    ///
    /// The load-bearing assertion is that the decode succeeds at all: a typed
    /// envelope cannot represent `0x7e`, so this fails unless the catch-all
    /// (`AnyTxEnvelope::Unknown`) path handles it. The field checks confirm the
    /// common fields are projected for the unknown type.
    #[tokio::test]
    async fn non_ethereum_transaction_type_decodes_and_keeps_its_type() {
        let deposit = json!({
            "hash": hash(0x11), "blockHash": hash(0xab), "blockNumber": "0x112a880",
            "transactionIndex": "0x0", "from": address(0xf0), "to": address(0xf1),
            "value": "0x0", "gas": "0x5208", "gasPrice": "0x0",
            "input": "0xdeadbeef", "nonce": "0x1", "type": "0x7e",
            // Fields alloy captures for the unknown type and does not model.
            "sourceHash": hash(0x42), "mint": "0x0", "depositReceiptVersion": "0x1",
            "v": "0x0", "r": hash(0x00), "s": hash(0x00), "yParity": "0x0",
        });
        let mut block = block();
        block["transactions"] = json!([deposit]);
        // The deposit receipt carries no blob fields; keep the block/receipt
        // hashes aligned with the block fixture.
        let mut receipts = receipts();
        receipts.as_array_mut().expect("array").truncate(1);
        receipts[0]["transactionHash"] = json!(hash(0x11));
        receipts[0]["type"] = json!("0x7e");
        receipts[0]["logs"] = json!([]);

        let (url, server) = rpc_server(1, move |request| primary_reply(request, &block, &receipts));
        let source = from_urls("base", url, "ws://unused", Datasets::all()).expect("source");
        let fetched = source
            .fetch_block(18_000_000, None)
            .await
            .expect("OP deposit block decodes");
        server.join().expect("server");
        assert_eq!(fetched.events.len(), 2, "block, transaction");

        let Event::Transaction(transaction) = &fetched.events[1] else {
            panic!("second event must be a transaction");
        };
        assert_eq!(transaction.transaction_type, 0x7e);
        assert_eq!(transaction.gas_price, Some(0));
        assert_eq!(transaction.input.len(), 4);
        assert_eq!(transaction.nonce, 1);
        assert!(transaction.receipt_status);
    }

    /// A log's transaction hash is part of its identity; defaulting it would make
    /// unrelated logs dedupe together, so it is rejected instead.
    #[test]
    fn a_log_without_a_transaction_hash_is_rejected() {
        let mut receipts = receipts();
        for log in receipts[0]["logs"]
            .as_array_mut()
            .expect("logs are an array")
        {
            log.as_object_mut()
                .expect("log is an object")
                .remove("transactionHash");
        }
        let error = decode_block(batch(&block(), &receipts))
            .expect_err("a log without an identity must fail");
        let message = error.to_string();
        assert!(
            message.contains("eth_getBlockReceipts") && message.contains("transactionHash"),
            "{message}"
        );
    }
}
