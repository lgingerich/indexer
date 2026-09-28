//! An EVM block source: live heads over WebSocket, full blocks over JSON-RPC.
//!
//! The two transports map onto the two roles in [`BlockSource`]. `newHeads` over
//! WebSocket is the live path, and it is the only way to observe a head without
//! paying a poll interval. JSON-RPC over HTTP is the pull path. Requirements worth
//! stating because they are easy to get wrong:
//!
//! - Blocks are requested with full transaction objects, and receipts come from
//!   `eth_getBlockReceipts`, so nothing the node returns is dropped. Some nodes
//!   lack that method (some L2s, pre-Cancun Ethereum); it is answered with `-32601`
//!   or null, and the source then fetches each receipt with
//!   `eth_getTransactionReceipt` in batches of [`RECEIPT_BATCH_LIMIT`].
//! - Finality comes from the node's `finalized` block tag, so each chain's own
//!   rules apply: about two epochs on Ethereum, L1 finality of the batch on an L2.
//! - All three calls are sent as one batch, so a block costs one round trip. They
//!   still execute separately on the node, so a reorg between them can pair a
//!   block with another fork's receipts; every receipt's `blockHash` is checked.
//! - The batch is parsed into alloy's RPC types, then projected field by field into
//!   the [`crate::datasets`] records; nothing is kept as opaque JSON.
//! - Parsing uses alloy's *catch-all* (`any`) types, so a chain's non-Ethereum
//!   transaction types — an OP-stack deposit (`0x7e`), an Arbitrum retry (`0x6a`) —
//!   decode instead of failing the block, and chain-specific extras are captured.
//!   Everything read from them goes through the standard `Transaction` accessors,
//!   except the transaction type, which is kept as its raw `u8`.

use std::fmt;

use alloy_consensus::Transaction as ConsensusTransaction;
use alloy_network::TransactionResponse;
use alloy_network::any::{AnyRpcBlock, AnyRpcTransaction, AnyTransactionReceipt};
use alloy_network::eip2718::Typed2718 as _;
use alloy_primitives::B256;
use alloy_rpc_types_eth::Log as RpcLog;
use futures_util::{SinkExt as _, StreamExt};
use serde::Deserialize;
use serde_json::json;
use serde_json::value::RawValue;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::{BlockId, BlockSource, FetchedBlock, HeadStream, SourceError};
use crate::datasets::evm::{Block, Log, Receipt, Transaction};
use crate::envelope::{ChainId, Event};

/// How many `eth_getTransactionReceipt` calls to put in one JSON-RPC batch.
///
/// Public nodes cap a batch — Base and Optimism reject anything above ten calls
/// with `-32014` — so the per-transaction receipt fallback requests hashes in
/// chunks this size. Ten is the smallest observed cap, so it is safe everywhere.
const RECEIPT_BATCH_LIMIT: usize = 10;

/// A source that talks to one EVM chain over HTTP JSON-RPC and WebSocket.
#[derive(Debug, Clone)]
pub struct EvmSource {
    chain: ChainId,
    http_url: String,
    ws_url: String,
    client: reqwest::Client,
}

impl EvmSource {
    /// Builds a source for `chain`.
    #[must_use]
    pub fn new(
        chain: impl Into<ChainId>,
        http_url: impl Into<String>,
        ws_url: impl Into<String>,
    ) -> Self {
        Self {
            chain: chain.into(),
            http_url: http_url.into(),
            ws_url: ws_url.into(),
            // A bounded timeout keeps a node that accepts the connection and then
            // stalls from hanging a fetch forever. `build` only fails on TLS
            // backend init, in which case the default client is still usable.
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
        }
    }
}

/// A block's identity fields, enough for a `newHeads` notification or the
/// finalized block; every other header field is ignored.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcBlockId {
    number: String,
    hash: String,
}

impl RpcBlockId {
    fn into_block_id(self) -> Result<BlockId, String> {
        Ok(BlockId {
            height: decode_u64(&self.number)?,
            hash: decode_hash(&self.hash)?,
        })
    }
}

/// The subset of a WebSocket frame the source cares about.
#[derive(Debug, Deserialize)]
struct SubscriptionMessage {
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    params: Option<SubscriptionParams>,
}

#[derive(Debug, Deserialize)]
struct SubscriptionParams {
    #[serde(default)]
    result: Option<RpcBlockId>,
}

/// One JSON-RPC response inside a batch, borrowing its result from the body.
#[derive(Debug, Deserialize)]
struct RpcResponse<'a> {
    id: u64,
    #[serde(borrow, default)]
    result: Option<&'a RawValue>,
    #[serde(default)]
    error: Option<RpcErrorObject>,
}

#[derive(Debug, Deserialize)]
struct RpcErrorObject {
    code: i64,
    message: String,
}

fn malformed(context: &str, detail: impl fmt::Display) -> SourceError {
    SourceError::Malformed {
        context: context.to_owned(),
        detail: detail.to_string(),
    }
}

/// Parses a `0x`-prefixed hex quantity, or a bare hex string.
fn decode_u64(text: &str) -> Result<u64, String> {
    let digits = text.strip_prefix("0x").unwrap_or(text);
    u64::from_str_radix(digits, 16).map_err(|error| format!("invalid quantity {text:?}: {error}"))
}

/// Parses a 32-byte hash render, rejecting anything alloy would not round-trip.
fn decode_hash(text: &str) -> Result<B256, String> {
    text.parse()
        .map_err(|error| format!("invalid 32-byte hash {text:?}: {error}"))
}

/// Pulls one id's result out of a batch response.
///
/// A null result is returned as `Ok(None)` rather than an error, because null has
/// meaning for some methods: `eth_getBlockReceipts` answers null on nodes and
/// forks that do not support it, so the caller decides whether that is fatal.
///
/// # Errors
///
/// Returns [`SourceError::Malformed`] when the batch has no entry for `id`, and
/// [`SourceError::Transport`] when the node answered that call with an error.
fn take_result<'a>(
    responses: &[RpcResponse<'a>],
    id: u64,
    method: &str,
) -> Result<Option<&'a RawValue>, SourceError> {
    let response = responses
        .iter()
        .find(|response| response.id == id)
        .ok_or_else(|| malformed(method, format!("batch response had no entry with id {id}")))?;
    if let Some(error) = &response.error {
        return Err(SourceError::Transport(format!(
            "{method} returned error {}: {}",
            error.code, error.message
        )));
    }
    Ok(response.result)
}

/// Reads the height and hash out of a block header.
fn decode_block_id(header: &RawValue) -> Result<BlockId, SourceError> {
    const CONTEXT: &str = "finalized block";
    let block: RpcBlockId =
        serde_json::from_str(header.get()).map_err(|error| malformed(CONTEXT, error))?;
    block
        .into_block_id()
        .map_err(|detail| malformed(CONTEXT, detail))
}

/// Decodes one WebSocket frame into a head, when the frame is a head notification.
fn decode_head_frame(text: &str) -> Option<Result<BlockId, SourceError>> {
    let message: SubscriptionMessage = match serde_json::from_str(text) {
        Ok(message) => message,
        Err(error) => return Some(Err(malformed("websocket frame", error))),
    };
    if message.method.as_deref() != Some("eth_subscription") {
        // The subscription confirmation and any other notification land here.
        return None;
    }
    let head = message.params.and_then(|params| params.result)?;
    Some(
        head.into_block_id()
            .map_err(|detail| malformed("newHeads", detail)),
    )
}

/// The three results one batched block request carries, still in alloy's RPC types
/// and not yet projected into [`crate::datasets`] records.
///
/// The block and receipts use alloy's *catch-all* (`any`) types, so a chain's
/// non-Ethereum transaction types — an OP-stack deposit (`0x7e`), an Arbitrum
/// retry (`0x6a`) — decode instead of failing the whole block. Everything this
/// source reads is reachable through the standard `Transaction`/`BlockHeader`
/// accessors; only the transaction type falls back to a raw `u8`.
#[derive(Debug)]
pub struct RpcBatch {
    /// The block, from `eth_getBlockByNumber` with full transactions.
    pub block: AnyRpcBlock,
    /// The block's receipts, from `eth_getBlockReceipts`.
    ///
    /// `None` when the node does not serve that method; the caller then has to
    /// fetch each receipt by transaction hash. Some nodes answer the method with
    /// a `-32601` error and others with a `null` result, so both are treated the
    /// same way.
    pub receipts: Option<Vec<AnyTransactionReceipt>>,
    /// The chain's newest finalized block at request time.
    pub finalized: BlockId,
}

/// Parses a batch response body into the three results it carries.
///
/// The batch holds `eth_getBlockByNumber` with full transactions (id 1),
/// `eth_getBlockReceipts` (id 2), and the `finalized` block (id 3), in any order.
///
/// # Errors
///
/// Returns [`SourceError::Transport`] when the node answered a call with an
/// error, and [`SourceError::Malformed`] when the body is not a usable batch.
pub fn parse_batch(body: &[u8]) -> Result<RpcBatch, SourceError> {
    const BLOCK: &str = "eth_getBlockByNumber";
    const RECEIPTS: &str = "eth_getBlockReceipts";

    let responses: Vec<RpcResponse<'_>> =
        serde_json::from_slice(body).map_err(|error| malformed("rpc batch", error))?;
    let block_result =
        take_result(&responses, 1, BLOCK)?.ok_or_else(|| malformed(BLOCK, "result was null"))?;
    let block: AnyRpcBlock =
        serde_json::from_str(block_result.get()).map_err(|error| malformed(BLOCK, error))?;

    let receipts = match take_result(&responses, 2, RECEIPTS) {
        Ok(Some(result)) => {
            Some(serde_json::from_str(result.get()).map_err(|error| malformed(RECEIPTS, error))?)
        }
        // An unsupported method is reported as `-32601`, and some nodes answer
        // with a null result instead; both mean the caller fetches per transaction.
        Err(error) if is_method_not_found(&error) => None,
        Ok(None) => None,
        Err(error) => return Err(error),
    };

    let finalized = decode_block_id(
        take_result(&responses, 3, "finalized block")?
            .ok_or_else(|| malformed("finalized block", "result was null"))?,
    )?;
    Ok(RpcBatch {
        block,
        receipts,
        finalized,
    })
}

/// Whether a transport error is the node saying it does not have `eth_getBlockReceipts`.
fn is_method_not_found(error: &SourceError) -> bool {
    matches!(error, SourceError::Transport(message) if message.contains("-32601"))
}

/// Turns a parsed batch into ordered dataset events and the finality watermark.
///
/// Emits the block, then each transaction followed by its receipt and that
/// receipt's logs, all in index order, so a consumer sees every transaction before
/// its receipt and every receipt before its logs.
///
/// # Errors
///
/// Returns [`SourceError::Malformed`] when the receipts do not belong to the block
/// or do not line up with its transactions.
pub fn decode_block(batch: RpcBatch) -> Result<FetchedBlock, SourceError> {
    let RpcBatch {
        block,
        receipts,
        finalized,
    } = batch;
    // The batched method is the only way receipts reach a pure decode; when a node
    // lacks it, `fetch_block` fills them in per transaction before calling here.
    let receipts =
        receipts.ok_or_else(|| malformed("eth_getBlockReceipts", "receipts were not fetched"))?;
    Ok(FetchedBlock {
        events: decode_events(&block, &receipts)?,
        finalized,
    })
}

/// Projects a block and its receipts into dataset events.
fn decode_events(
    block: &AnyRpcBlock,
    receipts: &[AnyTransactionReceipt],
) -> Result<Vec<Event>, SourceError> {
    const RECEIPTS: &str = "eth_getBlockReceipts";

    let transactions = block
        .0
        .inner
        .transactions
        .as_transactions()
        .ok_or_else(|| {
            malformed(
                "eth_getBlockByNumber",
                "block returned transaction hashes only",
            )
        })?;
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

    let number = block.0.inner.header.inner.number;
    let hash = block.0.inner.header.hash;

    // One block, a transaction and receipt per transaction, and every log.
    let log_count: usize = receipts.iter().map(|receipt| receipt.logs().len()).sum();
    let mut events = Vec::with_capacity(1 + transactions.len() * 2 + log_count);
    events.push(Event::Block(Box::new(decode_header(block, transactions))));

    for (position, (transaction, receipt)) in transactions.iter().zip(receipts).enumerate() {
        let tx_hash = transaction.tx_hash();
        let tx_index = position as u64;

        let receipt_block = receipt.block_hash.unwrap_or_default();
        if receipt_block != hash {
            return Err(malformed(
                RECEIPTS,
                format!(
                    "receipt belongs to block {receipt_block}, not {hash}; the chain reorganised mid-request"
                ),
            ));
        }
        if receipt.transaction_index != Some(tx_index) {
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
            tx_index,
            number,
            block.0.inner.header.inner.timestamp,
            hash,
        ))));
        events.push(Event::Receipt(Box::new(decode_receipt(
            receipt, tx_hash, tx_index, number, hash,
        ))));
        for (position, log) in receipt.logs().iter().enumerate() {
            events.push(Event::Log(Box::new(log_record(
                log,
                position as u64,
                number,
                hash,
            ))));
        }
    }
    Ok(events)
}

/// Flattens a block header and its transaction hashes into the block dataset.
fn decode_header(block: &AnyRpcBlock, transactions: &[AnyRpcTransaction]) -> Block {
    let header = &block.0.inner.header;
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
        transaction_hashes: transactions
            .iter()
            .map(AnyRpcTransaction::tx_hash)
            .collect(),
    }
}

/// Flattens one RPC transaction into the transaction dataset.
///
/// Every field comes from the standard `alloy_consensus::Transaction` accessors,
/// which work for Ethereum and non-Ethereum types alike (an OP deposit reads its
/// `gasPrice` from the unknown transaction's captured fields). The type is kept as
/// the raw `u8` so a non-Ethereum type — `0x7e`, `0x6a` — survives.
fn decode_transaction(
    transaction: &AnyRpcTransaction,
    tx_index: u64,
    block_number: u64,
    block_timestamp: u64,
    block_hash: B256,
) -> Transaction {
    Transaction {
        hash: transaction.tx_hash(),
        nonce: ConsensusTransaction::nonce(transaction),
        transaction_index: tx_index,
        from: transaction.from(),
        to: ConsensusTransaction::to(transaction),
        value: ConsensusTransaction::value(transaction),
        gas: ConsensusTransaction::gas_limit(transaction),
        // `gas_price`/`max_fee_per_gas` exist on both `Transaction` and
        // `TransactionResponse` for an RPC transaction; the RPC one is correct.
        // `max_fee_per_gas` is `Some` for dynamic-fee types and `None` for legacy,
        // where the consensus accessor saturates to the gas price instead.
        gas_price: TransactionResponse::gas_price(transaction),
        max_fee_per_gas: TransactionResponse::max_fee_per_gas(transaction)
            .unwrap_or_else(|| ConsensusTransaction::max_fee_per_gas(transaction)),
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
        block_timestamp,
        block_number,
        block_hash,
    }
}

/// Flattens one RPC receipt into the receipt dataset, without its logs.
fn decode_receipt(
    receipt: &AnyTransactionReceipt,
    transaction_hash: B256,
    tx_index: u64,
    block_number: u64,
    block_hash: B256,
) -> Receipt {
    Receipt {
        transaction_hash,
        transaction_index: tx_index,
        from: receipt.from,
        to: receipt.to,
        status: receipt.inner.inner.status(),
        transaction_type: receipt.inner.inner.r#type,
        gas_used: receipt.gas_used,
        cumulative_gas_used: receipt.inner.inner.cumulative_gas_used(),
        effective_gas_price: receipt.effective_gas_price,
        contract_address: receipt.contract_address,
        logs_bloom: receipt.inner.inner.bloom(),
        blob_gas_used: receipt.blob_gas_used,
        blob_gas_price: receipt.blob_gas_price,
        log_count: receipt.logs().len() as u64,
        block_number,
        block_hash,
    }
}

/// Flattens one RPC log into the log dataset, defaulting absent position fields to
/// `fallback_index`.
fn log_record(log: &RpcLog, fallback_index: u64, block_number: u64, block_hash: B256) -> Log {
    let topics = log.topics();
    Log {
        log_index: log.log_index.unwrap_or(fallback_index),
        transaction_hash: log.transaction_hash.unwrap_or_default(),
        transaction_index: log.transaction_index.unwrap_or(fallback_index),
        address: log.address(),
        topic0: topics.first().copied(),
        topic1: topics.get(1).copied(),
        topic2: topics.get(2).copied(),
        topic3: topics.get(3).copied(),
        data: log.data().data.clone(),
        removed: log.removed,
        block_number,
        block_hash,
    }
}

impl BlockSource for EvmSource {
    fn chain(&self) -> &ChainId {
        &self.chain
    }

    async fn subscribe_heads(&self) -> Result<HeadStream, SourceError> {
        let (mut socket, _response) = connect_async(self.ws_url.as_str())
            .await
            .map_err(|error| SourceError::Transport(error.to_string()))?;
        let request = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": ["newHeads"],
        });
        socket
            .send(Message::text(request.to_string()))
            .await
            .map_err(|error| SourceError::Transport(error.to_string()))?;

        let heads = socket.filter_map(|frame| async move {
            match frame {
                Ok(Message::Text(text)) => decode_head_frame(&text),
                // Ping/Pong/Binary/Close carry no head; the socket handles pings.
                Ok(_) => None,
                Err(error) => Some(Err(SourceError::Transport(error.to_string()))),
            }
        });
        Ok(Box::pin(heads) as HeadStream)
    }

    async fn fetch_block(&self, height: u64) -> Result<FetchedBlock, SourceError> {
        let body = self.post_batch(height).await?;
        let mut batch = parse_batch(&body)?;
        // Nodes that do not serve `eth_getBlockReceipts` (some L2s, and pre-Cancun
        // Ethereum) answer it with `-32601` or null. Fetch the receipts by
        // transaction hash instead, so the block still becomes events.
        if batch.receipts.is_none() {
            batch.receipts = Some(self.fetch_receipts(&batch.block).await?);
        }
        decode_block(batch)
    }
}

impl EvmSource {
    /// POSTs one JSON-RPC request body and returns the response bytes.
    async fn post(&self, body: &serde_json::Value) -> Result<Vec<u8>, SourceError> {
        let transport = |error: reqwest::Error| SourceError::Transport(error.to_string());
        self.client
            .post(self.http_url.as_str())
            .json(body)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(transport)?
            .bytes()
            .await
            .map_err(transport)
            .map(|bytes| bytes.to_vec())
    }

    /// Fetches the block's receipts one transaction at a time, in index order.
    ///
    /// The fallback for nodes without `eth_getBlockReceipts`. Many public nodes cap
    /// a JSON-RPC batch at [`RECEIPT_BATCH_LIMIT`] calls — Base and Optimism answer
    /// `-32014` above it — so the hashes are requested in chunks of that size.
    async fn fetch_receipts(
        &self,
        block: &AnyRpcBlock,
    ) -> Result<Vec<AnyTransactionReceipt>, SourceError> {
        const RECEIPT: &str = "eth_getTransactionReceipt";
        let transactions = block
            .0
            .inner
            .transactions
            .as_transactions()
            .ok_or_else(|| {
                malformed(
                    "eth_getBlockByNumber",
                    "block returned transaction hashes only",
                )
            })?;

        let mut receipts = Vec::with_capacity(transactions.len());
        for chunk in transactions.chunks(RECEIPT_BATCH_LIMIT) {
            let requests: Vec<serde_json::Value> = chunk
                .iter()
                .enumerate()
                .map(|(index, transaction)| {
                    json!({
                        "jsonrpc": "2.0",
                        "id": index + 1,
                        "method": RECEIPT,
                        "params": [transaction.tx_hash()],
                    })
                })
                .collect();
            let body = self.post(&serde_json::Value::Array(requests)).await?;
            let responses: Vec<RpcResponse<'_>> =
                serde_json::from_slice(&body).map_err(|error| malformed(RECEIPT, error))?;
            // Match by id so order is the block's, whatever order the node replies in.
            for index in 0..chunk.len() {
                let result = take_result(&responses, index as u64 + 1, RECEIPT)?
                    .ok_or_else(|| malformed(RECEIPT, "result was null"))?;
                receipts.push(
                    serde_json::from_str(result.get())
                        .map_err(|error| malformed(RECEIPT, error))?,
                );
            }
        }
        Ok(receipts)
    }

    /// Sends the batched block request and returns the raw response body.
    ///
    /// This is the only I/O in a block fetch; parsing and projection are pure and
    /// separate, so the shape of a fetch is visible in [`EvmSource::fetch_block`].
    async fn post_batch(&self, height: u64) -> Result<Vec<u8>, SourceError> {
        let tag = format!("0x{height:x}");
        let batch = json!([
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getBlockByNumber",
                "params": [tag, true],
            },
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "eth_getBlockReceipts",
                "params": [tag],
            },
            // ponytail: fetches the finalized header with every block, which also
            // carries its transaction-hash list (tens of KB on a busy chain). If that
            // shows up in latency, poll `finalized` on a timer instead.
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "eth_getBlockByNumber",
                "params": ["finalized", false],
            },
        ]);
        self.post(&batch).await
    }
}

/// Renders the source as its chain, for log fields.
impl fmt::Display for EvmSource {
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
    use alloy_primitives::{Address, B256};
    use serde_json::{Value, json};

    use super::{decode_block, decode_head_frame, decode_u64, parse_batch};
    use crate::envelope::Event;

    /// Runs the full parse-and-project path, as production does.
    fn decode_batch(body: &[u8]) -> Result<crate::source::FetchedBlock, super::SourceError> {
        decode_block(parse_batch(body)?)
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
    /// carries every header field the block dataset reads.
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

    /// A batch body as the node returns it: responses out of request order.
    fn batch(block: &Value, receipts: &Value) -> Vec<u8> {
        json!([
            {"jsonrpc": "2.0", "id": 3, "result": {
                "number": "0x112a840", "hash": hash(0xf0), "parentHash": hash(0xef),
            }},
            {"jsonrpc": "2.0", "id": 2, "result": receipts},
            {"jsonrpc": "2.0", "id": 1, "result": block},
        ])
        .to_string()
        .into_bytes()
    }

    #[test]
    fn decodes_hex_quantities() {
        assert_eq!(decode_u64("0x0"), Ok(0));
        assert_eq!(decode_u64("0x10"), Ok(16));
        assert_eq!(decode_u64("ff"), Ok(255));
        assert!(decode_u64("0xzz").is_err());
    }

    #[test]
    fn events_are_block_then_each_transaction_followed_by_its_receipt_and_logs() {
        let fetched = decode_batch(&batch(&block(), &receipts())).expect("batch decodes");
        let kinds: Vec<&str> = fetched.events.iter().map(Event::kind).collect();
        assert_eq!(
            kinds,
            [
                "block",
                "transaction",
                "receipt",
                "log",
                "log",
                "transaction",
                "receipt",
            ]
        );

        let Event::Block(marker) = &fetched.events[0] else {
            panic!("first event must be the block marker");
        };
        assert_eq!(marker.number, 18_000_000);
        assert_eq!(marker.transaction_count, 2);
        assert_eq!(marker.parent_hash.to_string(), hash(0xaa));
        assert_eq!(
            marker.ommers,
            vec![hash(0x99).parse::<B256>().expect("hash")]
        );
    }

    #[test]
    fn datasets_carry_their_fields_and_reference_children_by_key() {
        let fetched = decode_batch(&batch(&block(), &receipts())).expect("batch decodes");

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

        let Event::Receipt(receipt) = &fetched.events[2] else {
            panic!("third event must be a receipt");
        };
        assert_eq!(receipt.transaction_hash, transaction.hash);
        assert!(receipt.status);
        assert_eq!(receipt.gas_used, 21_000);
        assert_eq!(receipt.cumulative_gas_used, 21_000);
        // Logs are their own dataset; the receipt carries only their count.
        assert_eq!(receipt.log_count, 2);
        assert_eq!(receipt.transaction_type, TxType::Eip1559 as u8);

        let Event::Log(log) = &fetched.events[4] else {
            panic!("fifth event must be a log");
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
        let fetched = decode_batch(&batch(&block(), &receipts())).expect("batch decodes");
        let Event::Log(first) = &fetched.events[3] else {
            panic!("fourth event must be a log");
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
    fn finalized_block_comes_from_the_batch() {
        let fetched = decode_batch(&batch(&block(), &receipts())).expect("batch decodes");
        assert_eq!(fetched.finalized.height, 17_999_936);
        assert_eq!(fetched.finalized.hash, B256::from([0xf0; 32]));
    }

    #[test]
    fn receipts_from_another_block_are_rejected() {
        let mut receipts = receipts();
        receipts[1]["blockHash"] = json!(hash(0xcd));
        let error = decode_batch(&batch(&block(), &receipts)).expect_err("fork mismatch must fail");
        assert!(error.to_string().contains("reorganised"), "{error}");
    }

    #[test]
    fn receipt_count_must_match_transactions() {
        let mut receipts = receipts();
        receipts
            .as_array_mut()
            .expect("receipts are an array")
            .pop();
        let error =
            decode_batch(&batch(&block(), &receipts)).expect_err("count mismatch must fail");
        assert!(
            error.to_string().contains("1 receipts for 2 transactions"),
            "{error}"
        );
    }

    #[test]
    fn a_missing_log_index_is_rejected() {
        // alloy models `logIndex` as required, so the dataset cannot fall back to
        // position: a receipt without one is malformed, not a log at index zero.
        let mut receipts = receipts();
        for log in receipts[0]["logs"]
            .as_array_mut()
            .expect("logs are an array")
        {
            log.as_object_mut()
                .expect("log is an object")
                .remove("logIndex");
        }
        let error =
            decode_batch(&batch(&block(), &receipts)).expect_err("missing logIndex must fail");
        let message = error.to_string();
        assert!(
            message.contains("eth_getBlockReceipts") && message.contains("logIndex"),
            "{message}"
        );
    }

    #[test]
    fn missing_block_field_reports_its_context() {
        let mut block = block();
        block
            .as_object_mut()
            .expect("block is an object")
            .remove("hash");
        let error = decode_batch(&batch(&block, &receipts())).expect_err("missing hash must fail");
        let message = error.to_string();
        assert!(message.contains("eth_getBlockByNumber"), "{message}");
    }

    #[test]
    fn node_error_in_the_batch_is_a_transport_failure() {
        let body = json!([
            {"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "header not found"}},
        ])
        .to_string();
        let error = decode_batch(body.as_bytes()).expect_err("node error must fail");
        assert!(error.to_string().contains("header not found"), "{error}");
    }

    /// A node without `eth_getBlockReceipts` answers `-32601`; the batch must parse
    /// with receipts absent so `fetch_block` can fall back to per-transaction
    /// fetches, rather than failing the block.
    #[test]
    fn unsupported_receipts_method_leaves_receipts_absent() {
        let body = json!([
            {"jsonrpc": "2.0", "id": 1, "result": block()},
            {"jsonrpc": "2.0", "id": 2, "error": {"code": -32601, "message": "method not found"}},
            {"jsonrpc": "2.0", "id": 3, "result": {"number": "0x10", "hash": hash(0xf0)}},
        ])
        .to_string();
        let batch = parse_batch(body.as_bytes()).expect("batch parses without receipts");
        assert!(batch.receipts.is_none());
    }

    /// A `null` result (rather than an error) also means the caller must fall back.
    #[test]
    fn null_receipts_result_leaves_receipts_absent() {
        let body = json!([
            {"jsonrpc": "2.0", "id": 1, "result": block()},
            {"jsonrpc": "2.0", "id": 2, "result": null},
            {"jsonrpc": "2.0", "id": 3, "result": {"number": "0x10", "hash": hash(0xf0)}},
        ])
        .to_string();
        let batch = parse_batch(body.as_bytes()).expect("batch parses without receipts");
        assert!(batch.receipts.is_none());
    }

    #[test]
    fn subscription_confirmation_is_not_a_head() {
        let confirmation = r#"{"jsonrpc":"2.0","id":1,"result":"0xsub"}"#;
        assert!(decode_head_frame(confirmation).is_none());
    }

    #[test]
    fn head_notification_decodes() {
        // A real `newHeads` notification carries far more than identity; the extra
        // fields are ignored.
        let frame = r#"{"jsonrpc":"2.0","method":"eth_subscription","params":{"result":{
            "number":"0x10",
            "hash":"0x0000000000000000000000000000000000000000000000000000000000000001",
            "parentHash":"0x0000000000000000000000000000000000000000000000000000000000000002",
            "timestamp":"0x6530a1b0"
        }}}"#;
        let head = decode_head_frame(frame)
            .expect("frame is a head")
            .expect("head decodes");
        assert_eq!(head.height, 16);
        assert_eq!(head.hash, B256::with_last_byte(1));
    }

    /// An OP-stack block carrying a deposit transaction (`type: 0x7e`), which
    /// Ethereum-only types reject outright. This is the regression that matters
    /// most: the deposit type is outside 0-4, so the whole block used to fail to
    /// decode. The fixture is trimmed from a real Base block.
    #[test]
    fn non_ethereum_transaction_type_decodes_and_keeps_its_type() {
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

        let fetched = decode_batch(&batch(&block, &receipts)).expect("OP deposit block decodes");
        assert_eq!(fetched.events.len(), 3, "block, transaction, receipt");

        let Event::Transaction(transaction) = &fetched.events[1] else {
            panic!("second event must be a transaction");
        };
        assert_eq!(transaction.transaction_type, 0x7e);
        assert_eq!(
            transaction.to,
            Some(address(0xf1).parse().expect("to parses"))
        );
        assert_eq!(transaction.nonce, 1);

        let Event::Receipt(receipt) = &fetched.events[2] else {
            panic!("third event must be a receipt");
        };
        assert_eq!(receipt.transaction_type, 0x7e);
    }
}
