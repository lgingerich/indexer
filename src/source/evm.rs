//! An EVM block source: live heads over WebSocket, full blocks over JSON-RPC.
//!
//! The two transports map onto the two roles in [`BlockSource`]. `newHeads` over
//! WebSocket is the live path, and it is the only way to observe a head without
//! paying a poll interval. JSON-RPC over HTTP is the pull path. Requirements worth
//! stating because they are easy to get wrong:
//!
//! - Blocks are requested with full transaction objects, and receipts come from
//!   `eth_getBlockReceipts`, so nothing the node returns is dropped. Some
//!   non-Ethereum nodes lack `eth_getBlockReceipts`.
//! - Finality comes from the node's `finalized` block tag, so each chain's own
//!   rules apply: about two epochs on Ethereum, L1 finality of the batch on an L2.
//! - All three calls are sent as one batch, so a block costs one round trip. They
//!   still execute separately on the node, so a reorg between them can pair a
//!   block with another fork's receipts; every receipt's `blockHash` is checked.
//! - The batch is parsed into alloy's RPC types, then projected field by field into
//!   the [`crate::datasets`] records; nothing is kept as opaque JSON.

use std::fmt;

use alloy_consensus::TxReceipt as _;
use alloy_primitives::B256;
use alloy_rpc_types_eth::{
    Block as RpcBlock, Log as RpcLog, Transaction as RpcTransaction, TransactionReceipt,
    TransactionTrait as _,
};
use futures_util::{SinkExt as _, StreamExt};
use serde::Deserialize;
use serde_json::json;
use serde_json::value::RawValue;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::{BlockId, BlockSource, FetchedBlock, Head, HeadStream, SourceError};
use crate::datasets::evm::{Block, Log, Receipt, Transaction};
use crate::envelope::{ChainId, Event};

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
            client: reqwest::Client::new(),
        }
    }
}

/// A block header's identity fields: a `newHeads` notification, or the finalized
/// block, whose other fields are ignored.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcHead {
    number: String,
    hash: String,
    parent_hash: String,
}

impl RpcHead {
    fn into_head(self) -> Result<Head, String> {
        Ok(Head {
            height: decode_u64(&self.number)?,
            hash: decode_hash(&self.hash)?,
            parent_hash: decode_hash(&self.parent_hash)?,
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
    result: Option<RpcHead>,
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
fn take_result<'a>(
    responses: &[RpcResponse<'a>],
    id: u64,
    method: &str,
) -> Result<&'a RawValue, SourceError> {
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
    response
        .result
        .ok_or_else(|| malformed(method, "result was null"))
}

/// Reads the height and hash out of a block header.
fn decode_block_id(header: &RawValue) -> Result<BlockId, SourceError> {
    const CONTEXT: &str = "finalized block";
    let head: RpcHead =
        serde_json::from_str(header.get()).map_err(|error| malformed(CONTEXT, error))?;
    let head = head
        .into_head()
        .map_err(|detail| malformed(CONTEXT, detail))?;
    Ok(BlockId {
        height: head.height,
        hash: head.hash,
    })
}

/// Decodes one WebSocket frame into a head, when the frame is a head notification.
fn decode_head_frame(text: &str) -> Option<Result<Head, SourceError>> {
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
        head.into_head()
            .map_err(|detail| malformed("newHeads", detail)),
    )
}

/// The three results one batched block request carries, still in alloy's RPC types
/// and not yet projected into [`crate::datasets`] records.
#[derive(Debug)]
pub struct RpcBatch {
    /// The block, from `eth_getBlockByNumber` with full transactions.
    pub block: RpcBlock<RpcTransaction>,
    /// The block's receipts, from `eth_getBlockReceipts`.
    pub receipts: Vec<TransactionReceipt>,
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
    let block: RpcBlock<RpcTransaction> =
        serde_json::from_str(take_result(&responses, 1, BLOCK)?.get())
            .map_err(|error| malformed(BLOCK, error))?;
    let receipts: Vec<TransactionReceipt> =
        serde_json::from_str(take_result(&responses, 2, RECEIPTS)?.get())
            .map_err(|error| malformed(RECEIPTS, error))?;
    let finalized = decode_block_id(take_result(&responses, 3, "finalized block")?)?;
    Ok(RpcBatch {
        block,
        receipts,
        finalized,
    })
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
    Ok(FetchedBlock {
        events: decode_events(&block, &receipts)?,
        finalized,
    })
}

/// Projects a block and its receipts into dataset events.
fn decode_events(
    block: &RpcBlock<RpcTransaction>,
    receipts: &[TransactionReceipt],
) -> Result<Vec<Event>, SourceError> {
    const RECEIPTS: &str = "eth_getBlockReceipts";

    let transactions = block.transactions.as_transactions().ok_or_else(|| {
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

    let number = block.header.inner.number;
    let hash = block.header.hash;

    let mut events = Vec::with_capacity(1 + transactions.len() * 3);
    events.push(Event::Block(Box::new(decode_header(block, transactions))));

    for (position, (transaction, receipt)) in transactions.iter().zip(receipts).enumerate() {
        let tx_hash = *transaction.inner.tx_hash();
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
            block.header.inner.timestamp,
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
fn decode_header(block: &RpcBlock<RpcTransaction>, transactions: &[RpcTransaction]) -> Block {
    let header = &block.header.inner;
    Block {
        number: header.number,
        hash: block.header.hash,
        parent_hash: header.parent_hash,
        timestamp: header.timestamp,
        nonce: header.nonce,
        ommers_hash: header.ommers_hash,
        transactions_root: header.transactions_root,
        state_root: header.state_root,
        receipts_root: header.receipts_root,
        withdrawals_root: header.withdrawals_root,
        logs_bloom: header.logs_bloom,
        miner: header.beneficiary,
        difficulty: header.difficulty,
        total_difficulty: block.header.total_difficulty,
        size: block.header.size,
        extra_data: header.extra_data.clone(),
        gas_limit: header.gas_limit,
        gas_used: header.gas_used,
        transaction_count: transactions.len() as u64,
        base_fee_per_gas: header.base_fee_per_gas,
        blob_gas_used: header.blob_gas_used,
        excess_blob_gas: header.excess_blob_gas,
        parent_beacon_block_root: header.parent_beacon_block_root,
        ommers: block.uncles.clone(),
        transaction_hashes: transactions
            .iter()
            .map(|transaction| *transaction.inner.tx_hash())
            .collect(),
    }
}

/// Flattens one RPC transaction into the transaction dataset.
fn decode_transaction(
    transaction: &RpcTransaction,
    tx_index: u64,
    block_number: u64,
    block_timestamp: u64,
    block_hash: B256,
) -> Transaction {
    Transaction {
        hash: *transaction.inner.tx_hash(),
        nonce: transaction.nonce(),
        transaction_index: tx_index,
        from: transaction.inner.signer(),
        to: transaction.inner.to(),
        value: transaction.inner.value(),
        gas: transaction.inner.gas_limit(),
        gas_price: transaction.inner.gas_price(),
        max_fee_per_gas: transaction.inner.max_fee_per_gas(),
        max_priority_fee_per_gas: transaction.inner.max_priority_fee_per_gas(),
        max_fee_per_blob_gas: transaction.inner.max_fee_per_blob_gas(),
        input: transaction.inner.input().clone(),
        transaction_type: transaction.inner.tx_type(),
        chain_id: transaction.inner.chain_id(),
        access_list: transaction.inner.access_list().cloned(),
        blob_versioned_hashes: transaction
            .inner
            .blob_versioned_hashes()
            .map(<[B256]>::to_vec),
        authorization_list: transaction.inner.authorization_list().map(<[_]>::to_vec),
        block_timestamp,
        block_number,
        block_hash,
    }
}

/// Flattens one RPC receipt into the receipt dataset, without its logs.
fn decode_receipt(
    receipt: &TransactionReceipt,
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
        status: receipt.status(),
        transaction_type: receipt.transaction_type(),
        gas_used: receipt.gas_used,
        cumulative_gas_used: receipt.inner.cumulative_gas_used(),
        effective_gas_price: receipt.effective_gas_price,
        contract_address: receipt.contract_address,
        logs_bloom: receipt.inner.bloom(),
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
        decode_block(parse_batch(&body)?)
    }
}

impl EvmSource {
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
        let transport = |error: reqwest::Error| SourceError::Transport(error.to_string());
        self.client
            .post(self.http_url.as_str())
            .json(&batch)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(transport)?
            .bytes()
            .await
            .map_err(transport)
            .map(|bytes| bytes.to_vec())
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
        assert_eq!(transaction.transaction_type, TxType::Eip1559);
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
        assert_eq!(receipt.transaction_type, TxType::Eip1559);

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

    #[test]
    fn subscription_confirmation_is_not_a_head() {
        let confirmation = r#"{"jsonrpc":"2.0","id":1,"result":"0xsub"}"#;
        assert!(decode_head_frame(confirmation).is_none());
    }

    #[test]
    fn head_notification_decodes() {
        let frame = r#"{"jsonrpc":"2.0","method":"eth_subscription","params":{"result":{
            "number":"0x10",
            "hash":"0x0000000000000000000000000000000000000000000000000000000000000001",
            "parentHash":"0x0000000000000000000000000000000000000000000000000000000000000002"
        }}}"#;
        let head = decode_head_frame(frame)
            .expect("frame is a head")
            .expect("head decodes");
        assert_eq!(head.height, 16);
        assert_eq!(head.parent_hash, B256::with_last_byte(2));
    }
}
