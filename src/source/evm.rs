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
//! - Payloads are carried as unparsed JSON text. Only the handful of fields the
//!   indexer needs are read, and a nested array that is published as its own
//!   events is removed so no field is sent twice.

use std::collections::BTreeMap;
use std::fmt;

use alloy_primitives::{B256, TxHash};
use futures_util::{SinkExt as _, StreamExt};
use serde::Deserialize;
use serde_json::json;
use serde_json::value::RawValue;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::{BlockId, BlockSource, FetchedBlock, Head, HeadStream, SourceError};
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

/// A JSON object whose values stay as unparsed JSON text.
type RawObject<'a> = BTreeMap<&'a str, &'a RawValue>;

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

/// A log's position, read without parsing the rest of the log.
#[derive(Debug, Deserialize)]
struct LogPosition<'a> {
    #[serde(rename = "logIndex", borrow, default)]
    log_index: Option<&'a str>,
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

/// Reads a string field, such as a hex quantity or hash, out of a raw object.
fn text_field<'a>(
    object: &RawObject<'a>,
    key: &str,
    context: &str,
) -> Result<&'a str, SourceError> {
    let raw = object
        .get(key)
        .ok_or_else(|| malformed(context, format!("missing field {key}")))?;
    serde_json::from_str(raw.get()).map_err(|error| malformed(context, format!("{key}: {error}")))
}

fn quantity_field(object: &RawObject<'_>, key: &str, context: &str) -> Result<u64, SourceError> {
    decode_u64(text_field(object, key, context)?).map_err(|detail| malformed(context, detail))
}

fn hash_field(object: &RawObject<'_>, key: &str, context: &str) -> Result<B256, SourceError> {
    decode_hash(text_field(object, key, context)?).map_err(|detail| malformed(context, detail))
}

/// Removes an array field from a raw object, returning its unparsed elements.
fn take_array<'a>(
    object: &mut RawObject<'a>,
    key: &str,
    context: &str,
) -> Result<Vec<&'a RawValue>, SourceError> {
    let raw = object
        .remove(key)
        .ok_or_else(|| malformed(context, format!("missing field {key}")))?;
    serde_json::from_str(raw.get()).map_err(|error| malformed(context, format!("{key}: {error}")))
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

/// Decodes the body of the batch that [`EvmSource`] sends for one block.
///
/// The batch holds `eth_getBlockByNumber` with full transactions (id 1),
/// `eth_getBlockReceipts` (id 2), and the `finalized` block (id 3), in any order.
///
/// # Errors
///
/// Returns [`SourceError::Transport`] when the node answered a call with an
/// error, and [`SourceError::Malformed`] when the body cannot be decoded or the
/// receipts do not belong to the block.
pub fn decode_batch(body: &[u8]) -> Result<FetchedBlock, SourceError> {
    let responses: Vec<RpcResponse<'_>> =
        serde_json::from_slice(body).map_err(|error| malformed("rpc batch", error))?;
    let block = take_result(&responses, 1, "eth_getBlockByNumber")?;
    let receipts = take_result(&responses, 2, "eth_getBlockReceipts")?;
    let finalized = decode_block_id(take_result(&responses, 3, "finalized block")?)?;
    Ok(FetchedBlock {
        events: decode_block(block, receipts)?,
        finalized,
    })
}

/// Turns a block and its receipts into ordered events: the block marker, then
/// each transaction followed by its logs.
fn decode_block(block: &RawValue, receipts: &RawValue) -> Result<Vec<Event>, SourceError> {
    const BLOCK: &str = "eth_getBlockByNumber";
    const RECEIPTS: &str = "eth_getBlockReceipts";

    let mut block: RawObject<'_> =
        serde_json::from_str(block.get()).map_err(|error| malformed(BLOCK, error))?;
    let transactions = take_array(&mut block, "transactions", BLOCK)?;
    let receipts: Vec<RawObject<'_>> =
        serde_json::from_str(receipts.get()).map_err(|error| malformed(RECEIPTS, error))?;
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

    let height = quantity_field(&block, "number", BLOCK)?;
    let hash = hash_field(&block, "hash", BLOCK)?;
    let mut events = Vec::with_capacity(1 + transactions.len() * 2);
    events.push(Event::Block {
        height,
        hash,
        parent_hash: hash_field(&block, "parentHash", BLOCK)?,
        timestamp: quantity_field(&block, "timestamp", BLOCK)?,
        tx_count: transactions.len() as u64,
        raw: serde_json::to_string(&block).map_err(|error| malformed(BLOCK, error))?,
    });

    for (position, (transaction, mut receipt)) in transactions.into_iter().zip(receipts).enumerate()
    {
        let receipt_block = hash_field(&receipt, "blockHash", RECEIPTS)?;
        if receipt_block != hash {
            return Err(malformed(
                RECEIPTS,
                format!(
                    "receipt belongs to block {receipt_block}, not {hash}; the chain reorganised mid-request"
                ),
            ));
        }
        let tx_index = quantity_field(&receipt, "transactionIndex", RECEIPTS)?;
        if tx_index != position as u64 {
            return Err(malformed(
                RECEIPTS,
                format!("receipt {position} has transactionIndex {tx_index}"),
            ));
        }
        let tx_id = TxHash::from(hash_field(&receipt, "transactionHash", RECEIPTS)?);
        let logs = take_array(&mut receipt, "logs", RECEIPTS)?;

        events.push(Event::Transaction {
            height,
            block_hash: hash,
            tx_id,
            tx_index,
            raw: transaction.get().to_owned(),
            receipt: serde_json::to_string(&receipt).map_err(|error| malformed(RECEIPTS, error))?,
        });
        for (position, log) in logs.into_iter().enumerate() {
            let log_position: LogPosition<'_> =
                serde_json::from_str(log.get()).map_err(|error| malformed("log", error))?;
            let item_index = match log_position.log_index {
                Some(text) => {
                    decode_u64(text).map_err(|detail| malformed("log.logIndex", detail))?
                }
                None => position as u64,
            };
            events.push(Event::Log {
                height,
                block_hash: hash,
                tx_id,
                tx_index,
                item_index,
                raw: log.get().to_owned(),
            });
        }
    }
    Ok(events)
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
        let body = self
            .client
            .post(self.http_url.as_str())
            .json(&batch)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(transport)?
            .bytes()
            .await
            .map_err(transport)?;
        decode_batch(&body)
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
    use alloy_primitives::{B256, TxHash};
    use serde_json::{Value, json};

    use super::{decode_batch, decode_head_frame, decode_u64};
    use crate::envelope::Event;

    fn hash(byte: u8) -> String {
        format!("0x{}", format!("{byte:02x}").repeat(32))
    }

    fn block() -> Value {
        json!({
            "number": "0x112a880",
            "hash": hash(0xab),
            "parentHash": hash(0xaa),
            "timestamp": "0x6530a1b0",
            "miner": "0x4200000000000000000000000000000000000011",
            "transactions": [
                {"hash": hash(0x11), "from": "0xf0", "input": "0xdeadbeef"},
                {"hash": hash(0x22), "from": "0xf1", "input": "0x"},
            ],
        })
    }

    fn receipts() -> Value {
        json!([
            {
                "blockHash": hash(0xab),
                "transactionHash": hash(0x11),
                "transactionIndex": "0x0",
                "status": "0x1",
                "gasUsed": "0x5208",
                "logs": [
                    {"logIndex": "0x0", "address": "0xdead", "topics": ["0x1"]},
                    {"logIndex": "0x1", "address": "0xbeef", "topics": ["0x2"]},
                ],
            },
            {
                "blockHash": hash(0xab),
                "transactionHash": hash(0x22),
                "transactionIndex": "0x1",
                "status": "0x0",
                "gasUsed": "0x5208",
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

    fn raw_json(text: &str) -> Value {
        serde_json::from_str(text).expect("raw payload is json")
    }

    #[test]
    fn decodes_hex_quantities() {
        assert_eq!(decode_u64("0x0"), Ok(0));
        assert_eq!(decode_u64("0x10"), Ok(16));
        assert_eq!(decode_u64("ff"), Ok(255));
        assert!(decode_u64("0xzz").is_err());
    }

    #[test]
    fn events_are_block_then_each_transaction_followed_by_its_logs() {
        let fetched = decode_batch(&batch(&block(), &receipts())).expect("batch decodes");
        let kinds: Vec<&str> = fetched.events.iter().map(Event::kind).collect();
        assert_eq!(kinds, ["block", "transaction", "log", "log", "transaction"]);

        let Event::Block {
            height,
            tx_count,
            parent_hash,
            ..
        } = &fetched.events[0]
        else {
            panic!("first event must be the block marker");
        };
        assert_eq!(*height, 18_000_000);
        assert_eq!(*tx_count, 2);
        assert_eq!(parent_hash.to_string(), hash(0xaa));
    }

    #[test]
    fn every_field_is_published_exactly_once() {
        let fetched = decode_batch(&batch(&block(), &receipts())).expect("batch decodes");

        let Event::Block { raw, .. } = &fetched.events[0] else {
            panic!("first event must be the block marker");
        };
        let block_raw = raw_json(raw);
        assert_eq!(block_raw["miner"], block()["miner"]);
        assert!(
            block_raw.get("transactions").is_none(),
            "transactions are their own events"
        );

        let Event::Transaction {
            tx_id,
            tx_index,
            raw,
            receipt,
            ..
        } = &fetched.events[1]
        else {
            panic!("second event must be a transaction");
        };
        assert_eq!(*tx_id, hash(0x11).parse::<TxHash>().expect("tx id parses"));
        assert_eq!(*tx_index, 0);
        assert_eq!(raw_json(raw), block()["transactions"][0]);
        let receipt_raw = raw_json(receipt);
        assert_eq!(receipt_raw["status"], "0x1");
        assert_eq!(receipt_raw["gasUsed"], "0x5208");
        assert!(
            receipt_raw.get("logs").is_none(),
            "logs are their own events"
        );

        let Event::Log {
            raw, item_index, ..
        } = &fetched.events[3]
        else {
            panic!("fourth event must be a log");
        };
        assert_eq!(*item_index, 1);
        assert_eq!(raw_json(raw), receipts()[0]["logs"][1]);
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
    fn missing_log_index_falls_back_to_position() {
        let mut receipts = receipts();
        for log in receipts[0]["logs"]
            .as_array_mut()
            .expect("logs are an array")
        {
            log.as_object_mut()
                .expect("log is an object")
                .remove("logIndex");
        }
        let fetched = decode_batch(&batch(&block(), &receipts)).expect("batch decodes");
        let indices: Vec<u64> = fetched
            .events
            .iter()
            .filter_map(|event| match event {
                Event::Log { item_index, .. } => Some(*item_index),
                _ => None,
            })
            .collect();
        assert_eq!(indices, [0, 1]);
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
        assert!(
            message.contains("eth_getBlockByNumber") && message.contains("hash"),
            "{message}"
        );
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
