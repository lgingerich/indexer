//! An EVM block source: live heads over WebSocket, full blocks over JSON-RPC.
//!
//! The two transports map onto the two roles in [`BlockSource`]. `newHeads` over
//! WebSocket is the live path, and it is the only way to observe a head without
//! paying a poll interval. JSON-RPC over HTTP is the pull path, used for full
//! blocks and receipts. Requirements worth stating because they are easy to get
//! wrong:
//!
//! - Full transactions are requested with `false`; the indexer does not decode
//!   calldata in v1, and `true` inflates every response for nothing.
//! - Receipts come from `eth_getBlockReceipts`, which is far cheaper than one
//!   `eth_getTransactionReceipt` per transaction. Some non-Ethereum nodes lack it.
//! - Finality comes from the node's `finalized` block tag, so each chain's own
//!   rules apply: about two epochs on Ethereum, L1 finality of the batch on an L2.
//! - All three RPC calls are sent as one batch, so a block costs one round trip.

use std::fmt;

use futures_util::{SinkExt as _, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use alloy_primitives::{B256, TxHash};

use super::{BlockId, BlockSource, Head, HeadStream, RawBlock, SourceError};
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

/// A live `newHeads` notification.
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

/// A full block, minus transaction bodies.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcBlock {
    number: String,
    hash: String,
    parent_hash: String,
    timestamp: String,
    transactions: Vec<String>,
}

/// A transaction receipt. Logs stay as [`Value`] so their verbatim JSON survives.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcReceipt {
    transaction_hash: String,
    transaction_index: String,
    #[serde(default)]
    logs: Vec<Value>,
}

/// One JSON-RPC response inside a batch.
#[derive(Debug, Deserialize)]
struct RpcResponse {
    id: u64,
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<RpcErrorObject>,
}

#[derive(Debug, Deserialize)]
struct RpcErrorObject {
    code: i64,
    message: String,
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
fn take_result(responses: &[RpcResponse], id: u64, method: &str) -> Result<Value, SourceError> {
    let response = responses.iter().find(|response| response.id == id);
    let Some(response) = response else {
        return Err(SourceError::Malformed {
            context: method.to_owned(),
            detail: format!("batch response had no entry with id {id}"),
        });
    };
    if let Some(error) = &response.error {
        return Err(SourceError::Transport(format!(
            "{method} returned error {}: {}",
            error.code, error.message
        )));
    }
    match &response.result {
        Some(value) if !value.is_null() => Ok(value.clone()),
        _ => Err(SourceError::Malformed {
            context: method.to_owned(),
            detail: "result was null".to_owned(),
        }),
    }
}

/// Reads the height and hash out of a block header.
fn decode_block_id(header: Value) -> Result<BlockId, SourceError> {
    let head: RpcHead = serde_json::from_value(header).map_err(|error| SourceError::Malformed {
        context: "finalized block".to_owned(),
        detail: error.to_string(),
    })?;
    let head = head.into_head().map_err(|detail| SourceError::Malformed {
        context: "finalized block".to_owned(),
        detail,
    })?;
    Ok(BlockId {
        height: head.height,
        hash: head.hash,
    })
}

/// Decodes one WebSocket frame into a head, when the frame is a head notification.
fn decode_head_frame(text: &str) -> Option<Result<Head, SourceError>> {
    let message: SubscriptionMessage = match serde_json::from_str(text) {
        Ok(message) => message,
        Err(error) => {
            return Some(Err(SourceError::Malformed {
                context: "websocket frame".to_owned(),
                detail: error.to_string(),
            }));
        }
    };
    if message.method.as_deref() != Some("eth_subscription") {
        // The subscription confirmation and any other notification land here.
        return None;
    }
    let head = message.params.and_then(|params| params.result)?;
    Some(head.into_head().map_err(|detail| SourceError::Malformed {
        context: "newHeads".to_owned(),
        detail,
    }))
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

    async fn block_at(&self, height: u64) -> Result<RawBlock, SourceError> {
        let tag = format!("0x{height:x}");
        let batch = json!([
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getBlockByNumber",
                "params": [tag, false],
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
        let responses: Vec<RpcResponse> = self
            .client
            .post(self.http_url.as_str())
            .json(&batch)
            .send()
            .await
            .map_err(|error| SourceError::Transport(error.to_string()))?
            .json()
            .await
            .map_err(|error| SourceError::Malformed {
                context: "rpc batch".to_owned(),
                detail: error.to_string(),
            })?;

        let block = take_result(&responses, 1, "eth_getBlockByNumber")?;
        let receipts = take_result(&responses, 2, "eth_getBlockReceipts")?;
        let finalized = decode_block_id(take_result(&responses, 3, "finalized block")?)?;
        Ok(RawBlock {
            raw_block: block.to_string(),
            raw_receipts: receipts.to_string(),
            finalized,
        })
    }

    fn encode_block(&self, raw: &RawBlock) -> Result<Vec<Event>, SourceError> {
        decode_block(&raw.raw_block, &raw.raw_receipts)
    }
}

/// Turns verbatim block and receipt payloads into ordered events.
fn decode_block(raw_block: &str, raw_receipts: &str) -> Result<Vec<Event>, SourceError> {
    let malformed = |context: &str, detail: String| SourceError::Malformed {
        context: context.to_owned(),
        detail,
    };
    let block: RpcBlock = serde_json::from_str(raw_block)
        .map_err(|error| malformed("eth_getBlockByNumber", error.to_string()))?;
    let receipts: Vec<RpcReceipt> = serde_json::from_str(raw_receipts)
        .map_err(|error| malformed("eth_getBlockReceipts", error.to_string()))?;

    let height = decode_u64(&block.number).map_err(|detail| malformed("block.number", detail))?;
    let hash = decode_hash(&block.hash).map_err(|detail| malformed("block.hash", detail))?;
    let parent_hash =
        decode_hash(&block.parent_hash).map_err(|detail| malformed("block.parentHash", detail))?;
    let timestamp =
        decode_u64(&block.timestamp).map_err(|detail| malformed("block.timestamp", detail))?;

    let mut events = Vec::with_capacity(receipts.len());
    events.push(Event::Block {
        height,
        hash,
        parent_hash,
        timestamp,
        tx_count: block.transactions.len() as u64,
        raw: raw_block.to_owned(),
    });

    for receipt in &receipts {
        let tx_index = decode_u64(&receipt.transaction_index)
            .map_err(|detail| malformed("receipt.transactionIndex", detail))?;
        let tx_id = match receipt.transaction_hash.parse::<TxHash>() {
            Ok(tx_id) => tx_id,
            Err(error) => {
                return Err(malformed("receipt.transactionHash", error.to_string()));
            }
        };
        for (position, log) in receipt.logs.iter().enumerate() {
            let item_index = match log.get("logIndex").and_then(Value::as_str) {
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
                raw: log.to_string(),
            });
        }
    }
    Ok(events)
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

    use super::BlockSource;
    use super::{EvmSource, decode_block, decode_block_id, decode_head_frame, decode_u64};
    use crate::envelope::Event;

    const BLOCK: &str = r#"{
        "number": "0x112a880",
        "hash": "0x0000000000000000000000000000000000000000000000000000000000000abc",
        "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000def",
        "timestamp": "0x6530a1b0",
        "transactions": ["0x1111111111111111111111111111111111111111111111111111111111111111", "0x2222222222222222222222222222222222222222222222222222222222222222"]
    }"#;

    const RECEIPTS: &str = r#"[
        {
            "transactionHash": "0x1111111111111111111111111111111111111111111111111111111111111111",
            "transactionIndex": "0x0",
            "logs": [
                {"logIndex": "0x0", "address": "0xdead", "topics": ["0x1"]},
                {"logIndex": "0x1", "address": "0xbeef", "topics": ["0x2"]}
            ]
        },
        {
            "transactionHash": "0x2222222222222222222222222222222222222222222222222222222222222222",
            "transactionIndex": "0x1",
            "logs": []
        }
    ]"#;

    #[test]
    fn decodes_hex_quantities() {
        assert_eq!(decode_u64("0x0"), Ok(0));
        assert_eq!(decode_u64("0x10"), Ok(16));
        assert_eq!(decode_u64("ff"), Ok(255));
        assert!(decode_u64("0xzz").is_err());
    }

    #[test]
    fn decode_block_leads_with_a_block_marker_then_logs() {
        let events = decode_block(BLOCK, RECEIPTS).expect("fixture decodes");
        assert_eq!(events.len(), 3);

        let Event::Block {
            height,
            tx_count,
            parent_hash,
            ..
        } = &events[0]
        else {
            panic!("first event must be the block marker");
        };
        assert_eq!(*height, 18_000_000);
        assert_eq!(*tx_count, 2);
        assert_eq!(parent_hash.to_string(), format!("0x{:0>64}", "def"));

        let Event::Log {
            tx_id,
            tx_index,
            item_index,
            ..
        } = &events[1]
        else {
            panic!("second event must be a log");
        };
        assert_eq!(
            *tx_id,
            "0x1111111111111111111111111111111111111111111111111111111111111111"
                .parse::<TxHash>()
                .expect("tx id parses")
        );
        assert_eq!(*tx_index, 0);
        assert_eq!(*item_index, 0);
    }

    #[test]
    fn log_raw_payload_keeps_every_field_verbatim() {
        let events = decode_block(BLOCK, RECEIPTS).expect("fixture decodes");
        let Event::Log { raw, .. } = &events[1] else {
            panic!("second event must be a log");
        };
        let value: serde_json::Value = serde_json::from_str(raw).expect("raw is json");
        assert_eq!(value["address"], "0xdead");
        assert_eq!(value["topics"][0], "0x1");
    }

    #[test]
    fn missing_log_index_falls_back_to_position() {
        let receipts = r#"[{"transactionHash":"0x1111111111111111111111111111111111111111111111111111111111111111","transactionIndex":"0x0",
            "logs":[{"address":"0x1"},{"address":"0x2"}]}]"#;
        let events = decode_block(BLOCK, receipts).expect("fixture decodes");
        let indices: Vec<u64> = events
            .iter()
            .filter_map(|event| match event {
                Event::Log { item_index, .. } => Some(*item_index),
                _ => None,
            })
            .collect();
        assert_eq!(indices, [0, 1]);
    }

    #[test]
    fn malformed_payload_reports_its_context() {
        let error = decode_block("{}", RECEIPTS).expect_err("missing fields must fail");
        assert!(error.to_string().contains("eth_getBlockByNumber"));
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

    #[test]
    fn finalized_header_decodes_to_its_height_and_hash() {
        let header = serde_json::from_str(BLOCK).expect("fixture is json");
        let id = decode_block_id(header).expect("header decodes");
        assert_eq!(id.height, 18_000_000);
        assert_eq!(id.hash.to_string(), format!("0x{:0>64}", "abc"));
    }

    #[test]
    fn finalized_header_without_a_hash_is_malformed() {
        let error = decode_block_id(serde_json::json!({"number": "0x1"}))
            .expect_err("missing hash must fail");
        assert!(error.to_string().contains("finalized block"));
    }

    #[test]
    fn encode_block_matches_the_pure_decoder() {
        let source = EvmSource::new("ethereum", "http://localhost:8545", "ws://localhost:8546");
        let raw = super::RawBlock {
            raw_block: BLOCK.to_owned(),
            raw_receipts: RECEIPTS.to_owned(),
            finalized: super::BlockId {
                height: 17_999_936,
                hash: B256::ZERO,
            },
        };
        let via_trait = source.encode_block(&raw).expect("encodes");
        let direct = decode_block(BLOCK, RECEIPTS).expect("encodes");
        assert_eq!(via_trait, direct);
    }
}
