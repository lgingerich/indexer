//! The simulated JSON-RPC node, behind alloy's own client.
//!
//! [`SimNode`] is a tower service, so it slots in where the HTTP transport would and the
//! real retry layer still wraps it. [`SimHeads`] is a pubsub connector, so alloy's own
//! subscription service — reconnects and resubscriptions included — drives `newHeads`.
//! Responses are alloy's RPC types, serialized by alloy, so their shape is the one alloy
//! reads from a real node.
//!
//! The node behaves like a load-balanced provider. Each batch goes to one backend, and
//! sometimes its calls fan out to several; a backend may lag, serving an older view of
//! the chain, so a header and the logs fetched with it can disagree. Requests take a
//! seeded time, and with faults on are refused (`503`) or time out after the client's
//! ten seconds. The head subscription drops, skips heads, and repeats old ones.

use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use alloy_json_rpc::{PubSubItem, RequestPacket, ResponsePacket, SerializedRequest};
use alloy_pubsub::{ConnectionHandle, ConnectionInterface, PubSubConnect};
use alloy_rpc_types_eth::{Block, BlockTransactions, Log};
use alloy_transport::{TransportError, TransportErrorKind, TransportFut, TransportResult};
use serde_json::{Value, json};
use tokio::sync::broadcast;
use tower::Service;

use super::chain::{Shared, SimBlock, View, World, lock};
use crate::ingest::source::NetworkError;

/// The HTTP client's request timeout, which a request that never answers costs.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

/// The node's HTTP side: blocks and logs on request.
#[derive(Debug, Clone)]
pub(super) struct SimNode {
    pub(super) world: Shared,
}

impl Service<RequestPacket> for SimNode {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, packet: RequestPacket) -> Self::Future {
        let world = Arc::clone(&self.world);
        Box::pin(async move {
            let (delay, timeout) = {
                let mut world = lock(&world);
                let timeout = world.fault(|p| p.timeout);
                (world.rng.below(80), timeout)
            };
            if timeout {
                tokio::time::sleep(CLIENT_TIMEOUT).await;
                lock(&world).reached("rpc timed out");
                return Err(TransportErrorKind::custom(NetworkError::timeout(
                    "simulated timeout",
                )));
            }
            tokio::time::sleep(Duration::from_millis(delay)).await;
            let mut world = lock(&world);
            if world.fault(|p| p.refuse) {
                world.reached("rpc refused");
                return Err(TransportErrorKind::http_error(503, String::new()));
            }
            let response = match &packet {
                RequestPacket::Single(request) => {
                    let view = world.pick_view();
                    respond(&mut world, view, request)
                }
                RequestPacket::Batch(requests) => {
                    let split = world.fault(|p| p.split);
                    if split {
                        world.reached("batch split across backends");
                    }
                    let mut view = world.pick_view();
                    let mut responses = Vec::with_capacity(requests.len());
                    for request in requests {
                        if split {
                            view = world.pick_view();
                        }
                        responses.push(respond(&mut world, view, request));
                    }
                    Value::Array(responses)
                }
            };
            serde_json::from_value(response).map_err(TransportErrorKind::custom)
        })
    }
}

/// One call's response object, answered from `view`.
fn respond(world: &mut World, view: usize, request: &SerializedRequest) -> Value {
    let params: Value = request
        .params()
        .and_then(|params| serde_json::from_str(params.get()).ok())
        .unwrap_or(Value::Null);
    world
        .trace
        .push(format!("rpc {} {params} @{view}", request.method()));
    let view = world.view(view);
    let answer = match request.method() {
        "eth_getBlockByNumber" => block_by_number(&view, &params),
        "eth_getLogs" => logs(&view, &params),
        _ => Err((-32601, "method not found")),
    };
    let id = request.id();
    match answer {
        Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
        Err((code, message)) => {
            json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
        }
    }
}

type Answer = Result<Value, (i64, &'static str)>;

/// A hashes-only block, or `null` past the backend's tip.
fn block_by_number(view: &View<'_>, params: &Value) -> Answer {
    if params[1] != Value::Bool(false) {
        return Err((-32602, "the simulated node serves hashes-only blocks"));
    }
    let height = match params[0].as_str() {
        Some("latest") => view.tip(),
        Some(tag) => quantity(tag).ok_or((-32602, "bad block tag"))?,
        None => return Err((-32602, "missing block tag")),
    };
    Ok(view.at(height).map_or(Value::Null, render_block))
}

/// Logs for one block by hash, or for a range of the backend's canonical heights.
fn logs(view: &View<'_>, params: &Value) -> Answer {
    let filter = &params[0];
    if let Some(hash) = filter["blockHash"].as_str() {
        let hash = hash.parse().map_err(|_| (-32602, "bad block hash"))?;
        let block = view.known(&hash).ok_or((-32000, "unknown block"))?;
        return Ok(Value::Array(render_logs(block)));
    }
    let bound = |key: &str| filter[key].as_str().and_then(quantity);
    let (Some(from), Some(to)) = (bound("fromBlock"), bound("toBlock")) else {
        return Err((-32602, "the simulated node needs a block hash or a range"));
    };
    if to > view.tip() {
        return Err((-32000, "block range extends beyond current head block"));
    }
    Ok(Value::Array(
        (from..=to)
            .filter_map(|height| view.at(height))
            .flat_map(render_logs)
            .collect(),
    ))
}

fn quantity(tag: &str) -> Option<u64> {
    u64::from_str_radix(tag.strip_prefix("0x")?, 16).ok()
}

fn render_block(block: &SimBlock) -> Value {
    let body = Block::<alloy_rpc_types_eth::Transaction>::new(
        block.header.clone(),
        BlockTransactions::Hashes(block.transactions()),
    );
    serde_json::to_value(body).unwrap_or(Value::Null)
}

/// The block's logs as `eth_getLogs` returns them.
pub(super) fn render_logs(block: &SimBlock) -> Vec<Value> {
    let transactions = block.transactions();
    block
        .logs
        .iter()
        .zip(0_u64..)
        .map(|(log, index)| {
            let transaction_index = transactions
                .iter()
                .position(|hash| *hash == log.transaction_hash)
                .map(|index| index as u64);
            let log = Log {
                inner: alloy_primitives::Log::new_unchecked(
                    log.address,
                    log.topics.clone(),
                    log.data.clone(),
                ),
                block_hash: Some(block.hash()),
                block_number: Some(block.number()),
                block_timestamp: Some(block.header.inner.timestamp),
                transaction_hash: Some(log.transaction_hash),
                transaction_index,
                log_index: Some(index),
                removed: false,
            };
            serde_json::to_value(log).unwrap_or(Value::Null)
        })
        .collect()
}

/// The node's WebSocket side: `newHeads`, connected afresh after each drop.
#[derive(Debug, Clone)]
pub(super) struct SimHeads {
    pub(super) world: Shared,
}

impl PubSubConnect for SimHeads {
    fn is_local(&self) -> bool {
        true
    }

    async fn connect(&self) -> TransportResult<ConnectionHandle> {
        let (handle, interface) = ConnectionHandle::new();
        let heads = lock(&self.world).subscribe();
        tokio::spawn(serve_heads(Arc::clone(&self.world), interface, heads));
        Ok(handle.with_retry_interval(Duration::from_secs(1)))
    }
}

/// One connection: answers `eth_subscribe`, then announces each new canonical block —
/// or, with faults, skips it, repeats an older one, or drops the connection.
async fn serve_heads(
    world: Shared,
    mut interface: ConnectionInterface,
    mut heads: broadcast::Receiver<alloy_primitives::B256>,
) {
    const SUBSCRIPTION: &str = "0x1";
    let mut subscribed = false;
    let mut previous = None;
    loop {
        tokio::select! {
            // Requests first, so a subscription is answered before a head that arrived
            // alongside it; either way the order is the same every run.
            biased;
            request = interface.recv_from_frontend() => {
                let Some(request) = request else { return };
                let request: Value = serde_json::from_str(request.get()).unwrap_or(Value::Null);
                let method = request["method"].as_str().unwrap_or_default();
                lock(&world).trace.push(format!("ws {method}"));
                let result = match method {
                    "eth_subscribe" => {
                        subscribed = true;
                        json!(SUBSCRIPTION)
                    }
                    _ => json!(true),
                };
                let response = json!({"jsonrpc": "2.0", "id": request["id"], "result": result});
                if let Ok(item) = serde_json::from_value::<PubSubItem>(response) {
                    let _ = interface.send_to_frontend(item);
                }
            }
            head = heads.recv() => {
                let hash = match head {
                    Ok(hash) => hash,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return,
                };
                let mut world = lock(&world);
                if world.fault(|p| p.drop_heads) {
                    world.reached("head subscription dropped");
                    drop(world);
                    interface.close_with_error();
                    return;
                }
                if world.fault(|p| p.skip_head) {
                    world.reached("head skipped");
                    continue;
                }
                let announced = if world.fault(|p| p.stale_head) {
                    world.reached("stale head announced");
                    stale(&mut world).or(previous).unwrap_or(hash)
                } else {
                    hash
                };
                previous = Some(hash);
                let Some(header) = world.block(&announced).map(|block| block.header.clone()) else {
                    continue;
                };
                if subscribed {
                    world.trace.push(format!("ws head {announced}"));
                    let notification = json!({
                        "jsonrpc": "2.0",
                        "method": "eth_subscription",
                        "params": {"subscription": SUBSCRIPTION, "result": header},
                    });
                    if let Ok(item) = serde_json::from_value::<PubSubItem>(notification) {
                        let _ = interface.send_to_frontend(item);
                    }
                }
            }
        }
    }
}

/// The tip of a view a few steps old: possibly a block a reorg has since replaced.
fn stale(world: &mut World) -> Option<alloy_primitives::B256> {
    let index = world.newest().saturating_sub(1 + world.index_below(6));
    let view = world.view(index);
    view.at(view.tip()).map(SimBlock::hash)
}
