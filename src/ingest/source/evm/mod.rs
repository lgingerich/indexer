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
//!   `RANGE_LIMIT` heights — their headers and one `eth_getLogs` over the range. A
//!   range the provider fails on is fetched in halves, and the size that worked is
//!   remembered for the ranges after it (`Span`).
//! - The batch is parsed into alloy's RPC types, then projected field by field into
//!   the [`crate::wire::datasets`] records; nothing is kept as opaque JSON.
//! - Parsing uses alloy's *catch-all* (`any`) types, so a chain's non-Ethereum
//!   transaction types — an OP-stack deposit (`0x7e`), an Arbitrum retry (`0x6a`) —
//!   decode instead of failing the block, and chain-specific extras are captured.
//!   Everything read from them goes through the standard `Transaction` accessors,
//!   except the transaction type, which is kept as its raw `u8`.

mod fetch;
mod plan;
mod project;
#[cfg(test)]
mod testing;

use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};

use alloy_network::AnyNetwork;
use alloy_provider::{Provider, RootProvider, WsConnect};
use alloy_pubsub::PubSubConnect;
use alloy_rpc_client::{ClientBuilder, RpcClient};
use alloy_transport::TransportErrorKind;
use futures_util::StreamExt;
use tracing::warn;

use super::retry::{RetryLayer, os_seed};
use super::{
    BlockSource, Datasets, FetchedBlock, HeadStream, SourceError, inconsistent, malformed,
};
use crate::metrics;
use crate::wire::envelope::{BlockMeta, ChainId};

use fetch::{RpcBlockMeta, block_read, invalid_json, transport};
use plan::{FetchPlan, Span};
use project::project;
pub use project::{RpcBatch, decode_block};

/// The method labels these calls are reported under, shared by the fetch path and
/// the projection so an error names the same boundary wherever it is raised.
const BLOCK: &str = "eth_getBlockByNumber";

/// Why a `null` answer for a block, its header, or a receipt is [`inconsistent`] rather
/// than malformed: the heights the source is asked for are ones the chain has
/// announced or already buried, so the node has them, and a `null` is a backend behind
/// the one that announced the block, as a load-balanced provider's often is. The live
/// loop skips that head and catches up at the next; asking again succeeds.
const NOT_SERVED: &str = "result was null: the node does not serve it yet";
const RECEIPTS: &str = "eth_getBlockReceipts";
const LOGS: &str = "eth_getLogs";

/// A source that talks to one EVM chain: blocks over a JSON-RPC client, live heads
/// over a subscription connection `C`.
///
/// A deployment reads over HTTP ([`http_client`]) and subscribes over WebSocket
/// ([`ws_connect`]); a simulation passes in-memory ones instead.
pub struct EvmSource<C = WsConnect> {
    chain: ChainId,
    heads: C,
    provider: RootProvider<AnyNetwork>,
    plan: FetchPlan,
    span: Span,
    concurrency: NonZeroUsize,
    finality_depth: NonZeroU64,
}

impl<C> EvmSource<C> {
    /// Builds a source for `chain` that fetches `datasets` over `http` and subscribes
    /// to heads through `heads`, which is connected afresh for each subscription.
    /// Backfill keeps `concurrency` fetches in flight, and treats the heights more than
    /// `finality_depth` under the head as ones no fork reaches.
    ///
    /// A live notification's metadata stands in for the header only on the logs-only
    /// live path; every other dataset still reads the block body, and the header is
    /// what supplies linkage when no notification did.
    pub fn new(
        chain: impl Into<ChainId>,
        http: RpcClient,
        heads: C,
        datasets: Datasets,
        concurrency: NonZeroUsize,
        finality_depth: NonZeroU64,
    ) -> Self {
        Self {
            chain: chain.into(),
            heads,
            provider: RootProvider::new(http),
            plan: FetchPlan::new(datasets),
            span: Span::new(),
            concurrency,
            finality_depth,
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
    Ok(ClientBuilder::default()
        .layer(RetryLayer::new(os_seed()))
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

impl<C: PubSubConnect + Clone> BlockSource for EvmSource<C> {
    fn chain(&self) -> &ChainId {
        &self.chain
    }

    fn follows_heads(&self) -> bool {
        true
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

    /// Fetches `from ..= to` in ranges of the learned `Span` when the plan is ranged,
    /// and a height at a time otherwise.
    ///
    /// Logs per block vary widely, so a range a provider cannot serve is not the end: a
    /// transport failure — a refusal of the range's size, a timeout, or an error that
    /// outlasted the retry layer — splits the range in halves, the first rounded up,
    /// and fetches both, down to one height; only a single height's failure is
    /// returned. The span shrinks to the half, so the ranges after it start at a size
    /// that works. A range that comes back but fails its checks is returned as is: a
    /// smaller range would not fix data the node got wrong.
    async fn fetch_blocks(&self, from: u64, to: u64) -> Result<Vec<FetchedBlock>, SourceError> {
        let mut blocks = Vec::new();
        if !self.plan.ranged() {
            for height in from..=to {
                blocks.push(self.fetch_block(height, None).await?);
            }
            return Ok(blocks);
        }
        // Ranges still to fetch, the lowest last, so they are fetched in height order.
        let mut pending = vec![(from, to)];
        while let Some((low, high)) = pending.pop() {
            // Wider than the span, which may have shrunk since the caller cut the range:
            // split it without first asking for what is known to fail.
            let span = self.span.get();
            if high - low >= span {
                let cut = low + (span - 1);
                pending.extend([(cut + 1, high), (low, cut)]);
                continue;
            }
            match self.fetch_range(low, high).await {
                Ok(mut fetched) => {
                    self.span.served();
                    blocks.append(&mut fetched);
                }
                Err(error @ SourceError::Transport { .. }) if high > low => {
                    let cut = low + (high - low) / 2;
                    metrics::rpc_range_split();
                    warn!(from = low, to = high, %error, "range fetch failed; fetching it in halves");
                    self.span.shrink(cut - low + 1);
                    pending.extend([(cut + 1, high), (low, cut)]);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(blocks)
    }

    fn backfill_span(&self) -> u64 {
        if self.plan.ranged() {
            self.span.get()
        } else {
            1
        }
    }

    fn backfill_concurrency(&self) -> usize {
        self.concurrency.get()
    }

    fn finality_depth(&self) -> u64 {
        self.finality_depth.get()
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
            .map_err(|source| block_read(CONTEXT, source))?;
        let missing = format!("no block at {tag}");
        header.map(BlockMeta::from).ok_or_else(|| match height {
            Some(_) => inconsistent(CONTEXT, format!("{missing}: {NOT_SERVED}")),
            None => malformed(CONTEXT, missing),
        })
    }
}

/// Leaves out the subscription connection and the HTTP provider: both hold the node's
/// URL, which usually carries its API key, and a WebSocket connection can hold an
/// authorization header too.
impl<C> fmt::Debug for EvmSource<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EvmSource")
            .field("chain", &self.chain)
            .field("plan", &self.plan)
            .field("span", &self.span)
            .field("concurrency", &self.concurrency)
            .field("finality_depth", &self.finality_depth)
            .finish_non_exhaustive()
    }
}

/// Renders the source as its chain, for log fields.
impl<C> fmt::Display for EvmSource<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "evm[{}]", self.chain)
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::B256;
    use serde_json::{Value, json};

    use super::*;
    use crate::ingest::source::evm::testing::*;
    use crate::ingest::source::{BlockSource, Datasets, SourceError};
    use crate::wire::envelope::{BlockMeta, Event};

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
        rpc_source(url, serde_json::from_str(r#"["logs"]"#).expect("datasets"))
    }

    /// Answers a range's batch, refusing its log query when the range spans more than
    /// `largest` heights.
    fn refusing_reply(request: &Value, largest: u64) -> Value {
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
    }

    /// With transactions, a height is one batch of two calls — the full block and its
    /// receipts — matched to its answers however the node orders them, and the header is
    /// reported as metadata with no further call. Transactions come with their receipts,
    /// which are per block, so a backfill fetches a height at a time, the same way.
    #[tokio::test]
    async fn a_transaction_dataset_fetches_a_height_as_one_two_call_batch() {
        let (url, server) = rpc_server(2, |request| {
            let mut reply = primary_reply(request, &block(), &receipts());
            reply.as_array_mut().expect("batch").reverse();
            reply
        });
        let source = rpc_source(url, Datasets::all());
        assert_eq!(source.backfill_span(), 1);
        let fetched = source.fetch_block(18_000_000, None).await.expect("fetch");
        let backfilled = source
            .fetch_blocks(18_000_000, 18_000_000)
            .await
            .expect("backfill");
        assert_eq!(server.join().expect("server").len(), 2);
        assert_eq!(fetched.meta, block_meta());
        let kinds: Vec<&str> = fetched.events.iter().map(Event::kind).collect();
        assert_eq!(kinds, ["block", "transaction", "log", "log", "transaction"]);
        let [backfilled] = &backfilled[..] else {
            panic!("one block, not {}", backfilled.len());
        };
        assert_eq!(backfilled.meta, fetched.meta);
        assert_eq!(backfilled.events, fetched.events);
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
            let source = rpc_source(url, serde_json::from_str(selection).expect("datasets"));
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
        let kinds: Vec<&str> = fetched.events.iter().map(Event::kind).collect();
        assert_eq!(kinds, ["log"]);
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
        for (case, logs, bloom_at, requests) in [
            ("logs", vec![log_at(100, 0)], None, 1),
            ("no logs under an empty bloom", vec![], None, 2),
            ("no logs under a set bloom", vec![], Some(100), 2),
        ] {
            let count = logs.len();
            let (url, server) = rpc_server(requests, move |request| {
                if request.is_array() {
                    return range_reply(request, &logs, bloom_at);
                }
                assert_eq!(request["params"][0]["blockHash"], json!(hash(100)));
                json!({"jsonrpc": "2.0", "id": request["id"], "result": logs})
            });
            let fetched = logs_only(url).fetch_block(100, Some(&head)).await;
            assert_eq!(server.join().expect("server").len(), requests, "{case}");
            match bloom_at {
                None => {
                    let fetched = fetched.expect(case);
                    assert_eq!(fetched.meta, head, "{case}");
                    assert_eq!(fetched.events.len(), count, "{case}");
                }
                Some(_) => assert_eq!(
                    fetched.expect_err(case).to_string(),
                    "inconsistent data from eth_getLogs: no logs returned for block 100, whose logsBloom is set",
                    "{case}"
                ),
            }
        }
    }

    /// A log query answered by a backend that has not reached the block is a lagging
    /// node, as a header read's is, so the live loop asks again rather than stopping:
    /// Geth's `-32000 unknown block` for a hash-pinned query on the live path, and the
    /// same answer to the log query in a range's batch.
    #[tokio::test]
    async fn a_log_query_the_node_does_not_serve_yet_is_inconsistent() {
        const UNKNOWN: &str = "unknown block";
        let head = BlockMeta {
            height: 100,
            hash: hash(100).parse().expect("hash"),
            parent_hash: hash(99).parse().expect("hash"),
            timestamp: TIMESTAMP,
        };
        for (case, reused) in [("a hash-pinned query", Some(head)), ("a range", None)] {
            let (url, server) = rpc_server(1, |request| {
                let refuse = |call: &Value| {
                    json!({"jsonrpc": "2.0", "id": call["id"],
                        "error": {"code": -32000, "message": UNKNOWN}})
                };
                let Some(calls) = request.as_array() else {
                    return refuse(request);
                };
                let mut reply = range_reply(request, &[], None);
                let position = calls
                    .iter()
                    .position(|call| call["method"] == "eth_getLogs")
                    .expect("a log query");
                reply[position] = refuse(&calls[position]);
                reply
            });
            let error = logs_only(url)
                .fetch_block(100, reused.as_ref())
                .await
                .expect_err(case);
            assert_eq!(server.join().expect("server").len(), 1, "{case}");
            assert!(
                matches!(&error, SourceError::Inconsistent { context, detail }
                    if context == "eth_getLogs" && *detail == format!("{NOT_SERVED}: {UNKNOWN}")),
                "{case}: {error:?}"
            );
        }
    }

    /// A range the provider refuses is fetched in halves, the first rounded up, and every
    /// height is returned. The span learned from the refusal sizes the halves after it,
    /// so the second half is not refused first. The first attempt is capped at the range
    /// limit, however far the caller allows.
    #[tokio::test]
    async fn a_refused_range_is_fetched_in_halves_and_the_span_kept() {
        // 100..=224 is cut at the limit. 100..=199 is refused, then 100..=149; after that
        // every range is cut to twenty-five before it is sent — 150..=199 and 200..=224
        // included — so nothing else is refused.
        let spans = vec![100, 50, 25, 25, 25, 25, 25];
        let (url, server) = rpc_server(spans.len(), move |request| refusing_reply(request, 25));
        let source = logs_only(url);
        let blocks = source.fetch_blocks(100, 224).await.expect("every height");
        let observed = server.join().expect("server");
        assert_eq!(observed.iter().map(span).collect::<Vec<_>>(), spans);
        let heights: Vec<u64> = blocks.iter().map(|block| block.meta.height).collect();
        assert_eq!(heights, (100..=224).collect::<Vec<_>>());
        assert_eq!(source.backfill_span(), 25);
    }

    /// A single height the provider still refuses fails the whole call: nothing of it is
    /// returned.
    #[tokio::test]
    async fn a_refused_single_height_fails_the_range() {
        let spans = vec![2, 1];
        let (url, server) = rpc_server(spans.len(), move |request| refusing_reply(request, 0));
        let result = logs_only(url).fetch_blocks(100, 101).await;
        let observed = server.join().expect("server");
        assert_eq!(observed.iter().map(span).collect::<Vec<_>>(), spans);
        assert!(
            matches!(
                &result,
                Err(SourceError::Transport {
                    context: "eth_getLogs",
                    source: alloy_json_rpc::RpcError::ErrorResp(payload),
                }) if payload.code == -32602
            ),
            "{result:?}"
        );
    }

    /// A range that comes back but breaks a range check fails as it is: a smaller range
    /// would not fix data the node got wrong.
    #[tokio::test]
    async fn a_range_that_fails_its_checks_is_not_retried_smaller() {
        const MISORDERED: &str = "malformed data from eth_getLogs: logs of block 101 are not in ascending logIndex order";
        const LEFTOVER: &str =
            "malformed data from eth_getLogs: rows outside the blocks 100..=102, or out of order";
        let mut wrong_hash = log_at(101, 0);
        wrong_hash["blockHash"] = json!(hash(0xee));
        let another_block = format!(
            "inconsistent data from eth_getLogs: log belongs to block {}, not {}",
            hash(0xee),
            hash(101)
        );
        for (case, logs, bloom_at, expected) in [
            ("a log from another block", vec![wrong_hash], None, another_block),
            (
                "logs out of order",
                vec![log_at(101, 1), log_at(101, 0)],
                None,
                MISORDERED.to_owned(),
            ),
            (
                "a duplicate log",
                vec![log_at(101, 0), log_at(101, 0)],
                None,
                MISORDERED.to_owned(),
            ),
            ("a log below the range", vec![log_at(99, 0)], None, LEFTOVER.to_owned()),
            ("a log above the range", vec![log_at(103, 0)], None, LEFTOVER.to_owned()),
            (
                "no logs for a header whose bloom is set",
                vec![],
                Some(101),
                "inconsistent data from eth_getLogs: no logs returned for block 101, whose logsBloom is set"
                    .to_owned(),
            ),
        ] {
            let (url, server) = rpc_server(1, move |request| range_reply(request, &logs, bloom_at));
            let error = logs_only(url).fetch_blocks(100, 102).await.expect_err(case);
            assert_eq!(error.to_string(), expected, "{case}");
            assert_eq!(server.join().expect("server").len(), 1, "{case}");
        }
    }

    /// A reusable head stands in for the header only when logs are the only thing
    /// fetched. Anything else reads the block body, so a `transactions` dataset still
    /// fetches it and still emits its rows — silently dropping them would index
    /// transactions for history and not for live blocks.
    #[tokio::test]
    async fn a_reusable_head_still_reads_the_body_for_a_transaction_dataset() {
        // One batch, two calls: the body is read even though a head was announced.
        let (url, server) = rpc_server(1, |request| primary_reply(request, &block(), &receipts()));
        let source = rpc_source(
            url,
            serde_json::from_str(r#"["transactions", "logs"]"#).expect("datasets"),
        );
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
        assert_eq!(
            fetched.meta,
            block_meta(),
            "the body's metadata, not the head's"
        );
        server.join().expect("server");
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

    /// A source is logged with `{:?}`, and its endpoints' URLs, which hold the API key,
    /// are not in it.
    #[test]
    fn a_source_does_not_print_its_endpoint_urls() {
        let source = from_urls(
            "ethereum",
            "https://rpc.example/v2/secret-http-key",
            "wss://ws.example/v2/secret-ws-key",
            Datasets::all(),
        )
        .expect("source");
        let printed = format!("{source:?}");
        assert!(!printed.contains("secret"), "{printed}");
        assert!(printed.contains("ethereum"), "{printed}");
    }
}
