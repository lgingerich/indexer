//! The node's calls, and what each failure of them means.

use alloy_json_rpc::RpcError;
use alloy_network::any::{AnyRpcBlock, AnyTransactionReceipt};
use alloy_primitives::B256;
use alloy_provider::Provider;
use alloy_rpc_types_eth::Filter;
use alloy_rpc_types_eth::Log as RpcLog;
use alloy_transport::TransportError;
use serde::Deserialize;

use super::plan::FetchPlan;
use super::plan::{BATCH_LIMIT, LogSource};
use super::project::project;
use super::{BLOCK, EvmSource, LOGS, NOT_SERVED, RECEIPTS};
use crate::ingest::source::assemble::{check_height, check_log_order, check_no_leftovers};
use crate::ingest::source::retry::{NetworkError, not_reached};
use crate::ingest::source::{FetchedBlock, METHOD_NOT_FOUND, SourceError, inconsistent};
use crate::wire::envelope::BlockMeta;

/// A block's header fields a `newHeads` notification carries: identity, parent
/// linkage, and timestamp. Every other header field is ignored here.
#[derive(Debug, Deserialize)]
pub(super) struct RpcBlockMeta {
    // A quantity on the wire; alloy's `quantity` handles the `0x` hex form and
    // rejects anything else. `B256`'s own `Deserialize` parses the hash.
    #[serde(with = "alloy_serde::quantity")]
    pub(super) number: u64,
    pub(super) hash: B256,
    #[serde(rename = "parentHash")]
    pub(super) parent_hash: B256,
    #[serde(with = "alloy_serde::quantity")]
    pub(super) timestamp: u64,
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

pub(super) fn invalid_json(context: &'static str, source: serde_json::Error) -> SourceError {
    SourceError::Json { context, source }
}

/// [`transport`], for a read of a block, header, receipt, or log at a height, except that an
/// error response saying the node has not reached that height is [`inconsistent`]: it
/// is a `null` answer in other words, from a backend behind the one that announced the
/// block (see [`NOT_SERVED`]). Envio's `HyperRPC` answers so, with `-32602` and "requested
/// block N is greater than latest block N-1".
pub(super) fn block_read(context: &'static str, source: TransportError) -> SourceError {
    if let RpcError::ErrorResp(payload) = &source
        && not_reached(&payload.message)
    {
        return inconsistent(context, format!("{NOT_SERVED}: {}", payload.message));
    }
    transport(context, source)
}

/// The error for a failed call at `context`, with any HTTP client error in it a
/// [`NetworkError`], which carries no request URL: a provider's URL usually holds its
/// API key, and these errors are logged. The retry layer has converted those already;
/// this covers a client built without it.
pub(super) fn transport(context: &'static str, source: TransportError) -> SourceError {
    SourceError::Transport {
        context,
        source: NetworkError::from_transport(source),
    }
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
        check_height(BLOCK, height, meta.height)?;
        // In order, this height's logs are the next run carrying its number.
        let own = match rest {
            Some(logs) => {
                let count = logs
                    .iter()
                    .take_while(|log| log.block_number == Some(height))
                    .count();
                let (own, after) = logs.split_at(count);
                check_log_order(LOGS, height, own.iter().map(|log| log.log_index))?;
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
    let last = (from + headers.len() as u64).saturating_sub(1);
    check_no_leftovers(LOGS, from, last, rest.is_some_and(|rest| !rest.is_empty()))?;
    Ok(blocks)
}

impl<C> EvmSource<C> {
    /// Fetches the block's receipts one transaction at a time, in index order.
    ///
    /// The fallback for nodes without `eth_getBlockReceipts`. The hashes are requested
    /// in chunks of [`BATCH_LIMIT`], so no batch outruns a provider's per-second limit.
    pub(super) async fn fetch_receipts(
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
                        .map_err(|source| block_read(RECEIPT, source))?
                        .ok_or_else(|| inconsistent(RECEIPT, NOT_SERVED))?,
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
    pub(super) async fn fetch_batch(
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
            .map_err(|source| block_read(BLOCK, source))?
            .ok_or_else(|| inconsistent(BLOCK, NOT_SERVED))?;
        let receipts = match receipts.await {
            Ok(receipts) => receipts,
            Err(RpcError::ErrorResp(error)) if error.code == METHOD_NOT_FOUND => None,
            Err(source) => return Err(block_read(RECEIPTS, source)),
        };
        Ok((block, receipts))
    }

    /// Fetches one height's logs, pinned to `hash` so a reorg cannot silently answer
    /// with the replacement branch: the query returns that block's logs or nothing.
    pub(super) async fn fetch_logs(&self, hash: B256) -> Result<Vec<RpcLog>, SourceError> {
        let filter = Filter::new().at_block_hash(hash);
        self.provider
            .client()
            .request(LOGS, (filter,))
            .await
            .map_err(|source| block_read(LOGS, source))
    }

    /// Fetches the heights `from ..= to` in one batch: each height's hashes-only header
    /// and, when logs are selected, one `eth_getLogs` over the range. One height is a
    /// range of one, so this is also the per-height fetch of a plan without
    /// transactions.
    pub(super) async fn fetch_range(
        &self,
        from: u64,
        to: u64,
    ) -> Result<Vec<FetchedBlock>, SourceError> {
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
                    .map_err(|source| block_read(BLOCK, source))?
                    .ok_or_else(|| inconsistent(BLOCK, NOT_SERVED))?,
            );
        }
        let logs = match logs {
            Some(logs) => Some(logs.await.map_err(|source| block_read(LOGS, source))?),
            None => None,
        };
        split_range(from, &blocks, logs.as_deref(), self.plan)
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
    use crate::wire::envelope::Event;

    /// A node behind the one that announced a block answers it `null`. That is a
    /// backend catching up, which the live loop skips and retries, not malformed data
    /// that stops the run. A node that answers a height it has not reached with an error
    /// rather than `null`, as Envio's `HyperRPC` does, is just as inconsistent. Any other
    /// error response is still the transport failure it was.
    #[tokio::test]
    async fn a_block_the_node_does_not_serve_yet_is_inconsistent() {
        const BEYOND: &str =
            "Invalid params: requested block 18000000 is greater than latest block 17999999";
        let cases: [(&str, Value, fn(&SourceError) -> bool); 3] = [
            ("a null block", json!({"result": null}), |error| {
                matches!(error, SourceError::Inconsistent { context, detail }
                    if context == "eth_getBlockByNumber" && detail == NOT_SERVED)
            }),
            (
                "a block beyond the latest",
                json!({"error": {"code": -32602, "message": BEYOND}}),
                |error| {
                    matches!(error, SourceError::Inconsistent { context, detail }
                        if context == "eth_getBlockByNumber"
                            && *detail == format!("{NOT_SERVED}: {BEYOND}"))
                },
            ),
            (
                "another error",
                json!({"error": {"code": -32000, "message": "execution aborted"}}),
                |error| {
                    matches!(error, SourceError::Transport {
                        context: "eth_getBlockByNumber",
                        source: RpcError::ErrorResp(payload),
                    } if payload.code == -32000)
                },
            ),
        ];
        for (case, answer, expected) in cases {
            let error = fetch_primary(move |request| {
                let calls = request.as_array().expect("a batch");
                Value::Array(
                    calls
                        .iter()
                        .map(|call| {
                            let mut response = json!({"jsonrpc": "2.0", "id": call["id"]});
                            response
                                .as_object_mut()
                                .expect("response")
                                .extend(answer.as_object().expect("answer").clone());
                            response
                        })
                        .collect(),
                )
            })
            .await
            .expect_err(case);
            assert!(expected(&error), "{case}: {error:?}");
        }
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
            let source = rpc_source(url, Datasets::all());
            let fetched = source
                .fetch_block(18_000_000, None)
                .await
                .expect("fallback fetch");
            assert_eq!(fetched.events.len(), 22, "unsupported: {unsupported}");
            let observed = server.join().expect("server");
            assert_eq!(
                observed
                    .iter()
                    .map(|batch| batch.as_array().expect("batch").len())
                    .collect::<Vec<_>>(),
                [2, 10, 10, 1],
                "unsupported: {unsupported}"
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
            let error = fetch_primary(move |request| {
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
            })
            .await
            .expect_err("must fail");
            if malformed {
                assert!(
                    matches!(
                        error,
                        SourceError::Transport {
                            context: "eth_getBlockByNumber",
                            source: RpcError::DeserError { .. }
                        }
                    ),
                    "{error:?}"
                );
            } else {
                assert!(
                    matches!(error, SourceError::Transport { context: "eth_getBlockReceipts", source: RpcError::ErrorResp(ref payload) } if payload.code == -32000),
                    "{error:?}"
                );
            }
        }
    }

    /// A provider's URL holds its API key, and a failed request's error is logged with
    /// `{:?}`, so neither rendering may carry the URL reqwest attaches. The listener
    /// stays bound and never answers, so each request times out and is retried with
    /// backoff first; paused time skips those waits.
    #[tokio::test(start_paused = true)]
    async fn a_transport_error_does_not_carry_the_endpoint_url() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("address");
        let url = format!("http://{address}/v2/secret-api-key");
        let error = rpc_source(url, Datasets::all())
            .fetch_block(1, None)
            .await
            .expect_err("nothing answers");
        drop(listener);

        assert!(
            matches!(
                error,
                SourceError::Transport {
                    context: "rpc batch",
                    ..
                }
            ),
            "{error:?}"
        );
        let printed = format!("{error} {error:?}");
        assert!(!printed.contains("secret-api-key"), "{printed}");
    }

    /// A log's transaction hash and its index in the block are its identity, and a log
    /// lacking either is rejected rather than filled in. An absent `logIndex` fails
    /// alloy's decode. An explicit `null` is the case the old fallback got wrong: alloy
    /// accepts it and hands over `None`, and the fallback supplied the log's position
    /// *within its receipt*, which is not the block index the field means. On a block
    /// whose second transaction emits a log the two differ, and the wrong value collides
    /// on dedupe with a different log. A defaulted transaction hash would make unrelated
    /// logs dedupe together the same way.
    #[tokio::test]
    async fn a_log_without_its_identity_is_rejected() {
        let cases: [(&str, fn(&mut Value), fn(&SourceError) -> bool); 3] = [
            (
                "a missing logIndex",
                |log| {
                    log.as_object_mut().expect("log").remove("logIndex");
                },
                |error| {
                    matches!(
                        error,
                        SourceError::Transport {
                            context: "eth_getBlockReceipts",
                            source: RpcError::DeserError { .. }
                        }
                    )
                },
            ),
            (
                "a null logIndex",
                |log| log["logIndex"] = Value::Null,
                |error| {
                    error.to_string()
                        == "malformed data from eth_getBlockReceipts: log has no logIndex; it cannot be placed in the block"
                },
            ),
            (
                "a missing transactionHash",
                |log| {
                    log.as_object_mut().expect("log").remove("transactionHash");
                },
                |error| {
                    error.to_string()
                        == "malformed data from eth_getBlockReceipts: log has no transactionHash"
                },
            ),
        ];
        for (case, change, expected) in cases {
            let mut receipts = receipts();
            for log in receipts[0]["logs"].as_array_mut().expect("logs") {
                change(log);
            }
            let error = fetch_primary(move |request| primary_reply(request, &block(), &receipts))
                .await
                .expect_err(case);
            assert!(expected(&error), "{case}: {error:?}");
        }
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

        let fetched = fetch_primary(move |request| primary_reply(request, &block, &receipts))
            .await
            .expect("OP deposit block decodes");
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
}
