//! The fixtures and fake node the EVM source's tests read from.

#![expect(clippy::expect_used)]

use serde_json::{Value, json};

use super::*;
use crate::ingest::source::{BlockSource, FetchedBlock, SourceError};
use crate::wire::datasets::Datasets;
use crate::wire::envelope::BlockMeta;

/// A source over real HTTP and WebSocket endpoints, as a deployment builds one.
pub(super) fn from_urls(
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
        NonZeroUsize::MIN,
        NonZeroU64::MAX,
    ))
}

/// A source reading the RPC node at `url`, with no subscription.
pub(super) fn rpc_source(url: String, datasets: Datasets) -> EvmSource {
    from_urls("ethereum", url, "ws://unused", datasets).expect("source")
}

/// Matches the `block()` fixture's `timestamp`, so metadata and body agree.
pub(super) const TIMESTAMP: u64 = 0x6530_a1b0;

/// A real HTTP boundary: answers each of `requests` requests with `reply`, and
/// returns what was asked.
pub(super) fn rpc_server(
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

pub(super) fn primary_reply(request: &Value, block: &Value, receipts: &Value) -> Value {
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

/// Fetches block 18,000,000 with every dataset from a node that answers its one
/// request with `reply`.
pub(super) async fn fetch_primary(
    reply: impl FnMut(&Value) -> Value + Send + 'static,
) -> Result<FetchedBlock, SourceError> {
    let (url, server) = rpc_server(1, reply);
    let fetched = rpc_source(url, Datasets::all())
        .fetch_block(18_000_000, None)
        .await;
    assert_eq!(server.join().expect("server").len(), 1);
    fetched
}

/// The metadata of the `block()` fixture.
pub(super) fn block_meta() -> BlockMeta {
    BlockMeta {
        height: 18_000_000,
        hash: hash(0xab).parse().expect("hash"),
        parent_hash: hash(0xaa).parse().expect("hash"),
        timestamp: TIMESTAMP,
    }
}

pub(super) fn hash(byte: u8) -> String {
    format!("0x{}", format!("{byte:02x}").repeat(32))
}

pub(super) fn address(byte: u8) -> String {
    format!("0x{}", format!("{byte:02x}").repeat(20))
}

pub(super) fn bloom() -> String {
    format!("0x{}", "00".repeat(256))
}

/// A block as `eth_getBlockByNumber` returns it with full transactions, so it
/// carries every header field the `blocks` dataset reads.
pub(super) fn block() -> Value {
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
pub(super) fn receipts() -> Value {
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
