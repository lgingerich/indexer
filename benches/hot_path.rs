//! Microbenchmarks for the hot path: dataset projection and event serialisation.
//!
//! Alloy owns RPC response decoding. This measures projection of its typed block
//! and receipts into events, and rendering each event to JSON. Fixture creation
//! and cloning stay outside the timed projection, so no duplicate RPC parser is
//! needed.
//!
//! Run with `cargo bench`. The harness is hand-rolled and dependency-free so it
//! can report percentiles rather than means, which is the number that matters for
//! a latency target.

#![expect(clippy::print_stdout, clippy::expect_used)]

use std::hint::black_box;
use std::time::{Duration, Instant};

use indexer::ingest::source::evm::{RpcBatch, decode_block};
use indexer::wire::envelope::{ChainId, Envelope};
use serde_json::json;

/// Timed samples collected per benchmark.
const SAMPLES: usize = 1_000;

/// Picks a percentile without floating point, so the index cannot round out of
/// bounds.
fn percentile(sorted: &[Duration], numerator: usize, denominator: usize) -> Duration {
    let last = sorted.len().saturating_sub(1);
    let index = (sorted.len() * numerator / denominator).min(last);
    sorted.get(index).copied().unwrap_or_default()
}

/// Prints min/p50/p90/p99/mean and per-`unit` throughput for one benchmark.
fn report(label: &str, samples: &mut [Duration], unit: &str) {
    samples.sort_unstable();
    let Some(min) = samples.first().copied() else {
        return;
    };
    let total: Duration = samples.iter().sum();
    let count = u32::try_from(samples.len()).expect("sample count fits in u32");
    let mean = total / count;
    let p50 = percentile(samples, 50, 100);

    println!("{label}");
    println!(
        "  min {min:>10.3?}  p50 {p50:>10.3?}  p90 {:>10.3?}  p99 {:>10.3?}  mean {mean:>10.3?}",
        percentile(samples, 90, 100),
        percentile(samples, 99, 100),
    );
    let seconds = p50.as_secs_f64();
    if seconds > 0.0 {
        println!("  p50 throughput: {:.0} {unit}s/s", 1.0 / seconds);
    }
}

/// Builds typed input for [`decode_block`]: full transactions and receipts.
///
/// Synthetic rather than captured so the benchmark can scale the input, and so the
/// repository does not carry a multi-megabyte fixture. The field names and value
/// shapes match what a node returns, including a typical EIP-1559 transaction.
fn synthetic_batch(tx_count: usize, logs_per_tx: usize) -> RpcBatch {
    let block_hash = format!("0x{:064x}", 0xabc);
    let tx_hash = |i: usize| format!("0x{i:064x}");
    let transactions: Vec<serde_json::Value> = (0..tx_count)
        .map(|i| {
            json!({
                "type": "0x2", "chainId": "0x2105", "nonce": format!("0x{i:x}"),
                "hash": tx_hash(i), "blockHash": block_hash, "blockNumber": "0x112a880",
                "transactionIndex": format!("0x{i:x}"),
                "from": format!("0x{:040x}", i), "to": format!("0x{:040x}", i + 1),
                "value": "0x0", "gas": "0x5208", "gasPrice": "0x4c4b40",
                "maxFeePerGas": "0x989680", "maxPriorityFeePerGas": "0x0",
                "input": format!("0xa9059cbb{:064x}{:064x}", i, 1),
                "accessList": [], "v": "0x1", "yParity": "0x1",
                "r": format!("0x{:064x}", i), "s": format!("0x{:064x}", i + 1),
            })
        })
        .collect();
    let receipts: Vec<serde_json::Value> = (0..tx_count)
        .map(|i| {
            let logs: Vec<serde_json::Value> = (0..logs_per_tx)
                .map(|j| {
                    json!({
                        "address": format!("0x{:040x}", i),
                        "topics": [format!("0x{:064x}", i), format!("0x{:064x}", j)],
                        "data": "0x0000000000000000000000000000000000000000000000000000000000000001",
                        "blockNumber": "0x112a880",
                        "blockHash": block_hash,
                        "transactionHash": tx_hash(i),
                        "transactionIndex": format!("0x{i:x}"),
                        "logIndex": format!("0x{:x}", i * logs_per_tx + j),
                        "removed": false,
                    })
                })
                .collect();
            json!({
                "blockHash": block_hash, "blockNumber": "0x112a880",
                "transactionHash": tx_hash(i), "transactionIndex": format!("0x{i:x}"),
                "from": format!("0x{:040x}", i), "to": format!("0x{:040x}", i + 1),
                "status": "0x1", "gasUsed": "0x5208", "cumulativeGasUsed": "0x5208",
                "effectiveGasPrice": "0x4c4b40", "type": "0x2", "contractAddress": null,
                "logsBloom": format!("0x{}", "0".repeat(512)),
                "logs": logs,
            })        })
        .collect();
    let block = json!({
            "number": "0x112a880",
            "hash": block_hash,
            "parentHash": format!("0x{:064x}", 0xdef),
            "timestamp": "0x6530a1b0",
            "miner": "0x4200000000000000000000000000000000000011",
            "nonce": "0x0000000000000042",
            "sha3Uncles": format!("0x{:064x}", 0),
            "transactionsRoot": format!("0x{:064x}", 1),
            "stateRoot": format!("0x{:064x}", 2),
            "receiptsRoot": format!("0x{:064x}", 3),
            "mixHash": format!("0x{:064x}", 0),
            "difficulty": "0x0",
            "extraData": "0x",
            "gasLimit": "0x1c9c380",
            "gasUsed": "0x5208",
            "baseFeePerGas": "0x4c4b40",
            "logsBloom": format!("0x{}", "0".repeat(512)),
            "transactions": transactions,
    });
    let parent_hash: alloy_primitives::B256 = format!("0x{:064x}", 0xdef)
        .parse()
        .expect("parent hash fixture");
    let block_hash: alloy_primitives::B256 = format!("0x{:064x}", 0xabc)
        .parse()
        .expect("block hash fixture");
    RpcBatch {
        meta: indexer::wire::envelope::BlockMeta {
            height: 18_000_000,
            hash: block_hash,
            parent_hash,
            timestamp: 0x6530_a1b0,
        },
        block: Some(serde_json::from_value(block).expect("block fixture")),
        receipts: Some(serde_json::from_value(json!(receipts)).expect("receipt fixture")),
        logs: None,
    }
}

fn main() {
    for (tx_count, logs_per_tx) in [(100_usize, 2_usize), (500, 4), (2000, 5)] {
        let body = synthetic_batch(tx_count, logs_per_tx);
        // One event per thing: the block, then each transaction (joined to its
        // receipt) and its logs.
        let expected = 1 + tx_count * (1 + logs_per_tx);
        let events = decode_block(body.clone())
            .expect("benchmark fixture decodes")
            .events;
        assert_eq!(
            events.len(),
            expected,
            "fixture shape changed; the benchmark would measure the wrong work"
        );
        let mut samples = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let input = body.clone();
            let start = Instant::now();
            let decoded = decode_block(black_box(input)).expect("benchmark fixture decodes");
            samples.push(start.elapsed());
            black_box(&decoded);
        }
        report(
            &format!("project_block txs={tx_count} logs/tx={logs_per_tx} events={expected}"),
            &mut samples,
            "block",
        );

        let envelopes: Vec<Envelope> = events
            .into_iter()
            .map(|event| Envelope::new(ChainId::new("ethereum"), event))
            .collect();
        let mut samples = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let start = Instant::now();
            for envelope in &envelopes {
                let rendered = serde_json::to_string(envelope).expect("envelope serialises");
                black_box(&rendered);
            }
            samples.push(start.elapsed());
        }
        let [marker, transaction, log, ..] = envelopes.as_slice() else {
            panic!("every benchmark block has a marker, a transaction, and a log");
        };
        report(
            &format!(
                "serialise     {expected} envelopes (p50 block {:.1?}, transaction {:.1?}, log {:.1?})",
                median_serialise(marker),
                median_serialise(transaction),
                median_serialise(log),
            ),
            &mut samples,
            "block",
        );
    }
}

/// Measures one envelope's serialisation cost at the median.
///
/// Reported separately per event kind, because their payloads differ by an order
/// of magnitude, so a single "per event" figure would be misleading.
fn median_serialise(envelope: &Envelope) -> Duration {
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let start = Instant::now();
        let rendered = serde_json::to_string(black_box(envelope)).expect("envelope serialises");
        samples.push(start.elapsed());
        black_box(&rendered);
    }
    samples.sort_unstable();
    percentile(&samples, 50, 100)
}
