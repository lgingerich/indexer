//! Microbenchmarks for the hot path: block decode and event serialisation.
//!
//! End-to-end latency is dominated by the network and is not reproducible in a
//! benchmark, but the CPU work between the socket and the sink is: parsing a block
//! and its receipts into events, and rendering each event to JSON. Those are the
//! two things that scale with chain activity, so they are what this measures.
//!
//! Run with `cargo bench`. The harness is hand-rolled and dependency-free so it
//! can report percentiles rather than means, which is the number that matters for
//! a latency target.

#![expect(clippy::print_stdout, clippy::expect_used)]

use std::hint::black_box;
use std::time::{Duration, Instant};

use alloy_primitives::B256;
use indexer::envelope::{ChainId, Envelope};
use indexer::source::EvmSource;
use indexer::source::{BlockId, BlockSource as _, RawBlock};
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

/// Builds a block and receipts payload shaped like a real `eth_getBlockByNumber`
/// and `eth_getBlockReceipts` response.
///
/// Synthetic rather than captured so the benchmark can scale the input, and so the
/// repository does not carry a multi-megabyte fixture. The field names and types
/// match what a node returns, which is what the decoder cares about.
fn synthetic_block(tx_count: usize, logs_per_tx: usize) -> RawBlock {
    let tx_hash = |i: usize| format!("0x{i:064x}");
    let transactions: Vec<String> = (0..tx_count).map(tx_hash).collect();
    let receipts: Vec<serde_json::Value> = (0..tx_count)
        .map(|i| {
            let logs: Vec<serde_json::Value> = (0..logs_per_tx)
                .map(|j| {
                    json!({
                        "address": format!("0x{:040x}", i),
                        "topics": [format!("0x{:064x}", i), format!("0x{:064x}", j)],
                        "data": "0x0000000000000000000000000000000000000000000000000000000000000001",
                        "blockNumber": "0x112a880",
                        "transactionHash": tx_hash(i),
                        "logIndex": format!("0x{:x}", i * logs_per_tx + j),
                    })
                })
                .collect();
            json!({
                "transactionHash": tx_hash(i),
                "transactionIndex": format!("0x{i:x}"),
                "logs": logs,
            })
        })
        .collect();
    let block = json!({
        "number": "0x112a880",
        "hash": format!("0x{:064x}", 0xabc),
        "parentHash": format!("0x{:064x}", 0xdef),
        "timestamp": "0x6530a1b0",
        "transactions": transactions,
    });

    RawBlock {
        raw_block: block.to_string(),
        raw_receipts: serde_json::Value::Array(receipts).to_string(),
        finalized: BlockId {
            height: 0,
            hash: B256::ZERO,
        },
    }
}

fn main() {
    let source = EvmSource::new("ethereum", "http://localhost:8545", "ws://localhost:8546");

    for (tx_count, logs_per_tx) in [(100_usize, 2_usize), (500, 4), (2000, 5)] {
        let raw = synthetic_block(tx_count, logs_per_tx);
        let expected = 1 + tx_count * logs_per_tx;
        let events = source
            .encode_block(&raw)
            .expect("benchmark fixture decodes");
        assert_eq!(
            events.len(),
            expected,
            "fixture shape changed; the benchmark would measure the wrong work"
        );

        let mut samples = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let start = Instant::now();
            let decoded = source
                .encode_block(black_box(&raw))
                .expect("benchmark fixture decodes");
            samples.push(start.elapsed());
            black_box(&decoded);
        }
        report(
            &format!("decode_block  txs={tx_count} logs/tx={logs_per_tx} events={expected}"),
            &mut samples,
            "block",
        );

        let envelopes: Vec<Envelope> = events
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                Envelope::new(
                    ChainId::new("ethereum"),
                    u64::try_from(index).expect("event index fits in u64"),
                    event,
                )
            })
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
        let marker = envelopes.first().expect("block has a marker");
        let log = envelopes.get(1).unwrap_or(marker);
        report(
            &format!(
                "serialise    {expected} envelopes (p50 block marker {:.1?}, per log {:.1?})",
                median_serialise(marker),
                median_serialise(log),
            ),
            &mut samples,
            "block",
        );
    }
}

/// Measures one envelope's serialisation cost at the median.
///
/// Reported separately for the block marker and for a log, because the marker
/// embeds the whole block payload while a log does not, so a single "per event"
/// figure would be misleading.
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
