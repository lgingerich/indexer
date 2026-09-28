//! Decodes real Uniswap V3 swap logs, with no arguments and no broker.
//!
//! ```bash
//! cargo run --example decode_logs
//! ```
//!
//! It reads two real `Swap` logs captured from a Uniswap V3 pool on Base, embeds the
//! pool's ABI, and runs the same [`Transform`] the decode stage runs in production. For
//! each input it prints the raw envelope followed by the decoded record, which is what
//! the stage publishes: the decoded stream is a lossless superset, so nothing is
//! replaced.
//!
//! # What to look for
//!
//! - `amount0` is **negative** in the first swap. The amount is an `int256`, and the
//!   sign says which way the pool sent that token. Reading the two's-complement word as
//!   unsigned would give ~1.15e77 instead.
//! - `sqrtPriceX96` carries `bits: 160` and `tick` carries `bits: 24`. The declared
//!   width travels with the value, because a width is not recoverable from a number and
//!   a store needs it to pick a column.
//! - Every argument carries the ABI's own name. That is what lets a consumer address
//!   `amount0` rather than count positions, which breaks silently when an ABI revision
//!   reorders a parameter.
//!
//! # Decoding other contracts
//!
//! This example is self-contained on purpose: it takes no arguments and needs no broker,
//! because a demo that has to be configured is a demo that gets skipped. To decode a
//! different contract, change `POOL`, `CHAIN`, and the two `include_str!` files above.
//!
//! To capture real input for it, run the indexer with `stdout = true` in `[ingest]`,
//! which prints what it would publish instead of sending it to a broker:
//!
//! ```bash
//! RUST_LOG=warn cargo run --release 2>/dev/null | head -200 > envelopes.ndjson
//! ```
//!
//! A real registry is keyed by `(chain, address, block)` and answers per log; the fixed
//! registry here stands in for it so the example needs no configuration.

// A runnable tool rather than a library, so printing is the whole job.
#![expect(clippy::print_stdout, clippy::print_stderr, clippy::expect_used)]

use std::process::ExitCode;

use alloy_primitives::Address;
use indexer::decode::Transform;
use indexer::decode::registry::{Abi, AbiRegistry};
use indexer::wire::envelope::ChainId;
use indexer::wire::envelope::Envelope;

/// The pool these logs came from, and the address its ABI is registered against.
const POOL: &str = "0xd0b53D9277642d899DF5C87A3966A349A798F224";

/// The chain the captured logs are from.
const CHAIN: &str = "base";

/// Two real `Swap` logs from the pool above, one per line.
///
/// Captured from Base rather than synthesized, so the example proves the decoder against
/// bytes the chain produced. It is small enough to embed because one log is under a
/// kilobyte, and a fixture that needs fetching would make this unrunnable.
const SWAPS: &str = include_str!("fixtures/uniswap_v3_swaps.ndjson");

/// One ABI for every address on one chain, which is all a demo needs.
struct FixedRegistry {
    chain: ChainId,
    address: Address,
    abi: Abi,
}

impl AbiRegistry for FixedRegistry {
    fn abi(&self, chain: &ChainId, address: Address, _block: u64) -> Option<&Abi> {
        (chain == &self.chain && address == self.address).then_some(&self.abi)
    }
}

fn main() -> ExitCode {
    let address: Address = POOL.parse().expect("the pool address parses");
    let abi = Abi::from_json(include_str!("../src/decode/abi/uniswap_v3_pool.json"))
        .expect("the pool ABI loads");
    let transform = Transform::new(FixedRegistry {
        chain: ChainId::new(CHAIN),
        address,
        abi,
    });

    let mut decoded_count = 0_usize;
    for (number, line) in SWAPS.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let envelope: Envelope =
            serde_json::from_str(line).expect("the fixture is a published envelope");
        for out in transform.apply(envelope).expect("the log decodes") {
            if matches!(out.event, indexer::wire::envelope::Event::Decoded(_)) {
                decoded_count += 1;
            }
            println!(
                "{}",
                serde_json::to_string(&out).expect("envelope serializes")
            );
        }
        eprintln!("line {}: decoded", number + 1);
    }

    eprintln!("{decoded_count} swaps decoded, each printed after its source log");
    if decoded_count == 0 {
        eprintln!("nothing decoded, which means the fixture or the ABI changed");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
