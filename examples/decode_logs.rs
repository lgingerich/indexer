//! Decodes real Uniswap V3 swap logs, with no arguments and no network.
//!
//! ```bash
//! cargo run --example decode_logs
//! ```
//!
//! It reads two real `Swap` logs captured from a Uniswap V3 pool on Base, embeds the
//! pool's ABI, and runs the same [`Transform`] the decode stage runs in production. For
//! each input it prints the decoded record, which is what the pipeline stores beside the
//! raw log; a raw dataset has no decoded form, so it produces none.
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
//! - The record carries `protocol`, taken from the registry entry that matched the
//!   address. Nothing in the ABI could supply it, and it is what lets a downstream
//!   consumer group rows by protocol without knowing any address.
//!
//!    The record deliberately carries **no** dataset. Turning these arguments into a
//!   `dex.trades` row needs to know which tokens the pool trades and how many decimals
//!   they have, and none of that is in a log — it comes from calling the pool and the
//!   tokens. That projection belongs where those joins are, not here.
//!
//! # Decoding other contracts
//!
//! This example is self-contained on purpose: it takes no arguments and needs no network,
//! because a demo that has to be configured is a demo that gets skipped. To decode a
//! different contract, change `POOL`, `CHAIN`, and the ABI path in the registry below.
//!
//! To capture real input for it, run the indexer with `stdout = true` in `[ingest]`,
//! which prints the stream instead of storing it:
//!
//! ```bash
//! RUST_LOG=warn cargo run --release 2>/dev/null | head -200 > envelopes.ndjson
//! ```
//!
//! A real registry is keyed by `(chain, address, block)` and answers per log. This
//! example builds the same `ContractRegistry` the settings file builds, so it exercises
//! the path the pipeline does — including the `protocol` stamped from the entry.

// A runnable tool rather than a library, so printing is the whole job.
#![expect(clippy::print_stdout, clippy::print_stderr, clippy::expect_used)]

use std::process::ExitCode;

use indexer::decode::Transform;
use indexer::decode::registry::{AbiEntry, ContractEntry, ContractRegistry, RegistryConfig};
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

fn main() -> ExitCode {
    // The real registry, built the way the settings file builds it, so the example
    // exercises the same path the pipeline does rather than a stub.
    let registry = ContractRegistry::load(
        &RegistryConfig {
            abi: vec![AbiEntry {
                name: "uniswap_v3_pool".to_owned(),
                path: std::path::PathBuf::from(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/abis/uniswap_v3_pool.json"
                )),
            }],
            contract: vec![ContractEntry {
                chain: CHAIN.to_owned(),
                address: POOL.to_owned(),
                abi: "uniswap_v3_pool".to_owned(),
            }],
            ..RegistryConfig::default()
        },
        ".",
    )
    .expect("the registry loads");

    let mut decoded_count = 0_usize;
    for (number, line) in SWAPS.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let envelope: Envelope =
            serde_json::from_str(line).expect("the fixture is a published envelope");
        let applied = Transform::apply(&registry, &envelope);
        if let Some(error) = applied.error {
            eprintln!("line {}: {error}", number + 1);
        }
        if let Some(out) = applied.output {
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

    eprintln!("{decoded_count} swaps decoded into their records");
    if decoded_count == 0 {
        eprintln!("nothing decoded, which means the fixture or the ABI changed");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
