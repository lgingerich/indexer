//! Decodes real Uniswap V3 swap logs, with no arguments and no network.
//!
//! ```bash
//! cargo run --example decode_logs
//! ```
//!
//! It reads two real `Swap` logs captured from a Uniswap V3 pool on Base, embeds the
//! pool's ABI, and runs the same [`Decoder`] the decode stage runs in production. For
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
//! - The record carries `protocol`, taken from the manifest that lists the pool.
//!   Nothing in the ABI could supply it, and it is what lets a downstream consumer group
//!   rows by protocol without knowing any address.
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
//! different contract, list its address in a protocol manifest under `protocols/` and
//! change `CHAIN` and the fixture below.
//!
//! To capture real input for it, run the indexer with `stdout = true` in `[ingest]`,
//! which prints the stream instead of storing it:
//!
//! ```bash
//! RUST_LOG=warn cargo run --release 2>/dev/null | head -200 > envelopes.ndjson
//! ```
//!
//! This example builds the same `Catalog` and `Decoder` the settings file builds, so it
//! exercises the path the pipeline does — including the `protocol` stamped from the
//! manifest.

// A runnable tool rather than a library, so printing is the whole job.
#![expect(clippy::print_stdout, clippy::print_stderr, clippy::expect_used)]

use std::process::ExitCode;

use indexer::decode::{Catalog, Decoder};
use indexer::wire::envelope::Envelope;

/// The chain the captured logs are from.
const CHAIN: &str = "base";

/// Two real `Swap` logs from the pool above, one per line.
///
/// Captured from Base rather than synthesized, so the example proves the decoder against
/// bytes the chain produced. It is small enough to embed because one log is under a
/// kilobyte, and a fixture that needs fetching would make this unrunnable.
const SWAPS: &str = include_str!("fixtures/uniswap_v3_swaps.ndjson");

fn main() -> ExitCode {
    // The shipped protocol manifests, loaded the way the settings file loads them, so
    // the example exercises the same path the pipeline does rather than a stub. The pool
    // is one of the Uniswap V3 manifest's seed addresses on Base.
    let catalog = Catalog::load(
        concat!(env!("CARGO_MANIFEST_DIR"), "/protocols"),
        &CHAIN.into(),
    )
    .expect("the shipped protocols load");
    let mut decoder = Decoder::new(catalog);
    let mut decoded_count = 0_usize;
    for (number, line) in SWAPS.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let envelope: Envelope =
            serde_json::from_str(line).expect("the fixture is a published envelope");
        let indexer::wire::envelope::Event::Log(log) = &envelope.event else {
            continue;
        };
        match decoder.decode(log) {
            Ok(Some(decoding)) => {
                decoded_count += 1;
                let out = Envelope::new(
                    envelope.chain,
                    indexer::wire::envelope::Event::Decoded(Box::new(decoding.decoded)),
                );
                println!(
                    "{}",
                    serde_json::to_string(&out).expect("envelope serializes")
                );
            }
            Ok(None) => eprintln!("line {}: no registered event", number + 1),
            Err(error) => eprintln!("line {}: {error}", number + 1),
        }
    }

    eprintln!("{decoded_count} swaps decoded into their records");
    if decoded_count == 0 {
        eprintln!("nothing decoded, which means the fixture or the ABI changed");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
