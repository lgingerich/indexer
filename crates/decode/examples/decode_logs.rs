//! Runs the decode transform over a file of raw envelopes and prints NDJSON.
//!
//! This is a way to *see* decoded output before the broker source and sink exist.
//! It reads envelopes in the shape the indexer publishes, decodes each, and writes
//! the transform's own output — the raw envelope followed by its decoded record —
//! to stdout.
//!
//! ```bash
//! cargo run -p decode --example decode_logs -- \
//!   --abi crates/decode/abi/uniswap_v3_pool.json \
//!   --address 0xd0b53D9277642d899DF5C87A3966A349A798F224 \
//!   --chain base \
//!   envelopes.ndjson
//! ```
//!
//! Every envelope in the file goes to the one address; a real registry is keyed by
//! `(chain, address, block)` and would answer per log. The seam is the same either
//! way, which is the point.

// A runnable tool rather than a library, so printing is the whole job.
#![expect(clippy::print_stdout, clippy::print_stderr, clippy::expect_used)]

use std::process::ExitCode;

use alloy_primitives::Address;
use decode::Transform;
use decode::registry::{Abi, AbiRegistry};
use wire::envelope::ChainId;
use wire::envelope::Envelope;

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
    let mut abi_path = None;
    let mut address = None;
    let mut chain = "base".to_owned();
    let mut input = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if !arg.starts_with("--") {
            input = Some(arg);
            continue;
        }
        let Some(value) = args.next() else {
            eprintln!("{arg} needs a value");
            return ExitCode::FAILURE;
        };
        match arg.as_str() {
            "--abi" => abi_path = Some(value),
            "--address" => address = Some(value),
            "--chain" => chain = value,
            other => {
                eprintln!("unknown flag {other}");
                return ExitCode::FAILURE;
            }
        }
    }

    let (Some(abi_path), Some(address), Some(input)) = (abi_path, address, input) else {
        eprintln!(
            "usage: decode_logs --abi <abi.json> --address <contract> [--chain <id>] <envelopes.ndjson>"
        );
        return ExitCode::FAILURE;
    };

    let address: Address = address.parse().expect("address parses");
    let abi = Abi::from_json(&std::fs::read_to_string(&abi_path).expect("ABI file reads"))
        .expect("ABI loads");
    let registry = FixedRegistry {
        chain: ChainId::new(chain),
        address,
        abi,
    };
    let transform = Transform::new(registry);

    let contents = std::fs::read_to_string(&input).expect("input file reads");
    let (mut lines, mut decoded_count, mut failed) = (0_usize, 0_usize, 0_usize);
    for line in contents.lines().filter(|line| !line.trim().is_empty()) {
        let envelope: Envelope = serde_json::from_str(line).expect("envelope parses");
        lines += 1;
        match transform.apply(envelope) {
            Ok(output) => {
                for out in output {
                    if matches!(out.event, wire::envelope::Event::Decoded(_)) {
                        decoded_count += 1;
                    }
                    println!(
                        "{}",
                        serde_json::to_string(&out).expect("envelope serializes")
                    );
                }
            }
            Err(error) => {
                failed += 1;
                eprintln!("decode failed: {error}");
            }
        }
    }

    eprintln!("{lines} envelopes in, {decoded_count} decoded, {failed} failed");
    if failed > 0 {
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
