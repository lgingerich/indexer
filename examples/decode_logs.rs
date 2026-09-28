//! Runs the decode transform over a file of raw envelopes and prints NDJSON.
//!
//! This is a way to *see* decoded output without a broker. It reads envelopes in the
//! shape the indexer publishes, decodes each, and writes the transform's own output —
//! the raw envelope followed by its decoded record — to stdout.
//!
//! # Getting the input
//!
//! The input is NDJSON, one published envelope per line. There is no fixture in the
//! repository, so produce one from the chain first: run the indexer with `STDOUT=1`,
//! which prints what it would publish instead of sending it to a broker, and keep a
//! slice.
//!
//! ```bash
//! EVM_CHAIN=base \
//! EVM_HTTP_URL=https://base-rpc.publicnode.com \
//! EVM_WS_URL=wss://base-rpc.publicnode.com \
//! STDOUT=1 RUST_LOG=warn \
//! cargo run --release 2>/dev/null | head -200 > envelopes.ndjson
//! ```
//!
//! Then decode that file. `--address` is the contract whose ABI applies to every line
//! in it, so the capture is worth filtering to one contract's logs; a real registry is
//! keyed by `(chain, address, block)` and answers per log, and this stands in for it.
//!
//! ```bash
//! cargo run --example decode_logs -- \
//!   --abi src/decode/abi/uniswap_v3_pool.json \
//!   --address 0xd0b53D9277642d899DF5C87A3966A349A798F224 \
//!   --chain base \
//!   envelopes.ndjson
//! ```

// A runnable tool rather than a library, so printing is the whole job.
#![expect(clippy::print_stdout, clippy::print_stderr, clippy::expect_used)]

use std::process::ExitCode;

use alloy_primitives::Address;
use indexer::decode::Transform;
use indexer::decode::registry::{Abi, AbiRegistry};
use indexer::wire::envelope::ChainId;
use indexer::wire::envelope::Envelope;

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

    let contents = match std::fs::read_to_string(&input) {
        Ok(contents) => contents,
        Err(error) => {
            // A missing input is the most likely way to get here, and the raw `Os` error
            // does not say where the file was supposed to come from. See the module docs.
            eprintln!("cannot read {input}: {error}");
            eprintln!(
                "the input is NDJSON of published envelopes; see the module docs for \
                 how to capture one with STDOUT=1"
            );
            return ExitCode::FAILURE;
        }
    };
    let (mut lines, mut decoded_count, mut failed) = (0_usize, 0_usize, 0_usize);
    for (number, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let envelope: Envelope = match serde_json::from_str(line) {
            Ok(envelope) => envelope,
            Err(error) => {
                // Named so a bad line in a 200-line capture is findable.
                eprintln!("line {}: not an envelope: {error}", number + 1);
                failed += 1;
                continue;
            }
        };
        lines += 1;
        match transform.apply(envelope) {
            Ok(output) => {
                for out in output {
                    if matches!(out.event, indexer::wire::envelope::Event::Decoded(_)) {
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
                eprintln!("line {}: decode failed: {error}", number + 1);
            }
        }
    }

    eprintln!("{lines} envelopes in, {decoded_count} decoded, {failed} failed");
    if failed > 0 {
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
