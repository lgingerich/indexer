//! Writes one envelope per dataset to a newline-delimited JSON fixture, with no network.
//!
//! ```bash
//! cargo run --example datasets
//! ```
//!
//! The published wire shape is the crate's most-depended-on contract, and this example
//! makes it inspectable: it builds one of every [`Event`] variant, serializes each the
//! way every sink serializes it, and writes them to
//! `examples/fixtures/datasets.ndjson`. Read the file to see exactly what a consumer
//! receives.
//!
//! The line each envelope gets is the same bytes the `stdout` sink prints and the
//! `DuckDB` sink keeps in its `envelope` column, so this fixture is what a consumer
//! actually parses — not a prettified approximation of it.
//!
//! # What to look for
//!
//! - Integers render as `0x` **quantities**, not JSON numbers: `"number":"0x1406f40"`.
//!   A live `eth_getBlockByNumber` returns every integer that way, so matching it is what
//!   keeps a consumer from special-casing our encoding against the node's.
//! - `v` is a plain JSON number. It is ours, not the chain's, so it is not a quantity.
//!   There is no `sequence`: a record's position in the stream is the dataset's own, so
//!   the key in [`Event::dedupe_key`] is what a consumer deduplicates on.
//! - The event's fields are **flat**, beside a `"type"` tag — `{"type":"log","log_index":…}`.
//!   There is no nested `event` object to unwrap.
//! - Optional fields are **omitted**, not `null`, unless they are genuinely
//!   per-record (`topic0`..`topic3`, `to`, `withdrawals_root`). A consumer must tolerate
//!   a missing key, not only a null one.
//! - `logs_bloom` is `"0x…"` hex — 512 characters of it, because a bloom filter is 256
//!   bytes and does not compress. It is most of a block's and a receipt's line.
//!
//! This is a fixture generator, not a decoder: the values are hand-filled to exercise the
//! encoding (a negative `int256` in the decoded record, a `uint160` and an `int24` whose
//! widths travel with their values) rather than captured from a node. For real chain
//! bytes in the same format, see `decode_logs` and its `uniswap_v3_swaps.ndjson` fixture.

// A runnable tool rather than a library, so writing and printing is the whole job.
#![expect(clippy::print_stdout)]

use std::fs::File;
use std::io::{BufWriter, Write as _};
use std::process::ExitCode;

use alloy_primitives::{Address, B256, Bloom, Bytes, I256, TxHash, U256};
use indexer::wire::envelope::{
    Block, ChainId, Contract, Decoded, DecodedArg, Envelope, Event, Log, Receipt, Reorg,
    Transaction, TypedValue,
};

/// The chain these are from.
const CHAIN: &str = "ethereum";

/// Where the fixture is written, under the crate so the run does not depend on the
/// working directory — the same reason `decode_logs` embeds its fixture by path.
const OUTPUT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/fixtures/datasets.ndjson"
);

fn main() -> ExitCode {
    let mut writer = BufWriter::new(
        File::create(OUTPUT).unwrap_or_else(|error| panic!("create {OUTPUT}: {error}")),
    );

    for envelope in envelopes() {
        // The same encoding every sink uses: compact, one envelope per line.
        let line = serde_json::to_string(&envelope)
            .unwrap_or_else(|error| panic!("{} serializes: {error}", envelope.kind()));
        writeln!(writer, "{line}").unwrap_or_else(|error| panic!("write {OUTPUT}: {error}"));
        println!(
            "{:<12} {:>5} bytes  dedupe_key={}",
            envelope.kind(),
            line.len(),
            envelope.event.dedupe_key()
        );
    }

    writer
        .flush()
        .unwrap_or_else(|error| panic!("flush {OUTPUT}: {error}"));
    println!("wrote {OUTPUT}");
    ExitCode::SUCCESS
}

/// One envelope per [`Event`] variant, in the order a block publishes them.
///
/// All of them share one block/tx identity, as a real block's records would, so the
/// fixture also shows that the child datasets reference the block by scalar key rather
/// than embedding it.
fn envelopes() -> Vec<Envelope> {
    vec![
        block(),
        transaction(),
        receipt(),
        log(),
        decoded(),
        contract(),
        reorg(),
    ]
}

/// A block header, carrying its transactions' hashes rather than the transactions.
fn block() -> Envelope {
    envelope(Event::Block(Box::new(Block {
        number: BLOCK_NUMBER,
        hash: BLOCK_HASH,
        parent_hash: hash(0x10),
        timestamp: TIMESTAMP,
        ommers_hash: B256::ZERO,
        transactions_root: hash(0x12),
        state_root: hash(0x13),
        receipts_root: hash(0x14),
        // Post-Shanghai; `None` before EIP-4895.
        withdrawals_root: Some(hash(0x15)),
        logs_bloom: Bloom::ZERO,
        miner: Address::from([0x44; 20]),
        // Zero post-merge. `total_difficulty` is `None` on a chain that dropped it.
        difficulty: U256::ZERO,
        total_difficulty: Some(U256::from(58_750_000_000_000_000_000_000_u128)),
        size: Some(U256::from(1_234)),
        extra_data: Bytes::from_static(b"indexer-example"),
        gas_limit: 30_000_000,
        gas_used: 21_000,
        transaction_count: 1,
        base_fee_per_gas: Some(1_000_000_000),
        transaction_hashes: vec![TX_HASH],
        ..Block::default()
    })))
}

/// An EIP-1559 transaction: `gas_price` is left out, because the key is omitted rather
/// than null when it does not apply.
fn transaction() -> Envelope {
    envelope(Event::Transaction(Box::new(Transaction {
        hash: TX_HASH,
        nonce: 130_000,
        transaction_index: 0,
        from: Address::from([0x55; 20]),
        // `None` when the transaction deploys a contract.
        to: Some(CONTRACT),
        value: U256::from(10u64.pow(18)),
        gas: 45_000,
        max_fee_per_gas: Some(2_000_000_000),
        max_priority_fee_per_gas: Some(1_000_000_000),
        input: Bytes::from_static(&[0x01, 0x02, 0x03]),
        // 0 legacy, 1 access list, 2 dynamic fee, 3 blob, 4 set-code. Not an enum:
        // an OP-stack deposit is `0x7e` and an Arbitrum retry `0x6a`.
        transaction_type: 2,
        chain_id: Some(1),
        block_timestamp: TIMESTAMP,
        block_number: BLOCK_NUMBER,
        block_hash: BLOCK_HASH,
        ..Transaction::default()
    })))
}

/// A successful receipt, with the contract-creation fields absent.
fn receipt() -> Envelope {
    envelope(Event::Receipt(Box::new(Receipt {
        transaction_hash: TX_HASH,
        transaction_index: 0,
        from: Address::from([0x55; 20]),
        to: Some(CONTRACT),
        status: true,
        transaction_type: 2,
        gas_used: 21_000,
        cumulative_gas_used: 21_000,
        effective_gas_price: 1_500_000_000,
        contract_address: None,
        logs_bloom: Bloom::ZERO,
        log_count: 1,
        block_timestamp: TIMESTAMP,
        block_number: BLOCK_NUMBER,
        block_hash: BLOCK_HASH,
        ..Receipt::default()
    })))
}

/// One log, topics flattened to `topic0`..`topic3` so a row is fixed-shape.
fn log() -> Envelope {
    envelope(Event::Log(Box::new(Log {
        log_index: 7,
        transaction_hash: TX_HASH,
        transaction_index: 0,
        address: CONTRACT,
        // `topic0` is the event selector. An indexed address is a 32-byte word,
        // left-padded, which is how a node returns a topic.
        topic0: Some(hash(0x07)),
        topic1: Some(topic(Address::from([0x55; 20]))),
        topic2: Some(topic(Address::from([0x66; 20]))),
        // Fewer than four topics is normal, so this is `null` rather than omitted.
        topic3: None,
        data: Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]),
        removed: false,
        block_number: BLOCK_NUMBER,
        block_hash: BLOCK_HASH,
        block_timestamp: TIMESTAMP,
    })))
}

/// One decoded event, with typed named arguments and the widths a store needs.
fn decoded() -> Envelope {
    envelope(Event::Decoded(Box::new(Decoded {
        name: "Swap".to_owned(),
        address: CONTRACT,
        // From the protocol manifest that listed the address, not from the ABI.
        protocol: "uniswap_v3".to_owned(),
        contract: "UniswapV3Pool".to_owned(),
        event_id: hash(0x08),
        selector: hash(0x07),
        signature: "Swap(address,address,int256,int256,uint160,uint128,int24)".to_owned(),
        anonymous: false,
        transaction_hash: TX_HASH,
        transaction_index: 0,
        log_index: 7,
        // Each argument carries its ABI name, so a consumer addresses `amount0`
        // rather than counting positions.
        indexed: vec![
            decoded_arg(
                "sender",
                TypedValue::Address {
                    value: Address::from([0x55; 20]),
                },
            ),
            decoded_arg(
                "recipient",
                TypedValue::Address {
                    value: Address::from([0x66; 20]),
                },
            ),
        ],
        // A negative `int256`, plus a `uint160` and an `int24`: the widths travel
        // with the values because a width is not recoverable from a number.
        body: vec![
            decoded_arg(
                "amount0",
                TypedValue::Int {
                    value: I256::MINUS_ONE,
                    bits: 256,
                },
            ),
            decoded_arg(
                "sqrtPriceX96",
                TypedValue::Uint {
                    // Shifted as a `u128`, which `U256::from` accepts; a `u64` would
                    // overflow at 96 bits.
                    value: U256::from(1u128 << 96),
                    bits: 160,
                },
            ),
            decoded_arg(
                "tick",
                TypedValue::Int {
                    // Negated rather than written as a negative literal: `I256::from`
                    // only takes an integer of its own width.
                    value: -I256::from_raw(U256::from(2_017_000)),
                    bits: 24,
                },
            ),
        ],
        block_number: BLOCK_NUMBER,
        block_hash: BLOCK_HASH,
        block_timestamp: TIMESTAMP,
    })))
}

/// A contract a factory's creation event named, keyed by that creation log.
fn contract() -> Envelope {
    envelope(Event::Contract(Box::new(Contract {
        // From the protocol manifest whose `created_by` rule matched.
        protocol: "uniswap_v3".to_owned(),
        name: "UniswapV3Pool".to_owned(),
        address: Address::from([0x77; 20]),
        factory_address: CONTRACT,
        transaction_hash: TX_HASH,
        transaction_index: 0,
        log_index: 7,
        block_number: BLOCK_NUMBER,
        block_hash: BLOCK_HASH,
        block_timestamp: TIMESTAMP,
    })))
}

/// A discontinuity: the last few published blocks are no longer canonical.
fn reorg() -> Envelope {
    envelope(Event::Reorg(Reorg {
        height: BLOCK_NUMBER,
        new_head_hash: hash(0x17),
        // Newest first.
        orphaned_hashes: vec![BLOCK_HASH],
    }))
}

/// The height every record in the fixture belongs to.
const BLOCK_NUMBER: u64 = 21_000_000;
/// The timestamp denormalized onto every dataset that outlives its block.
const TIMESTAMP: u64 = 1_700_000_000;
/// The block hash every record in the fixture belongs to.
const BLOCK_HASH: B256 = B256::new([0x11; 32]);
/// The transaction hash every non-block record in the fixture belongs to.
const TX_HASH: TxHash = TxHash::new([0x22; 32]);
/// The contract the fixture's log and decoded record were emitted by.
const CONTRACT: Address = Address::new([0x33; 20]);

fn envelope(event: Event) -> Envelope {
    Envelope::new(ChainId::new(CHAIN), event)
}

fn hash(byte: u8) -> B256 {
    B256::from([byte; 32])
}

/// An indexed address as a 32-byte topic word: the address, left-padded with zeros.
fn topic(address: Address) -> B256 {
    B256::left_padding_from(address.as_slice())
}

fn decoded_arg(name: &str, value: TypedValue) -> DecodedArg {
    let (position, kind) = match name {
        "sender" => (0, "address"),
        "recipient" => (1, "address"),
        "amount0" => (2, "int256"),
        "sqrtPriceX96" => (4, "uint160"),
        "tick" => (6, "int24"),
        _ => unreachable!("fixture argument"),
    };
    DecodedArg {
        name: name.to_owned(),
        position,
        abi_type: indexer::wire::typed::AbiType {
            kind: kind.to_owned(),
            components: Vec::new(),
        },
        value,
    }
}
