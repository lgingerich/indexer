// The crate denies `expect`; a test may use it, since a failed expectation there means
// the fixture is wrong.
#![expect(clippy::expect_used)]

use alloy_primitives::{Address, B64, B256, Bloom, Bytes, Function, I256, TxHash, U256};
use alloy_rpc_types_eth::{
    AccessList, AccessListItem, Authorization, SignedAuthorization, Withdrawal,
};
use serde::de::{IgnoredAny, MapAccess};
use serde::{Deserialize, Deserializer};

use super::{
    AbiComponent, AbiType, Block, BlockMeta, ChainId, Contract, Decoded, DecodedArg, Derived,
    DerivedField, Envelope, Event, Log, Reorg, Transaction, TypedValue,
};

fn contract(block_hash: B256) -> Contract {
    Contract {
        protocol: "uniswap_v3".to_owned(),
        name: "UniswapV3Pool".to_owned(),
        address: Address::from([0xd0; 20]),
        factory_address: Address::from([0xfa; 20]),
        transaction_hash: TxHash::from([0xbb; 32]),
        transaction_index: 1,
        log_index: 3,
        block_number: 100,
        block_hash,
        block_timestamp: 1_700_000_000,
    }
}

fn hash(byte: u8) -> B256 {
    B256::from([byte; 32])
}

/// A decoded record's key extends its raw log's key, so it traces to the exact log
/// it came from, and a later interpretation of the same log is a separate key with
/// the same log prefix.
#[test]
fn a_decoded_records_key_extends_its_raw_logs_key() {
    let log = Log {
        log_index: 767,
        transaction_hash: TxHash::from([0x2a; 32]),
        block_number: 51_913_794,
        block_hash: hash(0xd4),
        ..Log::default()
    };
    let decoded = Decoded {
        name: "Swap".to_owned(),
        address: Address::from([0xd0; 20]),
        protocol: "uniswap_v3".to_owned(),
        contract: "UniswapV3Pool".to_owned(),
        event_id: hash(0x08),
        selector: hash(0x07),
        signature: "Swap(address,address,int256,int256,uint160,uint128,int24)".to_owned(),
        anonymous: false,
        transaction_hash: log.transaction_hash,
        transaction_index: 12,
        log_index: log.log_index,
        indexed: Vec::new(),
        body: Vec::new(),
        block_number: log.block_number,
        block_hash: hash(0xd4),
        block_timestamp: 1_700_000_000,
    };
    let prefix = format!("{}:", log.dedupe_key());
    let mut later_interpretation = decoded.clone();
    later_interpretation.event_id = hash(0x09);
    assert!(decoded.dedupe_key().starts_with(&prefix));
    assert!(later_interpretation.dedupe_key().starts_with(&prefix));
    assert_ne!(decoded.dedupe_key(), later_interpretation.dedupe_key());
}

/// The key is what a store deduplicates on, so its exact shape is a published
/// contract. These pin it as a literal, since a test that compares two functions
/// against each other cannot catch both drifting the same way.
#[test]
fn every_dedupe_key_is_the_documented_shape() {
    let block_hex = "aa".repeat(32);
    let tx_hex = "bb".repeat(32);
    let selector_hex = "cc".repeat(32);
    let block_hash = hash(0xaa);
    let tx = TxHash::from([0xbb; 32]);
    let selector = hash(0xcc);

    assert_eq!(
        Block {
            number: 100,
            hash: block_hash,
            ..Block::default()
        }
        .dedupe_key(),
        format!("0x{block_hex}:block")
    );
    assert_eq!(
        Transaction {
            block_number: 100,
            block_hash,
            hash: tx,
            ..Transaction::default()
        }
        .dedupe_key(),
        format!("0x{block_hex}:0x{tx_hex}:tx")
    );
    assert_eq!(
        Log {
            block_number: 100,
            block_hash,
            transaction_hash: tx,
            log_index: 3,
            ..Log::default()
        }
        .dedupe_key(),
        format!("0x{block_hex}:0x{tx_hex}:3")
    );
    assert_eq!(
        Event::Decoded(Box::new(Decoded {
            name: "Swap".to_owned(),
            address: Address::from([0xd0; 20]),
            protocol: "uniswap_v3".to_owned(),
            contract: "UniswapV3Pool".to_owned(),
            event_id: hash(0x08),
            selector,
            signature: "Swap(address)".to_owned(),
            anonymous: false,
            transaction_hash: tx,
            transaction_index: 0,
            log_index: 3,
            indexed: Vec::new(),
            body: Vec::new(),
            block_number: 100,
            block_hash,
            block_timestamp: 1_700_000_000,
        }))
        .dedupe_key(),
        format!(
            "0x{block_hex}:0x{tx_hex}:3:0x{selector_hex}:0x{}:decoded",
            "08".repeat(32)
        )
    );
    assert_eq!(
        Event::Contract(Box::new(contract(block_hash))).dedupe_key(),
        format!("0x{block_hex}:0x{tx_hex}:3:0x{}:contract", "d0".repeat(20))
    );
    assert_eq!(
        Event::Reorg(Reorg {
            height: 100,
            new_head_hash: block_hash,
            orphaned_hashes: vec![hash(0xdd)],
        })
        .dedupe_key(),
        format!("0x{block_hex}:reorg")
    );
    assert_eq!(
        Event::AcceptedBlock(BlockMeta {
            height: 100,
            hash: block_hash,
            parent_hash: hash(0xdd),
            timestamp: 1_700_000_000,
        })
        .dedupe_key(),
        format!("0x{block_hex}:accepted_block")
    );
}

/// Every variant, with every optional field set, as the line a sink writes.
///
/// The dataset literals name every field rather than spreading a default, so a new
/// field fails to compile here until it is set, and so is covered by the test below.
#[expect(
    clippy::too_many_lines,
    reason = "one literal per variant is the point"
)]
fn every_variant_fully_populated() -> Vec<Event> {
    let (block_hash, tx) = (hash(0xaa), TxHash::from([0xbb; 32]));
    let address = Address::from([0xd0; 20]);
    let typed = |value: TypedValue| DecodedArg {
        position: 0,
        name: "value".to_owned(),
        abi_type: AbiType {
            kind: "tuple".to_owned(),
            components: vec![AbiComponent {
                name: "inner".to_owned(),
                abi_type: AbiType {
                    kind: "uint256".to_owned(),
                    components: Vec::new(),
                },
            }],
        },
        value,
    };
    let block = Block {
        number: 100,
        hash: block_hash,
        parent_hash: hash(0x01),
        timestamp: 1_700_000_000,
        nonce: B64::from([0x02; 8]),
        ommers_hash: hash(0x03),
        transactions_root: hash(0x04),
        state_root: hash(0x05),
        receipts_root: hash(0x06),
        withdrawals_root: Some(hash(0x07)),
        logs_bloom: Bloom::from([0x08; 256]),
        miner: address,
        difficulty: U256::from(9),
        total_difficulty: Some(U256::from(10)),
        size: Some(U256::from(11)),
        extra_data: Bytes::from_static(b"extra"),
        gas_limit: 12,
        gas_used: 13,
        transaction_count: 1,
        base_fee_per_gas: Some(14),
        blob_gas_used: Some(15),
        excess_blob_gas: Some(16),
        parent_beacon_block_root: Some(hash(0x11)),
        mix_hash: Some(hash(0x12)),
        withdrawals: Some(vec![Withdrawal {
            index: 1,
            validator_index: 2,
            address,
            amount: 3,
        }]),
        requests_hash: Some(hash(0x13)),
        l1_block_number: Some(17),
        send_count: Some(18),
        send_root: Some(hash(0x14)),
        ommers: vec![hash(0x15)],
        transaction_hashes: vec![tx],
    };
    let transaction = Transaction {
        hash: tx,
        nonce: 1,
        transaction_index: 2,
        from: address,
        to: Some(Address::from([0xd1; 20])),
        value: U256::from(3),
        gas: 4,
        gas_price: Some(5),
        max_fee_per_gas: Some(6),
        max_priority_fee_per_gas: Some(7),
        max_fee_per_blob_gas: Some(8),
        input: Bytes::from_static(&[0x01, 0x02]),
        transaction_type: 4,
        chain_id: Some(1),
        access_list: Some(AccessList(vec![AccessListItem {
            address,
            storage_keys: vec![hash(0x16)],
        }])),
        blob_versioned_hashes: Some(vec![hash(0x17)]),
        authorization_list: Some(vec![SignedAuthorization::new_unchecked(
            Authorization {
                chain_id: U256::from(1),
                address,
                nonce: 9,
            },
            1,
            U256::from(10),
            U256::from(11),
        )]),
        r: Some(U256::from(12)),
        s: Some(U256::from(13)),
        // An EIP-155 legacy `v`: the field whose name the envelope must not share.
        v: Some(0x25),
        y_parity: Some(1),
        receipt_status: true,
        receipt_gas_used: 14,
        receipt_cumulative_gas_used: 15,
        receipt_effective_gas_price: 16,
        receipt_contract_address: Some(Address::from([0xd2; 20])),
        receipt_logs_bloom: Bloom::from([0x09; 256]),
        receipt_blob_gas_used: Some(17),
        receipt_blob_gas_price: Some(18),
        receipt_gas_used_for_l1: Some(19),
        receipt_l1_block_number: Some(20),
        log_count: 1,
        block_timestamp: 1_700_000_000,
        block_number: 100,
        block_hash,
    };
    let log = Log {
        log_index: 3,
        transaction_hash: tx,
        transaction_index: 2,
        address,
        topic0: Some(hash(0x18)),
        topic1: Some(hash(0x19)),
        topic2: Some(hash(0x1a)),
        topic3: Some(hash(0x1b)),
        data: Bytes::from_static(&[0xde, 0xad]),
        removed: false,
        block_number: 100,
        block_hash,
        block_timestamp: 1_700_000_000,
    };
    let decoded = Decoded {
        event_id: hash(0x08),
        name: "Swap".to_owned(),
        address,
        protocol: "uniswap_v3".to_owned(),
        contract: "UniswapV3Pool".to_owned(),
        selector: hash(0x18),
        signature: "Swap(address)".to_owned(),
        anonymous: false,
        transaction_hash: tx,
        transaction_index: 2,
        log_index: 3,
        indexed: vec![typed(TypedValue::IndexedHash { value: hash(0x1c) })],
        body: vec![typed(TypedValue::Tuple {
            value: vec![
                TypedValue::Bool { value: true },
                TypedValue::Int {
                    value: I256::MINUS_ONE,
                    bits: 24,
                },
                TypedValue::Uint {
                    value: U256::MAX,
                    bits: 256,
                },
                TypedValue::FixedBytes {
                    value: Bytes::from_static(&[0xab; 4]),
                    size: 4,
                },
                TypedValue::Address { value: address },
                TypedValue::Bytes {
                    value: Bytes::from_static(&[0xcd]),
                },
                TypedValue::String {
                    value: Bytes::from_static(b"text"),
                    text: Some("text".to_owned()),
                },
                TypedValue::Function {
                    value: Function::from([0xef; 24]),
                },
                TypedValue::Array {
                    value: vec![TypedValue::Bool { value: false }],
                },
                TypedValue::FixedArray {
                    value: vec![TypedValue::Bool { value: true }],
                    size: 1,
                },
            ],
        })],
        block_number: 100,
        block_hash,
        block_timestamp: 1_700_000_000,
    };
    let derived = Derived {
        table: "metric_v1_swaps".to_owned(),
        address,
        transaction_hash: tx,
        transaction_index: 2,
        log_index: 3,
        row: 0,
        transaction_from_address: Some(Address::from([0xd3; 20])),
        transaction_to_address: Some(Address::from([0xd4; 20])),
        fields: vec![DerivedField {
            name: "amount0_raw".to_owned(),
            value: TypedValue::Int {
                value: I256::MINUS_ONE,
                bits: 256,
            },
        }],
        block_number: 100,
        block_hash,
        block_timestamp: 1_700_000_000,
    };
    vec![
        Event::Block(Box::new(block)),
        Event::Transaction(Box::new(transaction)),
        Event::Log(Box::new(log)),
        Event::Decoded(Box::new(decoded)),
        Event::Contract(Box::new(contract(block_hash))),
        Event::Derived(Box::new(derived)),
        Event::Reorg(Reorg {
            height: 100,
            new_head_hash: block_hash,
            orphaned_hashes: vec![hash(0xdd)],
        }),
        Event::AcceptedBlock(BlockMeta {
            height: 100,
            hash: block_hash,
            parent_hash: hash(0x01),
            timestamp: 1_700_000_000,
        }),
    ]
}

/// An object's top-level keys in the order they appear, repeats kept.
///
/// A `serde_json::Value` cannot be used to find a repeat: its map keeps the last
/// value for a key and drops the earlier one without error.
struct Keys(Vec<String>);

impl<'de> Deserialize<'de> for Keys {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Keys;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Keys, A::Error> {
                let mut keys = Vec::new();
                while let Some(key) = map.next_key::<String>()? {
                    map.next_value::<IgnoredAny>()?;
                    keys.push(key);
                }
                Ok(Keys(keys))
            }
        }
        deserializer.deserialize_map(Visitor)
    }
}

/// The line every sink writes is the wire contract: every variant, with every
/// optional field set, reads back as the envelope that wrote it, and no key appears
/// twice on the line. A repeated key is the failure a flattened event invites — a
/// strict reader rejects the line, and a last-wins one such as `jq` silently reads
/// the wrong value.
#[test]
fn every_variant_round_trips_through_json_with_unique_keys() {
    for event in every_variant_fully_populated() {
        let envelope = Envelope::new(ChainId::new("ethereum"), event);
        let line = serde_json::to_string(&envelope).expect("serializes");

        let Keys(mut keys) = serde_json::from_str(&line).expect("an object");
        let count = keys.len();
        keys.sort_unstable();
        keys.dedup();
        assert_eq!(keys.len(), count, "a key repeats on the line {line}");
        for envelope_key in ["chain", "schema_version", "type"] {
            assert!(keys.iter().any(|key| key == envelope_key), "{line}");
        }

        let read: Envelope = serde_json::from_str(&line)
            .unwrap_or_else(|error| panic!("{line} reads back: {error}"));
        assert_eq!(read, envelope);
    }
}

/// An absent value is written as `null` whatever its encoding, a quantity or not, so
/// a consumer meets one kind of absence; and the line reads back as `None`.
#[test]
fn an_absent_value_is_null_and_not_omitted() {
    let envelope = Envelope::new(ChainId::new("ethereum"), Event::Transaction(Box::default()));
    let line = serde_json::to_string(&envelope).expect("serializes");
    let object: serde_json::Value = serde_json::from_str(&line).expect("an object");
    for key in [
        "v",
        "gas_price",
        "receipt_l1_block_number",
        "r",
        "to",
        "access_list",
    ] {
        assert_eq!(object.get(key), Some(&serde_json::Value::Null), "{key}");
    }
    let read: Envelope = serde_json::from_str(&line).expect("reads back");
    assert_eq!(read, envelope);
}

/// The checked-in fixtures are lines a consumer can copy as examples, so each must be
/// exactly what the encoder writes today: read back and written again, it is the same
/// bytes. A stale fixture — an old key, an omitted absent field — fails here.
#[test]
fn the_published_fixtures_are_what_the_encoder_writes() {
    let fixtures = [
        include_str!("../../../examples/fixtures/datasets.ndjson"),
        include_str!("../../../examples/fixtures/uniswap_v3_swaps.ndjson"),
    ];
    for line in fixtures.iter().flat_map(|fixture| fixture.lines()) {
        let envelope: Envelope =
            serde_json::from_str(line).unwrap_or_else(|error| panic!("{line} reads: {error}"));
        assert_eq!(serde_json::to_string(&envelope).expect("serializes"), line);
    }
}

/// A derived row's key is its log's, its table, and its position among the log's
/// rows, pinned as a literal like every other key.
#[test]
fn a_derived_rows_dedupe_key_is_the_documented_shape() {
    let block_hash = hash(0xaa);
    let tx = TxHash::from([0xbb; 32]);
    let (block_hex, tx_hex) = ("aa".repeat(32), "bb".repeat(32));
    assert_eq!(
        Event::Derived(Box::new(Derived {
            table: "metric_v1_swaps".to_owned(),
            address: Address::from([0xd0; 20]),
            transaction_hash: tx,
            transaction_index: 0,
            log_index: 3,
            row: 2,
            transaction_from_address: None,
            transaction_to_address: None,
            fields: Vec::new(),
            block_number: 100,
            block_hash,
            block_timestamp: 1_700_000_000,
        }))
        .dedupe_key(),
        format!("0x{block_hex}:0x{tx_hex}:3:metric_v1_swaps:2")
    );
}
