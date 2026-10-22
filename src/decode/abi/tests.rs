// The crate denies `expect`/`unwrap` to keep production paths honest; tests are allowed
// them per the repository test style, since a failed expectation there means the fixture
// or setup is wrong.
#![expect(clippy::expect_used)]

use super::*;
use alloy_primitives::U256;

fn decode(abi: &str, values: &[DynSolValue], indexed: Option<B256>) -> DecodedEvent {
    let abi = Abi::from_json(abi).expect("valid ABI");
    let (selector, _) = *abi.events.keys().next().expect("event");
    abi.decode_log(&Log {
        topic0: Some(selector),
        topic1: indexed,
        data: DynSolValue::Tuple(values.to_vec())
            .abi_encode_params()
            .into(),
        ..Log::default()
    })
    .expect("decode")
    .expect("match")
}

#[test]
fn invalid_utf8_nul_and_nested_strings_remain_bytes() {
    let result = decode(
        r#"[{"type":"event","name":"Text","anonymous":false,"inputs":[{"name":"s","type":"string[]","indexed":false}]}]"#,
        &[DynSolValue::Array(vec![
            DynSolValue::Bytes(vec![0xff]),
            DynSolValue::Bytes(b"hello".to_vec()),
            DynSolValue::Bytes(b"a\0b".to_vec()),
        ])],
        None,
    );
    assert_eq!(
        result.body[0].value,
        TypedValue::Array {
            value: vec![
                TypedValue::String {
                    value: Bytes::from_static(&[0xff]),
                    text: None
                },
                TypedValue::String {
                    value: Bytes::from_static(b"hello"),
                    text: Some("hello".into())
                },
                TypedValue::String {
                    value: Bytes::from_static(b"a\0b"),
                    text: None
                },
            ]
        }
    );
}

#[test]
fn schema_preserves_empty_arrays_tuple_names_and_original_positions() {
    let result = decode(
        r#"[{"type":"event","name":"Data","anonymous":false,"inputs":[{"name":"id","type":"bytes32","indexed":true},{"name":"entries","type":"tuple[]","indexed":false,"components":[{"name":"owner","type":"address"}]}]}]"#,
        &[DynSolValue::Array(vec![])],
        Some(B256::ZERO),
    );
    assert_eq!(result.body[0].position, 1);
    assert_eq!(result.body[0].abi_type.kind, "tuple[]");
    assert_eq!(result.body[0].abi_type.components[0].name, "owner");
    assert_eq!(
        result.body[0].abi_type.components[0].abi_type.kind,
        "address"
    );
    assert_eq!(
        result.indexed[0].value,
        TypedValue::FixedBytes {
            value: Bytes::copy_from_slice(B256::ZERO.as_slice()),
            size: 32
        }
    );
}

#[test]
fn indexed_strings_are_hashes_not_values() {
    let hash = keccak256("text");
    let result = decode(
        r#"[{"type":"event","name":"Text","anonymous":false,"inputs":[{"name":"s","type":"string","indexed":true}]}]"#,
        &[],
        Some(hash),
    );
    assert_eq!(
        result.indexed[0].value,
        TypedValue::IndexedHash { value: hash }
    );
    assert_eq!(result.indexed[0].abi_type.kind, "string");
}

/// A tuple argument whose `internalType` names a struct decodes as a tuple, in the
/// body, indexed, and in an array. With alloy's `eip712` feature on, such an argument
/// resolves to a `CustomStruct`, which must not reach the decoder as a shape it does
/// not match.
#[test]
fn struct_arguments_decode_as_tuples() {
    let key = |name: &str, kind: &str, indexed: bool| {
        format!(
            r#"{{"name":"{name}","type":"tuple{kind}","internalType":"struct Pool.Key{kind}",
                "indexed":{indexed},"components":[
                {{"name":"owner","type":"address","internalType":"address"}}]}}"#
        )
    };
    let abi = format!(
        r#"[{{"type":"event","name":"Keyed","anonymous":false,"inputs":[{},{},{}]}}]"#,
        key("hashed", "", true),
        key("key", "", false),
        key("keys", "[]", false),
    );
    let owner = DynSolValue::Tuple(vec![DynSolValue::Address(alloy_primitives::Address::ZERO)]);
    let result = decode(
        &abi,
        &[owner.clone(), DynSolValue::Array(vec![owner])],
        Some(B256::repeat_byte(1)),
    );
    let tuple = TypedValue::Tuple {
        value: vec![TypedValue::Address {
            value: alloy_primitives::Address::ZERO,
        }],
    };
    assert_eq!(
        result.indexed[0].value,
        TypedValue::IndexedHash {
            value: B256::repeat_byte(1)
        }
    );
    assert_eq!(result.body[0].value, tuple);
    assert_eq!(
        result.body[1].value,
        TypedValue::Array { value: vec![tuple] }
    );
}

#[test]
fn functions_decode() {
    let result = decode(
        r#"[{"type":"event","name":"Callback","anonymous":false,"inputs":[{"name":"f","type":"function","indexed":false}]}]"#,
        &[DynSolValue::Function(alloy_primitives::Function::ZERO)],
        None,
    );
    assert_eq!(
        result.body[0].value,
        TypedValue::Function {
            value: alloy_primitives::Function::ZERO
        }
    );
}

/// An integer converts up to its declared width's extremes and is refused one past
/// them, and a boolean or address word holding more than its type is refused, each
/// naming why.
#[test]
fn integer_widths_and_scalar_constraints_are_checked() {
    use alloy_primitives::I256;
    let int = |value: i64| I256::try_from(value).expect("integer");
    for (case, ty, value, expected) in [
        (
            "int8 -128",
            DynSolType::Int(8),
            DynSolValue::Int(int(-128), 8),
            Ok(TypedValue::Int {
                value: int(-128),
                bits: 8,
            }),
        ),
        (
            "int8 127",
            DynSolType::Int(8),
            DynSolValue::Int(int(127), 8),
            Ok(TypedValue::Int {
                value: int(127),
                bits: 8,
            }),
        ),
        (
            "int8 128",
            DynSolType::Int(8),
            DynSolValue::Int(int(128), 8),
            Err("signed integer exceeds declared width"),
        ),
        (
            "int8 -129",
            DynSolType::Int(8),
            DynSolValue::Int(int(-129), 8),
            Err("signed integer exceeds declared width"),
        ),
        (
            "uint8 255",
            DynSolType::Uint(8),
            DynSolValue::Uint(U256::from(255), 8),
            Ok(TypedValue::Uint {
                value: U256::from(255),
                bits: 8,
            }),
        ),
        (
            "uint8 256",
            DynSolType::Uint(8),
            DynSolValue::Uint(U256::from(256), 8),
            Err("unsigned integer exceeds declared width"),
        ),
        (
            "bool 2",
            DynSolType::Bool,
            DynSolValue::Uint(U256::from(2), 256),
            Err("boolean must be zero or one"),
        ),
        (
            "address of 256 bits",
            DynSolType::Address,
            DynSolValue::Uint(U256::MAX, 256),
            Err("address exceeds 160 bits"),
        ),
    ] {
        let converted = convert(&ty, &value).map_err(|error| match error {
            DecodeError::Value { kind } => kind,
            other => panic!("{case}: {other:?}"),
        });
        assert_eq!(converted, expected, "{case}");
    }
}

/// A `bytesN` or `function` word with a nonzero byte past its value is refused, as a
/// word holding more than its integer type is; with clean padding it decodes.
#[test]
fn short_bytes_and_functions_with_dirty_padding_are_refused() {
    let mut dirty = [0_u8; 32];
    dirty[31] = 1;
    let dirty = DynSolValue::FixedBytes(B256::from(dirty), 32);
    let clean = DynSolValue::FixedBytes(B256::ZERO, 32);
    for (ty, kind) in [
        (DynSolType::FixedBytes(4), "bytesN has nonzero padding"),
        (DynSolType::Function, "function has nonzero padding"),
    ] {
        assert!(convert(&ty, &clean).is_ok(), "{kind}");
        assert!(
            matches!(convert(&ty, &dirty), Err(DecodeError::Value { kind: found }) if found == kind),
            "{kind}"
        );
    }
    assert!(
        convert(&DynSolType::FixedBytes(32), &dirty).is_ok(),
        "bytes32 has no padding"
    );
}

/// An address argument is found where the encoder put it, past a dynamic argument's
/// one head word and a static tuple's and fixed array's full encodings, or in its
/// topic, and read from there without decoding the rest.
#[test]
fn an_address_argument_is_read_from_its_slot() {
    let abi = Abi::from_json(
        r#"[{"type":"event","name":"Made","anonymous":false,"inputs":[
            {"name":"owner","type":"address","indexed":true},
            {"name":"note","type":"bytes","indexed":false},
            {"name":"pair","type":"tuple","indexed":false,"components":[
                {"name":"a","type":"uint256"},{"name":"b","type":"uint256"}]},
            {"name":"two","type":"uint8[2]","indexed":false},
            {"name":"child","type":"address","indexed":false}]}]"#,
    )
    .expect("valid ABI");
    let (key, _) = abi.events.first_key_value().expect("event");
    let child = alloy_primitives::Address::repeat_byte(0xc0);
    let owner = alloy_primitives::Address::repeat_byte(0x0e);
    let log = Log {
        topic0: Some(key.0),
        topic1: Some(owner.into_word()),
        data: DynSolValue::Tuple(vec![
            DynSolValue::Bytes(vec![1, 2, 3]),
            DynSolValue::Tuple(vec![
                DynSolValue::Uint(U256::ZERO, 256),
                DynSolValue::Uint(U256::ZERO, 256),
            ]),
            DynSolValue::FixedArray(vec![
                DynSolValue::Uint(U256::ZERO, 8),
                DynSolValue::Uint(U256::ZERO, 8),
            ]),
            DynSolValue::Address(child),
        ])
        .abi_encode_params()
        .into(),
        ..Log::default()
    };
    let slot = abi.address_input(*key, "child").expect("an address input");
    assert_eq!(slot, AddressSlot::Word(5 * 32));
    assert_eq!(slot.read(&log), Some(child));
    let slot = abi.address_input(*key, "owner").expect("an address input");
    assert_eq!(slot, AddressSlot::Topic(1));
    assert_eq!(slot.read(&log), Some(owner));
    assert_eq!(abi.address_input(*key, "note"), None, "not an address");
}

/// Garbled topics and data for an event with indexed string and tuple arguments and
/// dynamic body ones are ordinary decode failures or decodes, never the invariant
/// failure that stops the run: a hostile log must not be able to halt ingest.
#[test]
fn garbled_logs_never_fail_as_an_invariant() {
    let abi = Abi::from_json(
        r#"[{"type":"event","name":"Odd","anonymous":false,"inputs":[
            {"name":"label","type":"string","indexed":true},
            {"name":"key","type":"tuple","indexed":true,"components":[
                {"name":"a","type":"address"},{"name":"b","type":"bool"}]},
            {"name":"note","type":"string","indexed":false},
            {"name":"pairs","type":"tuple[]","indexed":false,"components":[
                {"name":"a","type":"bytes4"},{"name":"b","type":"int8"}]}]}]"#,
    )
    .expect("valid ABI");
    let (selector, _) = *abi.events.keys().next().expect("event");
    let words = [
        B256::ZERO,
        B256::repeat_byte(0xff),
        B256::with_last_byte(0x20),
    ];
    let mut checked = 0;
    for &topic1 in &words {
        for &topic2 in &words {
            for length in (0..=320).step_by(8) {
                for fill in [0x00_u8, 0x01, 0x20, 0xff] {
                    let mut data = vec![fill; length];
                    // Offsets that point inside the data, at its end, and past it.
                    if let Some(word) = data.get_mut(..32) {
                        word.fill(0);
                        word[31] = u8::try_from(length % 256).expect("a byte");
                    }
                    let log = Log {
                        topic0: Some(selector),
                        topic1: Some(topic1),
                        topic2: Some(topic2),
                        data: data.into(),
                        ..Log::default()
                    };
                    if let Err(error) = abi.decode_log(&log) {
                        assert!(
                            !error.is_invariant(),
                            "{length} bytes of {fill:#x}: {error}"
                        );
                    }
                    checked += 1;
                }
            }
        }
    }
    assert!(checked > 1000);
}

const ERC20_TRANSFER: &str = r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[{"name":"from","type":"address","indexed":true},{"name":"to","type":"address","indexed":true},{"name":"value","type":"uint256","indexed":false}]}]"#;
const ERC721_TRANSFER: &str = r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[{"name":"from","type":"address","indexed":true},{"name":"to","type":"address","indexed":true},{"name":"tokenId","type":"uint256","indexed":true}]}]"#;

/// One selector, two layouts: the topic count picks the one a log was emitted with,
/// and each carries its own identity.
#[test]
fn a_shared_selector_decodes_by_topic_count() {
    let mut abi = Abi::from_json(ERC20_TRANSFER).expect("erc20");
    abi.merge(Abi::from_json(ERC721_TRANSFER).expect("erc721"))
        .expect("different topic counts do not conflict");
    let (selector, _) = *abi.events.keys().next().expect("event");
    let fungible = Log {
        topic0: Some(selector),
        topic1: Some(B256::ZERO),
        topic2: Some(B256::ZERO),
        data: DynSolValue::Uint(U256::from(5), 256).abi_encode().into(),
        ..Log::default()
    };
    let token = Log {
        topic3: Some(B256::with_last_byte(5)),
        data: Bytes::new(),
        ..fungible
    };
    let fungible = abi.decode_log(&fungible).expect("decode").expect("erc20");
    let token = abi.decode_log(&token).expect("decode").expect("erc721");
    assert_eq!(fungible.body[0].name, "value");
    assert_eq!(token.indexed[2].name, "tokenId");
    assert_ne!(fungible.id, token.id);
}

/// Merging another version keeps an identical event once and refuses a different
/// definition under the same key, since a log could then decode either way.
#[test]
fn merging_dedupes_identical_events_and_rejects_conflicts() {
    let mut abi = Abi::from_json(ERC20_TRANSFER).expect("erc20");
    abi.merge(Abi::from_json(ERC20_TRANSFER).expect("erc20"))
        .expect("identical events merge");
    assert_eq!(abi.events.len(), 1);
    let renamed = ERC20_TRANSFER.replace("\"value\"", "\"amount\"");
    match abi.merge(Abi::from_json(&renamed).expect("renamed")) {
        Err(AbiError::Conflict { selector }) => {
            assert_eq!(selector, keccak256("Transfer(address,address,uint256)"));
        }
        other => panic!("expected a conflict, got {other:?}"),
    }
}

/// One file listing an event twice identically keeps it once, as two files do, and
/// still refuses two definitions under one key.
#[test]
fn a_file_may_list_an_identical_event_twice() {
    let event = ERC20_TRANSFER.trim_start_matches('[').trim_end_matches(']');
    let twice = Abi::from_json(&format!("[{event},{event}]")).expect("identical events");
    assert_eq!(twice.events.len(), 1);
    let renamed = event.replace("\"value\"", "\"amount\"");
    assert!(matches!(
        Abi::from_json(&format!("[{event},{renamed}]")),
        Err(AbiError::Conflict { .. })
    ));
}

/// An event's identity is its own definition: an unrelated event added to the file
/// does not move it.
#[test]
fn an_event_id_ignores_the_rest_of_the_file() {
    let alone = Abi::from_json(ERC20_TRANSFER).expect("alone");
    let with_more = Abi::from_json(&ERC20_TRANSFER.replace(
        "]}]",
        r#"]},{"type":"event","name":"Other","anonymous":false,"inputs":[]}]"#,
    ))
    .expect("with another event");
    let id = |abi: &Abi| {
        abi.events
            .values()
            .find(|e| e.name == "Transfer")
            .expect("transfer")
            .id
    };
    assert_eq!(id(&alone), id(&with_more));
}

#[test]
fn actual_swap_fixture_decodes() {
    let abi = Abi::from_json(include_str!(
        "../../../protocols/uniswap/v3/UniswapV3Pool.json"
    ))
    .expect("pool ABI");
    let source: crate::wire::envelope::Envelope = serde_json::from_str(
        include_str!("../../../examples/fixtures/uniswap_v3_swaps.ndjson")
            .lines()
            .next()
            .expect("fixture"),
    )
    .expect("envelope");
    let crate::wire::envelope::Event::Log(log) = source.event else {
        panic!("log");
    };
    let decoded = abi.decode_log(&log).expect("decode").expect("match");
    assert_eq!(decoded.name, "Swap");
    let names =
        |args: &[DecodedArg]| -> Vec<String> { args.iter().map(|arg| arg.name.clone()).collect() };
    assert_eq!(names(&decoded.indexed), ["sender", "recipient"]);
    assert_eq!(
        names(&decoded.body),
        ["amount0", "amount1", "sqrtPriceX96", "liquidity", "tick"]
    );
    assert!(matches!(
        decoded.body[0].value,
        TypedValue::Int { bits: 256, .. }
    ));
}
