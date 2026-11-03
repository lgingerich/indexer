//! The matching against real logs of the shipped Ethereum catalog: Metric pools naming
//! pairs, the Uniswap pools on them found and recorded, reorgs, and restarts.

// Test code: a failed expectation means the fixture or setup is wrong.
#![expect(clippy::expect_used)]

use alloy_dyn_abi::DynSolValue;
use alloy_primitives::{Address, B256, I256, U256, address, keccak256};

use super::{Kept, Matching};
use crate::decode::fixtures::shipped;
use crate::decode::{Decoder, Record};
use crate::wire::datasets::evm::{Contract, Decoded, Log};
use crate::wire::envelope::{BlockMeta, ChainId, Event, Matched};

pub(in crate::decode) const USDC: Address = address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
pub(in crate::decode) const WETH: Address = address!("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
const V3_FACTORY: Address = address!("0x1F98431c8aD98523631AE4a59f267346ea31F984");
const POOL_MANAGER: Address = address!("0x000000000004444c5dc75cB358380D2e3dE08A90");
const DAY: u64 = 86_400;
/// When the first Metric pool on the pair opened: after the V3 pool was created, and
/// within 30 days after the V4 pool was.
const METRIC_OPENED: u64 = 1_700_000_020 + 10 * DAY;

/// The matching over the shipped catalog on Ethereum, keeping 30 days, and a decoder
/// over the same catalog.
pub(in crate::decode) fn ethereum() -> (Matching, Decoder) {
    let chain = ChainId::new("ethereum");
    let matching = Matching::new(&shipped("ethereum"), &chain, 30 * DAY).expect("matches");
    (matching, Decoder::new(shipped("ethereum"), []))
}

/// `log` stamped as of `block` at `timestamp`.
fn at(log: Log, block: u64, timestamp: u64) -> Log {
    Log {
        block_number: block,
        block_hash: B256::with_last_byte(u8::try_from(block % 256).expect("small")),
        block_timestamp: timestamp,
        ..log
    }
}

/// Metric's `PoolCreated` on `token0`/`token1`, at `block` and `timestamp`.
pub(in crate::decode) fn metric(
    token0: Address,
    token1: Address,
    block: u64,
    timestamp: u64,
) -> Log {
    at(
        crate::sink::fixtures::metric_pool_created(token0, token1),
        block,
        timestamp,
    )
}

/// Uniswap V3's factory creating `pool` on `token0`/`token1` at fee 500.
pub(in crate::decode) fn v3_created(
    token0: Address,
    token1: Address,
    pool: Address,
    block: u64,
) -> Log {
    at(
        Log {
            address: V3_FACTORY,
            topic0: Some(keccak256(
                "PoolCreated(address,address,uint24,int24,address)",
            )),
            topic1: Some(token0.into_word()),
            topic2: Some(token1.into_word()),
            topic3: Some(B256::left_padding_from(&500_u16.to_be_bytes())),
            data: DynSolValue::Tuple(vec![
                DynSolValue::Int(I256::try_from(10).expect("fits"), 24),
                DynSolValue::Address(pool),
            ])
            .abi_encode_params()
            .into(),
            log_index: 3,
            ..Log::default()
        },
        block,
        1_600_000_000 + block,
    )
}

/// Uniswap V4's `PoolManager` initializing pool `id` on `currency0`/`currency1` at
/// fee 3000, with no hooks.
pub(in crate::decode) fn v4_initialized(
    id: B256,
    currency0: Address,
    currency1: Address,
    block: u64,
) -> Log {
    at(
        Log {
            address: POOL_MANAGER,
            topic0: Some(keccak256(
                "Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)",
            )),
            topic1: Some(id),
            topic2: Some(currency0.into_word()),
            topic3: Some(currency1.into_word()),
            data: DynSolValue::Tuple(vec![
                DynSolValue::Uint(U256::from(3000), 24),
                DynSolValue::Int(I256::try_from(60).expect("fits"), 24),
                DynSolValue::Address(Address::ZERO),
                DynSolValue::Uint(U256::from(1) << 96, 160),
                DynSolValue::Int(I256::ZERO, 24),
            ])
            .abi_encode_params()
            .into(),
            log_index: 5,
            ..Log::default()
        },
        block,
        1_700_000_000 + block,
    )
}

/// `log` decoded, with the contract its claims name.
pub(in crate::decode) fn decode(
    decoder: &Decoder,
    log: &Log,
) -> (Decoded, super::DecodedAs, Option<Contract>) {
    let (record, claims) = decoder.decode(log).expect("a listed contract's event");
    let Record { decoded, event } = record.expect("decodes");
    let contract = claims.contracts().next().cloned();
    (decoded, event, contract)
}

fn block(height: u64) -> BlockMeta {
    BlockMeta {
        height,
        hash: B256::with_last_byte(u8::try_from(height % 256).expect("small")),
        parent_hash: B256::ZERO,
        timestamp: 1_800_000_000 + height,
    }
}

fn matched(events: &[Event]) -> Vec<&Matched> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Matched(matched) => Some(matched.as_ref()),
            _ => None,
        })
        .collect()
}

/// The startup search asks for every Metric pool once; the pairs Metric pools name
/// are searched together for every Uniswap V3 pool and every V4 pool on them, the V4
/// ones holding the wrapped coin or the native one, and a known pair is not searched
/// again.
#[test]
fn new_pairs_are_searched_for_every_pool_on_them() {
    let (mut matching, decoder) = ethereum();
    let start = matching.start(100).expect("the first block asks");
    assert_eq!(
        start.addresses,
        [address!("0x2a53833cc95548cf52c7b159110e22d3a9018f32")]
    );
    assert_eq!(start.through, 100);
    assert!(matching.start(101).is_none(), "and only once");

    let dai = address!("0x6B175474E89094C44Da98b954EedeAC495271d0F");
    for (token0, token1) in [(USDC, WETH), (dai, USDC)] {
        let (record, event, _) = decode(&decoder, &metric(token0, token1, 50, 30 * DAY));
        assert!(!matching.fetched(&record, event, None));
    }
    let queries = matching.queries(100);
    let word = |address: Address| address.into_word();
    let mut asked: Vec<_> = queries
        .iter()
        .map(|query| (query.addresses.clone(), query.topics[1..].to_vec()))
        .collect();
    asked.sort();
    let mut lower = vec![word(USDC), word(dai)];
    lower.sort();
    let mut upper = vec![word(USDC), word(WETH)];
    upper.sort();
    let mut expected = vec![
        (
            vec![V3_FACTORY],
            vec![lower.clone(), upper.clone(), Vec::new()],
        ),
        (vec![POOL_MANAGER], vec![Vec::new(), lower, upper]),
        (
            vec![POOL_MANAGER],
            vec![Vec::new(), vec![word(Address::ZERO)], vec![word(USDC)]],
        ),
    ];
    expected.sort();
    assert_eq!(asked, expected);
    assert!(queries.iter().all(|query| query.through == 100));
    assert!(matching.queries(101).is_empty(), "searched once");

    let (record, event, _) = decode(&decoder, &metric(USDC, WETH, 60, 31 * DAY));
    assert!(!matching.fetched(&record, event, None));
    assert!(
        matching.queries(102).is_empty(),
        "a known pair is not searched again"
    );
}

/// Pools a fetch finds on a known pair join at the next block: a V3 pool with its
/// `contracts` row, a V4 pool by id, each kept from then on and filled before it, from
/// the lookback before the pair's first Metric pool. A pool on another pair is not
/// matched, and one created in the stream is kept from its creation, filled already.
#[test]
fn found_pools_join_at_the_next_block_and_are_filled_before_it() {
    let (mut matching, mut decoder) = ethereum();
    let (record, event, _) = decode(&decoder, &metric(USDC, WETH, 50, METRIC_OPENED));
    let _ = matching.fetched(&record, event, None);

    let pool = Address::repeat_byte(0x31);
    let id = B256::repeat_byte(0x41);
    let (record, event, contract) = decode(&decoder, &v3_created(USDC, WETH, pool, 10));
    assert!(contract.is_some(), "a pool the run does not decode yet");
    assert!(matching.fetched(&record, event, contract));
    let (record, event, _) = decode(&decoder, &v4_initialized(id, Address::ZERO, USDC, 20));
    assert!(matching.fetched(&record, event, None));
    let other = B256::repeat_byte(0x42);
    let (record, event, _) = decode(
        &decoder,
        &v4_initialized(other, USDC, Address::repeat_byte(0x99), 20),
    );
    assert!(!matching.fetched(&record, event, None), "another pair");

    let kept = matching.kept();
    let swap = |address| {
        Event::Log(Box::new(Log {
            address,
            ..Log::default()
        }))
    };
    assert!(kept.read().expect("readable").keeps(&swap(pool)));
    assert!(
        !kept
            .read()
            .expect("readable")
            .keeps(&swap(Address::repeat_byte(0x32)))
    );

    let records = matching.events(&block(101));
    assert!(
        matches!(records[0], Event::Contract(_)),
        "the pool's row first"
    );
    let pools = matched(&records);
    assert_eq!(pools.len(), 2);
    let v3 = pools
        .iter()
        .find(|pool| pool.pool_id.is_none())
        .expect("v3");
    assert_eq!(
        (v3.contract, v3.token0, v3.token1, v3.fee),
        (pool, USDC, WETH, 500)
    );
    assert_eq!(
        v3.from_timestamp,
        METRIC_OPENED - 30 * DAY,
        "30 days before the pair's first pool"
    );
    assert!(!v3.filled);
    let v4 = pools
        .iter()
        .find(|pool| pool.pool_id.is_some())
        .expect("v4");
    assert_eq!(
        (v4.contract, v4.pool_id, v4.token0, v4.fee),
        (POOL_MANAGER, Some(id), Address::ZERO, 3000)
    );
    assert_eq!(v4.block_number, 101);
    assert_eq!(
        v4.from_timestamp, 1_700_000_020,
        "created within the lookback, so kept from its creation"
    );

    let fill = matching.recorded(&block(101)).expect("a fill");
    assert_eq!(fill.through, 100);
    assert_eq!(fill.pools.len(), 2);
    assert!(matching.events(&block(102)).is_empty(), "recorded once");

    // A pool created in the stream on the pair is kept from its creation.
    let created = Address::repeat_byte(0x33);
    let (record, event, contract) = decode(&decoder, &v3_created(WETH, USDC, created, 102));
    if let Some(contract) = contract {
        let (_, claims) = decoder
            .decode(&v3_created(WETH, USDC, created, 102))
            .expect("decodes");
        decoder.admit(claims);
        let _ = contract;
    }
    matching.decoded(&record, event);
    let records = matching.events(&block(102));
    assert_eq!(matched(&records).len(), 1);
    assert!(matched(&records)[0].filled, "nothing before its creation");
    assert!(
        matching.recorded(&block(102)).is_none(),
        "and nothing to fill"
    );

    // Once filled, each is recorded so.
    matching.filled(&fill);
    let records = matching.events(&block(103));
    assert!(matched(&records).iter().all(|pool| pool.filled));
    assert_eq!(matched(&records).len(), 2);
    assert!(matching.recorded(&block(103)).is_none());
}

/// A reorg of the block recording a pool records it again with the next, without
/// filling it again; a restart takes the records back, keeps their pools, and fills
/// what was not filled.
#[test]
fn records_survive_a_reorg_and_a_restart() {
    let (mut matching, decoder) = ethereum();
    let (record, event, _) = decode(&decoder, &metric(USDC, WETH, 50, 40 * DAY));
    let _ = matching.fetched(&record, event, None);
    let id = B256::repeat_byte(0x41);
    let (record, event, _) = decode(&decoder, &v4_initialized(id, USDC, WETH, 20));
    assert!(matching.fetched(&record, event, None));
    let records = matching.events(&block(101));
    assert!(matching.recorded(&block(101)).is_some());

    matching.rejoin(&[block(101).hash]);
    let again = matching.events(&block(102));
    assert_eq!(matched(&again).len(), 1);
    assert!(matching.recorded(&block(102)).is_none(), "filled only once");

    let (mut restarted, _) = ethereum();
    let stored: Vec<Matched> = matched(&again).into_iter().cloned().collect();
    let fills = restarted.restore(stored);
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].through, 101);
    let manager = Event::Decoded(Box::new(record));
    assert!(restarted.kept().read().expect("readable").keeps(&manager));
    let _ = records;
}

/// A V3 pool created in the stream on a known pair is kept from its creation, its
/// factory's record creating it too; a reorg orphaning the block that created it
/// forgets it rather than recording it again.
#[test]
fn a_pool_created_in_an_orphaned_block_is_forgotten() {
    let (mut matching, decoder) = ethereum();
    let (record, event, _) = decode(&decoder, &metric(USDC, WETH, 50, METRIC_OPENED));
    let _ = matching.fetched(&record, event, None);
    let pool = Address::repeat_byte(0x35);
    let created = v3_created(USDC, WETH, pool, 102);
    let (record, event, _) = decode(&decoder, &created);
    matching.decoded(&record, event);
    let kept = matching.kept();
    assert!(
        kept.read()
            .expect("readable")
            .keeps(&Event::Decoded(Box::new(record))),
        "the factory's record creating a kept pool"
    );
    assert_eq!(matched(&matching.events(&block(102))).len(), 1);
    assert!(matching.recorded(&block(102)).is_none());
    matching.rejoin(&[block(102).hash]);
    assert!(
        matching.events(&block(103)).is_empty(),
        "not recorded again"
    );
    let swap = Event::Log(Box::new(Log {
        address: pool,
        ..Log::default()
    }));
    assert!(
        !kept.read().expect("readable").keeps(&swap),
        "and no longer kept"
    );
}

/// A restart takes each pool's newest record, so a reorg of the block holding its
/// `filled` record is seen, and fills only a pool with no `filled` record, up to the
/// block of its first.
#[test]
fn a_restart_takes_the_newest_record_and_fills_only_what_is_unfilled() {
    let (mut matching, _) = ethereum();
    let record = |id: u8, height: u64, filled: bool| Matched {
        protocol: "uniswap_v4".to_owned(),
        contract: POOL_MANAGER,
        pool_id: Some(B256::repeat_byte(id)),
        token0: USDC,
        token1: WETH,
        fee: 3000,
        hooks: None,
        created_block: 1,
        from_timestamp: 0,
        filled,
        block_number: height,
        block_hash: block(height).hash,
        block_timestamp: block(height).timestamp,
    };
    let fills = matching.restore(vec![
        record(1, 10, true),
        record(1, 5, false),
        record(2, 7, false),
    ]);
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].through, 6);
    assert_eq!(fills[0].pools[0].pool_id, Some(B256::repeat_byte(2)));
    matching.rejoin(&[block(10).hash]);
    let again = matching.events(&block(11));
    assert_eq!(matched(&again).len(), 1);
    assert!(matched(&again)[0].filled, "the filled record written again");
}

/// A contract holding many pools keeps the decoded records naming a kept pool's
/// `id`, none of its raw logs, and no record naming another pool or a token's `id`.
#[test]
fn a_pool_manager_keeps_only_its_kept_pools_records() {
    let (_, decoder) = ethereum();
    let id = B256::repeat_byte(0x41);
    let kept = Kept::of(&[], &[(POOL_MANAGER, id)]);
    let ours = v4_initialized(id, USDC, WETH, 20);
    let theirs = v4_initialized(B256::repeat_byte(0x42), USDC, WETH, 20);
    let (record, _, _) = decode(&decoder, &ours);
    assert!(kept.keeps(&Event::Decoded(Box::new(record))));
    let (record, _, _) = decode(&decoder, &theirs);
    assert!(!kept.keeps(&Event::Decoded(Box::new(record))));
    assert!(!kept.keeps(&Event::Log(Box::new(ours))), "no raw log");
    let transfer = Log {
        address: POOL_MANAGER,
        topic0: Some(keccak256(
            "Transfer(address,address,address,uint256,uint256)",
        )),
        topic1: Some(Address::repeat_byte(5).into_word()),
        topic2: Some(Address::repeat_byte(6).into_word()),
        topic3: Some(id),
        data: vec![0; 64].into(),
        ..Log::default()
    };
    let (record, _, _) = decode(&decoder, &transfer);
    assert!(
        !kept.keeps(&Event::Decoded(Box::new(record))),
        "a token's id is not a pool's"
    );
}
