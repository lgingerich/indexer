//! Catalogs, logs, and decoded records the `decode` tests share.
#![expect(clippy::expect_used)]

use std::path::PathBuf;

use alloy_primitives::{I256, U256};

use super::derive::{DeriveError, Row};
use super::{Catalog, CatalogError};
use crate::wire::envelope::ChainId;
use crate::wire::typed::TypedValue;

/// The shipped protocols directory.
pub(super) fn protocols() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("protocols")
}

/// The shipped protocols, on `chain`.
pub(super) fn shipped(chain: &str) -> Catalog {
    Catalog::load(protocols(), &ChainId::new(chain)).expect("shipped protocols load")
}

/// A protocols directory in a temporary location holding one protocol of `files`, one of
/// which is its `protocol.toml`, loaded on Base.
pub(super) fn protocol(files: &[(&str, &str)]) -> Result<Catalog, CatalogError> {
    let root = std::env::temp_dir().join(format!(
        "indexer-protocol-{}-{:x}",
        std::process::id(),
        alloy_primitives::keccak256(format!("{files:?}"))
    ));
    let dir = root.join("p");
    std::fs::create_dir_all(&dir).expect("create protocol dir");
    for (name, contents) in files {
        std::fs::write(dir.join(name), contents).expect("write file");
    }
    let catalog = Catalog::load(&root, &ChainId::new("base"));
    std::fs::remove_dir_all(&root).expect("clean up");
    catalog
}

/// A signed value `bits` wide.
pub(super) fn int(value: i128, bits: u16) -> TypedValue {
    TypedValue::Int {
        value: I256::try_from(value).expect("fits"),
        bits,
    }
}

/// An unsigned value `bits` wide.
pub(super) const fn uint(value: U256, bits: u16) -> TypedValue {
    TypedValue::Uint { value, bits }
}

/// The values of the rows a derivation computed, in its order. Which column each value
/// names is checked against the table by `Derivation::rows`, for every derivation in the
/// shipped-ABI test of its protocol.
pub(super) fn unnamed(derived: Result<Vec<Row>, DeriveError>) -> Vec<Vec<TypedValue>> {
    derived
        .expect("derives")
        .into_iter()
        .map(|row| row.into_iter().map(|(_, value)| value).collect())
        .collect()
}

/// Asserts that `derived` refused argument `position`, `name`, for not holding
/// `expected`.
pub(super) fn refused(
    derived: Result<Vec<Row>, DeriveError>,
    position: usize,
    name: &str,
    expected: &str,
) {
    match derived {
        Err(DeriveError::Argument {
            position: at,
            name: named,
            expected: wanted,
        }) => assert_eq!((at, named, wanted), (position, name, expected)),
        other => panic!("expected {name} refused, got {other:?}"),
    }
}

/// Uniswap V3 on Base: the seeded factory's `PoolCreated` and a real pool `Swap`.
pub(super) mod uniswap_v3 {
    use alloy_dyn_abi::DynSolValue;
    use alloy_primitives::{Address, B256, I256, keccak256};

    use crate::wire::datasets::evm::Log;
    use crate::wire::envelope::{Envelope, Event};

    /// The factory the shipped manifest seeds on Base.
    pub(in crate::decode) const FACTORY: &str = "0x33128a8fC17869897dcE68Ed026d694621f6FDfD";

    /// The factory's `PoolCreated` naming `pool`, log 4 of block 10, hashed `block`.
    pub(in crate::decode) fn pool_created(pool: Address, block: B256) -> Log {
        Log {
            address: FACTORY.parse().expect("factory"),
            topic0: Some(keccak256(
                "PoolCreated(address,address,uint24,int24,address)",
            )),
            topic1: Some(B256::with_last_byte(1)),
            topic2: Some(B256::with_last_byte(2)),
            topic3: Some(B256::left_padding_from(&[0x0b, 0xb8])),
            data: DynSolValue::Tuple(vec![
                DynSolValue::Int(I256::try_from(60).expect("int"), 24),
                DynSolValue::Address(pool),
            ])
            .abi_encode_params()
            .into(),
            log_index: 4,
            block_number: 10,
            block_hash: block,
            ..Log::default()
        }
    }

    /// A real Uniswap V3 `Swap`, re-addressed to `pool`.
    pub(in crate::decode) fn swap(pool: Address) -> Log {
        let envelope: Envelope = serde_json::from_str(
            include_str!("../../examples/fixtures/uniswap_v3_swaps.ndjson")
                .lines()
                .next()
                .expect("fixture"),
        )
        .expect("envelope");
        let Event::Log(mut log) = envelope.event else {
            panic!("log");
        };
        log.address = pool;
        *log
    }
}

/// Metric pool events to derive from, and the words they pack, on Robinhood Chain.
pub(super) mod metric {
    use alloy_dyn_abi::DynSolValue;
    use alloy_primitives::{Address, B256, I256, U256, keccak256};

    use crate::decode::Decoder;
    use crate::decode::derive::metric::{BIN_FILLS, LIQUIDITY, POOL_BINS, SWAPS};
    use crate::sink::StoredContract;
    use crate::wire::datasets::evm::{Decoded, Log};

    use super::shipped;

    /// `Packed2Int128Lib.pack`: `high` in the top 128 bits, `low` in the bottom.
    pub(in crate::decode) fn int128s(high: i128, low: i128) -> U256 {
        (U256::from(high.cast_unsigned()) << 128) | U256::from(low.cast_unsigned())
    }

    /// `Packed2Uint128Lib.pack`.
    pub(in crate::decode) fn uint128s(high: u128, low: u128) -> U256 {
        (U256::from(high) << 128) | U256::from(low)
    }

    /// `PackedSwapDetailsLib.pack`.
    pub(in crate::decode) fn details(exact_input: bool, bin_idx: i16, pos_in_bin: U256) -> U256 {
        pos_in_bin
            | (U256::from(bin_idx.cast_unsigned()) << 104)
            | (U256::from(u8::from(exact_input)) << 120)
    }

    /// `BinDataLibrary.pack` for each bin, five to a word, least significant first:
    /// `(lengthE6, addFeeBuyE6, addFeeSellE6)`.
    pub(in crate::decode) fn ladder(bins: &[(u16, u16, u16)]) -> Vec<U256> {
        bins.chunks(5)
            .map(|word| {
                word.iter()
                    .enumerate()
                    .fold(U256::ZERO, |packed, (slot, &(length, buy, sell))| {
                        let bin =
                            U256::from(length) | (U256::from(buy) << 16) | (U256::from(sell) << 32);
                        packed | (bin << (48 * slot))
                    })
            })
            .collect()
    }

    /// The pool the fixtures' events come from, and the block that discovered it.
    const POOL: Address = Address::repeat_byte(0xd0);
    const CREATED: B256 = B256::with_last_byte(0x0b);

    /// A decoder over the shipped catalog that knows [`POOL`], as a previous run would
    /// have discovered it.
    pub(in crate::decode) fn decoder() -> Decoder {
        let mut decoder = Decoder::new(shipped("robinhood"));
        decoder.restore([StoredContract {
            protocol: "metric_v1".to_owned(),
            name: "MetricOmmPool".to_owned(),
            address: POOL,
            block_hash: CREATED,
            block_number: 1,
            log_index: 0,
        }]);
        decoder
    }

    /// A log of [`POOL`], in the block that created it.
    fn pool_log(topic0: B256, data: &DynSolValue) -> Log {
        Log {
            address: POOL,
            topic0: Some(topic0),
            data: data.abi_encode_params().into(),
            block_number: 1,
            block_hash: CREATED,
            ..Log::default()
        }
    }

    /// [`POOL`]'s `Swap` log carrying the three packed words, sent by `0x5e…` to `0x4e…`.
    pub(in crate::decode) fn swap_log(
        details: U256,
        amount_deltas: U256,
        platform_fees: U256,
    ) -> Log {
        Log {
            topic1: Some(Address::repeat_byte(0x5e).into_word()),
            topic2: Some(Address::repeat_byte(0x4e).into_word()),
            ..pool_log(
                keccak256(SWAPS.signatures[0]),
                &DynSolValue::Tuple(vec![
                    DynSolValue::Uint(details, 256),
                    DynSolValue::Uint(amount_deltas, 256),
                    DynSolValue::Uint(platform_fees, 256),
                ]),
            )
        }
    }

    /// [`swap_log`], decoded against the shipped catalog.
    pub(in crate::decode) fn swap(
        details: U256,
        amount_deltas: U256,
        platform_fees: U256,
    ) -> Decoded {
        decode(&swap_log(details, amount_deltas, platform_fees))
    }

    /// [`POOL`]'s `BinSwapped` for `bin_idx`, carrying the two packed words, decoded
    /// against the shipped catalog.
    pub(in crate::decode) fn bin_swapped(bin_idx: I256, bin_deltas: U256, fees: U256) -> Decoded {
        decode(&pool_log(
            keccak256(BIN_FILLS.signatures[0]),
            &DynSolValue::Tuple(vec![
                DynSolValue::Int(bin_idx, 256),
                DynSolValue::Uint(bin_deltas, 256),
                DynSolValue::Uint(fees, 256),
            ]),
        ))
    }

    /// [`POOL`]'s `LiquidityAdded`, or `LiquidityRemoved` when `removed`, for owner
    /// `0x0e…` and salt 7: per bin, its index, its `(delta0, delta1)`, and its shares.
    pub(in crate::decode) fn liquidity(removed: bool, bins: &[(i32, i128, i128, u64)]) -> Decoded {
        let event = LIQUIDITY.signatures[usize::from(removed)];
        let int = |value: i128| DynSolValue::Int(I256::try_from(value).expect("fits"), 256);
        decode(&Log {
            topic1: Some(Address::repeat_byte(0x0e).into_word()),
            topic2: Some(B256::with_last_byte(7)),
            ..pool_log(
                keccak256(event),
                &DynSolValue::Tuple(vec![
                    DynSolValue::Array(bins.iter().map(|&(bin, ..)| int(bin.into())).collect()),
                    DynSolValue::Array(
                        bins.iter()
                            .map(|&(_, delta0, delta1, _)| {
                                DynSolValue::Tuple(vec![int(delta0), int(delta1)])
                            })
                            .collect(),
                    ),
                    DynSolValue::Array(
                        bins.iter()
                            .map(|&(.., shares)| DynSolValue::Uint(U256::from(shares), 256))
                            .collect(),
                    ),
                ]),
            )
        })
    }

    /// The seeded factory's `PoolCreated` for [`POOL`], with bin 0's lower edge at
    /// `bin0_lower` and the two packed ladder arrays.
    pub(in crate::decode) fn pool_created(
        bin0_lower: i32,
        above: Vec<U256>,
        below: Vec<U256>,
    ) -> Decoded {
        let factory: Address = "0x2a53833cc95548cf52c7b159110e22D3a9018f32"
            .parse()
            .expect("the factory");
        let uint = |value: u64| DynSolValue::Uint(U256::from(value), 256);
        let words = |words: Vec<U256>| {
            DynSolValue::Array(
                words
                    .into_iter()
                    .map(|word| DynSolValue::Uint(word, 256))
                    .collect(),
            )
        };
        decode(&Log {
            address: factory,
            topic1: Some(POOL.into_word()),
            topic2: Some(Address::repeat_byte(0x70).into_word()),
            topic3: Some(Address::repeat_byte(0x71).into_word()),
            ..pool_log(
                keccak256(POOL_BINS.signatures[0]),
                &DynSolValue::Tuple(vec![
                    uint(1),
                    DynSolValue::Address(factory),
                    DynSolValue::Address(Address::repeat_byte(0xad)),
                    DynSolValue::Address(Address::repeat_byte(0xbb)),
                    DynSolValue::Array(Vec::new()),
                    DynSolValue::Tuple(vec![uint(0); 6]),
                    uint(0),
                    uint(1),
                    uint(1),
                    uint(1),
                    DynSolValue::Uint(U256::ZERO, 24),
                    DynSolValue::Uint(U256::ZERO, 24),
                    DynSolValue::Uint(U256::ZERO, 24),
                    DynSolValue::Uint(U256::ZERO, 24),
                    DynSolValue::Address(Address::repeat_byte(0xfe)),
                    DynSolValue::Int(I256::try_from(bin0_lower).expect("fits"), 24),
                    words(above),
                    words(below),
                ]),
            )
        })
    }

    fn decode(log: &Log) -> Decoded {
        decoder()
            .decode(log)
            .expect("a known pool's event")
            .decoded
            .expect("decodes")
    }
}
