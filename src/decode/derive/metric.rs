//! Metric OMM v1: pool events with their packed words unpacked, as the pool's
//! `types/Packed.sol` lays them out.
//!
//! `metric_v1_swaps` is one row per `Swap`, which packs five values into three words:
//!
//! | Word | Bits | Column |
//! | --- | --- | --- |
//! | `amountDeltas` (`Packed2Int128`) | 128–255, signed | `amount0_raw` |
//! | | 0–127, signed | `amount1_raw` |
//! | `platformFees` (`Packed2Uint128`) | 128–255 | `platform_spread_fee_scaled` |
//! | | 0–127 | `platform_notional_fee_scaled` |
//! | `details` (`PackedSwapDetails`) | 0–103 | `pos_in_bin_after` |
//! | | 104–119, signed | `bin_idx_after` |
//! | | 120 | `exact_input` |
//!
//! Units follow the contract. `_raw` amounts are native token units, from the pool's
//! side: positive when the pool received the token. `_scaled` fees are the pool's
//! internal units, both tokens at `max(18, decimals0, decimals1)` decimals, on the input
//! token. `zero_for_one` is whether the pool received token0, which is selling token0
//! for token1. `bin_idx_after` and `pos_in_bin_after` are where the pool's price sits
//! after the swap.
//!
//! `metric_v1_bin_fills` is one row per `BinSwapped`: one bin a swap filled, emitted
//! before that swap's `Swap` log.
//!
//! | Word | Bits | Column |
//! | --- | --- | --- |
//! | `binDeltas` (`Packed2Int128`) | 128–255, signed | `delta0_scaled` |
//! | | 0–127, signed | `delta1_scaled` |
//! | `fees` (`Packed2Uint128`) | 128–255 | `lp_fee_scaled` |
//! | | 0–127 | `platform_fee_scaled` |
//!
//! The deltas are the change in the bin's own balances, positive when the bin gained the
//! token; the input side includes the LP fee, which stays in the bin. The platform fee
//! never enters a bin.
//!
//! `metric_v1_liquidity` is one row per bin of a `LiquidityAdded` or `LiquidityRemoved`,
//! whose parallel arrays name the bins, their scaled balance deltas, and the shares minted
//! or burned in each. A position is `(pool, owner, salt)`, with shares per bin. `shares`
//! is signed like the deltas: positive on an add, negative on a remove.
//!
//! `metric_v1_pool_bins` is one row per bin of a pool's ladder, from the factory's
//! `PoolCreated`, as `_unpackAndValidateBinStates` reads it: five bins per word, 48 bits
//! each, least significant first, `lengthE6` in bits 0–15, `addFeeBuyE6` 16–31, and
//! `addFeeSellE6` 32–47. The first zero length ends a side. Entry `k` of the non-negative
//! array is bin `k`, of the negative array bin `-k-1`. Edges are distances from the
//! reference price in E6: bin 0's lower edge is `curBinDistFromProvidedPriceE6`, and each
//! bin's runs a length on from its neighbour's. The additional fees are as created;
//! `BinAdditionalFeesUpdated` changes them later.
//!
//! A bin index is an `int256` in the events and an `int16` in the pool, so a value outside
//! that range is unreadable.

use alloy_primitives::{Bytes, I256, U256};

use crate::sink::table::ColumnType;
use crate::wire::envelope::{Decoded, TypedValue};

use super::{Derivation, DeriveError, address, array, int, uint};

/// `metric_v1_swaps`: one row per pool `Swap`.
pub(super) const SWAPS: Derivation = Derivation {
    protocol: "metric_v1",
    contract: "MetricOmmPool",
    signatures: &["Swap(address,address,uint256,uint256,uint256)"],
    table: "metric_v1_swaps",
    columns: &[
        ("sender", ColumnType::Text),
        ("recipient", ColumnType::Text),
        ("amount0_raw", ColumnType::BigInt),
        ("amount1_raw", ColumnType::BigInt),
        ("zero_for_one", ColumnType::Bool),
        ("exact_input", ColumnType::Bool),
        ("bin_idx_after", ColumnType::Int),
        ("pos_in_bin_after", ColumnType::BigInt),
        ("platform_spread_fee_scaled", ColumnType::BigInt),
        ("platform_notional_fee_scaled", ColumnType::BigInt),
    ],
    derive: swap,
};

/// A `Swap`'s row, in [`SWAPS`]' column order.
fn swap(decoded: &Decoded) -> Result<Vec<Vec<TypedValue>>, DeriveError> {
    let sender = address(decoded, 0, "sender")?;
    let recipient = address(decoded, 1, "recipient")?;
    let details = uint(decoded, 2, "details", 256)?;
    let deltas = uint(decoded, 3, "amountDeltas", 256)?;
    let fees = uint(decoded, 4, "platformFees", 256)?;
    let amount0 = signed_bits(deltas, 128, 128);
    Ok(vec![vec![
        TypedValue::Address { value: sender },
        TypedValue::Address { value: recipient },
        signed(amount0, 128),
        signed(signed_bits(deltas, 0, 128), 128),
        TypedValue::Bool {
            value: amount0.is_positive(),
        },
        TypedValue::Bool {
            value: details.bit(120),
        },
        signed(signed_bits(details, 104, 16), 16),
        unsigned(bits(details, 0, 104), 104),
        unsigned(bits(fees, 128, 128), 128),
        unsigned(bits(fees, 0, 128), 128),
    ]])
}

/// `metric_v1_bin_fills`: one row per bin a swap filled.
pub(super) const BIN_FILLS: Derivation = Derivation {
    protocol: "metric_v1",
    contract: "MetricOmmPool",
    signatures: &["BinSwapped(int256,uint256,uint256)"],
    table: "metric_v1_bin_fills",
    columns: &[
        ("bin_idx", ColumnType::Int),
        ("delta0_scaled", ColumnType::BigInt),
        ("delta1_scaled", ColumnType::BigInt),
        ("lp_fee_scaled", ColumnType::BigInt),
        ("platform_fee_scaled", ColumnType::BigInt),
    ],
    derive: bin_fill,
};

/// A `BinSwapped`'s row, in [`BIN_FILLS`]' column order.
fn bin_fill(decoded: &Decoded) -> Result<Vec<Vec<TypedValue>>, DeriveError> {
    let bin = bin_index(int(decoded, 0, "binIdx", 256)?, 0, "binIdx")?;
    let deltas = uint(decoded, 1, "binDeltas", 256)?;
    let fees = uint(decoded, 2, "fees", 256)?;
    Ok(vec![vec![
        bin,
        signed(signed_bits(deltas, 128, 128), 128),
        signed(signed_bits(deltas, 0, 128), 128),
        unsigned(bits(fees, 128, 128), 128),
        unsigned(bits(fees, 0, 128), 128),
    ]])
}

/// `metric_v1_liquidity`: one row per bin of a liquidity add or remove.
pub(super) const LIQUIDITY: Derivation = Derivation {
    protocol: "metric_v1",
    contract: "MetricOmmPool",
    signatures: &[
        "LiquidityAdded(address,uint80,int256[],(int256,int256)[],uint256[])",
        "LiquidityRemoved(address,uint80,int256[],(int256,int256)[],uint256[])",
    ],
    table: "metric_v1_liquidity",
    columns: &[
        ("direction", ColumnType::Text),
        ("owner", ColumnType::Text),
        ("salt", ColumnType::BigInt),
        ("bin_idx", ColumnType::Int),
        ("delta0_scaled", ColumnType::BigInt),
        ("delta1_scaled", ColumnType::BigInt),
        ("shares", ColumnType::BigInt),
    ],
    derive: liquidity,
};

/// A `LiquidityAdded` or `LiquidityRemoved`'s rows, one per bin, in [`LIQUIDITY`]'
/// column order.
fn liquidity(decoded: &Decoded) -> Result<Vec<Vec<TypedValue>>, DeriveError> {
    let removed = decoded.name == "LiquidityRemoved";
    let direction = if removed { "remove" } else { "add" };
    let owner = address(decoded, 0, "provider")?;
    let salt = uint(decoded, 1, "salt", 80)?;
    let bins = array(decoded, 2, "binIdxs")?;
    let balances = array(decoded, 3, "binBalanceDeltas")?;
    let shares = array(decoded, 4, "shares")?;
    let one_per_bin = |position, name| DeriveError::Argument {
        position,
        name,
        expected: "an array with one entry per bin",
    };
    if balances.len() != bins.len() {
        return Err(one_per_bin(3, "binBalanceDeltas"));
    }
    if shares.len() != bins.len() {
        return Err(one_per_bin(4, "shares"));
    }
    bins.iter()
        .zip(balances)
        .zip(shares)
        .map(|((bin, delta), minted)| {
            let bin = match bin {
                TypedValue::Int { value, bits: 256 } => bin_index(*value, 2, "binIdxs")?,
                _ => return Err(element(2, "binIdxs", "int256 entries")),
            };
            let TypedValue::Tuple { value: delta } = delta else {
                return Err(element(3, "binBalanceDeltas", "(int256,int256) entries"));
            };
            let [
                TypedValue::Int {
                    value: delta0,
                    bits: 256,
                },
                TypedValue::Int {
                    value: delta1,
                    bits: 256,
                },
            ] = delta[..]
            else {
                return Err(element(3, "binBalanceDeltas", "(int256,int256) entries"));
            };
            let TypedValue::Uint {
                value: minted,
                bits: 256,
            } = minted
            else {
                return Err(element(4, "shares", "uint256 entries"));
            };
            let shares = I256::checked_from_sign_and_abs(
                if removed {
                    alloy_primitives::Sign::Negative
                } else {
                    alloy_primitives::Sign::Positive
                },
                *minted,
            )
            .ok_or_else(|| element(4, "shares", "entries within int256"))?;
            Ok(vec![
                TypedValue::String {
                    value: Bytes::from_static(direction.as_bytes()),
                    text: Some(direction.to_owned()),
                },
                TypedValue::Address { value: owner },
                unsigned(salt, 80),
                bin,
                signed(delta0, 256),
                signed(delta1, 256),
                signed(shares, 256),
            ])
        })
        .collect()
}

/// `metric_v1_pool_bins`: one row per bin of a pool's ladder, as created.
pub(super) const POOL_BINS: Derivation = Derivation {
    protocol: "metric_v1",
    contract: "MetricOmmPoolFactory",
    signatures: &[
        "PoolCreated(address,address,address,uint256,address,address,address,address[],\
         (uint256,uint256,uint256,uint256,uint256,uint256),uint256,uint256,uint256,uint256,\
         uint24,uint24,uint24,uint24,address,int24,uint256[],uint256[])",
    ],
    table: "metric_v1_pool_bins",
    columns: &[
        ("pool", ColumnType::Text),
        ("bin_idx", ColumnType::Int),
        ("length_e6", ColumnType::Int),
        ("add_fee_buy_e6", ColumnType::Int),
        ("add_fee_sell_e6", ColumnType::Int),
        ("lower_e6", ColumnType::Int),
        ("upper_e6", ColumnType::Int),
    ],
    derive: pool_bins,
};

/// A `PoolCreated`'s rows, one per bin from the lowest to the highest, in [`POOL_BINS`]'
/// column order.
fn pool_bins(decoded: &Decoded) -> Result<Vec<Vec<TypedValue>>, DeriveError> {
    let pool = address(decoded, 0, "poolAddress")?;
    let bin0_lower = int(decoded, 18, "curBinDistFromProvidedPriceE6", 24)?;
    let above = ladder_side(
        array(decoded, 19, "nonNegativeBinDataArray")?,
        19,
        "nonNegativeBinDataArray",
    )?;
    let below = ladder_side(
        array(decoded, 20, "negativeBinDataArray")?,
        20,
        "negativeBinDataArray",
    )?;
    let row = |index: I256, [length, buy, sell]: [U256; 3], lower: I256, upper: I256| {
        vec![
            TypedValue::Address { value: pool },
            signed(index, 16),
            unsigned_small(length),
            unsigned_small(buy),
            unsigned_small(sell),
            signed(lower, 64),
            signed(upper, 64),
        ]
    };
    let mut rows = Vec::with_capacity(above.len() + below.len());
    // Below bin 0, each bin's upper edge is the lower edge of the bin above it.
    let mut upper = bin0_lower;
    let mut index = I256::ZERO;
    for &bin in &below {
        let lower = upper - I256::from_raw(bin[0]);
        index -= I256::ONE;
        rows.push(row(index, bin, lower, upper));
        upper = lower;
    }
    rows.reverse();
    let mut lower = bin0_lower;
    let mut index = I256::ZERO;
    for &bin in &above {
        let upper = lower + I256::from_raw(bin[0]);
        rows.push(row(index, bin, lower, upper));
        index += I256::ONE;
        lower = upper;
    }
    Ok(rows)
}

/// One side of a ladder: each bin's `[lengthE6, addFeeBuyE6, addFeeSellE6]`, nearest to
/// bin 0 first, up to the first zero length.
fn ladder_side(
    words: &[TypedValue],
    position: usize,
    name: &'static str,
) -> Result<Vec<[U256; 3]>, DeriveError> {
    let mut bins = Vec::new();
    for word in words {
        let TypedValue::Uint {
            value: word,
            bits: 256,
        } = word
        else {
            return Err(element(position, name, "uint256 entries"));
        };
        for slot in 0..5 {
            let bin = bits(*word, 48 * slot, 48);
            let length = bits(bin, 0, 16);
            if length.is_zero() {
                return Ok(bins);
            }
            bins.push([length, bits(bin, 16, 16), bits(bin, 32, 16)]);
        }
    }
    Ok(bins)
}

/// A bin index as the pool keeps it, an `int16`, from the `int256` an event carries.
fn bin_index(value: I256, position: usize, name: &'static str) -> Result<TypedValue, DeriveError> {
    i16::try_from(value)
        .map(|_| signed(value, 16))
        .map_err(|_| element(position, name, "bin indexes in the int16 range"))
}

/// The error for argument `position`, `name`, not holding what the derivation reads.
const fn element(position: usize, name: &'static str, expected: &'static str) -> DeriveError {
    DeriveError::Argument {
        position,
        name,
        expected,
    }
}

/// A signed value `bits` wide.
const fn signed(value: I256, bits: u16) -> TypedValue {
    TypedValue::Int { value, bits }
}

/// An unsigned value `bits` wide.
const fn unsigned(value: U256, bits: u16) -> TypedValue {
    TypedValue::Uint { value, bits }
}

/// A 16-bit unsigned field, stored in a signed 64-bit column.
fn unsigned_small(value: U256) -> TypedValue {
    signed(I256::from_raw(value), 32)
}

/// `len` bits of `word` from bit `low`, the least significant being bit 0.
fn bits(word: U256, low: usize, len: usize) -> U256 {
    (word >> low) & (U256::MAX >> (256 - len))
}

/// `len` bits of `word` from bit `low`, read as a two's-complement signed integer.
fn signed_bits(word: U256, low: usize, len: usize) -> I256 {
    let value = bits(word, low, len);
    if value.bit(len - 1) {
        I256::from_raw(value | (U256::MAX << len))
    } else {
        I256::from_raw(value)
    }
}

/// Metric pool events to derive from, and the words they pack.
#[cfg(test)]
#[expect(clippy::expect_used)]
pub(in crate::decode) mod fixtures {
    use alloy_dyn_abi::DynSolValue;
    use alloy_primitives::{Address, B256, I256, U256, keccak256};

    use crate::decode::{Catalog, Decoder};
    use crate::wire::envelope::{ChainId, Decoded, Log, StoredContract};

    use super::{BIN_FILLS, LIQUIDITY, POOL_BINS, SWAPS};

    /// The shipped protocols, on Robinhood Chain.
    pub(in crate::decode) fn catalog() -> Catalog {
        Catalog::load(
            format!("{}/protocols", env!("CARGO_MANIFEST_DIR")),
            &ChainId::new("robinhood"),
        )
        .expect("shipped protocols load")
    }

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

    /// The pool the fixtures' swaps come from, and the block that discovered it.
    const POOL: Address = Address::repeat_byte(0xd0);
    const CREATED: B256 = B256::with_last_byte(0x0b);

    /// A decoder over the shipped catalog that knows [`POOL`], as a previous run would
    /// have discovered it.
    pub(in crate::decode) fn decoder() -> Decoder {
        let mut decoder = Decoder::new(catalog());
        decoder.restore([StoredContract {
            protocol: "metric_v1".to_owned(),
            name: "MetricOmmPool".to_owned(),
            address: POOL,
            block_hash: CREATED,
        }]);
        decoder
    }

    /// [`POOL`]'s `Swap` log carrying the three packed words, sent by `0x5e…` to `0x4e…`.
    pub(in crate::decode) fn swap_log(
        details: U256,
        amount_deltas: U256,
        platform_fees: U256,
    ) -> Log {
        Log {
            address: POOL,
            topic0: Some(keccak256(SWAPS.signatures[0])),
            topic1: Some(Address::repeat_byte(0x5e).into_word()),
            topic2: Some(Address::repeat_byte(0x4e).into_word()),
            data: DynSolValue::Tuple(vec![
                DynSolValue::Uint(details, 256),
                DynSolValue::Uint(amount_deltas, 256),
                DynSolValue::Uint(platform_fees, 256),
            ])
            .abi_encode_params()
            .into(),
            block_number: 1,
            block_hash: CREATED,
            ..Log::default()
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
        decode(&Log {
            address: POOL,
            topic0: Some(keccak256(BIN_FILLS.signatures[0])),
            data: DynSolValue::Tuple(vec![
                DynSolValue::Int(bin_idx, 256),
                DynSolValue::Uint(bin_deltas, 256),
                DynSolValue::Uint(fees, 256),
            ])
            .abi_encode_params()
            .into(),
            block_number: 1,
            block_hash: CREATED,
            ..Log::default()
        })
    }

    /// [`POOL`]'s `LiquidityAdded`, or `LiquidityRemoved` when `removed`, for owner
    /// `0x0e…` and salt 7: per bin, its index, its `(delta0, delta1)`, and its shares.
    pub(in crate::decode) fn liquidity(removed: bool, bins: &[(i32, i128, i128, u64)]) -> Decoded {
        let event = LIQUIDITY.signatures[usize::from(removed)];
        let int = |value: i128| DynSolValue::Int(I256::try_from(value).expect("fits"), 256);
        decode(&Log {
            address: POOL,
            topic0: Some(keccak256(event)),
            topic1: Some(Address::repeat_byte(0x0e).into_word()),
            topic2: Some(B256::with_last_byte(7)),
            data: DynSolValue::Tuple(vec![
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
            ])
            .abi_encode_params()
            .into(),
            block_number: 1,
            block_hash: CREATED,
            ..Log::default()
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
            topic0: Some(keccak256(POOL_BINS.signatures[0])),
            topic1: Some(POOL.into_word()),
            topic2: Some(Address::repeat_byte(0x70).into_word()),
            topic3: Some(Address::repeat_byte(0x71).into_word()),
            data: DynSolValue::Tuple(vec![
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
            ])
            .abi_encode_params()
            .into(),
            block_number: 1,
            block_hash: CREATED,
            ..Log::default()
        })
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

    fn decode(log: &Log) -> Decoded {
        decoder()
            .decode(log)
            .expect("decodes")
            .expect("a known pool's event")
            .decoded
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{Address, I256, U256};

    use crate::wire::envelope::{ChainId, TypedValue};

    use super::super::DeriveError;
    use super::fixtures::{catalog, details, int128s, uint128s};
    use super::{
        BIN_FILLS, LIQUIDITY, POOL_BINS, SWAPS, bin_fill, fixtures, liquidity, pool_bins, swap,
    };

    fn int(value: i128, bits: u16) -> TypedValue {
        TypedValue::Int {
            value: I256::try_from(value).expect("fits"),
            bits,
        }
    }

    fn uint(value: U256, bits: u16) -> TypedValue {
        TypedValue::Uint { value, bits }
    }

    fn max_pos() -> U256 {
        (U256::ONE << 104) - U256::ONE
    }

    /// Asserts that `derived` refused argument `position`, `name`, for not holding
    /// `expected`.
    fn refused(
        derived: Result<Vec<Vec<TypedValue>>, DeriveError>,
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
            Ok(rows) => panic!("expected {name} refused, derived {rows:?}"),
        }
    }
    /// The boundaries `Packed.t.sol` pins: both int128 extremes in either half, a
    /// negative low half that must not leak into the high one, both uint128 halves full,
    /// and the details at their extremes with a negative bin index.
    #[test]
    fn a_swap_unpacks_as_the_contract_packs() {
        let cases = [
            (
                details(true, i16::MAX, max_pos()),
                int128s(i128::MIN, i128::MAX),
                uint128s(u128::MAX, u128::MAX),
                [
                    int(i128::MIN, 128),
                    int(i128::MAX, 128),
                    TypedValue::Bool { value: false },
                    TypedValue::Bool { value: true },
                    int(i16::MAX.into(), 16),
                    uint(max_pos(), 104),
                    uint(U256::from(u128::MAX), 128),
                    uint(U256::from(u128::MAX), 128),
                ],
            ),
            (
                details(false, i16::MIN, max_pos()),
                int128s(i128::MAX, i128::MIN),
                uint128s(0, u128::MAX),
                [
                    int(i128::MAX, 128),
                    int(i128::MIN, 128),
                    TypedValue::Bool { value: true },
                    TypedValue::Bool { value: false },
                    int(i16::MIN.into(), 16),
                    uint(max_pos(), 104),
                    uint(U256::ZERO, 128),
                    uint(U256::from(u128::MAX), 128),
                ],
            ),
            (
                details(true, -1, U256::ONE),
                int128s(7, -1),
                uint128s(3, 0),
                [
                    int(7, 128),
                    int(-1, 128),
                    TypedValue::Bool { value: true },
                    TypedValue::Bool { value: true },
                    int(-1, 16),
                    uint(U256::ONE, 104),
                    uint(U256::from(3), 128),
                    uint(U256::ZERO, 128),
                ],
            ),
        ];
        for (details, amounts, fees, expected) in cases {
            let [values] = &swap(&fixtures::swap(details, amounts, fees)).expect("derives")[..]
            else {
                panic!("one row");
            };
            assert_eq!(values.len(), SWAPS.columns.len());
            assert_eq!(
                values[..2],
                [
                    TypedValue::Address {
                        value: Address::repeat_byte(0x5e)
                    },
                    TypedValue::Address {
                        value: Address::repeat_byte(0x4e)
                    },
                ]
            );
            assert_eq!(values[2..], expected);
        }
    }

    /// The same boundaries for a fill: both int128 extremes in either delta, a negative
    /// low delta that must not leak into the high one, and either fee full. A bin index
    /// outside the pool's int16 range is unreadable rather than clipped.
    #[test]
    fn a_bin_fill_unpacks_as_the_contract_packs() {
        let bin = |value: i32| I256::try_from(value).expect("fits");
        let cases = [
            (
                bin(-1),
                int128s(i128::MIN, i128::MAX),
                uint128s(u128::MAX, 0),
                [
                    int(-1, 16),
                    int(i128::MIN, 128),
                    int(i128::MAX, 128),
                    uint(U256::from(u128::MAX), 128),
                    uint(U256::ZERO, 128),
                ],
            ),
            (
                bin(i16::MAX.into()),
                int128s(7, -1),
                uint128s(3, u128::MAX),
                [
                    int(i16::MAX.into(), 16),
                    int(7, 128),
                    int(-1, 128),
                    uint(U256::from(3), 128),
                    uint(U256::from(u128::MAX), 128),
                ],
            ),
            (
                bin(i16::MIN.into()),
                U256::ZERO,
                U256::ZERO,
                [
                    int(i16::MIN.into(), 16),
                    int(0, 128),
                    int(0, 128),
                    uint(U256::ZERO, 128),
                    uint(U256::ZERO, 128),
                ],
            ),
        ];
        for (bin_idx, deltas, fees, expected) in cases {
            let rows = bin_fill(&fixtures::bin_swapped(bin_idx, deltas, fees)).expect("derives");
            assert_eq!(rows, [expected]);
        }
        for outside in [i32::from(i16::MIN) - 1, i32::from(i16::MAX) + 1] {
            refused(
                bin_fill(&fixtures::bin_swapped(bin(outside), U256::ZERO, U256::ZERO)),
                0,
                "binIdx",
                "bin indexes in the int16 range",
            );
        }
    }

    /// A bin's liquidity row per entry of the parallel arrays: an add's shares positive,
    /// a remove's negative, deltas signed as emitted. Arrays of different lengths, or a bin
    /// index outside the pool's int16 range, are unreadable.
    #[test]
    fn liquidity_derives_a_row_per_bin() {
        let text = |text: &str| TypedValue::String {
            value: text.as_bytes().to_vec().into(),
            text: Some(text.to_owned()),
        };
        let owner = TypedValue::Address {
            value: Address::repeat_byte(0x0e),
        };
        let salt = uint(U256::from(7), 80);
        let rows = liquidity(&fixtures::liquidity(
            false,
            &[(-2, 5, 0), (0, 3, 4)].map(|(bin, d0, d1)| (bin, d0, d1, 10)),
        ))
        .expect("derives");
        assert_eq!(
            rows,
            [
                vec![
                    text("add"),
                    owner.clone(),
                    salt.clone(),
                    int(-2, 16),
                    int(5, 256),
                    int(0, 256),
                    int(10, 256)
                ],
                vec![
                    text("add"),
                    owner.clone(),
                    salt.clone(),
                    int(0, 16),
                    int(3, 256),
                    int(4, 256),
                    int(10, 256)
                ],
            ]
        );
        let rows = liquidity(&fixtures::liquidity(true, &[(1, -5, -6, 10)])).expect("derives");
        assert_eq!(
            rows,
            [vec![
                text("remove"),
                owner,
                salt,
                int(1, 16),
                int(-5, 256),
                int(-6, 256),
                int(-10, 256)
            ]]
        );

        let mut uneven = fixtures::liquidity(false, &[(0, 1, 1, 1), (1, 1, 1, 1)]);
        let TypedValue::Array { value: shares } = &mut uneven.body[2].value else {
            panic!("shares");
        };
        shares.pop();
        refused(
            liquidity(&uneven),
            4,
            "shares",
            "an array with one entry per bin",
        );
        refused(
            liquidity(&fixtures::liquidity(
                false,
                &[(i32::from(i16::MAX) + 1, 1, 1, 1)],
            )),
            2,
            "binIdxs",
            "bin indexes in the int16 range",
        );
    }

    /// A ladder's bins, lowest first, with edges running out from bin 0's lower edge: the
    /// first zero length ends a side, dropping the rest of its word and any later words,
    /// and a side may span words or be empty.
    #[test]
    fn a_pool_ladder_derives_a_row_per_bin_with_its_edges() {
        use super::fixtures::ladder;

        let mut above = ladder(&[(100, 1, 2), (200, 0, 0), (0, 9, 9), (900, 9, 9)]);
        above.push(ladder(&[(900, 9, 9)])[0]);
        let below = ladder(&[
            (10, 3, 4),
            (20, 0, 0),
            (30, 0, 0),
            (40, 0, 0),
            (50, 0, 0),
            (60, 5, 6),
        ]);
        let pool = TypedValue::Address {
            value: Address::repeat_byte(0xd0),
        };
        let bin = |index: i128, length: i128, buy: i128, sell: i128, lower: i128, upper: i128| {
            vec![
                pool.clone(),
                int(index, 16),
                int(length, 32),
                int(buy, 32),
                int(sell, 32),
                int(lower, 64),
                int(upper, 64),
            ]
        };
        let rows = pool_bins(&fixtures::pool_created(-50, above, below)).expect("derives");
        assert_eq!(
            rows,
            [
                bin(-6, 60, 5, 6, -260, -200),
                bin(-5, 50, 0, 0, -200, -150),
                bin(-4, 40, 0, 0, -150, -110),
                bin(-3, 30, 0, 0, -110, -80),
                bin(-2, 20, 0, 0, -80, -60),
                bin(-1, 10, 3, 4, -60, -50),
                bin(0, 100, 1, 2, -50, 50),
                bin(1, 200, 0, 0, 50, 250),
            ]
        );
        let rows = pool_bins(&fixtures::pool_created(0, ladder(&[(7, 0, 0)]), Vec::new()))
            .expect("derives");
        assert_eq!(rows, [bin(0, 7, 0, 0, 0, 7)]);
    }

    /// Pool events decoded against the shipped ABI have the argument names and positions
    /// the derivations read, and every row fits its table: an ABI edit that renames or
    /// reorders an event's arguments fails here.
    #[test]
    fn pool_events_decoded_against_the_shipped_abi_fill_their_tables() {
        let catalog = catalog();
        let chain = ChainId::new("base");
        let cases = [
            (
                SWAPS,
                fixtures::swap(
                    details(false, -3, U256::from(42)),
                    int128s(-1_500_000, 2),
                    uint128s(5, 6),
                ),
                1,
            ),
            (
                BIN_FILLS,
                fixtures::bin_swapped(I256::MINUS_ONE, int128s(-4, 9), uint128s(1, 2)),
                1,
            ),
            (
                LIQUIDITY,
                fixtures::liquidity(false, &[(-1, 1, 2, 3), (0, 4, 5, 6)]),
                2,
            ),
            (LIQUIDITY, fixtures::liquidity(true, &[(0, -4, -5, 6)]), 1),
            (
                POOL_BINS,
                fixtures::pool_created(
                    0,
                    fixtures::ladder(&[(10, 0, 0)]),
                    fixtures::ladder(&[(10, 0, 0)]),
                ),
                2,
            ),
        ];
        for (derivation, decoded, expected) in cases {
            let sent = Some((Address::repeat_byte(0xf0), Some(Address::repeat_byte(0xf1))));
            let rows = derivation.rows(&decoded, sent).expect("derives");
            assert_eq!(rows.len(), expected, "{}", derivation.table);
            for (row, number) in rows.iter().zip(0..) {
                assert_eq!(row.row, number);
                catalog.schema().derived_row(&chain, row).expect("a row");
            }
        }
    }
}
