//! `metric_v1_swaps`: one row per pool `Swap`, which packs five values into three words:
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

use crate::sink::table::ColumnType;
use crate::wire::datasets::evm::Decoded;
use crate::wire::typed::TypedValue;

use super::super::value::{address, bits, signed, signed_bits, uint, unsigned};
use super::super::{Derivation, DeriveError, Row};

/// `metric_v1_swaps`: one row per pool `Swap`.
pub(in crate::decode) const SWAPS: Derivation = Derivation {
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
    // A pool's swaps, paged in log order.
    indexes: &[&["chain", "address", "block_number", "log_index"]],
    derive: swap,
};

/// A `Swap`'s row.
fn swap(decoded: &Decoded, _: usize) -> Result<Vec<Row>, DeriveError> {
    let sender = address(decoded, 0, "sender")?;
    let recipient = address(decoded, 1, "recipient")?;
    let details = uint(decoded, 2, "details", 256)?;
    let deltas = uint(decoded, 3, "amountDeltas", 256)?;
    let fees = uint(decoded, 4, "platformFees", 256)?;
    let amount0 = signed_bits(deltas, 128, 128);
    Ok(vec![vec![
        ("sender", TypedValue::Address { value: sender }),
        ("recipient", TypedValue::Address { value: recipient }),
        ("amount0_raw", signed(amount0, 128)),
        ("amount1_raw", signed(signed_bits(deltas, 0, 128), 128)),
        (
            "zero_for_one",
            TypedValue::Bool {
                value: amount0.is_positive(),
            },
        ),
        (
            "exact_input",
            TypedValue::Bool {
                value: details.bit(120),
            },
        ),
        ("bin_idx_after", signed(signed_bits(details, 104, 16), 16)),
        ("pos_in_bin_after", unsigned(bits(details, 0, 104), 104)),
        (
            "platform_spread_fee_scaled",
            unsigned(bits(fees, 128, 128), 128),
        ),
        (
            "platform_notional_fee_scaled",
            unsigned(bits(fees, 0, 128), 128),
        ),
    ]])
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, U256};

    use crate::decode::fixtures::metric::{details, int128s, swap, uint128s};
    use crate::decode::fixtures::{int, uint, unnamed};
    use crate::wire::typed::TypedValue;

    use super::SWAPS;

    fn max_pos() -> U256 {
        (U256::ONE << 104) - U256::ONE
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
            let [values] = &unnamed((SWAPS.derive)(&swap(details, amounts, fees), 0))[..] else {
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
}
