//! `metric_v1_bin_fills`: one row per `BinSwapped`, one bin a swap filled, emitted
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

use crate::sink::table::ColumnType;
use crate::wire::datasets::evm::Decoded;

use super::super::value::{bits, int, signed, signed_bits, uint, unsigned};
use super::super::{Derivation, DeriveError, Row};
use super::bin_index;

/// `metric_v1_bin_fills`: one row per bin a swap filled.
pub(in crate::decode) const BIN_FILLS: Derivation = Derivation {
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
    // A pool's fills, by block.
    indexes: &[&["chain", "address", "block_number"]],
    derive: bin_fill,
};

/// A `BinSwapped`'s row.
fn bin_fill(decoded: &Decoded, _: usize) -> Result<Vec<Row>, DeriveError> {
    let bin = bin_index(int(decoded, 0, "binIdx", 256)?, 0, "binIdx")?;
    let deltas = uint(decoded, 1, "binDeltas", 256)?;
    let fees = uint(decoded, 2, "fees", 256)?;
    Ok(vec![vec![
        ("bin_idx", bin),
        ("delta0_scaled", signed(signed_bits(deltas, 128, 128), 128)),
        ("delta1_scaled", signed(signed_bits(deltas, 0, 128), 128)),
        ("lp_fee_scaled", unsigned(bits(fees, 128, 128), 128)),
        ("platform_fee_scaled", unsigned(bits(fees, 0, 128), 128)),
    ]])
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{I256, U256};

    use crate::decode::fixtures::metric::{bin_swapped, int128s, uint128s};
    use crate::decode::fixtures::{int, refused, uint, unnamed};

    use super::bin_fill;

    /// The same boundaries as a swap's: both int128 extremes in either delta, a negative
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
            let rows = unnamed(bin_fill(&bin_swapped(bin_idx, deltas, fees), 0));
            assert_eq!(rows, [expected]);
        }
        for outside in [i32::from(i16::MIN) - 1, i32::from(i16::MAX) + 1] {
            refused(
                bin_fill(&bin_swapped(bin(outside), U256::ZERO, U256::ZERO), 0),
                0,
                "binIdx",
                "bin indexes in the int16 range",
            );
        }
    }
}
