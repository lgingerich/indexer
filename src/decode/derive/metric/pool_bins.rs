//! `metric_v1_pool_bins`: one row per bin of a pool's ladder, from the factory's
//! `PoolCreated`, as `_unpackAndValidateBinStates` reads it: five bins per word, 48 bits
//! each, least significant first, `lengthE6` in bits 0–15, `addFeeBuyE6` 16–31, and
//! `addFeeSellE6` 32–47. The first zero length ends a side. Entry `k` of the non-negative
//! array is bin `k`, of the negative array bin `-k-1`. Edges are distances from the
//! reference price in E6: bin 0's lower edge is `curBinDistFromProvidedPriceE6`, and each
//! bin's runs a length on from its neighbour's. The additional fees are as created;
//! `BinAdditionalFeesUpdated` changes them later.

use alloy_primitives::{I256, U256};

use crate::sink::table::ColumnType;
use crate::wire::datasets::evm::Decoded;
use crate::wire::typed::TypedValue;

use super::super::value::{address, array, bits, element, int, signed};
use super::super::{Derivation, DeriveError, Row};

/// `metric_v1_pool_bins`: one row per bin of a pool's ladder, as created.
pub(in crate::decode) const POOL_BINS: Derivation = Derivation {
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
    // A pool's deployer, read from the index alone.
    indexes: &[&["chain", "pool", "transaction_from_address"]],
    derive: pool_bins,
};

/// A `PoolCreated`'s rows, one per bin from the lowest to the highest.
fn pool_bins(decoded: &Decoded, _: usize) -> Result<Vec<Row>, DeriveError> {
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
            ("pool", TypedValue::Address { value: pool }),
            ("bin_idx", signed(index, 16)),
            ("length_e6", unsigned_small(length)),
            ("add_fee_buy_e6", unsigned_small(buy)),
            ("add_fee_sell_e6", unsigned_small(sell)),
            ("lower_e6", signed(lower, 64)),
            ("upper_e6", signed(upper, 64)),
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

/// A 16-bit unsigned field, stored in a signed 64-bit column beside the signed edges.
///
/// As a signed value it needs 17 bits, so it is labelled `int24`, the narrowest signed ABI
/// width that holds every `uint16`. The store checks the value against the column, not
/// this label.
fn unsigned_small(value: U256) -> TypedValue {
    signed(I256::from_raw(value), 24)
}

#[cfg(test)]
mod tests {
    use alloy_primitives::Address;

    use crate::decode::fixtures::metric::{ladder, pool_created};
    use crate::decode::fixtures::{int, unnamed};
    use crate::wire::typed::TypedValue;

    use super::pool_bins;

    /// A ladder's bins, lowest first, with edges running out from bin 0's lower edge: the
    /// first zero length ends a side, dropping the rest of its word and any later words,
    /// and a side may span words or be empty.
    #[test]
    fn a_pool_ladder_derives_a_row_per_bin_with_its_edges() {
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
                int(length, 24),
                int(buy, 24),
                int(sell, 24),
                int(lower, 64),
                int(upper, 64),
            ]
        };
        let rows = unnamed(pool_bins(&pool_created(-50, above, below), 0));
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
        let rows = unnamed(pool_bins(
            &pool_created(0, ladder(&[(7, 0, 0)]), Vec::new()),
            0,
        ));
        assert_eq!(rows, [bin(0, 7, 0, 0, 0, 7)]);
    }
}
