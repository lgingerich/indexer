//! `metric_v1_liquidity`: one row per bin of a `LiquidityAdded` or `LiquidityRemoved`,
//! whose parallel arrays name the bins, their scaled balance deltas, and the shares minted
//! or burned in each. A position is `(pool, owner, salt)`, with shares per bin. `shares`
//! is signed like the deltas: positive on an add, negative on a remove.

use alloy_primitives::{Bytes, I256, Sign};

use crate::sink::table::ColumnType;
use crate::wire::envelope::{Decoded, TypedValue};

use super::super::value::{address, array, element, signed, uint, unsigned};
use super::super::{Derivation, DeriveError, Row};
use super::bin_index;

/// `metric_v1_liquidity`: one row per bin of a liquidity add or remove.
pub(in crate::decode) const LIQUIDITY: Derivation = Derivation {
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
    // An account's positions: the rows it owns or sent, then every row of those
    // positions.
    indexes: &[
        &["chain", "owner"],
        &["chain", "transaction_from_address"],
        &["chain", "address", "owner"],
    ],
    derive: liquidity,
};

/// A `LiquidityAdded` or `LiquidityRemoved`'s rows, one per bin.
fn liquidity(decoded: &Decoded) -> Result<Vec<Row>, DeriveError> {
    let removed = decoded.name == "LiquidityRemoved";
    let direction = if removed { "remove" } else { "add" };
    let owner = address(decoded, 0, "provider")?;
    let salt = uint(decoded, 1, "salt", 80)?;
    let bins = array(decoded, 2, "binIdxs")?;
    let balances = array(decoded, 3, "binBalanceDeltas")?;
    let shares = array(decoded, 4, "shares")?;
    let one_per_bin = |position, name| element(position, name, "an array with one entry per bin");
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
            let sign = if removed {
                Sign::Negative
            } else {
                Sign::Positive
            };
            let shares = I256::checked_from_sign_and_abs(sign, *minted)
                .ok_or_else(|| element(4, "shares", "entries within int256"))?;
            Ok(vec![
                (
                    "direction",
                    TypedValue::String {
                        value: Bytes::from_static(direction.as_bytes()),
                        text: Some(direction.to_owned()),
                    },
                ),
                ("owner", TypedValue::Address { value: owner }),
                ("salt", unsigned(salt, 80)),
                ("bin_idx", bin),
                ("delta0_scaled", signed(delta0, 256)),
                ("delta1_scaled", signed(delta1, 256)),
                ("shares", signed(shares, 256)),
            ])
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, U256};

    use crate::decode::fixtures::metric::liquidity as event;
    use crate::decode::fixtures::{int, refused, uint, unnamed};
    use crate::wire::envelope::TypedValue;

    use super::liquidity;

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
        let rows = unnamed(liquidity(&event(
            false,
            &[(-2, 5, 0), (0, 3, 4)].map(|(bin, d0, d1)| (bin, d0, d1, 10)),
        )));
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
        let rows = unnamed(liquidity(&event(true, &[(1, -5, -6, 10)])));
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

        let mut uneven = event(false, &[(0, 1, 1, 1), (1, 1, 1, 1)]);
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
            liquidity(&event(false, &[(i32::from(i16::MAX) + 1, 1, 1, 1)])),
            2,
            "binIdxs",
            "bin indexes in the int16 range",
        );
    }
}
