//! Metric OMM v1: pool events with their packed words unpacked, as the pool's
//! `types/Packed.sol` lays them out, one file per table.
//!
//! A bin index is an `int256` in the events and an `int16` in the pool, so a value outside
//! that range is unreadable.

mod bin_fills;
mod liquidity;
mod pool_bins;
mod swaps;

use alloy_primitives::I256;

use crate::wire::envelope::TypedValue;

use super::DeriveError;
use super::value::{element, signed};

pub(in crate::decode) use bin_fills::BIN_FILLS;
pub(in crate::decode) use liquidity::LIQUIDITY;
pub(in crate::decode) use pool_bins::POOL_BINS;
pub(in crate::decode) use swaps::SWAPS;

/// A bin index as the pool keeps it, an `int16`, from the `int256` an event carries.
fn bin_index(value: I256, position: usize, name: &'static str) -> Result<TypedValue, DeriveError> {
    i16::try_from(value)
        .map(|_| signed(value, 16))
        .map_err(|_| element(position, name, "bin indexes in the int16 range"))
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{Address, I256, TxHash, U256};

    use crate::decode::fixtures::metric::{
        bin_swapped, details, int128s, ladder, liquidity, pool_created, swap, uint128s,
    };
    use crate::decode::fixtures::shipped;
    use crate::wire::envelope::{ChainId, TypedValue};

    use super::super::Sent;
    use super::{BIN_FILLS, LIQUIDITY, POOL_BINS, SWAPS};

    /// Pool events decoded against the shipped ABI have the argument names and positions
    /// the derivations read, and every row names its table's columns and fits them: an
    /// ABI edit that renames or reorders an event's arguments fails here.
    #[test]
    fn pool_events_decoded_against_the_shipped_abi_fill_their_tables() {
        let catalog = shipped("robinhood");
        let chain = ChainId::new("robinhood");
        let cases = [
            (
                SWAPS,
                swap(
                    details(false, -3, U256::from(42)),
                    int128s(-1_500_000, 2),
                    uint128s(5, 6),
                ),
                1,
            ),
            (
                BIN_FILLS,
                bin_swapped(I256::MINUS_ONE, int128s(-4, 9), uint128s(1, 2)),
                1,
            ),
            (
                LIQUIDITY,
                liquidity(false, &[(-1, 1, 2, 3), (0, 4, 5, 6)]),
                2,
            ),
            (LIQUIDITY, liquidity(true, &[(0, -4, -5, 6)]), 1),
            (
                POOL_BINS,
                pool_created(0, ladder(&[(10, 0, 0)]), ladder(&[(10, 0, 0)])),
                2,
            ),
        ];
        for (derivation, decoded, expected) in cases {
            let sent = Some(Sent {
                hash: TxHash::ZERO,
                from: Address::repeat_byte(0xf0),
                to: Some(Address::repeat_byte(0xf1)),
            });
            // Through the catalog's bindings, as the sink runs them.
            let entry = catalog
                .entry(derivation.protocol, derivation.contract)
                .expect("the contract");
            let event = (decoded.selector, decoded.indexed.len() + 1);
            let rows = catalog
                .derivations
                .rows(&chain, (entry, event), &decoded, sent)
                .expect("derives");
            assert_eq!(rows.len(), expected, "{}", derivation.table);
            for (row, number) in rows.iter().zip(0..) {
                assert_eq!((row.table.as_str(), row.row), (derivation.table, number));
                catalog.schema().derived_row(&chain, row).expect("a row");
            }
        }
    }

    /// A liquidity row's direction follows the event its record is bound as, not the
    /// record's name: `LiquidityRemoved` removes, `LiquidityAdded` adds.
    #[test]
    fn liquidity_direction_follows_the_bound_event() {
        let catalog = shipped("robinhood");
        let pool = catalog
            .entry("metric_v1", "MetricOmmPool")
            .expect("the pool");
        for (removed, direction) in [(false, "add"), (true, "remove")] {
            let mut decoded = liquidity(removed, &[(0, 1, 1, 1)]);
            let event = (decoded.selector, 3);
            decoded.name = "renamed".to_owned();
            let rows = catalog
                .derivations
                .rows(&ChainId::new("robinhood"), (pool, event), &decoded, None)
                .expect("derives");
            let [row] = &rows[..] else {
                panic!("one row: {rows:?}");
            };
            assert_eq!(
                row.fields[0].value,
                TypedValue::String {
                    value: direction.as_bytes().to_vec().into(),
                    text: Some(direction.to_owned()),
                }
            );
        }
    }
}
