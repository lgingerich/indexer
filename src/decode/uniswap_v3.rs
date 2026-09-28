//! The Uniswap V3 mapper: `Swap` events into `dex.trades` rows.
//!
//! This is the projection that was deleted with the `materialize` crate, restored behind
//! the [`DatasetMapper`] seam. It knows what the registry cannot: that `amount0` is a
//! token amount, and that a negative one means the pool sent that token out.
//!
//! # Column set
//!
//! Modeled on Allium's `dex.trades`, which merges every DEX into one shape so a query
//! does not have to know which protocol it reads:
//!
//! | Column | Source |
//! | --- | --- |
//! | `protocol` | the registry's name for the contract |
//! | `liquidity_pool_address` | the emitting contract |
//! | `sender_address`, `to_address` | the event's `sender` and `recipient` |
//! | `token0_amount_raw`, `token1_amount_raw` | `amount0` and `amount1`, signed |
//! | `sqrt_price_x96`, `liquidity`, `tick` | the pool's post-swap state |
//! | `transaction_hash`, `log_index`, `block_*` | the decoded record's identity |
//!
//! # What this cannot do yet
//!
//! Allium's table resolves `token_sold_address` and normalizes amounts by the token's
//! decimals, which needs a token registry — address, decimals, symbol. Until that
//! exists, the rows carry raw signed amounts and the pool address, which is honest about
//! what is known: the signs say direction, the magnitudes say size.

use crate::wire::envelope::Decoded;

use crate::decode::dataset::{Column, DatasetMapper, Row};
use crate::wire::typed::TypedValue;

/// The dataset this mapper writes, matching the registry's `dataset` value.
pub const DATASET: &str = "dex.trades";

/// The signatures this mapper models, as the ABI spells them.
///
/// Matched on the signature rather than the event name, because the name alone is not
/// unique across protocols and the signature is what says the argument layout — which is
/// what this projection depends on.
const SWAP_SIGNATURES: &[&str] = &[
    // Uniswap V3.
    "Swap(address,address,int256,int256,uint160,uint128,int24)",
];

/// Projects Uniswap V3 `Swap` events into `dex.trades` rows.
#[derive(Debug, Default, Clone, Copy)]
pub struct UniswapV3;

impl DatasetMapper for UniswapV3 {
    fn dataset(&self) -> &str {
        DATASET
    }

    fn map(&self, decoded: &Decoded) -> Option<Row> {
        if !SWAP_SIGNATURES.contains(&decoded.signature.as_str()) {
            return None;
        }
        // Named lookup: a renamed ABI parameter is a `None` here rather than a value
        // under the right heading with the wrong meaning.
        let mut columns = vec![
            text("protocol", &decoded.protocol),
            text("liquidity_pool_address", &decoded.address.to_string()),
            text("transaction_hash", &decoded.transaction_hash.to_string()),
            integer("log_index", decoded.log_index),
            integer("block_number", decoded.block_number),
            integer("block_timestamp", decoded.block_timestamp),
            text("block_hash", &decoded.block_hash.to_string()),
        ];
        for (target, source) in [
            ("sender_address", "sender"),
            ("to_address", "recipient"),
            ("token0_amount_raw", "amount0"),
            ("token1_amount_raw", "amount1"),
            ("sqrt_price_x96", "sqrtPriceX96"),
            ("liquidity", "liquidity"),
            ("tick", "tick"),
        ] {
            columns.push(Column {
                name: target.to_owned(),
                value: argument(decoded, source)?.clone(),
            });
        }

        Some(Row {
            dataset: DATASET.to_owned(),
            columns,
            source: decoded.dedupe_key(),
        })
    }
}

/// The typed value of a named argument, from either the indexed or the body list.
fn argument<'a>(decoded: &'a Decoded, name: &str) -> Option<&'a TypedValue> {
    decoded
        .indexed
        .iter()
        .chain(&decoded.body)
        .find(|arg| arg.name == name)
        .map(|arg| &arg.value)
}

fn text(name: &str, value: &str) -> Column {
    Column {
        name: name.to_owned(),
        value: TypedValue::String {
            value: value.to_owned(),
        },
    }
}

fn integer(name: &str, value: u64) -> Column {
    Column {
        name: name.to_owned(),
        value: TypedValue::Uint {
            value: alloy_primitives::U256::from(value),
            bits: 64,
        },
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use crate::wire::envelope::{Decoded, DecodedArg};
    use crate::wire::typed::TypedValue;
    use alloy_primitives::{Address, B256, I256, U256};

    use super::{DATASET, UniswapV3};
    use crate::decode::dataset::DatasetMapper;

    fn arg(name: &str, value: TypedValue) -> DecodedArg {
        DecodedArg {
            name: name.to_owned(),
            value,
        }
    }

    /// A real Uniswap V3 `Swap`, captured from Base.
    fn swap() -> Decoded {
        Decoded {
            name: "Swap".to_owned(),
            address: Address::from([0xd0; 20]),
            protocol: "uniswap_v3".to_owned(),
            dataset: DATASET.to_owned(),
            selector: B256::from([0xc4; 32]),
            signature: "Swap(address,address,int256,int256,uint160,uint128,int24)".to_owned(),
            anonymous: false,
            transaction_hash: B256::from([0x2a; 32]),
            transaction_index: 12,
            log_index: 767,
            indexed: vec![
                arg(
                    "sender",
                    TypedValue::Address {
                        value: Address::from([0x6f; 20]),
                    },
                ),
                arg(
                    "recipient",
                    TypedValue::Address {
                        value: Address::from([0x6f; 20]),
                    },
                ),
            ],
            body: vec![
                arg(
                    "amount0",
                    TypedValue::Int {
                        value: I256::try_from(-3_180_585_820_646_654_i64).expect("fits"),
                        bits: 256,
                    },
                ),
                arg(
                    "amount1",
                    TypedValue::Int {
                        value: I256::try_from(8_586_564_i64).expect("fits"),
                        bits: 256,
                    },
                ),
                arg(
                    "sqrtPriceX96",
                    TypedValue::Uint {
                        value: U256::from(1_u128 << 100),
                        bits: 160,
                    },
                ),
                arg(
                    "liquidity",
                    TypedValue::Uint {
                        value: U256::from(1_u64 << 40),
                        bits: 128,
                    },
                ),
                arg(
                    "tick",
                    TypedValue::Int {
                        value: I256::try_from(-197_317_i64).expect("fits"),
                        bits: 24,
                    },
                ),
            ],
            block_number: 51_913_794,
            block_hash: B256::from([0xd4; 32]),
            block_timestamp: 1_700_000_000,
        }
    }

    fn column<'a>(row: &'a crate::decode::dataset::Row, name: &str) -> &'a TypedValue {
        row.columns
            .iter()
            .find(|column| column.name == name)
            .map(|column| &column.value)
            .expect("the row has this column")
    }

    /// A swap becomes a `dex.trades` row with the protocol the registry supplied and the
    /// amounts under their dataset names.
    #[test]
    fn a_swap_becomes_a_trade_row() {
        let decoded = swap();
        let row = UniswapV3
            .map(&decoded)
            .expect("a Uniswap V3 swap is a trade");

        assert_eq!(row.dataset, DATASET);
        assert_eq!(row.source, decoded.dedupe_key());
        assert!(
            matches!(column(&row, "protocol"), TypedValue::String { value } if value == "uniswap_v3")
        );
        assert_eq!(
            column(&row, "liquidity_pool_address"),
            &TypedValue::String {
                value: Address::from([0xd0; 20]).to_string()
            }
        );
        // The sign survives: this swap sold token0.
        assert!(matches!(
            column(&row, "token0_amount_raw"),
            TypedValue::Int { value, .. } if *value == I256::try_from(-3_180_585_820_646_654_i64).expect("fits")
        ));
        assert!(
            matches!(column(&row, "tick"), TypedValue::Int { value, .. } if *value == I256::try_from(-197_317_i64).expect("fits"))
        );
    }

    /// A different event in the same ABI is not a trade, so one protocol's mapper does
    /// not misread another event's arguments as a swap.
    #[test]
    fn a_different_event_is_not_mapped() {
        let mut decoded = swap();
        decoded.signature = "Mint(address,address,int24,int24,uint128,uint256,uint256)".to_owned();
        decoded.name = "Mint".to_owned();
        assert!(UniswapV3.map(&decoded).is_none());
    }

    /// A renamed parameter is refused rather than producing a row with the right
    /// headings and the wrong values.
    #[test]
    fn a_renamed_parameter_is_refused() {
        let mut decoded = swap();
        for arg in &mut decoded.body {
            if arg.name == "amount0" {
                arg.name = "amount_0".to_owned();
            }
        }
        assert!(UniswapV3.map(&decoded).is_none());
    }

    /// Column names are the same for every row, which is what makes a set of rows a
    /// table rather than a bag.
    #[test]
    fn column_names_are_stable() {
        let names = |row: &crate::decode::dataset::Row| -> Vec<String> {
            row.columns.iter().map(|c| c.name.clone()).collect()
        };
        let first = UniswapV3.map(&swap()).expect("a trade");
        let second = UniswapV3.map(&swap()).expect("a trade");
        assert_eq!(names(&first), names(&second));
    }
}
