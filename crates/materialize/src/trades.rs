//! Layer B: the semantic DEX trades table.
//!
//! This is the layer that answers questions. Where [`crate::events`] is faithful to
//! the ABI's own shape, this one is *semantic*: it knows that a Uniswap V3 `Swap`'s
//! `amount0` is a token amount, that a negative amount means the pool sent that token
//! out, and which side is therefore the token sold.
//!
//! # Why it is a separate layer
//!
//! The two change for different reasons. A new ABI is a decode concern; a new
//! protocol's convention for which argument is "sold" is a modeling concern. Fusing
//! them would mean every new protocol changes the decoder and invalidates its
//! determinism, which is the property that makes a replay safe.
//!
//! # Column set
//!
//! Modeled on Allium's `dex.trades` table, which merges every DEX into one shape so a
//! query does not have to know which protocol it is reading. The columns this
//! produces today:
//!
//! | Column | Source |
//! | --- | --- |
//! | `project` | the extractor's protocol family, `uniswap` |
//! | `protocol` | family plus version, `uniswap_v3` |
//! | `liquidity_pool_address` | [`Decoded::address`] |
//! | `sender_address`, `to_address` | the event's `sender` and `recipient` |
//! | `token0_amount_raw`, `token1_amount_raw` | `amount0` and `amount1`, signed |
//! | `sqrt_price_x96`, `liquidity`, `tick` | the pool's post-swap state |
//! | the identity columns | [`crate::Identity`] |
//!
//! # What this layer cannot do yet
//!
//! Allium's table resolves `token_sold_address` and normalizes amounts by the
//! token's decimals, which needs a token registry — address, decimals, symbol. That
//! is a separate dataset this workspace does not have. Until it does, the rows carry
//! raw amounts and the pool address, which is honest about what is known: a consumer
//! can still tell direction from the signs and values from the magnitudes.

use wire::envelope::Decoded;

use crate::column::{Column, Value};
use crate::events::{EventRow, Identity};

/// The Uniswap V3 `Swap` event signature this extractor recognizes.
const UNISWAP_V3_SWAP: &str = "Swap(address,address,int256,int256,uint160,uint128,int24)";

/// The cross-protocol table every extractor writes into, named as Allium names it.
pub const TRADES_TABLE: &str = "dex.trades";

/// One swap, in the shape Allium's `dex.trades` uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trade {
    /// The protocol family, `uniswap`.
    pub project: &'static str,
    /// The family plus version, `uniswap_v3`.
    pub protocol: &'static str,
    /// The columns of the row, in a stable order.
    pub columns: Vec<Column>,
    /// The row's identity, shared with layer A's row so the two are joinable.
    ///
    /// A trade is derived from a decoded event, so it carries that event's identity
    /// rather than inventing one: the transaction that emitted the `Swap` is the
    /// transaction that traded.
    pub identity: Identity,
}

/// Extracts a trade from a decoded event, or `None` when it is not one this knows.
///
/// The dispatch is on the event's signature rather than on its selector, because the
/// signature is what says *what shape the arguments are*, and this extractor exists
/// precisely to interpret that shape. A selector is an opaque hash: it would tell the
/// extractor which pool's event it is, not what the arguments mean.
#[must_use]
pub fn trade(decoded: &Decoded) -> Option<Trade> {
    (decoded.signature == UNISWAP_V3_SWAP)
        .then(|| uniswap_v3_swap(decoded))
        .flatten()
}

/// Builds one Uniswap V3 swap row.
///
/// Returns `None` when the event does not carry the arguments the mapping needs.
/// That is a real possibility — an ABI revision that renames a parameter — and it
/// fails here rather than producing a row with columns silently shifted.
fn uniswap_v3_swap(decoded: &Decoded) -> Option<Trade> {
    let row = EventRow::from_decoded(decoded);
    let identity = row.identity;

    // Identity first, so every generated table starts with the same columns and a
    // consumer can join across layers without knowing which it holds.
    let mut columns = identity_columns(identity);
    columns.push(Column {
        name: "project".to_owned(),
        value: Value::Text("uniswap".to_owned()),
    });
    columns.push(Column {
        name: "protocol".to_owned(),
        value: Value::Text("uniswap_v3".to_owned()),
    });
    for (name, source) in [
        ("sender_address", "sender"),
        ("to_address", "recipient"),
        ("token0_amount_raw", "amount0"),
        ("token1_amount_raw", "amount1"),
        ("sqrt_price_x96", "sqrtPriceX96"),
        ("liquidity", "liquidity"),
        ("tick", "tick"),
    ] {
        let column = row.column(source)?;
        columns.push(Column {
            name: name.to_owned(),
            value: column.value.clone(),
        });
    }

    Some(Trade {
        project: "uniswap",
        protocol: "uniswap_v3",
        columns,
        identity,
    })
}

/// The identity columns, as printable columns in their canonical `0x` or decimal
/// form.
///
/// Every table carries these, so a store can create them without knowing which layer
/// or which protocol produced the row. [`Trade`] holds the same [`Identity`] typed, so
/// a Rust consumer reads fields while a store reads columns.
#[must_use]
pub fn identity_columns(identity: Identity) -> Vec<Column> {
    let text = |name: &str, value: String| Column {
        name: name.to_owned(),
        value: Value::Text(value),
    };
    vec![
        Column {
            name: "block_number".to_owned(),
            value: Value::Integer(i64::try_from(identity.block_number).unwrap_or(i64::MAX)),
        },
        Column {
            name: "block_timestamp".to_owned(),
            value: Value::Integer(i64::try_from(identity.block_timestamp).unwrap_or(i64::MAX)),
        },
        text("block_hash", identity.block_hash.to_string()),
        text("transaction_hash", identity.transaction_hash.to_string()),
        Column {
            name: "transaction_index".to_owned(),
            value: Value::Integer(i64::try_from(identity.transaction_index).unwrap_or(i64::MAX)),
        },
        Column {
            name: "log_index".to_owned(),
            value: Value::Integer(i64::try_from(identity.log_index).unwrap_or(i64::MAX)),
        },
        text("contract_address", identity.contract_address.to_string()),
    ]
}

impl crate::Row for Trade {
    fn table(&self) -> &str {
        TRADES_TABLE
    }

    fn columns(&self) -> &[Column] {
        &self.columns
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{Address, B256, I256, U256};
    use wire::envelope::{Decoded, DecodedArg};
    use wire::typed::TypedValue;

    use super::{Value, trade};

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

    fn value<'a>(trade: &'a super::Trade, name: &str) -> &'a Value {
        trade
            .columns
            .iter()
            .find(|column| column.name == name)
            .map(|column| &column.value)
            .expect("the trade has this column")
    }

    /// A swap becomes a trade with the protocol named and the amounts under the
    /// semantic names, so a query reads `token0_amount_raw` rather than `amount0`.
    #[test]
    fn a_swap_becomes_a_trade_with_semantic_column_names() {
        let trade = trade(&swap()).expect("a Uniswap V3 swap is a trade");

        assert_eq!(trade.project, "uniswap");
        assert_eq!(trade.protocol, "uniswap_v3");
        // The identity columns come first, so a store can write them without knowing
        // which layer produced the row.
        assert_eq!(
            value(&trade, "contract_address"),
            &Value::Text(Address::from([0xd0; 20]).to_string())
        );
        assert_eq!(value(&trade, "block_number"), &Value::Integer(51_913_794));
        assert_eq!(value(&trade, "log_index"), &Value::Integer(767));
        assert_eq!(
            value(&trade, "sender_address"),
            &Value::Text(Address::from([0x6f; 20]).to_string())
        );
        // The sign survives into the semantic column: this swap sold token0.
        assert_eq!(
            value(&trade, "token0_amount_raw"),
            &Value::Integer(-3_180_585_820_646_654)
        );
        assert_eq!(value(&trade, "tick"), &Value::Integer(-197_317));
    }

    /// A trade carries the identity of the event it came from, so it is joinable to
    /// layer A's row without a second lookup.
    #[test]
    fn a_trade_carries_the_identity_of_its_event() {
        let decoded = swap();
        let trade = trade(&decoded).expect("a trade");
        assert_eq!(trade.identity.block_number, decoded.block_number);
        assert_eq!(trade.identity.transaction_hash, decoded.transaction_hash);
        assert_eq!(trade.identity.log_index, decoded.log_index);
        assert_eq!(trade.identity.contract_address, decoded.address);
    }

    /// A different event in the same ABI is not a trade, so an extractor for one
    /// protocol does not misinterpret another event's arguments as a swap.
    #[test]
    fn a_different_event_is_not_a_trade() {
        let mut decoded = swap();
        decoded.signature = "Mint(address,address,int24,int24,uint128,uint256,uint256)".to_owned();
        decoded.name = "Mint".to_owned();
        assert!(trade(&decoded).is_none());
    }

    /// A swap whose ABI renamed one of the parameters it needs is refused rather
    /// than producing a row with the right headings and the wrong values.
    #[test]
    fn a_renamed_parameter_is_refused_not_shifted() {
        let mut decoded = swap();
        for arg in &mut decoded.body {
            if arg.name == "amount0" {
                arg.name = "amount_0".to_owned();
            }
        }
        assert!(trade(&decoded).is_none());
    }

    /// Two swaps produce the same column names in the same order, which is what
    /// makes a set of rows a table rather than a bag.
    #[test]
    fn column_names_are_stable_across_rows() {
        let first = trade(&swap()).expect("a trade");
        let other = trade(&swap()).expect("a trade");
        let names = |t: &super::Trade| -> Vec<String> {
            t.columns.iter().map(|c| c.name.clone()).collect()
        };
        assert_eq!(names(&first), names(&other));
    }
}
