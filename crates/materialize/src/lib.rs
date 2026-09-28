//! Turns decoded events into typed, queryable tables.
//!
//! The decode stage publishes one generic record per log: an event name, and its
//! arguments under their ABI names. That is deliberately not a table. This crate is
//! what makes it one, and it does that in two layers because they answer different
//! questions.
//!
//! # Two layers
//!
//! - **Layer A** ([`events`]) is faithful. One row per decoded event, every argument
//!   a column under its ABI name. It works for any event in any ABI without a
//!   per-protocol extractor, which is why it is the foundation rather than the
//!   refinement.
//! - **Layer B** ([`trades`]) is semantic. It knows what a Uniswap V3 `Swap`'s
//!   arguments *mean* and produces the cross-protocol shape Allium calls
//!   `dex.trades`, so a query does not have to know which protocol it is reading.
//!
//! Both are pure functions of a decoded record. Nothing here reads a clock, a
//! database, or the network, so a table can be rebuilt by replaying the decoded
//! stream — which is the point of keeping the decode stage stateless.
//!
//! # Where the values go
//!
//! [`column`](mod@column) is the narrowing step, and it is where the ABI's width meets a
//! database's. An ABI integer can be 256 bits; no common integer column holds that,
//! so a value that fits an `i64` becomes one and anything wider becomes exact decimal
//! text. Rounding a wei amount into a float would corrupt it silently.

pub mod column;
pub mod events;
pub mod trades;

pub use column::{Column, Value};
pub use events::{EventRow, Identity};
pub use trades::Trade;

use wire::envelope::Decoded;

/// Every table a decoded event produces.
///
/// One decoded event yields its faithful row, and its semantic row when an extractor
/// recognizes it. Returning both rather than choosing means a store can materialize
/// the layer it wants without this crate guessing, and a new extractor adds a row
/// rather than replacing one.
///
/// Layer A's row is always present. A `Decoded` record that produces nothing would be
/// a decode that ran for no reason.
#[must_use]
pub fn tables(decoded: &Decoded) -> Tables {
    Tables {
        event: EventRow::from_decoded(decoded),
        trade: trades::trade(decoded),
    }
}

/// The rows one decoded event produces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tables {
    /// Layer A: the faithful per-event row, always present.
    pub event: EventRow,
    /// Layer B: the semantic trade row, when an extractor recognized the event.
    pub trade: Option<Trade>,
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

    use super::tables;

    fn swap() -> Decoded {
        Decoded {
            name: "Swap".to_owned(),
            address: Address::from([0xd0; 20]),
            selector: B256::from([0xc4; 32]),
            signature: "Swap(address,address,int256,int256,uint160,uint128,int24)".to_owned(),
            source: format!("51913794:{}:767", B256::from([0x2a; 32])),
            indexed: vec![
                DecodedArg {
                    name: "sender".to_owned(),
                    value: TypedValue::Address {
                        value: Address::from([0x6f; 20]),
                    },
                },
                DecodedArg {
                    name: "recipient".to_owned(),
                    value: TypedValue::Address {
                        value: Address::from([0x6f; 20]),
                    },
                },
            ],
            body: vec![
                DecodedArg {
                    name: "amount0".to_owned(),
                    value: TypedValue::Int {
                        value: I256::try_from(-1_i64).expect("fits"),
                        bits: 256,
                    },
                },
                DecodedArg {
                    name: "amount1".to_owned(),
                    value: TypedValue::Int {
                        value: I256::from_raw(U256::from(1)),
                        bits: 256,
                    },
                },
                DecodedArg {
                    name: "sqrtPriceX96".to_owned(),
                    value: TypedValue::Uint {
                        value: U256::from(1),
                        bits: 160,
                    },
                },
                DecodedArg {
                    name: "liquidity".to_owned(),
                    value: TypedValue::Uint {
                        value: U256::from(1),
                        bits: 128,
                    },
                },
                DecodedArg {
                    name: "tick".to_owned(),
                    value: TypedValue::Int {
                        value: I256::from_raw(U256::from(1)),
                        bits: 24,
                    },
                },
            ],
            block_number: 51_913_794,
            block_hash: B256::from([0xd4; 32]),
            block_timestamp: 1_700_000_000,
        }
    }

    /// A recognized event produces both layers: the faithful row is never replaced
    /// by the semantic one.
    #[test]
    fn a_recognized_event_produces_both_layers() {
        let produced = tables(&swap());
        assert_eq!(produced.event.table, "Swap");
        assert!(produced.trade.is_some());
    }

    /// An event no extractor knows still produces its faithful row, so layer A
    /// covers the long tail rather than only the protocols that have been modeled.
    #[test]
    fn an_unrecognized_event_still_produces_its_row() {
        let mut decoded = swap();
        decoded.name = "SomethingElse".to_owned();
        decoded.signature = "SomethingElse(uint256)".to_owned();

        let produced = tables(&decoded);
        assert_eq!(produced.event.table, "SomethingElse");
        assert!(produced.trade.is_none());
    }
}
