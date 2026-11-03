//! `metric_v1_oracle_prices`: one row per oracle `FeedUpdated`, the price the oracle
//! stored, read from its transaction's input.
//!
//! `FeedUpdated` carries only the feed's id: the price travels in the update's calldata.
//! Both oracles store a feed the same way, and the row is what they store:
//!
//! | Column | Value |
//! | --- | --- |
//! | `feed_id` | the feed's `bytes32` id, as `FeedUpdated` carries it |
//! | `price_e8` | the price at 8 decimals, rounded down |
//! | `spread0` | the price's uncertainty over the price, in millionths, rounded up, at most 65534; 65535 for a closed market |
//! | `feed_timestamp_ms` | when the price was observed, in milliseconds |
//!
//! A feed's id says which oracle stored it: a Pyth Lazer feed's is its `u32` id, the rest
//! of the word zero, read by [`lazer`], and a Chainlink stream's carries its report schema
//! in its first two bytes, read by [`data_streams`]. Each reads every update in the input,
//! at any offset, so one wrapped by another contract's call reads too. The oracle emits
//! `FeedUpdated` only when it stores a newer price, so of the updates holding the feed the
//! row takes the newest.

mod data_streams;
mod lazer;

use alloy_primitives::{Bytes, U256};

use super::super::value::{fixed_bytes, unsigned};
use super::super::{Derivation, DeriveError, Row};
use crate::sink::table::ColumnType;
use crate::wire::datasets::evm::Decoded;
use crate::wire::typed::TypedValue;

/// `metric_v1_oracle_prices`: one row per oracle `FeedUpdated`.
pub(in crate::decode) const ORACLE_PRICES: Derivation = Derivation {
    protocol: "metric_v1",
    contract: "OracleProvider",
    signatures: &["FeedUpdated(bytes32)"],
    table: "metric_v1_oracle_prices",
    columns: &[
        ("feed_id", ColumnType::Text),
        ("price_e8", ColumnType::Uint),
        ("spread0", ColumnType::Uint),
        ("feed_timestamp_ms", ColumnType::Uint),
    ],
    // A feed's prices, in block order.
    indexes: &[&["chain", "address", "feed_id", "block_number"]],
    derive: oracle_price,
};

/// The oracles' spread scale, millionths.
const SPREAD_SCALE: u64 = 1_000_000;
/// The widest spread the oracles store for an open market.
const MAX_SPREAD: u16 = 65_534;

/// What an oracle stores for one feed.
struct Stored {
    price: u64,
    spread0: u16,
    timestamp_ms: u64,
}

/// A `FeedUpdated`'s row.
fn oracle_price(
    decoded: &Decoded,
    _: usize,
    input: Option<&[u8]>,
) -> Result<Vec<Row>, DeriveError> {
    let id: [u8; 32] = fixed_bytes(decoded, 0, "feedId", 32)?
        .try_into()
        .map_err(|_| DeriveError::Argument {
            position: 0,
            name: "feedId",
            expected: "fixed bytes of its declared size",
        })?;
    let input = input.ok_or(DeriveError::Input {
        reason: "was not fetched",
    })?;
    let stored = match lazer_feed(&id) {
        Some(feed) => lazer::stored(input, feed).ok_or(DeriveError::Input {
            reason: "holds no Pyth Lazer update the oracle stores for the feed",
        })?,
        None => data_streams::stored(input, &id).ok_or(DeriveError::Input {
            reason: "holds no Chainlink Data Streams report the oracle stores for the feed",
        })?,
    };
    Ok(vec![vec![
        (
            "feed_id",
            TypedValue::FixedBytes {
                value: Bytes::copy_from_slice(&id),
                size: 32,
            },
        ),
        ("price_e8", unsigned(U256::from(stored.price), 64)),
        ("spread0", unsigned(U256::from(stored.spread0), 16)),
        (
            "feed_timestamp_ms",
            unsigned(U256::from(stored.timestamp_ms), 56),
        ),
    ]])
}

/// The `u32` a Lazer feed's `bytes32` id holds, or `None` for a wider one.
fn lazer_feed(id: &[u8; 32]) -> Option<u32> {
    let (high, low) = id.split_at(28);
    let low = u32::from_be_bytes(low.try_into().ok()?);
    high.iter().all(|&byte| byte == 0).then_some(low)
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{B256, Bytes, U256, b256, hex};

    use crate::decode::fixtures::metric::{chainlink_update, feed_updated, pyth_lazer_update};
    use crate::wire::typed::TypedValue;

    use super::super::super::DeriveError;
    use super::ORACLE_PRICES;

    /// The Chainlink stream of [`chainlink_update`], a v3 report in seconds.
    const STREAM: B256 =
        b256!("0x00036cb94fa3facfcd023e7045fb47a589ed8041fd9a81bf1aae7c8ede91cf58");

    fn row(feed: B256, input: Option<&[u8]>) -> Result<Vec<TypedValue>, DeriveError> {
        let rows = (ORACLE_PRICES.derive)(&feed_updated(feed), 0, input)?;
        assert!(rows.len() <= 1);
        Ok(rows.into_iter().flatten().map(|(_, value)| value).collect())
    }

    fn uint(value: u64, bits: u16) -> TypedValue {
        TypedValue::Uint {
            value: U256::from(value),
            bits,
        }
    }

    fn id(feed: B256) -> TypedValue {
        TypedValue::FixedBytes {
            value: Bytes::copy_from_slice(feed.as_slice()),
            size: 32,
        }
    }

    /// ETH/USD, Lazer feed 2, as `PythOracle` stored it: $2,249.17124907 at exponent −8
    /// kept at 8 decimals, its confidence over its price in millionths, rounded up, and
    /// its feed timestamp in milliseconds.
    #[test]
    fn a_lazer_feed_reads_as_the_oracle_stores_it() {
        let feed = B256::with_last_byte(2);
        // Price 0x345e1af72b, confidence 0x1233a775: 1e6 × 305375093 / 224917124907 is
        // 1357.7, rounded up.
        assert_eq!(
            row(feed, Some(&pyth_lazer_update())).expect("reads"),
            [
                id(feed),
                uint(0x34_5e1a_f72b, 64),
                uint(1358, 16),
                uint(0x0006_5f51_a8b6_b5c0 / 1000, 56),
            ]
        );
    }

    /// A Chainlink v3 report as `ChainlinkOracle` stored it: its 18-decimal price,
    /// 908.91810386, at 8 decimals; bid and ask at the price, so no spread; and its
    /// observation timestamp, in seconds, in milliseconds.
    #[test]
    fn a_chainlink_stream_reads_as_the_oracle_stores_it() {
        assert_eq!(
            row(STREAM, Some(&chainlink_update())).expect("reads"),
            [
                id(STREAM),
                uint(90_891_810_386, 64),
                uint(0, 16),
                uint(1_792_687_922_000, 56),
            ]
        );
    }

    /// An update wrapped in another contract's call, after a selector and padding, reads
    /// the same, for either oracle.
    #[test]
    fn a_wrapped_update_reads_the_same() {
        for (feed, input) in [
            (B256::with_last_byte(2), pyth_lazer_update()),
            (STREAM, chainlink_update()),
        ] {
            let mut wrapped = hex!("deadbeef").to_vec();
            wrapped.extend([0; 61]);
            wrapped.extend(&input);
            wrapped.extend([0; 7]);
            assert_eq!(
                row(feed, Some(&wrapped)).expect("reads"),
                row(feed, Some(&input)).expect("reads")
            );
        }
    }

    /// A feed without its transaction's input, one the update does not hold, the other
    /// oracle's update, or a truncated update, derives nothing and says why. A Lazer
    /// payload ends at the input's end; a Chainlink report, 9 words from its id at byte
    /// 325, ends before the signatures that follow it.
    #[test]
    fn an_update_without_the_feed_is_an_input_error() {
        for (feed, input, truncated) in [
            (B256::with_last_byte(2), pyth_lazer_update(), 526),
            (STREAM, chainlink_update(), 325 + 9 * 32 - 1),
        ] {
            let cases = [None, Some(&input[..truncated]), Some(&[][..])];
            for input in cases {
                assert!(matches!(row(feed, input), Err(DeriveError::Input { .. })));
            }
        }
        let cases = [
            (B256::with_last_byte(3), pyth_lazer_update()),
            (B256::with_last_byte(2), chainlink_update()),
            (STREAM, pyth_lazer_update()),
        ];
        for (feed, input) in cases {
            assert!(matches!(
                row(feed, Some(&input)),
                Err(DeriveError::Input { .. })
            ));
        }
    }
}
