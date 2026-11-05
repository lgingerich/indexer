//! `metric_v1_compressed_oracle_prices`: one row per compressed oracle `FeedUpdated`, the
//! price the oracle stored, read from its transaction's input.
//!
//! The compressed oracle keeps four feeds to a 32-byte storage slot, and a market maker
//! pushes whole slots: calldata of a zero byte and then slot words, or one word through
//! `updateBySignature`. A word is, most significant first, four 6-byte feeds, a 7-byte
//! timestamp in milliseconds, and the slot's index. A feed is a 4-byte price, a pseudo-float
//! of a 27-bit mantissa shifted left by a 5-bit exponent at 8 decimals, and two 1-byte
//! spread indexes into the oracle's codebook. A feed's id holds its slot and position in
//! its last two bytes, and the oracle emits `FeedUpdated` for every live feed of a word it
//! accepts, newer than the slot held:
//!
//! | Column | From |
//! | --- | --- |
//! | `feed_id` | the feed's `bytes32` id, as `FeedUpdated` carries it |
//! | `price_e8` | its price, at 8 decimals |
//! | `spread0`, `spread1` | its two spreads, in millionths of the price |
//! | `feed_timestamp_ms` | the word's timestamp |
//!
//! Of the words in the input for the feed's slot, the row takes the newest: the oracle
//! writes them in order, and only a newer one.

use alloy_primitives::{Bytes, U256};

use super::super::value::{element, fixed_bytes, unsigned};
use super::super::{Derivation, DeriveError, Row};
use crate::sink::table::ColumnType;
use crate::wire::datasets::evm::Decoded;
use crate::wire::typed::TypedValue;

/// `metric_v1_compressed_oracle_prices`: one row per compressed oracle `FeedUpdated`.
pub(in crate::decode) const COMPRESSED_ORACLE_PRICES: Derivation = Derivation {
    protocol: "metric_v1",
    contract: "CompressedOracle",
    signatures: &["FeedUpdated(bytes32)"],
    table: "metric_v1_compressed_oracle_prices",
    columns: &[
        ("feed_id", ColumnType::Text),
        ("price_e8", ColumnType::Uint),
        ("spread0", ColumnType::Uint),
        ("spread1", ColumnType::Uint),
        ("feed_timestamp_ms", ColumnType::Uint),
    ],
    // A feed's prices in block order, and in time order: metric-tools reads a feed's price as of
    // a moment, and thins them by time.
    indexes: &[
        &["chain", "address", "feed_id", "block_number"],
        &["chain", "address", "feed_id", "block_timestamp"],
    ],
    derive: compressed_price,
};

/// `updateBySignature(address,uint256,bytes)`'s selector.
const UPDATE_BY_SIGNATURE: [u8; 4] = [0x73, 0x32, 0x11, 0x2d];
/// The feeds one slot holds.
const PER_SLOT: usize = 4;
/// `Codebook256`'s table: a spread index's value, in millionths of the price.
const CODEBOOK: [u16; 256] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42,
    44, 46, 48, 50, 52, 54, 56, 58, 60, 62, 64, 66, 68, 70, 72, 74, 76, 78, 80, 82, 84, 86, 88, 90,
    92, 94, 96, 98, 100, 102, 104, 106, 108, 110, 112, 114, 116, 118, 123, 128, 133, 138, 143, 148,
    153, 158, 163, 168, 173, 178, 183, 188, 193, 198, 203, 208, 213, 218, 223, 228, 233, 238, 243,
    248, 253, 258, 263, 268, 273, 278, 283, 288, 293, 298, 303, 308, 313, 318, 323, 328, 333, 338,
    343, 348, 353, 358, 363, 368, 373, 378, 383, 388, 393, 398, 403, 408, 413, 418, 423, 428, 433,
    438, 448, 458, 468, 478, 488, 498, 508, 518, 528, 538, 548, 558, 568, 578, 588, 598, 608, 618,
    628, 638, 648, 658, 668, 678, 688, 698, 708, 718, 728, 738, 748, 758, 768, 778, 788, 798, 808,
    818, 828, 838, 848, 858, 868, 878, 888, 898, 908, 918, 928, 938, 948, 958, 968, 978, 988, 998,
    1008, 1018, 1028, 1038, 1048, 1058, 1068, 1078, 1228, 1378, 1528, 1678, 1828, 1978, 2128, 2278,
    2428, 2578, 2728, 2878, 3028, 3178, 3328, 3478, 3628, 3778, 3928, 4078, 4228, 4378, 4528, 4678,
    4828, 4978, 5128, 5278, 5428, 5578, 5728, 5878, 6028, 6178, 6328, 6478, 6628, 6778, 6928, 7078,
    7228, 7378, 7528, 7678, 7828, 7978, 8128, 8278, 8428, 8578, 8728, 8878, 9028, 9178, 9328, 9478,
    9628, 9778, 9928, 10000, 10000, 10000, 10000, 10000,
];

/// A `FeedUpdated`'s row.
fn compressed_price(
    decoded: &Decoded,
    _: usize,
    input: Option<&[u8]>,
) -> Result<Vec<Row>, DeriveError> {
    let id = fixed_bytes(decoded, 0, "feedId", 32)?;
    let (Some(&slot), Some(&position)) = (id.get(30), id.get(31)) else {
        return Err(element(0, "feedId", "a compressed feed id"));
    };
    let position = usize::from(position);
    if position >= PER_SLOT {
        return Err(element(0, "feedId", "a compressed feed id"));
    }
    let input = input.ok_or(DeriveError::Input {
        reason: "was not fetched",
    })?;
    let word = words(input)
        .filter(|word| word[31] == slot)
        .max_by_key(|word| timestamp_ms(word))
        .ok_or(DeriveError::Input {
            reason: "holds no push of the feed's slot",
        })?;
    let feed = &word[6 * position..6 * position + 6];
    let packed = u32::from_be_bytes([feed[0], feed[1], feed[2], feed[3]]);
    if packed == 0 || (feed[4], feed[5]) == (0xff, 0xff) {
        return Err(DeriveError::Input {
            reason: "pushes no price for the feed",
        });
    }
    // A 27-bit mantissa shifted by at most 31 fits in 58 bits.
    let price = u64::from(packed & ((1 << 27) - 1)) << (packed >> 27);
    let spread = |index: u8| U256::from(CODEBOOK[usize::from(index)]);
    Ok(vec![vec![
        (
            "feed_id",
            TypedValue::FixedBytes {
                value: Bytes::copy_from_slice(id),
                size: 32,
            },
        ),
        ("price_e8", unsigned(U256::from(price), 64)),
        ("spread0", unsigned(spread(feed[4]), 16)),
        ("spread1", unsigned(spread(feed[5]), 16)),
        (
            "feed_timestamp_ms",
            unsigned(U256::from(timestamp_ms(word)), 56),
        ),
    ]])
}

/// The slot words `input` pushes: after its zero byte, or `updateBySignature`'s one.
fn words(input: &[u8]) -> impl Iterator<Item = &[u8; 32]> {
    let pushed = match input.split_first() {
        Some((0, rest)) if !rest.is_empty() && rest.len() % 32 == 0 => rest,
        _ if input.starts_with(&UPDATE_BY_SIGNATURE) => input.get(4 + 32..4 + 64).unwrap_or(&[]),
        _ => &[],
    };
    pushed
        .chunks_exact(32)
        .filter_map(|word| word.try_into().ok())
}

/// A slot word's timestamp, milliseconds: its 7 bytes before the slot's index.
fn timestamp_ms(word: &[u8; 32]) -> u64 {
    word[24..31]
        .iter()
        .fold(0, |ms, &byte| (ms << 8) | u64::from(byte))
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{B256, Bytes, U256, b256};

    use crate::decode::fixtures::metric::{compressed_push, compressed_updated};
    use crate::wire::typed::TypedValue;

    use super::super::super::DeriveError;
    use super::{COMPRESSED_ORACLE_PRICES, UPDATE_BY_SIGNATURE};

    /// The first feed of [`compressed_push`]'s first slot.
    const FEED: B256 = b256!("0x279216df9114f7448b43de7056cbb31a01128013000000000000000000380000");

    fn row(feed: B256, input: Option<&[u8]>) -> Result<Vec<TypedValue>, DeriveError> {
        let rows = (COMPRESSED_ORACLE_PRICES.derive)(&compressed_updated(feed), 0, input)?;
        assert!(rows.len() <= 1);
        Ok(rows.into_iter().flatten().map(|(_, value)| value).collect())
    }

    fn uint(value: u64, bits: u16) -> TypedValue {
        TypedValue::Uint {
            value: U256::from(value),
            bits,
        }
    }

    /// The feed as the oracle stored it, read back with `getOracleData` at the push's
    /// block: $754.64510464, both spreads 153 millionths, and the word's timestamp.
    #[test]
    fn a_feed_reads_as_the_oracle_stores_it() {
        assert_eq!(
            row(FEED, Some(&compressed_push())).expect("reads"),
            [
                TypedValue::FixedBytes {
                    value: Bytes::copy_from_slice(FEED.as_slice()),
                    size: 32,
                },
                uint(75_464_510_464, 64),
                uint(153, 16),
                uint(153, 16),
                uint(1_793_665_446_552, 56),
            ]
        );
    }

    /// The same word pushed through `updateBySignature` reads the same.
    #[test]
    fn a_signed_push_reads_the_same() {
        let push = compressed_push();
        let mut signed = UPDATE_BY_SIGNATURE.to_vec();
        signed.extend([0; 32]);
        signed.extend(&push[1..33]);
        signed.extend([0; 96]);
        assert_eq!(
            row(FEED, Some(&signed)).expect("reads"),
            row(FEED, Some(&push)).expect("reads")
        );
    }

    /// A feed without its transaction's input, whose slot the push lacks, left empty by
    /// it, or out of a push that is not one, derives nothing and says why.
    #[test]
    fn an_input_without_the_feed_is_an_input_error() {
        let push = compressed_push();
        let mut other_slot = FEED.0;
        other_slot[30] = 9;
        let mut empty = push.clone();
        empty[1..7].fill(0);
        for (feed, input) in [
            (FEED, None),
            (B256::from(other_slot), Some(&push[..])),
            (FEED, Some(&empty[..])),
            (FEED, Some(&push[..push.len() - 1])),
            (FEED, Some(&push[1..])),
        ] {
            assert!(
                matches!(row(feed, input), Err(DeriveError::Input { .. })),
                "{input:?}"
            );
        }
    }

    /// A feed id whose position is past a slot's four is no compressed feed.
    #[test]
    fn a_position_past_the_slot_is_an_argument_error() {
        let mut past = FEED.0;
        past[31] = 4;
        assert!(matches!(
            row(B256::from(past), Some(&compressed_push())),
            Err(DeriveError::Argument { .. })
        ));
    }
}
