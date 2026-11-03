//! `ChainlinkOracle`'s updates: Chainlink Data Streams reports.
//!
//! The oracle is called with `updateReport(bytes)` or `updateReports(bytes[])`, each a
//! version tag byte and a DON-signed full report, which the verifier checks and returns
//! the report of. The report is an ABI-encoded struct of whole words, the feed's id first;
//! the id's first two bytes are its schema, a timestamp resolution in the top four bits,
//! 0 for seconds and 1 for milliseconds, and the schema version in the rest:
//!
//! | Schema | After the id | Price | Spread | Timestamp |
//! | --- | --- | --- | --- | --- |
//! | v3, seconds | `validFrom u32`, `observations u32`, two `uint192` fees, `expiresAt u32`, `price`, `bid`, `ask` (`int192`) | `price` | `bid` and `ask` | `observations` × 1000 |
//! | v4, seconds | as v3 to `price`, then `marketStatus u32` | `price` | 0 when the market is open (2), else 65535 | `observations` × 1000 |
//! | v3, milliseconds (High Frequency Streams) | as v3 with `u64` timestamps, `benchmarkPrice` for `price` | `benchmarkPrice` | `bid` and `ask` | `observations` |
//!
//! Every place the feed's id appears in the input is read as a report of its schema, and
//! kept when its words fit their types, as `abi.decode` checks them, and the oracle stores
//! it, as `ChainlinkOracle` normalizes it: the 18-decimal price at 8 decimals, rounded
//! down, and the spread the wider of `ask` above and `bid` below the price, over the
//! price, in millionths, rounded up.

use alloy_primitives::{I256, U256};

use super::super::super::value::signed_bits;
use super::{MAX_SPREAD, SPREAD_SCALE, Stored};

/// Data Streams prices are 18-decimal; the oracle stores 8.
const PRICE_SCALE: u64 = 10_000_000_000;
/// The spread the oracle stores for a closed market, which halts its pools.
const STALL_SPREAD: u16 = u16::MAX;

/// The schemas the oracle reads.
#[derive(Clone, Copy)]
enum Schema {
    V3,
    V4,
    HighFrequency,
}

/// What the oracle stores for stream `id` from `input`, the newest of its reports there,
/// or `None` when it holds none the oracle stores or `id`'s schema is not one it reads.
pub(super) fn stored(input: &[u8], id: &[u8; 32]) -> Option<Stored> {
    let schema = schema(id)?;
    (0..input.len())
        .filter_map(|start| input.get(start..).filter(|report| report.starts_with(id)))
        .filter_map(|report| normalize(schema, &Words(report)))
        .max_by_key(|stored| stored.timestamp_ms)
}

/// The schema of stream `id`, by its resolution and version, as the oracle routes it.
fn schema(id: &[u8; 32]) -> Option<Schema> {
    let version = u16::from_be_bytes([id[0], id[1]]) & 0x0FFF;
    match (id[0] >> 4, version) {
        (0, 3) => Some(Schema::V3),
        (0, 4) => Some(Schema::V4),
        (1, 3) => Some(Schema::HighFrequency),
        _ => None,
    }
}

/// What the oracle stores from `report`, a report of `schema`, or `None` when it does not
/// decode or the oracle would refuse or skip it.
fn normalize(schema: Schema, report: &Words<'_>) -> Option<Stored> {
    let time_bits = match schema {
        Schema::V3 | Schema::V4 => 32,
        Schema::HighFrequency => 64,
    };
    report.uint(1, time_bits)?;
    let observed = u64::try_from(report.uint(2, time_bits)?).ok()?;
    report.uint(3, 192)?;
    report.uint(4, 192)?;
    report.uint(5, time_bits)?;
    let price = report.int(6, 192)?;
    let spread0 = match schema {
        Schema::V4 => {
            if report.uint(7, 32)? == U256::from(2) {
                0
            } else {
                STALL_SPREAD
            }
        }
        Schema::V3 | Schema::HighFrequency => {
            spread(price, report.int(7, 192)?, report.int(8, 192)?)?
        }
    };
    if !price.is_positive() {
        return None;
    }
    let price_e8 = u64::try_from(price.into_raw() / U256::from(PRICE_SCALE)).ok()?;
    let timestamp_ms = match schema {
        Schema::V3 | Schema::V4 => observed.checked_mul(1000)?,
        Schema::HighFrequency => observed,
    };
    (price_e8 > 0 && (1..1 << 56).contains(&timestamp_ms)).then_some(Stored {
        price: price_e8,
        spread0,
        timestamp_ms,
    })
}

/// The wider of `ask` above and `bid` below `price`, over `price`, in millionths, rounded
/// up and at most [`MAX_SPREAD`]; `None` when `ask` is below `bid` or `price` is not
/// positive, which the oracle refuses.
fn spread(price: I256, bid: I256, ask: I256) -> Option<u16> {
    if !price.is_positive() || ask < bid {
        return None;
    }
    let deviation = (ask - price).max(price - bid).into_raw();
    let spread = (U256::from(SPREAD_SCALE) * deviation).div_ceil(price.into_raw());
    u16::try_from(spread.min(U256::from(MAX_SPREAD))).ok()
}

/// A report's words, read by index.
struct Words<'a>(&'a [u8]);

impl Words<'_> {
    /// Word `index`, when the report is that long.
    fn word(&self, index: usize) -> Option<U256> {
        self.0
            .get(32 * index..32 * (index + 1))
            .map(U256::from_be_slice)
    }

    /// Word `index`, a `uint{bits}`: `None` when a bit above them is set.
    fn uint(&self, index: usize, bits: usize) -> Option<U256> {
        self.word(index).filter(|&word| (word >> bits).is_zero())
    }

    /// Word `index`, an `int{bits}`: `None` when it is not sign-extended from them.
    fn int(&self, index: usize, bits: usize) -> Option<I256> {
        let word = self.word(index)?;
        let value = signed_bits(word, 0, bits);
        (value.into_raw() == word).then_some(value)
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{B256, I256, U256};

    use super::stored;

    /// A report of stream `id`: its words after the id, as `abi.encode` writes them.
    fn report(id: B256, words: &[I256]) -> Vec<u8> {
        let mut report = id.to_vec();
        for word in words {
            report.extend(word.into_raw().to_be_bytes::<32>());
        }
        report
    }

    fn int(value: i128) -> I256 {
        I256::try_from(value).expect("fits")
    }

    const E18: i128 = 1_000_000_000_000_000_000;

    /// A v4 report stores no spread while its market is open and the stall spread
    /// otherwise.
    #[test]
    fn a_v4_report_spreads_by_its_market_status() {
        let mut id = [0; 32];
        id[..2].copy_from_slice(&[0x00, 0x04]);
        id[31] = 1;
        let id = B256::from(id);
        for (status, spread) in [(2, 0), (1, u16::MAX), (0, u16::MAX)] {
            let words = [1, 1_700_000_000, 0, 0, 1_800_000_000, 250 * E18, status].map(int);
            let stored = stored(&report(id, &words), &id.0).expect("stores");
            assert_eq!(
                (stored.price, stored.spread0, stored.timestamp_ms),
                (25_000_000_000, spread, 1_700_000_000_000)
            );
        }
    }

    /// A High Frequency Streams report keeps its millisecond timestamp, and spreads by
    /// the wider of its bid and ask from its price.
    #[test]
    fn a_high_frequency_report_spreads_by_its_wider_side() {
        let mut id = [0; 32];
        id[..2].copy_from_slice(&[0x10, 0x03]);
        id[31] = 1;
        let id = B256::from(id);
        let words = [
            1,
            1_700_000_000_123,
            0,
            0,
            1_800_000_000_000,
            100 * E18,
            99 * E18,
            100 * E18 + E18 / 2,
        ]
        .map(int);
        let stored = stored(&report(id, &words), &id.0).expect("stores");
        assert_eq!(
            (stored.price, stored.spread0, stored.timestamp_ms),
            (10_000_000_000, 10_000, 1_700_000_000_123)
        );
    }

    /// A report whose ask is below its bid, whose price is not positive, or whose word
    /// overflows its type, is not one the oracle stores.
    #[test]
    fn a_report_the_oracle_refuses_is_not_stored() {
        let mut id = [0; 32];
        id[..2].copy_from_slice(&[0x00, 0x03]);
        id[31] = 1;
        let id = B256::from(id);
        let good = [
            1,
            1_700_000_000,
            0,
            0,
            1_800_000_000,
            100 * E18,
            99 * E18,
            101 * E18,
        ];
        assert!(stored(&report(id, &good.map(int)), &id.0).is_some());
        let mut crossed = good;
        crossed[6] = 102 * E18;
        let mut free = good;
        free[5] = 0;
        let mut wide = good.map(int);
        wide[1] = I256::from_raw(U256::from(1) << 32);
        for words in [crossed.map(int), free.map(int), wide] {
            assert!(stored(&report(id, &words), &id.0).is_none());
        }
        // An unknown schema, v5 in seconds, is not read.
        let mut other = id.0;
        other[1] = 0x05;
        assert!(stored(&report(B256::from(other), &good.map(int)), &other).is_none());
    }
}
