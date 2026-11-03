//! `PythOracle`'s updates: Pyth Lazer EVM updates.
//!
//! The oracle is called with `[feedsLength u16][feedIds u32…][priceUpdate]`, where
//! `priceUpdate` is a Lazer EVM update: a magic `0x2a22999a`, a 65-byte signature, the
//! payload's length as a `u16`, and the payload. The payload is a magic `0x93c7d375`, a
//! `u64` timestamp, a `u8` channel, a `u8` feed count, and per feed its `u32` id, a `u8`
//! property count, and each property's `u8` id and value. Every number is big-endian.
//!
//! Every update in the input is parsed as the oracle's `LazerConsumer` parses it, and what
//! it stores for a feed is:
//!
//! | Value | From |
//! | --- | --- |
//! | price | `Price` (property 0) at 8 decimals by `Exponent` (property 4), rounded down |
//! | spread | `Confidence` (property 5) over `Price`, in millionths, rounded up |
//! | timestamp | `FeedUpdateTimestamp` (property 12), microseconds, in milliseconds |

use alloy_primitives::U256;

use super::{MAX_SPREAD, SPREAD_SCALE, Stored};

/// The Lazer EVM update's magic.
const UPDATE_MAGIC: [u8; 4] = 0x2a22_999a_u32.to_be_bytes();
/// The Lazer payload's magic, the oracle's `FORMAT_MAGIC`.
const PAYLOAD_MAGIC: [u8; 4] = 0x93c7_d375_u32.to_be_bytes();
/// The bytes before an update's payload: the magic, the signature, and the length.
const UPDATE_HEADER: usize = 4 + 65 + 2;
/// The decimals the oracle stores a price at.
const TARGET_DECIMALS: i32 = 8;

/// What the oracle stores for `feed` from `input`, the newest of its updates there, or
/// `None` when it holds none the oracle stores.
pub(super) fn stored(input: &[u8], feed: u32) -> Option<Stored> {
    payloads(input)
        .filter_map(|payload| stored_in(payload, feed))
        .max_by_key(|stored| stored.timestamp_ms)
}

/// Every Lazer payload in `input`, by its update's magic, its length fitting, and its own
/// magic.
fn payloads(input: &[u8]) -> impl Iterator<Item = &[u8]> {
    (0..input.len()).filter_map(|start| {
        let update = input.get(start..)?;
        if !update.starts_with(&UPDATE_MAGIC) {
            return None;
        }
        let length = usize::from(u16::from_be_bytes(
            update
                .get(UPDATE_HEADER - 2..UPDATE_HEADER)?
                .try_into()
                .ok()?,
        ));
        let payload = update.get(UPDATE_HEADER..UPDATE_HEADER + length)?;
        payload.starts_with(&PAYLOAD_MAGIC).then_some(payload)
    })
}

/// What the oracle stores for `feed` from `payload`, or `None` when the payload does not
/// hold it, the oracle would refuse the payload, or it would skip the feed.
fn stored_in(payload: &[u8], feed: u32) -> Option<Stored> {
    let mut stored = None;
    for parsed in parse(payload)? {
        if parsed.id == feed
            && let Some(value) = parsed.normalize().ok()?
            && stored
                .as_ref()
                .is_none_or(|kept: &Stored| value.timestamp_ms > kept.timestamp_ms)
        {
            stored = Some(value);
        }
    }
    stored
}

/// One feed of a payload, its properties as the oracle reads them, missing ones zero.
struct Feed {
    id: u32,
    price: i64,
    exponent: i16,
    confidence: u64,
    timestamp_us: u64,
}

/// Every feed of `payload`, or `None` when the oracle would refuse it: a variable-length
/// or unknown property, or a length that does not end at the last feed.
fn parse(payload: &[u8]) -> Option<Vec<Feed>> {
    let mut cursor = Cursor {
        payload,
        at: 4 + 8 + 1,
    };
    let count = cursor.take::<1>()?[0];
    let mut feeds = Vec::with_capacity(usize::from(count));
    for _ in 0..count {
        let mut feed = Feed {
            id: u32::from_be_bytes(cursor.take()?),
            price: 0,
            exponent: 0,
            confidence: 0,
            timestamp_us: 0,
        };
        let properties = cursor.take::<1>()?[0];
        for _ in 0..properties {
            match cursor.take::<1>()?[0] {
                0 => feed.price = i64::from_be_bytes(cursor.take()?),
                1 | 2 | 10 | 11 => cursor.skip(8)?,
                3 | 9 => cursor.skip(2)?,
                4 => feed.exponent = i16::from_be_bytes(cursor.take()?),
                5 => feed.confidence = u64::from_be_bytes(cursor.take()?),
                12 => {
                    if cursor.take::<1>()?[0] != 0 {
                        feed.timestamp_us = u64::from_be_bytes(cursor.take()?);
                    }
                }
                _ => return None,
            }
        }
        feeds.push(feed);
    }
    (cursor.at == payload.len()).then_some(feeds)
}

impl Feed {
    /// What the oracle stores, `None` when it skips the feed.
    ///
    /// # Errors
    ///
    /// Returns [`Refused`] when the oracle would refuse the payload: an exponent out of
    /// its range or a price past 64 bits.
    fn normalize(&self) -> Result<Option<Stored>, Refused> {
        let timestamp_ms = Some(self.timestamp_us / 1000).filter(|&ms| ms <= 0xFFFF_FFFF_FFFF);
        let (Ok(price), Some(timestamp_ms @ 1..)) = (u64::try_from(self.price), timestamp_ms)
        else {
            return Ok(None);
        };
        if price == 0 {
            return Ok(None);
        }
        let exponent = i32::from(self.exponent) + TARGET_DECIMALS;
        if !(-57..=57).contains(&exponent) {
            return Err(Refused);
        }
        let scale = U256::from(10).pow(U256::from(exponent.unsigned_abs()));
        let scaled = if exponent >= 0 {
            U256::from(price) * scale
        } else {
            U256::from(price) / scale
        };
        let price_e8 = u64::try_from(scaled).map_err(|_| Refused)?;
        if price_e8 == 0 {
            return Ok(None);
        }
        let spread =
            (u128::from(SPREAD_SCALE) * u128::from(self.confidence)).div_ceil(u128::from(price));
        Ok(Some(Stored {
            price: price_e8,
            spread0: u16::try_from(spread.min(MAX_SPREAD.into())).map_err(|_| Refused)?,
            timestamp_ms,
        }))
    }
}

/// A payload the oracle would refuse, reverting its update.
struct Refused;

/// A position in a payload, read forward.
struct Cursor<'a> {
    payload: &'a [u8],
    at: usize,
}

impl Cursor<'_> {
    /// The next `N` bytes.
    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let bytes = self.payload.get(self.at..self.at + N)?.try_into().ok()?;
        self.at += N;
        Some(bytes)
    }

    /// Skips `len` bytes.
    fn skip(&mut self, len: usize) -> Option<()> {
        self.payload.get(self.at..self.at + len)?;
        self.at += len;
        Some(())
    }
}
