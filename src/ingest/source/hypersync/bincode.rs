//! The bincode `HyperSync` writes nested lists in.

use alloy_primitives::{Address, B256, Bytes};
use alloy_rpc_types_eth::{
    AccessList, AccessListItem, Authorization, SignedAuthorization, Withdrawal,
};

use crate::ingest::source::{SourceError, malformed};

use super::CONTEXT;
use super::wire::{Quantity, parse_quantity};

/// 32-byte hashes, concatenated. Absent is none.
pub(super) fn hashes(bytes: Option<&Bytes>, field: &str) -> Result<Vec<B256>, SourceError> {
    let Some(bytes) = bytes else {
        return Ok(Vec::new());
    };
    if bytes.len() % 32 != 0 {
        return Err(malformed(
            CONTEXT,
            format!(
                "{field} is {} bytes, not a whole number of hashes",
                bytes.len()
            ),
        ));
    }
    Ok(bytes.chunks_exact(32).map(B256::from_slice).collect())
}

/// An access list, from `HyperSync`'s bincode: a list of items, each an optional address
/// and an optional list of storage keys, every value its `0x` hex string.
pub(super) fn access_list(bytes: &[u8]) -> Result<AccessList, SourceError> {
    let mut reader = Bincode::new(bytes, "access_list");
    let count = reader.len()?;
    let mut items = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let address = if reader.option()? {
            reader.parse::<Address>()?
        } else {
            return Err(malformed(CONTEXT, "access_list item has no address"));
        };
        let mut storage_keys = Vec::new();
        if reader.option()? {
            for _ in 0..reader.len()? {
                storage_keys.push(reader.parse::<B256>()?);
            }
        }
        items.push(AccessListItem {
            address,
            storage_keys,
        });
    }
    reader.finish()?;
    Ok(AccessList(items))
}

/// An authorization list, from `HyperSync`'s bincode: a list of authorizations, each its
/// chain id, address, nonce, y parity, `r`, and `s` as `0x` hex strings.
pub(super) fn authorization_list(bytes: &[u8]) -> Result<Vec<SignedAuthorization>, SourceError> {
    let mut reader = Bincode::new(bytes, "authorization_list");
    let count = reader.len()?;
    let mut authorizations = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let chain_id = reader.quantity()?.0;
        let address = reader.parse::<Address>()?;
        let nonce = reader.quantity()?.to_u64("authorization nonce")?;
        let y_parity = u8::try_from(reader.quantity()?.to_u64("authorization y_parity")?)
            .map_err(|_| malformed(CONTEXT, "authorization y_parity is above 255"))?;
        let r = reader.quantity()?.0;
        let s = reader.quantity()?.0;
        authorizations.push(SignedAuthorization::new_unchecked(
            Authorization {
                chain_id,
                address,
                nonce,
            },
            y_parity,
            r,
            s,
        ));
    }
    reader.finish()?;
    Ok(authorizations)
}

/// Validator withdrawals, from `HyperSync`'s bincode: a list of withdrawals, each its
/// index, validator index, address, and amount in gwei, every one an optional `0x` hex
/// string.
pub(super) fn withdrawals(bytes: &[u8]) -> Result<Vec<Withdrawal>, SourceError> {
    let mut reader = Bincode::new(bytes, "withdrawals");
    let count = reader.len()?;
    let mut withdrawals = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let index = reader.required_quantity("index")?;
        let validator_index = reader.required_quantity("validator_index")?;
        if !reader.option()? {
            return Err(reader.error("has a withdrawal with no address"));
        }
        let address = reader.parse::<Address>()?;
        let amount = reader.required_quantity("amount")?;
        withdrawals.push(Withdrawal {
            index,
            validator_index,
            address,
            amount,
        });
    }
    reader.finish()?;
    Ok(withdrawals)
}

/// A reader over bincode's default encoding, the one `HyperSync` writes nested fields in:
/// lengths are little-endian `u64`, an option is a `0` or `1` byte before its value,
/// and a string is its length and its UTF-8 bytes.
struct Bincode<'a> {
    pub(super) bytes: &'a [u8],
    pub(super) field: &'static str,
}

impl<'a> Bincode<'a> {
    const fn new(bytes: &'a [u8], field: &'static str) -> Self {
        Self { bytes, field }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], SourceError> {
        if self.bytes.len() < count {
            return Err(self.error("ends early"));
        }
        let (taken, rest) = self.bytes.split_at(count);
        self.bytes = rest;
        Ok(taken)
    }

    fn len(&mut self) -> Result<usize, SourceError> {
        let mut word = [0; 8];
        word.copy_from_slice(self.take(8)?);
        usize::try_from(u64::from_le_bytes(word)).map_err(|_| self.error("has a length past usize"))
    }

    fn option(&mut self) -> Result<bool, SourceError> {
        match self.take(1)? {
            [0] => Ok(false),
            [1] => Ok(true),
            _ => Err(self.error("has an option tag that is neither 0 nor 1")),
        }
    }

    fn string(&mut self) -> Result<&'a str, SourceError> {
        let len = self.len()?;
        std::str::from_utf8(self.take(len)?)
            .map_err(|_| self.error("has a string that is not UTF-8"))
    }

    fn parse<T: std::str::FromStr>(&mut self) -> Result<T, SourceError> {
        let text = self.string()?;
        T::from_str(text).map_err(|_| self.error(&format!("has {text:?}, which does not parse")))
    }

    fn quantity(&mut self) -> Result<Quantity, SourceError> {
        let text = self.string()?;
        parse_quantity(text).map_err(|detail| self.error(&detail))
    }

    /// An optional quantity that must be present, as a `u64`.
    fn required_quantity(&mut self, name: &str) -> Result<u64, SourceError> {
        if !self.option()? {
            return Err(self.error(&format!("has an item with no {name}")));
        }
        self.quantity()?.to_u64(name)
    }

    fn finish(&self) -> Result<(), SourceError> {
        if self.bytes.is_empty() {
            Ok(())
        } else {
            Err(self.error("has bytes past its end"))
        }
    }

    fn error(&self, detail: &str) -> SourceError {
        malformed(CONTEXT, format!("{} {detail}", self.field))
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use super::*;

    /// An authorization list as `HyperSync` encoded Ethereum's
    /// `0x60c1fb63a6a350008912ae671e6b6630eca2711a24f671d3075bf38f3dbbafcd`.
    #[test]
    fn an_authorization_list_decodes_from_bincode() {
        let bytes = alloy_primitives::hex::decode(
            "010000000000000003000000000000003078312a0000000000000030783561376663313133393765396138616434316266313062663133663232623061363366393666366403000000000000003078300300000000000000307831420000000000000030783534376134663966313238353263393865326330623661343935646239626432653138373430366639396437376635623461623966616631623434313534396541000000000000003078633734343831316537623532323935323235306234386466613436323131386230313661343137373665613230616533313166336230383264653131343964",
        )
        .expect("hex");
        let list = authorization_list(&bytes).expect("decodes");
        assert_eq!(list.len(), 1);
        let authorization = &list[0];
        assert_eq!(authorization.chain_id, alloy_primitives::U256::from(1));
        assert_eq!(
            authorization.address,
            "0x5a7fc11397e9a8ad41bf10bf13f22b0a63f96f6d"
                .parse::<Address>()
                .expect("address")
        );
        assert_eq!(authorization.nonce, 0);
        assert_eq!(authorization.y_parity(), 1);
        assert!(
            authorization_list(&bytes[..bytes.len() - 1]).is_err(),
            "a truncated list"
        );
    }

    /// The first of Ethereum block 23,000,000's withdrawals, as `HyperSync` encodes them.
    #[test]
    fn withdrawals_decode_from_bincode() {
        let bytes = alloy_primitives::hex::decode(
            "010000000000000001090000000000000030783562303465656201070000000000000030786237633430012a00000000000000307862396437393334383738623566623936313062336665386135653434316538666164376532393366010900000000000000307831323764626564",
        )
        .expect("hex");
        let list = withdrawals(&bytes).expect("decodes");
        assert_eq!(list.len(), 1);
        assert_eq!(
            (list[0].index, list[0].validator_index, list[0].amount),
            (0x05b0_4eeb, 0x000b_7c40, 0x0127_dbed)
        );
        assert_eq!(
            list[0].address,
            "0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f"
                .parse::<Address>()
                .expect("address")
        );
        assert!(
            withdrawals(&bytes[..bytes.len() - 1]).is_err(),
            "a truncated list"
        );
    }
}
