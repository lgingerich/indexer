//! Rows computed from decoded logs: the last step of decoding a record.
//!
//! A [`Derivation`] reads one or more events of one protocol's contract and writes one
//! table: rows per decoded record, one for most and one per array entry for some, computed
//! from that record alone. Nothing is carried from one record to the next, so a derived
//! row belongs to its log's block like the decoded row it came from. A `reorg` deletes it by that block, a restart has nothing to restore,
//! and any store, the lake included, can hold it.
//!
//! The [`Catalog`](super::Catalog) declares the table of every derivation whose event's
//! exact signature a manifest declares, beside the event tables, and the
//! [`DecodingSink`](super::DecodingSink) publishes each record's derived rows right after
//! it, so they commit with its block. The rows go to the stores that list the table, as
//! decoded rows go to the stores that list theirs.
//!
//! A derivation is Rust code in this module, one file per protocol. It pins the event's
//! full signature, so an ABI that retyped an argument leaves the table undeclared and a
//! store listing it fails at startup. It reads each argument by position and name, so one
//! that moved under an unchanged signature is caught at its first record. A record a
//! derivation cannot read derives nothing and is logged; the decoded record still
//! commits. A row that does not fit its table is a bug in the derivation, and the store
//! refuses it, which stops the run.
//!
//! # Removing it
//!
//! Derivations are meant to give way to a streaming layer outside the indexer. Taking
//! them out is this directory and its two callers:
//!
//! - `Catalog::add_derivations`, which declares their tables, in `catalog.rs`.
//! - The `derive::rows` call in `DecodingSink::decode`, in `sink.rs`.
//!
//! What carries a derived row to the stores is generic and can go with them:
//! `Event::Derived` and `DerivedField` in `wire::envelope`; `TableId::Derived`,
//! `Schema::add_derived`, `Schema::derived_table`, `Schema::derived_row`, and the
//! `UnknownTable`, `Field`, and `NotADataset` table errors in `sink::table`; and the
//! `Event::Derived` and `TableId::Derived` arms in `sink::route`, `sink::progress`, and
//! `ingest::source::datasets`.

mod metric;

#[cfg(test)]
pub(super) use metric::fixtures;

use alloy_primitives::{Address, I256, U256};
use tracing::warn;

use crate::sink::table::ColumnType;
use crate::wire::envelope::{ChainId, Decoded, Derived, DerivedField, TypedValue};

/// Every derivation the indexer has.
pub(super) const DERIVATIONS: [Derivation; 4] = [
    metric::SWAPS,
    metric::BIN_FILLS,
    metric::LIQUIDITY,
    metric::POOL_BINS,
];

/// One table computed from decoded events of one contract: rows per record, from that
/// record alone.
#[derive(Debug, Clone, Copy)]
pub(super) struct Derivation {
    /// The protocol whose events it reads, as its manifest names it.
    pub(super) protocol: &'static str,
    /// The contract that emits them, as the catalog names it.
    pub(super) contract: &'static str,
    /// Each event's canonical signature, for example `Swap(address,address,uint256)`.
    /// The table is declared only when the catalog declares every one.
    pub(super) signatures: &'static [&'static str],
    /// The table it writes.
    pub(super) table: &'static str,
    /// The table's own columns, in order: what follows the log's columns.
    pub(super) columns: &'static [(&'static str, ColumnType)],
    /// One record's rows, each a value per column in order. A record may derive none.
    pub(super) derive: fn(&Decoded) -> Result<Vec<Vec<TypedValue>>, DeriveError>,
}

impl Derivation {
    /// Whether `decoded` is a record of an event this reads.
    fn reads(&self, decoded: &Decoded) -> bool {
        decoded.protocol == self.protocol
            && decoded.contract == self.contract
            && self.signatures.contains(&decoded.signature.as_str())
    }

    /// The rows `decoded` derives, numbered in order.
    ///
    /// Each value is named for its column in order. A value past the last column gets an
    /// empty name rather than being dropped, so the store refuses the row instead of
    /// storing it short of what the derivation computed.
    ///
    /// # Errors
    ///
    /// Returns [`DeriveError::Argument`] when an argument is missing, misnamed, of another
    /// type, or out of the range the contract keeps it in.
    fn rows(&self, decoded: &Decoded, sent: Sent) -> Result<Vec<Derived>, DeriveError> {
        Ok((self.derive)(decoded)?
            .into_iter()
            .zip(0..)
            .map(|(values, row)| Derived {
                table: self.table.to_owned(),
                address: decoded.address,
                transaction_hash: decoded.transaction_hash,
                transaction_index: decoded.transaction_index,
                log_index: decoded.log_index,
                row,
                transaction_from_address: sent.map(|(from, _)| from),
                transaction_to_address: sent.and_then(|(_, to)| to),
                fields: values
                    .into_iter()
                    .enumerate()
                    .map(|(position, value)| DerivedField {
                        name: self
                            .columns
                            .get(position)
                            .map_or_else(String::new, |&(name, _)| name.to_owned()),
                        value,
                    })
                    .collect(),
                block_number: decoded.block_number,
                block_hash: decoded.block_hash,
                block_timestamp: decoded.block_timestamp,
            })
            .collect())
    }
}

/// Why a decoded record derived no row: the derivation could not read it.
#[derive(Debug, thiserror::Error)]
pub(super) enum DeriveError {
    /// The record has no argument `name` at `position` of the type the derivation reads.
    #[error("argument {position} is not {expected} named {name}")]
    Argument {
        /// The argument's position among all the event's inputs.
        position: usize,
        /// The argument's name in the ABI.
        name: &'static str,
        /// The type the derivation reads there.
        expected: &'static str,
    },
}

/// The sender and recipient of the transaction a log came from, or `None` when the
/// transaction was not fetched. The recipient is `None` for a contract deployment.
pub(super) type Sent = Option<(Address, Option<Address>)>;

/// The rows `decoded` derives, from every derivation that reads its event, each carrying
/// the transaction's sender and recipient.
///
/// A record a derivation cannot read is logged and derives nothing from it.
pub(super) fn rows(
    derivations: &[Derivation],
    chain: &ChainId,
    decoded: &Decoded,
    sent: Sent,
) -> Vec<Derived> {
    derivations
        .iter()
        .filter(|derivation| derivation.reads(decoded))
        .flat_map(|derivation| {
            derivation
                .rows(decoded, sent)
                .inspect_err(|error| {
                    warn!(%chain, table = derivation.table, address = %decoded.address,
                        block = decoded.block_number, block_hash = %decoded.block_hash,
                        transaction = %decoded.transaction_hash,
                        log_index = decoded.log_index, %error,
                        "derivation failed; keeping the decoded record");
                })
                .unwrap_or_default()
        })
        .collect()
}

/// Argument `position` of `decoded`, among all its inputs, when it is named `name`.
fn argument<'a>(decoded: &'a Decoded, position: usize, name: &str) -> Option<&'a TypedValue> {
    decoded
        .indexed
        .iter()
        .chain(&decoded.body)
        .find(|argument| argument.position == position && argument.name == name)
        .map(|argument| &argument.value)
}

/// Argument `position` of `decoded`, an `address` named `name`.
fn address(decoded: &Decoded, position: usize, name: &'static str) -> Result<Address, DeriveError> {
    match argument(decoded, position, name) {
        Some(TypedValue::Address { value }) => Ok(*value),
        _ => Err(DeriveError::Argument {
            position,
            name,
            expected: "an address",
        }),
    }
}

/// Argument `position` of `decoded`, an `int{bits}` named `name`.
fn int(
    decoded: &Decoded,
    position: usize,
    name: &'static str,
    bits: u16,
) -> Result<I256, DeriveError> {
    match argument(decoded, position, name) {
        Some(TypedValue::Int { value, bits: found }) if *found == bits => Ok(*value),
        _ => Err(DeriveError::Argument {
            position,
            name,
            expected: "a signed integer of its declared width",
        }),
    }
}

/// Argument `position` of `decoded`, a `uint{bits}` named `name`.
fn uint(
    decoded: &Decoded,
    position: usize,
    name: &'static str,
    bits: u16,
) -> Result<U256, DeriveError> {
    match argument(decoded, position, name) {
        Some(TypedValue::Uint { value, bits: found }) if *found == bits => Ok(*value),
        _ => Err(DeriveError::Argument {
            position,
            name,
            expected: "an unsigned integer of its declared width",
        }),
    }
}

/// Argument `position` of `decoded`, a dynamic array named `name`.
fn array<'a>(
    decoded: &'a Decoded,
    position: usize,
    name: &'static str,
) -> Result<&'a [TypedValue], DeriveError> {
    match argument(decoded, position, name) {
        Some(TypedValue::Array { value }) => Ok(value),
        _ => Err(DeriveError::Argument {
            position,
            name,
            expected: "an array",
        }),
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::U256;

    use crate::wire::envelope::{ChainId, TypedValue};

    use super::metric::{SWAPS, fixtures};
    use super::{DERIVATIONS, rows};

    /// Every derivation's pinned signature is declared in the shipped protocols, so the
    /// catalog declares its table: an ABI edit that changes the event fails here, not at
    /// a deploy.
    #[test]
    fn the_shipped_protocols_declare_every_derived_table() {
        let catalog = fixtures::catalog();
        for derivation in DERIVATIONS {
            assert!(
                catalog.schema().derived_table(derivation.table).is_some(),
                "{}",
                derivation.table
            );
        }
    }

    /// A record whose arguments the derivation cannot read derives nothing, rather than
    /// failing or guessing.
    #[test]
    fn an_unreadable_record_derives_nothing() {
        let chain = ChainId::new("base");
        let mut swap = fixtures::swap(U256::ZERO, U256::ZERO, U256::ZERO);
        assert_eq!(rows(&[SWAPS], &chain, &swap, None).len(), 1);
        swap.body[0].value = TypedValue::Bool { value: true };
        assert!(rows(&[SWAPS], &chain, &swap, None).is_empty());
    }
}
