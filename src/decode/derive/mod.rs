//! Rows computed from decoded logs: the last step of decoding a record.
//!
//! A [`Derivation`] reads one or more events of one protocol's contract and writes one
//! table: rows per decoded record, one for most and one per array entry for some, computed
//! from that record alone. Nothing is carried from one record to the next, so a derived
//! row belongs to its log's block like the decoded row it came from. A `reorg` deletes it
//! by that block, a restart has nothing to restore, and any store, the lake included, can
//! hold it.
//!
//! The [`Catalog`](super::Catalog) declares the table of every derivation whose event's
//! exact signature a manifest declares, beside the event tables, and the
//! [`DecodingSink`](super::DecodingSink) publishes each record's derived rows right after
//! it, so they commit with its block. The rows go to the stores that list the table, as
//! decoded rows go to the stores that list theirs.
//!
//! A derivation is Rust code in this module, one directory per protocol and one file per
//! table, reading its record with the helpers in `value`. It pins the event's
//! full signature, so an ABI that retyped an argument leaves the table undeclared and a
//! store listing it fails at startup. It reads each argument by position and name, so one
//! that moved under an unchanged signature is caught at its first record. A record a
//! derivation cannot read derives nothing and is logged; the decoded record still
//! commits.
//!
//! A derivation writes each value beside its column's name, so two columns of one type
//! cannot trade values unseen: the pair moves together, or the names give it away. A row
//! that does not fit its table is a bug in the derivation, and stops the run: one that
//! does not name the table's columns in order is refused here, and one whose value does
//! not fit its column by the store.
//!
//! # Removing it
//!
//! Derivations run in the indexer so app data reaches Postgres within a second of its
//! block. Should they ever move elsewhere, taking them out is this directory and its
//! hooks in `decode`:
//!
//! - `mod derive` in `decode/mod.rs`.
//! - `Catalog::add_derivations`, which declares their tables and binds them, its call
//!   in `Catalog::load`, and the `Catalog::derivations` field, in `catalog.rs`.
//! - `Decoding::event`, which names the event a record decoded as, in `decoder.rs`.
//! - The `derivations` field of `DecodingSink` and its `rows` call, the last
//!   transaction it keeps for a row's sender and recipient, and the test of a derived
//!   row, in `sink.rs`.
//! - `fixtures::metric`, in `fixtures.rs`.
//! - `metrics::derive_failed`.
//!
//! What carries a derived row to the stores is generic and can go with them:
//! `Event::Derived` and `DerivedField` in `wire::envelope`; `TableId::Derived`,
//! `Schema::add_derived`, `Schema::derived_table`, `Schema::derived_row`, and the
//! `UnknownTable`, `Field`, and `NotADataset` table errors in `sink::table`; and the
//! `Event::Derived` and `TableId::Derived` arms in `sink::route`, `sink::progress`, and
//! `ingest::source::datasets`.

pub(super) mod metric;
mod value;

use alloy_primitives::{Address, TxHash};
use tracing::warn;

use super::abi::EventKey;
use super::catalog::EntryId;
use crate::metrics;
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
    /// The table's own columns, in order: what follows the log's columns. Every row names
    /// them, in this order.
    pub(super) columns: &'static [(&'static str, ColumnType)],
    /// The indexes the app's reads need, each its columns in order, beside the ones every
    /// table has by block.
    pub(super) indexes: &'static [&'static [&'static str]],
    /// One record's rows. A record may derive none.
    pub(super) derive: fn(&Decoded) -> Result<Vec<Row>, DeriveError>,
}

/// One derived row: each of its table's columns, by name, with its value.
pub(super) type Row = Vec<(&'static str, TypedValue)>;

impl Derivation {
    /// The rows `decoded` derives, numbered in order.
    ///
    /// # Errors
    ///
    /// Returns [`DeriveError::Argument`] when an argument is missing, misnamed, of another
    /// type, or out of the range the contract keeps it in, and [`DeriveError::Columns`]
    /// when a row does not name the table's columns in order.
    fn rows(&self, decoded: &Decoded, sent: Option<Sent>) -> Result<Vec<Derived>, DeriveError> {
        (self.derive)(decoded)?
            .into_iter()
            .zip(0..)
            .map(|(values, row)| {
                let names = values.iter().map(|&(name, _)| name);
                if !names.clone().eq(self.columns.iter().map(|&(name, _)| name)) {
                    return Err(DeriveError::Columns {
                        table: self.table,
                        names: names.collect(),
                    });
                }
                Ok(Derived {
                    table: self.table.to_owned(),
                    address: decoded.address,
                    transaction_hash: decoded.transaction_hash,
                    transaction_index: decoded.transaction_index,
                    log_index: decoded.log_index,
                    row,
                    transaction_from_address: sent.map(|sent| sent.from),
                    transaction_to_address: sent.and_then(|sent| sent.to),
                    fields: values
                        .into_iter()
                        .map(|(name, value)| DerivedField {
                            name: name.to_owned(),
                            value,
                        })
                        .collect(),
                    block_number: decoded.block_number,
                    block_hash: decoded.block_hash,
                    block_timestamp: decoded.block_timestamp,
                })
            })
            .collect()
    }
}

/// Why a decoded record derived no row: the derivation could not read it, or computed a
/// row that does not match its own table.
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
    /// A row does not name its table's columns in order: one short, one over, or two
    /// swapped. A bug in the derivation, which stops the run rather than storing a row
    /// short, shifted, or with values under each other's columns.
    #[error("a {table} row names columns {names:?}, not its table's in order")]
    Columns {
        /// The table.
        table: &'static str,
        /// The columns the row names, in its order.
        names: Vec<&'static str>,
    },
}

/// The transaction a log came from, as ingest forwarded it just before its logs: whose
/// sender and recipient every derived row carries.
#[derive(Debug, Clone, Copy)]
pub(super) struct Sent {
    /// The transaction.
    pub(super) hash: TxHash,
    /// The account that sent it.
    pub(super) from: Address,
    /// Its recipient, `None` for a contract deployment.
    pub(super) to: Option<Address>,
}

/// The derivations a catalog declares, each bound at load to the catalog entry and the
/// events it reads, so a decoded record finds its derivations by the event it decoded
/// as rather than by comparing names.
#[derive(Debug, Clone, Default)]
pub(super) struct Derivations {
    bound: Vec<Bound>,
}

/// One derivation, bound to one event of one catalog entry.
#[derive(Debug, Clone, Copy)]
struct Bound {
    entry: EntryId,
    event: EventKey,
    derivation: Derivation,
}

impl Derivations {
    /// Runs `derivation` on every record of `entry` decoded as one of `events`.
    pub(super) fn bind(
        &mut self,
        derivation: Derivation,
        entry: EntryId,
        events: impl IntoIterator<Item = EventKey>,
    ) {
        self.bound.extend(events.into_iter().map(|event| Bound {
            entry,
            event,
            derivation,
        }));
    }

    /// The rows `decoded`, a record of `entry` decoded as `event`, derives from every
    /// derivation bound to that event, each carrying `sent`'s sender and recipient, or
    /// neither when the transaction was not fetched.
    ///
    /// A record a derivation cannot read is logged and derives nothing from it.
    ///
    /// # Errors
    ///
    /// Returns [`DeriveError::Columns`] when a derivation computes a row that does not
    /// match its table, a bug in it that stops the run.
    pub(super) fn rows(
        &self,
        chain: &ChainId,
        (entry, event): (EntryId, EventKey),
        decoded: &Decoded,
        sent: Option<Sent>,
    ) -> Result<Vec<Derived>, DeriveError> {
        let mut derived = Vec::new();
        for Bound { derivation, .. } in self
            .bound
            .iter()
            .filter(|bound| bound.entry == entry && bound.event == event)
        {
            match derivation.rows(decoded, sent) {
                Ok(rows) => derived.extend(rows),
                Err(error @ DeriveError::Columns { .. }) => return Err(error),
                Err(error @ DeriveError::Argument { .. }) => {
                    metrics::derive_failed(derivation.table);
                    warn!(%chain, table = derivation.table, address = %decoded.address,
                        block = decoded.block_number, block_hash = %decoded.block_hash,
                        transaction = %decoded.transaction_hash,
                        log_index = decoded.log_index, %error,
                        "derivation failed; keeping the decoded record");
                }
            }
        }
        Ok(derived)
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::U256;

    use crate::decode::fixtures::metric::swap;
    use crate::decode::fixtures::shipped;
    use crate::wire::envelope::{ChainId, Decoded, TypedValue};

    use super::metric::SWAPS;
    use super::{DERIVATIONS, Derivation, Derivations, DeriveError, Row};

    /// Every derivation's pinned signature is declared in the shipped protocols, so the
    /// catalog declares its table: an ABI edit that changes the event fails here, not at
    /// a deploy.
    #[test]
    fn the_shipped_protocols_declare_every_derived_table() {
        let catalog = shipped("robinhood");
        for derivation in DERIVATIONS {
            assert!(
                catalog.schema().derived_table(derivation.table).is_some(),
                "{}",
                derivation.table
            );
        }
    }

    /// The shipped catalog binds a pool `Swap` to its derivation by the pool's entry and
    /// the event's key: the record derives its row as that event, and nothing as another
    /// event or another contract's.
    #[test]
    fn a_record_derives_from_the_derivations_bound_to_its_event() {
        let catalog = shipped("robinhood");
        let chain = ChainId::new("robinhood");
        let pool = catalog
            .entry("metric_v1", "MetricOmmPool")
            .expect("the pool");
        let factory = catalog
            .entry("metric_v1", "MetricOmmPoolFactory")
            .expect("the factory");
        let swap = swap(U256::ZERO, U256::ZERO, U256::ZERO);
        let rows = |event| {
            catalog
                .derivations
                .rows(&chain, event, &swap, None)
                .expect("rows")
                .into_iter()
                .map(|row| row.table)
                .collect::<Vec<_>>()
        };
        assert_eq!(rows((pool, (swap.selector, 3))), [SWAPS.table]);
        assert!(
            rows((pool, (swap.selector, 2))).is_empty(),
            "another topic count"
        );
        assert!(
            rows((factory, (swap.selector, 3))).is_empty(),
            "another contract"
        );
    }

    /// A record whose arguments the derivation cannot read derives nothing, rather than
    /// failing or guessing.
    #[test]
    fn an_unreadable_record_derives_nothing() {
        let chain = ChainId::new("base");
        let mut swap = swap(U256::ZERO, U256::ZERO, U256::ZERO);
        let event = (0, (swap.selector, 3));
        let mut derivations = Derivations::default();
        derivations.bind(SWAPS, event.0, [event.1]);
        let rows = |swap: &Decoded| derivations.rows(&chain, event, swap, None).expect("rows");
        assert_eq!(rows(&swap).len(), 1);
        swap.body[0].value = TypedValue::Bool { value: true };
        assert!(rows(&swap).is_empty());
    }

    /// A row that does not name its table's columns in order is refused as a bug in the
    /// derivation: one value short, one over, or two of one type swapped, rather than
    /// stored short, shifted, or with values under each other's columns.
    #[test]
    fn a_row_not_naming_its_columns_in_order_is_an_error() {
        fn short(decoded: &Decoded) -> Result<Vec<Row>, DeriveError> {
            let mut rows = (SWAPS.derive)(decoded)?;
            for row in &mut rows {
                row.pop();
            }
            Ok(rows)
        }
        fn swapped(decoded: &Decoded) -> Result<Vec<Row>, DeriveError> {
            let mut rows = (SWAPS.derive)(decoded)?;
            for row in &mut rows {
                row.swap(2, 3);
            }
            Ok(rows)
        }
        let chain = ChainId::new("base");
        let swap = swap(U256::ZERO, U256::ZERO, U256::ZERO);
        let declared: Vec<&str> = SWAPS.columns.iter().map(|&(name, _)| name).collect();
        let mut exchanged = declared.clone();
        exchanged.swap(2, 3);
        for (case, derivation, expected) in [
            (
                "a surplus value",
                Derivation {
                    columns: &SWAPS.columns[1..],
                    ..SWAPS
                },
                declared.clone(),
            ),
            (
                "a missing value",
                Derivation {
                    derive: short,
                    ..SWAPS
                },
                declared[..declared.len() - 1].to_vec(),
            ),
            (
                "amount0_raw and amount1_raw swapped",
                Derivation {
                    derive: swapped,
                    ..SWAPS
                },
                exchanged,
            ),
        ] {
            let event = (0, (swap.selector, 3));
            let mut derivations = Derivations::default();
            derivations.bind(derivation, event.0, [event.1]);
            match derivations.rows(&chain, event, &swap, None) {
                Err(DeriveError::Columns { table, names }) => {
                    assert_eq!((table, names), (SWAPS.table, expected), "{case}");
                }
                other => panic!("{case}: {other:?}"),
            }
        }
    }
}
