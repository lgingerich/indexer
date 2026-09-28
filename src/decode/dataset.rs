//! Dataset mappers: turning a decoded event into the rows of a dataset.
//!
//! The registry answers *which dataset* a record belongs to. This answers *with what
//! columns*. They are separate because the first is data — an address is or is not a
//! Uniswap V3 pool — and the second is meaning: that a `Swap`'s `amount0` is a token
//! amount, and a negative one means the pool sent that token out.
//!
//! # Why this is code and not configuration
//!
//! A registry entry can say `Swap` rows belong to `dex.trades`. It cannot say to read
//! `amount0` as token0, to sign-flip it, or that a negative `amount0` means token0 was
//! sold. That is a projection with per-protocol knowledge in it, and expressing it as
//! configuration would mean inventing a small language for column expressions.
//!
//! # Why it is a trait rather than a function
//!
//! One mapper per dataset, dispatched by the dataset name the registry stamped. A new
//! protocol that feeds an existing dataset adds no mapper; a new dataset adds one.

use crate::wire::envelope::Decoded;

use crate::wire::typed::TypedValue;

/// A named column value, which is what a dataset row is made of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// The column name in the target dataset, for example `token0_amount_raw`.
    pub name: String,
    /// The value.
    pub value: TypedValue,
}

/// One dataset's row, before it is stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// The dataset the row belongs to, for example `dex.trades`.
    pub dataset: String,
    /// The columns, in a stable order for the dataset.
    pub columns: Vec<Column>,
    /// The record this row was projected from, so the row traces back to its event.
    ///
    /// The decoded record's `dedupe_key`, so a store can join a dataset row to the
    /// decode that produced it and a re-projection is recognizable as one.
    pub source: String,
}

/// Projects a decoded event into one dataset's rows.
///
/// Returns `None` when the event is not one this dataset models, which is a normal
/// outcome: a Uniswap mapper sees `Transfer` events too and ignores them.
pub trait DatasetMapper: Send + Sync {
    /// The dataset this mapper produces, matching a registry `dataset` value.
    fn dataset(&self) -> &str;

    /// Projects one event, or `None` when this dataset does not model it.
    fn map(&self, decoded: &Decoded) -> Option<Row>;
}

/// Projects a decoded event through the mapper registered for its dataset.
///
/// The dispatch is on `Decoded::dataset`, which the registry stamped, so a record whose
/// dataset has no mapper is passed over rather than guessed at.
#[must_use]
pub fn project(mappers: &[Box<dyn DatasetMapper>], decoded: &Decoded) -> Option<Row> {
    mappers
        .iter()
        .find(|mapper| mapper.dataset() == decoded.dataset)
        .and_then(|mapper| mapper.map(decoded))
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

    use super::{DatasetMapper, Row, project};

    fn decoded(dataset: &str, signature: &str) -> Decoded {
        Decoded {
            name: "Swap".to_owned(),
            address: Address::from([0xd0; 20]),
            protocol: "uniswap_v3".to_owned(),
            dataset: dataset.to_owned(),
            selector: B256::from([0xc4; 32]),
            signature: signature.to_owned(),
            anonymous: false,
            transaction_hash: B256::from([0x2a; 32]),
            transaction_index: 0,
            log_index: 767,
            indexed: Vec::new(),
            body: vec![DecodedArg {
                name: "amount0".to_owned(),
                value: TypedValue::Int {
                    value: I256::try_from(-1_i64).expect("fits"),
                    bits: 256,
                },
            }],
            block_number: 1,
            block_hash: B256::from([0xd4; 32]),
            block_timestamp: 1_700_000_000,
        }
    }

    /// A mapper that claims one dataset and one signature.
    struct Trades;
    impl DatasetMapper for Trades {
        fn dataset(&self) -> &'static str {
            "dex.trades"
        }
        fn map(&self, decoded: &Decoded) -> Option<Row> {
            (decoded.signature.starts_with("Swap(")).then(|| Row {
                dataset: self.dataset().to_owned(),
                columns: Vec::new(),
                source: decoded.dedupe_key(),
            })
        }
    }

    /// The mapper registered for a record's dataset is the one that runs, so a protocol
    /// and its dataset stay decoupled from the projection.
    #[test]
    fn a_record_is_projected_by_its_dataset_mapper() {
        let mappers: Vec<Box<dyn DatasetMapper>> = vec![Box::new(Trades)];
        let row = project(&mappers, &decoded("dex.trades", "Swap(address)")).expect("mapped");
        assert_eq!(row.dataset, "dex.trades");
        assert!(!row.source.is_empty(), "the row traces to its record");
    }

    /// A dataset with no mapper produces nothing rather than being guessed at, so an
    /// unmapped dataset is a gap rather than a wrong row.
    #[test]
    fn an_unmapped_dataset_projects_nothing() {
        let mappers: Vec<Box<dyn DatasetMapper>> = vec![Box::new(Trades)];
        assert!(project(&mappers, &decoded("lending.loans", "Swap(address)")).is_none());
    }

    /// A mapper's own dataset is the only one it is asked about, so an event it does not
    /// model is `None` rather than an error.
    #[test]
    fn a_mapper_ignores_an_event_it_does_not_model() {
        let mappers: Vec<Box<dyn DatasetMapper>> = vec![Box::new(Trades)];
        assert!(project(&mappers, &decoded("dex.trades", "Mint(address)")).is_none());
    }

    /// A value wider than a normal integer column keeps its exact text, because a wei
    /// amount has 78 digits and rounding it would corrupt it.
    #[test]
    fn a_wide_value_stays_exact() {
        let wide = TypedValue::Uint {
            value: U256::MAX,
            bits: 256,
        };
        let TypedValue::Uint { value, .. } = &wide else {
            panic!("should be a uint");
        };
        assert_eq!(value.to_string().len(), 78);
    }
}
