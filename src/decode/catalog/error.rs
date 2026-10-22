//! Why a catalog could not be loaded.

use alloy_primitives::Address;

use super::super::abi::AbiError;
use super::CHAINS;
use crate::sink::table::{MAX_IDENTIFIER, TableError};

/// A failure to add `table`, an event table of `contract`: a repeated name keeps its own
/// variant, since two contracts naming one table is a manifest mistake to fix with a
/// `table` key.
pub(super) fn event_table_error(
    source: TableError,
    table: String,
    contract: String,
) -> CatalogError {
    match source {
        TableError::DuplicateTable { .. } => CatalogError::DuplicateTable { table, contract },
        source => CatalogError::Table { contract, source },
    }
}

/// Why the catalog could not be loaded.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    /// The protocols directory could not be read.
    #[error("read protocols directory {path}: {source}")]
    Directory {
        /// The directory.
        path: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// The protocols directory holds no `protocol.toml` anywhere in its tree, so the
    /// setting names the wrong directory.
    #[error("no protocol.toml under {path}")]
    NoManifests {
        /// The directory.
        path: String,
    },
    /// A manifest or ABI file could not be read.
    #[error("read {path}: {source}")]
    File {
        /// The file.
        path: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// A manifest did not parse, or a required value was absent.
    #[error("invalid manifest {path}: {source}")]
    Manifest {
        /// The manifest.
        path: String,
        /// The underlying TOML error.
        source: toml::de::Error,
    },
    /// An ABI file did not parse, or conflicts with another file of the same contract.
    #[error("invalid ABI at {path}: {source}")]
    Abi {
        /// The ABI file.
        path: String,
        /// The ABI failure.
        source: AbiError,
    },
    /// A contract lists no ABI file, so it has no name and nothing to decode with.
    #[error("{protocol} has a contract with no ABI file")]
    NoAbi {
        /// The protocol.
        protocol: String,
    },
    /// Two manifests name the same protocol.
    #[error("protocol {protocol:?} is declared twice; first in {first}")]
    DuplicateProtocol {
        /// The protocol.
        protocol: String,
        /// The manifest that declared it first.
        first: String,
    },
    /// One manifest has two contracts whose first ABI file has the same name.
    #[error("{protocol} declares contract {name:?} twice")]
    DuplicateContract {
        /// The protocol.
        protocol: String,
        /// The contract's name.
        name: String,
    },
    /// A contract lists addresses under a chain the indexer does not know: a typo, which
    /// would otherwise leave that chain without the contract's seeds.
    #[error("{contract} lists addresses on unknown chain {chain:?}; known chains are {CHAINS:?}")]
    UnknownChain {
        /// The contract, as `protocol.name`.
        contract: String,
        /// The chain as written.
        chain: String,
    },
    /// One address is listed under two contracts on the loaded chain.
    #[error("{address} is listed as both {first} and {second}")]
    DuplicateAddress {
        /// The address.
        address: Address,
        /// The first contract, as `protocol.name`.
        first: String,
        /// The second contract, as `protocol.name`.
        second: String,
    },
    /// A rule names a parent contract its protocol does not declare.
    #[error("{contract} is created_by unknown contract {parent:?}")]
    UnknownParent {
        /// The child contract, as `protocol.name`.
        contract: String,
        /// The parent as written.
        parent: String,
    },
    /// A rule's event is absent from the parent's ABI, or overloaded.
    #[error("{contract} is created_by event {event:?}, which the parent declares {found} times")]
    Event {
        /// The child contract, as `protocol.name`.
        contract: String,
        /// The event as written.
        event: String,
        /// How many events by that name the parent declares; exactly one is required.
        found: usize,
    },
    /// An event table's name is longer than `PostgreSQL`'s identifier limit.
    #[error(
        "event table {table} is longer than {MAX_IDENTIFIER} bytes; give {contract} a shorter `table`"
    )]
    TableName {
        /// The table.
        table: String,
        /// The contract, as `protocol.name`.
        contract: String,
    },
    /// An event table's name is produced twice.
    #[error(
        "event table {table} already exists when adding {contract}: two contracts share a \
         `table`, or the contract declares two events by one name"
    )]
    DuplicateTable {
        /// The table.
        table: String,
        /// The contract that produced it again, as `protocol.name`.
        contract: String,
    },
    /// A dataset table's declaration is inconsistent: a bug in the indexer, not in a
    /// manifest.
    #[error("the dataset tables: {0}")]
    Datasets(#[source] TableError),
    /// A table generated for a contract's events is inconsistent.
    #[error("{contract}: {source}")]
    Table {
        /// The contract whose events the table holds, as `protocol.name`.
        contract: String,
        /// What is wrong with the table.
        source: TableError,
    },
    /// A rule's parameter is absent from its event, or not an `address`.
    #[error("{contract} is created_by {event}.{param}, which is not an address argument")]
    Param {
        /// The child contract, as `protocol.name`.
        contract: String,
        /// The event.
        event: String,
        /// The parameter as written.
        param: String,
    },
}
