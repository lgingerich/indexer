//! Decoding a log against a contract ABI.
//!
//! - [`registry`] loads one contract's ABI and decodes a log against it.
//! - [`contracts`] answers which contract applies to a log, and what it is.
//! - [`convert`] is the only place that knows alloy's dynamic value model.
//! - [`transform`] is the stateless transform: one envelope in, its decoded records out.
//! - [`stage`] is the runtime that drives it off a source.
//!
//! # What this deliberately does not do
//!
//! It produces *facts*: an argument's name and its typed value, straight off the ABI.
//! It does not reshape them into a dataset, because that needs context this stage does
//! not have — which token a pool trades, how many decimals it has, which table a swap
//! belongs to. Those are joins against reference data, so the projection belongs where
//! the joins are.
//!
//! That is why there is no per-protocol mapper here. A decoder that knew what `amount0`
//! *means* would be a decoder that has to know every protocol.
//!
//! # Dependency direction
//!
//! This module may depend on [`crate::connectors`] and [`crate::wire`]. It deliberately
//! knows nothing about [`crate::ingest`]: the ordering and reorg state machine lives
//! there, and a decode that needs it would be a decode that has to reimplement it.
//! Nothing enforces that now that these are modules rather than crates, so it is on a
//! reviewer.

pub mod contracts;
pub mod convert;
pub mod registry;
#[cfg(all(feature = "kafka", feature = "duckdb"))]
pub mod stage;
pub mod transform;

pub use convert::ConversionError;
pub use registry::{Abi, AbiRegistry, Contract, DecodedEvent, RegistryError};
#[cfg(all(feature = "kafka", feature = "duckdb"))]
pub use stage::{Decode, DecodeBuilder};
pub use transform::{Applied, Transform};
