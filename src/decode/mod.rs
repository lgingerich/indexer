//! Decoding a log against a contract ABI.
//!
//! - [`registry`] answers which ABI applies to a log at a height.
//! - [`convert`] is the only place that knows alloy's dynamic value model.
//! - [`transform`] is the stateless transform: one envelope in, its decoded records out.
//! - [`stage`] is the runtime that drives it off a broker.
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
pub mod dataset;
pub mod registry;
#[cfg(feature = "kafka")]
pub mod stage;
pub mod transform;
pub mod uniswap_v3;

pub use convert::ConversionError;
pub use registry::{Abi, AbiRegistry, FileRegistry, RawLog, RegistryError};
#[cfg(feature = "kafka")]
pub use stage::Decode;
pub use transform::Transform;
