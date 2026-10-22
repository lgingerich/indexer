//! Ingestion: chain data in, ordered events out.
//!
//! - [`source`] knows one chain: how to hear about heads and fetch a block.
//! - [`pipeline`] drives a source: it turns its blocks into one ordered stream and
//!   hands each envelope to a sink.
//!
//! # Dependency direction
//!
//! This module depends on [`crate::sink`] for the sink it publishes to,
//! [`crate::wire`] for what it publishes, and [`crate::metrics`]. It knows nothing
//! about [`crate::decode`]: what happens to an envelope after it is published is not
//! ingestion's business.
//!
//! The direction is not yet one way: [`crate::sink`] and [`crate::config`] import
//! [`LEDGER_WINDOW`](pipeline::LEDGER_WINDOW),
//! [`MAX_UNFINALIZED_BLOCKS`](pipeline::MAX_UNFINALIZED_BLOCKS), and
//! [`Datasets`](source::Datasets) from here, and [`crate::sink::resume`] builds the
//! [`Ledger`](pipeline::Ledger) a resumed run starts from.

pub mod pipeline;
pub mod source;

pub use pipeline::{Machine, PipelineError};
pub use source::{BlockSource, EvmSource};
