//! Ingestion: chain data in, ordered events out.
//!
//! - [`source`] knows one chain: how to hear about heads and fetch a block.
//! - [`pipeline`] drives a source: it turns its blocks into one ordered stream and
//!   hands each envelope to a sink.
//!
//! # Dependency direction
//!
//! This module depends on [`crate::sink`] for the sink it publishes to,
//! [`crate::wire`] for what it publishes, [`crate::window`] for the undo window and the
//! ledger it resumes from, and [`crate::metrics`]. Nothing below it depends on it, and it
//! knows nothing about [`crate::decode`]: what happens to an envelope after it is
//! published is not ingestion's business.

pub mod pipeline;
pub mod source;

pub use pipeline::{Machine, PipelineError};
pub use source::{BlockSource, EvmSource};
