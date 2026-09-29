//! Ingestion: chain data in, ordered events out.
//!
//! - [`source`] knows one chain: how to hear about heads and fetch a block.
//! - [`pipeline`] is the only stateful part: it turns a source's events into one
//!   ordered stream and hands each envelope to a sink.
//! - [`run`] is the runtime that drives the two together.
//!
//! # Dependency direction
//!
//! This module may depend on [`crate::sink`] and [`crate::wire`], and nothing
//! else. In particular it knows nothing about [`crate::decode`]:
//! what happens to an envelope after it is published is not ingestion's business.

pub mod pipeline;
pub mod run;
pub mod source;

pub use pipeline::Pipeline;
pub use run::Ingest;
pub use source::{BlockSource, EvmSource};
