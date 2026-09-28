//! Storage: the pipeline's terminal sink.
//!
//! Consumes every topic the pipeline produces to and persists the envelopes to a local
//! `DuckDB` database. Nothing downstream depends on how it drains, so it is the simplest
//! stage in the chain — consume, publish, flush, commit, with no transform in between.
//!
//! # Dependency direction
//!
//! This module may depend on [`crate::connectors`] and [`crate::wire`]. It is the end of
//! the chain, so it has no opinion about what produced what it stores.

pub mod stage;

pub use stage::Storage;
