//! The wire contract: what the indexer publishes, and nothing about how.
//!
//! This crate is pure data. It does no I/O, holds no async, and depends on no
//! other crate in the workspace, so both the process that produces events and any
//! process that consumes them can share one definition of the stream without
//! sharing a runtime.
//!
//! - [`envelope`] is the published shape: an [`envelope::Envelope`] wrapping an
//!   [`envelope::Event`], with the schema version and the per-chain sequence.
//! - [`datasets`] are the durable records an event may carry, one normalized table
//!   per dataset.
//! - [`typed`] is the typed form of a decoded ABI argument, which is what a decoded
//!   record carries instead of opaque bytes.

pub mod datasets;
pub mod envelope;
pub mod typed;
