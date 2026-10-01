//! The wire contract: what the indexer publishes, and nothing about how.
//!
//! Pure data. It does no I/O, holds no async, and depends on no other module in the
//! crate, so the code that produces events and the code that consumes them share one
//! definition of the stream.
//!
//! - [`envelope`] is the published shape: an [`envelope::Envelope`] wrapping an
//!   [`envelope::Event`], with the schema version.
//! - [`datasets`] are the durable records an event may carry, one normalized table
//!   per dataset.
//! - [`row`] renders a dataset as the storage-agnostic rows a store persists, so the
//!   mapping from a log to its columns is decided once rather than per store.
//! - [`typed`] is the typed form of a decoded ABI argument, which is what a decoded
//!   record carries instead of opaque bytes.

pub mod datasets;
pub mod envelope;
pub mod row;
pub mod typed;
