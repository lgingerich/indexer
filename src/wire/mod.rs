//! The wire contract: what the indexer publishes, and nothing about how.
//!
//! Pure data. It does no I/O, holds no async, and depends on no other module in the
//! crate, so both the code that produces events and any code that consumes them can
//! share one definition of the stream.
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
