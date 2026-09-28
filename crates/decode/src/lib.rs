//! Decodes raw indexer events against a contract ABI.
//!
//! This crate is the second stage of the pipeline: it consumes the raw envelopes
//! the indexer publishes, decodes what it has an ABI for, and republishes. It is a
//! *stateless transform*, which is the property the whole design rests on — it
//! preserves each source envelope's `chain`, `sequence`, and `dedupe_key`, and it
//! reimplements none of the ordering or reorg logic that lives in the indexer's
//! pipeline. That crate is deliberately not a dependency here, so the
//! decode crate cannot reach into the pipeline's state machine even by accident.
//!
//! Because the transform is a pure function of its input, at-least-once redelivery
//! is safe: replaying the same record emits the same decoded records with the same
//! `dedupe_key`, and a store that upserts on that key is idempotent.
//!
//! # Control signals pass through
//!
//! [`Event::Reorg`](wire::envelope::Event::Reorg) and
//! [`Event::Finalized`](wire::envelope::Event::Finalized) are republished verbatim,
//! never swallowed. They are the only thing that lets a downstream store retract
//! orphaned rows or compact below a finality watermark; dropping either would make
//! the decoded stream quietly wrong rather than incomplete.
//!
//! Not built yet: the broker source and sink that drive the transform, and a
//! proxy-aware registry backed by a store rather than an in-process map.

pub mod convert;
pub mod registry;
pub mod transform;

pub use convert::ConversionError;
pub use registry::{Abi, AbiRegistry, RawLog, RegistryError};
pub use transform::Transform;
