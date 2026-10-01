//! A low-latency, chain-agnostic blockchain indexer.
//!
//! One crate, one binary, four layers, each a module: chain data goes in through
//! [`ingest`], is decoded against a contract ABI by [`decode`] in the same call, crosses
//! one in-process channel, and is persisted by a [`sink`]. [`wire`] is the published
//! shape all of them agree on.
//!
//! ```text
//! ingest ─▶ decode ─▶ channel ─▶ store
//! ```
//!
//! Ingest and decode are direct calls in one task; the channel is the only queue, and it
//! exists so a stalled store does not stall ingest. See `sink::channel`.
//!
//! # Layering
//!
//! The dependency direction is one-way. [`ingest`] and [`decode`] each depend on
//! [`sink`] and [`wire`] and not on each other, so neither can reach into the
//! other's ordering or reorg state. The layers are modules rather than crates, so that
//! direction is a convention a reviewer checks rather than one the compiler enforces;
//! each module documents the direction it may depend in.
//!
//! # What works
//!
//! - EVM live heads over WebSocket and full blocks over JSON-RPC (see
//!   [`ingest::source::EvmSource`]).
//! - Per-chain sequence numbering, finality tagging, and reorg retraction with a
//!   bounded undo ring (see [`ingest::pipeline::Pipeline`]).
//! - Decoding a log against a contract ABI into typed, named arguments, as a
//!   stateless transform (see [`decode::Transform`]).
//! - A local `DuckDB` store, behind the `duckdb` feature.
//!
//! # Not built yet
//!
//! Resuming from the store's high-water mark after a restart, and rebuilding the undo
//! ring from it — the channel is not durable, so a crash loses what is in flight and the
//! store is what says where to pick up. Also backfill and the backfill-to-live handoff,
//! mempool, aggregation and windowing, filtered subscriptions, derived state, and the
//! Parquet archiver.

// The EVM source tests nest JSON objects deep enough to exceed the default macro
// recursion budget.
#![recursion_limit = "256"]

pub mod config;
pub mod decode;
pub mod ingest;
/// Absent without the `duckdb` feature: it assembles a store, so nothing in it can build
/// without the engine. The binary requires the feature too, so this only affects the
/// library's other consumers.
#[cfg(feature = "duckdb")]
pub mod runtime;
pub mod sink;
pub mod wire;
