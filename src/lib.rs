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
//! - Per-chain ordering by each dataset's own natural key and reorg
//!   retraction with a bounded sliding undo window (see [`ingest::Machine`]), resumed
//!   from the store's ledger of committed blocks after a restart.
//! - Decoding a log against a contract ABI into typed, named arguments, as a
//!   stateless transform (see [`decode::Decoder`]).
//! - A local `DuckDB` store of typed per-dataset tables, behind the `duckdb` feature.
//!
//! # Not built yet
//!
//! Mempool, cross-block batching and range logs, aggregation and windowing, filtered
//! subscriptions, derived state, and the Parquet archiver.

// The EVM source tests nest JSON objects deep enough to exceed the default macro
// recursion budget.
#![recursion_limit = "256"]

pub mod config;
pub mod decode;
pub mod ingest;
/// Assembles the configured ingest, decode, and sink pipeline.
pub mod runtime;
pub mod sink;
pub mod wire;
