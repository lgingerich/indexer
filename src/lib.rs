//! A low-latency, chain-agnostic blockchain indexer.
//!
//! One crate, one binary, four layers, each a module: chain data goes in through
//! [`ingest`], out to a bus through [`connectors`], is decoded against a contract ABI by
//! [`decode`], and is persisted through [`connectors`]. [`wire`] is the published shape
//! all of them agree on.
//!
//! # Layering
//!
//! The dependency direction is one-way. [`ingest`] and [`decode`] each depend on
//! [`connectors`] and [`wire`] and not on each other, so neither can reach into the
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
//! - Kafka-protocol connectors both ways, behind the `kafka` feature.
//! - A local `DuckDB` store, behind the `duckdb` feature.
//!
//! # Not built yet
//!
//! Mempool, aggregation and windowing, filtered subscriptions, derived state, backfill
//! to live handoff, checkpoint resume, and the Parquet archiver.

// The EVM source tests nest JSON objects deep enough to exceed the default macro
// recursion budget.
#![recursion_limit = "256"]

pub mod config;
pub mod connectors;
pub mod decode;
pub mod ingest;
pub mod runtime;
pub mod wire;
