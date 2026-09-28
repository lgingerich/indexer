//! A low-latency, chain-agnostic blockchain indexer.
//!
//! One crate, one binary, four layers, each a module: the chain data goes in through
//! [`ingest`], out to a bus through [`connectors`], is decoded against a contract ABI
//! by [`decode`], and is persisted through [`connectors`]. [`wire`] is the published
//! shape all of them agree on.
//!
//! # Why modules rather than crates
//!
//! The layers were separate crates, which bought one thing: `decode` structurally could
//! not depend on `ingest`, so it could not reach into the pipeline's ordering and reorg
//! state machine. That guarantee is now a convention rather than something the compiler
//! enforces — reaching into [`ingest::pipeline`] from [`decode`] compiles.
//!
//! The trade is deliberate. Breaking the `decode` → `ingest` cycle was what forced the
//! published shape out into its own crate, and the shared bus into another, and four
//! manifests, and feature forwarding between them. That is real structure for a
//! guarantee worth about one edge, at a scale where one owner reads all of it. The
//! dependency direction is documented per module instead, and a reviewer can check it.
//!
//! # What works today
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
pub mod wire;
