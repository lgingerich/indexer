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
//! exists so a stalled store does not stall ingest. See `sink::channel`. [`runtime`]
//! assembles the four from the settings and runs them.
//!
//! # Layering
//!
//! The dependency direction is one-way. [`ingest`] and [`decode`] each depend on
//! [`sink`] and [`wire`] and not on each other, so neither can reach into the
//! other's ordering or reorg state. [`ingest`] and [`sink`] also depend on [`window`],
//! the undo window that ingest retracts within and the stores keep a ledger of, which
//! depends only on [`wire`], and [`wire`] on nothing. [`config`] sits outside the layers
//! and names each layer's settings, and [`sink`]'s stores read their own from it; a value
//! that must not be logged is a [`secret::Secret`], which any layer may take, and any
//! layer may record into [`metrics`]. [`runtime`] sits above them all. The layers are
//! modules rather than crates, so that direction is a convention a reviewer checks rather
//! than one the compiler enforces; each module documents the direction it may depend in.
//! CI checks the one between [`decode`] and [`ingest`] with a grep.
//!
//! # What works
//!
//! - EVM live heads over WebSocket and blocks, receipts, and logs over JSON-RPC (see
//!   [`ingest::source::EvmSource`]), fetching only the datasets the sinks' tables need
//!   — `eth_getLogs` over a range of heights in backfill when logs are all they need —
//!   or a backfill of the buried heights from Envio `HyperSync` (see
//!   [`ingest::source::HyperSyncSource`]).
//! - Per-chain ordering by each dataset's own natural key and reorg
//!   retraction with a bounded sliding undo window (see [`ingest::Machine`]), resumed
//!   from the stores' ledgers of committed blocks after a restart, every store brought
//!   to the newest block they all hold.
//! - Decoding a log against a contract ABI into typed, named arguments, as a
//!   stateless transform (see [`decode::Decoder`]), and deriving a table from one decoded
//!   event, a row per record, in the same step.
//! - Any combination of stores, each writing the tables it lists: an embedded `DuckDB`
//!   database (the `duckdb` feature), `PostgreSQL` over binary COPY (`postgres`), and a
//!   Delta Lake on S3-compatible storage, written as Parquet (`delta`); or the stream
//!   printed as NDJSON with no store. See [`sink`].
//! - Prometheus metrics for heads, commits, lag, and failures (see [`metrics`]).
//!
//! # Not built yet
//!
//! Mempool, re-decoding stored logs, streaming aggregation and windowing, filtered
//! subscriptions, cross-block transaction batching, and decoding by event signature
//! alone. The README's "Not built yet" has the detail.

// The EVM source tests nest JSON objects deep enough to exceed the default macro
// recursion budget.
#![recursion_limit = "256"]

pub mod config;
pub mod decode;
pub mod ingest;
pub mod metrics;
/// Assembles the configured ingest, decode, and sink pipeline.
pub mod runtime;
pub mod secret;
#[cfg(all(test, feature = "duckdb"))]
// Test code: a failed expectation means the harness itself is wrong.
#[expect(clippy::expect_used)]
mod sim;
pub mod sink;
pub mod window;
pub mod wire;
