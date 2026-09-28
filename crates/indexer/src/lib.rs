//! A low-latency, chain-agnostic blockchain indexer.
//!
//! The crate ingests blocks from the live chain tip and publishes ordered,
//! finality-tagged events. Chain-specific knowledge is isolated behind
//! [`source::BlockSource`], and egress is isolated behind [`connectors::EventSink`], so
//! new chains and new brokers are additions rather than rewrites.
//!
//! The published shape lives in the [`wire`] crate rather than here: it is the
//! contract between this crate and whatever consumes the stream, so it is a
//! dependency both sides share rather than something an egress path owns.
//!
//! The large fixtures in the EVM source tests nest JSON objects deep enough to
//! exceed the default macro recursion budget.
#![recursion_limit = "256"]
//!
//! # What works today
//!
//! - EVM live heads over WebSocket (`eth_subscribe`/`newHeads`) and full blocks
//!   over JSON-RPC (see [`source::EvmSource`]).
//! - Per-chain sequence numbering, finality tagging, and reorg retraction with a
//!   bounded undo ring (see [`pipeline::Pipeline`]).
//! - Newline-delimited JSON to standard output (see [`connectors::StdoutJsonSink`]).
//! - A Kafka-protocol egress sink on `librdkafka`, behind the `kafka` feature
//!   (`connectors::kafka`).
//! - A local `DuckDB` store, behind the `duckdb` feature (`connectors::duckdb`).
//!
//! Both optional sinks are off by default so a plain `cargo build` neither
//! compiles C `librdkafka` nor the `DuckDB` C++ engine. Enable them with
//! `--features kafka,duckdb`; neither is wired into the binary yet.
//!
//! # Not built yet
//!
//! Mempool, ABI/IDL decoding, filtered subscriptions, derived state, backfill to
//! live handoff, checkpoint resume, and the Parquet archiver.

pub mod pipeline;
pub mod source;
