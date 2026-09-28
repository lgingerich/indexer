//! A low-latency, chain-agnostic blockchain indexer.
//!
//! The crate is a walking skeleton: it ingests blocks from the live chain tip and
//! publishes ordered, finality-tagged events. Chain-specific knowledge is isolated
//! behind [`source::BlockSource`], and egress is isolated behind
//! [`sink::EventSink`], so new chains and new brokers are additions rather than
//! rewrites.
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
//! - Newline-delimited JSON to standard output (see [`sink::StdoutJsonSink`]).
//!
//! # Not built yet
//!
//! Mempool, ABI/IDL decoding, filtered subscriptions, derived state, backfill to
//! live handoff, checkpoint resume, the Redpanda sink, and the Parquet archiver.

pub mod datasets;
pub mod envelope;
pub mod pipeline;
pub mod sink;
pub mod source;
