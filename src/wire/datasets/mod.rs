//! Chain dataset records.
//!
//! A *dataset* is a durable on-chain record the indexer forwards — a block, a
//! transaction with its receipt, or a log. Each is one normalized table with a natural
//! key, so a row maps directly to a persistence row. Their shape is per-chain, so
//! the EVM records live in [`evm`]; another chain adds a sibling module and a
//! variant at the boundary rather than widening a shared type.

pub mod evm;
