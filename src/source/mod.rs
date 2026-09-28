//! The ingestion boundary: where blocks come from.
//!
//! Everything chain-specific lives behind [`BlockSource`]. That trait deliberately
//! separates the two roles a data source plays, because they have different
//! latency and trust profiles:
//!
//! - [`BlockSource::subscribe_heads`] is the *live* path. Rarity here is the point:
//!   it must be push-based to meet a sub-100ms budget.
//! - [`BlockSource::block_at`] is the *pull* path. It fetches a full block and its
//!   receipts on demand, which is what backfill and reorg reconciliation need.
//!
//! A source also owns two things the generic pipeline must not hardcode: what
//! finality means for this chain, reported as [`RawBlock::finalized`], and how to
//! turn a raw block into [`Event`]s. Adding Solana, Reth `ExEx`, or a Bitcoin source means
//! implementing this trait, not touching the pipeline.

use std::future::Future;
use std::pin::Pin;

use alloy_primitives::B256;
use futures_util::Stream;
use thiserror::Error;

pub mod evm;

pub use evm::EvmSource;

use crate::envelope::{ChainId, Event};

/// A live head notification: the block the chain is currently building on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Head {
    /// Height of the new head, or slot on slot-based chains.
    pub height: u64,
    /// Hash of the new head.
    pub hash: B256,
    /// Hash of the block the new head builds on.
    pub parent_hash: B256,
}

/// A block's height and hash, without the rest of its contents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockId {
    /// Block height, or slot on slot-based chains.
    pub height: u64,
    /// Block hash.
    pub hash: B256,
}

/// A block and its transaction receipts, exactly as the source returned them.
///
/// The payloads stay unparsed here so a [`BlockSource`] can hand them to
/// [`BlockSource::encode_block`] without this layer knowing the chain's shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawBlock {
    /// Verbatim block payload.
    pub raw_block: String,
    /// Verbatim receipts payload for the block's transactions.
    pub raw_receipts: String,
    /// The chain's newest finalized block when this block was fetched.
    ///
    /// Each chain defines finality its own way, so the source reports it rather
    /// than the pipeline counting confirmations.
    pub finalized: BlockId,
}

/// A stream of live head notifications. Ends when the connection does.
pub type HeadStream = Pin<Box<dyn Stream<Item = Result<Head, SourceError>> + Send>>;

/// Failure while talking to a chain data source.
#[derive(Debug, Error)]
pub enum SourceError {
    /// The connection, socket, or HTTP request failed.
    #[error("transport failure: {0}")]
    Transport(String),
    /// The source responded, but the payload was not usable.
    #[error("malformed data from {context}: {detail}")]
    Malformed {
        /// What was being parsed, for example a method name.
        context: String,
        /// Why the payload could not be used.
        detail: String,
    },
}

/// A chain data source: one live subscription plus on-demand block fetches.
pub trait BlockSource: Send + Sync {
    /// The chain this source reads.
    fn chain(&self) -> &ChainId;

    /// Opens the live head subscription.
    fn subscribe_heads(&self) -> impl Future<Output = Result<HeadStream, SourceError>> + Send;

    /// Fetches a block, its receipts, and the chain's current finalized block.
    fn block_at(&self, height: u64) -> impl Future<Output = Result<RawBlock, SourceError>> + Send;

    /// Turns a raw block into its ordered events, starting with the block marker.
    ///
    /// The returned `Vec` is empty, or its first element is
    /// [`Event::Block`]. The pipeline relies on that to read parent linkage for
    /// reorg detection, so implementations must preserve it.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError::Malformed`] when the payload is not decodable.
    fn encode_block(&self, raw: &RawBlock) -> Result<Vec<Event>, SourceError>;
}
