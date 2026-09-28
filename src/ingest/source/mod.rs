//! The ingestion boundary: where blocks come from.
//!
//! Everything chain-specific lives behind [`BlockSource`]. That trait deliberately
//! separates the two roles a data source plays, because they have different
//! latency and trust profiles:
//!
//! - [`BlockSource::subscribe_heads`] is the *live* path. It must be push-based to
//!   meet a sub-100ms budget.
//! - [`BlockSource::fetch_block`] is the *pull* path. It fetches one block with
//!   everything in it, which is what backfill and reorg reconciliation need.
//!
//! A source also owns two things the generic pipeline must not hardcode: how to
//! turn a chain's block into [`Event`]s, and what finality means for this chain,
//! reported as [`FetchedBlock::finalized`]. Adding Solana, Reth `ExEx`, or a Bitcoin source means
//! implementing this trait, not touching the pipeline.

use std::future::Future;
use std::pin::Pin;

use alloy_primitives::B256;
use futures_util::Stream;
use thiserror::Error;

pub mod evm;

pub use evm::EvmSource;

use crate::wire::envelope::{ChainId, Event};

/// A block's height and hash, without the rest of its contents.
///
/// Used for both a live head notification and the finalized-block watermark: in
/// each case the pipeline needs only where the block is, not what is in it. The
/// head's parent hash is not carried because linkage is checked on the fetched
/// block's marker, not on the notification that prompted the fetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockId {
    /// Block height, or slot on slot-based chains.
    pub height: u64,
    /// Block hash.
    pub hash: B256,
}

/// One block, already turned into events, plus the chain's finality at the time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedBlock {
    /// The block's events in publish order.
    ///
    /// Either empty, or led by [`Event::Block`]. The pipeline reads parent linkage
    /// from that marker to detect reorgs, so implementations must preserve it.
    pub events: Vec<Event>,
    /// The chain's newest finalized block when this block was fetched.
    ///
    /// Each chain defines finality its own way, so the source reports it rather
    /// than the pipeline counting confirmations.
    pub finalized: BlockId,
}

/// A stream of live head notifications. Ends when the connection does.
pub type HeadStream = Pin<Box<dyn Stream<Item = Result<BlockId, SourceError>> + Send>>;

/// The JSON-RPC code for a method the node does not serve.
///
/// Nodes differ on methods outside the Ethereum spec: some L2s and pre-Cancun
/// Ethereum answer `eth_getBlockReceipts` with this code, and some answer `null`.
pub const METHOD_NOT_FOUND: i64 = -32601;

/// Failure while talking to a chain data source.
#[derive(Debug, Error)]
pub enum SourceError {
    /// The connection, socket, or HTTP request failed.
    #[error("transport failure: {0}")]
    Transport(String),
    /// The node received the request but answered with a JSON-RPC error object.
    ///
    /// The code is kept typed rather than folded into the message, so a caller can
    /// branch on it (an unsupported method is [`METHOD_NOT_FOUND`]).
    ///
    /// [`METHOD_NOT_FOUND`]: crate::ingest::source::METHOD_NOT_FOUND
    #[error("{method} returned error {code}: {message}")]
    Rpc {
        /// The method the node rejected.
        method: String,
        /// The node's JSON-RPC error code.
        code: i64,
        /// The node's error message.
        message: String,
    },
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

    /// Fetches the block at `height` with everything in it, as events.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError::Transport`] when the request fails,
    /// [`SourceError::Rpc`] when a call in it was rejected, and
    /// [`SourceError::Malformed`] when the response cannot be decoded.
    fn fetch_block(
        &self,
        height: u64,
    ) -> impl Future<Output = Result<FetchedBlock, SourceError>> + Send;
}
