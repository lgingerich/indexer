//! The ingestion boundary: where blocks come from.
//!
//! Everything chain-specific lives behind [`BlockSource`]. That trait deliberately
//! separates the two roles a data source plays, because they have different
//! latency and trust profiles:
//!
//! - [`BlockSource::subscribe_heads`] is the *live* path. It must be push-based to
//!   meet a sub-100ms budget.
//! - [`BlockSource::fetch_block`] is the *pull* path. It fetches one block, limited
//!   to the datasets the source was built with, which is what backfill and reorg
//!   reconciliation need.
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

/// One block, already turned into events, plus the chain's finality when it was
/// fetched.
#[derive(Debug)]
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

/// Live heads, including decode errors; ends after transport recovery is exhausted.
pub type HeadStream = Pin<Box<dyn Stream<Item = Result<BlockId, SourceError>> + Send>>;

/// The JSON-RPC code for a method the node does not serve.
///
/// Nodes differ on methods outside the Ethereum spec: some L2s and pre-Cancun
/// Ethereum answer `eth_getBlockReceipts` with this code, and some answer `null`.
pub const METHOD_NOT_FOUND: i64 = -32601;

/// Failure while constructing or talking to a chain data source.
#[derive(Debug, Error)]
pub enum SourceError {
    /// HTTP client construction failed.
    #[error("http client failure: {0}")]
    Http(#[from] reqwest::Error),
    /// Alloy's typed RPC, decoding, or transport error at the named boundary.
    #[error("{context} failed: {source}")]
    Transport {
        /// The method or connection being attempted.
        context: &'static str,
        /// The typed error, including node error payloads and HTTP status codes.
        #[source]
        source: alloy_transport::TransportError,
    },
    /// A subscription notification could not be decoded.
    #[error("malformed data from {context}: {source}")]
    Json {
        /// What was being parsed.
        context: &'static str,
        /// The decoding error.
        #[source]
        source: serde_json::Error,
    },
    /// A decoded response violates a source invariant.
    #[error("malformed data from {context}: {detail}")]
    Malformed {
        /// What was being parsed.
        context: String,
        /// Which invariant failed.
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
    /// Returns a typed transport error or a violated projection invariant.
    fn fetch_block(
        &self,
        height: u64,
    ) -> impl Future<Output = Result<FetchedBlock, SourceError>> + Send;

    /// The chain's head as it is right now, fetched on demand.
    ///
    /// The live path hears about heads through [`BlockSource::subscribe_heads`], but a
    /// backfill runs for a long time and needs to ask where the chain has got to. Without
    /// this it would aim at a fixed height set when it started, and chase a target that no
    /// longer exists.
    ///
    /// # Errors
    ///
    /// Returns a typed transport error or an absent head.
    fn current_head(&self) -> impl Future<Output = Result<BlockId, SourceError>> + Send;
}
