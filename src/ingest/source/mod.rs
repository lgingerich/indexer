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
//!   reconciliation need. [`BlockSource::fetch_blocks`] is its buried-backfill form,
//!   which may fetch several consecutive heights at once.
//! - [`BlockSource::fetch_header`] reads one header, at a height or the head, for when
//!   only a block's identity is needed: sampling the head, and checking a restored tip.
//!
//! A source also owns how to turn a chain's block into [`Event`]s. It reports a
//! block's [`BlockMeta`] separately from those events, so parent linkage and the
//! block timestamp are available even when the `blocks` dataset is not stored: a
//! live notification carries enough metadata to reuse, and a fetched block carries
//! the authoritative header. Adding Solana, Reth `ExEx`, or a Bitcoin source means
//! implementing this trait, not touching the pipeline.

use std::future::Future;
use std::pin::Pin;

use futures_util::Stream;
use thiserror::Error;

mod datasets;
pub mod evm;
mod retry;
pub use datasets::Datasets;
pub use evm::EvmSource;
// The simulation builds its client the way `evm::http_client` does, over its own node.
#[cfg(all(test, feature = "duckdb"))]
pub(crate) use retry::{NetworkError, RetryLayer};

use crate::wire::envelope::{BlockMeta, ChainId, Event};

/// One block, already turned into events, with the metadata it was fetched under.
#[derive(Debug)]
pub struct FetchedBlock {
    /// The block's identity, parent hash, and timestamp.
    pub meta: BlockMeta,
    /// The block's events in publish order.
    ///
    /// Empty when no selected dataset produced a row — an empty block, or a
    /// logs-only block with no logs. The leading [`Event::Block`] appears only when
    /// the `blocks` dataset is stored; the pipeline reads linkage from [`Self::meta`],
    /// not from the events, so metadata is always present.
    pub events: Vec<Event>,
}

/// Live heads, including decode errors; ends after transport recovery is exhausted.
pub type HeadStream = Pin<Box<dyn Stream<Item = Result<BlockMeta, SourceError>> + Send>>;

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
    /// The node's answers disagree with each other — the chain moved mid-fetch, or a
    /// backend lags the block it announced — so, unlike [`Self::Malformed`], asking again
    /// can succeed.
    #[error("inconsistent data from {context}: {detail}")]
    Inconsistent {
        /// What was being read.
        context: String,
        /// Which answers disagreed.
        detail: String,
    },
}

/// A chain data source: one live subscription plus on-demand block fetches.
pub trait BlockSource: Send + Sync {
    /// The chain this source reads.
    fn chain(&self) -> &ChainId;

    /// Opens the live head subscription.
    ///
    /// A notification carries the announced block's [`BlockMeta`], which the fetch path
    /// may reuse to avoid a redundant header read. Notifications are hints, not a
    /// replay log: gaps are reconciled over HTTP.
    fn subscribe_heads(&self) -> impl Future<Output = Result<HeadStream, SourceError>> + Send;

    /// Fetches the block at `height` with the datasets this source was built for.
    ///
    /// `head` is metadata already known for that height, from a live notification.
    /// A source may reuse it when its selected datasets need no more than identity,
    /// parent hash, and timestamp; it must fetch a full block when they do, and must
    /// reject a fetched block whose identity disagrees with `head`.
    ///
    /// # Errors
    ///
    /// Returns a typed transport error or a violated projection invariant.
    fn fetch_block(
        &self,
        height: u64,
        head: Option<&BlockMeta>,
    ) -> impl Future<Output = Result<FetchedBlock, SourceError>> + Send;

    /// Fetches consecutive blocks from `from`, at most through `to`, in height order,
    /// with the datasets this source was built for.
    ///
    /// The buried-backfill path, where no fork can reach, so no notification is reused.
    /// The source chooses how many heights to return, but must return at least one, and
    /// none above `to`; a source that cannot fetch several heights at once returns one.
    /// All or nothing: an error means no block of the call was returned.
    ///
    /// # Errors
    ///
    /// Returns a typed transport error or a violated projection invariant.
    fn fetch_blocks(
        &self,
        from: u64,
        to: u64,
    ) -> impl Future<Output = Result<Vec<FetchedBlock>, SourceError>> + Send {
        let _ = to;
        async move { Ok(vec![self.fetch_block(from, None).await?]) }
    }

    /// One block's header as [`BlockMeta`], fetched on demand: the block at `height`, or
    /// the chain's head as it is right now when `height` is `None`.
    ///
    /// Reads the header alone — no transactions, receipts, or logs — so it is the
    /// cheap way to learn a block's identity. The head form is used for startup and
    /// live reconciliation, never during backfill: backfill captures its head once at
    /// startup and then reads concrete heights only. The height form checks a restored
    /// tip against the source before resuming.
    ///
    /// # Errors
    ///
    /// Returns a typed transport error, or a malformed-data error when the source has
    /// no block there.
    fn fetch_header(
        &self,
        height: Option<u64>,
    ) -> impl Future<Output = Result<BlockMeta, SourceError>> + Send;
}
