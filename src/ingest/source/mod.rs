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
//!   which fetches a range of consecutive heights; backfill keeps several ranges in
//!   flight at once.
//! - [`BlockSource::fetch_header`] reads one header, at a height or the head, for when
//!   only a block's identity is needed: sampling the head, and checking a restored tip.
//!
//! A source also owns how to turn a chain's block into [`Event`]s. It reports a
//! block's [`BlockMeta`] separately from those events, so parent linkage and the
//! block timestamp are available even when the `blocks` dataset is not stored: a
//! live notification carries enough metadata to reuse, and a fetched block carries
//! the authoritative header. Adding Solana, Reth `ExEx`, or a Bitcoin source means
//! implementing this trait, not touching the pipeline.
//!
//! Two EVM sources implement it: [`EvmSource`] over a node's JSON-RPC, which follows the
//! chain, and [`HyperSyncSource`] over Envio `HyperSync`, which only backfills.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use futures_util::Stream;
use thiserror::Error;

mod any_source;
mod assemble;
mod datasets;
pub mod evm;
pub mod hypersync;
mod retry;
pub use any_source::AnySource;
pub use datasets::Datasets;
pub use evm::EvmSource;
pub use hypersync::{HyperSyncRanges, HyperSyncSource};
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
    /// The HTTP client failed: it could not be built, or a request did not complete.
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
    /// The source answered a request with an HTTP error status: one asking again will
    /// not change, such as a refused API token, or a transient one every retry got.
    #[error("{context} answered HTTP {status}: {detail}")]
    Status {
        /// What was being read.
        context: &'static str,
        /// The HTTP status code.
        status: u16,
        /// The response body, truncated.
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

/// A source invariant failed: the response at `context` cannot be right, so asking again
/// would fail the same way.
pub(crate) fn malformed(context: &str, detail: impl fmt::Display) -> SourceError {
    SourceError::Malformed {
        context: context.to_owned(),
        detail: detail.to_string(),
    }
}

/// The source's answers at `context` disagree with each other, so asking again can
/// succeed.
pub(crate) fn inconsistent(context: &str, detail: impl fmt::Display) -> SourceError {
    SourceError::Inconsistent {
        context: context.to_owned(),
        detail: detail.to_string(),
    }
}

/// A chain data source: one live subscription plus on-demand block fetches.
pub trait BlockSource: Send + Sync {
    /// The chain this source reads.
    fn chain(&self) -> &ChainId;

    /// Whether the source can follow the chain: announce heads through
    /// [`Self::subscribe_heads`], and answer at the tip, where a fork can still reach.
    ///
    /// A source that cannot only backfills: a run on it indexes the buried heights and
    /// stops, and leaves the rest to a run on one that can. It also refuses to resume
    /// past a stored tip it disagrees with, since it has no view of the fork that would
    /// retract it.
    fn follows_heads(&self) -> bool {
        true
    }

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

    /// Fetches the blocks `from ..= to`, in height order, with the datasets this source
    /// was built for.
    ///
    /// The buried-backfill path, where no fork can reach, so no notification is reused.
    /// Every height of the range is returned, however the source splits it into
    /// requests; backfill asks for [`Self::backfill_span`] heights at a time. All or
    /// nothing: an error means no block of the call was returned.
    ///
    /// # Errors
    ///
    /// Returns a typed transport error or a violated projection invariant.
    fn fetch_blocks(
        &self,
        from: u64,
        to: u64,
    ) -> impl Future<Output = Result<Vec<FetchedBlock>, SourceError>> + Send {
        async move {
            let mut blocks = Vec::new();
            for height in from..=to {
                blocks.push(self.fetch_block(height, None).await?);
            }
            Ok(blocks)
        }
    }

    /// How many heights one [`Self::fetch_blocks`] call should cover right now.
    ///
    /// Read each time backfill cuts a range, so a source that learns its provider's
    /// limits can change it as it goes. At least one; a source that fetches a height at a
    /// time keeps the default.
    fn backfill_span(&self) -> u64 {
        1
    }

    /// How many blocks under the head a fork may still reach on this source's chain: the
    /// buried heights, below it, are backfilled straight through, and only those above
    /// it go block by block through the reorg-aware path. At most
    /// [`MAX_UNFINALIZED_BLOCKS`](crate::ingest::pipeline::MAX_UNFINALIZED_BLOCKS), the
    /// deepest fork a run can retract, which is the default.
    fn finality_depth(&self) -> u64 {
        crate::ingest::pipeline::MAX_UNFINALIZED_BLOCKS as u64
    }

    /// How many [`Self::fetch_blocks`] calls backfill keeps in flight at once.
    ///
    /// Blocks are accepted in height order whatever this is; it only overlaps requests.
    /// At least one.
    fn backfill_concurrency(&self) -> usize {
        1
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
