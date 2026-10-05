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
// The error type and the close code, so [`SourceError`] can carry them typed rather than
// flattened to a message. Re-exported through `tokio_tungstenite`, which is already a
// dependency for the EVM source's socket.
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

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
    /// An HTTP request failed, carrying `reqwest`'s typed error.
    ///
    /// The variant to match when deciding whether to retry: [`reqwest::Error::status`]
    /// tells a rate limit from a 5xx, and [`reqwest::Error::is_timeout`] a stall from a
    /// refusal. This is why the error is not stringified — as a `String` the only thing
    /// left to match on was the message, so no retry policy could be written against
    /// this error at all.
    #[error("http failure: {0}")]
    Http(#[from] reqwest::Error),
    /// A websocket frame failed to send or receive.
    ///
    /// Typed for the same reason as [`Self::Http`]: `tungstenite::Error` is `Send + Sync`,
    /// so there is nothing to gain by flattening it. The case a caller most wants to
    /// branch on — the peer going away — is [`Self::Closed`] instead, since a closure is
    /// not a frame error but a normal end of stream.
    ///
    /// `context` is set at the call (`connect`, `subscribe`, `read frame`), so this is
    /// not `#[from]` — a `From<tungstenite::Error>` would have to drop which attempt failed.
    #[error("websocket {context} failed: {source}")]
    Websocket {
        /// What was being attempted, `connect`, `subscribe`, or `read frame`.
        context: &'static str,
        /// `tungstenite`'s error, typed.
        #[source]
        source: tungstenite::Error,
    },
    /// The peer sent a close frame, ending the head subscription.
    ///
    /// Carries the close code, because *how* the peer hung up is the difference between
    /// a reconnect and a diagnosis. `Normal` and `Away` mean the node finished or is
    /// restarting and the same socket should be reopened; `Protocol` and `Error` mean it
    /// rejected something we sent and reconnecting unchanged fails the same way.
    ///
    /// The code is the type rather than an `Option`, because a close frame carrying no
    /// payload is the case RFC 6455 gives its own code — `Status` — and so needs no
    /// sentinel here. A connection dropped with *no close frame at all* is a different
    /// thing: it ends the stream without producing an item, and the caller reports that as
    /// an ended subscription instead.
    ///
    /// [`CloseCode::Normal`]: tungstenite::protocol::frame::coding::CloseCode::Normal
    /// [`CloseCode::Away`]: tungstenite::protocol::frame::coding::CloseCode::Away
    /// [`CloseCode::Protocol`]: tungstenite::protocol::frame::coding::CloseCode::Protocol
    /// [`CloseCode::Error`]: tungstenite::protocol::frame::coding::CloseCode::Error
    /// [`CloseCode::Status`]: tungstenite::protocol::frame::coding::CloseCode::Status
    #[error("websocket closed ({code})")]
    Closed {
        /// The peer's close code, which displays as its RFC 6455 number — `1000` for a
        /// normal closure.
        code: CloseCode,
    },
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
    /// A payload was not JSON of the shape being decoded.
    ///
    /// `context` names which payload failed, which [`serde_json::Error`] cannot know, so
    /// this is not `#[from]`.
    #[error("malformed data from {context}: {source}")]
    Json {
        /// What was being parsed, for example a method name.
        context: &'static str,
        /// `serde_json`'s error, typed.
        #[source]
        source: serde_json::Error,
    },
    /// The source responded, but the payload was not usable.
    ///
    /// Distinct from [`Self::Json`]: the body parsed, and then failed a check this
    /// source owns (a null result, a log with no index, a receipt from another block).
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
    /// Returns [`SourceError::Http`] when the request fails, [`SourceError::Rpc`] when a
    /// call in it was rejected, and [`SourceError::Json`] or [`SourceError::Malformed`]
    /// when the response cannot be decoded.
    fn fetch_block(
        &self,
        height: u64,
    ) -> impl Future<Output = Result<FetchedBlock, SourceError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::SourceError;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    /// A clean shutdown and a protocol error used to be indistinguishable: both ended
    /// the head stream, and the run reported only that it stopped. The close code is
    /// what separates "reconnect" from "something is wrong", so it has to survive into
    /// the error a caller sees.
    #[test]
    fn a_close_carries_the_code_that_says_why() {
        let orderly = SourceError::Closed {
            code: CloseCode::Normal,
        };
        let fault = SourceError::Closed {
            code: CloseCode::Protocol,
        };

        assert!(
            matches!(
                orderly,
                SourceError::Closed {
                    code: CloseCode::Normal
                }
            ),
            "a normal close must stay distinguishable from a protocol error: {orderly}"
        );
        assert!(
            matches!(
                fault,
                SourceError::Closed {
                    code: CloseCode::Protocol
                }
            ),
            "a protocol error must stay distinguishable from a normal close: {fault}"
        );
        // The code renders as its RFC 6455 number, so an operator reading the log can
        // look it up without matching in code.
        assert_eq!(orderly.to_string(), "websocket closed (1000)");
        assert_eq!(fault.to_string(), "websocket closed (1002)");
    }
}
