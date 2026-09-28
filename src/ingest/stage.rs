//! Running the ingest stage: one source into one sink.
//!
//! The builder is the whole surface. A caller names a chain, its endpoints, and where
//! the events go, and gets a future that runs until the source ends. Nothing here reads
//! the environment — that is [`crate::config`]'s job, so this is constructible in a test.

use anyhow::Result;
use tracing::info;

use crate::connectors::EventSink;
use crate::ingest::pipeline::Pipeline;
use crate::ingest::source::EvmSource;

/// Builds and runs the ingest stage.
#[derive(Debug)]
pub struct Ingest {
    chain: String,
    http_url: String,
    ws_url: String,
    undo_depth: usize,
}

impl Ingest {
    /// Starts a build for `chain`, whose endpoints are set on the builder.
    #[must_use]
    pub fn builder(chain: impl Into<String>) -> IngestBuilder {
        IngestBuilder::new(chain)
    }

    /// Follows the chain's live tip, publishing through `sink` until it ends.
    ///
    /// # Errors
    ///
    /// Returns an error when the subscription, a block fetch, or a publish fails, and
    /// when the subscription closes — a live indexer should never stop.
    pub async fn run<S: EventSink>(self, sink: S) -> Result<()> {
        info!(
            chain = %self.chain,
            http = %self.http_url,
            ws = %self.ws_url,
            undo_depth = self.undo_depth,
            "ingest started"
        );
        let source = EvmSource::new(self.chain, self.http_url, self.ws_url);
        Pipeline::with_undo_depth(source, sink, self.undo_depth)
            .run()
            .await
    }
}

/// Builds an [`Ingest`].
#[derive(Debug)]
pub struct IngestBuilder {
    chain: String,
    http_url: Option<String>,
    ws_url: Option<String>,
    undo_depth: usize,
}

impl IngestBuilder {
    /// Starts a build for `chain`.
    #[must_use]
    pub fn new(chain: impl Into<String>) -> Self {
        Self {
            chain: chain.into(),
            http_url: None,
            ws_url: None,
            undo_depth: crate::ingest::pipeline::DEFAULT_UNDO_DEPTH,
        }
    }

    /// Sets the JSON-RPC endpoint used for blocks, receipts, and the finalized block.
    #[must_use]
    pub fn http_url(mut self, url: impl Into<String>) -> Self {
        self.http_url = Some(url.into());
        self
    }

    /// Sets the WebSocket endpoint used for heads.
    #[must_use]
    pub fn ws_url(mut self, url: impl Into<String>) -> Self {
        self.ws_url = Some(url.into());
        self
    }

    /// Sets how many published blocks the undo ring remembers, and therefore how deep a
    /// reorg can be retracted.
    #[must_use]
    pub const fn undo_depth(mut self, depth: usize) -> Self {
        self.undo_depth = depth;
        self
    }

    /// Finishes the build.
    ///
    /// # Errors
    ///
    /// Returns an error when an endpoint is unset, naming it.
    pub fn build(self) -> Result<Ingest> {
        Ok(Ingest {
            chain: self.chain,
            http_url: self
                .http_url
                .ok_or_else(|| anyhow::anyhow!("ingest http_url is required"))?,
            ws_url: self
                .ws_url
                .ok_or_else(|| anyhow::anyhow!("ingest ws_url is required"))?,
            undo_depth: self.undo_depth,
        })
    }
}
