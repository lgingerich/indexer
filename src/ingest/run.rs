//! Running the ingest stage: one source into one sink.
//!
//! A caller names a chain and its endpoints and gets a future that runs until the
//! source ends. Nothing here reads the environment — that is [`crate::config`]'s job,
//! which has already required both endpoints before this is constructed.

use tracing::info;

use crate::ingest::pipeline::{Machine, PipelineError};
use crate::ingest::source::EvmSource;
use crate::sink::EnvelopeSink;

/// Builds and runs the ingest stage.
#[derive(Debug)]
pub struct Ingest {
    chain: String,
    http_url: String,
    ws_url: String,
}

impl Ingest {
    /// Builds an ingest stage for `chain` and its two endpoints.
    ///
    /// Both endpoints are required strings. A settings file that omits one never
    /// reaches here: serde rejects it first.
    #[must_use]
    pub fn new(
        chain: impl Into<String>,
        http_url: impl Into<String>,
        ws_url: impl Into<String>,
    ) -> Self {
        Self {
            chain: chain.into(),
            http_url: http_url.into(),
            ws_url: ws_url.into(),
        }
    }

    /// Indexes the finalized anchor and unfinalized tail, then follows live heads.
    ///
    /// Source failures propagate without retries. Startup does not restore stored history.
    ///
    /// # Errors
    ///
    /// Returns an error when the subscription, a block fetch, or a publish fails, and
    /// [`PipelineError::SubscriptionClosed`] when the subscription ends — a live indexer
    /// should never stop.
    pub async fn run<S: EnvelopeSink>(self, sink: S) -> Result<(), PipelineError> {
        info!(
            chain = %self.chain,
            http = %self.http_url,
            ws = %self.ws_url,
            "ingest started"
        );
        let source = EvmSource::new(self.chain, self.http_url, self.ws_url);
        Machine::new(source, sink).run().await
    }
}
