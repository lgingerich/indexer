//! Running the ingest stage: one source into one sink.
//!
//! The builder is the whole surface. A caller names a chain, its endpoints, and where
//! the events go, and gets a future that runs until the source ends. Nothing here reads
//! the environment — that is [`crate::config`]'s job, so this is constructible in a test.

use thiserror::Error;
use tracing::info;

use crate::ingest::pipeline::{Pipeline, PipelineError};
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
        Pipeline::new(source, sink).run().await
    }
}

/// Builds an [`Ingest`].
#[derive(Debug)]
pub struct IngestBuilder {
    chain: String,
    http_url: Option<String>,
    ws_url: Option<String>,
}

impl IngestBuilder {
    /// Starts a build for `chain`.
    #[must_use]
    pub fn new(chain: impl Into<String>) -> Self {
        Self {
            chain: chain.into(),
            http_url: None,
            ws_url: None,
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

    /// Finishes the build.
    ///
    /// # Errors
    ///
    /// Returns [`IngestError::MissingEndpoint`] naming whichever endpoint is unset. The
    /// field is the name in the settings file, so a typo in a builder call and a typo in
    /// a settings key are both located by the same message.
    ///
    /// Unreachable from [`crate::runtime`], which sets both endpoints from settings that
    /// serde has already required and refused to leave empty — but reachable from any
    /// other caller, which is why this returns a `Result` rather than asserting.
    pub fn build(self) -> Result<Ingest, IngestError> {
        let http_url = self
            .http_url
            .ok_or(IngestError::MissingEndpoint { field: "http_url" })?;
        let ws_url = self
            .ws_url
            .ok_or(IngestError::MissingEndpoint { field: "ws_url" })?;
        Ok(Ingest {
            chain: self.chain,
            http_url,
            ws_url,
        })
    }
}

/// Why an [`Ingest`] could not be built.
///
/// Only the build half, and it is its own type rather than a variant of
/// [`PipelineError`] because the two are resolved differently: a missing endpoint is a
/// programming error at the call site, fixed by editing code, while everything the
/// pipeline reports happens at runtime and is fixed by fixing data or restarting.
/// Merging them would leave a caller unable to tell "never started" from "stopped".
///
/// Reachable from outside this crate — [`Ingest`] has no other constructor — so this
/// guards a real path rather than an impossible one.
#[derive(Debug, Error)]
pub enum IngestError {
    /// A required endpoint was never set on the builder.
    #[error("ingest {field} is required")]
    MissingEndpoint {
        /// The unset endpoint's settings-file name, `http_url` or `ws_url`.
        field: &'static str,
    },
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are allowed
// them per the repository test style, since a failed expectation there means the fixture
// or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use super::{Ingest, IngestError};

    /// A caller that names a chain but forgets an endpoint is told which one, by the
    /// settings-file name — so the fix is the same whether the mistake was here or in
    /// the settings.
    ///
    /// `runtime` cannot hit this (serde requires both fields), so the case is only
    /// reachable by a direct builder user. It is still a real path, so it is checked.
    #[test]
    fn an_unset_endpoint_names_the_field_that_is_missing() {
        let error = Ingest::builder("base")
            .ws_url("wss://example.invalid")
            .build()
            .expect_err("http_url was never set");
        assert!(
            matches!(error, IngestError::MissingEndpoint { field: "http_url" }),
            "the error must name the unset field: {error}"
        );

        let error = Ingest::builder("base")
            .http_url("https://example.invalid")
            .build()
            .expect_err("ws_url was never set");
        assert!(
            matches!(error, IngestError::MissingEndpoint { field: "ws_url" }),
            "the error must name the unset field: {error}"
        );
    }

    /// Both set is the ordinary path, and it must not error.
    #[test]
    fn both_endpoints_assemble() {
        Ingest::builder("base")
            .http_url("https://example.invalid")
            .ws_url("wss://example.invalid")
            .build()
            .expect("both endpoints set");
    }
}
