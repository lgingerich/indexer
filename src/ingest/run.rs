//! Running the ingest stage: one source into one sink.
//!
//! A caller names a chain and its endpoints and gets a future that runs until the
//! source ends. Nothing here reads the environment — that is [`crate::config`]'s job,
//! which has already required both endpoints before this is constructed.

use alloy_primitives::Address;
use tracing::info;

use crate::ingest::pipeline::{Machine, PipelineError};
use crate::ingest::source::{EvmSource, SourceError};
use crate::sink::{Datasets, EnvelopeSink};

/// Builds and runs the ingest stage.
#[derive(Debug)]
pub struct Ingest {
    chain: String,
    http_url: String,
    ws_url: String,
    datasets: Datasets,
    log_addresses: Vec<Address>,
}

impl Ingest {
    /// Builds an ingest stage for `chain` and its two endpoints.
    ///
    /// Both endpoints are required strings. A settings file that omits one never
    /// reaches here: serde rejects it first. `log_addresses` limits `eth_getLogs`;
    /// an empty slice fetches every log.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError::LogAddresses`] when addresses are set but logs are not
    /// fetched with `eth_getLogs`.
    pub fn new(
        chain: impl Into<String>,
        http_url: impl Into<String>,
        ws_url: impl Into<String>,
        datasets: &Datasets,
        log_addresses: &[Address],
    ) -> Result<Self, SourceError> {
        if !log_addresses.is_empty() && (!datasets.log || datasets.receipt) {
            return Err(SourceError::LogAddresses);
        }
        Ok(Self {
            chain: chain.into(),
            http_url: http_url.into(),
            ws_url: ws_url.into(),
            datasets: *datasets,
            log_addresses: log_addresses.to_vec(),
        })
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
            datasets = %self.datasets,
            log_addresses = self.log_addresses.len(),
            "ingest started"
        );
        let source = EvmSource::new(
            self.chain,
            self.http_url,
            self.ws_url,
            self.datasets,
            &self.log_addresses,
        )?;
        Machine::new(source, sink).run().await
    }
}
