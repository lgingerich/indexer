//! Running the ingest stage: one source into one sink.
//!
//! A caller names a chain and its endpoints and gets a future that runs until the
//! source ends. Nothing here reads the environment — that is [`crate::config`]'s job,
//! which has already required both endpoints before this is constructed.

use alloy_primitives::Address;
use tracing::info;

use crate::config::Secret;
use crate::ingest::pipeline::{Machine, PipelineError};
use crate::ingest::source::{BlockMeta, EvmSource, SourceError};
use crate::sink::{Datasets, EnvelopeSink};

/// Builds and runs the ingest stage.
#[derive(Debug)]
pub struct Ingest {
    chain: String,
    http_url: Secret,
    ws_url: Secret,
    datasets: Datasets,
    log_addresses: Vec<Address>,
    start_block: Option<u64>,
}

impl Ingest {
    /// Builds an ingest stage for `chain` and its two endpoints.
    ///
    /// Both endpoints are required [`Secret`]s, since a provider's URL usually carries
    /// its API key. A settings file that omits one never reaches here: serde rejects it
    /// first. `log_addresses` limits `eth_getLogs`;
    /// an empty slice fetches every log. `start_block` selects the first height of a
    /// fresh run: `None` starts at the observed head, `Some` indexes from that height.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError::LogAddresses`] when addresses are set but logs are not
    /// fetched with `eth_getLogs`.
    pub fn new(
        chain: impl Into<String>,
        http_url: Secret,
        ws_url: Secret,
        datasets: &Datasets,
        log_addresses: &[Address],
        start_block: Option<u64>,
    ) -> Result<Self, SourceError> {
        if !log_addresses.is_empty() && (!datasets.logs || datasets.receipts) {
            return Err(SourceError::LogAddresses);
        }
        Ok(Self {
            chain: chain.into(),
            http_url,
            ws_url,
            datasets: *datasets,
            log_addresses: log_addresses.to_vec(),
            start_block,
        })
    }

    /// Indexes through the sampled head, then follows live heads.
    ///
    /// `ledger` is the accepted history a store read back, oldest first. Empty starts
    /// fresh, at `start_block` or the head; otherwise the run resumes after the ledger's
    /// tip, and a `start_block` is refused rather than silently ignored. Source failures
    /// propagate without retries.
    ///
    /// # Errors
    ///
    /// Returns an error when the subscription, a block fetch, or a publish fails,
    /// [`PipelineError::StartWithHistory`] when both a ledger and `start_block` are given,
    /// and [`PipelineError::SubscriptionClosed`] when the subscription ends — a live
    /// indexer should never stop.
    pub async fn run<S: EnvelopeSink>(
        self,
        sink: S,
        ledger: Vec<BlockMeta>,
    ) -> Result<(), PipelineError> {
        info!(
            chain = %self.chain,
            datasets = %self.datasets,
            log_addresses = self.log_addresses.len(),
            start_block = ?self.start_block,
            restored = ledger.len(),
            "ingest started"
        );
        let source = EvmSource::new(
            self.chain,
            self.http_url.expose(),
            self.ws_url.expose(),
            self.datasets,
            &self.log_addresses,
        )?;
        let machine = Machine::new(source, sink, ledger);
        match self.start_block {
            Some(from) => machine.backfill(from).await?.run().await,
            None => machine.run().await,
        }
    }
}
