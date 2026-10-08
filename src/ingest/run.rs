//! Running the ingest stage: one source into one sink.
//!
//! A caller hands over a built source and gets a future that runs until the source
//! ends. Building the source — endpoints, clients, subscriptions — is the caller's job,
//! so a deployment and a simulation run the same stage over different sources.

use tracing::info;

use crate::ingest::pipeline::{Machine, PipelineError};
use crate::ingest::source::{BlockMeta, BlockSource};
use crate::sink::EnvelopeSink;
use crate::wire::envelope::ChainId;

/// The ingest stage: a source and where a fresh run starts.
#[derive(Debug)]
pub struct Ingest<B> {
    source: B,
    start_block: Option<u64>,
}

impl<B: BlockSource> Ingest<B> {
    /// An ingest stage reading `source`.
    ///
    /// `start_block` selects the first height of a fresh run: `None` starts at the
    /// observed head, `Some` indexes from that height.
    #[must_use]
    pub const fn new(source: B, start_block: Option<u64>) -> Self {
        Self {
            source,
            start_block,
        }
    }

    /// The chain the source reads.
    pub fn chain(&self) -> &ChainId {
        self.source.chain()
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
            chain = %self.source.chain(),
            start_block = ?self.start_block,
            restored = ledger.len(),
            "ingest started"
        );
        let machine = Machine::new(self.source, sink, ledger);
        match self.start_block {
            Some(from) => machine.backfill(from).await?.run().await,
            None => machine.run().await,
        }
    }
}
