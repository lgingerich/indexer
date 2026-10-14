//! Either EVM source, as one type, so a run is built from whichever its settings name.

use super::{BlockSource, EvmSource, FetchedBlock, HeadStream, HyperSyncSource, SourceError};
use crate::wire::envelope::{BlockMeta, ChainId};

/// The source the settings named: a node's JSON-RPC, or `HyperSync` for a backfill.
#[derive(Debug)]
pub enum AnySource {
    /// A node's JSON-RPC and WebSocket endpoints, which follow the chain.
    Rpc(EvmSource),
    /// Envio `HyperSync`, which backfills and stops.
    HyperSync(HyperSyncSource),
}

/// Calls `$call` on whichever source `$source` holds.
macro_rules! each {
    ($source:expr, $inner:ident => $call:expr) => {
        match $source {
            AnySource::Rpc($inner) => $call,
            AnySource::HyperSync($inner) => $call,
        }
    };
}

impl BlockSource for AnySource {
    fn chain(&self) -> &ChainId {
        each!(self, source => source.chain())
    }

    fn follows_heads(&self) -> bool {
        each!(self, source => source.follows_heads())
    }

    async fn subscribe_heads(&self) -> Result<HeadStream, SourceError> {
        each!(self, source => source.subscribe_heads().await)
    }

    async fn fetch_block(
        &self,
        height: u64,
        head: Option<&BlockMeta>,
    ) -> Result<FetchedBlock, SourceError> {
        each!(self, source => source.fetch_block(height, head).await)
    }

    async fn fetch_blocks(&self, from: u64, to: u64) -> Result<Vec<FetchedBlock>, SourceError> {
        each!(self, source => source.fetch_blocks(from, to).await)
    }

    fn backfill_span(&self) -> u64 {
        each!(self, source => source.backfill_span())
    }

    fn backfill_concurrency(&self) -> usize {
        each!(self, source => source.backfill_concurrency())
    }

    async fn fetch_header(&self, height: Option<u64>) -> Result<BlockMeta, SourceError> {
        each!(self, source => source.fetch_header(height).await)
    }
}
