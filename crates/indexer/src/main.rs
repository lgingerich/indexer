//! Live EVM head subscription, printed as newline-delimited JSON.
//!
//! Backfill-to-live handoff is not implemented yet, so this binary starts at
//! whatever the chain submits next: it does not fill gaps that existed before it
//! started. That is the deliberate scope of the walking skeleton.

use std::process::ExitCode;

use anyhow::{Context as _, Result};
use connectors::StdoutJsonSink;
use indexer::pipeline::Pipeline;
use indexer::source::EvmSource;
use tracing::error;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .json()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            error!(%error, "indexer stopped");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let chain = std::env::var("EVM_CHAIN").context("EVM_CHAIN must be set")?;
    let http_url = std::env::var("EVM_HTTP_URL").context("EVM_HTTP_URL must be set")?;
    let ws_url = std::env::var("EVM_WS_URL").context("EVM_WS_URL must be set")?;

    let source = EvmSource::new(chain, http_url, ws_url);
    let sink = StdoutJsonSink::new();
    Pipeline::new(source, sink).run().await
}
