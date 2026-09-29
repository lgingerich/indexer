//! The indexer's entry point: reads the settings file and runs the pipeline it describes.
//!
//! One binary, started as a process. What runs is not decided here — the settings file
//! says it, and [`indexer::runtime`] assembles it: ingest iff a chain is configured,
//! decode iff a registry has entries, and storage always, on whichever bus and store the
//! settings chose. So this file is only the process boundary: logging, the settings path,
//! and the exit code.
//!
//! # Settings
//!
//! A TOML file, named by the first argument or `indexer.toml`. Every setting and its
//! default is documented in [`indexer::config`], and the required ones — a broker for a
//! Kafka bus, a chain's endpoints when ingest is configured — error at startup naming the
//! field.
//!
//! ```bash
//! cargo run --release --features kafka,duckdb -- indexer.toml
//! ```

use std::process::ExitCode;

use tracing::error;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .json()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let path = indexer::runtime::settings_path();
    match indexer::runtime::run(&path).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            // `{error:?}` prints the whole `anyhow` chain, where `{error}` prints only the
            // outermost context. A stage failure is wrapped in "decode stopped", so the
            // Display form loses the cause — which is the only part worth having.
            error!(error = ?error, "indexer stopped");
            ExitCode::FAILURE
        }
    }
}
