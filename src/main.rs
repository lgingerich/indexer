//! The indexer's entry point: reads the settings file and runs the pipeline it describes.
//!
//! One binary, started as a process. What runs is not decided here — the settings file
//! says it, and [`indexer::runtime`] assembles it: ingest follows the configured chain,
//! decode uses the configured protocol manifests, and storage writes the configured store.
//! So this file is only the process boundary: logging, the settings path, and the exit
//! code.
//!
//! # Settings
//!
//! A TOML file, named by the first argument or `indexer.toml`. Every setting and its
//! default is documented in [`indexer::config`], and the required ones — the chain and its
//! endpoints — error at startup naming the field.
//!
//! ```bash
//! cargo run --release -- indexer.toml
//! ```

use std::process::ExitCode;

use tracing::error;
use tracing_subscriber::EnvFilter;

/// The settings file used when none is named on the command line.
const DEFAULT_SETTINGS: &str = "indexer.toml";

/// The settings file's path as given on the command line, or [`DEFAULT_SETTINGS`].
fn settings_path() -> String {
    std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_SETTINGS.to_owned())
}

// A DuckDB flush blocks the worker that polls it, and
// ingest has to keep running on another one. `rt-multi-thread` makes this the default;
// naming it means dropping that feature fails the build instead of silently running
// ingest and the flush on one thread.
#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let path = settings_path();
    match indexer::runtime::run(&path).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            // `{error:?}` walks the `#[from]` chain, so the log carries every layer that
            // named the failure — settings, pipeline, store — where `{error}` would print
            // only the outermost variant's message and lose the cause underneath it.
            error!(error = ?error, "indexer stopped");
            ExitCode::FAILURE
        }
    }
}
