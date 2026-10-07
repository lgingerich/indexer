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
use tracing_subscriber::filter::LevelFilter;

/// The settings file used when none is named on the command line.
const DEFAULT_SETTINGS: &str = "indexer.toml";

/// The log level when `RUST_LOG` is unset: `info` in a debug build, so the developer sees
/// the run, and `error` in a release one, so a deployment's log volume stays the
/// operator's call. A set `RUST_LOG` wins either way.
const DEFAULT_LOG_LEVEL: LevelFilter = if cfg!(debug_assertions) {
    LevelFilter::INFO
} else {
    LevelFilter::ERROR
};

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
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(DEFAULT_LOG_LEVEL.into())
                .from_env_lossy(),
        )
        .init();

    let path = settings_path();
    match indexer::runtime::run(&path).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            // Display, not Debug: every variant in the chain prints its cause — `{source}`
            // or `transparent` — so this already names every layer that failed, settings
            // to store. Debug would wrap the same chain in the raw values behind it: a
            // settings parse error alone would print the whole document, its byte spans,
            // and its key list. The path is named here because this caller is the one
            // that has it.
            error!(%error, settings = %path, "indexer stopped");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use indexer::config::Settings;
    use indexer::runtime::RuntimeError;

    /// What an operator sees when the indexer stops on a bad settings file: every layer
    /// named, the leaf cause under it — and not the raw document the parse error carries
    /// inside it.
    #[test]
    fn a_stop_error_names_the_cause_chain_without_the_raw_document() {
        let input = r#"
# THE DOCUMENT IS NOT THE MESSAGE

[ingest]
chain = "base"
http_url = "https://example.invalid"
ws_url = "wss://example.invalid"

[sink.nope]
"#;
        let Err(error) = Settings::from_str(input) else {
            panic!("an unknown sink backend must be a parse failure");
        };
        let shown = RuntimeError::from(error).to_string();
        assert!(shown.contains("settings could not be loaded"), "{shown}");
        assert!(shown.contains("invalid settings"), "{shown}");
        assert!(shown.contains("unknown variant `nope`"), "{shown}");
        assert!(
            !shown.contains("THE DOCUMENT IS NOT THE MESSAGE"),
            "the raw document is not the message: {shown}"
        );
    }
}
