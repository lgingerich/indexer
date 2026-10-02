//! Newline-delimited JSON to standard output.
//!
//! Beside the sink rather than in [`crate::config`] because it is the `[sink.stdout]`
//! table's payload, and every sink keeps its own there: `DuckDB`'s are
//! [`DuckDbSettings`](crate::sink::duckdb::DuckDbSettings), so where a backend's
//! settings live is one rule rather than one case and one exception.

use crate::wire::envelope::Envelope;
use serde::Deserialize;
use tokio::io::AsyncWriteExt as _;

use crate::sink::{EnvelopeSink, SinkError};

/// `[sink.stdout]` takes no settings, and this is the type that says so.
///
/// Empty, so there is nothing an operator can set — but a distinct type rather than
/// `()`, so serde can answer a stray key with a list of what is valid. Every message
/// reaches the user through [`SettingsError`](crate::config::SettingsError), so an
/// unexplained empty list is a real error text, not a debug artifact.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StdoutSettings {}

/// Writes newline-delimited JSON to standard output.
///
/// Unlike the buffering sinks, each line is written and flushed as it arrives:
/// stdout is the human/pipe view of the live stream, so its value is liveness,
/// not throughput.
#[derive(Debug, Default)]
pub struct StdoutJsonSink;

impl StdoutJsonSink {
    /// Builds a sink that writes to standard output.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl EnvelopeSink for StdoutJsonSink {
    async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
        // The same encoding the store keeps as its `envelope` column, so a consumer of
        // the pipe and a reader of the store see the same bytes.
        let mut line = serde_json::to_string(&envelope)?;
        line.push('\n');
        let mut stdout = tokio::io::stdout();
        stdout.write_all(line.as_bytes()).await?;
        stdout.flush().await?;
        Ok(())
    }
}
