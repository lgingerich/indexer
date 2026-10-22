//! Newline-delimited JSON to standard output.
//!
//! Beside the sink rather than in [`crate::config`] because it is the `[sink.stdout]`
//! table's payload, and every sink keeps its own there: `DuckDB`'s are
//! [`DuckDbSettings`](crate::sink::duckdb::DuckDbSettings), so where a backend's
//! settings live is one rule rather than one case and one exception.

use crate::wire::envelope::Envelope;
use serde::Deserialize;
use tokio::io::AsyncWriteExt as _;

use crate::sink::route::Tables;
use crate::sink::{EnvelopeSink, SinkError};

/// `[sink.stdout]`'s settings: only which tables to print, since printing has nothing
/// else to configure.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StdoutSettings {
    /// The tables whose envelopes are printed. Required.
    pub tables: Tables,
}

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
        // Compact, one envelope per line: the encoding the wire contract pins in
        // `crate::wire::envelope`.
        let mut line = serde_json::to_string(&envelope)?;
        line.push('\n');
        let mut stdout = tokio::io::stdout();
        stdout.write_all(line.as_bytes()).await?;
        stdout.flush().await?;
        Ok(())
    }
}
