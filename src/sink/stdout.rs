//! Newline-delimited JSON to standard output.

use crate::wire::envelope::Envelope;
use tokio::io::AsyncWriteExt as _;

use crate::sink::EnvelopeSink;

/// Writes newline-delimited JSON to standard output.
///
/// Unlike the buffering sinks, each line is written and flushed as it arrives:
/// stdout is the human/pipe view of the live stream, so its value is liveness,
/// not throughput.
#[derive(Debug, Default, Clone)]
pub struct StdoutJsonSink;

impl StdoutJsonSink {
    /// Builds a sink that writes to standard output.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl EnvelopeSink for StdoutJsonSink {
    async fn publish(&mut self, envelope: Envelope) -> anyhow::Result<()> {
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
