//! Newline-delimited JSON to standard output.

use tokio::io::AsyncWriteExt as _;

use crate::envelope::Envelope;
use crate::sink::EventSink;

/// Writes newline-delimited JSON to standard output.
///
/// Unlike the buffering sinks, each line is written and flushed as it arrives:
/// stdout is the human/pipe view of the live stream, so its value is liveness,
/// not throughput. Use a broker sink when batching matters.
#[derive(Debug, Default, Clone)]
pub struct StdoutJsonSink;

impl StdoutJsonSink {
    /// Builds a sink that writes to standard output.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl EventSink for StdoutJsonSink {
    async fn publish(&mut self, envelope: &Envelope) -> anyhow::Result<()> {
        let mut line = serde_json::to_string(envelope)?;
        line.push('\n');
        let mut stdout = tokio::io::stdout();
        stdout.write_all(line.as_bytes()).await?;
        stdout.flush().await?;
        Ok(())
    }
}
