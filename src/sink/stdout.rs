//! Newline-delimited JSON to standard output.

use crate::envelope::Envelope;
use crate::sink::EventSink;

/// Writes newline-delimited JSON to standard output.
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
    async fn publish(&self, envelope: &Envelope) -> anyhow::Result<()> {
        let mut line = serde_json::to_string(envelope)?;
        // serialises writers behind one lock. Fine for one process and
        // one stdout; move to a buffered channel when brokers are added.
        line.push('\n');
        let mut stdout = tokio::io::stdout();
        tokio::io::AsyncWriteExt::write_all(&mut stdout, line.as_bytes()).await?;
        tokio::io::AsyncWriteExt::flush(&mut stdout).await?;
        Ok(())
    }
}
