//! The egress boundary: where events go.
//!
//! [`EventSink`] is the seam between publishing and the rest of the system. v1
//! ships an NDJSON sink to standard output, which keeps the ingestion loop
//! testable without a broker. The production sink is Redpanda.

use crate::envelope::Envelope;

/// Receives events in per-chain sequence order.
///
/// The pipeline publishes inside an exclusive borrow of the sink, so a slow sink
/// applies backpressure to the whole pipeline. That is safe only because
/// consumers read from their own queues downstream; until the bus lands, a slow
/// sink is a slow indexer, not a silently lossy one.
pub trait EventSink: Send + Sync {
    /// Publishes one event.
    ///
    /// # Errors
    ///
    /// Returns an error if the event cannot be delivered. The caller stops the
    /// pipeline rather than skipping the event.
    fn publish(&self, envelope: &Envelope) -> impl Future<Output = anyhow::Result<()>> + Send;
}

/// Writes newline-delimited JSON to standard output.
#[derive(Debug, Default)]
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
        let line = serde_json::to_string(envelope)?;
        // ponytail: serialises writers behind one lock. Fine for one process and
        // one stdout; move to a buffered channel when brokers are added.
        let mut stdout = tokio::io::stdout();
        tokio::io::AsyncWriteExt::write_all(&mut stdout, line.as_bytes()).await?;
        tokio::io::AsyncWriteExt::write_all(&mut stdout, b"\n").await?;
        tokio::io::AsyncWriteExt::flush(&mut stdout).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{EventSink, StdoutJsonSink};
    use crate::envelope::{ChainId, Envelope, Event};

    #[tokio::test]
    async fn stdout_sink_accepts_an_envelope() {
        let envelope = Envelope::new(
            ChainId::new("ethereum"),
            0,
            Event::Reorg {
                height: 1,
                new_head_hash: alloy_primitives::B256::from([0; 32]),
                orphaned_hashes: Vec::new(),
            },
        );
        assert!(StdoutJsonSink::new().publish(&envelope).await.is_ok());
    }
}
