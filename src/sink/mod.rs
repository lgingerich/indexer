//! The egress boundary: where events go.
//!
//! [`EventSink`] is the seam between publishing and the rest of the system.
//!
//! The sink is a dumb serializing boundary: it renders the [`Envelope`] as-is and
//! knows nothing about chains. A message-format version would belong here once a
//! broker keeps history a consumer reads across a shape change — Redis/Kafka-style
//! topics are the first case — because that is the only setting where one consumer
//! sees two shapes interleaved. Add it at v1 when the broker lands, with a written
//! compatibility policy (or a schema registry), before it produces anything.

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
#[derive(Debug, Default, Clone)]
pub struct StdoutJsonSink;

impl StdoutJsonSink {
    /// Builds a sink that writes to standard output.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Renders one envelope to a single JSON line.
    ///
    /// The envelope already serializes to `chain`, `sequence`, and the event's
    /// fields, so this is just the line encoding; it becomes the place a
    /// message-format version is stamped when a broker needs one.
    ///
    /// # Errors
    ///
    /// Returns an error if the envelope cannot be serialized.
    pub fn render(&self, envelope: &Envelope) -> Result<String, serde_json::Error> {
        serde_json::to_string(envelope)
    }
}

impl EventSink for StdoutJsonSink {
    async fn publish(&self, envelope: &Envelope) -> anyhow::Result<()> {
        let mut line = self.render(envelope)?;
        // serialises writers behind one lock. Fine for one process and
        // one stdout; move to a buffered channel when brokers are added.
        line.push('\n');
        let mut stdout = tokio::io::stdout();
        tokio::io::AsyncWriteExt::write_all(&mut stdout, line.as_bytes()).await?;
        tokio::io::AsyncWriteExt::flush(&mut stdout).await?;
        Ok(())
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use super::{EventSink as _, StdoutJsonSink};
    use crate::envelope::{ChainId, Envelope, Event, Finalized, Reorg};

    #[tokio::test]
    async fn stdout_sink_accepts_an_envelope() {
        let envelope = Envelope::new(
            ChainId::new("ethereum"),
            0,
            Event::Reorg(Reorg {
                height: 1,
                new_head_hash: alloy_primitives::B256::from([0; 32]),
                orphaned_hashes: Vec::new(),
            }),
        );
        assert!(StdoutJsonSink::new().publish(&envelope).await.is_ok());
    }

    #[test]
    fn render_keeps_the_envelope_fields_flat_on_one_line() {
        let envelope = Envelope::new(
            ChainId::new("base"),
            7,
            Event::Finalized(Finalized {
                height: 42,
                hash: alloy_primitives::B256::from([0x11; 32]),
            }),
        );
        let rendered = StdoutJsonSink::new()
            .render(&envelope)
            .expect("envelope renders");
        let value: serde_json::Value = serde_json::from_str(&rendered).expect("renders as JSON");
        // The consumer-facing wire contract: one flat object carrying chain, the
        // sequence, and the event's fields under its `type` tag.
        assert_eq!(value["chain"], "base");
        assert_eq!(value["sequence"], 7);
        assert_eq!(value["type"], "finalized");
        assert_eq!(value["height"], 42);
        assert_eq!(value["hash"], format!("0x{}", "11".repeat(32)));
        // Exactly the envelope's keys plus the event's: chain, sequence, type, and
        // the two `finalized` fields — nothing else rides along.
        assert_eq!(value.as_object().expect("object").len(), 5);
    }
}
