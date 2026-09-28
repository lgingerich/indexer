//! The `DuckDB` sink: envelopes into a local, queryable database.
//!
//! Where [`StdoutJsonSink`](crate::connectors::StdoutJsonSink) is fire-and-forward, this
//! one is a *store*: it appends each envelope as a row so a later process can
//! query the history locally with SQL. That split matters — `DuckDB` is an
//! embedded, single-writer engine, so it is an archive/analytics endpoint, not a
//! horizontally-scaled egress. Keep it for a local replica or an analytical
//! sidecar, not as the fan-out for many consumers.
//!
//! One flat row per event; the envelope rides along as `JSON` rather than as a
//! second copy of its fields:
//!
//! | column       | source                                                  |
//! |--------------|---------------------------------------------------------|
//! | `sequence`   | [`Envelope::sequence`]                                  |
//! | `chain`      | [`Envelope::chain`]                                     |
//! | `event_type` | [`Envelope::kind`] (`block`, `log`, `reorg`, …)         |
//! | `dedupe_key` | [`Event::dedupe_key`](crate::wire::envelope::Event::dedupe_key) |
//! | `envelope`   | `serde_json::to_string(envelope)`                       |
//!
//! `sequence`/`chain` are lifted into columns because they are the columns you
//! filter and order on; the full envelope still rides along as `JSON` so nothing
//! is lost, and consumers dig into event fields with `DuckDB`'s JSON functions.
//! The table is append-only and un-keyed, unlike the dataset tables — it is the
//! raw stream, and readers deduplicate on `dedupe_key`.
//!
//! The sink takes a [`Connection`] the runtime opened (path, settings, extensions,
//! threads) so the library stays out of the runtime's connection policy. It does
//! own the table DDL: `new` runs `CREATE TABLE IF NOT EXISTS` once, so a restart
//! reuses the existing table.
//!
//! `DuckDB`'s appender is the bulk-import path, and it borrows the connection, so
//! the sink cannot hold one open across calls. Instead `publish` buffers a
//! rendered row and [`flush`](crate::connectors::EventSink::flush) opens one appender and commits the
//! whole batch — the `DuckDB` analogue of the Kafka sink's accumulator. One
//! process writes at a time, so there is no lock and no `Mutex`; a second process
//! against the same file is the engine's error to report, not this sink's.

use crate::wire::envelope::Envelope;
use anyhow::Context as _;
use duckdb::{Connection, params};

use crate::connectors::EventSink;

/// DDL for the append-only event table.
///
/// `event_type`, not `type`, because `type` is a keyword in enough SQL dialects
/// to be a footgun; `IF NOT EXISTS` makes [`DuckDbSink::new`] idempotent across
/// restarts.
const CREATE_TABLE: &str = "\
CREATE TABLE IF NOT EXISTS events (
    sequence   BIGINT  NOT NULL,
    chain      VARCHAR NOT NULL,
    event_type VARCHAR NOT NULL,
    dedupe_key VARCHAR NOT NULL,
    envelope   JSON    NOT NULL
)";

/// Appends envelopes to a local `DuckDB` database, one batch per [`flush`].
///
/// [`flush`]: EventSink::flush
pub struct DuckDbSink {
    connection: Connection,
    rows: Vec<Row>,
}

impl std::fmt::Debug for DuckDbSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDbSink")
            .field("buffered", &self.rows.len())
            .finish_non_exhaustive()
    }
}

impl DuckDbSink {
    /// Takes ownership of `connection` and ensures the `events` table exists.
    ///
    /// The runtime opens the connection with whatever path and settings it needs;
    /// the library owns only the schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be created.
    pub fn new(connection: Connection) -> anyhow::Result<Self> {
        connection
            .execute_batch(CREATE_TABLE)
            .context("create events table")?;
        Ok(Self {
            connection,
            rows: Vec::new(),
        })
    }

    /// Buffers one envelope as a row, rendered ready for the appender.
    fn write(&mut self, envelope: &Envelope) -> anyhow::Result<()> {
        self.rows.push(Row::from_envelope(envelope)?);
        Ok(())
    }
}

impl EventSink for DuckDbSink {
    async fn publish(&mut self, envelope: &Envelope) -> anyhow::Result<()> {
        self.write(envelope)
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        // One appender for the whole batch; `flush` is the durability point.
        let mut appender = self
            .connection
            .appender("events")
            .context("open appender")?;
        for row in &self.rows {
            appender
                .append_row(params![
                    row.sequence,
                    row.chain,
                    row.event_type,
                    row.dedupe_key,
                    row.envelope,
                ])
                .context("append event row")?;
        }
        appender.flush().context("flush appender")?;
        self.rows.clear();
        Ok(())
    }
}

/// One row's worth of an envelope, already rendered for `DuckDB`.
struct Row {
    sequence: u64,
    chain: String,
    event_type: &'static str,
    dedupe_key: String,
    envelope: String,
}

impl Row {
    fn from_envelope(envelope: &Envelope) -> anyhow::Result<Self> {
        Ok(Self {
            sequence: envelope.sequence,
            chain: envelope.chain.as_str().to_owned(),
            event_type: envelope.kind(),
            dedupe_key: envelope.event.dedupe_key(),
            envelope: serde_json::to_string(envelope)?,
        })
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use crate::wire::envelope::{ChainId, Envelope, Event, Finalized};
    use duckdb::Connection;

    use crate::connectors::EventSink as _;
    use crate::connectors::duckdb::DuckDbSink;

    fn sink() -> DuckDbSink {
        let connection = Connection::open_in_memory().expect("open in-memory DuckDB");
        DuckDbSink::new(connection).expect("create events table")
    }

    fn envelope() -> Envelope {
        Envelope::new(
            ChainId::new("base"),
            7,
            Event::Finalized(Finalized {
                height: 42,
                hash: alloy_primitives::B256::from([0x11; 32]),
            }),
        )
    }

    /// The columns are lifted from the envelope and the raw JSON is the same
    /// bytes the other sinks put on the wire, so a `DuckDB` consumer sees the
    /// unreduced event.
    #[tokio::test]
    async fn a_row_carries_the_lifted_columns_and_the_raw_envelope() {
        let mut sink = sink();
        let source = envelope();
        sink.publish(&source).await.expect("row buffers");
        // Nothing is durable before the flush.
        assert_eq!(row_count(&sink), 0);
        sink.flush().await.expect("batch flushes");

        let connection = &sink.connection;
        let (chain, event_type, sequence, dedupe_key, raw): (String, String, u64, String, String) =
            connection
                .query_row(
                    "SELECT chain, event_type, sequence, dedupe_key, envelope FROM events",
                    [],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .expect("row reads back");

        assert_eq!(chain, "base");
        assert_eq!(event_type, "finalized");
        assert_eq!(sequence, 7);
        assert_eq!(dedupe_key, source.event.dedupe_key());
        assert_eq!(
            serde_json::from_str::<Envelope>(&raw).expect("stored envelope decodes"),
            source
        );
    }

    /// Connecting twice to the same file must not fail on the existing table.
    #[tokio::test]
    async fn new_is_idempotent() {
        let path = std::env::temp_dir().join(format!("indexer-sink-{}.duckdb", std::process::id()));
        let open = || {
            DuckDbSink::new(
                Connection::open(path.to_string_lossy().into_owned()).expect("open temp database"),
            )
            .expect("create or reuse the events table")
        };
        open();
        open();
        // Leave nothing behind; the connection is released when the sinks drop.
        std::fs::remove_file(&path).expect("remove temp database");
    }

    /// A batch of several rows lands in one flush.
    #[tokio::test]
    async fn a_flush_writes_the_whole_batch() {
        let mut sink = sink();
        for sequence in 0..5 {
            let envelope = Envelope::new(
                ChainId::new("base"),
                sequence,
                Event::Finalized(Finalized {
                    height: 42,
                    hash: alloy_primitives::B256::from([0x11; 32]),
                }),
            );
            sink.publish(&envelope).await.expect("row buffers");
        }
        sink.flush().await.expect("batch flushes");

        assert_eq!(row_count(&sink), 5);
    }

    fn row_count(sink: &DuckDbSink) -> i64 {
        sink.connection
            .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
            .expect("count reads back")
    }
}
