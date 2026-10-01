//! The `DuckDB` sink: envelopes into a local, queryable database.
//!
//! Where [`StdoutJsonSink`](crate::sink::StdoutJsonSink) is fire-and-forward, this
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
//! threads) so the library stays out of the runtime's connection policy —
//! [`DuckDbSink::open`] is that opening, kept here because it is the one place that knows
//! how `DuckDB` takes its settings. It does
//! own the table DDL: `new` runs `CREATE TABLE IF NOT EXISTS` once, so a restart
//! reuses the existing table.
//!
//! `DuckDB`'s appender is the bulk-import path, and it borrows the connection, so
//! the sink cannot hold one open across calls. Instead `publish` buffers a
//! rendered row and [`flush`](crate::sink::EnvelopeSink::flush) opens one appender and
//! commits the whole batch. One process writes at a time, so there is no lock and no
//! `Mutex`; a second process against the same file is the engine's error to report,
//! not this sink's.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::wire::envelope::Envelope;
use anyhow::Context as _;
use duckdb::{Connection, params};
use serde::Deserialize;
use tracing::info;

use crate::sink::EnvelopeSink;

/// The `DuckDB` file written when the settings name no path.
const DEFAULT_PATH: &str = "indexer.duckdb";

/// The most records one commit may cover when the settings name no bound.
///
/// Sized above a single block's worth of envelopes, or the fold in
/// [`ChannelReceiver::drain`](crate::sink::channel::ChannelReceiver::drain) could never
/// join a backlog and the bound would be inert.
const DEFAULT_BATCH_RECORDS: usize = 500;

/// `DuckDB`'s settings: the database to write, and the engine settings passed through.
///
/// Beside the sink rather than in [`crate::config`] because these are `DuckDB`'s: the
/// engine settings are opaque keys the engine validates, and a build without the
/// `duckdb` feature has no use for either. What the *file* may say about `DuckDB` is the
/// `[sink.duckdb]` table, which [`Storage::DuckDb`](crate::config::Sink::DuckDb)
/// names.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DuckDbSettings {
    /// The `DuckDB` database file to write. Defaults to `indexer.duckdb`.
    pub path: PathBuf,
    /// The most records one store commit may cover.
    ///
    /// Storage receives one block at a time and commits each as it arrives. When it has
    /// fallen behind, it folds the blocks already waiting into one commit until this many
    /// records are reached, so a stalled store catches up in fewer, larger transactions.
    /// A block is never split, so a commit can run past the bound by up to one block.
    ///
    /// The bound only applies to the blocks already waiting, so a single block carrying
    /// this many envelopes commits on its own no matter what this is set to. Below that,
    /// a larger value folds more of a backlog into one transaction.
    pub batch_records: usize,
    /// Any other `DuckDB` setting, passed straight through.
    ///
    /// `DuckDB` accepts dozens of settings and this file does not restate them. Anything
    /// here reaches [`duckdb::Config::with`], which validates it, so a misspelled key is
    /// an error from the engine naming the setting rather than a silent no-op. The keys
    /// and their meanings are listed in `DuckDB`'s
    /// [configuration overview](https://duckdb.org/docs/stable/configuration/overview).
    ///
    /// ```toml
    /// [sink.duckdb.settings]
    /// threads = "4"
    /// max_memory = "1GB"
    /// ```
    pub settings: BTreeMap<String, String>,
}

impl Default for DuckDbSettings {
    fn default() -> Self {
        Self {
            path: PathBuf::from(DEFAULT_PATH),
            batch_records: DEFAULT_BATCH_RECORDS,
            settings: BTreeMap::new(),
        }
    }
}

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
/// [`flush`]: EnvelopeSink::flush
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
    /// Opens the database the settings named and returns a sink writing to it.
    ///
    /// The path and the engine settings are `DuckDB`'s, so opening lives here rather than
    /// in the runtime: this is the one place that knows [`duckdb::Config`] is how the
    /// engine takes its settings, and a test can go through it without a settings file.
    /// [`new`](Self::new) stays the way to supply a [`Connection`] of your own.
    ///
    /// # Errors
    ///
    /// Returns an error when an engine setting is rejected, the database cannot be
    /// opened, or the table cannot be created.
    pub fn open(settings: &DuckDbSettings) -> anyhow::Result<Self> {
        let mut config = duckdb::Config::default();
        for (key, value) in &settings.settings {
            config = config
                .with(key, value)
                .with_context(|| format!("duckdb setting {key:?} was rejected"))?;
        }
        let connection = Connection::open_with_flags(&settings.path, config)
            .with_context(|| format!("open store at {}", settings.path.display()))?;
        info!(store = %settings.path.display(), "storage opened");
        Self::new(connection)
    }

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

impl EnvelopeSink for DuckDbSink {
    async fn publish(&mut self, envelope: Envelope) -> anyhow::Result<()> {
        self.write(&envelope)
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

    use crate::sink::EnvelopeSink as _;
    use crate::sink::duckdb::DuckDbSink;

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
        sink.publish(source.clone()).await.expect("row buffers");
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
            sink.publish(envelope).await.expect("row buffers");
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
