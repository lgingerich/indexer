//! Where envelopes go: the [`EnvelopeSink`] contract and the sinks that implement it.
//!
//! Ingest hands each envelope of a block to one sink and flushes at the block boundary.
//! [`DecodingSink`](crate::decode::DecodingSink) wraps a sink and adds decoded records to
//! what it forwards, so the layers compose by nesting rather than by a queue between
//! each pair:
//!
//! ```text
//! ingest ─▶ DecodingSink ─▶ ChannelSink ═ channel ═▶ ChannelReceiver::drain ─▶ DuckDbSink
//!           (same task, direct calls)                (own task, the store's writer)
//! ```
//!
//! - `channel` — the one hop that crosses tasks: a bounded in-process channel of
//!   blocks, from decode to storage. It is what lets a slow store stall without stalling
//!   ingest. Commit progress is logged from `progress`, not from the channel.
//! - `datasets` — which datasets a run fetches and keeps.
//! - `duckdb` — an embedded `DuckDB` database.
//! - `postgres` — a remote `PostgreSQL` 18 database with asynchronous transactional COPY.
//!   Both stores upsert on `(chain, dedupe_key)`.
//! - [`stdout`] — newline-delimited JSON, for watching the stream.
//!
//! Stores take a connection or open one from their own settings, so client settings
//! live beside the backend that knows how to apply them.

/// The bounded channel from decode to storage: the one hop that crosses tasks.
///
/// Enabled with either store feature: its consumer is the store's writer in
/// [`runtime`](crate::runtime). A stdout-only build has no storage task or channel.
#[cfg(any(feature = "duckdb", feature = "postgres"))]
pub(crate) mod channel;
mod datasets;
#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(any(feature = "duckdb", feature = "postgres"))]
mod progress;
pub mod stdout;

#[cfg(feature = "postgres")]
pub use postgres::{PostgresSettings, PostgresSink};

pub use datasets::Datasets;
#[cfg(feature = "duckdb")]
pub use duckdb::{DuckDbSettings, DuckDbSink};
pub use stdout::{StdoutJsonSink, StdoutSettings};

use thiserror::Error;

#[cfg(any(feature = "duckdb", feature = "postgres"))]
use crate::wire::envelope::AcceptedBlock;
use crate::wire::envelope::Envelope;
#[cfg(any(feature = "duckdb", feature = "postgres"))]
use crate::wire::envelope::Event;
#[cfg(any(feature = "duckdb", feature = "postgres"))]
use crate::wire::row::{Row, Schema, TableId, row_for};

/// What a store has been handed since its last commit: the rows to write, and the
/// blocks a buffered `reorg` retracted.
///
/// A decoded record is two rows: its generic `decoded_logs` row, and its event's typed row,
/// from the run's [`Schema`].
///
/// A store holds only the canonical chain. A `reorg` orphans blocks that are either
/// already committed or still in this buffer — blocks are published in order and the
/// storage channel is FIFO, so an orphaned block can never arrive after its `reorg`.
/// [`push`](Self::push) drops the buffered ones at once, and the store deletes the
/// committed ones by [`Table::block_hash_column`] in the same transaction that writes
/// [`rows`](Self::rows), before writing them. Deleting first is what makes a block that
/// returns — orphaned, then canonical again in a later `reorg` — end up stored: its
/// rows, published again after the `reorg` that orphaned it, are written after the
/// delete.
///
/// Both are cleared only after a commit succeeds, so a failed commit retries the deletes
/// with the rows, and a retry of a committed batch deletes nothing and upserts onto
/// itself.
#[cfg(any(feature = "duckdb", feature = "postgres"))]
#[derive(Debug)]
struct Batch {
    /// Rows to upsert, in publish order.
    rows: Vec<Row>,
    /// Orphaned block hashes to delete from the store, as `0x` hex, by chain.
    orphaned: std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
    /// Every table the run writes, which renders each envelope's rows.
    schema: std::sync::Arc<Schema>,
}

#[cfg(any(feature = "duckdb", feature = "postgres"))]
impl Batch {
    /// An empty batch writing into `schema`.
    fn new(schema: std::sync::Arc<Schema>) -> Self {
        Self {
            rows: Vec::new(),
            orphaned: std::collections::BTreeMap::new(),
            schema,
        }
    }

    /// Buffers one envelope's rows. A `reorg` first drops every buffered row of the
    /// blocks it orphans and records them for deletion; its own row is kept as the
    /// record of the retraction.
    ///
    /// # Errors
    ///
    /// Returns [`SinkError::UnknownEvent`] for a decoded record no event table holds,
    /// which means it was decoded against a different catalog than the store opened with.
    fn push(&mut self, envelope: &Envelope) -> Result<(), SinkError> {
        if let Event::Reorg(reorg) = &envelope.event
            && !reorg.orphaned_hashes.is_empty()
        {
            let chain = envelope.chain.as_str();
            let hashes: std::collections::BTreeSet<String> = reorg
                .orphaned_hashes
                .iter()
                .map(|hash| format!("{hash:#x}"))
                .collect();
            self.rows.retain(|row| {
                row.chain() != chain || !row.block_hash().is_some_and(|h| hashes.contains(h))
            });
            self.orphaned
                .entry(chain.to_owned())
                .or_default()
                .extend(hashes);
        }
        if let Event::Decoded(decoded) = &envelope.event {
            let row = self
                .schema
                .event_row(&envelope.chain, decoded)
                .ok_or_else(|| SinkError::UnknownEvent {
                    protocol: decoded.protocol.clone(),
                    contract: decoded.contract.clone(),
                    event: decoded.name.clone(),
                })?;
            self.rows.push(row);
        }
        self.rows.push(row_for(&envelope.chain, &envelope.event));
        Ok(())
    }

    /// The rows each table should load: the last buffered copy of each
    /// `(chain, dedupe_key)`, in publish order.
    ///
    /// A store's merge must not see a key twice — both engines refuse to update one
    /// conflict row twice in a statement — and "last" means last published, which only
    /// the buffer knows; the staging table's physical order does not promise it.
    fn by_table(&self) -> std::collections::HashMap<TableId, Vec<&Row>> {
        let mut seen = std::collections::HashSet::new();
        let mut tables: std::collections::HashMap<TableId, Vec<&Row>> =
            std::collections::HashMap::new();
        for row in self.rows.iter().rev() {
            if seen.insert((row.table(), row.chain(), row.dedupe_key())) {
                tables.entry(row.table()).or_default().push(row);
            }
        }
        for rows in tables.values_mut() {
            rows.reverse();
        }
        tables
    }

    /// Whether there is nothing to commit. A `reorg` always buffers its own row, so a
    /// batch with deletions is never empty.
    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Forgets everything, once a commit has made it durable.
    fn clear(&mut self) {
        self.rows.clear();
        self.orphaned.clear();
    }
}

/// `name` as a quoted SQL identifier, for a database schema named at runtime.
#[cfg(any(feature = "duckdb", feature = "postgres"))]
fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A stored discovered contract, as a store's query returns it.
///
/// Only the address and block hash need parsing: the protocol and name are matched
/// against the catalog as text.
#[cfg(any(feature = "duckdb", feature = "postgres"))]
fn stored_contract(
    protocol: String,
    name: String,
    address: &str,
    block_hash: &str,
) -> Result<crate::decode::StoredContract, InvalidStoredValue> {
    Ok(crate::decode::StoredContract {
        protocol,
        name,
        address: parse_stored("contract.address", address)?,
        block_hash: parse_stored("contract.block_hash", block_hash)?,
    })
}

/// A stored `accepted_blocks` row, as a store's query returns it.
#[cfg(any(feature = "duckdb", feature = "postgres"))]
fn stored_block(
    height: u64,
    hash: &str,
    parent_hash: &str,
    timestamp: u64,
) -> Result<AcceptedBlock, InvalidStoredValue> {
    Ok(AcceptedBlock {
        height,
        hash: parse_stored("accepted_block.hash", hash)?,
        parent_hash: parse_stored("accepted_block.parent_hash", parent_hash)?,
        timestamp,
    })
}

/// Parses one stored text column, naming the column when it does not parse.
#[cfg(any(feature = "duckdb", feature = "postgres"))]
fn parse_stored<T: std::str::FromStr>(
    column: &'static str,
    value: &str,
) -> Result<T, InvalidStoredValue> {
    value.parse().map_err(|_| InvalidStoredValue {
        column,
        value: value.to_owned(),
    })
}

/// A value read back at startup that does not parse as its column's type.
#[derive(Debug, Error)]
#[error("stored {column} is invalid: {value:?}")]
pub struct InvalidStoredValue {
    /// The table and column, for example `contract.address`.
    pub column: &'static str,
    /// The stored text.
    pub value: String,
}

/// Receives envelopes in per-chain order, as the pipeline publishes them.
///
/// The driver holds the sink through an exclusive borrow, so it may buffer across calls
/// — a rendered row, an open appender, a block awaiting its send — instead of paying the
/// engine's per-record cost. [`flush`](EnvelopeSink::flush) is the batch boundary; the
/// ingest pipeline calls it once per block, so everything published between two flushes
/// is one block's worth. A slow sink applies backpressure to whatever drives it.
pub trait EnvelopeSink: Send {
    /// Accepts one envelope.
    ///
    /// Takes the envelope by value: each sink either keeps it (a channel buffer) or
    /// renders it, and none needs a copy, so ownership moves down the chain instead of
    /// cloning per hop. May buffer; nothing is durable until [`EnvelopeSink::flush`]
    /// succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if the envelope cannot be accepted. The caller stops rather
    /// than skipping it.
    fn publish(&mut self, envelope: Envelope)
    -> impl Future<Output = Result<(), SinkError>> + Send;

    /// Ends the batch: hands everything published since the last flush onward, as one.
    ///
    /// # Errors
    ///
    /// Returns an error if the buffered envelopes cannot be delivered. The caller stops
    /// rather than continuing past a lost batch.
    fn flush(&mut self) -> impl Future<Output = Result<(), SinkError>> + Send {
        async { Ok(()) }
    }

    /// Records the newest sampled canonical head, so a commit can report its lag.
    ///
    /// The default ignores it. The storage channel keeps the latest sample for its
    /// progress line; a sink with no commit log has nothing to compare against.
    fn observe_head(&mut self, height: u64) {
        let _ = height;
    }
}

/// Why a sink could not accept, render, or deliver an envelope.
///
/// The layer's own error, and the reason [`EnvelopeSink`] is typed rather than generic:
/// every sink that implements the trait has to say what can go wrong, so a caller can
/// branch on it instead of reading a string. The variants that matter are structural
/// rather than textual — a caller distinguishes a dead store from a failed HTTP status
/// from a malformed row by matching, not by formatting.
///
/// Leaf errors arrive through `#[from]`, so a sink propagates them with `?` rather than
/// wrapping them in a message: the store's error and `serde_json`'s and
/// `std::io`'s own errors each name their cause better than this layer could.
#[derive(Debug, Error)]
pub enum SinkError {
    /// An internal decoding invariant failed; continuing could publish incorrect data.
    #[error(transparent)]
    Decode(#[from] crate::decode::DecodeError),
    /// The store this sink writes to has stopped, so the batch cannot be delivered.
    ///
    /// Distinct from a *failed* store: nothing went wrong, the destination is simply
    /// gone. A channel sink reports this when the receiving half was dropped.
    #[error("storage has stopped, so the batch cannot be delivered")]
    StorageClosed,
    /// A decoded record names an event no event table holds: it was decoded against a
    /// different catalog than the store was opened with.
    #[error("no event table holds {protocol}.{contract}.{event}")]
    UnknownEvent {
        /// The record's protocol.
        protocol: String,
        /// The record's contract.
        contract: String,
        /// The record's event name.
        event: String,
    },
    /// Rendering the envelope for a transport failed.
    #[error("serialize envelope: {0}")]
    Serialize(#[from] serde_json::Error),
    /// Writing the rendered envelope failed.
    #[error("write envelope: {0}")]
    Write(#[from] std::io::Error),
    /// A store rejected the write, or could not be opened.
    ///
    /// Transparent, so the store's own variants — the setting it refused, the path it
    /// could not open — survive to the caller instead of being flattened to a string.
    #[cfg(feature = "duckdb")]
    #[error(transparent)]
    Store(#[from] duckdb::StoreError),
    /// `PostgreSQL` connection, schema, or transactional write failed.
    #[cfg(feature = "postgres")]
    #[error(transparent)]
    Postgres(#[from] postgres::StoreError),
}

/// Fixtures the store tests share: the shipped catalog's event tables, and a real decoded
/// record to write into them.
#[cfg(all(test, any(feature = "duckdb", feature = "postgres")))]
#[expect(clippy::expect_used)]
pub(crate) mod fixtures {
    use std::sync::Arc;

    use crate::decode::{Catalog, Decoder};
    use crate::wire::envelope::{ChainId, Decoded, Envelope, Event};
    use crate::wire::row::Schema;

    fn catalog() -> Catalog {
        Catalog::load(
            format!("{}/protocols", env!("CARGO_MANIFEST_DIR")),
            &ChainId::new("base"),
        )
        .expect("shipped protocols load")
    }

    /// The shipped protocols' schema: the dataset tables and their event tables.
    pub(crate) fn schema() -> Arc<Schema> {
        Arc::new(catalog().schema().clone())
    }

    /// The first real Uniswap V3 `Swap` in the fixtures, decoded against the shipped
    /// catalog: a seeded pool, so it decodes with no discovery.
    pub(crate) fn decoded_swap() -> Decoded {
        let line = include_str!("../../examples/fixtures/uniswap_v3_swaps.ndjson")
            .lines()
            .next()
            .expect("a fixture line");
        let envelope: Envelope = serde_json::from_str(line).expect("a published envelope");
        let Event::Log(log) = envelope.event else {
            panic!("the fixture is a log");
        };
        Decoder::new(catalog())
            .decode(&log)
            .expect("decodes")
            .expect("a seeded pool's swap")
            .decoded
    }
}
