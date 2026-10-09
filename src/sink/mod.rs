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
//! - `duckdb` — an embedded `DuckDB` database.
//! - `postgres` — a remote `PostgreSQL` 18 database with asynchronous transactional COPY.
//! - [`table`] — the tables a store persists, declared once; `sql` renders their
//!   statements, and [`store`] is the write path both stores share. Both upsert on
//!   `(chain, dedupe_key)`.
//! - [`stdout`] — newline-delimited JSON, for watching the stream.
//! - `delta` — Delta Lake tables on object storage: the cheap raw-data lake. Not a SQL
//!   store: it appends, and holds replays out with its ledger instead of upserting.
//! - `batch` — the rows a store buffers between commits, with a `reorg`'s orphans
//!   dropped and the last copy of each row kept, the same for all three stores.
//! - `store_error` — the `StoreError` all three stores return.
//!
//! Stores take a connection or open one from their own settings, so client settings
//! live beside the backend that knows how to apply them.

/// The rows a store buffers between commits, which every store fills the same way.
#[cfg(feature = "store")]
mod batch;
/// The bounded channel from decode to storage: the one hop that crosses tasks.
///
/// Enabled with any store feature: its consumer is the store's writer in
/// [`runtime`](crate::runtime). A stdout-only build has no storage task or channel.
#[cfg(feature = "store")]
pub(crate) mod channel;
#[cfg(feature = "delta")]
pub mod delta;
#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "store")]
mod progress;
#[cfg(any(feature = "duckdb", feature = "postgres"))]
pub mod sql;
pub mod stdout;
#[cfg(any(feature = "duckdb", feature = "postgres"))]
pub mod store;
#[cfg(feature = "store")]
pub mod store_error;
pub mod table;

#[cfg(feature = "postgres")]
pub use postgres::{PostgresSettings, PostgresSink};

#[cfg(feature = "delta")]
pub use delta::{DeltaSettings, DeltaSink};
#[cfg(feature = "duckdb")]
pub use duckdb::{DuckDbSettings, DuckDbSink};
pub use stdout::{StdoutJsonSink, StdoutSettings};
#[cfg(any(feature = "duckdb", feature = "postgres"))]
pub use store::SqlStore;
#[cfg(feature = "store")]
pub use store_error::{InvalidStoredValue, Operation, StoreError};

use thiserror::Error;

use crate::wire::envelope::Envelope;
#[cfg(feature = "store")]
use crate::wire::envelope::{BlockMeta, StoredContract};

/// Receives envelopes in per-chain order, as the pipeline publishes them.
///
/// The driver holds the sink through an exclusive borrow, so it may buffer across calls
/// — a rendered row, an open appender, a block awaiting its send — instead of paying the
/// engine's per-record cost. [`flush`](EnvelopeSink::flush) is the batch boundary; the
/// ingest pipeline calls it once per block, and the storage drain once per batch of
/// whole blocks it gathers for a store. A slow sink applies backpressure to whatever
/// drives it.
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

    /// Roughly how many bytes the sink holds that the next flush would write, as they
    /// sit in memory. The storage drain flushes early once this passes its limit.
    ///
    /// The default is zero: a sink that does not count is bounded by records alone.
    fn buffered_bytes(&self) -> usize {
        0
    }

    /// Records the newest sampled canonical head, so a commit can report its lag.
    ///
    /// The default ignores it. The storage channel keeps the latest sample for its
    /// progress line; a sink with no commit log has nothing to compare against.
    fn observe_head(&mut self, height: u64) {
        let _ = height;
    }
}

/// What a previous run left in a store for the next one: the contracts it discovered,
/// and the accepted blocks ingest resumes from.
#[cfg(feature = "store")]
#[derive(Debug, Default)]
pub struct Restored {
    /// Every contract discovered on the store's chain, each from a canonical block.
    pub contracts: Vec<StoredContract>,
    /// The newest [`LEDGER_WINDOW`](crate::ingest::pipeline::LEDGER_WINDOW) accepted
    /// blocks, oldest first: the undo window a restart resumes from.
    pub ledger: Vec<BlockMeta>,
}

/// A sink that persists, for one chain: what the storage task writes, and what a restart
/// reads back before it ingests.
#[cfg(feature = "store")]
pub trait Store: EnvelopeSink + 'static {
    /// Reads back what a previous run left, once, at startup and before the first
    /// envelope is published.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when a read fails and [`StoreError::Restore`] when
    /// a stored value does not parse.
    fn restore(&mut self) -> impl Future<Output = Result<Restored, StoreError>> + Send;
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
    /// Boxed so this layer does not name the decoder's error type.
    #[error("decoding invariant failed: {0}")]
    Decode(#[source] Box<dyn std::error::Error + Send + Sync>),
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
    /// An envelope's row could not be built.
    #[error(transparent)]
    Table(#[from] table::TableError),
    /// A store could not be opened, read, or written.
    #[cfg(feature = "store")]
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// Fixtures the store tests share: the shipped catalog's event tables, and a real decoded
/// record to write into them.
#[cfg(all(test, feature = "store"))]
#[expect(clippy::expect_used)]
pub(crate) mod fixtures {
    use std::sync::Arc;

    use crate::decode::{Catalog, Decoder};
    use crate::sink::table::Schema;
    use crate::wire::envelope::{ChainId, Decoded, Envelope, Event};

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

    /// A Metric `PoolCreated` carrying a real Base pool's arguments, decoded against the
    /// shipped catalog: an `address[]`, a tuple of `uint256`s, and `uint256[]`s of packed
    /// words beside scalars up to `type(uint256).max`.
    pub(crate) fn decoded_pool_created() -> Decoded {
        use alloy_dyn_abi::DynSolValue;
        use alloy_json_abi::JsonAbi;
        use alloy_primitives::{Address, B256, U256, address};

        let abi: JsonAbi = serde_json::from_str(include_str!(
            "../../protocols/metric/v1/MetricOmmPoolFactory.json"
        ))
        .expect("the factory ABI parses");
        let event = abi.event("PoolCreated").expect("the event")[0].clone();
        let topic = |address: Address| Some(address.into_word());
        let uint = |value: u64| DynSolValue::Uint(U256::from(value), 256);
        let packed =
            U256::from_str_radix("190000000000190000000000190000000000190000000000190", 16)
                .expect("a packed word");
        let words = DynSolValue::Array(vec![DynSolValue::Uint(packed, 256); 4]);
        let log = crate::wire::envelope::Log {
            address: address!("0x2a53833cc95548cf52c7b159110e22d3a9018f32"),
            topic0: Some(event.selector()),
            topic1: topic(address!("0xb030150465b706f81eecc453ba5e9bae7b06e50d")),
            topic2: topic(address!("0x0b3e328455c4059eeb9e3f84b5543f74e24e7e1b")),
            topic3: topic(address!("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")),
            data: DynSolValue::Tuple(vec![
                uint(92),
                DynSolValue::Address(address!("0x2a53833cc95548cf52c7b159110e22d3a9018f32")),
                DynSolValue::Address(address!("0x80f0a7d148729ccedb6cd07fbaaefc5a16f33d4f")),
                DynSolValue::Address(address!("0xb1a246b1131ff328067c4aaf4f772ff351475244")),
                DynSolValue::Array(vec![
                    DynSolValue::Address(address!("0xb1a246b1131ff328067c4aaf4f772ff351475244")),
                    DynSolValue::Address(address!("0xe4038fa09f0bb9963068afaf97be0c045155090d")),
                    DynSolValue::Address(address!("0xebc53e61078976118e384f110c262a263decb84b")),
                ]),
                DynSolValue::Tuple(vec![uint(0), uint(0), uint(0), uint(0), uint(3), uint(10)]),
                DynSolValue::Uint(U256::MAX, 256),
                uint(1_244_090_569_793_480_965),
                uint(1_000_000_000_000_000_000),
                uint(1_000_000_000_000_000_000),
                DynSolValue::Uint(U256::from(150_000), 24),
                DynSolValue::Uint(U256::ZERO, 24),
                DynSolValue::Uint(U256::from(200_000), 24),
                DynSolValue::Uint(U256::ZERO, 24),
                DynSolValue::Address(address!("0x53f0ace6156daf7f6b50b395f6dd66c9d2f50c8c")),
                DynSolValue::Int(alloy_primitives::I256::ZERO, 24),
                words.clone(),
                words,
            ])
            .abi_encode_params()
            .into(),
            block_hash: B256::with_last_byte(0x0b),
            ..crate::wire::envelope::Log::default()
        };
        Decoder::new(catalog())
            .decode(&log)
            .expect("decodes")
            .expect("the seeded factory's event")
            .decoded
    }
}
