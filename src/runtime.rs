//! The pipeline the settings describe, assembled and run.
//!
//! The shape is fixed — `ingest → decode → storage` — so what varies is a few choices the
//! settings file already states. This module reads them and builds the pipeline, so the
//! binary is "load the settings, run it" rather than a place that names every layer.
//!
//! # Two tasks, one channel
//!
//! ```text
//! task 1:  ingest ─▶ DecodingSink ─▶ ChannelSink ══╗
//!                    (direct calls, no queue)      ║  bounded channel of blocks
//! task 2:  DuckDbSink ◀─ ChannelReceiver::drain ◀══╝
//! ```
//!
//! Ingest and decode are one task because decoding a block is far cheaper than the block
//! time and needs no decoupling. Storage is its own task because a store stalls, and the
//! channel between them is what keeps a stall from stopping ingest; see
//! `crate::sink::channel`. The store gets its own task — spawned, not merely polled
//! alongside ingest — because its writes block, and a blocked poll would stall ingest
//! anyway.
//!
//! # What the settings decide
//!
//! - **Ingest** follows the chain `[ingest]` names. It is required, so it always runs.
//! - **Decode** uses the registry `[decode] registry` names. An absent or empty registry
//!   decodes nothing, which is a legitimate way to run and is said at startup.
//! - **The sink** is the `[sink.<backend>]` table. With `[sink.stdout]` no store is
//!   opened at all: the stream is printed instead.
//!
//! # Shutdown
//!
//! Ingest ends only by failing, and when it does its half of the channel closes, so
//! storage drains the blocks already queued and stops. If the store fails first, ingest
//! finds out at its next block, when the send fails. Either way the store's error is
//! reported ahead of ingest's, since a dead store is the cause and a failed send the
//! symptom.

use thiserror::Error;
use tracing::info;

use crate::config::{Settings, SettingsError, Sink};
use crate::decode::DecodingSink;
use crate::decode::{ContractRegistry, RegistryError};
use crate::ingest::Ingest;
use crate::ingest::pipeline::PipelineError;
#[cfg(any(feature = "duckdb", feature = "postgres"))]
use crate::sink;
#[cfg(feature = "duckdb")]
use crate::sink::DuckDbSink;
use crate::sink::{SinkError, StdoutJsonSink};

/// Why the indexer stopped.
///
/// The top of the chain and the only place the layers meet, so each variant names *which*
/// part stopped rather than restating what went wrong: the cause is already typed one
/// level down and travels intact inside the variant. That is what makes `{error:?}` worth
/// printing at the process boundary — it walks the `#[from]` chain and shows every layer,
/// where a single string would have shown only the outermost.
///
/// The assembly failures are kept apart from the runtime ones on purpose. A bad settings
/// file or an unreadable registry is fixed by editing a file and restarting; a pipeline
/// failure is what a running indexer reports when it stops. Collapsing them would leave a
/// caller unable to tell "this deployment never started" from "this run died".
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// The settings could not be read or parsed.
    #[error("settings could not be loaded: {0}")]
    Settings(#[from] SettingsError),
    /// The registry could not be loaded, so nothing would have decoded.
    #[error("contract registry could not be loaded: {0}")]
    Registry(#[from] RegistryError),
    /// The store could not be opened, so there was nowhere to write.
    #[cfg(feature = "duckdb")]
    #[error("storage could not be opened: {0}")]
    OpenStore(#[from] sink::duckdb::StoreError),
    /// `PostgreSQL` could not be connected or initialized before ingest started.
    #[cfg(feature = "postgres")]
    #[error("PostgreSQL storage could not be opened: {0}")]
    OpenPostgres(#[from] sink::postgres::StoreError),
    /// Ingest stopped: a source failed, a sink refused an envelope, or the head
    /// subscription ended.
    #[error("ingest stopped: {0}")]
    Ingest(#[from] PipelineError),
    /// Storage stopped. Reported ahead of ingest's error, because a dead store is the
    /// cause there and the failed send is only the symptom.
    #[error("storage stopped: {0}")]
    Storage(#[from] SinkError),
    /// The storage task panicked instead of returning an error, so nothing below it
    /// ever ran to explain why.
    ///
    /// Its own variant because `JoinError` says the task died, not that the store
    /// failed, and a panic in a writer is a different bug from a rejected write.
    #[error("storage task panicked: {source}")]
    StorageTaskPanicked {
        /// `tokio`'s join error, which carries the panic payload.
        #[from]
        source: tokio::task::JoinError,
    },
}

/// The pipeline the settings describe: the parts, built and ready.
#[derive(Debug)]
pub(crate) struct Pipeline {
    ingest: Ingest,
    registry: ContractRegistry,
}

impl Pipeline {
    /// Assembles the pipeline the settings describe.
    ///
    /// Opens nothing: the store is connected in [`Pipeline::run`], so building fails on an
    /// unreadable registry without touching a database file.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::Registry`] when the registry cannot be read.
    pub(crate) fn from_settings(settings: &Settings) -> Result<Self, RuntimeError> {
        let ingest = Ingest::new(
            &settings.ingest.chain,
            &settings.ingest.http_url,
            &settings.ingest.ws_url,
        );
        let registry = settings.registry_path().map_or_else(
            || Ok(ContractRegistry::default()),
            ContractRegistry::from_file,
        )?;
        Ok(Self { ingest, registry })
    }

    /// Runs until ingest or storage stops.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::OpenStore`] when the store cannot be opened, and
    /// [`RuntimeError::Ingest`] or [`RuntimeError::Storage`] when a part fails. Each
    /// carries that part's own error, because "storage stopped" says nothing about why.
    pub(crate) async fn run(self, settings: &Settings) -> Result<(), RuntimeError> {
        let Self { ingest, registry } = self;

        // The settings' backend is the branch, so a backend this build does not have is
        // already a startup error and each arm here opens exactly what it named.
        match &settings.sink {
            Sink::Stdout(_) => {
                ingest
                    .run(DecodingSink::new(registry, StdoutJsonSink::new()))
                    .await?;
                Ok(())
            }
            #[cfg(feature = "postgres")]
            Sink::Postgres(postgres) => {
                let mut store = sink::PostgresSink::open(postgres).await?;
                let (blocks, receiver) = sink::channel::ChannelSink::new();
                let batch_records = postgres.batch_records;
                let storage =
                    tokio::spawn(async move { receiver.drain(&mut store, batch_records).await });
                let ingest = ingest.run(DecodingSink::new(registry, blocks)).await;
                finish(storage.await, ingest)
            }
            #[cfg(feature = "duckdb")]
            Sink::DuckDb(duckdb) => {
                // Open the store before ingest starts, so a bad path fails at startup
                // rather than after the first block.
                let mut store = DuckDbSink::open(duckdb)?;

                let (blocks, receiver) = sink::channel::ChannelSink::new();
                let batch_records = duckdb.batch_records;
                // `ponytail:` the store's writes block, so this holds one runtime worker
                // for the length of each flush. Fine on the multi-threaded runtime the
                // binary uses; a dedicated blocking thread is the upgrade if the store
                // gets slow enough to starve other tasks.
                let storage =
                    tokio::spawn(async move { receiver.drain(&mut store, batch_records).await });

                let ingest = ingest.run(DecodingSink::new(registry, blocks)).await;

                // Ingest's half of the channel is gone by now, so storage drains what is
                // queued and ends. The join order is [`finish`].
                finish(storage.await, ingest)
            }
        }
    }
}

/// Reports how the two tasks stopped.
///
/// Storage comes first. Its error is a rejected write, and its panic is a `JoinError`:
/// the writer died outside the store's own error path (an unwrap on a row width, say),
/// which is neither a store failure nor an ingest failure. Ingest's error on this path
/// is the failed send that followed — [`crate::sink::SinkError::StorageClosed`] — so it
/// is returned only when the store itself finished.
#[cfg(any(feature = "duckdb", feature = "postgres"))]
fn finish(
    storage: Result<Result<u64, SinkError>, tokio::task::JoinError>,
    ingest: Result<(), PipelineError>,
) -> Result<(), RuntimeError> {
    let stored = storage??;
    info!(stored, "storage stopped");
    ingest?;
    Ok(())
}

/// Loads the settings at `path` and runs the pipeline they describe.
///
/// # Errors
///
/// Returns [`RuntimeError`] naming whichever layer stopped: the settings if they could
/// not be read, the pipeline if it could not be assembled, and otherwise the part that
/// failed while running.
pub async fn run(path: &str) -> Result<(), RuntimeError> {
    let settings = Settings::from_file(path)?;
    info!(
        settings = path,
        chain = %settings.ingest.chain,
        registry = settings
            .registry_path()
            .map_or_else(|| "-".to_owned(), |path| path.display().to_string()),
        storage = ?settings.sink,
        "starting"
    );
    Pipeline::from_settings(&settings)?.run(&settings).await
}

#[cfg(all(test, feature = "duckdb"))]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are allowed
// them per the repository test style, since a failed expectation there means the fixture
// or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::str::FromStr as _;

    use crate::config::Settings;
    use crate::decode::DecodingSink;
    use crate::decode::{AbiEntry, ContractEntry, ContractRegistry, RegistryConfig};
    use crate::ingest::pipeline::PipelineError;
    use crate::sink::duckdb::StoreError;
    use crate::sink::{self, DuckDbSink, EnvelopeSink as _, SinkError};
    use crate::wire::envelope::Envelope;

    use super::{Pipeline, RuntimeError, finish};

    /// The shipped registry, as an absolute path so [`Settings::from_str`] carries no
    /// directory to resolve it against.
    fn registry() -> String {
        format!("{}/registry.toml", env!("CARGO_MANIFEST_DIR"))
    }

    const SETTINGS: &str = r#"
[ingest]
chain = "base"
http_url = "https://example.invalid"
ws_url = "wss://example.invalid"

[sink.duckdb]
"#;

    /// A configured chain is the pipeline: assembly needs no store and no network, so a
    /// bad setting fails here rather than after a database file has been touched.
    #[test]
    fn a_chain_config_assembles_the_pipeline() {
        let settings = Settings::from_str(SETTINGS).expect("settings parse");
        Pipeline::from_settings(&settings).expect("the pipeline builds");
    }

    /// The store's write error is the one returned when ingest then fails because the
    /// receiver is gone. That second failure is the failed send, and reporting it would
    /// hide the write that caused it.
    #[test]
    fn a_failed_store_is_reported_ahead_of_the_closed_send() {
        let connection = duckdb::Connection::open_in_memory().expect("open in-memory DuckDB");
        connection
            .execute_batch("CREATE TABLE block (not_a_column INTEGER)")
            .expect("schema");
        let source = connection
            .execute("INSERT INTO block VALUES (1, 2)", [])
            .expect_err("two values cannot fit one column");
        let storage = Ok(Err(SinkError::Store(StoreError::Append {
            table: "block",
            source,
        })));
        let ingest = Err(PipelineError::Sink(SinkError::StorageClosed));

        let error = finish(storage, ingest).expect_err("the store rejected the batch");
        assert!(
            matches!(
                error,
                RuntimeError::Storage(SinkError::Store(StoreError::Append { table: "block", .. }))
            ),
            "the append error must be the one reported, not the closed send: {error:?}"
        );
    }

    /// A registry that names a file that is not there is an assembly error, not a quiet
    /// run that decodes nothing.
    #[test]
    fn a_missing_registry_file_is_an_assembly_error() {
        let settings = Settings::from_str(&format!(
            "{SETTINGS}\n[decode]\nregistry = \"/nonexistent/registry.toml\"\n"
        ))
        .expect("settings parse");
        Pipeline::from_settings(&settings).expect_err("the registry cannot be read");
    }

    /// The shipped registry loads, so the example settings a reader copies stay runnable.
    #[test]
    fn the_shipped_registry_assembles() {
        let settings = Settings::from_str(&format!(
            "{SETTINGS}\n[decode]\nregistry = {:?}\n",
            registry()
        ))
        .expect("settings parse");
        Pipeline::from_settings(&settings).expect("the pipeline builds");
    }

    /// The whole hand-off, minus the network: a raw log goes through decode, over the
    /// channel, and into the store, and its raw row and decoded row land in the same
    /// commit. This is the seam the runtime wires, so it is checked end to end.
    #[tokio::test]
    async fn a_block_lands_in_the_store_raw_and_decoded_together() {
        const POOL: &str = "0xd0b53D9277642d899DF5C87A3966A349A798F224";
        let registry = ContractRegistry::load(
            &RegistryConfig {
                abi: vec![AbiEntry {
                    name: "uniswap_v3_pool".to_owned(),
                    path: concat!(env!("CARGO_MANIFEST_DIR"), "/abis/uniswap_v3_pool.json").into(),
                }],
                contract: vec![ContractEntry {
                    chain: "base".to_owned(),
                    address: POOL.to_owned(),
                    abi: "uniswap_v3_pool".to_owned(),
                    protocol: "uniswap_v3".to_owned(),
                    from_block: 0,
                    to_block: None,
                }],
            },
            ".",
        )
        .expect("the registry loads");
        let swap: Envelope = serde_json::from_str(
            include_str!("../examples/fixtures/uniswap_v3_swaps.ndjson")
                .lines()
                .next()
                .expect("a fixture line"),
        )
        .expect("the fixture is a published envelope");

        let connection = duckdb::Connection::open_in_memory().expect("open in-memory DuckDB");
        let reader = connection.try_clone().expect("a second handle");
        let mut store = DuckDbSink::new(connection).expect("create events table");
        let (blocks, receiver) = sink::channel::ChannelSink::new();
        let storage = tokio::spawn(async move { receiver.drain(&mut store, 500).await });

        let mut decoding = DecodingSink::new(registry, blocks);
        decoding.publish(swap).await.expect("publish");
        decoding.flush().await.expect("flush");
        drop(decoding);

        let stored = storage.await.expect("no panic").expect("storage drains");
        assert_eq!(stored, 2, "the raw log and its decoded record");

        // One table per dataset: the log is a row of typed columns in `log` and the decode
        // beside it in `decoded`, not two rows of a shared `events` table.
        let log_count: i64 = reader
            .query_row("SELECT count(*) FROM log", [], |row| row.get(0))
            .expect("count logs");
        let decoded_count: i64 = reader
            .query_row("SELECT count(*) FROM decoded", [], |row| row.get(0))
            .expect("count decoded");
        assert_eq!((log_count, decoded_count), (1, 1));

        // The decoded row keys back to the raw log it came from.
        let decoded_key: String = reader
            .query_row("SELECT dedupe_key FROM decoded", [], |row| row.get(0))
            .expect("the decoded key");
        let log_key: String = reader
            .query_row("SELECT dedupe_key FROM log", [], |row| row.get(0))
            .expect("the log key");
        assert!(
            decoded_key.starts_with(&log_key),
            "the decoded row must key back to its log: {decoded_key} vs {log_key}"
        );
    }
}
