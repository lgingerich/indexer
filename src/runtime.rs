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

use anyhow::{Context as _, Result};
use tracing::info;

use crate::config::{Settings, Sink};
use crate::decode::DecodingSink;
use crate::decode::registry::ContractRegistry;
use crate::ingest::Ingest;
use crate::sink::{self, DuckDbSink, StdoutJsonSink};

/// The pipeline the settings describe: the parts, built and ready.
#[derive(Debug)]
pub(crate) struct Pipeline {
    ingest: Ingest,
    registry: ContractRegistry,
}

impl Pipeline {
    /// Assembles the pipeline the settings describe.
    ///
    /// Opens nothing: the store is connected in [`Pipeline::run`], so building fails on a
    /// bad registry or endpoint without touching a database file.
    ///
    /// # Errors
    ///
    /// Returns an error when the registry cannot be read or an ingest endpoint is
    /// missing.
    pub(crate) fn from_settings(settings: &Settings) -> Result<Self> {
        let ingest = Ingest::builder(&settings.ingest.chain)
            .http_url(&settings.ingest.http_url)
            .ws_url(&settings.ingest.ws_url)
            .build()?;
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
    /// Returns an error when the store cannot be opened, or when a part fails. The
    /// part's own error is carried with it, because "storage stopped" says nothing about
    /// why.
    pub(crate) async fn run(self, settings: &Settings) -> Result<()> {
        let Self { ingest, registry } = self;

        // The settings' backend is the branch, so a backend this build does not have is
        // already a startup error and each arm here opens exactly what it named.
        match &settings.sink {
            Sink::Stdout(_) => ingest
                .run(DecodingSink::new(registry, StdoutJsonSink::new()))
                .await
                .context("ingest stopped"),
            Sink::DuckDb(duckdb) => {
                // Open the store before ingest starts, so a bad path fails at startup
                // rather than after the first block.
                let mut store = DuckDbSink::open(duckdb)?;

                let (blocks, receiver) = sink::channel::open();
                let batch_records = duckdb.batch_records;
                // `ponytail:` the store's writes block, so this holds one runtime worker
                // for the length of each flush. Fine on the multi-threaded runtime the
                // binary uses; a dedicated blocking thread is the upgrade if the store
                // gets slow enough to starve other tasks.
                let storage =
                    tokio::spawn(async move { receiver.drain(&mut store, batch_records).await });

                let ingest = ingest.run(DecodingSink::new(registry, blocks)).await;

                // Ingest's half of the channel is gone by now, so storage drains what is
                // queued and ends. Its error comes first: if the store died, ingest's
                // failure is only the failed send.
                let stored = storage
                    .await
                    .context("storage task panicked")?
                    .context("storage stopped")?;
                info!(stored, "storage stopped");
                ingest.context("ingest stopped")
            }
        }
    }
}

/// Loads the settings at `path` and runs the pipeline they describe.
///
/// # Errors
///
/// Returns an error when the settings cannot be read, or the pipeline cannot be built or
/// run.
pub async fn run(path: &str) -> Result<()> {
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

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are allowed
// them per the repository test style, since a failed expectation there means the fixture
// or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::str::FromStr as _;

    use crate::config::Settings;
    use crate::decode::DecodingSink;
    use crate::decode::registry::{AbiEntry, ContractEntry, ContractRegistry};
    use crate::sink::{self, DuckDbSink, EnvelopeSink as _};
    use crate::wire::envelope::Envelope;

    use super::Pipeline;

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
            &[AbiEntry {
                name: "uniswap_v3_pool".to_owned(),
                path: concat!(env!("CARGO_MANIFEST_DIR"), "/abis/uniswap_v3_pool.json").into(),
            }],
            &[ContractEntry {
                chain: "base".to_owned(),
                address: POOL.to_owned(),
                abi: "uniswap_v3_pool".to_owned(),
            }],
            &[],
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
        let (blocks, receiver) = sink::channel::open();
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
