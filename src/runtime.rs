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
//! - **Decode** uses the protocol manifests `[decode] protocols` names. An absent or empty
//!   catalog decodes nothing, which is a legitimate way to run and is said at startup.
//!   Contracts a previous run discovered are read back from the store before ingest
//!   starts, so a restart decodes them; `[sink.stdout]` has no store and starts with the
//!   manifests' seeds only.
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
use tracing::{info, warn};

use crate::config::{Settings, SettingsError, Sink};
use crate::decode::{Catalog, CatalogError, Decoder, DecodingSink};
use crate::ingest::Ingest;
use crate::ingest::pipeline::PipelineError;
#[cfg(any(feature = "duckdb", feature = "postgres"))]
use crate::sink;
#[cfg(feature = "duckdb")]
use crate::sink::DuckDbSink;
use crate::sink::{SinkError, StdoutJsonSink};
use crate::wire::envelope::ChainId;

/// Why the indexer stopped.
///
/// The top of the chain and the only place the layers meet, so each variant names *which*
/// part stopped rather than restating what went wrong: the cause is already typed one
/// level down and travels intact inside the variant. That is what makes `{error:?}` worth
/// printing at the process boundary — it walks the `#[from]` chain and shows every layer,
/// where a single string would have shown only the outermost.
///
/// The assembly failures are kept apart from the runtime ones on purpose. A bad settings
/// file or unreadable manifests are fixed by editing a file and restarting; a pipeline
/// failure is what a running indexer reports when it stops. Collapsing them would leave a
/// caller unable to tell "this deployment never started" from "this run died".
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// The settings could not be read or parsed.
    #[error("settings could not be loaded: {0}")]
    Settings(#[from] SettingsError),
    /// The protocol manifests could not be loaded, so nothing would have decoded.
    #[error("protocol manifests could not be loaded: {0}")]
    Catalog(#[from] CatalogError),
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
    decoder: Decoder,
}

impl Pipeline {
    /// Assembles the pipeline the settings describe.
    ///
    /// Opens nothing: the store is connected in [`Pipeline::run`], so building fails on
    /// unreadable manifests without touching a database file.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::Catalog`] when the manifests cannot be loaded.
    pub(crate) fn from_settings(settings: &Settings) -> Result<Self, RuntimeError> {
        let ingest = Ingest::new(
            &settings.ingest.chain,
            settings.ingest.http_url.clone(),
            settings.ingest.ws_url.clone(),
            &settings.ingest.datasets,
            &settings.ingest.log_addresses,
            settings.ingest.start_block,
        )
        .map_err(PipelineError::from)?;
        let mut catalog = settings.protocols_path().map_or_else(
            || Ok(Catalog::default()),
            |path| Catalog::load(path, &ChainId::new(&settings.ingest.chain)),
        )?;
        // The address filter is an explicit "only these contracts", fixed at startup, so
        // a discovered contract's logs would never be fetched. Respect it instead.
        if catalog.discovers() && !settings.ingest.log_addresses.is_empty() {
            warn!("ingest.log_addresses is set; created_by discovery is disabled");
            catalog.disable_discovery();
        }
        Ok(Self {
            ingest,
            decoder: Decoder::new(catalog),
        })
    }

    /// Runs until ingest or storage stops.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::OpenStore`] when the store cannot be opened, and
    /// [`RuntimeError::Ingest`] or [`RuntimeError::Storage`] when a part fails. Each
    /// carries that part's own error, because "storage stopped" says nothing about why.
    pub(crate) async fn run(self, settings: &Settings) -> Result<(), RuntimeError> {
        let Self { ingest, decoder } = self;
        if !settings.ingest.datasets.log && decoder.contracts() > 0 {
            warn!("log dataset is not selected; registered contracts will not decode");
        }

        // The settings' backend is the branch, so a backend this build does not have is
        // already a startup error and each arm here opens exactly what it named.
        match &settings.sink {
            Sink::Stdout(_) => {
                // No store, so nothing to restore: a run starts from the manifests' seeds.
                ingest
                    .run(DecodingSink::new(decoder, StdoutJsonSink::new()))
                    .await?;
                Ok(())
            }
            #[cfg(feature = "postgres")]
            Sink::Postgres(postgres) => {
                let mut store = sink::PostgresSink::open(postgres).await?;
                let chain = ChainId::new(&settings.ingest.chain);
                let decoder = restore(decoder, &chain, store.contracts(&chain).await?);
                let (blocks, receiver) = sink::channel::ChannelSink::new();
                let batch_records = postgres.batch_records;
                let storage =
                    tokio::spawn(async move { receiver.drain(&mut store, batch_records).await });
                let ingest = ingest.run(DecodingSink::new(decoder, blocks)).await;
                finish(storage.await, ingest)
            }
            #[cfg(feature = "duckdb")]
            Sink::DuckDb(duckdb) => {
                // Open the store before ingest starts, so a bad path fails at startup
                // rather than after the first block.
                let mut store = DuckDbSink::open(duckdb)?;
                let chain = ChainId::new(&settings.ingest.chain);
                let decoder = restore(decoder, &chain, store.contracts(&chain)?);

                let (blocks, receiver) = sink::channel::ChannelSink::new();
                let batch_records = duckdb.batch_records;
                // `ponytail:` the store's writes block, so this holds one runtime worker
                // for the length of each flush. Fine on the multi-threaded runtime the
                // binary uses; a dedicated blocking thread is the upgrade if the store
                // gets slow enough to starve other tasks.
                let storage =
                    tokio::spawn(async move { receiver.drain(&mut store, batch_records).await });

                let ingest = ingest.run(DecodingSink::new(decoder, blocks)).await;

                // Ingest's half of the channel is gone by now, so storage drains what is
                // queued and ends. The join order is [`finish`].
                finish(storage.await, ingest)
            }
        }
    }
}

/// Adds the contracts a previous run discovered, as the store read them back.
#[cfg(any(feature = "duckdb", feature = "postgres"))]
fn restore(
    mut decoder: Decoder,
    chain: &ChainId,
    stored: Vec<crate::decode::StoredContract>,
) -> Decoder {
    let restored = decoder.restore(stored);
    info!(%chain, restored, contracts = decoder.contracts(), "discovered contracts restored");
    decoder
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
        protocols = settings
            .protocols_path()
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

    use alloy_dyn_abi::DynSolValue;
    use alloy_primitives::{Address, B256, keccak256};

    use crate::config::Settings;
    use crate::decode::{Catalog, Decoder, DecodingSink, StoredContract};
    use crate::ingest::pipeline::PipelineError;
    use crate::sink::duckdb::StoreError;
    use crate::sink::{self, DuckDbSink, EnvelopeSink as _, SinkError};
    use crate::wire::envelope::{ChainId, Envelope, Event, Log, Reorg};

    use super::{Pipeline, RuntimeError, finish};

    /// The shipped protocols, as an absolute path so [`Settings::from_str`] carries no
    /// directory to resolve it against.
    fn protocols() -> String {
        format!("{}/protocols", env!("CARGO_MANIFEST_DIR"))
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

    /// A protocols directory that is not there is an assembly error, not a quiet run that
    /// decodes nothing.
    #[test]
    fn a_missing_protocols_directory_is_an_assembly_error() {
        let settings = Settings::from_str(&format!(
            "{SETTINGS}\n[decode]\nprotocols = \"/nonexistent/protocols\"\n"
        ))
        .expect("settings parse");
        assert!(matches!(
            Pipeline::from_settings(&settings),
            Err(RuntimeError::Catalog(_))
        ));
    }

    /// The shipped protocols load, so the example settings a reader copies stay runnable,
    /// with or without a log address filter, which turns discovery off.
    #[test]
    fn the_shipped_protocols_assemble_with_and_without_an_address_filter() {
        let decode = format!("\n[decode]\nprotocols = {:?}\n", protocols());
        let settings = Settings::from_str(&format!("{SETTINGS}{decode}")).expect("settings parse");
        Pipeline::from_settings(&settings).expect("the pipeline builds");

        let filtered = SETTINGS.replace(
            "ws_url = \"wss://example.invalid\"",
            "ws_url = \"wss://example.invalid\"\ndatasets = [\"log\"]\n\
             log_addresses = [\"0x1111111111111111111111111111111111111111\"]",
        );
        let settings = Settings::from_str(&format!("{filtered}{decode}")).expect("settings parse");
        let mut pipeline = Pipeline::from_settings(&settings).expect("the pipeline builds");
        let Event::Log(created) = pool_created(Address::from([0xd0; 20]), B256::ZERO).event else {
            panic!("log");
        };
        let decoding = pipeline
            .decoder
            .decode(&created)
            .expect("decode")
            .expect("the factory still decodes");
        assert!(decoding.discovered.is_empty(), "discovery is off");
    }

    fn decoder() -> Decoder {
        Decoder::new(
            Catalog::load(protocols(), &ChainId::new("base")).expect("shipped protocols load"),
        )
    }

    /// A Uniswap V3 `PoolCreated` from the Base factory naming `pool`, in `block`.
    fn pool_created(pool: Address, block: B256) -> Envelope {
        let log = Log {
            address: "0x33128a8fC17869897dcE68Ed026d694621f6FDfD"
                .parse()
                .expect("factory"),
            topic0: Some(keccak256(
                "PoolCreated(address,address,uint24,int24,address)",
            )),
            topic1: Some(B256::with_last_byte(1)),
            topic2: Some(B256::with_last_byte(2)),
            topic3: Some(B256::with_last_byte(3)),
            data: DynSolValue::Tuple(vec![
                DynSolValue::Int(alloy_primitives::I256::try_from(60).expect("int"), 24),
                DynSolValue::Address(pool),
            ])
            .abi_encode_params()
            .into(),
            log_index: 1,
            block_number: 10,
            block_hash: block,
            ..Log::default()
        };
        Envelope::new(ChainId::new("base"), Event::Log(Box::new(log)))
    }

    /// A real Uniswap V3 `Swap`, re-addressed to `pool`.
    fn swap(pool: Address) -> Envelope {
        let mut swap: Envelope = serde_json::from_str(
            include_str!("../examples/fixtures/uniswap_v3_swaps.ndjson")
                .lines()
                .next()
                .expect("a fixture line"),
        )
        .expect("the fixture is a published envelope");
        let Event::Log(log) = &mut swap.event else {
            panic!("log");
        };
        log.address = pool;
        swap
    }

    fn count(reader: &duckdb::Connection, table: &str) -> i64 {
        reader
            .query_row(&format!("SELECT count(*) FROM \"{table}\""), [], |row| {
                row.get(0)
            })
            .expect("count")
    }

    /// The whole hand-off, minus the network: a factory's creation log goes through
    /// decode, over the channel, and into the store with the pool it discovered; the
    /// pool's swap decodes in the same batch; a restart reads the pool back and decodes
    /// it; and once a reorg orphans the creating block, a restart no longer does.
    #[tokio::test]
    async fn a_discovered_contract_is_stored_and_survives_a_restart() {
        let pool = Address::from([0xd0; 20]);
        let creating = B256::with_last_byte(0xcc);
        let chain = ChainId::new("base");

        let connection = duckdb::Connection::open_in_memory().expect("open in-memory DuckDB");
        let reader = connection.try_clone().expect("a second handle");
        let mut store = DuckDbSink::new(connection).expect("create tables");
        let (blocks, receiver) = sink::channel::ChannelSink::new();
        let storage = tokio::spawn(async move { receiver.drain(&mut store, 500).await });

        let mut decoding = DecodingSink::new(decoder(), blocks);
        decoding
            .publish(pool_created(pool, creating))
            .await
            .expect("creation");
        decoding.publish(swap(pool)).await.expect("swap");
        decoding.flush().await.expect("flush");
        drop(decoding);
        storage.await.expect("no panic").expect("storage drains");

        // One table per dataset: the raw logs, their decodes, and the discovery.
        assert_eq!(
            [
                count(&reader, "log"),
                count(&reader, "decoded"),
                count(&reader, "contract")
            ],
            [2, 2, 1]
        );
        // The decoded rows key back to the raw logs they came from.
        let orphans: i64 = reader
            .query_row(
                "SELECT count(*) FROM decoded d WHERE NOT EXISTS \
                 (SELECT 1 FROM log l WHERE starts_with(d.dedupe_key, l.dedupe_key))",
                [],
                |row| row.get(0),
            )
            .expect("join");
        assert_eq!(orphans, 0);

        // A restart: a fresh decoder knows the pool once the store's rows are restored.
        let mut store = DuckDbSink::new(reader.try_clone().expect("handle")).expect("reopen");
        let stored = store.contracts(&chain).expect("read back");
        assert_eq!(
            stored,
            [StoredContract {
                protocol: "uniswap_v3".to_owned(),
                name: "UniswapV3Pool".to_owned(),
                address: pool,
            }]
        );
        let mut restarted = decoder();
        assert_eq!(restarted.restore(stored), 1);
        let Event::Log(log) = swap(pool).event else {
            panic!("log");
        };
        assert!(restarted.decode(&log).expect("decode").is_some());

        // The creating block is orphaned: the stored row stays, but is not restored.
        store
            .publish(Envelope::new(
                chain.clone(),
                Event::Reorg(Reorg {
                    height: 10,
                    new_head_hash: B256::with_last_byte(0xdd),
                    orphaned_hashes: vec![creating],
                }),
            ))
            .await
            .expect("publish");
        store.flush().await.expect("flush");
        assert!(store.contracts(&chain).expect("read back").is_empty());
        assert_eq!(count(&reader, "contract"), 1);
    }
}
