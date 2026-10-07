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
//! - **Where ingest starts.** A store's ledger of accepted blocks is read back before
//!   ingest starts, and a run resumes after its tip; see `crate::ingest::pipeline`. An
//!   empty ledger starts at `ingest.start_block` or the head, and `[sink.stdout]`, which
//!   has no store, always starts fresh.
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
use crate::ingest::{pipeline::MAX_UNFINALIZED_BLOCKS, source::BlockMeta};
#[cfg(any(feature = "duckdb", feature = "postgres"))]
use crate::sink;
#[cfg(feature = "duckdb")]
use crate::sink::DuckDbSink;
use crate::sink::{SinkError, StdoutJsonSink};
#[cfg(any(feature = "duckdb", feature = "postgres"))]
use crate::wire::envelope::AcceptedBlock;
use crate::wire::envelope::ChainId;

/// Why the indexer stopped.
///
/// The top of the chain and the only place the layers meet, so each variant names *which*
/// part stopped rather than restating what went wrong: the cause is already typed one
/// level down and travels intact inside the variant. Every variant prints that cause —
/// `{source}` or `transparent` — so the `Display` the process boundary logs shows every
/// layer in one line, where `Debug` would bury the same chain under the raw values behind
/// it.
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
                // No store, so nothing to restore: a run starts fresh from the manifests'
                // seeds.
                ingest
                    .run(
                        DecodingSink::new(decoder, StdoutJsonSink::new()),
                        Vec::new(),
                    )
                    .await?;
                Ok(())
            }
            #[cfg(feature = "postgres")]
            Sink::Postgres(postgres) => {
                let mut store = sink::PostgresSink::open(postgres).await?;
                let chain = ChainId::new(&settings.ingest.chain);
                let decoder = restore(decoder, &chain, store.contracts(&chain).await?);
                let ledger = ledger(&chain, store.ledger(&chain, LEDGER_WINDOW).await?);
                let (blocks, receiver) = sink::channel::ChannelSink::new();
                let batch_records = postgres.batch_records;
                let storage =
                    tokio::spawn(async move { receiver.drain(&mut store, batch_records).await });
                let ingest = ingest.run(DecodingSink::new(decoder, blocks), ledger).await;
                finish(storage.await, ingest)
            }
            #[cfg(feature = "duckdb")]
            Sink::DuckDb(duckdb) => {
                // Open the store before ingest starts, so a bad path fails at startup
                // rather than after the first block.
                let mut store = DuckDbSink::open(duckdb)?;
                let chain = ChainId::new(&settings.ingest.chain);
                let decoder = restore(decoder, &chain, store.contracts(&chain)?);
                let ledger = ledger(&chain, store.ledger(&chain, LEDGER_WINDOW)?);

                let (blocks, receiver) = sink::channel::ChannelSink::new();
                let batch_records = duckdb.batch_records;
                // `ponytail:` the store's writes block, so this holds one runtime worker
                // for the length of each flush. Fine on the multi-threaded runtime the
                // binary uses; a dedicated blocking thread is the upgrade if the store
                // gets slow enough to starve other tasks.
                let storage =
                    tokio::spawn(async move { receiver.drain(&mut store, batch_records).await });

                let ingest = ingest.run(DecodingSink::new(decoder, blocks), ledger).await;

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

/// How many accepted blocks a restart reads back: the undo window plus its floor.
#[cfg(any(feature = "duckdb", feature = "postgres"))]
const LEDGER_WINDOW: usize = MAX_UNFINALIZED_BLOCKS + 1;

/// The accepted blocks a previous run committed, as ingest takes them.
#[cfg(any(feature = "duckdb", feature = "postgres"))]
fn ledger(chain: &ChainId, stored: Vec<AcceptedBlock>) -> Vec<BlockMeta> {
    if let (Some(oldest), Some(tip)) = (stored.first(), stored.last()) {
        info!(%chain, from = oldest.height, tip = tip.height, hash = %tip.hash,
            "accepted blocks restored");
    } else {
        info!(%chain, "no accepted blocks stored; starting fresh");
    }
    stored.into_iter().map(BlockMeta::from).collect()
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

    use std::collections::HashMap;

    use crate::config::Settings;
    use crate::decode::{Catalog, Decoder, DecodingSink, StoredContract};
    use crate::ingest::pipeline::{Machine, PipelineError};
    use crate::ingest::source::{BlockMeta, BlockSource, FetchedBlock, HeadStream, SourceError};
    use crate::sink::duckdb::StoreError;
    use crate::sink::{self, DuckDbSink, EnvelopeSink as _, SinkError};
    use crate::wire::envelope::{ChainId, Envelope, Event, Log, Reorg};

    use super::{LEDGER_WINDOW, Pipeline, RuntimeError, finish, ledger};

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
    /// it; and once a reorg orphans the creating block, the pool is deleted with it and a
    /// restart no longer decodes it.
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
                block_hash: creating,
            }]
        );
        let mut restarted = decoder();
        assert_eq!(restarted.restore(stored), 1);
        let Event::Log(log) = swap(pool).event else {
            panic!("log");
        };
        assert!(restarted.decode(&log).expect("decode").is_some());

        // The creating block is orphaned: the stored row is deleted, so nothing restores.
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
        assert_eq!(count(&reader, "contract"), 0);
    }

    /// A fixed chain: the head and, per height, the block's identity and its events. Its
    /// head subscription ends at once, so [`Machine::run`] returns after startup has
    /// converged — a stand-in for a process that indexes to the head and then stops.
    struct FakeChain {
        chain: ChainId,
        head: u64,
        blocks: HashMap<u64, (BlockMeta, Vec<Event>)>,
    }

    impl FakeChain {
        /// Linear blocks `1..=head`, each hashed `tag:height` and empty: a logs-only run
        /// sees most blocks with no matching log.
        fn new(head: u64, tag: u8) -> Self {
            let mut chain = Self {
                chain: ChainId::new("base"),
                head,
                blocks: HashMap::new(),
            };
            chain.branch(1, head, tag);
            chain
        }

        /// Replaces `from..=through` with blocks hashed `tag:height`, linked to the block
        /// below `from`.
        fn branch(&mut self, from: u64, through: u64, tag: u8) {
            for height in from..=through {
                let parent_hash = self
                    .blocks
                    .get(&(height - 1))
                    .map_or(B256::ZERO, |(meta, _)| meta.hash);
                let meta = BlockMeta {
                    height,
                    hash: block_hash(tag, height),
                    parent_hash,
                    timestamp: height,
                };
                self.blocks.insert(height, (meta, Vec::new()));
            }
            self.head = self.head.max(through);
        }

        /// Adds a log to the block at `height`, stamped with that block's identity.
        fn log(&mut self, height: u64, envelope: Envelope) {
            let (meta, events) = self.blocks.get_mut(&height).expect("block");
            let Event::Log(mut log) = envelope.event else {
                panic!("log");
            };
            log.block_number = height;
            log.block_hash = meta.hash;
            events.push(Event::Log(log));
        }
    }

    fn block_hash(tag: u8, height: u64) -> B256 {
        let mut bytes = [0; 32];
        bytes[0] = tag;
        bytes[24..].copy_from_slice(&height.to_be_bytes());
        B256::from(bytes)
    }

    impl BlockSource for FakeChain {
        fn chain(&self) -> &ChainId {
            &self.chain
        }
        async fn subscribe_heads(&self) -> Result<HeadStream, SourceError> {
            Ok(Box::pin(futures_util::stream::empty()))
        }
        async fn fetch_header(&self, height: Option<u64>) -> Result<BlockMeta, SourceError> {
            Ok(self.blocks[&height.unwrap_or(self.head)].0)
        }
        async fn fetch_block(
            &self,
            height: u64,
            _head: Option<&BlockMeta>,
        ) -> Result<FetchedBlock, SourceError> {
            let (meta, events) = self.blocks[&height].clone();
            Ok(FetchedBlock { meta, events })
        }
    }

    /// One process lifetime against the store behind `connection`: restore what the store
    /// holds, index `chain` to its head through decode and the storage channel, then stop
    /// and let storage drain, as a crash after the last commit would leave it.
    async fn run_once(connection: &duckdb::Connection, chain: FakeChain, start: Option<u64>) {
        let mut store = DuckDbSink::new(connection.try_clone().expect("handle")).expect("open");
        let id = chain.chain.clone();
        let mut decoder = decoder();
        decoder.restore(store.contracts(&id).expect("contracts"));
        let restored = ledger(&id, store.ledger(&id, LEDGER_WINDOW).expect("ledger"));
        let (blocks, receiver) = sink::channel::ChannelSink::new();
        let storage = tokio::spawn(async move { receiver.drain(&mut store, 500).await });
        let machine = Machine::new(chain, DecodingSink::new(decoder, blocks), restored);
        let ingest = match start {
            Some(from) => machine.backfill(from).await.map(drop),
            None => machine.run().await,
        };
        assert!(
            matches!(ingest, Ok(()) | Err(PipelineError::SubscriptionClosed)),
            "{ingest:?}"
        );
        storage.await.expect("no panic").expect("storage drains");
    }

    fn heights(reader: &duckdb::Connection) -> Vec<(u64, String)> {
        reader
            .prepare("SELECT height, hash FROM accepted_block ORDER BY height, hash")
            .expect("prepare")
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .expect("query")
            .collect::<Result<_, _>>()
            .expect("rows")
    }

    /// Three process lifetimes against one store. The first indexes from a start height;
    /// the second resumes after it while the chain only grew, discovering a pool created
    /// while nothing was running; the third resumes after a reorg replaced the pool's
    /// block while nothing was running. The store ends with no gap in its ledger, one
    /// reorg naming the stored suffix, nothing of the orphaned blocks, and the pool
    /// retracted — its later swap does not decode.
    #[tokio::test]
    async fn a_restart_resumes_from_the_store_and_reconciles_a_fork_while_down() {
        let pool = Address::from([0xd0; 20]);
        let connection = duckdb::Connection::open_in_memory().expect("open in-memory DuckDB");

        run_once(&connection, FakeChain::new(4, 0xa0), Some(1)).await;
        assert_eq!(
            heights(&connection).len(),
            4,
            "every block, empty ones included"
        );

        let mut grown = FakeChain::new(6, 0xa0);
        grown.log(5, pool_created(pool, B256::ZERO));
        run_once(&connection, grown, None).await;
        let ledger: Vec<u64> = heights(&connection).iter().map(|(h, _)| *h).collect();
        assert_eq!(ledger, [1, 2, 3, 4, 5, 6], "resumed after 4 with no gap");
        assert_eq!(count(&connection, "reorg"), 0);
        assert_eq!(
            count(&connection, "contract"),
            1,
            "created while down, discovered"
        );

        let mut forked = FakeChain::new(6, 0xa0);
        forked.log(5, pool_created(pool, B256::ZERO));
        forked.branch(5, 7, 0xb0);
        forked.log(7, swap(pool));
        run_once(&connection, forked, None).await;

        let orphaned: String = connection
            .query_row("SELECT orphaned_hashes FROM reorg", [], |row| row.get(0))
            .expect("one reorg");
        let expected: Vec<String> = [6, 5]
            .map(|height| format!("{:#x}", block_hash(0xa0, height)))
            .to_vec();
        assert_eq!(
            serde_json::from_str::<Vec<String>>(&orphaned).expect("hash list"),
            expected
        );
        assert_eq!(count(&connection, "reorg"), 1);
        assert_eq!(
            [
                count(&connection, "log"),
                count(&connection, "decoded"),
                count(&connection, "contract")
            ],
            [1, 0, 0],
            "the creation went with its orphaned block; only the undecoded swap remains"
        );
        assert_eq!(
            heights(&connection).len(),
            7,
            "the orphaned blocks left the ledger"
        );
        let store = DuckDbSink::new(connection.try_clone().expect("handle")).expect("open");
        let chain = ChainId::new("base");
        assert!(store.contracts(&chain).expect("contracts").is_empty());
        let canonical = store.ledger(&chain, LEDGER_WINDOW).expect("ledger");
        assert_eq!(
            canonical
                .iter()
                .map(|block| block.height)
                .collect::<Vec<_>>(),
            [1, 2, 3, 4, 5, 6, 7]
        );
        assert!(
            canonical
                .windows(2)
                .all(|pair| pair[1].parent_hash == pair[0].hash),
            "the restored ledger is one linked chain"
        );
        assert_eq!(canonical[6].hash, block_hash(0xb0, 7));
    }

    /// A start height and a stored ledger contradict each other, so the run refuses.
    #[tokio::test]
    async fn a_start_height_with_a_stored_ledger_is_refused() {
        let connection = duckdb::Connection::open_in_memory().expect("open in-memory DuckDB");
        run_once(&connection, FakeChain::new(3, 0xa0), Some(1)).await;
        let store = DuckDbSink::new(connection.try_clone().expect("handle")).expect("open");
        let chain = ChainId::new("base");
        let restored = ledger(&chain, store.ledger(&chain, LEDGER_WINDOW).expect("ledger"));
        let machine = Machine::new(FakeChain::new(5, 0xa0), store, restored);
        assert!(matches!(
            machine.backfill(1).await.map(drop),
            Err(PipelineError::StartWithHistory { start: 1, tip: 3 })
        ));
    }
}
