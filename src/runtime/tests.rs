//! Assembling a pipeline and running it against real stores: how a stop, a failed store,
//! and a restart end up in what the stores hold.

// The crate denies `expect`/`unwrap` to keep production paths honest; tests are allowed
// them per the repository test style, since a failed expectation there means the fixture
// or setup is wrong.
#![expect(clippy::expect_used)]

use alloy_dyn_abi::DynSolValue;
use alloy_primitives::{Address, B256, keccak256};

use std::collections::HashMap;
use std::sync::Arc;

use crate::config::Settings;
use crate::decode::{Catalog, Decoder, DecodingSink};
use crate::ingest::pipeline::{Machine, PipelineError};
use crate::ingest::source::{BlockSource, FetchedBlock, HeadStream, SourceError};
use crate::sink::channel::Batching;
use crate::sink::duckdb::DuckDb;
use crate::sink::resume::Opened;
use crate::sink::route::Backend;
use crate::sink::stop::Stop;
use crate::sink::store_runtime::StoreRuntime;
use crate::sink::table::Schema;
use crate::sink::{self, DuckDbSink, EnvelopeSink as _, SinkError, Store as _};
use crate::sink::{Operation, StoreError, StoredContract};
use crate::wire::datasets::evm::Log;
use crate::wire::envelope::{BlockMeta, ChainId, Envelope, Event, Reorg};

use super::stores::{finish, ledger};
use super::{Pipeline, RuntimeError};

/// The shipped protocols, as an absolute path so [`Settings::parse`] carries no
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
tables = ["blocks", "transactions", "logs", "decoded_logs", "contract_events"]
"#;

/// The store's write error is the one returned when ingest then fails because the
/// receiver is gone. That second failure is the failed send, and reporting it would
/// hide the write that caused it.
#[test]
fn a_failed_store_is_reported_ahead_of_the_closed_send() {
    let connection = duckdb::Connection::open_in_memory().expect("open in-memory DuckDB");
    connection
        .execute_batch("CREATE TABLE blocks (not_a_column INTEGER)")
        .expect("schema");
    let source = connection
        .execute("INSERT INTO blocks VALUES (1, 2)", [])
        .expect_err("two values cannot fit one column");
    let storage = Ok(Err(SinkError::Store(StoreError::Engine {
        operation: Operation::Stage,
        target: Some("blocks".to_owned()),
        source: source.into(),
    })));
    let ingest = Err(PipelineError::Sink(SinkError::StorageClosed));

    let error =
        finish(vec![(Backend::DuckDb, storage)], ingest).expect_err("the store rejected the batch");
    assert!(
        matches!(
            &error,
            RuntimeError::Storage {
                store: Backend::DuckDb,
                source: SinkError::Store(StoreError::Engine {
                    target: Some(table),
                    ..
                }),
            } if table == "blocks"
        ),
        "the append error must be the one reported, not the closed send: {error:?}"
    );
}

/// A store that cannot open or resume as it stands is the settings' fault, so the
/// process exits as a config error and the supervisor waits for a fix rather than
/// restarting into the same failure; a store that may come back is not.
#[test]
fn a_store_that_cannot_start_as_it_stands_is_a_config_error() {
    use crate::sink::resume::ResumeError;

    let open = |source| RuntimeError::OpenStore {
        store: Backend::Postgres,
        source,
    };
    for (case, error, config) in [
        (
            "a drifted table",
            open(StoreError::Drift {
                table: "logs".to_owned(),
                difference: "it has no data column".to_owned(),
            }),
            true,
        ),
        (
            "a locked schema",
            open(StoreError::Locked {
                schema: "base".to_owned(),
            }),
            false,
        ),
        (
            "an unreachable store",
            open(StoreError::Engine {
                operation: Operation::Open,
                target: None,
                source: "connection refused".into(),
            }),
            false,
        ),
        (
            "stores with no common block",
            RuntimeError::Resume(ResumeError::NoCommonBlock {
                tips: vec![(Backend::Postgres, 9), (Backend::Delta, 5)],
            }),
            true,
        ),
        (
            "a ledger that timed out",
            RuntimeError::Resume(ResumeError::Restore {
                store: Backend::Delta,
                source: StoreError::Engine {
                    operation: Operation::Read,
                    target: Some("accepted_blocks".to_owned()),
                    source: "timed out".into(),
                },
            }),
            false,
        ),
    ] {
        assert_eq!(error.is_config(), config, "{case}: {error}");
    }
}

/// The shipped protocols load, so the example settings a reader copies stay runnable.
#[test]
fn the_shipped_protocols_assemble() {
    let decode = format!("\n[decode]\nprotocols = {:?}\n", protocols());
    let settings = Settings::parse(&format!("{SETTINGS}{decode}"), crate::secret::process_env)
        .expect("settings parse");
    Pipeline::from_settings(&settings).expect("the pipeline builds");
}

/// The shipped protocols' schema, which a store opens with to hold what [`decoder`]
/// decodes.
fn schema() -> Arc<Schema> {
    sink::fixtures::schema()
}

/// A `base` store of [`schema`]'s tables over `connection`.
async fn duckdb_store(connection: duckdb::Connection) -> DuckDbSink {
    DuckDbSink::new(DuckDb { connection }, schema(), "base")
        .await
        .expect("open the store")
}

/// `store`, writing every table of [`schema`].
fn every_table<S>(backend: Backend, store: S, batching: Batching) -> Opened<S> {
    Opened {
        backend,
        store,
        batching,
        tables: schema().tables().iter().map(|def| def.id).collect(),
        runtime: StoreRuntime::current(),
    }
}

fn decoder() -> Decoder {
    Decoder::new(Catalog::load(protocols(), &ChainId::new("base")).expect("shipped protocols load"))
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
        include_str!("../../examples/fixtures/uniswap_v3_swaps.ndjson")
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
        .query_row(&format!("SELECT count(*) FROM base.{table}"), [], |row| {
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
    let mut store = duckdb_store(connection).await;
    let (blocks, receiver) = sink::channel::ChannelSink::new(Backend::DuckDb);
    let storage = tokio::spawn(async move {
        receiver
            .drain(&mut store, Batching::eager(500), Stop::never())
            .await
    });

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
            count(&reader, "logs"),
            count(&reader, "decoded_logs"),
            count(&reader, "contracts")
        ],
        [2, 2, 1]
    );
    // And each decode in its event's own typed table.
    assert_eq!(
        [
            count(&reader, "uniswap_v3_uniswap_v3_factory_pool_created"),
            count(&reader, "uniswap_v3_pool_swap")
        ],
        [1, 1]
    );
    // The decoded rows key back to the raw logs they came from. Joined on the
    // columns: a key prefix would also match log 3 to the decodes of log 30.
    let orphans: i64 = reader
        .query_row(
            "SELECT count(*) FROM base.decoded_logs d WHERE NOT EXISTS \
             (SELECT 1 FROM base.logs l WHERE l.block_hash = d.block_hash \
             AND l.transaction_hash = d.transaction_hash AND l.log_index = d.log_index)",
            [],
            |row| row.get(0),
        )
        .expect("join");
    assert_eq!(orphans, 0);

    // A restart: a fresh decoder knows the pool once the store's rows are restored.
    let mut store = duckdb_store(reader.try_clone().expect("handle")).await;
    let stored = store.contracts().await.expect("read back");
    assert_eq!(stored, [stored_pool(pool, creating, 10)]);
    let mut restarted = decoder();
    assert_eq!(restarted.restore(stored), 1);
    let Event::Log(log) = swap(pool).event else {
        panic!("log");
    };
    assert!(restarted.decode(&log).is_some());

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
    assert!(store.contracts().await.expect("read back").is_empty());
    assert_eq!(count(&reader, "contracts"), 0);
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
    async fn fetch_blocks(&self, from: u64, to: u64) -> Result<Vec<FetchedBlock>, SourceError> {
        let mut blocks = Vec::new();
        for height in from..=to {
            blocks.push(self.fetch_block(height, None).await?);
        }
        Ok(blocks)
    }
    fn follows_heads(&self) -> bool {
        true
    }
    fn backfill_span(&self) -> u64 {
        1
    }
    fn backfill_concurrency(&self) -> usize {
        1
    }
    fn finality_depth(&self) -> u64 {
        crate::window::MAX_UNFINALIZED_BLOCKS as u64
    }
}

/// One process lifetime against the store behind `connection`: restore what the store
/// holds, index `chain` to its head through decode and the storage channel, then stop
/// when the head subscription ends and let storage drain, as a crash after the last
/// commit would leave it.
async fn run_once(connection: &duckdb::Connection, chain: FakeChain, start: Option<u64>) {
    let store = duckdb_store(connection.try_clone().expect("handle")).await;
    let catalog =
        Catalog::load(protocols(), &ChainId::new("base")).expect("shipped protocols load");
    let result = Pipeline::new(chain, start, None, catalog)
        .run_with_stores(
            vec![every_table(Backend::DuckDb, store, Batching::eager(500))],
            Stop::never(),
        )
        .await;
    ran_to_head(&result);
}

/// Asserts that a run indexed to the head and stopped only because the head
/// subscription ended, as every [`FakeChain`] run does.
fn ran_to_head(result: &Result<(), RuntimeError>) {
    assert!(
        matches!(
            result,
            Err(RuntimeError::Ingest(PipelineError::SubscriptionClosed))
        ),
        "{result:?}"
    );
}

/// The pool [`pool_created`] names, stored with the block that created it.
fn stored_pool(pool: Address, block_hash: B256, block_number: u64) -> StoredContract {
    StoredContract {
        protocol: "uniswap_v3".to_owned(),
        name: "UniswapV3Pool".to_owned(),
        address: pool,
        block_hash,
        block_number,
        log_index: 1,
    }
}

/// The ledger of blocks `1..=through`, hashed as [`FakeChain`] hashes them: tagged
/// `0xa0` below `fork` and `0xb0` from it.
fn chain_through(through: u64, fork: u64) -> Vec<(u64, String)> {
    (1..=through)
        .map(|height| {
            let tag = if height < fork { 0xa0 } else { 0xb0 };
            (height, format!("{:#x}", block_hash(tag, height)))
        })
        .collect()
}

/// A restored ledger as `(height, hash)` pairs, to compare with [`chain_through`].
#[cfg(feature = "delta")]
fn ledger_of(restored: &sink::Restored) -> Vec<(u64, String)> {
    restored
        .ledger
        .iter()
        .map(|block| (block.height, format!("{:#x}", block.hash)))
        .collect()
}

fn heights(reader: &duckdb::Connection) -> Vec<(u64, String)> {
    reader
        .prepare("SELECT height, hash FROM base.accepted_blocks ORDER BY height, hash")
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
        heights(&connection),
        chain_through(4, u64::MAX),
        "every block, empty ones included"
    );

    let mut grown = FakeChain::new(6, 0xa0);
    grown.log(5, pool_created(pool, B256::ZERO));
    run_once(&connection, grown, None).await;
    assert_eq!(
        heights(&connection),
        chain_through(6, u64::MAX),
        "resumed after 4 with no gap"
    );
    assert_eq!(count(&connection, "reorgs"), 0);
    assert_eq!(
        count(&connection, "contracts"),
        1,
        "created while down, discovered"
    );

    let mut forked = FakeChain::new(6, 0xa0);
    forked.log(5, pool_created(pool, B256::ZERO));
    forked.branch(5, 7, 0xb0);
    forked.log(7, swap(pool));
    run_once(&connection, forked, None).await;

    let orphaned: String = connection
        .query_row("SELECT orphaned_hashes FROM base.reorgs", [], |row| {
            row.get(0)
        })
        .expect("one reorg");
    let expected: Vec<String> = [6, 5]
        .map(|height| format!("{:#x}", block_hash(0xa0, height)))
        .to_vec();
    assert_eq!(
        serde_json::from_str::<Vec<String>>(&orphaned).expect("hash list"),
        expected
    );
    assert_eq!(count(&connection, "reorgs"), 1);
    assert_eq!(
        [
            count(&connection, "logs"),
            count(&connection, "decoded_logs"),
            count(&connection, "contracts")
        ],
        [1, 0, 0],
        "the creation went with its orphaned block; only the undecoded swap remains"
    );
    assert_eq!(
        heights(&connection),
        chain_through(7, 5),
        "the orphaned blocks left the ledger"
    );
    let mut store = duckdb_store(connection.try_clone().expect("handle")).await;
    let restored = store.restore().await.expect("restore");
    assert!(restored.contracts.is_empty());
    assert_eq!(
        restored
            .ledger
            .iter()
            .map(|block| (block.height, format!("{:#x}", block.hash)))
            .collect::<Vec<_>>(),
        chain_through(7, 5)
    );
    assert!(
        restored
            .ledger
            .windows(2)
            .all(|pair| pair[1].parent_hash == pair[0].hash),
        "the restored ledger is one linked chain"
    );
}

/// A start height above a stored ledger's tip would leave a gap, so the run refuses.
#[tokio::test]
async fn a_start_height_above_a_stored_ledger_is_refused() {
    let connection = duckdb::Connection::open_in_memory().expect("open in-memory DuckDB");
    run_once(&connection, FakeChain::new(3, 0xa0), Some(1)).await;
    let mut store = duckdb_store(connection.try_clone().expect("handle")).await;
    let chain = ChainId::new("base");
    let restored = ledger(
        &chain,
        store.restore().await.expect("restore").ledger.into(),
    );
    let machine = Machine::new(FakeChain::new(5, 0xa0), store, restored);
    let result = machine.run(Some(5), None).await;
    assert!(
        matches!(
            result,
            Err(PipelineError::StartAboveHistory { start: 5, tip: 3 })
        ),
        "{result:?}"
    );
}

/// A lake directory for one test, removed when the guard drops.
#[cfg(feature = "delta")]
struct LakeDir(std::path::PathBuf);

#[cfg(feature = "delta")]
impl Drop for LakeDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A lake in a directory of its own named for `test`, writing `tables`, and the guard
/// that removes it. The interval is far longer than any run, so rows reach the lake
/// only through the drain's flush when the channel closes.
#[cfg(feature = "delta")]
fn lake(test: &str, tables: &str) -> (LakeDir, sink::DeltaSettings) {
    let dir = LakeDir(
        std::env::temp_dir().join(format!("indexer-runtime-{test}-{}", std::process::id())),
    );
    let settings = sink::DeltaSettings {
        tables: serde_json::from_str(tables).expect("tables"),
        uri: dir.0.display().to_string(),
        commit_interval_secs: 3600,
        max_buffer_bytes: usize::MAX,
        maintenance: false,
        vacuum_retention_hours: 168,
        partition_blocks: 100_000,
        storage: std::collections::BTreeMap::new(),
    };
    (dir, settings)
}

/// One process lifetime against the lake `settings` names, as [`run_once`] is against
/// a database: restore, index to the head, stop. The interval is far longer than the
/// run, so every row reaches the lake through the drain's flush when the channel
/// closes.
#[cfg(feature = "delta")]
async fn run_lake(settings: &sink::DeltaSettings, chain: FakeChain, start: Option<u64>) {
    let store = sink::DeltaSink::open(settings, schema(), "base")
        .await
        .expect("the lake opens");
    let catalog =
        Catalog::load(protocols(), &ChainId::new("base")).expect("shipped protocols load");
    let result = Box::pin(Pipeline::new(chain, start, None, catalog).run_with_stores(
        vec![every_table(Backend::Delta, store, settings.batching())],
        Stop::never(),
    ))
    .await;
    ran_to_head(&result);
}

/// What a fresh open of the lake restores.
#[cfg(feature = "delta")]
async fn restored(settings: &sink::DeltaSettings) -> sink::Restored {
    sink::DeltaSink::open(settings, schema(), "base")
        .await
        .expect("the lake opens")
        .restore()
        .await
        .expect("the lake restores")
}

/// The restart test of [`a_restart_resumes_from_the_store_and_reconciles_a_fork_while_down`]
/// against the lake: three process lifetimes, the second discovering a pool created
/// while down and the third reconciling a fork that replaced the pool's block. Each
/// commits only when storage drains at the end, so a lost final flush would show as
/// an empty restore.
#[cfg(feature = "delta")]
#[tokio::test]
async fn a_lake_restart_resumes_from_its_ledger_and_reconciles_a_fork_while_down() {
    let (_dir, settings) = lake("lake", r#"["contract_events"]"#);
    let pool = Address::from([0xd0; 20]);

    run_lake(&settings, FakeChain::new(4, 0xa0), Some(1)).await;
    assert_eq!(
        ledger_of(&restored(&settings).await),
        chain_through(4, u64::MAX)
    );

    let mut grown = FakeChain::new(6, 0xa0);
    grown.log(5, pool_created(pool, B256::ZERO));
    run_lake(&settings, grown, None).await;
    let after = restored(&settings).await;
    assert_eq!(
        ledger_of(&after),
        chain_through(6, u64::MAX),
        "resumed after 4 with no gap"
    );
    assert_eq!(
        after.contracts,
        [stored_pool(pool, block_hash(0xa0, 5), 5)],
        "created while down, discovered"
    );

    let mut forked = FakeChain::new(6, 0xa0);
    forked.log(5, pool_created(pool, B256::ZERO));
    forked.branch(5, 7, 0xb0);
    forked.log(7, swap(pool));
    run_lake(&settings, forked, None).await;

    let after = restored(&settings).await;
    assert!(
        after.contracts.is_empty(),
        "the creation went with its block"
    );
    assert_eq!(ledger_of(&after), chain_through(7, 5));
    assert!(
        after
            .ledger
            .windows(2)
            .all(|pair| pair[1].parent_hash == pair[0].hash),
        "the restored ledger is one linked chain"
    );
}

/// Two stores, the database ahead of the lake when the process stops, and the chain
/// forked below the database's tip while nothing ran. The restart resumes from the
/// block both hold; the database retracts each of its orphaned blocks as ingest hands
/// that height again, so neither keeps the orphaned branch and the lake is never
/// handed a block it has; both end on the same canonical chain.
#[cfg(feature = "delta")]
#[tokio::test]
async fn stores_at_different_heights_resume_from_the_block_they_share() {
    let (_dir, settings) = lake("two", r#"["blocks"]"#);
    let connection = duckdb::Connection::open_in_memory().expect("open in-memory DuckDB");
    let pool = Address::from([0xd0; 20]);
    let both = async |chain: FakeChain, start: Option<u64>| {
        run_both(&connection, &settings, chain, start).await;
    };

    both(FakeChain::new(4, 0xa0), Some(1)).await;

    // The database alone runs on: it holds 5 and 6, and a pool created at 6.
    let mut grown = FakeChain::new(6, 0xa0);
    grown.log(6, pool_created(pool, B256::ZERO));
    run_once(&connection, grown, None).await;
    assert_eq!(heights(&connection), chain_through(6, u64::MAX));
    assert_eq!(count(&connection, "contracts"), 1);

    // While down, 5 and 6 were replaced.
    let mut forked = FakeChain::new(6, 0xa0);
    forked.branch(5, 7, 0xb0);
    both(forked, None).await;

    let expected = chain_through(7, 5);
    assert_eq!(
        heights(&connection),
        expected,
        "the database holds one chain"
    );
    assert_eq!(
        count(&connection, "contracts"),
        0,
        "the pool went with its orphaned block"
    );
    assert_eq!(
        count(&connection, "reorgs"),
        2,
        "each orphaned block the database held is retracted as ingest reaches it"
    );
    assert_eq!(
        ledger_of(&restored(&settings).await),
        expected,
        "the lake holds the same chain"
    );
}

/// One process lifetime of a database and a lake together, the database first, as
/// [`run_once`] is for the database alone.
#[cfg(feature = "delta")]
async fn run_both(
    connection: &duckdb::Connection,
    settings: &sink::DeltaSettings,
    chain: FakeChain,
    start: Option<u64>,
) {
    use crate::sink::any_store::AnyStore;

    let database = duckdb_store(connection.try_clone().expect("handle")).await;
    let lake = sink::DeltaSink::open(settings, schema(), "base")
        .await
        .expect("the lake opens");
    let catalog =
        Catalog::load(protocols(), &ChainId::new("base")).expect("shipped protocols load");
    // Each store drains on a runtime of its own, as in a deployment; unlike one, they
    // were opened on the test's runtime.
    let own = |opened: Opened<AnyStore>| Opened {
        runtime: StoreRuntime::dedicated(opened.backend).expect("a runtime"),
        ..opened
    };
    let stores = vec![
        own(every_table(
            Backend::DuckDb,
            AnyStore::DuckDb(Box::new(database)),
            Batching::eager(500),
        )),
        own(every_table(
            Backend::Delta,
            AnyStore::Delta(Box::new(lake)),
            settings.batching(),
        )),
    ];
    let result =
        Box::pin(Pipeline::new(chain, start, None, catalog).run_with_stores(stores, Stop::never()))
            .await;
    ran_to_head(&result);
}

/// The database ahead of the lake on the chain that stayed canonical, as every
/// deploy leaves them: the restart deletes nothing from the database and writes
/// nothing again into it. A value changed by hand in a held block's row survives,
/// which neither a delete nor an upsert of the block would let it.
#[cfg(feature = "delta")]
#[tokio::test]
async fn a_store_ahead_on_the_canonical_chain_keeps_its_rows() {
    let (_dir, settings) = lake("ahead", r#"["blocks"]"#);
    let connection = duckdb::Connection::open_in_memory().expect("open in-memory DuckDB");
    let pool = Address::from([0xd0; 20]);
    let chain = || {
        let mut chain = FakeChain::new(6, 0xa0);
        chain.log(6, pool_created(pool, B256::ZERO));
        chain
    };

    run_both(&connection, &settings, FakeChain::new(4, 0xa0), Some(1)).await;
    // The database alone runs on to 6, discovering a pool there.
    run_once(&connection, chain(), None).await;
    connection
        .execute_batch("UPDATE base.contracts SET name = 'kept'")
        .expect("mark the held row");

    let mut grown = chain();
    grown.branch(7, 7, 0xa0);
    run_both(&connection, &settings, grown, None).await;

    let name: String = connection
        .query_row("SELECT name FROM base.contracts", [], |row| row.get(0))
        .expect("the pool is still stored");
    assert_eq!(
        name, "kept",
        "the held block was neither deleted nor rewritten"
    );
    assert_eq!(count(&connection, "reorgs"), 0);
    let expected = chain_through(7, u64::MAX);
    assert_eq!(
        heights(&connection),
        expected,
        "the database holds one chain"
    );
    let lake = restored(&settings).await;
    assert_eq!(
        ledger_of(&lake),
        expected,
        "the lake caught up to the same chain"
    );
    assert_eq!(
        lake.contracts,
        [stored_pool(pool, block_hash(0xa0, 6), 6)],
        "the lake discovered the pool once"
    );
}
