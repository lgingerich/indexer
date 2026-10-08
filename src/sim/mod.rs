//! Deterministic simulation of the whole indexer.
//!
//! One seed decides everything: the chain and its reorgs, which faults are on and how
//! often they fire, the node's latency, the dataset selection and batch size, and when
//! the process dies. The real pipeline, source, retry layer, and store run unchanged
//! against a simulated node ([`node`]) and a store whose engine fails at its boundary
//! ([`store`]), on a single-threaded runtime with paused time, so a run is a pure
//! function of its seed.
//!
//! A run is a few process lifetimes. Each opens the store, resumes from it, and runs
//! until the pipeline stops, the store kills it, or a seeded crash drops the whole
//! runtime mid-flight. The chain keeps moving underneath, and between lifetimes too,
//! sometimes forking below what was stored. After every lifetime the store must be
//! consistent: a linked ledger, and every row belonging to a ledger block and equal to
//! what the chain holds. A final lifetime with the faults off must run without stopping
//! and catch up, and then the store must hold exactly the canonical chain.
//!
//! Every seed runs twice, the second time in a fresh process, and the two must agree on
//! everything that happened.
//!
//! `SIM_SEED=n` replays one seed; `SIM_SEEDS=n` runs seeds `0..n` (default 4; a long run
//! is faster with `--release`). `SIM_TRACE` prints each seed's trace, and a run with
//! `--no-capture` prints which scenarios the seeds reached.

mod chain;
mod node;
mod store;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy_primitives::B256;
use alloy_rpc_client::ClientBuilder;
use tokio::sync::Notify;

use self::chain::{Shared, SimBlock, World, lock};
use self::node::{SimHeads, SimNode};
use self::store::FaultyEngine;
use crate::decode::Catalog;
use crate::ingest::pipeline::PipelineError;
use crate::ingest::source::{EvmSource, RetryLayer};
use crate::runtime::{Pipeline, RuntimeError};
use crate::sink::duckdb::DuckDb;
use crate::sink::{Datasets, SinkError, SqlStore};

const CHAIN: &str = "sim";

/// `SplitMix64`: small, fast, and the same sequence for the same seed everywhere.
#[derive(Debug, Clone)]
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) const fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(crate) const fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A number in `0..n`.
    pub(crate) const fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// True `percent` times in a hundred.
    pub(crate) const fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    pub(crate) fn b256(&mut self) -> B256 {
        let mut bytes = [0; 32];
        for chunk in bytes.chunks_mut(8) {
            chunk.copy_from_slice(&self.next().to_le_bytes());
        }
        B256::from(bytes)
    }
}

/// Rows as text, one table each, in the order the oracle compares them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Stored {
    blocks: Vec<Vec<String>>,
    logs: Vec<Vec<String>>,
    ledger: Vec<Vec<String>>,
}

/// Everything one run produced.
#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    trace: Vec<String>,
    stored: Stored,
    /// Invariants that failed, and stops that should not happen.
    problems: Vec<String>,
    coverage: BTreeMap<&'static str, u64>,
}

/// The choices a seed makes once, up front.
#[derive(Debug, Clone, Copy)]
struct Plan {
    start: u64,
    datasets: Datasets,
    batch_records: usize,
    lifetimes: u64,
}

/// How a process lifetime ended.
#[derive(Debug)]
enum End {
    /// Still running when the lifetime's time ran out.
    Running,
    /// Killed: by the crash timer, or at a store statement.
    Killed,
    /// The pipeline stopped on its own, with this error.
    Stopped(RuntimeError),
}

fn run(seed: u64, dir: &Path) -> Outcome {
    let mut rng = Rng::new(seed);
    let length = 10 + rng.below(40);
    let datasets = if rng.chance(50) {
        r#"["logs"]"#
    } else {
        r#"["blocks", "logs"]"#
    };
    let plan = Plan {
        start: 1 + rng.below(length),
        datasets: serde_json::from_str(datasets).expect("datasets"),
        batch_records: usize::try_from(1 + rng.below(40)).unwrap_or(1),
        lifetimes: 1 + rng.below(5),
    };
    let world: Shared = Arc::new(Mutex::new(World::new(rng, length)));
    lock(&world).trace.push(format!("{plan:?}"));
    let path = dir.join("store.duckdb");
    let mut problems = Vec::new();

    for lifetime in 0..plan.lifetimes {
        if lifetime > 0 {
            lock(&world).downtime();
        }
        let crash = Duration::from_millis(500 + lock(&world).rng.below(60_000));
        let end = live(&world, &path, plan, lifetime, Some(crash));
        problems.extend(unexpected(&end, &lock(&world), true));
        problems.extend(consistent(&lock(&world), &read(&path), plan));
    }

    // Faults off: the chain holds still, and the indexer must run without stopping and
    // catch up within a few minutes.
    lock(&world).faults = false;
    lock(&world).downtime();
    let end = live(&world, &path, plan, plan.lifetimes, None);
    problems.extend(unexpected(&end, &lock(&world), false));
    let stored = read(&path);
    let world = lock(&world);
    problems.extend(consistent(&world, &stored, plan));
    let expected = expected(&world, plan);
    if stored != expected {
        problems.push(format!(
            "the store is not the canonical chain from {}:\nstored   {stored:?}\nexpected {expected:?}",
            plan.start
        ));
    }
    Outcome {
        trace: world.trace.clone(),
        stored,
        problems,
        coverage: world.coverage.clone(),
    }
}

/// One process lifetime. With `crash`, the chain grows and the process is killed at
/// that time unless something kills it first; without, the chain holds still for five
/// minutes.
fn live(world: &Shared, path: &Path, plan: Plan, lifetime: u64, crash: Option<Duration>) -> End {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("runtime");
    // Only a fresh store is told where to start; a resumed one starts after its ledger.
    let start = read(path).ledger.is_empty().then_some(plan.start);
    let (jitter, killed) = {
        let mut world = lock(world);
        world.crash = Arc::new(Notify::new());
        world.store_faulted = false;
        (world.rng.next(), Arc::clone(&world.crash))
    };
    let end = runtime.block_on(async {
        let catalog = Catalog::empty().expect("empty catalog");
        let config = duckdb::Config::default()
            .with("threads", "1")
            .expect("threads setting");
        let connection =
            duckdb::Connection::open_with_flags(path, config).expect("store file opens");
        let engine = FaultyEngine {
            inner: DuckDb { connection },
            world: Arc::clone(world),
        };
        let schema = Arc::new(catalog.schema().clone());
        let http = ClientBuilder::default()
            .layer(RetryLayer::new(jitter))
            .transport(
                SimNode {
                    world: Arc::clone(world),
                },
                true,
            );
        let heads = SimHeads {
            world: Arc::clone(world),
        };
        let source = EvmSource::new(CHAIN, http, heads, plan.datasets);
        let pipeline = Pipeline::new(source, start, catalog);

        // The chain grows while faults run, and holds still for the final catch-up.
        let chain = Arc::clone(world);
        let ticker = crash.is_some().then(|| {
            tokio::spawn(async move {
                loop {
                    let interval = 500 + lock(&chain).rng.below(4000);
                    tokio::time::sleep(Duration::from_millis(interval)).await;
                    lock(&chain).step();
                }
            })
        });
        let run = async {
            let store = SqlStore::new(engine, schema, CHAIN)
                .await
                .map_err(RuntimeError::OpenStore)?;
            pipeline.run_with_store(store, plan.batch_records).await
        };
        let end = tokio::select! {
            biased;
            () = killed.notified() => End::Killed,
            result = Box::pin(run) => match result {
                Ok(()) => End::Running,
                Err(error) => End::Stopped(error),
            },
            () = tokio::time::sleep(crash.unwrap_or(Duration::from_mins(5))) => {
                if crash.is_some() { End::Killed } else { End::Running }
            }
        };
        if let Some(ticker) = ticker {
            ticker.abort();
        }
        end
    });
    lock(world)
        .trace
        .push(format!("lifetime {lifetime}: {end:?}"));
    // Dropping the runtime drops every task mid-flight, store included.
    drop(runtime);
    end
}

/// Why `end` should not have happened, if it should not. With faults on, a stop is the
/// indexer refusing data the node got wrong, or the store failing as it was made to;
/// anything else is a bug. With faults off, any stop is.
fn unexpected(end: &End, world: &World, faults: bool) -> Option<String> {
    let End::Stopped(error) = end else {
        return None;
    };
    let expected = faults
        && match error {
            RuntimeError::Ingest(error) => matches!(
                error,
                PipelineError::Source(_)
                    | PipelineError::IdentityMismatch { .. }
                    | PipelineError::BrokenLink { .. }
                    | PipelineError::UnstableSource
                    | PipelineError::SubscriptionClosed
                    | PipelineError::StartAboveHead { .. }
                    | PipelineError::Sink(SinkError::StorageClosed)
            ),
            RuntimeError::Storage(_) | RuntimeError::OpenStore(_) => world.store_faulted,
            _ => false,
        };
    (!expected).then(|| format!("unexpected stop: {error:?}"))
}

/// What must hold whenever the process is down, whatever it was doing when it went: a
/// linked ledger of blocks the chain has seen, and every row belonging to a ledger block
/// and equal to that block's contents.
fn consistent(world: &World, stored: &Stored, plan: Plan) -> Vec<String> {
    let mut problems = Vec::new();
    let mut ledger = BTreeSet::new();
    let mut previous: Option<&Vec<String>> = None;
    let mut by_height: Vec<&Vec<String>> = stored.ledger.iter().collect();
    by_height.sort_by_key(|row| row[0].parse::<u64>().unwrap_or(u64::MAX));
    for row in by_height {
        let block = row[1]
            .parse()
            .ok()
            .and_then(|hash: B256| world.block(&hash));
        match block {
            Some(block) if ledger_row(block) == *row => {}
            _ => problems.push(format!("ledger row is not a block the chain made: {row:?}")),
        }
        if let Some(previous) = previous
            && (row[0].parse::<u64>().ok() != previous[0].parse::<u64>().ok().map(|h| h + 1)
                || row[2] != previous[1])
        {
            problems.push(format!("ledger does not link: {previous:?} then {row:?}"));
        }
        if row[0]
            .parse::<u64>()
            .is_ok_and(|height| height < plan.start)
        {
            problems.push(format!("ledger row below the start: {row:?}"));
        }
        ledger.insert(row[1].clone());
        previous = Some(row);
    }
    let rows = stored.blocks.iter().map(|row| (&row[1], row, "block"));
    let logs = stored.logs.iter().map(|row| (&row[0], row, "log"));
    for (hash, row, kind) in rows.chain(logs) {
        if !ledger.contains(hash) {
            problems.push(format!("{kind} row outside the ledger: {row:?}"));
        }
        let Some(block) = hash.parse().ok().and_then(|hash: B256| world.block(&hash)) else {
            problems.push(format!(
                "{kind} row for a block the chain never made: {row:?}"
            ));
            continue;
        };
        let matches = if kind == "block" {
            block_row(block) == *row
        } else {
            log_rows(block).contains(row)
        };
        if !matches {
            problems.push(format!("{kind} row differs from the chain: {row:?}"));
        }
    }
    problems
}

/// The canonical chain from the start height, as the store should hold it.
fn expected(world: &World, plan: Plan) -> Stored {
    let blocks: Vec<&SimBlock> = (plan.start..=world.tip())
        .filter_map(|height| world.at(height))
        .collect();
    let mut stored = Stored {
        blocks: if plan.datasets.blocks {
            blocks.iter().map(|block| block_row(block)).collect()
        } else {
            Vec::new()
        },
        logs: blocks.iter().flat_map(|block| log_rows(block)).collect(),
        ledger: blocks.iter().map(|block| ledger_row(block)).collect(),
    };
    stored.blocks.sort();
    stored.logs.sort();
    stored.ledger.sort();
    stored
}

fn ledger_row(block: &SimBlock) -> Vec<String> {
    vec![
        block.number().to_string(),
        format!("{:#x}", block.hash()),
        format!("{:#x}", block.header.inner.parent_hash),
    ]
}

fn block_row(block: &SimBlock) -> Vec<String> {
    let header = &block.header.inner;
    vec![
        header.number.to_string(),
        format!("{:#x}", block.hash()),
        format!("{:#x}", header.parent_hash),
        header.timestamp.to_string(),
        format!("{:#x}", header.logs_bloom),
        block.transactions().len().to_string(),
        header.extra_data.to_string(),
    ]
}

fn log_rows(block: &SimBlock) -> Vec<Vec<String>> {
    let transactions = block.transactions();
    block
        .logs
        .iter()
        .zip(0_u64..)
        .map(|(log, index)| {
            let topic = |i: usize| {
                log.topics
                    .get(i)
                    .map_or_else(|| "-".to_owned(), |t| format!("{t:#x}"))
            };
            let transaction_index = transactions
                .iter()
                .position(|hash| *hash == log.transaction_hash)
                .unwrap_or_default();
            vec![
                format!("{:#x}", block.hash()),
                index.to_string(),
                format!("{:#x}", log.transaction_hash),
                transaction_index.to_string(),
                format!("{:#x}", log.address),
                topic(0),
                topic(1),
                topic(2),
                topic(3),
                log.data.to_string(),
                block.number().to_string(),
                block.header.inner.timestamp.to_string(),
                "false".to_owned(),
            ]
        })
        .collect()
}

/// The rows the oracle compares, read from the database file without writing to it.
fn read(path: &Path) -> Stored {
    if !path.exists() {
        return Stored::default();
    }
    let config = duckdb::Config::default()
        .access_mode(duckdb::AccessMode::ReadOnly)
        .expect("read-only mode");
    let connection = duckdb::Connection::open_with_flags(path, config).expect("open store to read");
    let tables: u64 = connection
        .query_row(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = ?",
            [CHAIN],
            |row| row.get(0),
        )
        .expect("list tables");
    if tables == 0 {
        return Stored::default();
    }
    let rows = |sql: &str, width: usize| -> Vec<Vec<String>> {
        let mut statement = connection.prepare(sql).expect("prepare");
        statement
            .query_map([], |row| {
                (0..width).map(|i| row.get::<_, String>(i)).collect()
            })
            .expect("query")
            .collect::<Result<_, _>>()
            .expect("rows")
    };
    let mut stored = Stored {
        blocks: rows(
            "SELECT number::VARCHAR, hash, parent_hash, epoch(timestamp)::BIGINT::VARCHAR, \
             logs_bloom, transaction_count::VARCHAR, extra_data FROM sim.blocks",
            7,
        ),
        logs: rows(
            "SELECT block_hash, log_index::VARCHAR, transaction_hash, \
             transaction_index::VARCHAR, address, coalesce(topic0, '-'), \
             coalesce(topic1, '-'), coalesce(topic2, '-'), coalesce(topic3, '-'), data, \
             block_number::VARCHAR, epoch(block_timestamp)::BIGINT::VARCHAR, \
             removed::VARCHAR FROM sim.logs",
            13,
        ),
        ledger: rows(
            "SELECT height::VARCHAR, hash, parent_hash FROM sim.accepted_blocks",
            3,
        ),
    };
    stored.blocks.sort();
    stored.logs.sort();
    stored.ledger.sort();
    stored
}

/// The seeds to run: `SIM_SEED` alone, or `0..SIM_SEEDS`.
fn seeds() -> Vec<u64> {
    let var = |name| {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
    };
    var("SIM_SEED").map_or_else(
        || (0..var("SIM_SEEDS").unwrap_or(4)).collect(),
        |seed| vec![seed],
    )
}

/// Runs `seed` in a scratch directory of its own.
fn run_fresh(seed: u64) -> Outcome {
    let dir = std::env::temp_dir().join(format!("indexer-sim-{}-{seed}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("sim directory");
    let outcome = run(seed, &dir);
    let _ = std::fs::remove_dir_all(&dir);
    outcome
}

/// `seed`'s outcome from a fresh process: this test binary, rerun on that seed alone.
fn rerun(seed: u64) -> String {
    let out = std::env::temp_dir().join(format!("indexer-sim-{}-{seed}.out", std::process::id()));
    let status = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args(["sim::simulate", "--exact", "--test-threads=1"])
        .env("SIM_SEED", seed.to_string())
        .env("SIM_CHILD_OUT", &out)
        .env_remove("SIM_TRACE")
        .stdout(std::process::Stdio::null())
        .status()
        .expect("rerun the seed");
    assert!(status.success(), "seed {seed}: the rerun failed");
    let outcome = std::fs::read_to_string(&out).expect("rerun outcome");
    let _ = std::fs::remove_file(&out);
    outcome
}

#[expect(
    clippy::print_stderr,
    reason = "printing is what SIM_TRACE and --no-capture ask for"
)]
fn print(text: &str) {
    eprintln!("{text}");
}

#[test]
fn simulate() {
    if let Some(out) = std::env::var_os("SIM_CHILD_OUT") {
        let seed = seeds()[0];
        std::fs::write(out, format!("{:#?}", run_fresh(seed))).expect("write outcome");
        return;
    }
    let mut coverage: BTreeMap<&'static str, u64> = BTreeMap::new();
    // Every seed runs, so one batch reports every failing seed rather than the first.
    let mut failures: Vec<String> = Vec::new();
    for seed in seeds() {
        let outcome = run_fresh(seed);
        if std::env::var_os("SIM_TRACE").is_some() {
            print(&format!("seed {seed}:\n{}", outcome.trace.join("\n")));
        }
        for (scenario, count) in &outcome.coverage {
            *coverage.entry(scenario).or_default() += count;
        }
        let replay = format!("replay with SIM_SEED={seed} SIM_TRACE=1");
        if !outcome.problems.is_empty() {
            failures.push(format!(
                "seed {seed}: {}\n({replay})",
                outcome.problems.join("\n")
            ));
            continue;
        }
        let here = format!("{outcome:#?}");
        let there = rerun(seed);
        if here != there {
            let diverged = here
                .lines()
                .zip(there.lines())
                .find(|(here, there)| here != there);
            failures.push(format!(
                "seed {seed}: a fresh process diverged at {diverged:?} ({replay})"
            ));
        }
    }
    print(&format!("scenarios reached: {coverage:#?}"));
    assert!(
        failures.is_empty(),
        "{} seeds failed:\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
