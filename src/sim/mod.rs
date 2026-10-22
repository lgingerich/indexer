//! Deterministic simulation of the whole indexer.
//!
//! One seed decides everything: the chain and its reorgs, which faults are on and how
//! often they fire, the node's latency, the dataset selection, the stores and when each
//! commits, the finality depth, and when the process dies or is asked to stop. The real
//! pipeline, source, retry layer, router, resume, and stores run unchanged against a
//! simulated node ([`node`]) and stores whose engine fails at its boundary ([`store`]),
//! on a single-threaded runtime with paused time, so a run is a pure function of its
//! seed.
//!
//! A run feeds one or two stores from one ingest, as a deployment feeds its database
//! and its lake: the database commits as soon as a batch is gathered or within a second,
//! the lake by time and bytes, and each writes the tables routed to it and fails at its
//! own rates. A run is a few process lifetimes. Each opens the stores, brings them to
//! one block, and runs until the pipeline stops, a store kills it, a seeded crash drops
//! the whole runtime mid-flight, or a seeded stop asks it to end as `SIGTERM` does. A
//! lifetime against stores that hold blocks is sometimes given a start height too,
//! which ingest must ignore or refuse. The chain keeps moving underneath, and between
//! lifetimes too, sometimes forking below what was stored. After every lifetime each
//! store must hold exactly what its ledger says ([`oracle`]): a linked branch of the
//! chain, and every row of the tables it writes for each block on it, as the chain made
//! the block. A final lifetime with the faults off must run without stopping and catch
//! up, and then every store must hold the canonical chain.
//!
//! Every seed runs twice, the second time in a fresh process, and the two must agree on
//! everything that happened.
//!
//! `SIM_SEED=n` replays one seed; `SIM_SEEDS=n` runs seeds `0..n` (default 4; a long run
//! is faster with `--release`). `SIM_TRACE` prints each seed's trace, and a run with
//! `--no-capture` prints which scenarios the seeds reached. The default
//! nextest profile skips it, so a run needs `--ignore-default-filter`.

mod chain;
mod node;
mod oracle;
mod protocol;
mod store;

use std::collections::{BTreeMap, HashSet};
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy_primitives::B256;
use alloy_rpc_client::ClientBuilder;
use tokio::sync::Notify;

use self::chain::{Shared, World, lock};
use self::node::{SimHeads, SimNode};
use self::oracle::Stored;
use self::store::{FaultyEngine, SimStore};
use crate::decode::Catalog;
use crate::ingest::pipeline::PipelineError;
use crate::ingest::source::{EvmSource, RetryLayer, SourceError};
use crate::runtime::{Pipeline, RuntimeError};
use crate::sink::channel::Batching;
use crate::sink::duckdb::DuckDb;
use crate::sink::resume::{Opened, ResumeError};
use crate::sink::route::StoreBackend;
use crate::sink::stop::Stop;
use crate::sink::store_runtime::StoreRuntime;
use crate::sink::table::{Schema, Table, TableId};
use crate::sink::{SinkError, SqlStore};
use crate::wire::datasets::Datasets;
use crate::wire::envelope::ChainId;

/// The chain the simulation indexes, named as a real one because the catalog accepts
/// only the chains the indexer knows. Nothing here talks to that chain.
const CHAIN: &str = "ethereum";

/// Backfill fetches in flight, as a deployment's default keeps them, so faults land
/// while several ranges are outstanding.
const CONCURRENCY: std::num::NonZeroUsize = std::num::NonZeroUsize::MIN.saturating_add(3);

/// The shallowest finality depth a plan draws. A fork the chain makes while the indexer
/// runs is at most three blocks deep, and a lagging backend serves a view at most four
/// steps old, which after a reorg's truncation is at most three blocks short of the
/// newest; four keeps every height backfill counts as buried the same on every view a
/// backend may serve, as a real chain's finality depth keeps it under a provider's lag.
const MIN_FINALITY: u64 = 4;

/// How long the host waits for a stopped process before killing it: `docker stop -t 30`.
const STOP_DEADLINE: Duration = Duration::from_secs(30);

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

/// Everything one run produced.
#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    trace: Vec<String>,
    /// Each store's rows at the end, in the plan's order.
    stored: Vec<Stored>,
    /// Invariants that failed, and stops that should not happen.
    problems: Vec<String>,
    coverage: BTreeMap<&'static str, u64>,
}

/// One store a plan runs, in one of the two roles a deployment's stores play. Both are
/// `DuckDB` files under the faulty engine; what differs is when they commit, what they
/// write, and how they fail.
#[derive(Debug, Clone)]
pub(super) struct StorePlan {
    /// The role, as errors and logs name the store.
    pub(super) backend: StoreBackend,
    file: &'static str,
    batching: Batching,
    /// Whether it reports the bytes it buffers, as the lake does.
    counts_bytes: bool,
    /// Whether it writes the raw datasets.
    pub(super) raw: bool,
    /// Whether it writes what decode makes of them.
    pub(super) decoded: bool,
}

impl StorePlan {
    /// The database: everything, committed as soon as a batch is gathered, or once the
    /// oldest block has waited up to a second, as `commit_wait_ms` lets tip blocks share
    /// a commit.
    fn database(rng: &mut Rng) -> Self {
        let wait = if rng.chance(50) {
            Duration::ZERO
        } else {
            Duration::from_millis(1 + rng.below(1000))
        };
        Self {
            backend: StoreBackend::Postgres,
            file: "database.duckdb",
            batching: Batching {
                wait,
                ..Batching::eager(usize::try_from(1 + rng.below(40)).unwrap_or(1))
            },
            counts_bytes: false,
            raw: true,
            decoded: true,
        }
    }

    /// The lake: the raw datasets, what decode makes, or both, committed once the
    /// oldest block has waited up to the lake's 150 s or the buffer passes a byte limit
    /// that, at the simulated chain's size, a few blocks fill.
    fn lake(rng: &mut Rng) -> Self {
        let (raw, decoded) = match rng.below(3) {
            0 => (true, true),
            1 => (true, false),
            _ => (false, true),
        };
        Self {
            backend: StoreBackend::Delta,
            file: "lake.duckdb",
            batching: Batching {
                records: usize::MAX,
                bytes: usize::try_from(512 + rng.below(16 * 1024)).unwrap_or(usize::MAX),
                wait: Duration::from_secs(1 + rng.below(150)),
            },
            counts_bytes: true,
            raw,
            decoded,
        }
    }

    /// The tables of `schema` routed to this store.
    fn tables(&self, schema: &Schema) -> HashSet<TableId> {
        schema
            .tables()
            .iter()
            .map(|def| def.id)
            .filter(|id| match id {
                TableId::Dataset(Table::Block | Table::Transaction | Table::Log) => self.raw,
                TableId::Dataset(Table::Decoded) | TableId::Event(_) | TableId::Derived(_) => {
                    self.decoded
                }
                TableId::Dataset(Table::Contract | Table::Reorg | Table::AcceptedBlock) => true,
            })
            .collect()
    }
}

/// The choices a seed makes once, up front.
#[derive(Debug, Clone)]
pub(super) struct Plan {
    pub(super) start: u64,
    pub(super) datasets: Datasets,
    lifetimes: u64,
    /// How many blocks under the head a fork may reach. Mostly shallow, so backfill
    /// runs over the buried heights below it while faults land; sometimes deeper than
    /// any run, so the whole run is the reorgable tail.
    finality: NonZeroU64,
    /// The stores one ingest feeds: a database, and sometimes a lake beside it.
    stores: Vec<StorePlan>,
}

/// The choices a seed makes for one process lifetime.
#[derive(Debug, Clone, Copy)]
struct Lifetime {
    /// The start height ingest is given: always when no store holds a block, and
    /// sometimes when one does, where it is ignored or refused.
    start: Option<u64>,
    /// Whether that start is past the block after every store's tip, so ingest must
    /// refuse it.
    refuse_start: bool,
    /// How the lifetime ends, unless something ends it first; `None` for the final one,
    /// which runs for five minutes.
    ending: Option<Ending>,
}

/// How a faulty lifetime ends, at a time after it starts.
#[derive(Debug, Clone, Copy)]
enum Ending {
    /// The process is killed.
    Crash(Duration),
    /// The process is asked to stop, as `SIGTERM` asks it.
    Stop(Duration),
}

/// How a process lifetime ended.
#[derive(Debug)]
enum End {
    /// Still running when the final lifetime's time ran out.
    Running,
    /// Killed by the crash timer.
    Crashed,
    /// Killed at a store statement.
    Killed,
    /// The pipeline stopped on its own, with this error.
    Stopped(RuntimeError),
    /// The pipeline returned, having been asked to stop.
    Drained,
    /// Asked to stop, and still running when the host would have killed it.
    Overran,
    /// The pipeline returned though nothing asked it to.
    Ended,
}

fn run(seed: u64, dir: &Path) -> Outcome {
    let mut rng = Rng::new(seed);
    let length = 10 + rng.below(40);
    // Without transactions, backfill fetches ranges of headers and logs; with them, full
    // blocks and their receipts a height at a time.
    let datasets = match rng.below(4) {
        0 => r#"["logs"]"#,
        1 => r#"["blocks", "logs"]"#,
        2 => r#"["transactions", "logs"]"#,
        _ => r#"["blocks", "transactions", "logs"]"#,
    };
    let mut roles = vec![StorePlan::database(&mut rng)];
    let plan = Plan {
        start: 1 + rng.below(length),
        datasets: serde_json::from_str(datasets).expect("datasets"),
        lifetimes: 1 + rng.below(5),
        finality: if rng.chance(80) {
            NonZeroU64::new(MIN_FINALITY + rng.below(13)).unwrap_or(NonZeroU64::MAX)
        } else {
            NonZeroU64::MAX
        },
        stores: {
            if rng.chance(50) {
                roles.push(StorePlan::lake(&mut rng));
            }
            roles
        },
    };
    let world: Shared = Arc::new(Mutex::new(World::new(rng, length)));
    lock(&world).trace.push(format!("{plan:?}"));
    protocol::write(dir);
    let mut problems = Vec::new();

    for lifetime in 0..plan.lifetimes {
        if lifetime > 0 {
            lock(&world).downtime();
        }
        let chosen = choose(&world, &plan, dir);
        let end = live(&world, dir, &plan, lifetime, chosen);
        let stored: Vec<Stored> = read_all(&plan, dir);
        let mut world = lock(&world);
        problems.extend(judge(&end, &world, chosen, true));
        problems.append(&mut world.problems);
        for (store, stored) in plan.stores.iter().zip(&stored) {
            problems.extend(oracle::check(&world, &plan, store, stored));
        }
    }

    // Faults off: the chain holds still, and the indexer must run without stopping and
    // catch up within a few minutes.
    lock(&world).faults = false;
    lock(&world).downtime();
    let fresh = read_all(&plan, dir)
        .iter()
        .all(|stored| oracle::ends(stored).is_none());
    let chosen = Lifetime {
        start: fresh.then_some(plan.start),
        refuse_start: false,
        ending: None,
    };
    let end = live(&world, dir, &plan, plan.lifetimes, chosen);
    let stored = read_all(&plan, dir);
    let mut world = lock(&world);
    problems.extend(judge(&end, &world, chosen, false));
    problems.append(&mut world.problems);
    for (store, stored) in plan.stores.iter().zip(&stored) {
        problems.extend(oracle::check(&world, &plan, store, stored));
    }
    problems.extend(caught_up(&world, &plan, &stored));
    Outcome {
        trace: world.trace.clone(),
        stored,
        problems,
        coverage: world.coverage.clone(),
    }
}

/// Every store's rows, in the plan's order.
fn read_all(plan: &Plan, dir: &Path) -> Vec<Stored> {
    plan.stores
        .iter()
        .map(|store| oracle::read(&dir.join(store.file)))
        .collect()
}

/// Draws how a faulty lifetime ends and where it is told to start.
fn choose(world: &Shared, plan: &Plan, dir: &Path) -> Lifetime {
    let stored = read_all(plan, dir);
    let mut world = lock(world);
    let ledgers: Vec<(u64, u64)> = stored
        .iter()
        .filter_map(oracle::ends)
        .filter_map(|(tip, first)| Some((first, world.block(&tip)?.number())))
        .collect();
    let at = Duration::from_millis(500 + world.rng.below(60_000));
    let ending = if world.rng.chance(25) {
        Ending::Stop(at)
    } else {
        Ending::Crash(at)
    };
    let (Some(oldest), Some(newest)) = (
        ledgers.iter().map(|&(first, _)| first).min(),
        ledgers.iter().map(|&(_, tip)| tip).max(),
    ) else {
        // No store holds a block, so ingest starts fresh, at the plan's start.
        return Lifetime {
            start: Some(plan.start),
            refuse_start: false,
            ending: Some(ending),
        };
    };
    let (start, refuse_start) = if !world.rng.chance(25) {
        (None, false)
    } else if world.rng.chance(50) {
        // At or below every store's oldest block: the stored history covers it, so
        // ingest resumes from the stores and ignores it.
        world.reached("start below the stored history");
        (Some(1 + world.rng.below(oldest)), false)
    } else {
        // Past the block after every store's tip: starting there would leave a gap.
        world.reached("start above the stored history");
        (Some(newest + 2 + world.rng.below(4)), true)
    };
    Lifetime {
        start,
        refuse_start,
        ending: Some(ending),
    }
}

/// Opens `store`, the plan's store at `index`, in `dir`, with the tables of `schema`
/// routed to it.
async fn open(
    world: &Shared,
    dir: &Path,
    schema: &Schema,
    index: usize,
    store: &StorePlan,
) -> Result<Opened<SimStore>, RuntimeError> {
    let config = duckdb::Config::default()
        .with("threads", "1")
        .expect("threads setting");
    let connection = duckdb::Connection::open_with_flags(dir.join(store.file), config)
        .expect("store file opens");
    let engine = FaultyEngine {
        inner: DuckDb { connection },
        world: Arc::clone(world),
        store: index,
        faults: lock(world).profile.stores[index],
        closed: false,
        landed: false,
    };
    let tables = store.tables(schema);
    let inner = SqlStore::new(engine, Arc::new(schema.select(&tables)), CHAIN)
        .await
        .map_err(|source| RuntimeError::OpenStore {
            store: store.backend,
            source,
        })?;
    Ok(Opened {
        backend: store.backend,
        store: SimStore {
            inner,
            world: Arc::clone(world),
            backend: store.backend,
            counts_bytes: store.counts_bytes,
            bytes: 0,
            holding: HashSet::new(),
            held: BTreeMap::new(),
        },
        batching: store.batching,
        tables,
        // The simulation runs on one thread to stay deterministic.
        runtime: StoreRuntime::current(),
    })
}

/// The source a lifetime reads: the real one, over the simulated node and its heads,
/// with the real retry layer jittered by `jitter`.
fn source(world: &Shared, plan: &Plan, jitter: u64) -> EvmSource<SimHeads> {
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
    EvmSource::new(
        CHAIN,
        http,
        heads,
        plan.datasets,
        CONCURRENCY,
        plan.finality,
    )
}

/// One process lifetime, ending as `chosen` says unless something ends it first. A
/// faulty one runs while the chain grows; the final one runs for five minutes while the
/// chain holds still.
fn live(world: &Shared, dir: &Path, plan: &Plan, lifetime: u64, chosen: Lifetime) -> End {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("runtime");
    let (jitter, killed) = {
        let mut world = lock(world);
        world.trace.push(format!("lifetime {lifetime}: {chosen:?}"));
        world.crash = Arc::new(Notify::new());
        world.store_faulted = [false; 2];
        world.stopping = false;
        world.fired.clear();
        (world.rng.next(), Arc::clone(&world.crash))
    };
    let end = runtime.block_on(async {
        let catalog = Catalog::load(dir.join("protocols"), &ChainId::new(CHAIN))
            .expect("the simulated protocol loads");
        let schema = catalog.schema().clone();
        let source = source(world, plan, jitter);
        let pipeline = Pipeline::new(source, chosen.start, None, catalog);

        // The chain grows while faults run, and holds still for the final catch-up.
        let chain = Arc::clone(world);
        let ticker = chosen.ending.is_some().then(|| {
            tokio::spawn(async move {
                loop {
                    let interval = 500 + lock(&chain).rng.below(4000);
                    tokio::time::sleep(Duration::from_millis(interval)).await;
                    lock(&chain).step();
                }
            })
        });
        let (request, stop) = Stop::new();
        let run = async {
            let mut stores = Vec::with_capacity(plan.stores.len());
            for (index, store) in plan.stores.iter().enumerate() {
                stores.push(open(world, dir, &schema, index, store).await?);
            }
            pipeline.run_with_stores(stores, stop).await
        };
        // A stop is asked for as `SIGTERM` asks it, and the host kills the process if it
        // has not ended by its stop timeout.
        let stopping = async {
            let Some(Ending::Stop(at)) = chosen.ending else {
                return std::future::pending().await;
            };
            tokio::time::sleep(at).await;
            {
                let mut world = lock(world);
                world.stopping = true;
                world.reached("graceful stop requested");
            }
            let _ = request.send(true);
            tokio::time::sleep(STOP_DEADLINE).await;
        };
        let deadline = match chosen.ending {
            Some(Ending::Crash(at)) => Some(at),
            Some(Ending::Stop(_)) => None,
            None => Some(Duration::from_mins(5)),
        };
        let timer = async {
            match deadline {
                Some(at) => tokio::time::sleep(at).await,
                None => std::future::pending().await,
            }
        };
        let end = tokio::select! {
            biased;
            () = killed.notified() => End::Killed,
            result = Box::pin(run) => match result {
                Ok(()) if lock(world).stopping => End::Drained,
                Ok(()) => End::Ended,
                Err(error) => End::Stopped(error),
            },
            () = stopping => End::Overran,
            () = timer => if chosen.ending.is_some() { End::Crashed } else { End::Running },
        };
        if let Some(ticker) = ticker {
            ticker.abort();
        }
        end
    });
    lock(world)
        .trace
        .push(format!("lifetime {lifetime}: {end:?}"));
    // Dropping the runtime drops every task mid-flight, stores included.
    drop(runtime);
    end
}

/// Why the way a lifetime ended should not have happened, if it should not.
///
/// A stop asked for must end the run within the host's stop timeout, unless a store
/// statement stalled past it, when the host's kill is the backstop it is in production.
/// A start above the stored history must be refused; the refusal comes before ingest
/// reads anything, so a lifetime that outlived it without a stall did not refuse.
fn judge(end: &End, world: &World, chosen: Lifetime, faults: bool) -> Option<String> {
    let stalled = world.fired.contains("store statement stalled");
    match end {
        End::Ended => Some("the run ended on its own, though nothing asked it to".to_owned()),
        End::Overran if !stalled => Some(format!(
            "a requested stop did not end the run within {} s",
            STOP_DEADLINE.as_secs()
        )),
        End::Stopped(RuntimeError::Ingest(PipelineError::StartAboveHistory { .. }))
            if chosen.refuse_start =>
        {
            None
        }
        End::Running | End::Crashed | End::Drained | End::Overran
            if chosen.refuse_start && !stalled && !world.stopping =>
        {
            Some(format!(
                "a start above the stored history was not refused: {chosen:?}"
            ))
        }
        End::Stopped(error) => unexpected(error, world, faults),
        _ => None,
    }
}

/// Why `error` should not have stopped the run, if it should not. With faults on, a
/// stop is the indexer giving up on a node that failed it as it was made to, or a store
/// failing as it was made to; anything else is a bug. With faults off, any stop is.
///
/// The simulated node never serves malformed data, so a source error is forgiven only
/// when a fault that fired in this lifetime explains it:
///
/// - A transport error or an HTTP status is a request that kept failing after the retry
///   layer gave up: refused or timed out until its retries ran out.
/// - An inconsistent answer outlives the pipeline's own retries only at startup, after
///   it asked a lagging backend thirty times; so does `UnstableSource`.
/// - A closed subscription is the head connection dropped until alloy stopped
///   reconnecting.
/// - A start above the head is a lagging backend sampled as the head, below a start
///   the newest view has reached; the pipeline refuses it rather than wait.
///
/// A store's error is forgiven only when that store's own faults fired.
fn unexpected(error: &RuntimeError, world: &World, faults: bool) -> Option<String> {
    let fired = |scenario: &str| world.fired.contains(scenario);
    let refused = fired("rpc refused") || fired("rpc timed out");
    let lagged = fired("backend lags") || fired("batch split across backends");
    let faulted = |backend: &StoreBackend| match backend {
        StoreBackend::Postgres => world.store_faulted[0],
        StoreBackend::Delta => world.store_faulted[1],
        StoreBackend::DuckDb => false,
    };
    let expected = faults
        && match error {
            RuntimeError::Ingest(error) => match error {
                PipelineError::Source(
                    SourceError::Transport { .. } | SourceError::Status { .. },
                ) if refused => true,
                PipelineError::Source(SourceError::Inconsistent { .. })
                | PipelineError::UnstableSource => lagged,
                PipelineError::SubscriptionClosed => fired("head subscription dropped"),
                PipelineError::StartAboveHead { .. } => fired("backend lags"),
                PipelineError::Sink(SinkError::StorageClosed) => {
                    world.store_faulted.iter().any(|&faulted| faulted)
                }
                _ => false,
            },
            RuntimeError::Storage { store, .. }
            | RuntimeError::OpenStore { store, .. }
            | RuntimeError::Resume(
                ResumeError::Restore { store, .. } | ResumeError::Rewind { store, .. },
            ) => faulted(store),
            _ => false,
        };
    (!expected).then(|| format!("unexpected stop: {error:?}"))
}

/// Why the stores, after the final lifetime, have not caught up: one store at least
/// holds blocks, from the plan's start, and every store that holds any has the
/// canonical tip. A store may start later than the plan's start, if it held nothing
/// while the others committed; it then joins where they resume, and the blocks before
/// are not backfilled into it (see `docs/multi-store-open-items.md`).
fn caught_up(world: &World, plan: &Plan, stored: &[Stored]) -> Vec<String> {
    let canonical = world.at(world.tip()).map(chain::SimBlock::hash);
    let mut problems = Vec::new();
    let mut oldest = None::<u64>;
    for (store, stored) in plan.stores.iter().zip(stored) {
        let Some((tip, first)) = oracle::ends(stored) else {
            continue;
        };
        oldest = Some(oldest.map_or(first, |oldest| oldest.min(first)));
        if Some(tip) != canonical {
            problems.push(format!(
                "{}'s tip {tip} is not the canonical tip {canonical:?}",
                store.backend
            ));
        }
    }
    if oldest != Some(plan.start) {
        problems.push(format!(
            "the stores begin at {oldest:?}, not at the start {}",
            plan.start
        ));
    }
    problems
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
