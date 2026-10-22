//! Store faults at the [`Engine`] boundary, whatever engine sits underneath, and the
//! store the simulation runs over it.
//!
//! The engine itself is trusted: a transaction commits whole or not at all, which is
//! its contract and not ours to model. What the simulation adds is what a client of any
//! database sees go wrong — a statement fails or stalls, a commit lands but its
//! acknowledgement is lost, the connection drops, or the process dies between two
//! statements — which `PostgreSQL` exhibits in production as readily as `DuckDB` does
//! here. Each store fails at its own rates, as a database and a lake do.
//!
//! A failed statement or a lost acknowledgement sometimes takes the connection with it,
//! as a server that closed the socket does. Every statement after fails until the store
//! [recovers](Engine::recover), which here opens a new session on the same database, as
//! `PostgreSQL`'s engine connects again: the transaction under way on the old one is
//! rolled back with it, and a flush whose commit landed is written again on the new one.
//!
//! [`SimStore`] wraps the SQL store to stand in for either role. It reports the bytes it
//! buffers, so a lake's byte limit flushes it, and it holds every store to the lake's
//! contract, which a SQL store's upsert would hide: no store is handed a block it
//! already holds, since the lake appends.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::B256;

use super::CHAIN;
use super::chain::{Shared, StoreFaults, lock};
use crate::sink::duckdb::DuckDb;
use crate::sink::route::Backend;
use crate::sink::sql::Dialect as _;
use crate::sink::store::Engine;
use crate::sink::store_error::{EngineError, Operation, StoreError};
use crate::sink::table::{Row, TableDef, Value};
use crate::sink::{EnvelopeSink, Restored, SinkError, SqlStore, Store};
use crate::wire::envelope::{BlockMeta, Envelope, Event};

/// An engine the simulation can connect again: a new session on the same database.
pub(super) trait Reconnect: Engine + Sized {
    /// A new session, with nothing of the old one's state.
    fn reconnect(&self) -> Result<Self, EngineError>;
}

impl Reconnect for DuckDb {
    fn reconnect(&self) -> Result<Self, EngineError> {
        Ok(Self {
            connection: self.connection.try_clone()?,
        })
    }
}

/// An engine that fails as its store's faults say.
#[derive(Debug)]
pub(super) struct FaultyEngine<E> {
    pub(super) inner: E,
    pub(super) world: Shared,
    /// The store's position in the plan, which its faults are recorded under.
    pub(super) store: usize,
    pub(super) faults: StoreFaults,
    /// Whether the connection is gone, failing every statement until a recover.
    pub(super) closed: bool,
    /// Whether the connection went with a commit that landed, which the store writes
    /// again once it connects again.
    pub(super) landed: bool,
}

/// What the gate decided for one statement.
enum Gate {
    Run { crash_after: bool },
    Fail,
}

/// Kills the process: the driver drops the runtime, and this never returns.
async fn die(world: &Shared, scenario: &'static str) -> Gate {
    let crash = {
        let mut world = lock(world);
        world.reached(scenario);
        Arc::clone(&world.crash)
    };
    crash.notify_one();
    std::future::pending().await
}

/// Yields a seeded number of times, so the storage tasks and ingest interleave
/// differently from seed to seed, sometimes stalls, then decides the fate of a
/// statement of the store at position `store`.
async fn gate(world: &Shared, faults: StoreFaults, store: usize) -> Gate {
    let (yields, stall) = {
        let mut world = lock(world);
        let yields = world.rng.below(3);
        let stall = world.fires(faults.stall).then(|| {
            world.reached("store statement stalled");
            Duration::from_secs(1 + world.rng.below(40))
        });
        (yields, stall)
    };
    for _ in 0..yields {
        tokio::task::yield_now().await;
    }
    if let Some(stall) = stall {
        tokio::time::sleep(stall).await;
    }
    let crash = {
        let mut world = lock(world);
        if world.fires(faults.crash) {
            world.store_faulted[store] = true;
            Some(world.rng.chance(50))
        } else if world.fires(faults.fail) {
            world.store_faulted[store] = true;
            world.reached("store statement failed");
            return Gate::Fail;
        } else {
            None
        }
    };
    match crash {
        Some(false) => die(world, "process killed before a store statement").await,
        Some(true) => Gate::Run { crash_after: true },
        None => Gate::Run { crash_after: false },
    }
}

impl<E> FaultyEngine<E> {
    /// Before a statement: fails it, kills the process, or lets it run, returning
    /// whether the process dies once it has. A failure sometimes closes the connection.
    async fn before(&mut self) -> Result<bool, EngineError> {
        if self.closed {
            return Err("simulated closed connection".into());
        }
        match gate(&self.world, self.faults, self.store).await {
            Gate::Fail => {
                self.closed = lock(&self.world).rng.chance(50);
                Err("simulated store failure".into())
            }
            Gate::Run { crash_after } => Ok(crash_after),
        }
    }

    /// After a statement ran: kills the process if the gate said so, and may lose a
    /// commit's acknowledgement, sometimes with the connection.
    async fn after(&mut self, crash_after: bool, committed: bool) -> Result<(), EngineError> {
        if crash_after {
            die(&self.world, "process killed after a store statement").await;
        }
        let mut world = lock(&self.world);
        if committed && world.fires(self.faults.commit_lost) {
            world.store_faulted[self.store] = true;
            world.reached("commit landed, acknowledgement lost");
            self.closed = world.rng.chance(50);
            self.landed = self.closed;
            return Err("simulated lost commit acknowledgement".into());
        }
        Ok(())
    }
}

impl<E: Reconnect> Engine for FaultyEngine<E> {
    type Dialect = E::Dialect;

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<(), EngineError> {
        let crash_after = self.before().await?;
        let result = self.inner.execute(sql, params).await;
        self.after(crash_after, sql == "COMMIT" && result.is_ok())
            .await?;
        result
    }

    async fn query(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>, EngineError> {
        let crash_after = self.before().await?;
        let result = self.inner.query(sql, params).await;
        self.after(crash_after, false).await?;
        result
    }

    async fn load(
        &mut self,
        table: &TableDef,
        staging: &str,
        rows: &[&Row],
    ) -> Result<(), EngineError> {
        let crash_after = self.before().await?;
        let result = self.inner.load(table, staging, rows).await;
        self.after(crash_after, false).await?;
        result
    }

    /// Connects again when the connection is closed, as `PostgreSQL`'s engine does, and
    /// makes the store's schema the session's default again. Connecting can fail too,
    /// which stops the store as a database that stays down does.
    async fn recover(&mut self) -> Result<bool, StoreError> {
        if !self.closed {
            return Ok(false);
        }
        let refused = {
            let mut world = lock(&self.world);
            let refused = world.fires(self.faults.fail * 10);
            world.reached(if refused {
                "store could not connect again"
            } else {
                "store connected again"
            });
            refused
        };
        if refused {
            return Err(StoreError::engine(Operation::Open, None)(
                "simulated refused connection",
            ));
        }
        let mut inner = self
            .inner
            .reconnect()
            .map_err(StoreError::engine(Operation::Open, None))?;
        inner
            .execute(&E::Dialect::use_schema(CHAIN), &[])
            .await
            .map_err(StoreError::engine(Operation::Configure, Some(CHAIN)))?;
        // The old session goes, and its open transaction with it.
        self.inner = inner;
        self.closed = false;
        if std::mem::take(&mut self.landed) {
            lock(&self.world).reached("landed commit written again");
        }
        Ok(true)
    }
}

/// The SQL store over a faulty engine, as either role a deployment's stores play.
#[derive(Debug)]
pub(super) struct SimStore {
    pub(super) inner: SqlStore<FaultyEngine<DuckDb>>,
    pub(super) world: Shared,
    pub(super) backend: Backend,
    /// Whether it reports what it buffers, as the lake does, so a byte limit applies.
    pub(super) counts_bytes: bool,
    /// What the envelopes buffered since the last flush take as JSON.
    pub(super) bytes: usize,
    /// The blocks committed and buffered, by hash: what the store holds once the next
    /// flush lands. A flush that fails stops the store, so the difference never matters.
    pub(super) holding: HashSet<B256>,
    /// The blocks resume said this store holds above where ingest resumes, by height,
    /// each of which ingest hands it once again.
    pub(super) held: BTreeMap<u64, B256>,
}

impl SimStore {
    /// Notes what `envelope` does to the blocks the store holds, and whether ingest
    /// handed it a block it already holds.
    fn track(&mut self, envelope: &Envelope) {
        match &envelope.event {
            Event::AcceptedBlock(meta) => {
                if let Some(held) = self.held.remove(&meta.height) {
                    if held == meta.hash {
                        lock(&self.world).reached("held block handed again and skipped");
                        return;
                    }
                    lock(&self.world).reached("held block replaced by another branch");
                    self.holding.remove(&held);
                }
                if !self.holding.insert(meta.hash) {
                    lock(&self.world).problems.push(format!(
                        "{} was handed block {} at {}, which it already holds; a lake would \
                         hold it twice",
                        self.backend, meta.hash, meta.height
                    ));
                }
            }
            Event::Reorg(reorg) => {
                for orphan in &reorg.orphaned_hashes {
                    self.holding.remove(orphan);
                }
            }
            _ => {}
        }
    }
}

/// A flush under way, which notes when a requested stop drops it before it finishes.
struct Flushing<'a> {
    world: &'a Shared,
    done: bool,
}

impl Drop for Flushing<'_> {
    fn drop(&mut self) {
        let mut world = lock(self.world);
        if !self.done && world.stopping {
            world.reached("stop abandoned a commit");
        }
    }
}

impl EnvelopeSink for SimStore {
    async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
        self.track(&envelope);
        if self.counts_bytes {
            self.bytes += serde_json::to_vec(&envelope).map_or(0, |json| json.len());
        }
        self.inner.publish(envelope).await
    }

    async fn flush(&mut self) -> Result<(), SinkError> {
        let mut flushing = Flushing {
            world: &self.world,
            done: false,
        };
        let flushed = self.inner.flush().await;
        flushing.done = true;
        drop(flushing);
        flushed?;
        self.bytes = 0;
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), SinkError> {
        self.inner.finish().await
    }

    fn buffered_bytes(&self) -> usize {
        self.bytes
    }

    fn observe_head(&mut self, height: u64) {
        self.inner.observe_head(height);
    }
}

impl Store for SimStore {
    async fn restore(&mut self) -> Result<Restored, StoreError> {
        let restored = self.inner.restore().await?;
        self.holding = restored.ledger.iter().map(|block| block.hash).collect();
        Ok(restored)
    }

    fn reconcile(&mut self, held: &[BlockMeta]) {
        self.held = held
            .iter()
            .map(|block| (block.height, block.hash))
            .collect();
        self.inner.reconcile(held);
    }

    async fn rewind(&mut self, height: u64) -> Result<(), StoreError> {
        lock(&self.world).reached("store rewound");
        // What it holds is read back again after a rewind.
        self.inner.rewind(height).await
    }
}
