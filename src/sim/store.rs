//! Store faults at the [`Engine`] boundary, whatever engine sits underneath.
//!
//! The engine itself is trusted: a transaction commits whole or not at all, which is
//! its contract and not ours to model. What the simulation adds is what a client of any
//! database sees go wrong — a statement fails, a commit lands but its acknowledgement is
//! lost, or the process dies between two statements — which `PostgreSQL` exhibits in
//! production as readily as `DuckDB` does here.

use std::sync::Arc;

use super::chain::{Shared, lock};
use crate::sink::store::{Engine, EngineError};
use crate::sink::table::{Row, TableDef, Value};

/// An engine that fails as the world's fault profile says.
#[derive(Debug)]
pub(super) struct FaultyEngine<E> {
    pub(super) inner: E,
    pub(super) world: Shared,
}

/// What the gate decided for one statement.
enum Gate {
    Run { crash_after: bool },
    Fail,
}

/// Yields a seeded number of times, so the storage task and ingest interleave
/// differently from seed to seed, then decides a statement's fate.
async fn gate(world: &Shared) -> Gate {
    let yields = lock(world).rng.below(3);
    for _ in 0..yields {
        tokio::task::yield_now().await;
    }
    let crash = {
        let mut world = lock(world);
        if world.fault(|p| p.store_crash) {
            world.store_faulted = true;
            Some(world.rng.chance(50))
        } else if world.fault(|p| p.store_fail) {
            world.store_faulted = true;
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

/// Before a statement: fails it, kills the process, or lets it run, returning whether
/// the process dies once it has.
async fn before(world: &Shared) -> Result<bool, EngineError> {
    match gate(world).await {
        Gate::Fail => Err("simulated store failure".into()),
        Gate::Run { crash_after } => Ok(crash_after),
    }
}

/// After a statement ran: kills the process if the gate said so, and may lose a commit's
/// acknowledgement.
async fn after(world: &Shared, crash_after: bool, committed: bool) -> Result<(), EngineError> {
    if crash_after {
        die(world, "process killed after a store statement").await;
    }
    let mut world = lock(world);
    if committed && world.fault(|p| p.commit_lost) {
        world.store_faulted = true;
        world.reached("commit landed, acknowledgement lost");
        return Err("simulated lost commit acknowledgement".into());
    }
    Ok(())
}

impl<E: Engine> Engine for FaultyEngine<E> {
    type Dialect = E::Dialect;

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<(), EngineError> {
        let crash_after = before(&self.world).await?;
        let result = self.inner.execute(sql, params).await;
        after(&self.world, crash_after, sql == "COMMIT" && result.is_ok()).await?;
        result
    }

    async fn query(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>, EngineError> {
        let crash_after = before(&self.world).await?;
        let result = self.inner.query(sql, params).await;
        after(&self.world, crash_after, false).await?;
        result
    }

    async fn load(
        &mut self,
        table: &TableDef,
        staging: &str,
        rows: &[&Row],
    ) -> Result<(), EngineError> {
        let crash_after = before(&self.world).await?;
        let result = self.inner.load(table, staging, rows).await;
        after(&self.world, crash_after, false).await?;
        result
    }
}
