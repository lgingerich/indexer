//! Store faults at the [`Engine`] boundary, whatever engine sits underneath.
//!
//! The engine itself is trusted: a transaction commits whole or not at all, which is
//! its contract and not ours to model. What the simulation adds is what a client of any
//! database sees go wrong — a statement fails, a commit lands but its acknowledgement is
//! lost, the connection drops, or the process dies between two statements — which
//! `PostgreSQL` exhibits in production as readily as `DuckDB` does here.
//!
//! A failed statement or a lost acknowledgement sometimes takes the connection with it,
//! as a server that closed the socket does. Every statement after fails until the store
//! [recovers](Engine::recover), which here opens a new session on the same database, as
//! `PostgreSQL`'s engine connects again: the transaction under way on the old one is
//! rolled back with it, and a flush whose commit landed is written again on the new one.

use std::sync::Arc;

use super::CHAIN;
use super::chain::{Shared, lock};
use crate::sink::duckdb::DuckDb;
use crate::sink::sql::Dialect as _;
use crate::sink::store::Engine;
use crate::sink::store_error::{EngineError, Operation, StoreError};
use crate::sink::table::{Row, TableDef, Value};

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

/// An engine that fails as the world's fault profile says.
#[derive(Debug)]
pub(super) struct FaultyEngine<E> {
    pub(super) inner: E,
    pub(super) world: Shared,
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

impl<E> FaultyEngine<E> {
    /// Before a statement: fails it, kills the process, or lets it run, returning
    /// whether the process dies once it has. A failure sometimes closes the connection.
    async fn before(&mut self) -> Result<bool, EngineError> {
        if self.closed {
            return Err("simulated closed connection".into());
        }
        match gate(&self.world).await {
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
        if committed && world.fault(|p| p.commit_lost) {
            world.store_faulted = true;
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
            let refused = world.fault(|p| p.store_fail * 10);
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
