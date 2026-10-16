//! Where a store's work runs: on a runtime of its own, so it never waits in line behind
//! another part's.
//!
//! Tokio cannot interrupt a task: one runs on its worker until it awaits something that
//! is not ready. Ingest and a database's writer are mostly waiting on the network and
//! need a worker briefly but often; the lake's commit encodes and compresses Parquet for
//! seconds at a time without awaiting anything. On one shared runtime, a database commit
//! that becomes ready waits for a worker until the lake's encoding lets one go.
//!
//! So each store gets its own [`StoreRuntime`], and every async call to it runs there:
//! its open, its startup reads and rewind, its drain, and what any of those spawn — a
//! database's connection task, the lake's appends, checkpoints, and maintenance, whose
//! `tokio::spawn`s land on whichever runtime runs them. Its one synchronous call,
//! `reconcile`, runs where it is called, and spawns nothing. Ingest keeps the process's
//! main runtime. The operating system then shares the cores among every runtime's
//! threads, switching every few milliseconds, so a database commit waits milliseconds
//! for a core where it waited seconds for a worker.
//!
//! What still shares is the CPU itself, which is why a store must not flood it with
//! threads: the operating system shares it per thread, not per runtime. The lake builds
//! its Arrow batches on its own workers rather than on threads of their own for this
//! reason (see `sink::delta`).

use std::num::NonZeroUsize;

use tokio::runtime::{Builder, Handle, Runtime};
use tokio::task::JoinHandle;

use crate::sink::route::Backend;

/// The runtime a store's work runs on: one of its own, or, in tests and the
/// simulation, the runtime already running.
///
/// One of its own is shut down when this is dropped, without waiting for what is still
/// running there. A run drops it once the store's drain has ended, leaving only
/// background work such as the lake's maintenance, or when it stops or fails before
/// ingest starts, cancelling a startup read as dropping it in place would.
#[derive(Debug)]
pub(crate) struct StoreRuntime {
    handle: Handle,
    /// The runtime, when this owns one.
    owned: Option<Runtime>,
}

impl Drop for StoreRuntime {
    fn drop(&mut self) {
        // Dropping a runtime waits for its workers, which panics inside another
        // runtime's task, where this is usually dropped; a background shutdown does not.
        if let Some(runtime) = self.owned.take() {
            runtime.shutdown_background();
        }
    }
}

impl StoreRuntime {
    /// A runtime of its own for `store`, with [`workers`] threads named after it.
    ///
    /// # Errors
    ///
    /// Returns the operating system's error when the runtime's threads cannot be made.
    pub(crate) fn dedicated(store: Backend) -> std::io::Result<Self> {
        let runtime = Builder::new_multi_thread()
            .worker_threads(workers(store).get())
            .thread_name(format!("store-{store}"))
            .enable_all()
            .build()?;
        Ok(Self {
            handle: runtime.handle().clone(),
            owned: Some(runtime),
        })
    }

    /// The runtime already running, for a store that should share it: in tests, and in
    /// the simulation, which must run on one thread to stay deterministic.
    ///
    /// # Panics
    ///
    /// Panics outside a runtime.
    #[cfg(all(test, feature = "duckdb"))]
    pub(crate) fn current() -> Self {
        Self {
            handle: Handle::current(),
            owned: None,
        }
    }

    /// Starts `work` on this runtime.
    pub(crate) fn spawn<T: Send + 'static>(
        &self,
        work: impl Future<Output = T> + Send + 'static,
    ) -> JoinHandle<T> {
        self.handle.spawn(work)
    }

    /// Runs `work` on this runtime and waits for it, from any runtime. A panic in it is
    /// a panic here, as if it had run in place.
    ///
    /// The task is never cancelled under its waiter: only shutting the runtime down
    /// cancels it, which dropping this does, and this is borrowed for the wait.
    pub(crate) async fn run<T: Send + 'static>(
        &self,
        work: impl Future<Output = T> + Send + 'static,
    ) -> T {
        self.spawn(work)
            .await
            .unwrap_or_else(|join| std::panic::resume_unwind(join.into_panic()))
    }
}

/// How many threads `store`'s runtime gets: as many as its work can use at once.
///
/// - The lake, one per core: a commit builds and encodes many tables' batches at once,
///   and the operating system shares the cores with the other runtimes' threads.
/// - A database, one: its drain commits over one connection, one transaction at a time,
///   mostly waiting on the network. `DuckDB`'s calls block, and it parallelizes queries
///   on threads of its own.
fn workers(store: Backend) -> NonZeroUsize {
    match store {
        Backend::Delta => std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN),
        Backend::DuckDb | Backend::Postgres | Backend::Stdout => NonZeroUsize::MIN,
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use super::StoreRuntime;
    use crate::sink::route::Backend;

    /// Work handed to a store's runtime runs on that runtime's threads, and what it
    /// spawns lands there too, while the caller's runtime only waits for it.
    #[tokio::test]
    async fn work_and_what_it_spawns_run_on_the_stores_own_threads() {
        let store = StoreRuntime::dedicated(Backend::Postgres).expect("a runtime");
        let names = store
            .run(async {
                let own = std::thread::current().name().map(str::to_owned);
                let spawned =
                    tokio::spawn(async { std::thread::current().name().map(str::to_owned) })
                        .await
                        .expect("the spawned task runs");
                (own, spawned)
            })
            .await;
        let store_thread = Some("store-postgres".to_owned());
        assert_eq!(names, (store_thread.clone(), store_thread));
        assert_ne!(
            std::thread::current().name(),
            Some("store-postgres"),
            "the caller stays on its own runtime"
        );
    }

    /// A runtime of a store's own may be dropped inside another runtime's task, which is
    /// where a run lets go of it.
    #[tokio::test]
    async fn a_stores_runtime_can_be_dropped_inside_another() {
        let store = StoreRuntime::dedicated(Backend::Postgres).expect("a runtime");
        store.spawn(std::future::pending::<()>());
        drop(store);
    }
}
