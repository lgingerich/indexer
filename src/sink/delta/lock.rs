//! One backfill at a time per chain and lake: a lease kept in a small Delta table,
//! `_backfill_lock`, beside the chain's tables.
//!
//! Every change to the lease is a commit carrying one application transaction id, and
//! Delta refuses a commit when a concurrent one carried the same id. So of two runs that
//! read the lease free and claim it at once, exactly one commits, whatever the object
//! store, with the same conditional writes every Delta commit relies on.
//!
//! The holder renews its lease every [`HEARTBEAT`] and releases it when done. A lease
//! whose last heartbeat is older than [`STALE`] was left by a run that stopped without
//! releasing it, and another run may take it over; the stopped run, should it come back,
//! finds its next renewal refused and stops.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use deltalake::DeltaTable;
use deltalake::arrow::array::{BooleanArray, Int64Array, RecordBatch, StringArray};
use deltalake::arrow::datatypes::{
    DataType as ArrowType, Field as ArrowField, Schema as ArrowSchema,
};
use deltalake::datafusion::prelude::{SessionContext, ident};
use deltalake::kernel::{DataType, StructField, Transaction};
use deltalake::protocol::SaveMode;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::warn;

use super::lake::commit_properties;
use super::open::Storage;
use crate::secret::Secret;
use crate::sink::store_error::{EngineError, Operation, StoreError};

/// The lease's table, under `<uri>/<chain>/`.
const TABLE: &str = "_backfill_lock";

/// The application transaction id every change to the lease carries.
const APP_ID: &str = "indexer-backfill";

/// How often the holder renews its lease.
const HEARTBEAT: Duration = Duration::from_mins(1);

/// How long after its last heartbeat a lease may be taken over.
const STALE: Duration = Duration::from_mins(10);

/// Why a backfill could not take, or lost, the lease.
#[derive(Debug, thiserror::Error)]
pub enum LockError {
    /// Another run holds the lease.
    #[error(
        "backfill {run} of {protocols} holds the lake (its last heartbeat {seconds}s ago); \
         one backfill runs at a time, and a stopped one's lease frees itself after {}s",
        STALE.as_secs()
    )]
    Held {
        /// The holder's run id.
        run: String,
        /// The protocols it backfills.
        protocols: String,
        /// Seconds since its last heartbeat.
        seconds: u64,
    },
    /// Another run took the lease over after this one missed its heartbeats.
    #[error("another backfill took over this run's lease after it missed its heartbeats")]
    Lost,
    /// The lease's table could not be read or written.
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// The newest change to the lease.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Lease {
    run: String,
    protocols: String,
    /// The change's position, which is its commit's application transaction version.
    sequence: i64,
    /// When it was made, in seconds since the Unix epoch.
    heartbeat: i64,
    released: bool,
}

impl Lease {
    /// Whether another run may take the lease: released, or not renewed for [`STALE`].
    fn free(&self, now: i64) -> bool {
        self.released || self.age(now) >= STALE.as_secs()
    }

    fn age(&self, now: i64) -> u64 {
        u64::try_from(now.saturating_sub(self.heartbeat)).unwrap_or_default()
    }
}

/// The lease, held: renewed in the background until [`release`](Self::release)d.
/// Dropped without a release, it stops renewing and frees itself after ten minutes.
#[derive(Debug)]
pub struct RunLock {
    /// Set once a renewal finds the lease taken over.
    lost: Arc<AtomicBool>,
    stop: Option<oneshot::Sender<()>>,
    renewing: Option<JoinHandle<Result<(), LockError>>>,
}

impl RunLock {
    /// Takes the lease on `<uri>/<chain>/` for run `run` backfilling `protocols`,
    /// creating its table when there is none.
    ///
    /// # Errors
    ///
    /// Returns [`LockError::Held`] when another run holds a lease renewed within
    /// ten minutes, or wins the race to claim a free one, and [`LockError::Store`] when the
    /// table cannot be read or written.
    pub async fn acquire(
        uri: &str,
        storage: &std::collections::BTreeMap<String, Secret>,
        chain: &str,
        run: &str,
        protocols: &str,
    ) -> Result<Self, LockError> {
        let root = uri.trim_end_matches('/');
        let storage = Storage::new(root, storage)?;
        let uri = format!("{root}/{chain}/{TABLE}");
        let table = open(&storage, &uri).await?;
        let now = unix_now();
        let next = match latest(&table).await? {
            Some(lease) if !lease.free(now) => return Err(held(lease, now)),
            Some(lease) => lease.sequence + 1,
            None => 0,
        };
        let claim = Lease {
            run: run.to_owned(),
            protocols: protocols.to_owned(),
            sequence: next,
            heartbeat: now,
            released: false,
        };
        let table = match commit(table.clone(), &claim).await {
            Ok(table) => table,
            // Another run claimed it first: name it.
            Err(error) => {
                let mut table = table;
                table
                    .update_state()
                    .await
                    .map_err(StoreError::engine(Operation::Open, Some(TABLE)))?;
                return match latest(&table).await? {
                    Some(lease) if lease.run != run => Err(held(lease, unix_now())),
                    _ => Err(error.into()),
                };
            }
        };
        let lost = Arc::new(AtomicBool::new(false));
        let (stop, stopped) = oneshot::channel();
        let renewing = tokio::spawn(renew(table, claim, Arc::clone(&lost), stopped));
        Ok(Self {
            lost,
            stop: Some(stop),
            renewing: Some(renewing),
        })
    }

    /// Fails once a renewal has found the lease taken over, so the run stops before its
    /// next write.
    ///
    /// # Errors
    ///
    /// Returns [`LockError::Lost`] once the lease is lost.
    pub fn check(&self) -> Result<(), LockError> {
        if self.lost.load(Ordering::Acquire) {
            Err(LockError::Lost)
        } else {
            Ok(())
        }
    }

    /// Releases the lease, so the next run need not wait for it to go stale.
    ///
    /// # Errors
    ///
    /// Returns [`LockError::Lost`] when the lease was taken over, and
    /// [`LockError::Store`] when the release cannot be written.
    pub async fn release(mut self) -> Result<(), LockError> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        match self.renewing.take() {
            Some(renewing) => renewing
                .await
                .unwrap_or_else(|join| std::panic::resume_unwind(join.into_panic())),
            None => Ok(()),
        }
    }
}

/// Whether a backfill holds the lake, as the running indexer asks it: cheaply, since it
/// asks every minute. Each check lists the lease's log for commits past the one it last
/// read, and reads the lease again only when there is one; the lease's table is not
/// created here, so a lake no backfill ever ran on stays as it is.
#[derive(Debug)]
pub(super) struct LeaseWatch {
    uri: String,
    table: Option<DeltaTable>,
    /// The newest change to the lease, as of `table`'s version.
    lease: Option<Lease>,
}

impl LeaseWatch {
    /// Watches the lease on `<root>/<chain>/`.
    pub(super) fn new(root: &str, chain: &str) -> Self {
        Self {
            uri: format!("{}/{chain}/{TABLE}", root.trim_end_matches('/')),
            table: None,
            lease: None,
        }
    }

    /// Whether a backfill holds the lease now: claimed, not released, and renewed within
    /// [`STALE`].
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when the table cannot be listed or read.
    pub(super) async fn held(&mut self, storage: &Storage) -> Result<bool, StoreError> {
        let reading = || StoreError::engine(Operation::Read, Some(TABLE));
        let version = if let Some(table) = &mut self.table {
            let before = table.version();
            table.update_state().await.map_err(reading())?;
            (table.version() != before).then_some(())
        } else {
            let table = storage.table(&self.uri).await.map_err(reading())?;
            if table.version().is_none() {
                return Ok(false);
            }
            self.table = Some(table);
            Some(())
        };
        if let (Some(()), Some(table)) = (version, &self.table) {
            self.lease = latest(table).await?;
        }
        Ok(self
            .lease
            .as_ref()
            .is_some_and(|lease| !lease.free(unix_now())))
    }
}

impl Drop for RunLock {
    fn drop(&mut self) {
        if let Some(renewing) = &self.renewing {
            renewing.abort();
        }
    }
}

/// Renews `lease` every [`HEARTBEAT`] until `stopped`, then releases it. A renewal that
/// fails is checked against the table: if another run's change is newest, the lease is
/// lost; otherwise the next renewal tries again.
async fn renew(
    mut table: DeltaTable,
    mut lease: Lease,
    lost: Arc<AtomicBool>,
    mut stopped: oneshot::Receiver<()>,
) -> Result<(), LockError> {
    let mut ticks = tokio::time::interval(HEARTBEAT);
    ticks.tick().await;
    loop {
        let release = tokio::select! {
            _ = &mut stopped => true,
            _ = ticks.tick() => false,
        };
        let change = Lease {
            sequence: lease.sequence + 1,
            heartbeat: unix_now(),
            released: release,
            ..lease.clone()
        };
        match commit(table.clone(), &change).await {
            Ok(committed) => {
                table = committed;
                lease = change;
            }
            Err(error) => {
                table
                    .update_state()
                    .await
                    .map_err(StoreError::engine(Operation::Open, Some(TABLE)))?;
                match latest(&table).await? {
                    Some(newest) if newest.run != lease.run => {
                        lost.store(true, Ordering::Release);
                        return Err(LockError::Lost);
                    }
                    Some(newest) => lease = newest,
                    None => {}
                }
                warn!(%error, "backfill lease renewal failed; trying again");
                if release {
                    return Err(error.into());
                }
                continue;
            }
        }
        if release {
            return Ok(());
        }
    }
}

/// The lease's table at `uri`, created when there is none. Two runs creating it at once
/// both end up opening the one that was made.
async fn open(storage: &Storage, uri: &str) -> Result<DeltaTable, StoreError> {
    let opening = || StoreError::engine(Operation::Open, Some(TABLE));
    let table = storage.table(uri).await.map_err(opening())?;
    if table.version().is_some() {
        return Ok(table);
    }
    let created = table
        .create()
        .with_table_name(TABLE)
        .with_columns([
            StructField::new("run", DataType::STRING, false),
            StructField::new("protocols", DataType::STRING, false),
            StructField::new("sequence", DataType::LONG, false),
            StructField::new("heartbeat", DataType::LONG, false),
            StructField::new("released", DataType::BOOLEAN, false),
        ])
        .await;
    match created {
        Ok(table) => Ok(table),
        Err(_) => storage.table(uri).await.map_err(opening()),
    }
}

/// The newest change to the lease, `None` before the first.
async fn latest(table: &DeltaTable) -> Result<Option<Lease>, StoreError> {
    let read = async {
        let ctx = SessionContext::new();
        table.update_datafusion_session(&ctx.state())?;
        let batches = ctx
            .read_table(table.table_provider().await?)?
            .sort(vec![ident("sequence").sort(false, false)])?
            .limit(0, Some(1))?
            .collect()
            .await?;
        Ok::<_, EngineError>(batches)
    };
    let batches = read
        .await
        .map_err(StoreError::engine(Operation::Read, Some(TABLE)))?;
    let Some(batch) = batches.iter().find(|batch| batch.num_rows() > 0) else {
        return Ok(None);
    };
    let column = |name: &str| batch.column_by_name(name);
    let malformed = || StoreError::engine(Operation::Read, Some(TABLE))("a malformed lease row");
    let text = |name: &str| -> Result<String, StoreError> {
        let values =
            deltalake::arrow::compute::cast(column(name).ok_or_else(malformed)?, &ArrowType::Utf8)
                .map_err(StoreError::engine(Operation::Read, Some(TABLE)))?;
        values
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|values| values.value(0).to_owned())
            .ok_or_else(malformed)
    };
    let long = |name: &str| -> Result<i64, StoreError> {
        column(name)
            .and_then(|values| values.as_any().downcast_ref::<Int64Array>())
            .map(|values| values.value(0))
            .ok_or_else(malformed)
    };
    Ok(Some(Lease {
        run: text("run")?,
        protocols: text("protocols")?,
        sequence: long("sequence")?,
        heartbeat: long("heartbeat")?,
        released: column("released")
            .and_then(|values| values.as_any().downcast_ref::<BooleanArray>())
            .map(|values| values.value(0))
            .ok_or_else(malformed)?,
    }))
}

/// Appends `lease` as one commit carrying the lease's application transaction, which
/// Delta refuses when a concurrent commit carried it too.
async fn commit(table: DeltaTable, lease: &Lease) -> Result<DeltaTable, StoreError> {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("run", ArrowType::Utf8, false),
        ArrowField::new("protocols", ArrowType::Utf8, false),
        ArrowField::new("sequence", ArrowType::Int64, false),
        ArrowField::new("heartbeat", ArrowType::Int64, false),
        ArrowField::new("released", ArrowType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![lease.run.as_str()])),
            Arc::new(StringArray::from(vec![lease.protocols.as_str()])),
            Arc::new(Int64Array::from(vec![lease.sequence])),
            Arc::new(Int64Array::from(vec![lease.heartbeat])),
            Arc::new(BooleanArray::from(vec![lease.released])),
        ],
    )
    .map_err(StoreError::engine(Operation::Append, Some(TABLE)))?;
    table
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .with_commit_properties(
            commit_properties()
                .with_application_transaction(Transaction::new(APP_ID, lease.sequence)),
        )
        .await
        .map_err(StoreError::engine(Operation::Commit, Some(TABLE)))
}

fn held(lease: Lease, now: i64) -> LockError {
    LockError::Held {
        seconds: lease.age(now),
        run: lease.run,
        protocols: lease.protocols,
    }
}

/// Seconds since the Unix epoch.
fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_secs()).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
// Test code: a failed expectation means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::{Lease, LockError, RunLock, STALE, Storage, TABLE, commit, latest, open, unix_now};

    const CHAIN: &str = "base";

    /// A fresh directory for one test's lake, removed when the guard drops.
    struct Dir(PathBuf);

    impl Dir {
        fn new() -> Self {
            static NEXT: AtomicU32 = AtomicU32::new(0);
            Self(std::env::temp_dir().join(format!(
                "indexer-lock-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )))
        }

        fn uri(&self) -> String {
            self.0.display().to_string()
        }

        async fn acquire(&self, run: &str) -> Result<RunLock, LockError> {
            RunLock::acquire(&self.uri(), &BTreeMap::new(), CHAIN, run, "uniswap_v4").await
        }

        /// Writes a change to the lease as another run would, behind the holder's back.
        async fn change(&self, lease: Lease) {
            let storage = Storage::new(&self.uri(), &BTreeMap::new()).expect("storage");
            let table = open(&storage, &format!("{}/{CHAIN}/{TABLE}", self.uri()))
                .await
                .expect("the lease's table");
            commit(table, &lease).await.expect("committed");
        }

        async fn latest(&self) -> Lease {
            let storage = Storage::new(&self.uri(), &BTreeMap::new()).expect("storage");
            let table = open(&storage, &format!("{}/{CHAIN}/{TABLE}", self.uri()))
                .await
                .expect("the lease's table");
            latest(&table).await.expect("read").expect("a lease")
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// A held lease refuses a second run, naming the holder; released, the next run takes
    /// it.
    #[tokio::test]
    async fn one_run_holds_the_lease_until_it_releases_it() {
        let dir = Dir::new();
        let first = dir.acquire("first").await.expect("the lease is free");
        let refused = dir.acquire("second").await;
        assert!(
            matches!(&refused, Err(LockError::Held { run, .. }) if run == "first"),
            "{refused:?}"
        );
        first.check().expect("still held");
        first.release().await.expect("released");
        assert!(dir.latest().await.released);
        let second = dir.acquire("second").await.expect("free once released");
        second.release().await.expect("released");
    }

    /// Of runs claiming a free lease at once, exactly one gets it.
    #[tokio::test(flavor = "multi_thread")]
    async fn of_runs_claiming_at_once_one_wins() {
        let dir = Dir::new();
        dir.acquire("setup")
            .await
            .expect("the lease is free")
            .release()
            .await
            .expect("released");
        let dir = &dir;
        let claims = futures_util::future::join_all(
            (0..4).map(|run| async move { dir.acquire(&format!("run-{run}")).await }),
        )
        .await;
        let (won, lost): (Vec<_>, Vec<_>) = claims.into_iter().partition(Result::is_ok);
        assert_eq!(won.len(), 1, "one claim commits");
        let winner = dir.latest().await.run;
        for refused in lost {
            assert!(
                matches!(&refused, Err(LockError::Held { run, .. }) if *run == winner),
                "a losing claim is refused naming the winner: {refused:?}"
            );
        }
    }

    /// A holder whose lease another run took over learns it at its next heartbeat, and
    /// from then on [`RunLock::check`] fails, which stops a backfill before its next
    /// write. The clock is paused, so the heartbeat comes at once.
    #[tokio::test(start_paused = true)]
    async fn a_holder_learns_at_its_next_heartbeat_that_it_lost_the_lease() {
        let dir = Dir::new();
        let holder = dir.acquire("holder").await.expect("the lease is free");
        let held = dir.latest().await;
        dir.change(Lease {
            run: "taker".to_owned(),
            sequence: held.sequence + 1,
            heartbeat: unix_now(),
            ..held
        })
        .await;
        holder.check().expect("not known yet: no heartbeat has run");
        tokio::time::sleep(super::HEARTBEAT).await;
        // The renewal writes to disk on other threads; give it real time to finish.
        for _ in 0..5_000 {
            if holder.check().is_err() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
            tokio::task::yield_now().await;
        }
        assert!(
            matches!(holder.check(), Err(LockError::Lost)),
            "the heartbeat found the lease taken"
        );
        assert_eq!(dir.latest().await.run, "taker", "and left it to the taker");
    }

    /// A lease not renewed for [`STALE`] may be taken over, and its old holder finds its
    /// lease lost when it next writes to it.
    #[tokio::test]
    async fn a_stale_lease_is_taken_over_and_its_holder_learns_it() {
        let dir = Dir::new();
        let holder = dir.acquire("holder").await.expect("the lease is free");
        let held = dir.latest().await;
        dir.change(Lease {
            sequence: held.sequence + 1,
            heartbeat: unix_now() - i64::try_from(STALE.as_secs()).expect("fits") - 1,
            ..held
        })
        .await;
        let taker = dir.acquire("taker").await.expect("a stale lease is free");
        assert!(matches!(holder.release().await, Err(LockError::Lost)));
        assert_eq!(dir.latest().await.run, "taker");
        taker.release().await.expect("released");
    }
}
