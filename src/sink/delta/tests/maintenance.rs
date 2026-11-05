//! Maintenance: compaction, vacuum, log cleanup, and checkpoints.

use std::time::{Duration, SystemTime};

use deltalake::logstore::LogStoreRef;
use deltalake::logstore::object_store::local::LocalFileSystem;
use deltalake::logstore::object_store::path::Path as ObjectPath;
use deltalake::logstore::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectResult,
};
use deltalake::{DeltaTableBuilder, ObjectStoreError, ensure_table_uri};
use futures_util::StreamExt as _;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;

use super::*;
use crate::sink::delta::arrow::PARTITION;
use crate::sink::delta::lake::CHECKPOINT_EVERY;
use crate::sink::delta::maintenance;

/// The files `table` holds in each partition, by partition.
fn files_by_partition(sink: &DeltaSink, table: Table) -> std::collections::BTreeMap<String, usize> {
    let lake = sink.lake(table);
    let mut files = std::collections::BTreeMap::new();
    for file in lake
        .table
        .snapshot()
        .expect("a snapshot")
        .snapshot()
        .try_log_data()
        .expect("the file list")
        .iter()
    {
        let partition = file
            .partition_values_map()
            .remove(PARTITION)
            .flatten()
            .expect("a partition");
        *files.entry(partition).or_default() += 1;
    }
    files
}

/// A partition the tip has left behind by more than the reorg window is compacted into
/// one file, with every row kept; the partitions a reorg can still reach are not
/// touched. Vacuum then deletes the files compaction replaced.
#[tokio::test]
async fn maintenance_compacts_finished_partitions_and_vacuums_what_they_replaced() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in [1, 2, 3, 100_001, 100_002] {
        publish(&mut sink, block(height, 0)).await;
    }
    let before = files_by_partition(&sink, Table::Block);
    assert_eq!(before, [("0".to_owned(), 3), ("1".to_owned(), 2)].into());

    let position = sink.positions[&TableId::Dataset(Table::Block)];
    let mut table = sink.lakes[position].table.clone();
    let width = sink.lakes[position].width;
    let writer = sink.writer.clone();
    // Partition 0 ends at 99_999: one height short of the window past it, nothing moves.
    let short = 100_000 + crate::window::MAX_UNFINALIZED_BLOCKS as u64 - 1;
    assert_eq!(
        maintenance::compact(
            "blocks",
            &mut table,
            width,
            short,
            crate::window::UndoWindow::DEFAULT,
            &writer
        )
        .await
        .expect("compacts"),
        0
    );
    let compacted = maintenance::compact(
        "blocks",
        &mut table,
        width,
        short + 1,
        crate::window::UndoWindow::DEFAULT,
        &writer,
    )
    .await
    .expect("compacts");
    assert_eq!(compacted, 1);
    sink.lakes[position].table = table.clone();
    assert_eq!(
        files_by_partition(&sink, Table::Block),
        [("0".to_owned(), 1), ("1".to_owned(), 2)].into()
    );
    let held = [(1, 0), (2, 0), (3, 0), (100_001, 0), (100_002, 0)];
    assert_eq!(stored(&sink, Table::Block, "hash").await, expected(&held));

    let on_disk = |partition: &str| {
        std::fs::read_dir(dir.table(&format!("blocks/{PARTITION}={partition}")))
            .expect("the partition directory")
            .filter(|entry| {
                entry
                    .as_ref()
                    .is_ok_and(|entry| entry.path().extension().is_some_and(|ext| ext == "parquet"))
            })
            .count()
    };
    assert_eq!(on_disk("0"), 4, "the replaced files stay until a vacuum");
    let vacuumed = maintenance::vacuum("blocks", &mut table, Duration::ZERO)
        .await
        .expect("vacuums");
    assert_eq!(vacuumed, 3);
    assert_eq!(on_disk("0"), 1);
    assert_eq!(on_disk("1"), 2);
}

/// Compaction is a second writer: the tip writer, holding a snapshot from before it,
/// still appends and deletes in the newest partition, and nothing is lost on either side.
#[tokio::test]
async fn the_writer_commits_past_a_compaction_it_did_not_see() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in [1, 2, 104_096, 104_097] {
        publish(&mut sink, block(height, 0)).await;
    }
    let position = sink.positions[&TableId::Dataset(Table::Block)];
    let mut behind = sink.lakes[position].table.clone();
    let writer = sink.writer.clone();
    maintenance::compact(
        "blocks",
        &mut behind,
        sink.lakes[position].width,
        104_097,
        crate::window::UndoWindow::DEFAULT,
        &writer,
    )
    .await
    .expect("compacts");

    let mut replacement = vec![reorg(104_097..=104_097, 1)];
    replacement.extend(block(104_097, 1));
    replacement.extend(block(104_098, 0));
    publish(&mut sink, replacement).await;
    drop(sink);

    let sink = open(&dir).await;
    assert_eq!(
        blocks(&sink).await,
        expected(&[(1, 0), (2, 0), (104_096, 0), (104_097, 1), (104_098, 0)])
    );
    assert_eq!(files_by_partition(&sink, Table::Block)["0"], 1);
}

/// Log cleanup deletes the transaction log's entries a checkpoint replaces, and keeps
/// the checkpoint and every entry after it, so the table still opens with every row.
#[tokio::test]
async fn log_cleanup_keeps_what_a_checkpoint_does_not_replace() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in [1, 2, 3] {
        publish(&mut sink, block(height, 0)).await;
    }
    let position = sink.positions[&TableId::Dataset(Table::Block)];
    deltalake::checkpoints::create_checkpoint(&sink.lakes[position].table, None)
        .await
        .expect("a checkpoint");
    let checkpoint = sink.lakes[position].table.version().expect("a version");
    publish(&mut sink, block(4, 0)).await;

    let entries = || {
        let mut versions: Vec<u64> = std::fs::read_dir(dir.table("blocks/_delta_log"))
            .expect("the log directory")
            .filter_map(|entry| {
                let name = entry.ok()?.file_name().into_string().ok()?;
                name.strip_suffix(".json")?.parse().ok()
            })
            .collect();
        versions.sort_unstable();
        versions
    };
    assert_eq!(entries(), (0..=checkpoint + 1).collect::<Vec<_>>());

    let mut table = sink.lakes[position].table.clone();
    let cleaned = maintenance::clean_log("blocks", &mut table, Duration::ZERO)
        .await
        .expect("cleans");
    assert_eq!(
        cleaned,
        usize::try_from(checkpoint).expect("a small version")
    );
    assert_eq!(entries(), vec![checkpoint, checkpoint + 1]);
    drop(sink);

    let sink = open(&dir).await;
    assert_eq!(
        blocks(&sink).await,
        expected(&[(1, 0), (2, 0), (3, 0), (4, 0)])
    );
}

/// A table committed to less often than the checkpoint interval in each run is still
/// checkpointed once the interval has passed across runs: the count starts at its
/// newest checkpoint, not at the version a run opened it at.
#[tokio::test]
async fn the_checkpoint_interval_counts_across_reopens() {
    let dir = Dir::new();
    let log = log_of(&dir, "accepted_blocks");
    let half = CHECKPOINT_EVERY / 2 + 1;
    let mut sink = open(&dir).await;
    accept(&mut sink, 1..=half).await;
    drop(sink);
    assert!(!log.join("_last_checkpoint").exists(), "checkpointed early");
    let mut sink = open(&dir).await;
    accept(&mut sink, half + 1..=CHECKPOINT_EVERY).await;
    assert_eq!(
        wait_for_checkpoint(&log).await,
        CHECKPOINT_EVERY,
        "the version that reached the interval"
    );
}

/// Opening starts the checkpoint a table is due, so one a stop interrupted, or a table
/// no commit reaches, is not left to read its whole log on every open.
#[tokio::test]
async fn opening_starts_a_checkpoint_that_is_due() {
    let dir = Dir::new();
    let log = log_of(&dir, "accepted_blocks");
    let mut sink = open(&dir).await;
    accept(&mut sink, 1..=CHECKPOINT_EVERY + 1).await;
    assert_eq!(wait_for_checkpoint(&log).await, CHECKPOINT_EVERY);
    drop(sink);
    // As if the process stopped before the checkpoint was written.
    for entry in std::fs::read_dir(&log).expect("the log").flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.contains(".checkpoint.") || name == "_last_checkpoint" {
            std::fs::remove_file(entry.path()).expect("removed");
        }
    }
    let _sink = open(&dir).await;
    assert_eq!(
        wait_for_checkpoint(&log).await,
        CHECKPOINT_EVERY + 1,
        "a checkpoint of the table as it was opened"
    );
}

/// Once a checkpoint is written it is read back as the table as of it, and the next
/// append takes that table up, keeping every entry before and after, each once. Whether
/// the append carried on from it or, its update failing, from the table it replaces,
/// the lake reads the same.
#[tokio::test]
async fn an_append_after_a_checkpoint_carries_on_from_it() {
    let dir = Dir::new();
    let log = log_of(&dir, "accepted_blocks");
    let mut sink = open(&dir).await;
    let last = CHECKPOINT_EVERY + 1;
    accept(&mut sink, 1..=last).await;
    assert_eq!(wait_for_checkpoint(&log).await, CHECKPOINT_EVERY);
    let position = sink.position(Table::AcceptedBlock);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while reopened(&sink, position).is_none() {
        assert!(
            std::time::Instant::now() < deadline,
            "the checkpoint was not read back"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        reopened(&sink, position),
        Some(CHECKPOINT_EVERY),
        "the table as of the checkpoint"
    );

    accept(&mut sink, last + 1..=last + 2).await;
    assert_eq!(reopened(&sink, position), None, "taken by the next append");
    assert_eq!(
        ledger_heights(&mut sink).await,
        (1..=last + 2).collect::<Vec<_>>()
    );
}

/// The version of the table a checkpoint left for the next append of the table at
/// `position`, if one is waiting.
fn reopened(sink: &DeltaSink, position: usize) -> Option<u64> {
    sink.lakes[position]
        .reopened
        .lock()
        .expect("unpoisoned")
        .as_ref()
        .and_then(DeltaTable::version)
}

/// Commits the ledger entries of the canonical blocks at `heights`, one commit each and
/// nothing else, so each takes the ledger one version on and no other table: all a
/// checkpoint counts, at a fraction of a whole block's commit.
async fn accept(sink: &mut DeltaSink, heights: std::ops::RangeInclusive<u64>) {
    for height in heights {
        let entry = block(height, 0).pop().expect("the ledger entry");
        publish(sink, vec![entry]).await;
    }
}

/// The transaction log of `table` in the lake at `dir`.
fn log_of(dir: &Dir, table: &str) -> PathBuf {
    PathBuf::from(dir.table(table)).join("_delta_log")
}

/// Waits for the log at `log` to name a checkpoint, and returns the version it names:
/// `_last_checkpoint` is written once the checkpoint itself is. The checkpoint runs on
/// a task the sink does not hand out, so this polls; the deadline only bounds a
/// failure, and is far past how long a checkpoint of a small table takes.
async fn wait_for_checkpoint(log: &std::path::Path) -> u64 {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let hint = log.join("_last_checkpoint");
    loop {
        // Read whole or not at all: a hint half-written reads as no hint yet.
        if let Some(version) = std::fs::read(&hint)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .and_then(|hint| hint["version"].as_u64())
        {
            return version;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no checkpoint was written"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A vacuum is due a table that never recorded one, or recorded one a day ago or more;
/// not one recorded since, nor one a store clock ahead of this one recorded.
#[test]
fn a_vacuum_is_due_a_day_after_the_last_that_succeeded() {
    let now = SystemTime::now();
    assert!(maintenance::due(None, now));
    assert!(!maintenance::due(Some(now - Duration::from_hours(23)), now));
    assert!(maintenance::due(Some(now - Duration::from_hours(24)), now));
    assert!(!maintenance::due(Some(now + Duration::from_hours(1)), now));
}

/// A pass records a table's vacuum on the table itself, and only once the vacuum and
/// the log cleanup succeed: one that failed is due again on the next pass, and one that
/// succeeded is not due again until a day later by the record, however often the
/// process restarts in between.
#[tokio::test]
async fn a_vacuum_is_recorded_on_its_table_only_once_it_succeeds() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in [1, 2, 3, 100_001, 100_002] {
        publish(&mut sink, block(height, 0)).await;
    }
    let position = sink.positions[&TableId::Dataset(Table::Block)];
    let width = sink.lakes[position].width;
    let log_store = sink.lakes[position].table.log_store();
    let writer = sink.writer.clone();
    // Partition 0 is finished, so compaction replaces its files, and a vacuum with no
    // retention deletes the files it replaced.
    let tip = 100_000 + crate::window::MAX_UNFINALIZED_BLOCKS as u64;
    let record = PathBuf::from(dir.table("blocks")).join(maintenance::VACUUMED);
    let due = async |log_store| {
        maintenance::vacuum_due(log_store)
            .await
            .expect("the record reads")
    };

    let undeletable = undeletable(&dir, "blocks").await;
    maintenance::maintain(
        "blocks",
        &undeletable,
        width,
        tip,
        crate::window::UndoWindow::DEFAULT,
        Duration::ZERO,
        &writer,
    )
    .await;
    assert!(!record.exists(), "a failed vacuum is not recorded");
    assert!(due(&log_store).await, "and is due again");

    maintenance::maintain(
        "blocks",
        &log_store,
        width,
        tip,
        crate::window::UndoWindow::DEFAULT,
        Duration::ZERO,
        &writer,
    )
    .await;
    assert!(record.exists(), "a vacuum that succeeded is recorded");
    assert!(!due(&log_store).await, "and not due again the same day");
    drop(sink);

    let sink = open(&dir).await;
    let log_store = sink.lakes[position].table.log_store();
    assert!(!due(&log_store).await, "a restart does not make it due");
    std::fs::File::options()
        .write(true)
        .open(&record)
        .expect("the record")
        .set_modified(SystemTime::now() - Duration::from_hours(25))
        .expect("backdated");
    assert!(due(&log_store).await, "a day after the record, it is due");
}

/// The log store of `table` in the lake at `dir`, through [`Undeletable`].
async fn undeletable(dir: &Dir, table: &str) -> LogStoreRef {
    let store = Arc::new(Undeletable(LocalFileSystem::new()));
    let url = ensure_table_uri(dir.table(table)).expect("a table url");
    let mut undeletable = DeltaTableBuilder::from_url(url.clone())
        .expect("a table builder")
        .with_storage_backend(store, url)
        .build()
        .expect("the table builds");
    undeletable.load().await.expect("the table loads");
    undeletable.log_store()
}

/// The local filesystem, except that every delete fails, as a vacuum's do on a store
/// that refuses them.
#[derive(Debug)]
struct Undeletable(LocalFileSystem);

impl std::fmt::Display for Undeletable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Undeletable({})", self.0)
    }
}

// `ObjectStore` is declared with `async_trait`, whose signatures are spelled out here
// rather than taking the macro on as a dependency for one fake.
impl ObjectStore for Undeletable {
    fn put_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> BoxFuture<'async_trait, ObjectResult<PutResult>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.0.put_opts(location, payload, options)
    }

    fn put_multipart_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjectPath,
        options: PutMultipartOptions,
    ) -> BoxFuture<'async_trait, ObjectResult<Box<dyn MultipartUpload>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.0.put_multipart_opts(location, options)
    }

    fn get_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjectPath,
        options: GetOptions,
    ) -> BoxFuture<'async_trait, ObjectResult<GetResult>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.0.get_opts(location, options)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectResult<ObjectPath>>,
    ) -> BoxStream<'static, ObjectResult<ObjectPath>> {
        locations
            .map(|_| {
                Err(ObjectStoreError::Generic {
                    store: "Undeletable",
                    source: "deletes are refused".into(),
                })
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, ObjectResult<ObjectMeta>> {
        self.0.list(prefix)
    }

    fn list_with_delimiter<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prefix: Option<&'life1 ObjectPath>,
    ) -> BoxFuture<'async_trait, ObjectResult<ListResult>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.0.list_with_delimiter(prefix)
    }

    fn copy_opts<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 ObjectPath,
        to: &'life2 ObjectPath,
        options: CopyOptions,
    ) -> BoxFuture<'async_trait, ObjectResult<()>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        self.0.copy_opts(from, to, options)
    }
}

/// A narrower undo window finishes a partition sooner: past its last height by that
/// window, where the default still leaves it alone.
#[tokio::test]
async fn a_narrower_window_compacts_a_partition_sooner() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in [1, 2, 3, 100_001] {
        publish(&mut sink, block(height, 0)).await;
    }
    let position = sink.positions[&TableId::Dataset(Table::Block)];
    let mut table = sink.lakes[position].table.clone();
    let width = sink.lakes[position].width;
    let writer = sink.writer.clone();
    let narrow = crate::window::UndoWindow::new(512).expect("in range");
    let tip = 100_000 + 512;
    let compact = async |table: &mut _, window| {
        maintenance::compact("blocks", table, width, tip, window, &writer)
            .await
            .expect("compacts")
    };
    assert_eq!(
        compact(&mut table, crate::window::UndoWindow::DEFAULT).await,
        0
    );
    assert_eq!(compact(&mut table, narrow).await, 1);
}
