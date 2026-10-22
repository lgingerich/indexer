//! Maintenance: compaction, vacuum, log cleanup, and checkpoints.

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
    let short = 100_000 + crate::ingest::pipeline::MAX_UNFINALIZED_BLOCKS as u64 - 1;
    assert_eq!(
        maintenance::compact("blocks", &mut table, width, short, &writer)
            .await
            .expect("compacts"),
        0
    );
    let compacted = maintenance::compact("blocks", &mut table, width, short + 1, &writer)
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
    let vacuumed = maintenance::vacuum("blocks", &mut table, std::time::Duration::ZERO)
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
    let cleaned = maintenance::clean_log("blocks", &mut table, std::time::Duration::ZERO)
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
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while reopened(&sink, position).is_none() {
        assert!(
            std::time::Instant::now() < deadline,
            "the checkpoint was not read back"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
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
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
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
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}
