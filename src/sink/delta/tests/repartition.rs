//! **Temporary**: the tests of the one-off rewrite to another partition width, deleted
//! with [`repartition`](super::super::repartition).

use std::collections::BTreeSet;
use std::sync::Arc;

use deltalake::ensure_table_uri;

use super::super::{lake_writer, repartition};
use super::{
    CHAIN, Dir, block, blocks, files_by_partition, keep_stats_on, ledger_heights, open, open_at,
    publish, rows, stats_kept,
};
use crate::sink::StoreError;
use crate::sink::table::{Schema, Table};

/// Rewrites the lake in `dir` to `width` heights a partition, with no quiet period: the
/// test's own writer has just stopped.
async fn rewrite(dir: &Dir, width: u64) {
    let schema = Schema::new().expect("the dataset tables");
    repartition::rewrite_lake(
        &dir.settings_at(width),
        &schema,
        CHAIN,
        std::time::Duration::ZERO,
    )
    .await
    .expect("the lake is rewritten");
}

/// The heights a repartition test commits: across the old partitions 0, 1, and 2, and
/// the new 0 to 4.
const SPREAD: [u64; 6] = [1, 99_999, 100_000, 150_000, 199_999, 200_001];

/// Writes [`SPREAD`] to a fresh lake, 100,000 heights a partition, and returns the
/// blocks it holds.
async fn spread(dir: &Dir) -> BTreeSet<String> {
    let mut sink = open(dir).await;
    for height in SPREAD {
        publish(&mut sink, block(height, 0)).await;
    }
    blocks(&sink).await
}

/// A rewrite moves every row into the partition its height falls in at the new width,
/// keeps every block, and leaves a lake only the new width opens, whose ledger reads
/// from its newest two partitions as before. A second rewrite finds nothing to do.
#[tokio::test]
async fn a_rewrite_moves_every_row_to_the_new_width() {
    let dir = Dir::new();
    let held = spread(&dir).await;

    rewrite(&dir, 50_000).await;
    let mut sink = open_at(&dir, 50_000).await.expect("the new width opens");
    assert_eq!(blocks(&sink).await, held, "every block is kept");
    for table in [
        Table::Block,
        Table::Transaction,
        Table::Log,
        Table::AcceptedBlock,
    ] {
        assert_eq!(
            rows(&sink, table).await,
            SPREAD.len(),
            "{table:?} has no duplicates"
        );
    }
    assert_eq!(
        files_by_partition(&sink, Table::Block)
            .into_keys()
            .collect::<Vec<_>>(),
        ["0", "1", "2", "3", "4"]
    );
    assert_eq!(ledger_heights(&mut sink).await, [150_000, 199_999, 200_001]);
    let version = sink.lake(Table::Block).table.version();
    publish(&mut sink, block(250_000, 0)).await;
    assert_eq!(
        files_by_partition(&sink, Table::Block).get("5"),
        Some(&1),
        "the writer carries on at the new width"
    );
    drop(sink);

    let error = open_at(&dir, 100_000)
        .await
        .expect_err("the old width is gone");
    assert!(matches!(error, StoreError::Drift { .. }), "{error:?}");

    rewrite(&dir, 50_000).await;
    let sink = open_at(&dir, 50_000).await.expect("still opens");
    assert_eq!(
        sink.lake(Table::Block).table.version(),
        version.map(|version| version + 1),
        "the second rewrite committed nothing"
    );
}

/// A rewrite that stopped part-way is refused by the store at either width, and the
/// next rewrite carries on from the partition after the last it replaced, rewriting
/// none twice.
#[tokio::test]
async fn a_rewrite_that_stopped_carries_on() {
    let dir = Dir::new();
    let held = spread(&dir).await;
    // Stopped after the blocks' newest old partition, before the rest.
    let schema = Schema::new().expect("the dataset tables");
    let def = Arc::clone(schema.dataset(Table::Block));
    let url = ensure_table_uri(dir.table(&def.name)).expect("a table url");
    let mut table = deltalake::open_table(url).await.expect("the blocks");
    repartition::start(&mut table, &def.name, 50_000)
        .await
        .expect("started");
    let writer = lake_writer();
    repartition::rewrite_partition(&mut table, &def, 2, 50_000, &writer)
        .await
        .expect("one partition rewritten");

    for width in [100_000, 50_000] {
        let error = open_at(&dir, width).await.expect_err("half done");
        assert!(
            matches!(&error, StoreError::Drift { difference, .. }
                if difference.contains("recorded as `rewriting from 100000 to 50000`")),
            "{error:?}"
        );
    }

    rewrite(&dir, 50_000).await;
    let sink = open_at(&dir, 50_000).await.expect("finished");
    assert_eq!(blocks(&sink).await, held);
    assert_eq!(rows(&sink, Table::Block).await, SPREAD.len());
    assert_eq!(
        files_by_partition(&sink, Table::Block),
        [("0", 1), ("1", 1), ("2", 1), ("3", 1), ("4", 1)]
            .map(|(partition, files)| (partition.to_owned(), files))
            .into(),
    );
}

/// A rewrite to another width than the one under way is refused, and so is one of a
/// table its writer committed to moments ago.
#[tokio::test]
async fn a_rewrite_refuses_a_live_or_different_one() {
    let dir = Dir::new();
    spread(&dir).await;
    let schema = Schema::new().expect("the dataset tables");

    let error = repartition(&dir.settings_at(50_000), &schema, CHAIN)
        .await
        .expect_err("just written");
    assert!(
        matches!(&error, StoreError::Drift { difference, .. } if difference.contains("stop its writer")),
        "{error:?}"
    );

    let def = schema.dataset(Table::Block);
    let url = ensure_table_uri(dir.table(&def.name)).expect("a table url");
    let mut table = deltalake::open_table(url).await.expect("the blocks");
    repartition::start(&mut table, &def.name, 50_000)
        .await
        .expect("started");
    let error = repartition::rewrite_lake(
        &dir.settings_at(25_000),
        &schema,
        CHAIN,
        std::time::Duration::ZERO,
    )
    .await
    .expect_err("another rewrite is under way");
    assert!(
        matches!(&error, StoreError::Drift { difference, .. } if difference.contains("to 50000")),
        "{error:?}"
    );
}

/// A rewrite of a table that names other statistics columns gives it the store's before
/// it writes a file, so every file it writes keeps them: how a lake written before they
/// were chosen gets them throughout.
#[tokio::test]
async fn a_rewrite_gives_every_file_the_statistics_columns() {
    let dir = Dir::new();
    spread(&dir).await;
    keep_stats_on(&dir, Table::Transaction, "hash").await;

    rewrite(&dir, 50_000).await;
    let sink = open_at(&dir, 50_000).await.expect("the new width opens");
    let kept = stats_kept(&sink, Table::Transaction);
    assert_eq!(kept.len(), 5);
    let wanted: BTreeSet<String> = ["block_number", "block_timestamp"]
        .map(str::to_owned)
        .into();
    assert!(kept.iter().all(|kept| *kept == wanted), "{kept:?}");
}
