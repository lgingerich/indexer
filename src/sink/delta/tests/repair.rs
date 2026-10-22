//! Opening the lake: the repair of a commit that stopped part-way, a write that
//! hangs past its deadline, and what opening checks and hands on.

use deltalake::kernel::{DataType, StructField};
use deltalake::logstore::object_store::local::LocalFileSystem;
use deltalake::logstore::object_store::path::Path as ObjectPath;
use deltalake::logstore::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectResult,
};
use deltalake::{DeltaTableBuilder, ensure_table_uri};
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;

use super::*;
use crate::sink::Operation;
use crate::sink::delta::open::STATS_COLUMNS;

/// A commit that stopped after recording a retraction but before taking its orphans out
/// of the ledger leaves a record of a retraction that did not happen. Opening the lake
/// deletes it, and the restart, still on the orphaned branch, records it once.
#[tokio::test]
async fn opening_deletes_a_retraction_the_ledger_never_saw() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=3 {
        publish(&mut sink, block(height, 0)).await;
    }
    append_only(&mut sink, &reorg(3..=3, 1)).await;
    drop(sink);

    let mut sink = open(&dir).await;
    assert!(retractions(&sink).await.is_empty());
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 0)]));

    let mut replacement = vec![reorg(3..=3, 1)];
    replacement.extend(block(3, 1));
    publish(&mut sink, replacement).await;
    drop(sink);

    let sink = open(&dir).await;
    assert_eq!(retractions(&sink).await, expected(&[(3, 1)]));
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 1)]));
}

/// A commit that stopped after taking the orphans out of the ledger has already recorded
/// the retraction. Opening the lake keeps the record and deletes the orphans' rows, and
/// the restart, resuming below the fork, has nothing to retract again.
#[tokio::test]
async fn a_retraction_out_of_the_ledger_keeps_its_record() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=3 {
        publish(&mut sink, block(height, 0)).await;
    }
    append_only(&mut sink, &reorg(3..=3, 1)).await;
    delete_only(&mut sink, Table::AcceptedBlock, "hash", hash(3, 0)).await;
    drop(sink);

    let mut sink = open(&dir).await;
    assert_eq!(retractions(&sink).await, expected(&[(3, 1)]));
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0)]));
    assert_eq!(ledger_heights(&mut sink).await, [1, 2]);
}

/// A commit that stopped after writing rows but before their ledger entry leaves rows the
/// ledger does not name — above its tip, or at a height it holds another block for.
/// Opening the lake deletes them, so a resumed run writes them once.
#[tokio::test]
async fn opening_repairs_rows_a_stopped_commit_left_behind() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=3 {
        publish(&mut sink, block(height, 0)).await;
    }
    // A stopped commit: rows of a fork at 3 and of 4, without their ledger entries.
    let schema = Schema::new().expect("the dataset tables");
    let chain = ChainId::new(CHAIN);
    let rows: Vec<_> = block(3, 1)
        .into_iter()
        .chain(block(4, 0))
        .filter(|envelope| !matches!(envelope.event, Event::AcceptedBlock(_)))
        .map(|envelope| schema.row(&chain, &envelope.event).expect("a row"))
        .collect();
    for table in [Table::Block, Table::Transaction, Table::Log] {
        let position = sink.positions[&TableId::Dataset(table)];
        let rows: Vec<_> = rows
            .iter()
            .filter(|row| row.table().id == TableId::Dataset(table))
            .collect();
        sink.append_some(position, &rows).await.expect("appended");
    }
    drop(sink);

    let mut sink = open(&dir).await;
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 0)]));
    assert_eq!(ledger_heights(&mut sink).await, [1, 2, 3]);
}

/// With no ledger at all, no block row counts: the first commit stopped before its
/// ledger entry, and the run starts over.
#[tokio::test]
async fn opening_with_no_ledger_keeps_no_block_rows() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    let schema = Schema::new().expect("the dataset tables");
    let chain = ChainId::new(CHAIN);
    let position = sink.positions[&TableId::Dataset(Table::Block)];
    let row = schema
        .row(&chain, &block(1, 0)[0].event)
        .expect("a block row");
    sink.append_some(position, &[&row]).await.expect("appended");
    drop(sink);

    let mut sink = open(&dir).await;
    assert_eq!(rows(&sink, Table::Block).await, 0);
    assert!(ledger(&mut sink).await.is_empty());
}

/// A commit that fails part-way refuses every later write rather than reporting the
/// lost batch as stored, and the next open repairs what it left behind.
#[tokio::test]
async fn a_failed_commit_refuses_every_later_write() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    publish(&mut sink, block(1, 0)).await;
    // A timestamp too large for the lake's microseconds fails the commit part-way.
    let mut broken = block(2, 0);
    for envelope in &mut broken {
        match &mut envelope.event {
            Event::Block(block) => block.timestamp = u64::MAX,
            Event::Transaction(transaction) => transaction.block_timestamp = u64::MAX,
            Event::Log(log) => log.block_timestamp = u64::MAX,
            _ => {}
        }
    }
    buffer(&mut sink, broken).await;
    let error = sink.flush().await.expect_err("the commit fails");
    assert!(
        matches!(
            &error,
            SinkError::Store(StoreError::Engine {
                operation: Operation::Append,
                target: Some(table),
                ..
            }) if table == "blocks"
        ),
        "{error:?}"
    );

    assert!(matches!(
        sink.flush().await,
        Err(SinkError::Store(StoreError::Failed))
    ));
    assert!(matches!(
        sink.publish(block(2, 0).remove(0)).await,
        Err(SinkError::Store(StoreError::Failed))
    ));
    drop(sink);

    let mut sink = open(&dir).await;
    assert_eq!(blocks(&sink).await, expected(&[(1, 0)]));
    assert_eq!(ledger_heights(&mut sink).await, [1]);
}

/// The local filesystem, except that no write ever completes: an object store whose
/// requests hang without failing.
#[derive(Debug)]
struct Hanging(LocalFileSystem);

impl std::fmt::Display for Hanging {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hanging({})", self.0)
    }
}

// `ObjectStore` is declared with `async_trait`, whose signatures are spelled out here
// rather than taking the macro on as a dependency for one fake.
impl ObjectStore for Hanging {
    fn put_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _: &'life1 ObjectPath,
        _: PutPayload,
        _: PutOptions,
    ) -> BoxFuture<'async_trait, ObjectResult<PutResult>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(std::future::pending())
    }

    fn put_multipart_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _: &'life1 ObjectPath,
        _: PutMultipartOptions,
    ) -> BoxFuture<'async_trait, ObjectResult<Box<dyn MultipartUpload>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(std::future::pending())
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
        self.0.delete_stream(locations)
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
        _: &'life1 ObjectPath,
        _: &'life2 ObjectPath,
        _: CopyOptions,
    ) -> BoxFuture<'async_trait, ObjectResult<()>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(std::future::pending())
    }
}

/// Makes every write to `table` of the lake in `dir` hang (see [`Hanging`]), while
/// reads still go through.
async fn hang(dir: &Dir, sink: &mut DeltaSink, table: Table) {
    let store = Arc::new(Hanging(LocalFileSystem::new()));
    let position = sink.positions[&TableId::Dataset(table)];
    let url = ensure_table_uri(dir.table(&sink.lakes[position].def.name)).expect("a table url");
    let mut hanging = DeltaTableBuilder::from_url(url.clone())
        .expect("a table builder")
        .with_storage_backend(store, url)
        .build()
        .expect("the table builds");
    hanging.load().await.expect("the table loads");
    sink.lakes[position].table = hanging;
}

/// A write that hangs without failing fails the store at its deadline rather than
/// stalling it, and the next open repairs what the cut commits left behind: here the
/// ledger's append hangs, so the commit after it is cut while it waits.
#[tokio::test]
async fn a_write_past_its_deadline_fails_the_store_and_the_reopen_repairs_it() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    publish(&mut sink, block(1, 0)).await;
    hang(&dir, &mut sink, Table::AcceptedBlock).await;
    // Its rows commit, and its ledger append starts and never lands.
    buffer(&mut sink, block(2, 0)).await;
    sink.flush()
        .await
        .expect("the commit before the ledger hangs");

    sink.deadline = std::time::Duration::from_millis(500);
    buffer(&mut sink, block(3, 0)).await;
    let error = sink
        .flush()
        .await
        .expect_err("the commit misses its deadline");
    assert!(
        matches!(
            error,
            SinkError::Store(StoreError::Stalled { after }) if after == sink.deadline
        ),
        "{error:?}"
    );
    assert!(matches!(
        sink.publish(block(4, 0).remove(0)).await,
        Err(SinkError::Store(StoreError::Failed))
    ));
    drop(sink);

    let mut sink = open(&dir).await;
    assert_eq!(blocks(&sink).await, expected(&[(1, 0)]));
    assert_eq!(ledger_heights(&mut sink).await, [1]);
}

/// A commit that stopped part-way through deleting a retraction's orphans — out of the
/// ledger and one table, not yet the others — is finished by the next open.
#[tokio::test]
async fn opening_finishes_a_retraction_stopped_between_tables() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=3 {
        publish(&mut sink, block(height, 0)).await;
    }
    append_only(&mut sink, &reorg(3..=3, 1)).await;
    for table in [Table::AcceptedBlock, Table::Block] {
        delete_only(&mut sink, table, "hash", hash(3, 0)).await;
    }
    drop(sink);

    let mut sink = open(&dir).await;
    assert_eq!(retractions(&sink).await, expected(&[(3, 1)]));
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0)]));
    assert_eq!(ledger_heights(&mut sink).await, [1, 2]);
}

/// A table already at the lake's location that does not match its definition stops the
/// open, rather than the first write.
#[tokio::test]
async fn opening_over_a_different_table_is_drift() {
    let dir = Dir::new();
    let schema = Schema::new().expect("the dataset tables");
    let name = schema.dataset(Table::Block).name.clone();
    let url = ensure_table_uri(dir.table(&name)).expect("a table url");
    DeltaTable::try_from_url(url)
        .await
        .expect("a location")
        .create()
        .with_columns([StructField::new("hash", DataType::STRING, false)])
        .await
        .expect("a stranger's table");

    let error = DeltaSink::open(&dir.settings(), Arc::new(schema), CHAIN)
        .await
        .expect_err("drift stops the open");
    assert!(
        matches!(&error, StoreError::Drift { table, difference }
            if *table == name && difference == "it has no number column"),
        "{error:?}"
    );
}

/// A contract created in block `height` on `branch`.
fn contract(height: u64, branch: u8) -> Envelope {
    Envelope::new(
        ChainId::new(CHAIN),
        Event::Contract(Box::new(crate::wire::datasets::evm::Contract {
            protocol: "uniswap_v3".to_owned(),
            name: "UniswapV3Pool".to_owned(),
            address: alloy_primitives::Address::from([branch.wrapping_add(0xd0); 20]),
            factory_address: alloy_primitives::Address::from([0xfa; 20]),
            transaction_hash: hash(height, branch.wrapping_add(100)),
            transaction_index: 0,
            log_index: 1,
            block_number: height,
            block_hash: hash(height, branch),
            block_timestamp: 1_700_000_000 + height,
        })),
    )
}

/// The first restore after opening hands on what opening read, and it matches what a
/// later read of the lake finds; once a commit lands, a restore reads it.
#[tokio::test]
async fn a_restore_after_opening_hands_on_what_opening_read() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    let mut first = block(1, 0);
    first.insert(1, contract(1, 0));
    publish(&mut sink, first).await;
    publish(&mut sink, block(2, 0)).await;
    drop(sink);

    let mut sink = open(&dir).await;
    let handed_on = sink.restore().await.expect("the lake restores");
    let read = sink.restore().await.expect("the lake restores again");
    assert_eq!(handed_on.ledger, read.ledger);
    assert_eq!(handed_on.contracts, read.contracts);
    assert_eq!(
        handed_on
            .contracts
            .iter()
            .map(|contract| contract.block_hash)
            .collect::<Vec<_>>(),
        [hash(1, 0)]
    );
    assert_eq!(
        handed_on
            .ledger
            .iter()
            .map(|block| block.height)
            .collect::<Vec<_>>(),
        [1, 2]
    );

    publish(&mut sink, block(3, 0)).await;
    assert_eq!(
        ledger_heights(&mut sink).await,
        [1, 2, 3],
        "a commit is read, not handed on"
    );
}

/// A contract a stopped commit left behind is repaired away before opening hands the
/// contracts on: they are read again when the repair deletes some of them.
#[tokio::test]
async fn opening_hands_on_contracts_as_repaired() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    let mut first = block(1, 0);
    first.insert(1, contract(1, 0));
    publish(&mut sink, first).await;
    // Block 2's contract, appended without its ledger entry, as a stopped commit would.
    append_only(&mut sink, &contract(2, 0)).await;
    drop(sink);

    let mut sink = open(&dir).await;
    let restored = sink.restore().await.expect("the lake restores");
    assert_eq!(
        restored
            .contracts
            .iter()
            .map(|contract| contract.block_hash)
            .collect::<Vec<_>>(),
        [hash(1, 0)],
        "the stray contract is gone"
    );
}

/// A lake written at one width is refused by a run at another, which names the way out,
/// and a new lake records its width so the next open is checked against it.
#[tokio::test]
async fn a_lake_at_another_width_is_refused() {
    let dir = Dir::new();
    drop(
        open_at(&dir, 50_000)
            .await
            .expect("a new lake opens at any width"),
    );

    let error = open_at(&dir, 100_000)
        .await
        .expect_err("the width is the lake's");
    assert!(
        matches!(&error, StoreError::Drift { difference, .. }
            if difference.contains("50000 heights wide, not 100000")),
        "{error:?}"
    );
    open_at(&dir, 50_000)
        .await
        .expect("the lake's own width opens");
}

/// A partition narrower than the ledger window is refused: the newest two would not
/// always hold all of it.
#[test]
fn a_partition_narrower_than_the_ledger_window_is_refused() {
    let settings = |width: u64| {
        toml::from_str::<DeltaSettings>(&format!(
            "tables = [\"blocks\"]\nuri = \"/tmp/lake\"\npartition_blocks = {width}"
        ))
    };
    let window = crate::ingest::pipeline::LEDGER_WINDOW as u64;
    let error = settings(window - 1).expect_err("too narrow");
    assert!(error.to_string().contains("at least 4097"), "{error}");
    assert_eq!(
        settings(window).expect("wide enough").partition_blocks,
        window
    );
    assert_eq!(
        toml::from_str::<DeltaSettings>("tables = [\"blocks\"]\nuri = \"/tmp/lake\"")
            .expect("parses")
            .partition_blocks,
        100_000
    );
}

/// The columns each of `table`'s files records a minimum of, as its log entry holds it.
fn stats_kept(sink: &DeltaSink, table: Table) -> Vec<BTreeSet<String>> {
    sink.lake(table)
        .table
        .snapshot()
        .expect("a snapshot")
        .snapshot()
        .try_log_data()
        .expect("the file list")
        .iter()
        .map(|file| {
            let stats: serde_json::Value =
                serde_json::from_str(&file.stats().expect("the file's statistics"))
                    .expect("statistics are JSON");
            stats["minValues"]
                .as_object()
                .expect("minimums")
                .iter()
                .filter(|(_, minimum)| !minimum.is_null())
                .map(|(column, _)| column.clone())
                .collect()
        })
        .collect()
}

/// Makes `table` in `dir` keep statistics on `columns` alone, as a table written
/// before the store chose its own did.
async fn keep_stats_on(dir: &Dir, table: Table, columns: &str) {
    let schema = Schema::new().expect("the dataset tables");
    let url = ensure_table_uri(dir.table(&schema.dataset(table).name)).expect("a table url");
    deltalake::open_table(url)
        .await
        .expect("the table")
        .set_tbl_properties()
        .with_properties([(STATS_COLUMNS.to_owned(), columns.to_owned())].into())
        .await
        .expect("set");
}

/// Each table's files keep statistics on its block height and timestamps, wherever in
/// the table they fall: `transactions` holds them past its 32nd column, Delta's default
/// reach.
#[tokio::test]
async fn every_file_keeps_statistics_on_its_height_and_timestamps() {
    assert_eq!(
        STATS_COLUMNS,
        deltalake::TableProperty::DataSkippingStatsColumns.as_ref()
    );
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    publish(&mut sink, block(1, 0)).await;
    publish(&mut sink, block(2, 0)).await;
    let mut replaced = vec![reorg(2..=2, 1)];
    replaced.extend(block(2, 1));
    publish(&mut sink, replaced).await;
    let set = |columns: &[&str]| -> BTreeSet<String> {
        columns.iter().map(|column| (*column).to_owned()).collect()
    };
    for (table, columns) in [
        (Table::Block, set(&["number", "timestamp"])),
        (
            Table::Transaction,
            set(&["block_number", "block_timestamp"]),
        ),
        (Table::Log, set(&["block_number", "block_timestamp"])),
        (Table::AcceptedBlock, set(&["height", "timestamp"])),
        (Table::Reorg, set(&["height"])),
    ] {
        let kept = stats_kept(&sink, table);
        assert!(!kept.is_empty(), "{table:?} has files");
        assert!(
            kept.iter().all(|kept| *kept == columns),
            "{table:?}: {kept:?}"
        );
    }
}

/// A table that names other statistics columns, as one written before the store chose
/// its own does, is given the store's at open, and the files written from then on keep
/// them.
#[tokio::test]
async fn opening_an_older_table_gives_it_the_statistics_columns() {
    let dir = Dir::new();
    drop(open(&dir).await);
    keep_stats_on(&dir, Table::Transaction, "hash").await;
    let mut sink = open(&dir).await;
    publish(&mut sink, block(1, 0)).await;
    assert_eq!(
        stats_kept(&sink, Table::Transaction),
        [["block_number", "block_timestamp"]
            .map(str::to_owned)
            .into()]
    );
}
