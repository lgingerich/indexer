//! Commits and reorgs: what the lake holds after blocks are written, retracted,
//! rewound, or replayed.

use deltalake::arrow::util::display::{ArrayFormatter, FormatOptions};
use deltalake::datafusion::prelude::ident;

use super::*;
use crate::sink::delta::{Phase, ROWS_PER_BATCH};
use crate::sink::fixtures;

/// Committed blocks survive a reopen, the ledger comes back oldest first, and wide
/// integers come back exactly.
#[tokio::test]
async fn committed_blocks_survive_a_reopen() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=12 {
        publish(&mut sink, block(height, 0)).await;
    }
    drop(sink);

    let mut sink = open(&dir).await;
    assert_eq!(
        ledger_heights(&mut sink).await,
        (1..=12).collect::<Vec<_>>()
    );
    let heights: Vec<(u64, u8)> = (1..=12).map(|height| (height, 0)).collect();
    assert_eq!(blocks(&sink).await, expected(&heights));
    assert_eq!(
        stored(&sink, Table::Transaction, "value").await,
        BTreeSet::from([U256::MAX.to_string()])
    );
    assert_eq!(
        stored(&sink, Table::Transaction, "gas_price").await,
        BTreeSet::from([(u128::from(u64::MAX) + 1).to_string()])
    );
}

/// Blocks a reorg orphans while still buffered are dropped before they are written,
/// and the retraction is recorded.
#[tokio::test]
async fn a_reorg_of_buffered_blocks_never_reaches_the_lake() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=3 {
        buffer(&mut sink, block(height, 0)).await;
    }
    buffer(&mut sink, vec![reorg(3..=3, 1)]).await;
    publish(&mut sink, block(3, 1)).await;

    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 1)]));
    assert_eq!(retractions(&sink).await, expected(&[(3, 1)]));
}

/// Committed blocks a reorg orphans are deleted from every table, the ledger included,
/// and their replacements take their place.
#[tokio::test]
async fn a_reorg_of_committed_blocks_deletes_them_everywhere() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=12 {
        publish(&mut sink, block(height, 0)).await;
    }
    let mut replacements = vec![reorg(10..=12, 1)];
    for height in 10..=12 {
        replacements.extend(block(height, 1));
    }
    publish(&mut sink, replacements).await;

    let canonical: Vec<(u64, u8)> = (1..=9)
        .map(|height| (height, 0))
        .chain((10..=12).map(|height| (height, 1)))
        .collect();
    assert_eq!(blocks(&sink).await, expected(&canonical));
    drop(sink);

    let mut sink = open(&dir).await;
    assert_eq!(
        ledger(&mut sink)
            .await
            .iter()
            .map(|block| (block.height, block.hash))
            .collect::<Vec<_>>(),
        canonical
            .iter()
            .map(|&(height, branch)| (height, hash(height, branch)))
            .collect::<Vec<_>>(),
        "the ledger names the replacements in the orphans' place"
    );
    assert_eq!(blocks(&sink).await, expected(&canonical));
    assert_eq!(retractions(&sink).await, expected(&[(12, 1)]));
}

/// A batch that accepts a committed block again, with no reorg orphaning it first, is
/// refused before anything is written: the lake appends, so it would hold the block
/// twice.
#[tokio::test]
async fn a_replayed_block_is_refused() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=3 {
        publish(&mut sink, block(height, 0)).await;
    }
    buffer(&mut sink, block(3, 0)).await;
    assert!(matches!(
        sink.flush().await,
        Err(SinkError::Store(StoreError::Replayed { block })) if block == hex(hash(3, 0))
    ));
    for table in [
        Table::Block,
        Table::Transaction,
        Table::Log,
        Table::AcceptedBlock,
    ] {
        assert_eq!(
            rows(&sink, table).await,
            3,
            "nothing of the replay was written to {table}"
        );
    }
}

/// A rewind deletes every block above its height from every table, keeps no record of
/// it, and lets the blocks it took be written again — on the same branch or another —
/// without counting as a replay.
#[tokio::test]
async fn a_rewind_takes_the_blocks_above_it_and_they_can_be_written_again() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=5 {
        publish(&mut sink, block(height, 0)).await;
    }
    sink.rewind(3).await.expect("rewound");
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 0)]));
    assert!(retractions(&sink).await.is_empty());

    publish(&mut sink, block(4, 0)).await;
    publish(&mut sink, block(5, 1)).await;
    drop(sink);
    let mut sink = open(&dir).await;
    assert_eq!(
        blocks(&sink).await,
        expected(&[(1, 0), (2, 0), (3, 0), (4, 0), (5, 1)])
    );
    assert_eq!(ledger_heights(&mut sink).await, [1, 2, 3, 4, 5]);
}

/// The lake reports what it buffers, so the drain can commit by size: it grows by each
/// block's rows, a reorg takes back exactly the rows of the buffered blocks it drops
/// and adds its own, and it is empty after a commit. The first block also buffers the
/// run's session, so blocks are measured from the second.
#[tokio::test]
async fn the_lake_reports_what_it_buffers() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    assert_eq!(sink.buffered_bytes(), 0);
    buffer(&mut sink, block(1, 0)).await;
    let first = sink.buffered_bytes();
    assert!(first > 0);
    buffer(&mut sink, block(2, 0)).await;
    let one = sink.buffered_bytes() - first;
    assert!(one > 0);
    buffer(&mut sink, block(3, 0)).await;
    assert_eq!(
        sink.buffered_bytes(),
        first + 2 * one,
        "a third block of the same shape"
    );
    buffer(&mut sink, vec![reorg(3..=3, 1)]).await;
    let after_reorg = sink.buffered_bytes();
    sink.flush().await.expect("commits");
    assert_eq!(sink.buffered_bytes(), 0);

    // The reorg's own row, measured alone.
    buffer(&mut sink, vec![reorg(3..=3, 1)]).await;
    let marker = sink.buffered_bytes();
    assert!(marker > 0);
    assert_eq!(
        after_reorg,
        first + one + marker,
        "block 3's rows are dropped"
    );
}

/// A reorg that orphans committed blocks and buffered ones at once deletes the first and
/// drops the second, and only the replacements are stored.
#[tokio::test]
async fn a_reorg_across_committed_and_buffered_blocks_keeps_only_its_replacements() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=3 {
        publish(&mut sink, block(height, 0)).await;
    }
    for height in 4..=5 {
        buffer(&mut sink, block(height, 0)).await;
    }
    let mut replacements = vec![reorg(3..=5, 1)];
    for height in 3..=5 {
        replacements.extend(block(height, 1));
    }
    publish(&mut sink, replacements).await;

    let canonical = [(1, 0), (2, 0), (3, 1), (4, 1), (5, 1)];
    assert_eq!(blocks(&sink).await, expected(&canonical));
    drop(sink);
    let sink = open(&dir).await;
    assert_eq!(blocks(&sink).await, expected(&canonical));
}

/// A committed block orphaned and canonical again within one buffer ends up stored, and
/// a later reorg can still orphan it.
#[tokio::test]
async fn a_block_orphaned_and_canonical_again_in_one_buffer_is_stored() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=3 {
        publish(&mut sink, block(height, 0)).await;
    }
    let mut flips = vec![reorg(3..=3, 1)];
    flips.extend(block(3, 1));
    flips.push(Envelope::new(
        ChainId::new(CHAIN),
        Event::Reorg(Reorg {
            height: 3,
            new_head_hash: hash(3, 0),
            orphaned_hashes: vec![hash(3, 1)],
        }),
    ));
    flips.extend(block(3, 0));
    publish(&mut sink, flips).await;

    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 0)]));

    let mut replacement = vec![reorg(3..=3, 2)];
    replacement.extend(block(3, 2));
    publish(&mut sink, replacement).await;
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 2)]));
}

/// The ledger is read from its newest two partitions: the window across a partition
/// boundary comes back whole, and a block partitions below it is not read.
#[tokio::test]
async fn the_ledger_is_read_from_its_newest_partitions() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in [1, 199_999, 200_000, 200_001] {
        publish(&mut sink, block(height, 0)).await;
    }
    drop(sink);

    let mut sink = open(&dir).await;
    assert_eq!(ledger_heights(&mut sink).await, [199_999, 200_000, 200_001]);
}

/// A decoded record is stored twice — its generic row and its event's typed row — with
/// an array as a typed list, a `uint256` element as exact decimal text, and a tuple as
/// one JSON object.
#[tokio::test]
async fn a_decoded_record_fills_its_event_table_with_typed_lists() {
    let dir = Dir::new();
    let schema = fixtures::schema();
    let mut sink = DeltaSink::open(&dir.settings(), Arc::clone(&schema), CHAIN)
        .await
        .expect("the lake opens with the event tables");
    let created = fixtures::decoded_pool_created();
    let swap = fixtures::decoded_swap();
    let event = schema
        .event_row(&ChainId::new(CHAIN), &created)
        .expect("a row")
        .expect("an event table");
    let name = event.table().name.clone();
    publish(
        &mut sink,
        [created, swap]
            .into_iter()
            .map(|decoded| Envelope::new(ChainId::new(CHAIN), Event::Decoded(Box::new(decoded))))
            .collect(),
    )
    .await;

    assert_eq!(stored(&sink, Table::Decoded, "dedupe_key").await.len(), 2);
    let lake = &sink.lakes[sink.positions[&event.table().id]];
    assert_eq!(lake.name(), name);
    let columns = [
        "extensions",
        "negative_bin_data_array",
        "extension_orders",
        "price_provider_timelock",
    ];
    let batches = lake
        .read(|frame| frame.select(columns.map(ident)))
        .await
        .expect("the event table reads");
    let values: Vec<String> = (0..columns.len())
        .map(|index| {
            ArrayFormatter::try_new(batches[0].column(index).as_ref(), &FormatOptions::default())
                .expect("formats")
                .value(0)
                .to_string()
        })
        .collect();
    assert_eq!(
        values[0],
        "[0xb1a246b1131ff328067c4aaf4f772ff351475244, \
         0xe4038fa09f0bb9963068afaf97be0c045155090d, \
         0xebc53e61078976118e384f110c262a263decb84b]"
    );
    let word = "2510840694154681225832395181564014445733957084757624461722000";
    assert_eq!(
        values[1],
        format!("[{word}, {word}, {word}, {word}]"),
        "a packed word is its exact integer"
    );
    assert_eq!(
        values[2],
        r#"{"beforeAddLiquidity":"0","afterAddLiquidity":"0","beforeRemoveLiquidity":"0","afterRemoveLiquidity":"0","beforeSwap":"3","afterSwap":"10"}"#
    );
    assert_eq!(values[3], U256::MAX.to_string());
}

/// A table with more rows than one Arrow batch holds is built in several, on several
/// threads, and still commits every row once.
#[tokio::test]
async fn a_table_larger_than_one_batch_commits_every_row() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    let logs = ROWS_PER_BATCH * 2 + 7;
    let mut envelopes = block(1, 0);
    let ledger_entry = envelopes.pop().expect("the ledger entry");
    let transaction = hash(1, 100);
    for index in 1..logs {
        envelopes.push(Envelope::new(
            ChainId::new(CHAIN),
            Event::Log(Box::new(Log {
                log_index: u64::try_from(index).expect("an index"),
                transaction_hash: transaction,
                block_number: 1,
                block_hash: hash(1, 0),
                block_timestamp: 1_700_000_001,
                ..Log::default()
            })),
        ));
    }
    envelopes.push(ledger_entry);
    publish(&mut sink, envelopes).await;

    assert_eq!(rows(&sink, Table::Log).await, logs);
    drop(sink);
    let mut sink = open(&dir).await;
    assert_eq!(rows(&sink, Table::Log).await, logs);
    assert_eq!(
        stored(&sink, Table::Log, "dedupe_key").await.len(),
        logs,
        "each row once"
    );
    assert_eq!(ledger_heights(&mut sink).await, [1]);
}

/// A commit's ledger append runs on past its flush, beside the next commit's appends,
/// and a restore waits for it: every block committed is in the ledger it reads.
#[tokio::test]
async fn a_ledger_append_overlaps_the_next_commit() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    let opened = ledger_version(&sink);
    for height in 1..=3 {
        buffer(&mut sink, block(height, 0)).await;
        sink.flush().await.expect("flushed");
        assert!(
            sink.appending.is_some(),
            "{height}: the flush left its append running"
        );
        assert_eq!(
            ledger_version(&sink),
            opened + height - 1,
            "{height}: the flush took in the last commit's append, not its own"
        );
    }
    assert_eq!(ledger_heights(&mut sink).await, [1, 2, 3]);
    assert!(
        sink.appending.is_none(),
        "the restore waited for the append"
    );
    assert_eq!(ledger_version(&sink), opened + 3);
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 0)]));
}

/// A commit that fails part-way still waits for the last commit's ledger append before
/// it returns, so that commit, already reported as committed, is never undone by an
/// append left to be aborted.
#[tokio::test]
async fn a_failed_commit_waits_for_the_last_ledger_append() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    let opened = ledger_version(&sink);
    buffer(&mut sink, block(1, 0)).await;
    sink.flush().await.expect("flushed");
    assert!(sink.appending.is_some());
    buffer(&mut sink, broken(2)).await;
    sink.flush()
        .await
        .expect_err("the commit fails building its rows");
    assert!(matches!(sink.phase, Phase::Failed));
    assert!(sink.appending.is_none());
    assert_eq!(
        ledger_version(&sink),
        opened + 1,
        "the last append was waited for and taken in"
    );
    drop(sink);

    let mut sink = open(&dir).await;
    assert_eq!(blocks(&sink).await, expected(&[(1, 0)]));
    assert_eq!(ledger_heights(&mut sink).await, [1]);
}

/// The ledger's version as of the last append the sink has taken in.
fn ledger_version(sink: &DeltaSink) -> u64 {
    sink.lake(Table::AcceptedBlock)
        .table
        .version()
        .expect("a created table")
}

/// A run writes its session with the first block it commits. A reorg that orphans that
/// block deletes the row with the block's others, and the block replacing it writes the
/// session again; a later run, reopened, writes its own.
#[tokio::test]
async fn a_session_goes_with_its_block_and_each_run_writes_one() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    publish(&mut sink, block(1, 0)).await;
    publish(&mut sink, block(2, 0)).await;
    assert_eq!(
        stored(&sink, Table::Session, "block_hash").await,
        BTreeSet::from([hex(hash(1, 0))])
    );
    publish(&mut sink, vec![reorg(1..=2, 1)]).await;
    publish(&mut sink, block(1, 1)).await;
    assert_eq!(
        stored(&sink, Table::Session, "block_hash").await,
        BTreeSet::from([hex(hash(1, 1))]),
        "the orphaned block's session is gone, its replacement's written"
    );
    drop(sink);

    let mut sink = open(&dir).await;
    publish(&mut sink, block(2, 1)).await;
    assert_eq!(
        stored(&sink, Table::Session, "block_number").await.len(),
        2,
        "the reopened run writes its own session"
    );
}

/// A handed-over contract recorded in a committed block is restored whole when the
/// lake opens again.
#[tokio::test]
async fn a_handed_over_contract_is_restored_whole() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    let handoff = fixtures::handoff(1, hash(1, 0));
    let mut envelopes = block(1, 0);
    envelopes.push(Envelope::new(
        ChainId::new(CHAIN),
        Event::Handoff(Box::new(handoff.clone())),
    ));
    publish(&mut sink, envelopes).await;
    drop(sink);

    let restored = open(&dir).await.restore().await.expect("the lake restores");
    assert_eq!(restored.handoffs, [handoff]);
}

/// A backfill's read for more contracts than a scan filters by, as a chain with hundreds
/// of thousands of pools asks, returns the same logs as a read for the one among them
/// that emitted any, rather than overflowing the stack planning the filter.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_for_many_contracts_finds_their_logs() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=20 {
        publish(&mut sink, block(height, 0)).await;
    }
    drop(sink);
    let reader = crate::sink::delta::LogReader::open(
        &dir.settings(),
        &Schema::new().expect("the dataset tables"),
        CHAIN,
        false,
    )
    .await
    .expect("the reader opens");
    let emitter = alloy_primitives::Address::ZERO;
    let mut many: Vec<alloy_primitives::Address> = (1..=crate::sink::delta::replay::SCAN_ADDRESSES)
        .map(|n| {
            let mut bytes = [0xaa; 20];
            bytes[..8].copy_from_slice(&u64::try_from(n).expect("fits").to_be_bytes());
            alloy_primitives::Address::from(bytes)
        })
        .collect();
    many.push(emitter);
    let one = reader.read(1..=20, &[emitter], 2).await.expect("read");
    assert_eq!(one.len(), 20);
    assert_eq!(reader.read(1..=20, &many, 2).await.expect("read"), one);
}
