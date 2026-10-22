//! The contract tests of retracting and reading back: a replay and a reorg, a rewind, the
//! ledger window a restart resumes from, and the contracts it restores.

// The crate denies `expect`/`unwrap` to keep production paths honest; tests are allowed
// them per the repository test style, since a failed expectation there means the fixture
// or setup is wrong.
#![expect(clippy::expect_used)]

use super::*;

/// A batch that holds a key twice keeps its last copy, a later flush of the same key
/// updates the stored row rather than appending a second, and a reorg replaces the
/// orphaned branch's row with the replacement's.
pub(crate) async fn a_replay_updates_the_row_and_a_reorg_replaces_the_branch<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let chain = harness.chain();
    let orphaned = Log {
        log_index: 0,
        transaction_hash: TxHash::from([0x11; 32]),
        block_number: 100,
        block_hash: hash(0xaa),
        block_timestamp: 1,
        ..Log::default()
    };
    let log = |log: &Log, block_timestamp: u64| {
        let mut log = log.clone();
        log.block_timestamp = block_timestamp;
        Envelope::new(chain.clone(), Event::Log(Box::new(log)))
    };
    commit(&mut sink, [log(&orphaned, 1), log(&orphaned, 5)]).await;
    assert_eq!(
        row_count(&mut harness, "logs").await,
        1,
        "a repeated key in one batch is one row"
    );
    assert_eq!(
        log_timestamp(&mut harness).await,
        5,
        "the later copy in the batch wins"
    );

    // The same key in a later flush updates the stored row, not appends a second — the
    // merge's `DO UPDATE` branch, which a first flush into an empty table misses.
    commit(&mut sink, [log(&orphaned, 7)]).await;
    assert_eq!(
        row_count(&mut harness, "logs").await,
        1,
        "a replay never appends a second"
    );
    assert_eq!(
        log_timestamp(&mut harness).await,
        7,
        "the replay's value replaces the stored one"
    );

    // The reorg deletes the stored branch; the replacement is its own key.
    let mut replacement = orphaned;
    replacement.block_hash = hash(0xbb);
    commit(&mut sink, [reorg(&chain, &[0xaa]), log(&replacement, 9)]).await;
    assert_eq!(
        texts(&mut harness, "SELECT block_hash FROM logs").await,
        [format!("{:#x}", hash(0xbb))],
        "only the replacement remains"
    );
    assert_eq!(log_timestamp(&mut harness).await, 9);
    harness.remove().await;
}

/// A reorg deletes a committed block's rows from every table that belongs to a block, the
/// ledger included, and keeps itself as the record of the retraction.
pub(crate) async fn a_reorg_deletes_a_committed_block_from_every_table<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let chain = harness.chain();
    commit(&mut sink, every_kind(&chain)).await;

    commit(&mut sink, [reorg(&chain, &[0x01])]).await;

    for (table, rows) in block_tables(&mut harness).await {
        assert_eq!(rows, 0, "{table} keeps nothing of the orphaned block");
    }
    assert_eq!(
        texts(
            &mut harness,
            "SELECT new_head_hash FROM reorgs ORDER BY new_head_hash"
        )
        .await,
        [format!("{:#x}", hash(0x55)), format!("{:#x}", hash(0x56))],
        "the fixture marker and this one"
    );
    harness.remove().await;
}

/// An orphaned block still in the buffer is dropped before it is ever written.
pub(crate) async fn a_reorg_drops_an_orphaned_block_from_the_same_batch<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let chain = harness.chain();
    buffer(&mut sink, every_kind(&chain)).await;
    sink.publish(reorg(&chain, &[0x01]))
        .await
        .expect("reorg buffers");
    assert_eq!(
        sink.buffered(),
        2,
        "only the two reorg markers stay buffered"
    );
    sink.flush().await.expect("batch commits");

    for (table, rows) in block_tables(&mut harness).await {
        assert_eq!(rows, 0, "{table} never sees the orphaned block");
    }
    assert_eq!(row_count(&mut harness, "reorgs").await, 2);
    harness.remove().await;
}

/// A block orphaned and then canonical again is stored, whether its return arrives in a
/// later batch or the same one: the deletes run before the rows are written.
pub(crate) async fn a_block_that_returns_after_a_reorg_is_stored<H: Harness>() {
    let cases: [(&str, fn(&ChainId) -> Vec<Vec<Envelope>>); 2] = [
        ("across batches", |chain| {
            vec![
                vec![block_log(chain, 0xaa)],
                vec![reorg(chain, &[0xaa]), block_log(chain, 0xbb)],
                vec![reorg(chain, &[0xbb]), block_log(chain, 0xaa)],
            ]
        }),
        (
            "within one batch, with the block already committed",
            |chain| {
                vec![
                    vec![block_log(chain, 0xaa)],
                    vec![
                        reorg(chain, &[0xaa]),
                        block_log(chain, 0xbb),
                        reorg(chain, &[0xbb]),
                        block_log(chain, 0xaa),
                    ],
                ]
            },
        ),
    ];
    for (case, batches) in cases {
        let (mut sink, mut harness) = H::open(datasets()).await;
        for batch in batches(&harness.chain()) {
            commit(&mut sink, batch).await;
        }
        assert_eq!(
            texts(
                &mut harness,
                "SELECT block_hash FROM logs ORDER BY block_hash"
            )
            .await,
            [format!("{:#x}", hash(0xaa))],
            "{case}"
        );
        harness.remove().await;
    }
}

/// The one log of block `block` at height 100.
fn block_log(chain: &ChainId, block: u8) -> Envelope {
    Envelope::new(
        chain.clone(),
        Event::Log(Box::new(Log {
            log_index: 0,
            transaction_hash: TxHash::from([0x11; 32]),
            block_number: 100,
            block_hash: hash(block),
            ..Log::default()
        })),
    )
}

/// A failed commit keeps the reorg's deletions with its rows, and the retry applies them.
pub(crate) async fn a_failed_reorg_commit_keeps_its_deletes_for_the_retry<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let chain = harness.chain();
    commit(&mut sink, every_kind(&chain)).await;
    sink.publish(reorg(&chain, &[0x01]))
        .await
        .expect("reorg buffers");
    harness.execute("DROP TABLE reorgs").await;
    assert_failed(sink.flush().await, Operation::Merge, "reorgs");
    for (table, rows) in block_tables(&mut harness).await {
        assert_eq!(
            rows,
            one_unless_decoded(table),
            "{table}'s delete rolled back"
        );
    }

    harness.execute(&reorg_ddl::<H>()).await;
    sink.flush().await.expect("the retry commits");
    for (table, rows) in block_tables(&mut harness).await {
        assert_eq!(rows, 0, "{table} is retracted on the retry");
    }
    assert_eq!(
        texts(&mut harness, "SELECT new_head_hash FROM reorgs").await,
        [format!("{:#x}", hash(0x56))],
        "the retry records the retraction"
    );
    harness.remove().await;
}

/// A rewind deletes every block above the height from every table, the ledger included,
/// and leaves the blocks at or below it.
pub(crate) async fn a_rewind_deletes_every_block_above_the_height<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let chain = harness.chain();
    for height in 1..=5 {
        let meta = accepted(height);
        for event in [
            Event::Block(Box::new(Block {
                number: height,
                hash: meta.hash,
                ..Block::default()
            })),
            Event::AcceptedBlock(meta),
        ] {
            sink.publish(Envelope::new(chain.clone(), event))
                .await
                .expect("buffer");
        }
    }
    sink.flush().await.expect("flush");

    sink.rewind(3).await.expect("rewind");

    for (table, height) in [("blocks", "number"), ("accepted_blocks", "height")] {
        assert_eq!(
            uints(
                &mut harness,
                &format!("SELECT {height} FROM {table} ORDER BY {height}")
            )
            .await,
            [1, 2, 3],
            "{table}"
        );
    }
    harness.remove().await;
}

/// A store more than the ledger's window ahead, the only kind that is rewound, has pruned
/// the ledger rows of the blocks just above the height; the rewind deletes those blocks'
/// rows all the same.
pub(crate) async fn a_rewind_deletes_blocks_whose_ledger_rows_were_pruned<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let chain = harness.chain();
    let tip = BlockMeta {
        height: LEDGER_WINDOW as u64 + 10,
        hash: B256::repeat_byte(0xee),
        parent_hash: B256::repeat_byte(0xed),
        timestamp: 1_700_000_000,
    };
    for meta in (1..=5).map(accepted).chain([tip]) {
        commit(
            &mut sink,
            [
                Event::Block(Box::new(Block {
                    number: meta.height,
                    hash: meta.hash,
                    ..Block::default()
                })),
                Event::AcceptedBlock(meta),
            ]
            .map(|event| Envelope::new(chain.clone(), event)),
        )
        .await;
    }
    assert_eq!(
        uints(&mut harness, "SELECT height FROM accepted_blocks").await,
        [tip.height],
        "the flush at the tip pruned the ledger below its window"
    );

    sink.rewind(3).await.expect("rewind");

    assert_eq!(
        uints(&mut harness, "SELECT number FROM blocks ORDER BY number").await,
        [1, 2, 3],
        "no block above the height is left"
    );
    harness.remove().await;
}

/// The ledger reads back the newest unorphaned accepted blocks, oldest first, and deletes
/// nothing.
pub(crate) async fn the_ledger_reads_back_the_newest_canonical_window<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let chain = harness.chain();
    assert!(sink.ledger(3).await.expect("empty").is_empty());
    let mut envelopes: Vec<Envelope> = (1..=10)
        .map(|height| Envelope::new(chain.clone(), Event::AcceptedBlock(accepted(height))))
        .collect();
    // Block 10 was orphaned and deleted, so the tip falls back to 9.
    envelopes.push(Envelope::new(
        chain.clone(),
        Event::Reorg(Reorg {
            height: 10,
            new_head_hash: hash(0xee),
            orphaned_hashes: vec![hash(10)],
        }),
    ));
    commit(&mut sink, envelopes).await;

    let ledger = sink.ledger(3).await.expect("read back");
    assert_eq!(ledger, [accepted(7), accepted(8), accepted(9)]);
    assert_eq!(
        uints(
            &mut harness,
            "SELECT height FROM accepted_blocks ORDER BY height"
        )
        .await,
        [1, 2, 3, 4, 5, 6, 7, 8, 9],
        "only the orphaned row was deleted"
    );
    harness.remove().await;
}

/// A flush deletes the ledger rows more than the window below the newest block it
/// accepts, and leaves the window itself.
pub(crate) async fn a_flush_prunes_the_ledger_below_the_window<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let chain = harness.chain();
    let block = |height: u64| BlockMeta {
        height,
        hash: B256::from(U256::from(height)),
        parent_hash: B256::from(U256::from(height - 1)),
        timestamp: 1_700_000_000 + height,
    };
    commit(
        &mut sink,
        (1..=3).map(|height| Envelope::new(chain.clone(), Event::AcceptedBlock(block(height)))),
    )
    .await;
    let tip = LEDGER_WINDOW as u64 + 2;
    commit(
        &mut sink,
        [Envelope::new(
            chain.clone(),
            Event::AcceptedBlock(block(tip)),
        )],
    )
    .await;

    assert_eq!(
        uints(
            &mut harness,
            "SELECT height FROM accepted_blocks ORDER BY height"
        )
        .await,
        [2, 3, tip],
        "the row more than the window below the tip was deleted"
    );
    harness.remove().await;
}

/// A stored contract reads back, and is deleted once a reorg orphans its creating block.
pub(crate) async fn stored_contracts_read_back_and_orphaned_ones_are_deleted<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let chain = harness.chain();
    let contract = contract();
    commit(
        &mut sink,
        [Envelope::new(
            chain.clone(),
            Event::Contract(Box::new(contract.clone())),
        )],
    )
    .await;
    assert_eq!(
        sink.contracts().await.expect("read back"),
        [StoredContract {
            protocol: contract.protocol,
            name: contract.name,
            address: contract.address,
            block_hash: contract.block_hash,
            block_number: contract.block_number,
            log_index: contract.log_index,
        }]
    );

    commit(&mut sink, [reorg(&chain, &[0x01])]).await;
    assert!(sink.contracts().await.expect("read back").is_empty());
    assert_eq!(row_count(&mut harness, "contracts").await, 0);
    harness.remove().await;
}
