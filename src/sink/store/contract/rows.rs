//! The contract tests of what a store writes: every column and value of a row, what a
//! flush makes durable and what a failed one keeps, and which tables a store fills.

// The crate denies `expect`/`unwrap` to keep production paths honest; tests are allowed
// them per the repository test style, since a failed expectation there means the fixture
// or setup is wrong.
#![expect(clippy::expect_used)]

use super::*;

/// A wei price above `u64::MAX` survives the round trip as an exact number, which is the
/// whole reason those columns are not 64-bit.
pub(crate) async fn a_wei_price_above_u64_max_is_not_truncated<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    commit(&mut sink, every_kind(&harness.chain())).await;

    let read = harness
        .query(
            "SELECT CAST(gas_price AS VARCHAR), CAST(gas_price + gas_price AS VARCHAR) \
             FROM transactions",
        )
        .await;
    let expected = u128::from(u64::MAX) + 1;
    assert_eq!(text(&read[0][0]), expected.to_string());
    assert_eq!(
        text(&read[0][1]),
        (expected * 2).to_string(),
        "it adds exactly"
    );
    harness.remove().await;
}

/// A block's wide row is written whole, in the order the generated DDL declares. The
/// header is the schema, so this checks the two agree end to end.
pub(crate) async fn a_wide_block_row_writes_every_column<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    commit(&mut sink, every_kind(&harness.chain())).await;

    let declared = texts(
        &mut harness,
        "SELECT CAST(column_name AS VARCHAR) FROM information_schema.columns \
         WHERE table_schema = current_schema() AND table_name = 'blocks' \
         ORDER BY ordinal_position",
    )
    .await;
    let header: Vec<String> = datasets()
        .dataset(Table::Block)
        .columns
        .iter()
        .map(|column| column.name.to_string())
        .collect();
    assert_eq!(declared, header, "the DDL and the header must agree");

    let row = harness
        .query(
            "SELECT number, hash, parent_hash, timestamp, gas_limit, gas_used, \
             transaction_count FROM blocks",
        )
        .await
        .remove(0);
    assert_eq!(uint(&row[0]), 100);
    assert_eq!(text(&row[1]), format!("{:#x}", hash(0x01)));
    assert_eq!(text(&row[2]), format!("{:#x}", hash(0x02)));
    assert_eq!(row[3], Value::Timestamp(1_700_000_000));
    assert_eq!(uint(&row[4]), 30_000_000);
    assert_eq!(uint(&row[5]), 21_000);
    assert_eq!(uint(&row[6]), 1);
    harness.remove().await;
}

/// An absent optional field is SQL `NULL`, not an empty string and not a zero.
/// `withdrawals_root` is `None` on a pre-merge block, so a consumer filtering
/// `WHERE withdrawals_root IS NOT NULL` has to get exactly those blocks. The fixture block
/// leaves every optional field absent, so every nullable column is checked.
pub(crate) async fn an_absent_optional_column_is_null<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    commit(&mut sink, every_kind(&harness.chain())).await;

    let schema = datasets();
    let nullable: Vec<&str> = schema
        .dataset(Table::Block)
        .columns
        .iter()
        .filter(|column| column.nullable)
        .map(|column| column.name.as_ref())
        .collect();
    assert!(nullable.contains(&"withdrawals_root"), "{nullable:?}");
    for name in nullable {
        let present = harness
            .query(&format!(
                "SELECT count(*) FROM blocks WHERE \"{name}\" IS NOT NULL"
            ))
            .await;
        assert_eq!(
            present[0][0],
            Value::Int(0),
            "the fixture block has no {name}"
        );
    }
    harness.remove().await;
}

/// Nothing is durable before the flush, in any table, and everything buffered is written
/// and gone from the buffer after it.
pub(crate) async fn nothing_is_durable_until_the_flush<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let envelopes = every_kind(&harness.chain());
    let buffered = envelopes.len();
    buffer(&mut sink, envelopes).await;
    assert_eq!(sink.buffered(), buffered, "buffered but not written");
    assert!(sink.buffered_bytes() > 0, "the drain sees what is buffered");
    for table in Table::ALL {
        assert_eq!(
            row_count(&mut harness, table.name()).await,
            0,
            "{table} before the flush"
        );
    }

    sink.flush().await.expect("batch flushes");
    assert_eq!(sink.buffered(), 0, "the buffers are cleared");
    assert_eq!(sink.buffered_bytes(), 0);
    for table in Table::ALL {
        assert_eq!(
            row_count(&mut harness, table.name()).await,
            one_unless_decoded(table),
            "{table} after the flush"
        );
    }
    harness.remove().await;
}

/// A table failing late in a flush rolls back the tables written before it and keeps the
/// batch, and the retry writes every row once.
pub(crate) async fn a_late_table_failure_rolls_back_and_a_retry_writes_each_row_once<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let envelopes = every_kind(&harness.chain());
    let buffered = envelopes.len();
    buffer(&mut sink, envelopes).await;
    harness.execute("DROP TABLE reorgs").await;

    assert_failed(sink.flush().await, Operation::Merge, "reorgs");
    assert_eq!(sink.buffered(), buffered, "failed batch stays buffered");
    for (table, rows) in block_tables(&mut harness).await {
        assert_eq!(rows, 0, "{table} rolls back");
    }

    harness.execute(&reorg_ddl::<H>()).await;
    sink.flush()
        .await
        .expect("retry commits the original batch");
    sink.flush().await.expect("empty flush is a no-op");
    assert_eq!(sink.buffered(), 0);
    for table in Table::ALL {
        assert_eq!(
            row_count(&mut harness, table.name()).await,
            one_unless_decoded(table),
            "{table} after the retry"
        );
    }
    harness.remove().await;
}

/// A constraint the engine checks only at the merge fails the flush there, keeps the
/// batch, and leaves the table empty; the retry writes the whole batch.
pub(crate) async fn a_deferred_constraint_failure_keeps_the_batch_and_rolls_back<H: Harness>() {
    let (mut sink, mut harness) = H::open(datasets()).await;
    let height = format!("\"height\" {}", Sql::<H>::type_name(ColumnType::Uint));
    let constrained = reorg_ddl::<H>().replace(&height, &format!("{height} CHECK (height < 1)"));
    harness
        .execute(&format!("DROP TABLE reorgs; {constrained}"))
        .await;
    let chain = harness.chain();
    buffer(
        &mut sink,
        (0..2).map(|height| {
            Envelope::new(
                chain.clone(),
                Event::Reorg(Reorg {
                    height,
                    new_head_hash: hash(u8::try_from(height).expect("heights 0 and 1")),
                    orphaned_hashes: vec![],
                }),
            )
        }),
    )
    .await;

    assert_failed(sink.flush().await, Operation::Merge, "reorgs");
    assert_eq!(row_count(&mut harness, "reorgs").await, 0);
    assert_eq!(sink.buffered(), 2);

    harness
        .execute(&format!("DROP TABLE reorgs; {}", reorg_ddl::<H>()))
        .await;
    sink.flush().await.expect("retry the whole batch");
    assert_eq!(
        uints(&mut harness, "SELECT height FROM reorgs ORDER BY height").await,
        [0, 1]
    );
    assert_eq!(sink.buffered(), 0);
    harness.remove().await;
}

/// A decoded record lands in `decoded_logs` and in its event's own table, where every
/// argument is a typed column: wide integers exact and signed, so they sum without a cast.
/// A reorg deletes the typed row with the rest of its block.
pub(crate) async fn a_decoded_record_lands_in_its_typed_event_table<H: Harness>() {
    let (mut sink, mut harness) = H::open(crate::sink::fixtures::schema()).await;
    let chain = harness.chain();
    let swap = crate::sink::fixtures::decoded_swap();
    let block = swap.block_hash;
    commit(
        &mut sink,
        [Envelope::new(chain.clone(), Event::Decoded(Box::new(swap)))],
    )
    .await;

    assert_eq!(row_count(&mut harness, "decoded_logs").await, 1);
    assert_eq!(row_count(&mut harness, "uniswap_v3_pool_swap").await, 1);
    let row = harness
        .query(
            "SELECT CAST(amount0 AS VARCHAR), CAST(amount1 AS VARCHAR), \
             CAST(sqrt_price_x96 AS VARCHAR), CAST(liquidity AS VARCHAR), tick, sender \
             FROM uniswap_v3_pool_swap",
        )
        .await
        .remove(0);
    assert_eq!(text(&row[0]), "-3180585820646654");
    assert_eq!(text(&row[1]), "8586564");
    assert_eq!(text(&row[2]), "4115542941155561242646778");
    assert_eq!(text(&row[3]), "1367693693161775005");
    assert_eq!(row[4], Value::Int(-197_317));
    assert_eq!(text(&row[5]), "0x6ff5693b99212da76ad316178a184ab56d299b43");
    assert_eq!(
        texts(
            &mut harness,
            "SELECT CAST(sum(sqrt_price_x96) + sum(liquidity) AS VARCHAR) \
             FROM uniswap_v3_pool_swap"
        )
        .await,
        ["4115544308849254404421783"],
        "exact, not a float"
    );

    commit(
        &mut sink,
        [Envelope::new(
            chain,
            Event::Reorg(Reorg {
                height: 1,
                new_head_hash: hash(0x56),
                orphaned_hashes: vec![block],
            }),
        )],
    )
    .await;
    assert_eq!(row_count(&mut harness, "uniswap_v3_pool_swap").await, 0);
    assert_eq!(row_count(&mut harness, "decoded_logs").await, 0);
    harness.remove().await;
}

/// A store routed only the event tables creates and fills those and the bookkeeping
/// tables: a decoded record's `decoded_logs` row, and a raw log, are not kept.
pub(crate) async fn a_store_writes_only_the_tables_routed_to_it<H: Harness>() {
    let schema = crate::sink::fixtures::schema();
    let (events, mut expected): (HashSet<TableId>, BTreeSet<String>) = schema
        .tables()
        .iter()
        .filter(|def| matches!(def.id, TableId::Event(_)))
        .map(|def| (def.id, def.name.clone()))
        .unzip();
    expected.extend(["accepted_blocks", "contracts", "reorgs"].map(str::to_owned));
    let (mut sink, mut harness) = H::open(Arc::new(schema.select(&events))).await;
    let chain = harness.chain();
    let swap = crate::sink::fixtures::decoded_swap();
    let log = Log {
        block_number: swap.block_number,
        block_hash: swap.block_hash,
        ..Log::default()
    };
    commit(
        &mut sink,
        [
            Envelope::new(chain.clone(), Event::Decoded(Box::new(swap))),
            Envelope::new(chain, Event::Log(Box::new(log))),
        ],
    )
    .await;

    let created: BTreeSet<String> = texts(
        &mut harness,
        "SELECT CAST(table_name AS VARCHAR) FROM information_schema.tables \
         WHERE table_schema = current_schema()",
    )
    .await
    .into_iter()
    .collect();
    assert_eq!(created, expected, "only the routed and bookkeeping tables");
    assert_eq!(row_count(&mut harness, "uniswap_v3_pool_swap").await, 1);
    for table in ["accepted_blocks", "reorgs", "contracts"] {
        assert_eq!(row_count(&mut harness, table).await, 0, "{table} is empty");
    }
    harness.remove().await;
}
