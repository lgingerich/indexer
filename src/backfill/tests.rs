//! A backfill against a local lake the running indexer's store wrote, as it would after
//! a deploy: blocks without the manifests, a restart with them, and blocks past the bound.

// Test code: a failed expectation means the fixture or setup is wrong.
#![expect(clippy::expect_used)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alloy_dyn_abi::DynSolValue;
use alloy_primitives::{Address, B256, U256, address, keccak256};
use deltalake::arrow::array::AsArray as _;
use deltalake::arrow::datatypes::Int64Type;
use deltalake::arrow::util::display::{ArrayFormatter, FormatOptions};
use deltalake::datafusion::prelude::SessionContext;

use super::handover::Coverage;
use super::write::{decode_all, parts, rows as rows_of};
use super::{BackfillError, BackfillSettings, COMMIT_BLOCKS, COMMIT_LOGS, backfill};
use crate::config::BackfillRun;
use crate::decode::{Catalog, Decoder, DecodingSink};
use crate::sink::SinkError;
use crate::sink::delta::testing::Dir;
use crate::sink::delta::{DecodedTables, DeltaSettings, DeltaSink, LockError, RunLock};
use crate::sink::resume::MAX_RESUME_LAG;
use crate::sink::table::{Schema, Session, SessionEvent};
use crate::sink::{EnvelopeSink as _, Store as _};
use crate::wire::datasets::evm::{Log, Transaction};
use crate::wire::envelope::{BlockMeta, ChainId, Envelope, Event, Reorg};

const CHAIN: &str = "base";

/// Uniswap V4's `PoolManager` on Base, as the shipped manifest lists it.
const POOL_MANAGER: Address = address!("0x498581fF718922c3f8e6A244956aF099B2652b2b");

/// The lake's first block: below a commit boundary, which the backfill in
/// [`a_backfill_fills_what_the_running_indexer_has_not_decoded`] crosses.
const FIRST: u64 = 89_000;

/// Where the running indexer restarts with the Uniswap V4 manifest.
const RESTART: u64 = FIRST + 2_000;

/// The lake's tip: far enough past the restart that the bound is above it.
const LAST: u64 = RESTART + MAX_RESUME_LAG + 1_000;

/// The bound a backfill of the full lake stops at.
const BOUND: u64 = LAST - MAX_RESUME_LAG;

impl Dir {
    /// This directory as the lake the running indexer's store opens.
    fn store(&self) -> DeltaSettings {
        DeltaSettings {
            tables: serde_json::from_str(r#"["*"]"#).expect("tables"),
            uri: self.uri(),
            commit_interval_secs: 30,
            max_buffer_bytes: usize::MAX,
            maintenance: false,
            vacuum_retention_hours: 168,
            partition_blocks: 100_000,
            storage: BTreeMap::new(),
        }
    }

    /// A backfill of `protocols` from this lake, with no bounds and no `out`.
    fn backfill(&self, protocols: &Path) -> BackfillSettings {
        BackfillSettings {
            chain: CHAIN.to_owned(),
            run: BackfillRun {
                protocols: protocols.to_path_buf(),
                from: None,
                to: None,
                only: None,
                out: None,
                hypersync: None,
            },
            decoded: protocols.to_path_buf(),
            lake: self.store(),
            #[cfg(feature = "postgres")]
            postgres: None,
        }
    }
}

fn protocols(path: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("protocols")
        .join(path)
}

fn uniswap_v4() -> PathBuf {
    protocols("uniswap/v4")
}

fn hash(height: u64, kind: u8) -> B256 {
    let mut bytes = [0; 32];
    bytes[0] = kind;
    bytes[24..].copy_from_slice(&height.to_be_bytes());
    B256::from(bytes)
}

/// A log of block `height`: a `PoolManager` `Donate`, or the same event from another
/// contract, which must not decode.
fn donate(height: u64, log_index: u64, from: Address) -> Log {
    Log {
        log_index,
        transaction_hash: hash(height, 1),
        address: from,
        topic0: Some(keccak256("Donate(bytes32,address,uint256,uint256)")),
        topic1: Some(B256::with_last_byte(7)),
        topic2: Some(B256::left_padding_from(&[0x11; 20])),
        data: DynSolValue::Tuple(vec![
            DynSolValue::Uint(U256::from(height), 256),
            DynSolValue::Uint(U256::from(log_index), 256),
        ])
        .abi_encode_params()
        .into(),
        block_number: height,
        block_hash: hash(height, 0),
        block_timestamp: 1_700_000_000 + height,
        ..Log::default()
    }
}

/// The running indexer's lake store, run with or without the Uniswap V4 manifest.
struct Live {
    lake: DeltaSink,
    decoder: Option<Decoder>,
}

impl Live {
    /// Opens the store as a run of the indexer would: with the Uniswap V4 manifest, or
    /// with none.
    async fn start(dir: &Dir, with_v4: bool) -> Self {
        let (schema, decoder) = if with_v4 {
            let catalog =
                Catalog::load(uniswap_v4(), &ChainId::new(CHAIN)).expect("uniswap v4 loads");
            (catalog.schema().clone(), Some(Decoder::new(catalog, [])))
        } else {
            (Schema::new().expect("the dataset tables"), None)
        };
        let mut lake = DeltaSink::open(&dir.store(), Arc::new(schema), CHAIN)
            .await
            .expect("the lake opens");
        lake.restore().await.expect("restored");
        Self { lake, decoder }
    }

    /// Indexes `heights`, three logs a block, two of them the `PoolManager`'s, decoding
    /// them when the run has the manifest, and commits them.
    async fn index(&mut self, heights: std::ops::RangeInclusive<u64>) {
        let chain = ChainId::new(CHAIN);
        for height in heights {
            for log in [
                donate(height, 0, POOL_MANAGER),
                donate(height, 1, Address::repeat_byte(0x22)),
                donate(height, 2, POOL_MANAGER),
            ] {
                let record = self
                    .decoder
                    .as_mut()
                    .and_then(|decoder| decoder.decode(&log))
                    .map(|(record, _)| record.expect("decodes").decoded);
                self.lake
                    .publish(Envelope::new(chain.clone(), Event::Log(Box::new(log))))
                    .await
                    .expect("accepted");
                if let Some(decoding) = record {
                    let event = Event::Decoded(Box::new(decoding));
                    self.lake
                        .publish(Envelope::new(chain.clone(), event))
                        .await
                        .expect("accepted");
                }
            }
            let block = BlockMeta {
                height,
                hash: hash(height, 0),
                parent_hash: hash(height - 1, 0),
                timestamp: 1_700_000_000 + height,
            };
            self.lake
                .publish(Envelope::new(chain.clone(), Event::AcceptedBlock(block)))
                .await
                .expect("accepted");
        }
        self.lake.flush().await.expect("committed");
        self.lake.finish().await.expect("ledger written");
    }
}

impl Live {
    /// Replaces block `height`, the newest committed, with an empty block on another
    /// branch, as a reorg at the tip would: the store deletes the orphaned block's rows.
    async fn reorg(&mut self, height: u64) {
        let chain = ChainId::new(CHAIN);
        for event in [
            Event::Reorg(Reorg {
                height,
                new_head_hash: hash(height, 9),
                orphaned_hashes: vec![hash(height, 0)],
            }),
            Event::AcceptedBlock(BlockMeta {
                height,
                hash: hash(height, 9),
                parent_hash: hash(height - 1, 0),
                timestamp: 1_700_000_000 + height,
            }),
        ] {
            self.lake
                .publish(Envelope::new(chain.clone(), event))
                .await
                .expect("accepted");
        }
        self.lake.flush().await.expect("committed");
        self.lake.finish().await.expect("ledger written");
    }
}

/// A lake the running indexer wrote from [`FIRST`] without the manifest, then from
/// `restart` through `last` with it. Returns the store, still open.
async fn deployed(dir: &Dir, restart: u64, last: u64) -> Live {
    Live::start(dir, false)
        .await
        .index(FIRST..=restart - 1)
        .await;
    let mut live = Live::start(dir, true).await;
    live.index(restart..=last).await;
    live
}

/// `(rows, distinct keys)` of `table` where `filter` holds.
async fn count(dir: &Dir, table: &str, filter: &str) -> (i64, i64) {
    let url = deltalake::ensure_table_uri(format!("{}/{CHAIN}/{table}", dir.uri())).expect("uri");
    let table = deltalake::open_table(url).await.expect("the table opens");
    let ctx = SessionContext::new();
    table
        .update_datafusion_session(&ctx.state())
        .expect("session");
    ctx.register_table("t", table.table_provider().await.expect("provider"))
        .expect("registered");
    let batches = ctx
        .sql(&format!(
            "SELECT count(*), count(DISTINCT dedupe_key) FROM t WHERE {filter}"
        ))
        .await
        .expect("planned")
        .collect()
        .await
        .expect("read");
    let column = |index: usize| {
        batches[0]
            .column(index)
            .as_primitive::<Int64Type>()
            .value(0)
    };
    (column(0), column(1))
}

/// Every row of `table` where `filter` holds, every column rendered, in key order: what a
/// reader of the table sees, whatever files hold it.
async fn rows(dir: &Dir, table: &str, filter: &str) -> Vec<String> {
    let url = deltalake::ensure_table_uri(format!("{}/{CHAIN}/{table}", dir.uri())).expect("uri");
    let table = deltalake::open_table(url).await.expect("the table opens");
    let ctx = SessionContext::new();
    table
        .update_datafusion_session(&ctx.state())
        .expect("session");
    ctx.register_table("t", table.table_provider().await.expect("provider"))
        .expect("registered");
    let batches = ctx
        .sql(&format!(
            "SELECT * FROM t WHERE {filter} ORDER BY dedupe_key"
        ))
        .await
        .expect("planned")
        .collect()
        .await
        .expect("read");
    let mut rows = Vec::new();
    for batch in &batches {
        let columns: Vec<_> = batch
            .columns()
            .iter()
            .map(|column| {
                ArrayFormatter::try_new(column.as_ref(), &FormatOptions::default())
                    .expect("formats")
            })
            .collect();
        for row in 0..batch.num_rows() {
            let values: Vec<String> = columns
                .iter()
                .map(|column| column.value(row).to_string())
                .collect();
            rows.push(values.join(" | "));
        }
    }
    rows
}

/// The whole hand-off: the running indexer decodes V4 from its restart, past the bound;
/// the backfill appends what it has not decoded below the bound, so every `PoolManager`
/// log is decoded once, and nothing from another contract. A second run appends
/// nothing, and the running store, open throughout, keeps committing.
#[tokio::test(flavor = "multi_thread")]
async fn a_backfill_fills_what_the_running_indexer_has_not_decoded() {
    let dir = Dir::new();
    let mut live = deployed(&dir, RESTART, LAST).await;

    let done = backfill(&dir.backfill(&uniswap_v4()))
        .await
        .expect("backfilled");
    assert_eq!(done.from, FIRST, "from the lake's first block");
    assert_eq!(done.to, BOUND, "through the bound");
    assert_eq!(done.decoded, done.logs);
    assert_eq!(
        done.appended,
        2 * 2 * usize::try_from(RESTART - FIRST).expect("fits"),
        "two rows for each PoolManager log the running indexer had not decoded"
    );
    assert_eq!(done.commits, 2, "split at 90,000");

    let expected = 2 * i64::try_from(LAST - FIRST + 1).expect("fits");
    let v4 = "protocol = 'uniswap_v4'";
    assert_eq!(count(&dir, "decoded_logs", v4).await, (expected, expected));
    assert_eq!(
        count(&dir, "uniswap_v4_pool_manager_donate", "true").await,
        (expected, expected)
    );

    let again = backfill(&dir.backfill(&uniswap_v4()))
        .await
        .expect("backfilled again");
    assert_eq!(again.appended, 0, "a rerun finds nothing missing");
    assert_eq!(count(&dir, "decoded_logs", v4).await, (expected, expected));

    live.index(LAST + 1..=LAST + 3).await;
    assert_eq!(
        count(&dir, "decoded_logs", v4).await.0,
        expected + 6,
        "the running store commits beside the backfill's appends"
    );
}

/// Once backfilled, the lake reads exactly as one the running indexer wrote with the
/// manifest from its first block: every decoded row, every column, once. A backfill
/// stopped between one table's commit and the next leaves a table short; a rerun appends
/// exactly what that table lacks, and nothing to the table that already has it.
#[tokio::test(flavor = "multi_thread")]
async fn a_backfilled_lake_reads_as_if_the_manifest_had_been_there_from_the_start() {
    let control = Dir::new();
    Live::start(&control, true).await.index(FIRST..=LAST).await;
    let dir = Dir::new();
    deployed(&dir, RESTART, LAST).await;
    backfill(&dir.backfill(&uniswap_v4()))
        .await
        .expect("backfilled");

    let v4 = "protocol = 'uniswap_v4'";
    let decoded = rows(&control, "decoded_logs", v4).await;
    let donates = rows(&control, "uniswap_v4_pool_manager_donate", "true").await;
    assert_eq!(
        decoded.len(),
        2 * usize::try_from(LAST - FIRST + 1).expect("fits")
    );
    assert_eq!(rows(&dir, "decoded_logs", v4).await, decoded);
    assert_eq!(
        rows(&dir, "uniswap_v4_pool_manager_donate", "true").await,
        donates
    );

    // A stop after `decoded_logs` committed and before the event table did.
    let url = deltalake::ensure_table_uri(format!(
        "{}/{CHAIN}/uniswap_v4_pool_manager_donate",
        dir.uri()
    ))
    .expect("uri");
    let lost = FIRST + 500;
    deltalake::open_table(url)
        .await
        .expect("the table opens")
        .delete()
        .with_predicate(format!("block_number < {lost}"))
        .await
        .expect("deleted");
    let done = backfill(&dir.backfill(&uniswap_v4()))
        .await
        .expect("backfilled again");
    assert_eq!(
        done.appended,
        2 * usize::try_from(lost - FIRST).expect("fits"),
        "only the event table's missing rows"
    );
    assert_eq!(rows(&dir, "decoded_logs", v4).await, decoded);
    assert_eq!(
        rows(&dir, "uniswap_v4_pool_manager_donate", "true").await,
        donates
    );
}

/// The backfill's appends commit beside the running store in either order: after the
/// store's own appends, and after a reorg's delete removed a file the backfill had read,
/// which Delta refuses once and the backfill retries on a fresh copy of the table. The
/// store commits on throughout.
#[tokio::test(flavor = "multi_thread")]
async fn appends_commit_beside_the_running_stores_appends_and_deletes() {
    let dir = Dir::new();
    let mut live = deployed(&dir, RESTART, LAST).await;
    live.index(LAST + 1..=LAST + 2).await;

    let chain = ChainId::new(CHAIN);
    let catalog = Catalog::load(uniswap_v4(), &chain).expect("loads");
    let schema = catalog.schema().clone();
    let mut decoder = Decoder::new(catalog, []);
    // Opened now, so its copy of the tables holds the file the reorg below rewrites.
    let width = dir.store().partition_blocks;
    let mut tables = DecodedTables::open(&dir.uri(), &BTreeMap::new(), &schema, CHAIN, width)
        .await
        .expect("opens");
    live.reorg(LAST + 2).await;
    live.index(LAST + 3..=LAST + 3).await;

    let logs: Vec<Log> = (FIRST..FIRST + 10)
        .flat_map(|height| {
            [
                donate(height, 0, POOL_MANAGER),
                donate(height, 2, POOL_MANAGER),
            ]
        })
        .collect();
    let records: Vec<Event> = decode_all(&mut decoder, &chain, &logs)
        .expect("decodes")
        .into_iter()
        .map(|record| Event::Decoded(Box::new(record.decoded)))
        .collect();
    let appended = tables
        .fill(
            FIRST..=FIRST + 9,
            &["uniswap_v4".to_owned()],
            rows_of(&chain, &schema, &records).expect("rows"),
        )
        .await
        .expect("appended despite the reorg's delete");
    assert_eq!(appended, 2 * 20);

    live.index(LAST + 4..=LAST + 4).await;
    let v4 = "protocol = 'uniswap_v4'";
    let expected = 2 * i64::try_from(LAST + 4 - RESTART + 1).expect("fits") - 2 + 20;
    assert_eq!(
        count(&dir, "decoded_logs", v4).await,
        (expected, expected),
        "the store's rows, less the orphaned block's, and the backfill's, once each"
    );
}

/// With `out`, the chain's lake is only read: the records go to the other lake, whatever
/// the running indexer decodes, and from `from` on when set.
#[tokio::test(flavor = "multi_thread")]
async fn a_backfill_into_another_lake_leaves_the_chains_alone() {
    let (dir, out) = (Dir::new(), Dir::new());
    Live::start(&dir, false)
        .await
        .index(FIRST..=FIRST + MAX_RESUME_LAG + 500)
        .await;
    let mut settings = dir.backfill(&uniswap_v4());
    settings.run.out = Some(out.uri());
    let done = backfill(&settings).await.expect("backfilled");
    let expected = 2 * 501;
    assert_eq!(
        done.decoded, expected,
        "every PoolManager log through the bound"
    );
    let expected = i64::try_from(expected).expect("fits");
    let v4 = "protocol = 'uniswap_v4'";
    assert_eq!(count(&out, "decoded_logs", v4).await, (expected, expected));
    assert_eq!(
        count(&out, "uniswap_v4_pool_manager_donate", "true").await,
        (expected, expected)
    );
    assert_eq!(
        count(&dir, "decoded_logs", v4).await.0,
        0,
        "the chain's lake is unchanged"
    );
    assert!(
        !out.0.join(CHAIN).join("logs").exists(),
        "only the decoded tables are written"
    );

    let resumed = Dir::new();
    let mut settings = dir.backfill(&uniswap_v4());
    settings.run.out = Some(resumed.uri());
    settings.run.from = Some(FIRST + 300);
    let done = backfill(&settings).await.expect("backfilled from a block");
    assert_eq!(done.from, FIRST + 300);
    assert_eq!(done.decoded, 2 * 201);
}

/// A lake whose running indexer recorded no session, decodes none of the protocols, or
/// started decoding them above the bound, is refused; so is a `to` above the bound.
#[tokio::test(flavor = "multi_thread")]
async fn a_lake_the_running_indexer_does_not_cover_is_refused() {
    let not_decoding = Dir::new();
    Live::start(&not_decoding, false)
        .await
        .index(FIRST..=FIRST + 10)
        .await;
    let refused = backfill(&not_decoding.backfill(&uniswap_v4())).await;
    assert!(
        matches!(&refused, Err(BackfillError::NotDecoding { store: "lake", protocol, from })
            if protocol == "uniswap_v4" && *from == FIRST),
        "{refused:?}"
    );

    // Without `to` a backfill would wait for the bound to pass the restart; with one
    // below it, it is refused.
    let too_recent = Dir::new();
    deployed(&too_recent, FIRST + 5, FIRST + 10).await;
    let mut below = too_recent.backfill(&uniswap_v4());
    below.run.to = Some(0);
    let refused = backfill(&below).await;
    assert!(
        matches!(refused, Err(BackfillError::SessionAboveTo { from, to: 0 }) if from == FIRST + 5),
        "{refused:?}"
    );

    let mut settings = too_recent.backfill(&uniswap_v4());
    settings.run.to = Some(FIRST + 10);
    let refused = backfill(&settings).await;
    assert!(
        matches!(
            refused,
            Err(BackfillError::ToAboveBound { to, bound })
            if to == FIRST + 10 && bound == FIRST + 10 - MAX_RESUME_LAG
        ),
        "{refused:?}"
    );

    std::fs::remove_dir_all(too_recent.0.join(CHAIN).join("sessions")).expect("removed");
    let refused = backfill(&too_recent.backfill(&uniswap_v4())).await;
    assert!(
        matches!(refused, Err(BackfillError::NoSession { store: "lake" })),
        "{refused:?}"
    );
}

/// A session that decodes the protocol with other event definitions is refused: the
/// backfill's keys would not match the running indexer's.
#[test]
fn a_session_with_other_event_definitions_is_refused() {
    let catalog = Catalog::load(uniswap_v4(), &ChainId::new(CHAIN)).expect("loads");
    let wanted = catalog.schema().decoded_events();
    let protocols = ["uniswap_v4".to_owned()];
    let coverage = Coverage {
        wanted: &wanted,
        protocols: &protocols,
    };
    let session = |events: Vec<SessionEvent>| Session {
        block_number: 5,
        block_hash: B256::ZERO,
        block_timestamp: 0,
        events,
    };
    assert!(
        coverage
            .decodes("lake", Some(&session(wanted.clone())))
            .is_ok()
    );
    let mut other = wanted.clone();
    other[0].event_id = B256::repeat_byte(0xee);
    assert!(matches!(
        coverage.decodes("lake", Some(&session(other))),
        Err(BackfillError::EventsDiffer { .. })
    ));
    let mut fewer = wanted.clone();
    fewer.pop();
    assert!(matches!(
        coverage.decodes("lake", Some(&session(fewer))),
        Err(BackfillError::EventsDiffer { .. })
    ));
    let mut more = wanted.clone();
    more.push(SessionEvent {
        event_id: B256::repeat_byte(0xef),
        ..wanted[0].clone()
    });
    assert!(
        matches!(
            coverage.decodes("lake", Some(&session(more))),
            Err(BackfillError::EventsDiffer { .. })
        ),
        "an event the running indexer decodes and the backfill would not"
    );
    let mut other_protocol = wanted.clone();
    other_protocol.push(SessionEvent {
        protocol: "metric_v1".to_owned(),
        ..wanted[0].clone()
    });
    assert!(
        coverage
            .decodes("lake", Some(&session(other_protocol)))
            .is_ok(),
        "other protocols the running indexer decodes are its own business"
    );
}

/// A lake another backfill holds is refused, naming the holder.
#[tokio::test(flavor = "multi_thread")]
async fn a_lake_another_backfill_holds_is_refused() {
    let dir = Dir::new();
    deployed(&dir, FIRST + 5, FIRST + 10).await;
    let holder = RunLock::acquire(&dir.uri(), &BTreeMap::new(), CHAIN, "other", "uniswap_v4")
        .await
        .expect("the lease is free");
    let refused = backfill(&dir.backfill(&uniswap_v4())).await;
    assert!(
        matches!(&refused, Err(BackfillError::Lock(LockError::Held { run, .. })) if run == "other"),
        "{refused:?}"
    );
    holder.release().await.expect("released");
}

/// A lake whose ledger is empty has indexed nothing to backfill from.
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_ledger_is_refused() {
    let dir = Dir::new();
    let catalog = Catalog::load(uniswap_v4(), &ChainId::new(CHAIN)).expect("loads");
    let mut lake = DeltaSink::open(&dir.store(), Arc::new(catalog.schema().clone()), CHAIN)
        .await
        .expect("the lake opens");
    lake.finish().await.expect("finished");
    let refused = backfill(&dir.backfill(&uniswap_v4())).await;
    assert!(
        matches!(refused, Err(BackfillError::EmptyLedger)),
        "{refused:?}"
    );
}

/// Manifests that list no contract on the chain are refused before the lake is opened.
#[tokio::test(flavor = "multi_thread")]
async fn manifests_a_backfill_cannot_replay_are_refused() {
    let dir = Dir::new();
    let settings = BackfillSettings {
        chain: "elsewhere".to_owned(),
        ..dir.backfill(&uniswap_v4())
    };
    let refused = backfill(&settings).await;
    assert!(
        matches!(refused, Err(BackfillError::NoContracts { .. })),
        "{refused:?}"
    );
    assert!(!dir.0.exists(), "the lake is never opened");
}

/// Commits hold every log once, cover their window without gaps, never split a block or
/// cross a window, and stay within [`COMMIT_LOGS`] unless one block holds more.
#[test]
fn commits_cover_the_logs_in_bounded_whole_blocks() {
    let at = |block_number: u64| Log {
        block_number,
        ..Log::default()
    };
    let mut logs = vec![at(5)];
    logs.extend((0..60_000).map(|_| at(15_000)));
    logs.extend((0..120_000).map(|index| at(20_000 + index / 10)));
    let parts = parts(&logs, &(3..=99_999));

    assert_eq!(parts[0], (3..=9_999, 0..1), "from the read's first block");
    assert_eq!(
        parts[1],
        (10_000..=19_999, 1..60_001),
        "one block is never split"
    );
    let window = |height: &u64| height / COMMIT_BLOCKS;
    let mut covered = 0;
    for (index, (blocks, positions)) in parts.iter().enumerate() {
        assert_eq!(positions.start, covered, "every log once, in order");
        covered = positions.end;
        let held = &logs[positions.clone()];
        assert!(held.iter().all(|log| blocks.contains(&log.block_number)));
        assert_eq!(
            window(blocks.start()),
            window(blocks.end()),
            "{blocks:?} stays in its window"
        );
        if held.len() > COMMIT_LOGS {
            assert!(
                held.iter()
                    .all(|log| log.block_number == held[0].block_number)
            );
        }
        if let Some((next, next_positions)) = parts.get(index + 1) {
            assert_ne!(
                logs[positions.end - 1].block_number,
                logs[next_positions.start].block_number,
                "a block in one commit"
            );
            if window(next.start()) == window(blocks.start()) {
                assert_eq!(*next.start(), blocks.end() + 1, "no gap in a window");
            }
        }
    }
    assert_eq!(covered, logs.len());
    assert_eq!(
        parts.len(),
        5,
        "the window from 20,000, with 100,000 logs, split in two by count"
    );
}

/// A stand-in for every secret a settings file names.
#[expect(clippy::unnecessary_wraps, reason = "a `secret::Env`")]
fn stand_in(_: &str) -> Result<String, std::env::VarError> {
    Ok("stand-in".to_owned())
}

/// A running indexer's settings file with `backfill` appended, written in `dir`.
fn settings_file(dir: &Dir, backfill: &str) -> PathBuf {
    let text = format!(
        "[ingest]\nchain = \"{CHAIN}\"\nhttp_url = \"http://node\"\nws_url = \"ws://node\"\n\
         [sink.delta]\ntables = [\"*\"]\nuri = \"s3://lake\"\n\
         [decode]\nprotocols = \"protocols\"\n{backfill}"
    );
    std::fs::create_dir_all(&dir.0).expect("created");
    let path = dir.0.join("indexer.base.toml");
    std::fs::write(&path, text).expect("written");
    path
}

/// A backfill runs on the running indexer's own settings file: the manifests its
/// `[backfill]` names and those the running indexer decodes, both found beside the file,
/// and its chain and lake. A file without `[backfill]`, or with an unknown key in it,
/// is refused.
#[test]
fn a_backfill_reads_the_running_indexers_settings() {
    let dir = Dir::new();
    let path = settings_file(&dir, "[backfill]\nprotocols = \"protocols/uniswap/v4\"\n");
    let settings = BackfillSettings::from_file(&path, stand_in).expect("parses");
    assert_eq!(settings.run.protocols, dir.0.join("protocols/uniswap/v4"));
    assert_eq!(settings.decoded, dir.0.join("protocols"));
    assert_eq!(
        (settings.chain.as_str(), settings.lake.uri.as_str()),
        (CHAIN, "s3://lake")
    );

    assert!(matches!(
        BackfillSettings::from_file(&settings_file(&dir, ""), stand_in),
        Err(BackfillError::Missing {
            section: "[backfill]"
        })
    ));
    assert!(matches!(
        BackfillSettings::from_file(
            &settings_file(&dir, "[backfill]\nprotocols = \"p\"\ntoo = 9\n"),
            stand_in
        ),
        Err(BackfillError::Settings(_))
    ));
}

/// The running indexer's database is filled beside it as its lake is: the backfill checks
/// the database's own session, then upserts what the running indexer did not decode, in
/// the tables `contract_events` routes there. Runs in a private schema, dropped after.
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires INDEXER_TEST_POSTGRES_URL pointing to PostgreSQL 18"]
async fn a_backfill_fills_the_running_indexers_database_too() {
    use crate::sink::PostgresSettings;

    let url = std::env::var("INDEXER_TEST_POSTGRES_URL").expect("test database URL");
    // A database schema of its own, so nothing else is touched; the manifest lists the
    // PoolManager on the chain.
    let chain = ChainId::new(CHAIN);
    let namespace = format!("backfill_test_{}", std::process::id());
    let manifests = Dir::new();
    std::fs::create_dir_all(&manifests.0).expect("a directory");
    std::fs::write(
        manifests.0.join("protocol.toml"),
        format!(
            "protocol = \"uniswap_v4\"\n[[contract]]\nabi = [{:?}]\naddresses = {{ {chain} = [\"{POOL_MANAGER}\"] }}\n",
            uniswap_v4().join("PoolManager.json").display().to_string()
        ),
    )
    .expect("written");
    let catalog = Catalog::load(&manifests.0, &chain).expect("the manifest loads");
    let decoding = catalog.schema().clone();
    let postgres: PostgresSettings = serde_json::from_value(serde_json::json!({
        "tables": ["contract_events"],
        "connection_string": url,
        "schema": namespace,
    }))
    .expect("settings");

    // The running indexer: both stores, without the manifest, then restarted with it.
    let dir = Dir::new();
    let restart = FIRST + 200;
    let last = restart + MAX_RESUME_LAG + 100;
    let mut decoder = Decoder::new(catalog, []);
    for (heights, decodes) in [(FIRST..=restart - 1, false), (restart..=last, true)] {
        let schema = if decodes {
            decoding.clone()
        } else {
            Schema::new().expect("the dataset tables")
        };
        let decoder = decodes.then_some(&mut decoder);
        index_both(&dir, &chain, &postgres, schema, decoder, heights).await;
    }

    let settings = BackfillSettings {
        chain: chain.as_str().to_owned(),
        postgres: Some(postgres.clone()),
        ..dir.backfill(&manifests.0)
    };
    let done = backfill(&settings).await.expect("backfilled");
    assert_eq!(
        done.appended,
        2 * 2 * usize::try_from(restart - FIRST).expect("fits")
    );
    // Upserted on its key, so a rerun leaves the database as it was.
    backfill(&settings).await.expect("backfilled again");

    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect");
    let task = tokio::spawn(connection);
    let row = client
        .query_one(
            &format!(
                "SELECT count(*), count(DISTINCT dedupe_key) FROM \"{namespace}\".uniswap_v4_pool_manager_donate"
            ),
            &[],
        )
        .await
        .expect("counted");
    let expected = 2 * i64::try_from(last - FIRST + 1).expect("fits");
    assert_eq!(
        (row.get::<_, i64>(0), row.get::<_, i64>(1)),
        (expected, expected)
    );
    client
        .batch_execute(&format!("DROP SCHEMA \"{namespace}\" CASCADE"))
        .await
        .expect("drop test schema");
    task.abort();
}

/// Indexes `heights` into both stores of the running indexer, as a run with `schema`
/// would: two `PoolManager` logs a block, decoded when the run has the manifest.
#[cfg(feature = "postgres")]
async fn index_both(
    dir: &Dir,
    chain: &ChainId,
    postgres: &crate::sink::PostgresSettings,
    schema: Schema,
    mut decoder: Option<&mut Decoder>,
    heights: std::ops::RangeInclusive<u64>,
) {
    let routed = postgres.tables.resolve(&schema).unwrap_or_default();
    let mut lake = DeltaSink::open(&dir.store(), Arc::new(schema.clone()), chain.as_str())
        .await
        .expect("the lake opens");
    let mut database = crate::sink::PostgresSink::open(
        postgres,
        Arc::new(schema.select(&routed)),
        chain.as_str(),
        crate::sink::postgres::Writer::Sole,
    )
    .await
    .expect("the database opens");
    for height in heights {
        let mut events = Vec::new();
        for log in [
            donate(height, 0, POOL_MANAGER),
            donate(height, 2, POOL_MANAGER),
        ] {
            let record = decoder
                .as_deref_mut()
                .and_then(|decoder| decoder.decode(&log))
                .map(|(record, _)| record.expect("decodes").decoded);
            events.push(Event::Log(Box::new(log)));
            events.extend(record.map(|decoded| Event::Decoded(Box::new(decoded))));
        }
        events.push(Event::AcceptedBlock(BlockMeta {
            height,
            hash: hash(height, 0),
            parent_hash: hash(height - 1, 0),
            timestamp: 1_700_000_000 + height,
        }));
        for event in events {
            let envelope = Envelope::new(chain.clone(), event);
            lake.publish(envelope.clone()).await.expect("accepted");
            database.publish(envelope).await.expect("accepted");
        }
    }
    for flushed in [lake.flush().await, database.flush().await] {
        flushed.expect("committed");
    }
    lake.finish().await.expect("ledger written");
}

/// A Metric pool listed by address rather than discovered, so a backfill replays it.
const METRIC_POOL: Address = Address::repeat_byte(0xd0);

/// The account every Metric swap's transaction is sent from.
const SWAPPER: Address = Address::repeat_byte(0x5a);

/// A directory holding one manifest: Metric's pool at [`METRIC_POOL`].
fn metric_pool() -> Dir {
    let manifests = Dir::new();
    std::fs::create_dir_all(&manifests.0).expect("a directory");
    std::fs::write(
        manifests.0.join("protocol.toml"),
        format!(
            "protocol = \"metric_v1\"\n[[contract]]\nabi = [{:?}]\naddresses = {{ {CHAIN} = [\"{METRIC_POOL}\"] }}\n",
            protocols("metric/v1/MetricOmmPool.json")
                .display()
                .to_string()
        ),
    )
    .expect("written");
    manifests
}

/// The store behind a borrow, so a [`DecodingSink`] can run in front of a store the test
/// keeps.
struct Borrowed<'a>(&'a mut DeltaSink);

impl crate::sink::EnvelopeSink for Borrowed<'_> {
    async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
        self.0.publish(envelope).await
    }

    async fn flush(&mut self) -> Result<(), SinkError> {
        self.0.flush().await
    }
}

/// Indexes `heights` into the lake as the running indexer would, with the manifests in
/// `manifests` decoding and deriving in front of the store when given: per block, a
/// transaction and its Metric `Swap`, except every hundredth block's, whose transaction
/// was not fetched.
async fn index_metric(dir: &Dir, manifests: Option<&Path>, heights: std::ops::RangeInclusive<u64>) {
    let chain = ChainId::new(CHAIN);
    let catalog = manifests.map(|manifests| Catalog::load(manifests, &chain).expect("loads"));
    let schema = catalog.as_ref().map_or_else(
        || Schema::new().expect("the dataset tables"),
        |catalog| catalog.schema().clone(),
    );
    let mut lake = DeltaSink::open(&dir.store(), Arc::new(schema), CHAIN)
        .await
        .expect("the lake opens");
    lake.restore().await.expect("restored");
    let envelopes = heights.flat_map(|height| {
        let transaction = hash(height, 1);
        let swap = Log {
            log_index: 0,
            transaction_hash: transaction,
            address: METRIC_POOL,
            topic0: Some(keccak256("Swap(address,address,uint256,uint256,uint256)")),
            topic1: Some(Address::repeat_byte(0x5e).into_word()),
            topic2: Some(Address::repeat_byte(0x4e).into_word()),
            data: DynSolValue::Tuple(vec![
                DynSolValue::Uint(U256::from(height), 256),
                DynSolValue::Uint((U256::from(height) << 128) | U256::from(7), 256),
                DynSolValue::Uint(U256::ZERO, 256),
            ])
            .abi_encode_params()
            .into(),
            block_number: height,
            block_hash: hash(height, 0),
            block_timestamp: 1_700_000_000 + height,
            ..Log::default()
        };
        let sent = (height % 100 != 0).then(|| {
            Event::Transaction(Box::new(Transaction {
                hash: transaction,
                from: SWAPPER,
                to: Some(METRIC_POOL),
                block_number: height,
                block_hash: hash(height, 0),
                ..Transaction::default()
            }))
        });
        let block = Event::AcceptedBlock(BlockMeta {
            height,
            hash: hash(height, 0),
            parent_hash: hash(height - 1, 0),
            timestamp: 1_700_000_000 + height,
        });
        sent.into_iter()
            .chain([Event::Log(Box::new(swap)), block])
            .map(|event| Envelope::new(chain.clone(), event))
            .collect::<Vec<_>>()
    });
    if let Some(catalog) = catalog {
        let mut sink = DecodingSink::new(Decoder::new(catalog, []), Borrowed(&mut lake));
        for envelope in envelopes {
            sink.publish(envelope).await.expect("accepted");
        }
        sink.flush().await.expect("committed");
    } else {
        for envelope in envelopes {
            lake.publish(envelope).await.expect("accepted");
        }
        lake.flush().await.expect("committed");
    }
    lake.finish().await.expect("ledger written");
}

/// A backfill fills a derived table as the running indexer's decode stage would have,
/// had the manifest been there from the first block: every row, every column, the
/// transaction's sender and recipient read back from the lake's `transactions`, and none
/// where the lake holds no transaction.
#[tokio::test(flavor = "multi_thread")]
async fn a_backfill_derives_as_the_running_indexer_would_have() {
    let manifests = metric_pool();
    let control = Dir::new();
    index_metric(&control, Some(&manifests.0), FIRST..=LAST).await;
    let dir = Dir::new();
    index_metric(&dir, None, FIRST..=RESTART - 1).await;
    index_metric(&dir, Some(&manifests.0), RESTART..=LAST).await;
    let done = backfill(&dir.backfill(&manifests.0))
        .await
        .expect("backfilled");
    assert!(done.appended > 0, "{done:?}");

    let swaps = rows(&control, "metric_v1_swaps", "true").await;
    assert_eq!(
        swaps.len(),
        usize::try_from(LAST - FIRST + 1).expect("fits")
    );
    assert_eq!(rows(&dir, "metric_v1_swaps", "true").await, swaps);
    let swapper = format!("{SWAPPER:#x}");
    assert_eq!(
        count(
            &control,
            "metric_v1_swaps",
            &format!("transaction_from_address = '{swapper}'")
        )
        .await
        .0,
        i64::try_from(LAST - FIRST + 1 - (LAST / 100 - (FIRST - 1) / 100)).expect("fits"),
        "every swap but those whose transaction was not fetched carries its sender"
    );
}

/// Uniswap V3's factory on Base, as the shipped manifest seeds it.
const V3_FACTORY: Address = address!("0x33128a8fC17869897dcE68Ed026d694621f6FDfD");

/// A pool the factory created below the running indexer's restart, which only a backfill
/// can discover.
const EARLY_POOL: Address = Address::repeat_byte(0xc1);

/// A pool created after the restart, which the running indexer discovers itself.
const LATE_POOL: Address = Address::repeat_byte(0xc2);

/// The factory's `PoolCreated` for `pool` in block `height`.
fn pool_created(height: u64, pool: Address, log_index: u64) -> Log {
    Log {
        log_index,
        transaction_hash: hash(height, 1),
        address: V3_FACTORY,
        topic0: Some(keccak256(
            "PoolCreated(address,address,uint24,int24,address)",
        )),
        topic1: Some(B256::with_last_byte(1)),
        topic2: Some(B256::with_last_byte(2)),
        topic3: Some(B256::left_padding_from(&[0x0b, 0xb8])),
        data: DynSolValue::Tuple(vec![
            DynSolValue::Int(alloy_primitives::I256::try_from(60).expect("int"), 24),
            DynSolValue::Address(pool),
        ])
        .abi_encode_params()
        .into(),
        block_number: height,
        block_hash: hash(height, 0),
        block_timestamp: 1_700_000_000 + height,
        ..Log::default()
    }
}

/// A swap of `pool` in block `height`.
fn pool_swap(height: u64, pool: Address, log_index: u64) -> Log {
    let envelope: Envelope = serde_json::from_str(
        include_str!("../../examples/fixtures/uniswap_v3_swaps.ndjson")
            .lines()
            .next()
            .expect("fixture"),
    )
    .expect("envelope");
    let Event::Log(log) = envelope.event else {
        panic!("a log");
    };
    Log {
        log_index,
        transaction_hash: hash(height, 2),
        address: pool,
        block_number: height,
        block_hash: hash(height, 0),
        block_timestamp: 1_700_000_000 + height,
        ..*log
    }
}

/// Block `height`'s envelopes: the factory's `PoolCreated` for [`EARLY_POOL`] ten blocks
/// past [`FIRST`] and for [`LATE_POOL`] ten past [`RESTART`], a swap of each pool in
/// every block from its creation on, then the block's marker.
fn v3_block(height: u64) -> Vec<Event> {
    let mut logs = Vec::new();
    for (pool, created_at, index) in [(EARLY_POOL, FIRST + 10, 0), (LATE_POOL, RESTART + 10, 2)] {
        if height == created_at {
            logs.push(pool_created(height, pool, index));
        }
        if height >= created_at {
            logs.push(pool_swap(height, pool, index + 1));
        }
    }
    logs.into_iter()
        .map(|log| Event::Log(Box::new(log)))
        .chain([Event::AcceptedBlock(BlockMeta {
            height,
            hash: hash(height, 0),
            parent_hash: hash(height - 1, 0),
            timestamp: 1_700_000_000 + height,
        })])
        .collect()
}

/// The running indexer with the Uniswap V3 manifest: its lake behind its decode stage,
/// restored as a restart would, and the sender its watch on the queue would hand
/// contracts through.
struct LiveV3 {
    sink: DecodingSink<DeltaSink>,
    handed: tokio::sync::mpsc::Sender<Vec<crate::wire::datasets::evm::Contract>>,
    next: u64,
}

impl LiveV3 {
    async fn start(dir: &Dir, next: u64) -> Self {
        let catalog = Catalog::load(protocols("uniswap/v3"), &ChainId::new(CHAIN)).expect("loads");
        let mut lake = DeltaSink::open(&dir.store(), Arc::new(catalog.schema().clone()), CHAIN)
            .await
            .expect("the lake opens");
        let restored = lake.restore().await.expect("restored");
        let decoder = Decoder::new(catalog, restored.contracts);
        let (handed, handing) = tokio::sync::mpsc::channel(4);
        Self {
            sink: DecodingSink::new(decoder, lake).with_handoffs(restored.handoffs, handing),
            handed,
            next,
        }
    }

    /// Indexes through `last` and commits.
    async fn index(&mut self, last: u64) {
        let chain = ChainId::new(CHAIN);
        for height in self.next..=last {
            for event in v3_block(height) {
                self.sink
                    .publish(Envelope::new(chain.clone(), event))
                    .await
                    .expect("accepted");
            }
        }
        self.next = last + 1;
        self.sink.flush().await.expect("committed");
        self.sink.finish().await.expect("ledger written");
    }

    /// Hands the decode stage what the lake's queue holds, as the watch does.
    async fn watch(&self, dir: &Dir, schema: &Schema) {
        let queue = crate::sink::delta::HandoffQueue::open(
            &dir.uri(),
            &BTreeMap::new(),
            schema,
            CHAIN,
            dir.store().partition_blocks,
        )
        .await
        .expect("the queue opens");
        let contracts = queue.read().await.expect("read");
        if !contracts.is_empty() {
            self.handed.send(contracts).await.expect("sent");
        }
    }
}

/// How many blocks the running indexer beside a backfill commits at a time: few enough
/// that a handed-over contract is recorded soon after it is queued, enough that a
/// backfill waiting for the bound sees it pass in a few dozen commits.
const STEP: u64 = 1_000;

/// A backfill run beside `live`, which watches the queue and indexes [`STEP`] blocks at a
/// time until the backfill ends, as the running indexer would.
async fn beside(live: &mut LiveV3, dir: &Dir, settings: BackfillSettings) -> super::Backfilled {
    let schema = Catalog::load(protocols("uniswap/v3"), &ChainId::new(CHAIN))
        .expect("loads")
        .schema()
        .clone();
    let backfilling = tokio::spawn(async move { backfill(&settings).await });
    while !backfilling.is_finished() {
        live.watch(dir, &schema).await;
        let next = live.next;
        live.index(next + STEP - 1).await;
    }
    backfilling.await.expect("joined").expect("backfilled")
}

/// The lake the running indexer wrote from [`FIRST`] without the Uniswap V3 manifest,
/// then from [`RESTART`] through `last` with it, which goes on beside the backfill.
async fn deployed_v3(dir: &Dir, last: u64) -> LiveV3 {
    let chain = ChainId::new(CHAIN);
    let mut lake = DeltaSink::open(
        &dir.store(),
        Arc::new(Schema::new().expect("the dataset tables")),
        CHAIN,
    )
    .await
    .expect("the lake opens");
    lake.restore().await.expect("restored");
    for height in FIRST..RESTART {
        for event in v3_block(height) {
            lake.publish(Envelope::new(chain.clone(), event))
                .await
                .expect("accepted");
        }
    }
    lake.flush().await.expect("committed");
    lake.finish().await.expect("ledger written");
    drop(lake);
    let mut live = LiveV3::start(dir, RESTART).await;
    live.index(last).await;
    live
}

/// Where the running indexer recorded each contract handed to it.
async fn records(dir: &Dir) -> Vec<(Address, u64)> {
    let schema = Schema::new().expect("schema");
    crate::sink::delta::recorded(&dir.uri(), &BTreeMap::new(), &schema, CHAIN)
        .await
        .expect("read")
        .iter()
        .map(|handoff| (handoff.address, handoff.block_number))
        .collect()
}

/// The early pool's swaps in `dir`, and as a run with the manifest from the first block
/// through `live`'s last would have decoded them.
async fn early_swaps(dir: &Dir, live: &LiveV3) -> (Vec<String>, Vec<String>) {
    let control = Dir::new();
    let mut whole = LiveV3::start(&control, FIRST).await;
    whole.index(live.next - 1).await;
    let early = format!("address = '{EARLY_POOL:#x}'");
    let expected = rows(&control, "uniswap_v3_pool_swap", &early).await;
    assert_eq!(
        expected.len(),
        usize::try_from(live.next - 1 - (FIRST + 10) + 1).expect("fits")
    );
    (rows(dir, "uniswap_v3_pool_swap", &early).await, expected)
}

/// A backfill started right after the deploy, its restart still above the bound, hands
/// a pool created below the restart to the running indexer at once, so the pool is
/// decoded live from soon after the restart, not four ledger windows later. It then waits
/// until the bound passes the record and fills everything below it in the one run: the
/// pool's swaps read as if the manifest had been there from the first block. `only`
/// naming a contract the factories did not create is refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_discovered_contract_is_handed_over_and_backfilled_without_a_gap() {
    let dir = Dir::new();
    let mut live = deployed_v3(&dir, RESTART + 10).await;

    let mut unknown = dir.backfill(&protocols("uniswap/v3"));
    unknown.run.only = Some(vec![Address::repeat_byte(0xee)]);
    let refused = backfill(&unknown).await;
    assert!(
        matches!(refused, Err(BackfillError::NotDiscovered { .. })),
        "{refused:?}"
    );

    let done = beside(&mut live, &dir, dir.backfill(&protocols("uniswap/v3"))).await;
    assert_eq!(
        (done.discovered, done.handed, done.rerun_through),
        (1, 1, None),
        "{done:?}"
    );
    let recorded = records(&dir).await;
    let [(pool, at)] = recorded[..] else {
        panic!("only the pool below the restart is handed over: {recorded:?}");
    };
    assert_eq!(pool, EARLY_POOL);
    assert!(
        at < RESTART + MAX_RESUME_LAG,
        "handed over before the bound passed the restart: {at}"
    );
    assert!(done.to >= at - 1, "filled up to the record: {done:?}");
    let (swaps, expected) = early_swaps(&dir, &live).await;
    assert_eq!(swaps, expected);
}

/// With `to` given, a backfill does not wait: it fills a handed-over pool through `to`
/// and says to run again from there. Once the tip is past the record, a rerun from there
/// completes it, handing over nothing new.
#[tokio::test(flavor = "multi_thread")]
async fn a_backfill_through_a_given_to_says_to_run_again() {
    let dir = Dir::new();
    let mut live = deployed_v3(&dir, LAST).await;

    let mut bounded = dir.backfill(&protocols("uniswap/v3"));
    bounded.run.to = Some(BOUND);
    let first = beside(&mut live, &dir, bounded).await;
    assert_eq!((first.discovered, first.handed, first.to), (1, 1, BOUND));
    let through = first.rerun_through.expect("the record is above `to`");
    assert_eq!(records(&dir).await, [(EARLY_POOL, through + 1)]);

    live.index(through + MAX_RESUME_LAG + 1).await;
    let mut rerun = dir.backfill(&protocols("uniswap/v3"));
    rerun.run.from = Some(first.to + 1);
    let second = beside(&mut live, &dir, rerun).await;
    assert_eq!(
        (second.discovered, second.handed, second.rerun_through),
        (0, 1, None),
        "{second:?}"
    );
    assert!(second.to >= through);
    let (swaps, expected) = early_swaps(&dir, &live).await;
    assert_eq!(swaps, expected);
}

/// A pool the factory created before the lake's first block, which only `HyperSync`
/// knows of.
const ANCIENT_POOL: Address = Address::repeat_byte(0xc0);

/// A pool the factory created before the lake's first block is found over `HyperSync`,
/// which is asked for only the factory's logs and only below that block. It is handed
/// over and backfilled with the pool created in the lake, so its swaps the lake holds
/// are decoded.
#[tokio::test(flavor = "multi_thread")]
async fn a_pool_created_before_the_lake_is_found_over_hypersync() {
    let dir = Dir::new();
    let chain = ChainId::new(CHAIN);
    let mut lake = DeltaSink::open(
        &dir.store(),
        Arc::new(Schema::new().expect("the dataset tables")),
        CHAIN,
    )
    .await
    .expect("the lake opens");
    lake.restore().await.expect("restored");
    for height in FIRST..RESTART {
        let mut events = v3_block(height);
        let marker = events.pop().expect("the marker");
        events.push(Event::Log(Box::new(pool_swap(height, ANCIENT_POOL, 9))));
        events.push(marker);
        for event in events {
            lake.publish(Envelope::new(chain.clone(), event))
                .await
                .expect("accepted");
        }
    }
    lake.flush().await.expect("committed");
    lake.finish().await.expect("ledger written");
    drop(lake);
    let mut live = LiveV3::start(&dir, RESTART).await;
    live.index(LAST).await;

    let created = pool_created(FIRST - 500, ANCIENT_POOL, 0);
    let topic = |topic: Option<B256>| format!("{:#x}", topic.expect("a topic"));
    let page = format!(
        r#"{{"data":[{{"blocks":[{{"number":{height},"hash":"{block}","timestamp":{timestamp}}}],
        "logs":[{{"block_hash":"{block}","block_number":{height},"transaction_index":0,
        "log_index":0,"transaction_hash":"{transaction}","address":"{address:#x}",
        "data":"{data}","topic0":"{topic0}","topic1":"{topic1}","topic2":"{topic2}",
        "topic3":"{topic3}"}}]}}],"next_block":{FIRST},"archive_height":{LAST}}}"#,
        height = created.block_number,
        block = created.block_hash,
        timestamp = created.block_timestamp,
        transaction = created.transaction_hash,
        address = created.address,
        data = created.data,
        topic0 = topic(created.topic0),
        topic1 = topic(created.topic1),
        topic2 = topic(created.topic2),
        topic3 = topic(created.topic3),
    );
    let (url, served) =
        crate::ingest::source::hypersync::testing::endpoint(vec![(200, page)]).await;
    let mut settings = dir.backfill(&protocols("uniswap/v3"));
    settings.run.hypersync =
        Some(toml::from_str(&format!("url = '{url}'\ntoken = 'token'")).expect("an endpoint"));
    let done = beside(&mut live, &dir, settings).await;
    assert_eq!((done.discovered, done.handed), (2, 2), "{done:?}");

    let asked = served.await.expect("served");
    assert_eq!(asked.len(), 1);
    let asked: serde_json::Value = serde_json::from_str(&asked[0]).expect("a query");
    assert_eq!(
        asked["to_block"], FIRST,
        "only below the lake's first block"
    );
    assert_eq!(
        asked["logs"][0]["address"],
        serde_json::json!([format!("{V3_FACTORY:#x}")]),
        "only the factory's logs"
    );

    let ancient = format!("address = '{ANCIENT_POOL:#x}'");
    assert_eq!(
        rows(&dir, "uniswap_v3_pool_swap", &ancient).await.len(),
        usize::try_from(RESTART - FIRST).expect("fits"),
        "every swap of it the lake holds"
    );
}

/// A backfill into another lake is a trial: it opens no database, not even one it
/// names, so nothing it writes reaches what the running indexer serves.
#[cfg(feature = "postgres")]
#[tokio::test]
async fn a_backfill_into_another_lake_opens_no_database() {
    let dir = Dir::new();
    let mut settings = dir.backfill(&uniswap_v4());
    settings.run.out = Some(Dir::new().uri());
    // A database nothing listens for, which a connection attempt fails on.
    settings.postgres = Some(
        toml::from_str("tables = [\"*\"]\nconnection_string = \"host=127.0.0.1 port=1\"")
            .expect("settings"),
    );
    let schema = Catalog::load(uniswap_v4(), &ChainId::new(CHAIN))
        .expect("loads")
        .schema()
        .clone();
    super::write::Database::open(&settings, &schema, &ChainId::new(CHAIN))
        .await
        .expect("no database opened");
}

/// A backfill of a subtree of the running indexer's manifests fills the tables of the
/// subtree its `[sink.postgres] tables` route, though most entries name other protocols'
/// tables: Robinhood's manifests, backfilling Uniswap, fill every routed Uniswap table.
#[cfg(feature = "postgres")]
#[test]
fn a_subtree_backfill_fills_the_tables_the_database_routes_of_it() {
    // The tables the route takes back out of `uniswap_v4_*`.
    const CLAIM_TOKENS: [&str; 3] = [
        "uniswap_v4_pool_manager_transfer",
        "uniswap_v4_pool_manager_approval",
        "uniswap_v4_pool_manager_operator_set",
    ];
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let running = crate::config::Settings::from_file(root.join("indexer.robinhood.toml"), stand_in)
        .expect("parses");
    let chain = ChainId::new(&running.ingest.chain);
    let uniswap = Catalog::load(root.join("protocols/uniswap"), &chain).expect("loads");
    let tables = serde_json::from_str(
        r#"["metric_v1_*", "chainlink_data_streams_*", "uniswap_v3_*", "uniswap_v4_*",
            "!uniswap_v4_pool_manager_transfer", "!uniswap_v4_pool_manager_approval",
            "!uniswap_v4_pool_manager_operator_set"]"#,
    )
    .expect("parses");
    let routed = super::write::routed(
        &tables,
        &running.protocols_path().expect("manifests"),
        &chain,
        uniswap.schema(),
    )
    .expect("routed");
    let events: std::collections::HashSet<_> = uniswap
        .schema()
        .tables()
        .iter()
        .filter(|def| matches!(def.id, crate::sink::table::TableId::Event(_)))
        .filter(|def| !CLAIM_TOKENS.contains(&def.name.as_ref()))
        .map(|def| def.id)
        .collect();
    assert!(!events.is_empty());
    assert_eq!(routed, events);
}
