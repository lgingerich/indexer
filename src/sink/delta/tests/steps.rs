//! A commit stopped before each of its [`Step`]s: the next open repairs what it left to
//! a lake every table agrees on, and the restart carries on from there to the lake the
//! whole commit would have left.

use super::*;
use crate::sink::delta::{Step, Stopped};

/// The step a test stopped the write before, when `error` is that stop and not a
/// genuine failure.
fn stopped(error: &StoreError) -> Option<Step> {
    match error {
        StoreError::Engine { source, .. } => {
            source.downcast_ref::<Stopped>().map(|Stopped(step)| *step)
        }
        _ => None,
    }
}

/// The commit every test stops: a fork replaces committed block 3 and adds block 4, so
/// each step has something to write.
fn replacement() -> Vec<Envelope> {
    let mut envelopes = vec![reorg(3..=3, 1)];
    envelopes.extend(block(3, 1));
    envelopes.extend(block(4, 1));
    envelopes
}

/// Commits blocks 1 to 3, stops the [`replacement`] before `step`, and reopens the lake,
/// which repairs what the stop left. The stopped store refuses every later write.
async fn stopped_before(step: Step, dir: &Dir) -> DeltaSink {
    let mut sink = open(dir).await;
    for height in 1..=3 {
        publish(&mut sink, block(height, 0)).await;
    }
    sink.stop_before = Some(step);
    buffer(&mut sink, replacement()).await;
    match sink.flush().await {
        Err(SinkError::Store(error)) => assert_eq!(stopped(&error), Some(step), "{error:?}"),
        other => panic!("the commit runs on past {step:?}: {other:?}"),
    }
    assert!(matches!(
        sink.publish(block(5, 1).remove(0)).await,
        Err(SinkError::Store(StoreError::Failed))
    ));
    drop(sink);
    open(dir).await
}

/// Carries on as the restart does, from the ledger the repair left: the reorg again if
/// the ledger still names the orphan, the fork's blocks alone if not. The lake then
/// holds what the whole commit would have, the retraction recorded once.
async fn carry_on(mut sink: DeltaSink, dir: &Dir) {
    let named = ledger(&mut sink)
        .await
        .iter()
        .any(|block| block.hash == hash(3, 0));
    let mut envelopes = replacement();
    if !named {
        envelopes.remove(0);
    }
    publish(&mut sink, envelopes).await;
    drop(sink);

    let mut sink = open(dir).await;
    assert_eq!(
        blocks(&sink).await,
        expected(&[(1, 0), (2, 0), (3, 1), (4, 1)])
    );
    assert_eq!(ledger_heights(&mut sink).await, [1, 2, 3, 4]);
    assert_eq!(retractions(&sink).await, expected(&[(3, 1)]));
    assert_eq!(rows(&sink, Table::Reorg).await, 1, "recorded once");
}

/// Stopped before its first step, the commit left nothing.
#[tokio::test]
async fn a_commit_stopped_before_recording_its_retractions_left_nothing() {
    let dir = Dir::new();
    let mut sink = stopped_before(Step::RecordRetractions, &dir).await;
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 0)]));
    assert!(retractions(&sink).await.is_empty());
    assert_eq!(ledger_heights(&mut sink).await, [1, 2, 3]);
    carry_on(sink, &dir).await;
}

/// Stopped once the retraction is recorded but its orphan still in the ledger, the
/// record is repaired away, and the restart, still on the orphan, records it again.
#[tokio::test]
async fn a_commit_stopped_before_deleting_from_the_ledger_loses_its_record() {
    let dir = Dir::new();
    let mut sink = stopped_before(Step::DeleteFromLedger, &dir).await;
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 0)]));
    assert!(retractions(&sink).await.is_empty());
    assert_eq!(ledger_heights(&mut sink).await, [1, 2, 3]);
    carry_on(sink, &dir).await;
}

/// Stopped once the orphan is out of the ledger, its rows elsewhere are repaired away,
/// and the record kept: the restart resumes below the fork, with nothing to retract.
#[tokio::test]
async fn a_commit_stopped_before_deleting_from_the_rest_is_finished_by_the_repair() {
    let dir = Dir::new();
    let mut sink = stopped_before(Step::DeleteFromRest, &dir).await;
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0)]));
    assert_eq!(retractions(&sink).await, expected(&[(3, 1)]));
    assert_eq!(ledger_heights(&mut sink).await, [1, 2]);
    carry_on(sink, &dir).await;
}

/// Stopped once the retraction is whole, the commit left nothing to repair.
#[tokio::test]
async fn a_commit_stopped_before_appending_its_rows_left_a_whole_retraction() {
    let dir = Dir::new();
    let mut sink = stopped_before(Step::AppendRows, &dir).await;
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0)]));
    assert_eq!(retractions(&sink).await, expected(&[(3, 1)]));
    assert_eq!(ledger_heights(&mut sink).await, [1, 2]);
    carry_on(sink, &dir).await;
}

/// Stopped with every row appended but none in the ledger, the rows are repaired away,
/// and the restart fetches them again.
#[tokio::test]
async fn a_commit_stopped_before_its_ledger_append_loses_its_rows() {
    let dir = Dir::new();
    let mut sink = stopped_before(Step::AppendLedger, &dir).await;
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0)]));
    assert_eq!(retractions(&sink).await, expected(&[(3, 1)]));
    assert_eq!(ledger_heights(&mut sink).await, [1, 2]);
    carry_on(sink, &dir).await;
}

/// A rewind stopped between its two deletes, its blocks out of the ledger but not out
/// of the rest, is finished by the repair.
#[tokio::test]
async fn a_rewind_stopped_before_deleting_from_the_rest_is_finished_by_the_repair() {
    let dir = Dir::new();
    let mut sink = open(&dir).await;
    for height in 1..=5 {
        publish(&mut sink, block(height, 0)).await;
    }
    sink.stop_before = Some(Step::DeleteFromRest);
    let error = sink.rewind(3).await.expect_err("the rewind is stopped");
    assert_eq!(stopped(&error), Some(Step::DeleteFromRest), "{error:?}");
    drop(sink);

    let mut sink = open(&dir).await;
    assert_eq!(blocks(&sink).await, expected(&[(1, 0), (2, 0), (3, 0)]));
    assert_eq!(ledger_heights(&mut sink).await, [1, 2, 3]);
}
