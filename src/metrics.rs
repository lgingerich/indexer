//! Prometheus metrics: the numbers an operator graphs and alerts on.
//!
//! The layers record through the `metrics` facade's macros, which do nothing until a
//! recorder is installed, so a test, a bench, or a run without `[metrics]` pays nothing.
//! [`serve`] installs the Prometheus recorder and the HTTP listener it is scraped from
//! when the settings name one. Like [`crate::config`], this sits outside the layers: any
//! of them may record, and it depends on none of them.
//!
//! Every series carries a `chain` label, added once here, since a process indexes one
//! chain; per-store series also carry `store`, the `[sink.<name>]` that committed.
//!
//! | Series | Kind | What it answers |
//! | --- | --- | --- |
//! | `indexer_start_time_seconds` | gauge | When the process started; a change is a restart |
//! | `indexer_chain_head_height` | gauge | The newest head the node announced or was sampled at |
//! | `indexer_ingest_height` | gauge | The newest block ingest handed to the stores |
//! | `indexer_reorgs_total` | counter | Reorgs retracted |
//! | `indexer_reorg_orphaned_blocks_total` | counter | Blocks those reorgs orphaned |
//! | `indexer_store_height` | gauge | The newest block a store committed |
//! | `indexer_store_lag_blocks` | gauge | How far that block is under the sampled head |
//! | `indexer_store_last_commit_timestamp_seconds` | gauge | When a store last committed |
//! | `indexer_store_blocks_total` | counter | Blocks a store committed |
//! | `indexer_store_rows_total` | counter | Records a store committed |
//! | `indexer_store_commit_duration_seconds` | histogram | How long a store's commits take |
//! | `indexer_logs_decoded_total` | counter | Logs decoded against a contract's ABI |
//! | `indexer_decode_failures_total` | counter | Logs from a known contract that did not decode |
//! | `indexer_derive_failures_total` | counter | Decoded records a derived `table` could not use |
//! | `indexer_contracts_discovered_total` | counter | Contracts found through a factory's event |
//! | `indexer_rpc_retries_total` | counter | Source requests retried after a transient failure, by `source`: `rpc` or `hypersync` |
//! | `indexer_rpc_range_splits_total` | counter | Backfill ranges split after a failed fetch |
//! | `indexer_lake_maintenance_failures_total` | counter | Failed Delta `operation`s: compact or vacuum |
//!
//! A stall is `time() - indexer_store_last_commit_timestamp_seconds`; a decode failure
//! is any increase of the failure counters, since every one of them is a record the
//! stores are missing.

use std::net::SocketAddr;
#[cfg(any(feature = "store", test))]
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(any(feature = "store", test))]
use metrics::histogram;
use metrics::{counter, gauge};
use metrics_exporter_prometheus::{BuildError, Matcher, PrometheusBuilder};
use serde::Deserialize;

/// The upper bounds of [`STORE_COMMIT_DURATION`]'s buckets, in seconds: a Postgres commit
/// at the tip takes milliseconds, a lake commit during backfill tens of seconds.
const COMMIT_BUCKETS: [f64; 12] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 15.0, 60.0,
];

const START_TIME: &str = "indexer_start_time_seconds";
const CHAIN_HEAD: &str = "indexer_chain_head_height";
const INGEST_HEIGHT: &str = "indexer_ingest_height";
const REORGS: &str = "indexer_reorgs_total";
const REORG_ORPHANED: &str = "indexer_reorg_orphaned_blocks_total";
#[cfg(any(feature = "store", test))]
const STORE_HEIGHT: &str = "indexer_store_height";
#[cfg(any(feature = "store", test))]
const STORE_LAG: &str = "indexer_store_lag_blocks";
#[cfg(any(feature = "store", test))]
const STORE_LAST_COMMIT: &str = "indexer_store_last_commit_timestamp_seconds";
#[cfg(any(feature = "store", test))]
const STORE_BLOCKS: &str = "indexer_store_blocks_total";
#[cfg(any(feature = "store", test))]
const STORE_ROWS: &str = "indexer_store_rows_total";
const STORE_COMMIT_DURATION: &str = "indexer_store_commit_duration_seconds";
const LOGS_DECODED: &str = "indexer_logs_decoded_total";
const DECODE_FAILURES: &str = "indexer_decode_failures_total";
const DERIVE_FAILURES: &str = "indexer_derive_failures_total";
const CONTRACTS_DISCOVERED: &str = "indexer_contracts_discovered_total";
const RPC_RETRIES: &str = "indexer_rpc_retries_total";
const RPC_RANGE_SPLITS: &str = "indexer_rpc_range_splits_total";
#[cfg(feature = "delta")]
const LAKE_MAINTENANCE_FAILURES: &str = "indexer_lake_maintenance_failures_total";

/// Where the Prometheus endpoint listens: the `[metrics]` table.
///
/// ```toml
/// [metrics]
/// listen = "0.0.0.0:9100"
/// ```
///
/// Absent, nothing listens and nothing is recorded. The endpoint serves every path, so
/// a scraper's default `/metrics` works.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsSettings {
    /// The address and port to serve on. Required: a listener with no address is not one.
    pub listen: SocketAddr,
}

/// Installs the Prometheus recorder and serves it at `settings.listen`, labelling every
/// series with `chain`. Must run inside the Tokio runtime, which the listener runs on.
///
/// # Errors
///
/// Returns [`BuildError`] when the address cannot be bound or a recorder is already
/// installed.
pub fn serve(settings: &MetricsSettings, chain: &str) -> Result<(), BuildError> {
    PrometheusBuilder::new()
        .with_http_listener(settings.listen)
        .add_global_label("chain", chain)
        .set_buckets_for_metric(
            Matcher::Full(STORE_COMMIT_DURATION.to_owned()),
            &COMMIT_BUCKETS,
        )?
        .install()?;
    gauge!(START_TIME).set(unix_seconds(SystemTime::now()));
    Ok(())
}

/// The node's head was sampled at `height`.
pub(crate) fn chain_head(height: u64) {
    gauge!(CHAIN_HEAD).set(as_value(height));
}

/// Ingest handed the block at `height` to the stores.
pub(crate) fn ingested(height: u64) {
    gauge!(INGEST_HEIGHT).set(as_value(height));
}

/// A reorg retracted `orphaned` blocks.
pub(crate) fn reorg(orphaned: usize) {
    counter!(REORGS).increment(1);
    counter!(REORG_ORPHANED).increment(orphaned as u64);
}

/// `store` committed `blocks` blocks and `rows` records, up to `height`, in `took`.
/// `head` is the newest head ingest had sampled, if any. Only a store commits, so a
/// build without one has no caller outside the tests.
#[cfg(any(feature = "store", test))]
pub(crate) fn committed(
    store: &'static str,
    height: u64,
    blocks: u64,
    rows: u64,
    took: Duration,
    head: Option<u64>,
) {
    gauge!(STORE_HEIGHT, "store" => store).set(as_value(height));
    if let Some(head) = head {
        gauge!(STORE_LAG, "store" => store).set(as_value(head.saturating_sub(height)));
    }
    gauge!(STORE_LAST_COMMIT, "store" => store).set(unix_seconds(SystemTime::now()));
    counter!(STORE_BLOCKS, "store" => store).increment(blocks);
    counter!(STORE_ROWS, "store" => store).increment(rows);
    histogram!(STORE_COMMIT_DURATION, "store" => store).record(took.as_secs_f64());
}

/// `count` logs decoded against their contracts' ABIs.
pub(crate) fn logs_decoded(count: u64) {
    counter!(LOGS_DECODED).increment(count);
}

/// A log from a known contract did not decode, and only its raw record is kept.
pub(crate) fn decode_failed() {
    counter!(DECODE_FAILURES).increment(1);
}

/// A decoded record could not be turned into a row of the derived `table`.
pub(crate) fn derive_failed(table: &'static str) {
    counter!(DERIVE_FAILURES, "table" => table).increment(1);
}

/// `count` contracts were found through a factory's event.
pub(crate) fn contracts_discovered(count: usize) {
    counter!(CONTRACTS_DISCOVERED).increment(count as u64);
}

/// A request to `source`, the RPC node or `HyperSync`, failed transiently and is being
/// retried.
pub(crate) fn source_retry(source: &'static str) {
    counter!(RPC_RETRIES, "source" => source).increment(1);
}

/// A backfill range failed to fetch and is being fetched in halves.
pub(crate) fn rpc_range_split() {
    counter!(RPC_RANGE_SPLITS).increment(1);
}

/// A lake maintenance `operation` failed and will be retried next pass.
#[cfg(feature = "delta")]
pub(crate) fn lake_maintenance_failed(operation: &'static str) {
    counter!(LAKE_MAINTENANCE_FAILURES, "operation" => operation).increment(1);
}

/// A height or a count as a sample value. Prometheus samples are `f64`, which holds every
/// integer below 2^53 exactly: block heights and lags are far under it.
#[expect(
    clippy::cast_precision_loss,
    reason = "heights and lags are far below 2^53, where f64 stops being exact"
)]
const fn as_value(value: u64) -> f64 {
    value as f64
}

/// Seconds since the Unix epoch, as Prometheus timestamps are kept. A clock before the
/// epoch reads as zero rather than failing a metric.
fn unix_seconds(at: SystemTime) -> f64 {
    at.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::time::Duration;

    use metrics_exporter_prometheus::PrometheusBuilder;

    use super::*;

    /// What a scrape shows: every per-store series labelled with its store, and the
    /// commit duration as a true histogram rather than a summary.
    #[test]
    fn a_commit_is_rendered_per_store_with_a_histogram() {
        let recorder = PrometheusBuilder::new()
            .add_global_label("chain", "bsc")
            .set_buckets_for_metric(
                Matcher::Full(STORE_COMMIT_DURATION.to_owned()),
                &COMMIT_BUCKETS,
            )
            .expect("buckets")
            .build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            chain_head(110);
            committed("postgres", 100, 3, 42, Duration::from_millis(30), Some(110));
            derive_failed("metric_v1_swaps");
        });
        let scraped = handle.render();
        for line in [
            r#"indexer_chain_head_height{chain="bsc"} 110"#,
            r#"indexer_store_height{chain="bsc",store="postgres"} 100"#,
            r#"indexer_store_lag_blocks{chain="bsc",store="postgres"} 10"#,
            r#"indexer_store_rows_total{chain="bsc",store="postgres"} 42"#,
            r#"indexer_store_commit_duration_seconds_bucket{chain="bsc",store="postgres",le="0.05"} 1"#,
            r#"indexer_derive_failures_total{chain="bsc",table="metric_v1_swaps"} 1"#,
        ] {
            assert!(scraped.contains(line), "missing {line} in:\n{scraped}");
        }
    }
}
