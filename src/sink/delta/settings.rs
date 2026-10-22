//! The Delta Lake store's settings, as `[sink.delta]` reads them.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Deserialize;

use super::arrow;
use crate::ingest::pipeline::LEDGER_WINDOW;
use crate::secret::Secret;
use crate::sink::channel::Batching;
use crate::sink::route::Tables;

/// The Delta Lake store's settings: where the tables live, how often to commit, and the
/// object store's own options, passed through.
///
/// ```toml
/// [sink.delta]
/// tables = ["blocks", "transactions", "logs"]
/// uri = "s3://indexer-lake"          # or a local directory
/// commit_interval_secs = 30
/// max_buffer_bytes = 268435456
/// maintenance = true
/// vacuum_retention_hours = 168
/// partition_blocks = 100000
///
/// [sink.delta.storage]               # passed to the object store
/// endpoint = "https://s3.example.com"  # omit for AWS S3
/// region = "us-east-1"
/// access_key_id = { env = "S3_ACCESS_KEY_ID" }
/// secret_access_key = { env = "S3_SECRET_ACCESS_KEY" }
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaSettings {
    /// The tables this store writes. Required.
    pub tables: Tables,
    /// Where the tables live: an `s3://bucket/prefix` URL, or a local directory. Each
    /// chain's tables go under `<uri>/<chain>/`. Required: there is no location that is
    /// right by default.
    #[serde(deserialize_with = "crate::config::non_empty")]
    pub uri: String,
    /// The longest a block waits in the buffer before a commit, in seconds, counted from
    /// when it arrived; a stalled chain still commits on time. Defaults to 30.
    ///
    /// At the tip this sets the file size: shorter puts rows in the lake sooner and makes
    /// more, smaller files.
    #[serde(default = "default_commit_interval_secs")]
    pub commit_interval_secs: u64,
    /// How many bytes of rows, roughly, may be buffered before a commit is made early.
    /// Defaults to 256 MiB. Reached while catching up, where it sets the file size.
    ///
    /// Counts the rows as they sit in memory. A commit builds each table's Arrow batch
    /// from them while they are still held, so it briefly needs as much again.
    #[serde(default = "default_max_buffer_bytes")]
    pub max_buffer_bytes: usize,
    /// Whether the store compacts and vacuums its own tables in the background. Defaults
    /// to true; turn it off only when another job does both.
    ///
    /// Compaction rewrites each partition the tip has left behind into a few large
    /// files, once. Vacuum deletes files no commit has referenced for
    /// [`vacuum_retention_hours`](Self::vacuum_retention_hours), once a day.
    #[serde(default = "default_maintenance")]
    pub maintenance: bool,
    /// How long a file no commit references is kept before vacuum deletes it, in hours:
    /// longer than any reader of an older version needs. Defaults to 168, a week. At
    /// least an hour is always kept, so a commit still writing its files is never
    /// vacuumed under it.
    #[serde(default = "default_vacuum_retention_hours")]
    pub vacuum_retention_hours: u64,
    /// How many block heights one partition covers. Defaults to 100,000: about 2.8 hours
    /// of Robinhood, half a day of BSC, two weeks of Ethereum. At least 4,097, the
    /// ledger window: opening reads the ledger from its newest two partitions.
    ///
    /// Narrower partitions make a restart and a reorg's delete read less, at the cost of
    /// more of them. The tip writes a small file into the newest partition every commit
    /// until compaction, so a chain whose partition fills in weeks wants a narrower one.
    ///
    /// Each table records the width it was written with, and a run whose setting differs
    /// is refused at open: rows stay in the partition they were written to, so changing
    /// the width of an existing lake means rewriting its tables.
    #[serde(
        default = "default_partition_blocks",
        deserialize_with = "partition_blocks"
    )]
    pub partition_blocks: u64,
    /// S3 options, passed straight through, such as `endpoint`, `region`, and
    /// `access_key_id`; the `aws_`-prefixed spellings work too. Every value is a
    /// [`Secret`], so keys can be named by environment variable and none is logged.
    /// Credentials may also come from the standard `AWS_*` environment variables.
    #[serde(default)]
    pub storage: BTreeMap<String, Secret>,
}

impl DeltaSettings {
    /// When the storage drain commits: once the oldest buffered block has waited
    /// [`commit_interval_secs`](Self::commit_interval_secs), or once
    /// [`max_buffer_bytes`](Self::max_buffer_bytes) are buffered, whichever is first.
    pub(crate) fn batching(&self) -> Batching {
        Batching {
            records: usize::MAX,
            bytes: self.max_buffer_bytes,
            wait: Duration::from_secs(self.commit_interval_secs),
        }
    }
}

const fn default_commit_interval_secs() -> u64 {
    30
}

const fn default_max_buffer_bytes() -> usize {
    256 * 1024 * 1024
}

const fn default_maintenance() -> bool {
    true
}

const fn default_vacuum_retention_hours() -> u64 {
    168
}

const fn default_partition_blocks() -> u64 {
    arrow::DEFAULT_PARTITION_BLOCKS
}

/// A partition width no narrower than the ledger window, so the newest two partitions
/// always hold the whole of it.
fn partition_blocks<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    let width = u64::deserialize(deserializer)?;
    if width < LEDGER_WINDOW as u64 {
        return Err(serde::de::Error::custom(format!(
            "partition_blocks is at least {LEDGER_WINDOW}, the ledger window"
        )));
    }
    Ok(width)
}
