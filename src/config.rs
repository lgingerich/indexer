//! The indexer's settings file, and the builders each stage takes.
//!
//! Configuration is a TOML file read once at startup. A setting either has a default in
//! this module or is required — there is no third case — so a missing required value is
//! a startup error naming the field rather than a failure somewhere inside a client.
//!
//! # Defaults
//!
//! Defaulted means "correct for the usual single-chain deployment": the topic names, the
//! store path, and the batch sizes. Overriding one is for a non-standard topology, not a
//! routine step.
//!
//! Required means there is no value that could be right by accident: an endpoint, a
//! broker address, or a chain. Guessing any of those produces a process that starts,
//! looks healthy, and indexes the wrong thing.
//!
//! # Example
//!
//! ```toml
//! [ingest]
//! chain = "base"
//! http_url = "https://base-rpc.publicnode.com"
//! ws_url = "wss://base-rpc.publicnode.com"
//!
//! [kafka]
//! brokers = "localhost:9092"
//!
//! [storage]
//! database = "indexer.duckdb"
//! ```
//!
//! Every field that is not required above may be omitted.
//!
//! # Why one file rather than the environment
//!
//! A settings file is reviewable: it is checked in, it diffs per deployment, and its
//! shape is one place rather than nine lookups scattered through a `main`. A typo is a
//! parse error naming a line, where an unset variable names itself only if the code
//! happens to check it.

use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

/// The whole settings file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    /// Ingest's settings. Absent means run without ingest.
    #[serde(default)]
    pub ingest: Option<IngestSettings>,
    /// The broker every stage connects to. Required.
    pub kafka: KafkaSettings,
    /// Where decoded records are persisted. Optional: absent means the defaults below.
    #[serde(default)]
    pub storage: StorageSettings,
}

/// How ingest follows a chain.
///
/// Required where present, because a chain with no endpoint cannot be followed and a
/// default would silently index nothing.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestSettings {
    /// The chain id stamped on every event.
    pub chain: String,
    /// The JSON-RPC endpoint used for blocks, receipts, and the finalized block.
    pub http_url: String,
    /// The WebSocket endpoint used for heads.
    pub ws_url: String,
    /// The topic ingest publishes to. Defaults to [`DEFAULT_RAW_TOPIC`].
    ///
    /// Ingest's output and decode's input are the same topic, so changing this means
    /// changing both. That is why the default exists and why overriding it is a
    /// deliberate act rather than a routine one.
    #[serde(default = "default_raw_topic")]
    pub raw_topic: String,
    /// Print to stdout instead of publishing, for watching the raw stream.
    ///
    /// A debug mode rather than a deployment setting: it changes where ingest writes,
    /// not what the process is.
    #[serde(default)]
    pub stdout: bool,
}

/// How to reach the broker, and the topics the stages share.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaSettings {
    /// Comma-separated `host:port` bootstrap servers.
    pub brokers: String,
    /// The topic decode writes and storage reads.
    #[serde(default = "default_decoded_topic")]
    pub decoded_topic: String,
    /// The consumer group prefix. Each stage appends its own name, and storage appends
    /// the topic, because an offset is per group.
    #[serde(default = "default_group_prefix")]
    pub group_prefix: String,
    /// How many records to accumulate before flushing, per stage.
    #[serde(default = "default_batch_records")]
    pub batch_records: usize,
    /// How long to wait before flushing a partial batch, in milliseconds.
    #[serde(default = "default_batch_ms")]
    pub batch_ms: u64,
    /// Any other librdkafka property, passed straight through.
    ///
    /// librdkafka has well over a hundred properties — `security.protocol`, `sasl.*`,
    /// `compression.type`, `message.timeout.ms`, `enable.idempotence` — and restating
    /// them here would be a second, stale copy of its documentation. Anything set here
    /// reaches [`rdkafka::ClientConfig::set`] on both the producer and the consumer, and
    /// an unrecognized key is an error from librdkafka naming the property.
    ///
    /// These apply to every Kafka client the process builds, so a property that means
    /// different things to a producer and a consumer — `auto.offset.reset` is the usual
    /// one — is better left unset here than set globally.
    ///
    /// ```toml
    /// [kafka.properties]
    /// security.protocol = "SASL_SSL"
    /// compression.type = "zstd"
    /// ```
    #[serde(default)]
    pub properties: std::collections::BTreeMap<String, String>,
}

/// Where decoded records are stored, and what the decode stage decodes.
///
/// No `#[serde(default)]` on the struct: that would fill an absent table from
/// `Default::default()`, which for a `PathBuf` is empty and would bypass the per-field
/// defaults below. Listing the table at all is optional via [`Settings`].
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageSettings {
    /// The `DuckDB` database to write.
    #[serde(default = "default_database")]
    pub database: PathBuf,
    /// How long a topic may be idle before storage treats it as drained, in seconds.
    ///
    /// Absent means never stop, which is what a live stream needs: a stage that exits
    /// when a topic goes quiet would take the process down with it. Set it for a bounded
    /// run — a backfill, a test — not for a live indexer.
    #[serde(default)]
    pub drain_secs: Option<u64>,
    /// The directory ABIs are discovered in.
    ///
    /// Every `{chain}.{address}.json` file in it is loaded, so adding a contract is
    /// dropping a file in rather than editing this one. Absent means the default, and a
    /// directory that does not exist means nothing is decoded — which is said at startup
    /// rather than being silent.
    #[serde(default = "default_abi_dir")]
    pub abi_dir: PathBuf,
    /// Any other `DuckDB` setting, passed straight through.
    ///
    /// `DuckDB` accepts dozens of settings and this file does not restate them. Anything
    /// here reaches [`duckdb::Config::with`], which validates it, so a misspelled key is
    /// an error from the engine naming the setting rather than a silent no-op.
    ///
    /// ```toml
    /// [storage.duckdb]
    /// threads = "4"
    /// max_memory = "1GB"
    /// ```
    #[serde(default)]
    pub duckdb: std::collections::BTreeMap<String, String>,
}

impl Default for StorageSettings {
    /// The same values the field-level `default` functions supply, so an absent
    /// `[storage]` table and an empty one agree.
    fn default() -> Self {
        Self {
            database: default_database(),
            drain_secs: None,
            abi_dir: default_abi_dir(),
            duckdb: std::collections::BTreeMap::new(),
        }
    }
}

impl Settings {
    /// Reads settings from a TOML file.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, or when it does not parse. A
    /// malformed file reports its line and column, and an unknown field is an error
    /// rather than being ignored, so a misspelled key is caught at startup instead of
    /// silently leaving a setting at its default.
    pub fn from_file(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|error| {
            anyhow::anyhow!("cannot read settings at {}: {error}", path.display())
        })?;
        toml::from_str(&text)
            .map_err(|error| anyhow::anyhow!("invalid settings at {}:\n{error}", path.display()))
    }

    /// The batch configuration the stages share.
    #[must_use]
    pub const fn batch(&self) -> BatchConfig {
        BatchConfig::new(
            self.kafka.batch_records,
            Duration::from_millis(self.kafka.batch_ms),
        )
    }

    /// The drain bound, if one is set.
    #[must_use]
    pub const fn drain(&self) -> Option<Duration> {
        match self.storage.drain_secs {
            Some(secs) => Some(Duration::from_secs(secs)),
            None => None,
        }
    }

    /// The topic ingest publishes to.
    #[must_use]
    pub fn raw_topic(&self) -> &str {
        self.ingest
            .as_ref()
            .map_or(DEFAULT_RAW_TOPIC, |ingest| ingest.raw_topic.as_str())
    }

    /// A librdkafka client config with the broker address and any passthrough applied.
    ///
    /// Both the producer and the consumer come from here, so a property set once in
    /// `[kafka.properties]` reaches every client rather than being duplicated per stage.
    #[cfg(feature = "kafka")]
    #[must_use]
    pub fn client_config(&self) -> rdkafka::ClientConfig {
        let mut config = rdkafka::ClientConfig::new();
        config.set("bootstrap.servers", &self.kafka.brokers);
        for (key, value) in &self.kafka.properties {
            config.set(key, value);
        }
        config
    }
}

/// The default topic ingest publishes to, and decode consumes.
pub const DEFAULT_RAW_TOPIC: &str = "raw.chain";

/// The default topic decode publishes to, and storage consumes.
pub const DEFAULT_DECODED_TOPIC: &str = "decoded.chain";

/// The default `DuckDB` path.
pub const DEFAULT_DATABASE: &str = "indexer.duckdb";

/// Where ABIs are discovered by default.
pub const DEFAULT_ABI_DIR: &str = "abis";

fn default_raw_topic() -> String {
    DEFAULT_RAW_TOPIC.to_owned()
}

fn default_decoded_topic() -> String {
    DEFAULT_DECODED_TOPIC.to_owned()
}

fn default_group_prefix() -> String {
    "indexer".to_owned()
}

const fn default_batch_records() -> usize {
    500
}

const fn default_batch_ms() -> u64 {
    1_000
}

fn default_database() -> PathBuf {
    PathBuf::from(DEFAULT_DATABASE)
}

fn default_abi_dir() -> PathBuf {
    PathBuf::from(DEFAULT_ABI_DIR)
}

/// Where a Kafka-protocol client connects, and which topics it uses.
///
/// Built from [`Settings`] rather than read from anywhere itself: a stage takes a value,
/// so it stays constructible in a test with no file and no environment.
#[cfg(feature = "kafka")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaConfig {
    /// Comma-separated `host:port` bootstrap servers.
    pub brokers: String,
    /// The consumer group this stage's offsets are committed under.
    ///
    /// An offset is per group, so a stage that reads two topics needs two groups; the
    /// builders take a prefix and derive them.
    pub group: String,
    /// The topic this stage reads.
    pub input_topic: String,
    /// The topic this stage writes, where it has one.
    pub output_topic: Option<String>,
}

#[cfg(feature = "kafka")]
impl KafkaConfig {
    /// Starts a configuration with the broker address.
    #[must_use]
    pub fn builder(brokers: impl Into<String>) -> KafkaConfigBuilder {
        KafkaConfigBuilder::new(brokers)
    }
}

/// Builds a [`KafkaConfig`].
#[cfg(feature = "kafka")]
#[derive(Debug, Clone)]
pub struct KafkaConfigBuilder {
    brokers: String,
    group: Option<String>,
    input_topic: Option<String>,
    output_topic: Option<String>,
}

#[cfg(feature = "kafka")]
impl KafkaConfigBuilder {
    /// Starts a build against `brokers`.
    #[must_use]
    pub fn new(brokers: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
            group: None,
            input_topic: None,
            output_topic: None,
        }
    }

    /// Sets the consumer group.
    #[must_use]
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Sets the topic this stage reads.
    #[must_use]
    pub fn input_topic(mut self, topic: impl Into<String>) -> Self {
        self.input_topic = Some(topic.into());
        self
    }

    /// Sets the topic this stage writes.
    #[must_use]
    pub fn output_topic(mut self, topic: impl Into<String>) -> Self {
        self.output_topic = Some(topic.into());
        self
    }

    /// Finishes the build.
    ///
    /// # Errors
    ///
    /// Returns an error when a required field is unset, naming it. A stage that started
    /// with a missing group id would fail later and less clearly, from inside the
    /// client.
    pub fn build(self) -> anyhow::Result<KafkaConfig> {
        Ok(KafkaConfig {
            brokers: self.brokers,
            group: self
                .group
                .ok_or_else(|| anyhow::anyhow!("kafka group is required"))?,
            input_topic: self
                .input_topic
                .ok_or_else(|| anyhow::anyhow!("kafka input_topic is required"))?,
            output_topic: self.output_topic,
        })
    }
}

/// How a stage batches before flushing.
///
/// Shared by every stage on the bus, because the trade is the same everywhere: a larger
/// batch amortizes the transport's per-request cost, and the time bound is what stops a
/// quiet topic from leaving records unflushed — which matters because an unflushed
/// record is an uncommitted offset, and so a record that will be replayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchConfig {
    /// Records to accumulate before flushing.
    pub records: usize,
    /// How long to wait before flushing a partial batch.
    pub every: Duration,
}

impl BatchConfig {
    /// A batch of `records` or `every`, whichever comes first.
    #[must_use]
    pub const fn new(records: usize, every: Duration) -> Self {
        Self { records, every }
    }
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self::new(500, Duration::from_secs(1))
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::time::Duration;

    use super::{DEFAULT_DATABASE, DEFAULT_DECODED_TOPIC, DEFAULT_RAW_TOPIC, Settings};

    /// The minimum a file needs: a broker and, to ingest, a chain and its endpoints.
    fn minimal() -> &'static str {
        r#"
[ingest]
chain = "base"
http_url = "https://example.invalid"
ws_url = "wss://example.invalid"

[kafka]
brokers = "localhost:9092"
"#
    }

    /// Everything not required takes a default, so a small file runs a standard
    /// deployment without listing the obvious.
    #[test]
    fn a_minimal_file_supplies_every_default() {
        let settings: Settings = toml::from_str(minimal()).expect("minimal settings parse");

        let ingest = settings.ingest.as_ref().expect("ingest is present");
        assert_eq!(ingest.raw_topic, DEFAULT_RAW_TOPIC);
        assert!(!ingest.stdout, "stdout is off unless asked for");

        assert_eq!(settings.kafka.decoded_topic, DEFAULT_DECODED_TOPIC);
        assert_eq!(settings.kafka.group_prefix, "indexer");
        assert_eq!(
            settings.batch(),
            super::BatchConfig::new(500, Duration::from_secs(1))
        );

        assert_eq!(
            settings.storage.database,
            std::path::PathBuf::from(DEFAULT_DATABASE)
        );
        assert_eq!(
            settings.storage.abi_dir,
            std::path::PathBuf::from(super::DEFAULT_ABI_DIR)
        );
        assert_eq!(
            settings.drain(),
            None,
            "no drain bound by default, because a live stream must not stop"
        );
    }

    /// A broker is required: no default could be right, and guessing one produces a
    /// process that starts and connects to nothing.
    #[test]
    fn a_missing_broker_is_an_error() {
        let result: Result<Settings, _> = toml::from_str("[storage]\ndatabase = \"x\"\n");
        assert!(result.is_err(), "kafka is required");
    }

    /// So is an endpoint once ingest is configured: a chain with nowhere to read is
    /// not a deployable state.
    #[test]
    fn a_missing_endpoint_is_an_error() {
        let result: Result<Settings, _> = toml::from_str(
            r#"
[ingest]
chain = "base"

[kafka]
brokers = "localhost:9092"
"#,
        );
        assert!(result.is_err(), "http_url and ws_url are required");
    }

    /// An unknown key is an error rather than ignored, so a misspelling is caught at
    /// startup instead of leaving a setting silently at its default.
    #[test]
    fn an_unknown_key_is_rejected() {
        let result: Result<Settings, _> = toml::from_str(
            r#"
[kafka]
brokers = "localhost:9092"
brokres = "typo"
"#,
        );
        let error = result.expect_err("a typo must not be ignored").to_string();
        assert!(
            error.contains("brokres"),
            "the error names the key: {error}"
        );
    }

    /// Ingest is optional, so decode and storage can run against a topic filled
    /// elsewhere.
    #[test]
    fn ingest_may_be_absent() {
        let settings: Settings =
            toml::from_str("[kafka]\nbrokers = \"localhost:9092\"\n").expect("settings parse");
        assert!(settings.ingest.is_none());
        // The raw topic still has a name, so decode and storage agree on it.
        assert_eq!(settings.raw_topic(), DEFAULT_RAW_TOPIC);
    }

    /// A drain bound is opt-in, and reaches the stage as a duration.
    #[test]
    fn a_drain_bound_is_opt_in() {
        let settings: Settings = toml::from_str(
            r#"
[kafka]
brokers = "localhost:9092"

[storage]
drain_secs = 5
"#,
        )
        .expect("settings parse");
        assert_eq!(settings.drain(), Some(Duration::from_secs(5)));
    }

    /// Inputs the operator must choose are taken as written, not defaulted.
    #[test]
    fn explicit_values_win() {
        let settings: Settings = toml::from_str(
            r#"
[ingest]
chain = "ethereum"
http_url = "https://example.invalid"
ws_url = "wss://example.invalid"
raw_topic = "custom.raw"

[kafka]
brokers = "broker:9092"
decoded_topic = "custom.decoded"
group_prefix = "team-x"
batch_records = 50
batch_ms = 250

[storage]
database = "/tmp/custom.duckdb"
drain_secs = 3
abi_dir = "custom-abis"
"#,
        )
        .expect("settings parse");

        assert_eq!(settings.raw_topic(), "custom.raw");
        assert_eq!(settings.kafka.decoded_topic, "custom.decoded");
        assert_eq!(settings.kafka.group_prefix, "team-x");
        assert_eq!(
            settings.batch(),
            super::BatchConfig::new(50, Duration::from_millis(250))
        );
        assert_eq!(
            settings.storage.database,
            std::path::PathBuf::from("/tmp/custom.duckdb")
        );
        assert_eq!(settings.drain(), Some(Duration::from_secs(3)));
        assert_eq!(
            settings.storage.abi_dir,
            std::path::PathBuf::from("custom-abis")
        );
    }
}
