//! The indexer's settings file, and the builders each stage takes.
//!
//! Configuration is a TOML file read once at startup. A setting either has a default in
//! this module or is required — there is no third case — so a missing required value is
//! a startup error naming the field rather than a failure somewhere inside a client.
//!
//! # Required vs defaulted
//!
//! A required field is one with no `#[serde(default)]`: serde refuses a document that
//! cannot fill it, naming the field, so absence is enforced by the type rather than
//! re-checked by hand. A required `String` also carries `deserialize_with = "non_empty"`,
//! because a present empty value — `brokers = ""` — is not absence and would otherwise
//! start a process that connects to nothing. A defaulted field carries
//! `#[serde(default)]`: the value's `Default` when that is the right one (`PathBuf`,
//! `BTreeMap`, an `Option` that means "unset"), or a `default_*` function when it is not.
//!
//! Required means no value could be right by accident. An endpoint, a broker address, or
//! a chain name that is guessed produces a process that starts, looks healthy, and
//! indexes the wrong thing, so the field is simply absent from the defaults.
//!
//! # Grouping
//!
//! A table names what owns its fields, not where a field was first needed:
//!
//! - `[bus]` — the transport (`kind`), the topics, and the group prefix. A topic is a bus
//!   concept, not a Kafka one, so the same names apply under either transport. The
//!   broker's own settings live under `[bus.kafka]`, needed only when `kind = "kafka"`.
//! - `[runtime]` — how every stage drains: the batch size and time bound, and the drain
//!   bound. Transport-independent, so it is not under `[bus]`.
//! - `[storage.<kind>]` — one backend's own settings, chosen by `[storage] kind`.
//!   Backend-specific keys stay out of the generic table so a second backend is an
//!   addition rather than an ambiguity about which `database` is meant.
//!
//! # Example
//!
//! ```toml
//! [ingest]
//! chain = "base"
//! http_url = "https://base-rpc.publicnode.com"
//! ws_url = "wss://base-rpc.publicnode.com"
//!
//! [bus]
//! kind = "kafka"   # or "memory" for a single, broker-less process
//!
//! [bus.kafka]
//! brokers = "localhost:9092"
//!
//! [runtime]
//! drain_secs = 5   # omit for a live indexer
//!
//! [storage]
//! kind = "duckdb"
//!
//! [storage.duckdb]
//! path = "indexer.duckdb"
//!
//! [decode]
//! registry = "registry.toml"   # the contract catalog; optional
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

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use serde::de::{self, Unexpected};

/// Deserializes a required string, refusing an empty or whitespace-only one.
///
/// A `#[serde(default)]`-less field already makes absence an error, but a present empty
/// value is not absence: `brokers = ""` or `chain = ""` is a deployment that starts and
/// connects to nothing, or stamps every event with a blank chain. Putting the check on
/// the field keeps it with the field's documentation and lets serde name it, rather than
/// a hand-written pass over every required key after the fact.
fn non_empty<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value.trim().is_empty() {
        return Err(de::Error::invalid_value(
            Unexpected::Str(&value),
            &"a non-empty value",
        ));
    }
    Ok(value)
}

/// The whole settings file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    /// Ingest's settings. Absent means run without ingest.
    #[serde(default)]
    pub ingest: Option<IngestSettings>,
    /// The bus the stages share: the transport, the topics, the group prefix.
    #[serde(default)]
    pub bus: BusSettings,
    /// How every stage batches, and when a bounded run stops.
    #[serde(default)]
    pub runtime: RuntimeSettings,
    /// Where decoded records are persisted.
    #[serde(default)]
    pub storage: StorageSettings,
    /// What the decode stage decodes.
    #[serde(default)]
    pub decode: DecodeSettings,
    /// The directory the settings file lives in, which a relative path in it resolves
    /// against. Filled by [`Settings::from_file`]; empty for text parsed from elsewhere,
    /// which leaves a relative path as written. Not a file key, so a `dir` in the file is
    /// still reported as a typo.
    #[serde(skip)]
    dir: PathBuf,
}

/// How ingest follows a chain.
///
/// `chain`, `http_url`, and `ws_url` are required — a chain with no endpoint cannot be
/// followed, and a default would silently index nothing — so they have no serde default
/// and serde refuses a table that omits them, naming the field. `stdout` is a debug mode
/// rather than a deployment setting: it changes where ingest writes, not what the process
/// is.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestSettings {
    /// The chain id stamped on every event. Required, and non-empty.
    #[serde(deserialize_with = "non_empty")]
    pub chain: String,
    /// The JSON-RPC endpoint used for blocks, receipts, and the finalized block.
    /// Required, and non-empty.
    #[serde(deserialize_with = "non_empty")]
    pub http_url: String,
    /// The WebSocket endpoint used for heads. Required, and non-empty.
    #[serde(deserialize_with = "non_empty")]
    pub ws_url: String,
    /// Print to stdout instead of publishing, for watching the raw stream.
    #[serde(default)]
    pub stdout: bool,
}

/// The bus the stages share: which transport, which topics, which group prefix.
///
/// A topic is a bus concept, not a Kafka one, which is why the topics live here and not
/// under [`KafkaSettings`]: the same names apply whichever transport carries them.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BusSettings {
    /// The transport the stages share. Absent means `kafka`.
    #[serde(default)]
    pub kind: BusKind,
    /// The topic ingest publishes to, and decode consumes. Defaults to
    /// [`DEFAULT_RAW_TOPIC`].
    ///
    /// Ingest's output and decode's input are the same topic, so changing this means
    /// changing both. That is why the default exists and why overriding it is a
    /// deliberate act rather than a routine one.
    #[serde(default = "default_raw_topic")]
    pub raw_topic: String,
    /// The topic decode writes and storage reads. Defaults to [`DEFAULT_DECODED_TOPIC`].
    #[serde(default = "default_decoded_topic")]
    pub decoded_topic: String,
    /// The consumer group prefix. Each stage appends its own name, and storage appends
    /// the topic, because an offset is per group.
    ///
    /// The in-memory bus has no offsets to keep, so the prefix is unused there; it is
    /// still named so one settings file reads the same under either transport.
    #[serde(default = "default_group_prefix")]
    pub group_prefix: String,
    /// The broker's own settings, when `kind = "kafka"`.
    #[serde(default)]
    pub kafka: KafkaSettings,
}

impl BusSettings {
    /// The consumer group a stage commits its offsets under, named `<prefix>-<stage>`.
    ///
    /// An offset is per group, so every stage that commits needs its own name.
    #[must_use]
    pub fn group(&self, stage: &str) -> String {
        format!("{}-{stage}", self.group_prefix)
    }

    /// The consumer group storage reads `topic` under, named `<prefix>-storage-<topic>`.
    ///
    /// Storage drains two topics (the raw stream and, when decode runs, the decoded one)
    /// and an offset is per group *and* per topic, so it needs one group each. The topic
    /// is in the name — not a `storage-raw`/`storage-decoded` alias — so the group is a
    /// pure function of the deployment's topic names. Renaming one resets that topic's
    /// committed offsets and replays it, so the name is a durability contract.
    #[must_use]
    pub fn storage_group(&self, topic: &str) -> String {
        self.group(&format!("storage-{topic}"))
    }
}

/// The transport the stages share.
///
/// One variant per bus. Serde rejects an unknown tag, so a typo or a transport this
/// build does not have is a startup error rather than a silent fallback to the default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BusKind {
    /// A Kafka-protocol broker over the network. Durable and resumable.
    #[default]
    Kafka,
    /// In-process queues. No broker and no durability: a crash loses in-flight events,
    /// and there is no offset to resume from. For a single-process run, not a deployment.
    Memory,
}

/// How to reach a Kafka-protocol broker, and the client properties passed through.
///
/// `brokers` is required — no default could be right, and a guessed one produces a
/// process that starts and connects to nothing — so it has no serde default. Present only
/// when the bus is Kafka; a memory bus ignores it.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct KafkaSettings {
    /// Comma-separated `host:port` bootstrap servers. Required when the bus is Kafka, and
    /// non-empty then; the check is in `Settings::confirm`, because serde cannot make a
    /// field required only for one value of a sibling.
    pub brokers: String,
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
    /// [bus.kafka.properties]
    /// security.protocol = "SASL_SSL"
    /// compression.type = "zstd"
    /// ```
    pub properties: BTreeMap<String, String>,
}

/// How every stage drains: the batch it flushes on, and when a bounded run stops.
///
/// Transport-independent by design. A stage's flush cadence is its own decision, not a
/// property of the bus, which is why these are not under `[bus]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RuntimeSettings {
    /// How many records to accumulate before flushing, per stage.
    pub batch_records: usize,
    /// How long to wait before flushing a partial batch, in milliseconds.
    pub batch_ms: u64,
    /// How long a topic may be idle before storage treats it as drained, in seconds.
    ///
    /// Absent means never stop, which is what a live stream needs: a stage that exits
    /// when a topic goes quiet would take the process down with it. Set it for a bounded
    /// run — a backfill, a test — not for a live indexer.
    pub drain_secs: Option<u64>,
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self {
            batch_records: 500,
            batch_ms: 1_000,
            drain_secs: None,
        }
    }
}

/// Where decoded records are stored, and which backend writes them.
///
/// The backend-specific settings live under the backend's own table, so this table stays
/// generic. [`StorageKind`] is the tag: exactly one backend is selected, and adding
/// `ClickHouse` or Postgres is a variant there plus its own table here, not another
/// top-level `database` whose owner a reader has to guess.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StorageSettings {
    /// The backend to write. Absent means `duckdb`.
    pub kind: StorageKind,
    /// `DuckDB`'s settings, when `kind = "duckdb"`.
    pub duckdb: DuckDbSettings,
}

/// The storage backend the process writes to.
///
/// One variant per backend. Serde rejects an unknown tag, so a typo or a backend this
/// build does not have is a startup error rather than a silent fallback to the default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageKind {
    /// An embedded `DuckDB` database.
    #[default]
    Duckdb,
}

/// `DuckDB`'s settings: the database to write, and the engine settings passed through.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DuckDbSettings {
    /// The `DuckDB` database file to write. Defaults to [`DEFAULT_DATABASE`].
    pub path: PathBuf,
    /// Any other `DuckDB` setting, passed straight through.
    ///
    /// `DuckDB` accepts dozens of settings and this file does not restate them. Anything
    /// here reaches [`duckdb::Config::with`], which validates it, so a misspelled key is
    /// an error from the engine naming the setting rather than a silent no-op.
    ///
    /// ```toml
    /// [storage.duckdb.settings]
    /// threads = "4"
    /// max_memory = "1GB"
    /// ```
    pub settings: BTreeMap<String, String>,
}

impl Default for DuckDbSettings {
    fn default() -> Self {
        Self {
            path: PathBuf::from(DEFAULT_DATABASE),
            settings: BTreeMap::new(),
        }
    }
}

/// What the decode stage decodes, in its own file.
///
/// Kept out of the indexer's settings because it is a different kind of thing: the
/// settings file is deployment topology — brokers, endpoints, paths — and the registry
/// is a catalog of contracts that grows on its own schedule. It also rotates addresses,
/// which would otherwise churn the settings file's diff on every new protocol.
///
/// A table, not an `Option`, because decode always runs: an absent `registry` means an
/// empty registry and nothing is decoded, which is a legitimate way to run and is said at
/// startup rather than being silent. See [`crate::decode::registry`] for the file's shape.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DecodeSettings {
    /// The registry file, relative to the settings file's directory. Absent means an
    /// empty registry.
    pub registry: Option<PathBuf>,
}

impl std::str::FromStr for Settings {
    type Err = anyhow::Error;

    /// Parses settings from TOML text, filling defaults and rejecting what is required
    /// but absent.
    ///
    /// This is the in-memory form of [`Settings::from_file`], for a caller that has the
    /// text already; prefer reading a file.
    fn from_str(text: &str) -> anyhow::Result<Self> {
        let settings: Self = toml::from_str(text)?;
        settings.confirm()?;
        Ok(settings)
    }
}

impl Settings {
    /// Refuses a combination of settings no default could make right.
    ///
    /// One rule, and the only one: a broker is required exactly when the bus is Kafka.
    /// The broker field must still default to something for `kind = "memory"`, where it
    /// is unused, and serde has no way to make a field conditional on a sibling — so the
    /// check lives here rather than on the field.
    fn confirm(&self) -> anyhow::Result<()> {
        if self.bus.kind == BusKind::Kafka && self.bus.kafka.brokers.trim().is_empty() {
            anyhow::bail!("bus.kafka.brokers is required when bus.kind = \"kafka\"");
        }
        Ok(())
    }

    /// Reads settings from a TOML file.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, when it does not parse, or when a
    /// required value is absent. A malformed file reports its line and column; an unknown
    /// field is an error rather than being ignored, so a misspelled key is caught at
    /// startup instead of silently leaving a setting at its default.
    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|error| {
            anyhow::anyhow!("cannot read settings at {}: {error}", path.display())
        })?;
        let mut settings: Self = text.parse().map_err(|error: anyhow::Error| {
            anyhow::anyhow!("invalid settings at {}:\n{error}", path.display())
        })?;
        // Remember where the file lives, so a relative path in it resolves against that
        // directory rather than the process's working directory. A bare filename has no
        // parent; the working directory stands in for it.
        settings.dir = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        Ok(settings)
    }

    /// The registry file decode loads, resolved against the settings file's directory.
    ///
    /// `None` when no registry is named, which is an empty registry rather than an error.
    /// Through [`Settings::from_file`] the path is absolute-or-relative-to-the-file; text
    /// parsed from a string has no directory, so the path is returned as written.
    #[must_use]
    pub fn registry_path(&self) -> Option<PathBuf> {
        self.decode
            .registry
            .as_ref()
            .map(|registry| self.dir.join(registry))
    }

    /// The batch configuration the stages share.
    #[must_use]
    pub const fn batch(&self) -> BatchConfig {
        BatchConfig::new(
            self.runtime.batch_records,
            Duration::from_millis(self.runtime.batch_ms),
        )
    }

    /// The drain bound, if one is set.
    #[must_use]
    pub const fn drain(&self) -> Option<Duration> {
        match self.runtime.drain_secs {
            Some(secs) => Some(Duration::from_secs(secs)),
            None => None,
        }
    }
}

/// The default topic ingest publishes to, and decode consumes.
pub const DEFAULT_RAW_TOPIC: &str = "raw.chain";

/// The default topic decode publishes to, and storage consumes.
pub const DEFAULT_DECODED_TOPIC: &str = "decoded.chain";

/// The default consumer group prefix. Each stage appends its own name.
pub const DEFAULT_GROUP_PREFIX: &str = "indexer";

/// The default `DuckDB` path.
pub const DEFAULT_DATABASE: &str = "indexer.duckdb";

fn default_raw_topic() -> String {
    DEFAULT_RAW_TOPIC.to_owned()
}

fn default_decoded_topic() -> String {
    DEFAULT_DECODED_TOPIC.to_owned()
}

fn default_group_prefix() -> String {
    DEFAULT_GROUP_PREFIX.to_owned()
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
    use std::str::FromStr as _;
    use std::time::Duration;

    use super::{DEFAULT_DATABASE, DEFAULT_DECODED_TOPIC, DEFAULT_RAW_TOPIC, Settings};

    /// The minimum a file needs: a broker and, to ingest, a chain and its endpoints.
    fn minimal() -> &'static str {
        r#"
[ingest]
chain = "base"
http_url = "https://example.invalid"
ws_url = "wss://example.invalid"

[bus.kafka]
brokers = "localhost:9092"
"#
    }

    /// Everything not required takes a default, so a small file runs a standard
    /// deployment without listing the obvious.
    #[test]
    fn a_minimal_file_supplies_every_default() {
        let settings = Settings::from_str(minimal()).expect("minimal settings parse");

        let ingest = settings.ingest.as_ref().expect("ingest is present");
        assert!(!ingest.stdout, "stdout is off unless asked for");

        assert_eq!(settings.bus.raw_topic, DEFAULT_RAW_TOPIC);
        assert_eq!(settings.bus.decoded_topic, DEFAULT_DECODED_TOPIC);
        assert_eq!(settings.bus.group_prefix, "indexer");
        assert_eq!(
            settings.batch(),
            super::BatchConfig::new(500, Duration::from_secs(1))
        );

        assert_eq!(settings.storage.kind, super::StorageKind::Duckdb);
        assert_eq!(
            settings.storage.duckdb.path,
            std::path::PathBuf::from(DEFAULT_DATABASE)
        );
        assert!(settings.decode.registry.is_none(), "nothing is decoded");
        assert_eq!(
            settings.drain(),
            None,
            "no drain bound by default, because a live stream must not stop"
        );
    }

    /// The shipped `indexer.toml` is an example an operator copies, so it must stay
    /// valid: a moved key would otherwise break the file the README points at.
    #[test]
    fn the_repository_settings_file_parses() {
        Settings::from_str(include_str!("../indexer.toml")).expect("indexer.toml parses");
    }

    /// The fixture file yields the values each stage is built from — the endpoints, the
    /// batch, the drain, and the resolved registry path — so the seam between the file
    /// and the stages is exercised without a broker or a store.
    #[test]
    fn the_repository_file_yields_the_values_the_stages_are_built_from() {
        let settings =
            Settings::from_str(include_str!("../indexer.toml")).expect("indexer.toml parses");

        let ingest = settings.ingest.as_ref().expect("ingest is configured");
        assert_eq!(ingest.chain, "base");
        assert!(!ingest.http_url.is_empty());
        assert!(!ingest.ws_url.is_empty());

        assert_eq!(settings.bus.raw_topic, DEFAULT_RAW_TOPIC);
        assert_eq!(settings.bus.decoded_topic, DEFAULT_DECODED_TOPIC);
        assert_eq!(settings.batch(), super::BatchConfig::default());
        assert_eq!(settings.drain(), None, "a live indexer does not stop");
        // The fixture names `registry.toml`. Parsed from text it has no directory, so the
        // path is left as written rather than resolved against the working directory.
        assert_eq!(
            settings.registry_path(),
            Some(std::path::PathBuf::from("registry.toml"))
        );
    }

    /// A registry path resolves against the settings file's own directory, not the
    /// process's working directory, so a deployment that moves wholesale keeps working.
    #[test]
    fn a_registry_path_resolves_against_the_settings_file() {
        let dir = std::env::temp_dir().join(format!("indexer-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("indexer.toml");
        std::fs::write(
            &path,
            r#"
[bus.kafka]
brokers = "localhost:9092"

[decode]
registry = "registry.toml"
"#,
        )
        .expect("write settings");

        let settings = Settings::from_file(&path).expect("settings load");
        assert_eq!(settings.registry_path(), Some(dir.join("registry.toml")));

        std::fs::remove_dir_all(&dir).expect("clean up");
    }

    /// Text parsed from a string has no directory, so a relative registry path is left as
    /// written rather than silently resolved against the working directory.
    #[test]
    fn a_registry_path_from_text_is_left_as_written() {
        let settings = Settings::from_str(
            "[bus.kafka]\nbrokers = \"x\"\n[decode]\nregistry = \"registry.toml\"\n",
        )
        .expect("settings parse");
        assert_eq!(
            settings.registry_path(),
            Some(std::path::PathBuf::from("registry.toml"))
        );
    }

    /// The topic defaults live with the broker, beside `decoded_topic`, because every
    /// stage agrees on them and none of them owns a topic.
    #[test]
    fn the_raw_topic_is_a_broker_setting() {
        let settings = Settings::from_str(minimal()).expect("minimal settings parse");
        // Decode and storage read the raw topic even with no ingest configured, so it
        // cannot be nested under `[ingest]` where it would be absent when they still
        // need it.
        assert_eq!(settings.bus.raw_topic, DEFAULT_RAW_TOPIC);
    }

    /// A broker is required when the bus is Kafka: no default could be right, and
    /// guessing one produces a process that starts and connects to nothing. The default
    /// bus is Kafka, so an absent `[bus.kafka]` is an error.
    #[test]
    fn a_kafka_bus_without_a_broker_is_an_error() {
        let error = Settings::from_str("[bus]\ngroup_prefix = \"x\"\n")
            .expect_err("a broker is required")
            .to_string();
        assert!(error.contains("brokers"), "the error names it: {error}");
    }

    /// The same absence is fine for a memory bus, where the broker is unused: the bus is
    /// chosen by `kind`, and only the chosen transport's requirements apply.
    #[test]
    fn a_memory_bus_needs_no_broker() {
        let settings = Settings::from_str("[bus]\nkind = \"memory\"\n").expect("memory bus parses");
        assert_eq!(settings.bus.kind, super::BusKind::Memory);
        assert_eq!(settings.bus.raw_topic, DEFAULT_RAW_TOPIC);
        assert_eq!(settings.bus.decoded_topic, DEFAULT_DECODED_TOPIC);
    }

    /// A present-but-empty required value is refused too. Serde's requiredness catches an
    /// absent key; `brokers = ""` would otherwise start a process that connects to
    /// nothing, and `chain = ""` would stamp every event with a blank chain.
    #[test]
    fn an_empty_required_value_is_an_error() {
        let error = Settings::from_str("[bus.kafka]\nbrokers = \"\"\n")
            .expect_err("an empty broker is not a broker")
            .to_string();
        assert!(error.contains("brokers"), "the error names it: {error}");

        let error = Settings::from_str(
            r#"
[ingest]
chain = "  "
http_url = "https://example.invalid"
ws_url = "wss://example.invalid"

[bus.kafka]
brokers = "localhost:9092"
"#,
        )
        .expect_err("a blank chain is not a chain")
        .to_string();
        assert!(error.contains("chain"), "the error names it: {error}");
    }

    /// So is an endpoint once ingest is configured: a chain with nowhere to read is
    /// not a deployable state, and `http_url`/`ws_url` carry no default.
    #[test]
    fn a_missing_endpoint_is_an_error() {
        let error = Settings::from_str(
            r#"
[ingest]
chain = "base"

[bus.kafka]
brokers = "localhost:9092"
"#,
        )
        .expect_err("http_url and ws_url are required")
        .to_string();
        assert!(
            error.contains("missing field") && error.contains("url"),
            "serde names the absent field: {error}"
        );
    }

    /// An unknown key is an error rather than ignored, so a misspelling is caught at
    /// startup instead of leaving a setting silently at its default.
    #[test]
    fn an_unknown_key_is_rejected() {
        let error = Settings::from_str(
            r#"
[bus.kafka]
brokers = "localhost:9092"
brokres = "typo"
"#,
        )
        .expect_err("a typo must not be ignored")
        .to_string();
        assert!(
            error.contains("brokres"),
            "the error names the key: {error}"
        );
    }

    /// Ingest is optional, so decode and storage can run against a topic filled
    /// elsewhere.
    #[test]
    fn ingest_may_be_absent() {
        let settings = Settings::from_str("[bus.kafka]\nbrokers = \"localhost:9092\"\n")
            .expect("settings parse");
        assert!(settings.ingest.is_none());
        // The raw topic still has a name, so decode and storage agree on it.
        assert_eq!(settings.bus.raw_topic, DEFAULT_RAW_TOPIC);
    }

    /// A stage's consumer group is `<prefix>-<stage>`, and storage's is
    /// `<prefix>-storage-<topic>`. The name is a durability contract — an offset is per
    /// group, so renaming one silently resets it — which is why it is derived in one place
    /// rather than formatted at each call site.
    #[test]
    fn a_consumer_group_is_the_prefix_and_stage() {
        let settings =
            Settings::from_str("[bus]\ngroup_prefix = \"team\"\n[bus.kafka]\nbrokers = \"x\"\n")
                .expect("settings parse");
        assert_eq!(settings.bus.group("decode"), "team-decode");
        // The topic is in the name, not a `storage-raw` alias: the group must not move
        // when the stage's shape does, or a deploy replays both topics from offset 0.
        assert_eq!(
            settings.bus.storage_group("raw.chain"),
            "team-storage-raw.chain"
        );
        assert_eq!(
            settings.bus.storage_group("decoded.chain"),
            "team-storage-decoded.chain"
        );
    }

    /// A drain bound is opt-in, lives with the other run settings, and reaches the stage
    /// as a duration.
    #[test]
    fn a_drain_bound_is_opt_in() {
        let settings = Settings::from_str(
            r#"
[bus.kafka]
brokers = "localhost:9092"

[runtime]
drain_secs = 5
"#,
        )
        .expect("settings parse");
        assert_eq!(settings.drain(), Some(Duration::from_secs(5)));
    }

    /// The `[decode]` table names a registry file, so a config points the stage at its
    /// contract catalog rather than carrying the catalog itself.
    #[test]
    fn the_decode_table_names_a_registry_file() {
        let settings = Settings::from_str(
            r#"
[bus.kafka]
brokers = "localhost:9092"

[decode]
registry = "registry.toml"
"#,
        )
        .expect("settings parse");

        assert_eq!(
            settings.decode.registry,
            Some(std::path::PathBuf::from("registry.toml"))
        );
    }

    /// The store's kind is a tagged choice, and a backend this build does not have is a
    /// startup error rather than a silent fallback to the default.
    #[test]
    fn an_unknown_storage_kind_is_rejected() {
        let error = Settings::from_str(
            r#"
[bus.kafka]
brokers = "localhost:9092"

[storage]
kind = "clickhouse"
"#,
        )
        .expect_err("an unknown backend must not fall back to duckdb")
        .to_string();
        assert!(error.contains("clickhouse"), "{error}");
    }

    /// A backend's settings live under its own table: an engine key is not a top-level
    /// store key, and the path is not a generic `database`.
    #[test]
    fn duckdb_settings_live_under_the_duckdb_table() {
        let settings = Settings::from_str(
            r#"
[bus.kafka]
brokers = "localhost:9092"

[storage]
kind = "duckdb"

[storage.duckdb]
path = "/tmp/custom.duckdb"

[storage.duckdb.settings]
threads = "4"
"#,
        )
        .expect("settings parse");

        assert_eq!(
            settings.storage.duckdb.path,
            std::path::PathBuf::from("/tmp/custom.duckdb")
        );
        assert_eq!(
            settings.storage.duckdb.settings.get("threads"),
            Some(&"4".to_owned())
        );
    }

    /// Inputs the operator must choose are taken as written, not defaulted.
    #[test]
    fn explicit_values_win() {
        let settings = Settings::from_str(
            r#"
[ingest]
chain = "ethereum"
http_url = "https://example.invalid"
ws_url = "wss://example.invalid"

[bus]
raw_topic = "custom.raw"
decoded_topic = "custom.decoded"
group_prefix = "team-x"

[bus.kafka]
brokers = "broker:9092"

[runtime]
batch_records = 50
batch_ms = 250
drain_secs = 3

[storage.duckdb]
path = "/tmp/custom.duckdb"
"#,
        )
        .expect("settings parse");

        assert_eq!(settings.bus.raw_topic, "custom.raw");
        assert_eq!(settings.bus.decoded_topic, "custom.decoded");
        assert_eq!(settings.bus.group_prefix, "team-x");
        assert_eq!(
            settings.batch(),
            super::BatchConfig::new(50, Duration::from_millis(250))
        );
        assert_eq!(
            settings.storage.duckdb.path,
            std::path::PathBuf::from("/tmp/custom.duckdb")
        );
        assert_eq!(settings.drain(), Some(Duration::from_secs(3)));
    }
}
