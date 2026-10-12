//! The indexer's settings file, and the builders each stage takes.
//!
//! TOML, read once at startup. Three conventions carry the contract:
//!
//! - **Required means no value could be right by accident.** A field with no
//!   `#[serde(default)]` has no default, so serde refuses a document that omits it and
//!   names the field. Required `String`s also carry `deserialize_with = "non_empty"`,
//!   since `chain = ""` is present, not absent.
//! - **A typo is a startup error, not a silent default.** Every table is
//!   `deny_unknown_fields`, and an unknown sink backend is refused rather than falling
//!   back to another one.
//! - **A secret is named, not written.** A [`Secret`] field takes `{ env = "NAME" }`, read
//!   from the environment as the file loads, so the file stays in git while the platform
//!   injects the value. A literal is accepted too, for a public endpoint or local
//!   development.
//!
//! Where records end up. The backend is the table: `[sink.duckdb]` opens `DuckDB` and
//! holds its settings, `[sink.delta]` the lake, and so on — any combination, one of
//! each, every one listing the `tables` it writes (see [`Sinks`]). `[sink.stdout]` prints
//! instead, and is the only sink when named. Every other field not shown below may be
//! omitted.
//!
//! Named for [`crate::sink`] rather than for storage: a sink is where envelopes go, and
//! not all of them are kept. `stdout` is a sink that writes nowhere, and a webhook or a
//! Kafka topic would be one too.
//!
//! ```toml
//! [ingest]
//! chain = "base"
//! http_url = "https://base-rpc.publicnode.com"   # or { env = "INDEXER_HTTP_URL" }
//! ws_url = "wss://base-rpc.publicnode.com"
//! # ...or, instead of the two endpoints, backfill from `HyperSync` and stop:
//! # [ingest.hypersync]
//! # url = "https://8453.hypersync.xyz"
//! # token = { env = "INDEXER_ENVIO_HYPERSYNC_TOKEN" }
//!
//! [sink.duckdb]
//! tables = ["blocks", "transactions", "logs", "decoded_logs", "contract_events"]
//! path = "indexer.duckdb"
//!
//! [sink.duckdb.settings]
//! threads = "4"
//!
//! # ...beside `[sink.postgres]` or `[sink.delta]`, or instead `[sink.stdout]`, to
//! # print the stream and open no store.
//!
//! [decode]
//! protocols = "protocols"      # the protocol manifests; optional
//!
//! [metrics]
//! listen = "0.0.0.0:9100"      # the Prometheus endpoint; optional
//! ```

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde::de::{self, Unexpected};

use crate::metrics::MetricsSettings;
use crate::secret::Secret;
use crate::sink::route::{Backend, Tables};

/// Deserializes a required string, refusing an empty or whitespace-only one.
///
/// A `#[serde(default)]`-less field already makes absence an error, but a present empty
/// value is not absence: `http_url = ""` is a deployment that starts and connects to
/// nothing, and `chain = ""` stamps every event with a blank chain. Putting the check on
/// the field keeps it with the field's documentation and lets serde name it, rather than
/// a hand-written pass over every required key after the fact.
pub(crate) fn non_empty<'de, D>(deserializer: D) -> Result<String, D::Error>
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
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    /// The chain to follow. Required: the pipeline starts at ingest.
    pub ingest: IngestSettings,
    /// Where records go. Required: a run with nowhere to send them is not a deployment.
    pub sink: Sinks,
    /// What the decode stage decodes.
    #[serde(default)]
    pub decode: DecodeSettings,
    /// Where the Prometheus endpoint listens. Absent serves and records nothing; see
    /// [`crate::metrics`].
    #[serde(default)]
    pub metrics: Option<MetricsSettings>,
    /// The directory the settings file lives in, which a relative path in it resolves
    /// against. Filled by [`Settings::from_file`]; empty for text parsed from elsewhere,
    /// which leaves a relative path as written. Not a file key, so a `dir` in the file is
    /// still reported as a typo.
    #[serde(skip)]
    dir: PathBuf,
}

/// How ingest follows a chain.
///
/// `chain` is required. So is one source, and only one: `http_url` and `ws_url` together,
/// a node's JSON-RPC that follows the chain, or `[ingest.hypersync]`, which backfills the
/// buried heights and stops (see [`HyperSyncSettings`]). A source cannot default — a
/// default would silently index nothing — so naming neither, half of the pair, or both
/// is a startup error; see [`IngestSettings::source`].
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestSettings {
    /// The chain id stamped on every event. Required, and non-empty.
    #[serde(deserialize_with = "non_empty")]
    pub chain: String,
    /// The JSON-RPC endpoint used for blocks, receipts, and logs. A [`Secret`], since
    /// a provider's URL usually carries its API key. Named with `ws_url`, or not at all.
    ///
    /// What ingest fetches is not set here: it is what the sinks' `tables` need (see
    /// [`crate::sink::route::datasets`]).
    #[serde(default)]
    pub http_url: Option<Secret>,
    /// The WebSocket endpoint used for heads. A [`Secret`], like `http_url`, and named
    /// with it.
    #[serde(default)]
    pub ws_url: Option<Secret>,
    /// Backfill from Envio `HyperSync` instead of an RPC node, and stop.
    #[serde(default)]
    pub hypersync: Option<HyperSyncSettings>,
    /// The first height to index on an empty store. Omitted starts at the observed head,
    /// which skips earlier history; a value indexes that height and every one after it
    /// before following live heads. A value above the sampled head is a startup error
    /// rather than a silent clamp. A store that already holds accepted blocks resumes
    /// after them instead: a value at or below the block after the stored tip is
    /// ignored, and one above it is a startup error, since it would leave a gap.
    /// A `HyperSync` backfill on an empty store needs it, since a backfill that starts at
    /// the head has nothing to do.
    #[serde(default)]
    pub start_block: Option<u64>,
    /// The last height to index. Omitted follows live heads without end; a value indexes
    /// through that height and exits instead of following the chain. A value above the
    /// sampled head, or below where the run starts, is a startup error. A store that
    /// already holds it has nothing to do, and the run exits without indexing.
    ///
    /// A stop inside the reorg window under the head is indexed as the chain stood then:
    /// a fork that later replaces it is not retracted, since no run follows it.
    #[serde(default)]
    pub stop_block: Option<u64>,
    /// How many backfill fetches are in flight at once. Blocks are still accepted one at a
    /// time, in height order; this only overlaps the requests, so the node is read while
    /// the stores write. Zero is refused. Each fetch counts against the provider's rate
    /// limit, so a provider that throttles hard wants this low.
    #[serde(default = "default_backfill_concurrency")]
    pub backfill_concurrency: NonZeroUsize,
}

/// Four: enough to keep the node busy while a store commits, few enough that a
/// provider's per-second limit is not the first thing it finds.
const fn default_backfill_concurrency() -> NonZeroUsize {
    NonZeroUsize::MIN.saturating_add(3)
}

/// The source `[ingest]` names, as [`IngestSettings::source`] resolves it.
#[derive(Debug, Clone, Copy)]
pub enum IngestSource<'a> {
    /// A node's JSON-RPC for blocks, and its WebSocket for heads.
    Rpc {
        /// The JSON-RPC endpoint.
        http_url: &'a Secret,
        /// The WebSocket endpoint.
        ws_url: &'a Secret,
    },
    /// Envio `HyperSync`, for a backfill that stops at the reorg window.
    HyperSync(&'a HyperSyncSettings),
}

impl IngestSettings {
    /// The one source these settings name.
    ///
    /// # Errors
    ///
    /// Returns [`SettingsError::IngestSource`] when they name neither an RPC node nor
    /// `HyperSync`, only one of the RPC node's two endpoints, or both sources.
    pub fn source(&self) -> Result<IngestSource<'_>, SettingsError> {
        match (&self.http_url, &self.ws_url, &self.hypersync) {
            (Some(http_url), Some(ws_url), None) => Ok(IngestSource::Rpc { http_url, ws_url }),
            (None, None, Some(hypersync)) => Ok(IngestSource::HyperSync(hypersync)),
            (None, None, None) => Err(SettingsError::IngestSource(
                "name http_url and ws_url, or [ingest.hypersync]",
            )),
            (_, _, Some(_)) => Err(SettingsError::IngestSource(
                "[ingest.hypersync] replaces http_url and ws_url; name one or the other",
            )),
            _ => Err(SettingsError::IngestSource(
                "http_url and ws_url are named together",
            )),
        }
    }
}

/// A backfill from [Envio HyperSync](https://docs.envio.dev/docs/`HyperSync`/overview).
///
/// `HyperSync` serves a chain's history thousands of blocks per request, but has no live
/// heads. A run on it indexes the buried heights — everything below the reorg window
/// under the head — and exits; following the chain is a restart with `http_url` and
/// `ws_url` in place of this table, which resumes after what the backfill committed.
///
/// ```toml
/// [ingest.hypersync]
/// url = "https://4663.hypersync.xyz"
/// token = { env = "INDEXER_ENVIO_HYPERSYNC_TOKEN" }
/// ```
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperSyncSettings {
    /// The chain's `HyperSync` endpoint, such as `https://4663.hypersync.xyz`. Required.
    #[serde(deserialize_with = "non_empty")]
    pub url: String,
    /// The Envio API token. Required; a [`Secret`].
    pub token: Secret,
}

/// Where records go: one table per backend, each holding that backend's settings and the
/// [`Tables`] it writes.
///
/// The backend *is* the table, which is what makes backend-specific keys unambiguous:
/// `[sink.duckdb.settings]` is inside the table that named `DuckDB`. A run writes any
/// combination of stores, at most one of each, and every store says which tables it
/// writes — there is no default, so what lands where is read off the file. What ingest
/// fetches follows from the union (see [`crate::sink::route`]).
///
/// ```toml
/// [sink.delta]
/// tables = ["blocks", "transactions", "logs", "decoded_logs"]
/// uri = "s3://indexer-lake"
///
/// [sink.postgres]
/// tables = ["contract_events"]
/// connection_string = { env = "INDEXER_POSTGRES_URL" }
/// ```
///
/// Naming none is an error, and so is `[sink.stdout]` beside a store: printing keeps no
/// ledger, so it cannot resume with the stores (see `crate::sink::resume`). A store
/// backend is behind its feature, so a build without the engine does not carry a store
/// it cannot open: there, naming `[sink.duckdb]` is an unknown key — a startup error
/// naming the table, rather than settings that parse and fail later.
///
/// A sink is not necessarily a store. `stdout` keeps nothing, and that is the point of
/// the wider name: the layer is where envelopes go.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sinks {
    /// Print the stream as newline-delimited JSON, for watching what the indexer would
    /// write. No store is opened and nothing is persisted.
    #[serde(default)]
    pub stdout: Option<crate::sink::stdout::StdoutSettings>,
    /// An embedded `DuckDB` database, opened and committed by its own task.
    #[cfg(feature = "duckdb")]
    #[serde(default)]
    pub duckdb: Option<crate::sink::duckdb::DuckDbSettings>,
    /// A remote `PostgreSQL` database, using asynchronous transactional COPY.
    #[cfg(feature = "postgres")]
    #[serde(default)]
    pub postgres: Option<crate::sink::postgres::PostgresSettings>,
    /// Delta Lake tables on S3 or an S3-compatible store: the cheap raw-data lake.
    #[cfg(feature = "delta")]
    #[serde(default)]
    pub delta: Option<crate::sink::delta::DeltaSettings>,
}

impl Sinks {
    /// Each named sink and the tables it writes, in a fixed order.
    #[must_use]
    pub fn tables(&self) -> Vec<(Backend, &Tables)> {
        let mut named = Vec::new();
        if let Some(stdout) = &self.stdout {
            named.push((Backend::Stdout, &stdout.tables));
        }
        #[cfg(feature = "duckdb")]
        if let Some(duckdb) = &self.duckdb {
            named.push((Backend::DuckDb, &duckdb.tables));
        }
        #[cfg(feature = "postgres")]
        if let Some(postgres) = &self.postgres {
            named.push((Backend::Postgres, &postgres.tables));
        }
        #[cfg(feature = "delta")]
        if let Some(delta) = &self.delta {
            named.push((Backend::Delta, &delta.tables));
        }
        named
    }

    /// Refuses a file that names no sink, or `stdout` beside a store.
    fn check(&self) -> Result<(), SettingsError> {
        let named = self.tables();
        if named.is_empty() {
            return Err(SettingsError::NoSink);
        }
        if self.stdout.is_some() && named.len() > 1 {
            return Err(SettingsError::StdoutBesideStore);
        }
        Ok(())
    }
}

/// What the decode stage decodes: a directory of protocol manifests.
///
/// The manifests are kept out of the indexer's settings because they are a different kind
/// of thing: the settings file is deployment topology — endpoints, paths — and the
/// manifests are a catalog of protocols that grows on its own schedule.
///
/// A table, not an `Option`, because decode always runs: an absent `protocols` means an
/// empty catalog and nothing is decoded, which is a legitimate way to run and is said at
/// startup rather than being silent. See [`crate::decode::Catalog`] for the manifest shape.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DecodeSettings {
    /// The protocols directory, relative to the settings file's directory. Absent means
    /// an empty catalog.
    pub protocols: Option<PathBuf>,
}

/// Why a settings file could not be turned into [`Settings`].
///
/// Typed because the two failures are different problems — an unreadable path and a
/// rejected document — and a caller that recovers from one (falling back to a default
/// file) may not want to swallow the other. Neither variant names the file: `toml` gives
/// the line and column, and the caller is the one that knows the path.
#[derive(Debug, thiserror::Error)]
pub enum SettingsError {
    /// The file could not be read.
    #[error("read settings at {path}: {source}")]
    Read {
        /// The path that failed.
        path: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// The document did not parse, or a required value was absent.
    #[error("invalid settings: {source}")]
    Parse {
        /// `toml`'s error, which carries the line and column.
        source: toml::de::Error,
    },
    /// No `[sink.<backend>]` is named, so there is nowhere to write.
    #[error("invalid settings: name at least one [sink.<backend>]")]
    NoSink,
    /// `[sink.stdout]` is named beside a store.
    #[error("invalid settings: [sink.stdout] cannot be named beside a store")]
    StdoutBesideStore,
    /// `[ingest]` names no source, half of one, or two.
    #[error("invalid settings: [ingest] {0}")]
    IngestSource(&'static str),
}

impl std::str::FromStr for Settings {
    /// [`SettingsError::Parse`]; there is no file to read in this form.
    type Err = SettingsError;

    /// Parses settings from TOML text, filling defaults and rejecting what is required
    /// but absent, an `[ingest]` that does not name one source, and sinks that cannot
    /// run together.
    ///
    /// This is the in-memory form of [`Settings::from_file`], for a caller that has the
    /// text already. A relative path in the text resolves against the process's working
    /// directory rather than a file's, since there is no file; prefer
    /// [`Settings::from_file`].
    fn from_str(text: &str) -> Result<Self, SettingsError> {
        let settings: Self =
            toml::from_str(text).map_err(|source| SettingsError::Parse { source })?;
        settings.ingest.source()?;
        settings.sink.check()?;
        Ok(settings)
    }
}

impl Settings {
    /// Reads settings from a TOML file.
    ///
    /// # Errors
    ///
    /// Returns [`SettingsError::Read`] when the file cannot be read,
    /// [`SettingsError::Parse`] when it does not parse or a required value is absent,
    /// [`SettingsError::IngestSource`] when `[ingest]` does not name one source, and
    /// [`SettingsError::NoSink`] or [`SettingsError::StdoutBesideStore`] when its sinks
    /// cannot run. A
    /// malformed file reports its line and column; an unknown field is an error rather
    /// than being ignored, so a misspelled key is caught at startup instead of silently
    /// leaving a setting at its default.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, SettingsError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|source| SettingsError::Read {
            path: path.display().to_string(),
            source,
        })?;
        let mut settings: Self = text.parse()?;
        // Remember where the file lives, so a relative path in it resolves against that
        // directory rather than the process's working directory. A bare filename's
        // parent is empty, and joining onto an empty path is the path as written.
        settings.dir = path.parent().unwrap_or_else(|| Path::new("")).to_path_buf();
        #[cfg(feature = "duckdb")]
        if let Some(duckdb) = &mut settings.sink.duckdb
            && duckdb.path != Path::new(":memory:")
        {
            duckdb.path = settings.dir.join(&duckdb.path);
        }
        Ok(settings)
    }

    /// The protocols directory decode loads, resolved against the settings file's
    /// directory.
    ///
    /// `None` when none is named, which is an empty catalog rather than an error. Through
    /// [`Settings::from_file`] the path is absolute-or-relative-to-the-file; text parsed
    /// from a string has no directory, so the path is returned as written.
    #[must_use]
    pub fn protocols_path(&self) -> Option<PathBuf> {
        self.decode
            .protocols
            .as_ref()
            .map(|protocols| self.dir.join(protocols))
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::str::FromStr as _;

    use super::{Settings, SettingsError};
    #[cfg(all(feature = "duckdb", feature = "postgres", feature = "delta"))]
    use crate::sink::route::Backend;

    /// The minimum a file needs: a chain, its endpoints, and a store.
    fn minimal() -> &'static str {
        r#"
[ingest]
chain = "base"
http_url = "https://example.invalid"
ws_url = "wss://example.invalid"

[sink.stdout]
tables = ["blocks"]
"#
    }

    /// `indexer.example.toml` documents every key, so it must run as shipped: it parses,
    /// and its sinks' tables resolve against the tables it declares. It names
    /// `[sink.stdout]`, which every build has, and comments out the stores. It is the
    /// fixture rather than `indexer.toml`, which is a deployment's own settings and
    /// changes with it.
    #[test]
    fn the_example_settings_file_assembles() {
        let settings = Settings::from_str(include_str!("../indexer.example.toml"))
            .expect("indexer.example.toml parses");
        crate::runtime::Pipeline::from_settings(&settings).expect("indexer.example.toml assembles");
    }

    /// A protocols path resolves against the settings file's own directory, not the
    /// process's working directory, so a deployment that moves wholesale keeps working.
    #[test]
    fn a_protocols_path_resolves_against_the_settings_file() {
        let dir = std::env::temp_dir().join(format!("indexer-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("indexer.toml");
        std::fs::write(
            &path,
            format!("{}\n[decode]\nprotocols = \"protocols\"\n", minimal()),
        )
        .expect("write settings");

        let settings = Settings::from_file(&path).expect("settings load");
        assert_eq!(settings.protocols_path(), Some(dir.join("protocols")));

        std::fs::remove_dir_all(&dir).expect("clean up");
    }

    /// A `DuckDB` path resolves against the settings file's directory too.
    #[cfg(feature = "duckdb")]
    #[test]
    fn a_duckdb_path_resolves_against_the_settings_file() {
        let dir = std::env::temp_dir().join(format!("indexer-duckdb-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("indexer.toml");
        for (written, resolved) in [
            ("store.duckdb", dir.join("store.duckdb")),
            (":memory:", ":memory:".into()),
        ] {
            let text = minimal().replace(
                "[sink.stdout]",
                &format!("[sink.duckdb]\npath = \"{written}\""),
            );
            std::fs::write(&path, text).expect("write settings");
            let settings = Settings::from_file(&path).expect("settings load");
            let duckdb = settings.sink.duckdb.expect("a DuckDB sink");
            assert_eq!(duckdb.path, resolved);
        }
        std::fs::remove_dir_all(&dir).expect("clean up");
    }

    /// Every sink lists its tables, at least one sink is named, and printing runs alone.
    #[test]
    fn sinks_name_their_tables_and_stdout_runs_alone() {
        let without_sink = minimal().replace("[sink.stdout]\ntables = [\"blocks\"]\n", "[sink]\n");
        assert!(matches!(
            Settings::from_str(&without_sink),
            Err(SettingsError::NoSink)
        ));
        let without_tables = minimal().replace("tables = [\"blocks\"]\n", "");
        let error = Settings::from_str(&without_tables).expect_err("tables are required");
        assert!(error.to_string().contains("tables"), "{error}");
        let empty = minimal().replace("[\"blocks\"]", "[]");
        assert!(Settings::from_str(&empty).is_err());
        let unknown = format!("{}\n[sink.kafka]\ntables = [\"logs\"]\n", minimal());
        assert!(Settings::from_str(&unknown).is_err());
    }

    /// `[ingest]` names one source: both RPC endpoints, or `HyperSync`.
    #[test]
    fn ingest_names_one_source() {
        let hypersync = minimal().replace(
            "http_url = \"https://example.invalid\"\nws_url = \"wss://example.invalid\"\n",
            "\n[ingest.hypersync]\nurl = \"https://4663.hypersync.xyz\"\ntoken = \"secret-token\"\n",
        );
        let settings = Settings::from_str(&hypersync).expect("hypersync alone parses");
        let Ok(super::IngestSource::HyperSync(source)) = settings.ingest.source() else {
            panic!("expected HyperSync");
        };
        assert_eq!(source.url, "https://4663.hypersync.xyz");
        assert!(!format!("{settings:?}").contains("secret-token"));

        let both = format!(
            "{}\n[ingest.hypersync]\nurl = \"https://4663.hypersync.xyz\"\ntoken = \"t\"\n",
            minimal().replace("[sink.stdout]\ntables = [\"blocks\"]\n", "")
        ) + "\n[sink.stdout]\ntables = [\"blocks\"]\n";
        let neither = minimal().replace(
            "http_url = \"https://example.invalid\"\nws_url = \"wss://example.invalid\"\n",
            "",
        );
        let half = minimal().replace("ws_url = \"wss://example.invalid\"\n", "");
        for (case, text) in [("both", both), ("neither", neither), ("half", half)] {
            assert!(
                matches!(
                    Settings::from_str(&text),
                    Err(SettingsError::IngestSource(_))
                ),
                "{case}"
            );
        }
    }

    /// Printing keeps no ledger, so it cannot resume beside a store.
    #[cfg(feature = "duckdb")]
    #[test]
    fn stdout_beside_a_store_is_refused() {
        let both = format!("{}\n[sink.duckdb]\ntables = [\"logs\"]\n", minimal());
        assert!(matches!(
            Settings::from_str(&both),
            Err(SettingsError::StdoutBesideStore)
        ));
    }

    /// Any combination of stores parses, each with its own tables.
    #[cfg(all(feature = "duckdb", feature = "postgres", feature = "delta"))]
    #[test]
    fn every_store_may_run_together() {
        let text = minimal().replace(
            "[sink.stdout]\ntables = [\"blocks\"]\n",
            r#"[sink.duckdb]
tables = ["blocks"]

[sink.postgres]
tables = ["contract_events"]
connection_string = "postgres://example.invalid"

[sink.delta]
tables = ["logs", "decoded_logs"]
uri = "/tmp/lake"
"#,
        );
        let settings = Settings::from_str(&text).expect("settings parse");
        let named: Vec<_> = settings
            .sink
            .tables()
            .into_iter()
            .map(|(backend, _)| backend)
            .collect();
        assert_eq!(named, [Backend::DuckDb, Backend::Postgres, Backend::Delta]);
    }

    /// A secret is the value or the environment variable that holds it, and either way it
    /// is never printed: a deployment logs its settings whole.
    #[test]
    fn a_secret_is_written_or_read_from_the_environment_and_never_printed() {
        // `PATH` is set in every test process, and reading it needs no `unsafe` set_var.
        let path = std::env::var("PATH").expect("PATH is set");
        let settings = Settings::from_str(
            r#"
[ingest]
chain = "base"
http_url = "https://example.invalid/v2/literal-key"
ws_url = { env = "PATH" }

[sink.stdout]
tables = ["blocks"]
"#,
        )
        .expect("both forms parse");

        let Ok(super::IngestSource::Rpc { http_url, ws_url }) = settings.ingest.source() else {
            panic!("expected an RPC source");
        };
        assert_eq!(http_url.expose(), "https://example.invalid/v2/literal-key");
        assert_eq!(ws_url.expose(), path);
        let printed = format!("{settings:?}");
        assert!(
            !printed.contains("literal-key") && !printed.contains(&path),
            "{printed}"
        );
    }
}
