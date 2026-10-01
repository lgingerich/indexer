//! The indexer's settings file, and the builders each stage takes.
//!
//! TOML, read once at startup. Two conventions carry the contract:
//!
//! - **Required means no value could be right by accident.** A field with no
//!   `#[serde(default)]` has no default, so serde refuses a document that omits it and
//!   names the field. Required `String`s also carry `deserialize_with = "non_empty"`,
//!   since `chain = ""` is present, not absent.
//! - **A typo is a startup error, not a silent default.** Every table is
//!   `deny_unknown_fields`, and an unknown [`Sink`] backend is refused rather than
//!   falling back to another one.
//!
//! Where records end up. The backend is the table: `[sink.duckdb]` selects `DuckDB` and
//! holds its settings, `[sink.stdout]` selects printing and takes none. Required,
//! because one run with no store and the next with one are different deployments, and
//! exactly one table may be named. Every other field not shown below may be omitted.
//!
//! Named for [`crate::sink`] rather than for storage: a sink is where envelopes go, and
//! not all of them are kept. `stdout` is a sink that writes nowhere, and a webhook or a
//! Kafka topic would be one too.
//!
//! ```toml
//! [ingest]
//! chain = "base"
//! http_url = "https://base-rpc.publicnode.com"
//! ws_url = "wss://base-rpc.publicnode.com"
//!
//! [sink.duckdb]
//! path = "indexer.duckdb"
//!
//! [sink.duckdb.settings]
//! threads = "4"
//!
//! # ...or `[sink.stdout]`, to print the stream and open no store.
//!
//! [decode]
//! registry = "registry.toml"   # the contract catalog; optional
//! ```

use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde::de::{self, Unexpected};

/// Deserializes a required string, refusing an empty or whitespace-only one.
///
/// A `#[serde(default)]`-less field already makes absence an error, but a present empty
/// value is not absence: `http_url = ""` is a deployment that starts and connects to
/// nothing, and `chain = ""` stamps every event with a blank chain. Putting the check on
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
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    /// The chain to follow. Required: the pipeline starts at ingest.
    pub ingest: IngestSettings,
    /// Where records go. Required: a run with nowhere to send them is not a deployment,
    /// and the table that is named is what says which one.
    pub sink: Sink,
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
/// and serde refuses a table that omits them, naming the field.
#[derive(Debug, Deserialize)]
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
}

/// Where records go, and by which backend.
///
/// The backend *is* the table, which is what makes `duckdb`-specific keys unambiguous:
/// `[sink.duckdb.settings]` is inside the table that named `DuckDB`, so switching to
/// another backend means switching tables, and a key that only `DuckDB` understands has
/// nowhere else to sit. The variant is load-bearing, not a label — `runtime` matches on
/// it rather than reading a field of a table the tag already selected.
///
/// Exactly one table may be named, and naming none is an error: a run with no store and
/// the next with one are different deployments, and there is no default that is right for
/// both. Serde refuses an unknown backend, so a typo here is a startup error rather than
/// a silent fallback.
///
/// A sink is not necessarily a store. [`Sink::Stdout`] keeps nothing, and that is the
/// point of the wider name: the layer is where envelopes go, and a webhook or a Kafka
/// topic would sit here beside the databases.
///
/// Each backend's settings live in that backend's sink — `DuckDB`'s are
/// [`DuckDbSettings`](crate::sink::duckdb::DuckDbSettings), `stdout`'s are
/// [`StdoutSettings`](crate::sink::stdout::StdoutSettings) — so where to look for a
/// backend's keys is one rule, and this file says only which backends exist. The
/// `duckdb` variant is behind its feature, so a build without the engine does not carry
/// a store it cannot open: there, naming `[sink.duckdb]` is an unknown backend — a
/// startup error naming the table, rather than settings that parse and fail later.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sink {
    /// Print the stream as newline-delimited JSON, for watching what the indexer would
    /// write. No store is opened and nothing is persisted.
    ///
    /// A payload rather than a unit variant so a key under `[sink.stdout]` is answered
    /// with what is valid — which is nothing — rather than an empty `available keys:`
    /// list. See [`StdoutSettings`](crate::sink::stdout::StdoutSettings).
    Stdout(crate::sink::stdout::StdoutSettings),
    /// An embedded `DuckDB` database, opened and committed by its own task.
    ///
    /// Renamed explicitly because `snake_case` would spell the table `duck_db`, and the
    /// engine's own name is the one operators already know.
    #[cfg(feature = "duckdb")]
    #[serde(rename = "duckdb")]
    DuckDb(crate::sink::duckdb::DuckDbSettings),
}

/// What the decode stage decodes, in its own file.
///
/// Kept out of the indexer's settings because it is a different kind of thing: the
/// settings file is deployment topology — endpoints, paths — and the registry
/// is a catalog of contracts that grows on its own schedule. It also rotates addresses,
/// which would otherwise churn the settings file's diff on every new protocol.
///
/// A table, not an `Option`, because decode always runs: an absent `registry` means an
/// empty registry and nothing is decoded, which is a legitimate way to run and is said at
/// startup rather than being silent. See [`crate::decode::registry`] for the file's shape.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DecodeSettings {
    /// The registry file, relative to the settings file's directory. Absent means an
    /// empty registry.
    pub registry: Option<PathBuf>,
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
}

impl std::str::FromStr for Settings {
    /// [`SettingsError::Parse`]; there is no file to read in this form.
    type Err = SettingsError;

    /// Parses settings from TOML text, filling defaults and rejecting what is required
    /// but absent.
    ///
    /// This is the in-memory form of [`Settings::from_file`], for a caller that has the
    /// text already. A relative path in the text resolves against the process's working
    /// directory rather than a file's, since there is no file; prefer
    /// [`Settings::from_file`].
    fn from_str(text: &str) -> Result<Self, SettingsError> {
        toml::from_str(text).map_err(|source| SettingsError::Parse { source })
    }
}

impl Settings {
    /// Reads settings from a TOML file.
    ///
    /// # Errors
    ///
    /// Returns [`SettingsError::Read`] when the file cannot be read and
    /// [`SettingsError::Parse`] when it does not parse or a required value is absent. A
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
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::path::PathBuf;
    use std::str::FromStr as _;

    use super::{Settings, Sink};
    use crate::sink::duckdb::DuckDbSettings;

    /// The minimum a file needs: a chain, its endpoints, and a store.
    fn minimal() -> &'static str {
        r#"
[ingest]
chain = "base"
http_url = "https://example.invalid"
ws_url = "wss://example.invalid"

[sink.duckdb]
"#
    }

    /// The same minimum with the store left to the caller, for a test that names a
    /// backend of its own.
    fn ingest_only() -> &'static str {
        r#"
[ingest]
chain = "base"
http_url = "https://example.invalid"
ws_url = "wss://example.invalid"
"#
    }

    /// Everything not required takes a default, so a small file runs a standard
    /// deployment without listing the obvious.
    #[test]
    fn a_minimal_file_supplies_every_default() {
        let settings = Settings::from_str(minimal()).expect("minimal settings parse");

        assert!(settings.decode.registry.is_none(), "nothing is decoded");
        // Compared against the sink's own defaults, so a retune there cannot leave this
        // asserting a value the crate no longer uses.
        let defaults = DuckDbSettings::default();
        let Sink::DuckDb(duckdb) = &settings.sink else {
            panic!("the table names the backend: {settings:?}");
        };
        assert_eq!(duckdb.path, defaults.path);
        assert_eq!(duckdb.batch_records, defaults.batch_records);
        assert!(
            duckdb.settings.is_empty(),
            "no engine settings unless asked"
        );
    }

    /// The shipped `indexer.toml` is an example an operator copies, so it must stay
    /// valid: a moved key would otherwise break the file the README points at.
    #[test]
    fn the_repository_settings_file_parses() {
        Settings::from_str(include_str!("../indexer.toml")).expect("indexer.toml parses");
    }

    /// The fixture file yields the values each stage is built from — the endpoints, the
    /// batch, and the resolved registry path — so the seam between the file and the
    /// stages is exercised without a store.
    #[test]
    fn the_repository_file_yields_the_values_the_stages_are_built_from() {
        let settings =
            Settings::from_str(include_str!("../indexer.toml")).expect("indexer.toml parses");

        assert_eq!(settings.ingest.chain, "base");
        assert!(!settings.ingest.http_url.is_empty());
        assert!(!settings.ingest.ws_url.is_empty());
        let Sink::DuckDb(duckdb) = &settings.sink else {
            panic!("the table names the backend: {settings:?}");
        };
        assert_eq!(
            duckdb.batch_records,
            DuckDbSettings::default().batch_records
        );
        // The fixture names `registry.toml`. Parsed from text it has no directory, so the
        // path is left as written rather than resolved against the working directory.
        assert_eq!(
            settings.registry_path(),
            Some(PathBuf::from("registry.toml"))
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
            format!("{}\n[decode]\nregistry = \"registry.toml\"\n", minimal()),
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
        let settings = Settings::from_str(&format!(
            "{}\n[decode]\nregistry = \"registry.toml\"\n",
            minimal()
        ))
        .expect("settings parse");
        assert_eq!(
            settings.registry_path(),
            Some(PathBuf::from("registry.toml"))
        );
    }

    /// Ingest is where the pipeline starts, so a file without it is an error rather than
    /// a process with nothing to do.
    #[test]
    fn a_file_without_ingest_is_an_error() {
        let error = Settings::from_str("[sink.duckdb]\n")
            .expect_err("ingest is required")
            .to_string();
        assert!(error.contains("ingest"), "the error names it: {error}");
    }

    /// A file with no `[sink.<backend>]` has nowhere to write, and the table is what
    /// says which write — so one is required, rather than defaulted to a backend nobody
    /// chose.
    #[test]
    fn a_file_without_sink_is_an_error() {
        let error = Settings::from_str(ingest_only())
            .expect_err("a sink is required")
            .to_string();
        assert!(error.contains("sink"), "the error names it: {error}");
    }

    /// `stdout` is a backend, not an ingest flag: it is where records go, and it is
    /// reached by naming its table rather than by a bool on the wrong one.
    #[test]
    fn a_stdout_backend_is_named_by_its_table() {
        let settings = Settings::from_str(&format!("{}\n[sink.stdout]\n", ingest_only()))
            .expect("settings parse");

        assert!(
            matches!(settings.sink, Sink::Stdout(_)),
            "[sink.stdout] is the no-store run: {settings:?}"
        );
    }

    /// `stdout` needs no engine, so it is a backend in every build. Gating the `DuckDB`
    /// variant instead of the whole enum keeps the no-engine build able to do the one
    /// thing it can: watch the stream.
    #[test]
    fn a_stdout_backend_needs_no_duckdb_feature() {
        let settings = Settings::from_str(&format!("{}\n[sink.stdout]\n", ingest_only()))
            .expect("settings parse");

        assert!(
            matches!(settings.sink, Sink::Stdout(_)),
            "the no-store run is available whatever the features: {settings:?}"
        );
    }

    /// Two backends named at once is a contradiction, and picking one silently is exactly
    /// the kind of wrong answer that looks like a working deployment.
    ///
    /// The wording is serde's — an externally tagged enum is a one-key map, so this reads
    /// as an element count rather than a table count. Asserting the span instead: the
    /// error has to point at the first table, which is what a reader needs to see.
    #[test]
    fn two_sink_backends_are_rejected() {
        let error = Settings::from_str(&format!(
            "{}\n[sink.stdout]\n\n[sink.duckdb]\npath = \"/tmp/x.duckdb\"\n",
            ingest_only()
        ))
        .expect_err("one backend is one table")
        .to_string();
        assert!(
            error.contains("[sink.stdout]") && error.contains("1 element"),
            "the error points at the first table: {error}"
        );
    }

    /// A `stdout` run takes no settings, so a key under it is a leftover from a backend
    /// that is no longer selected — not something to ignore.
    ///
    /// The message has to be readable, because it is all the operator gets. It names the
    /// key and says there is nothing to set, rather than trailing an empty list of valid
    /// keys, which is what an empty payload used to render.
    #[test]
    fn a_setting_under_the_stdout_table_is_rejected() {
        let error =
            Settings::from_str(&format!("{}\n[sink.stdout]\npath = \"x\"\n", ingest_only()))
                .expect_err("stdout stores nothing")
                .to_string();
        assert!(
            error.contains("unknown field `path`") && error.contains("no fields"),
            "the error names the key and says nothing is valid: {error}"
        );
    }

    /// A present-but-empty required value is refused too. Serde's requiredness catches an
    /// absent key; `chain = ""` would otherwise stamp every event with a blank chain.
    #[test]
    fn an_empty_required_value_is_an_error() {
        let error = Settings::from_str(
            r#"
[ingest]
chain = "  "
http_url = "https://example.invalid"
ws_url = "wss://example.invalid"
"#,
        )
        .expect_err("a blank chain is not a chain")
        .to_string();
        assert!(error.contains("chain"), "the error names it: {error}");
    }

    /// So is an endpoint: a chain with nowhere to read is not a deployable state, and
    /// `http_url`/`ws_url` carry no default.
    #[test]
    fn a_missing_endpoint_is_an_error() {
        let error = Settings::from_str("[ingest]\nchain = \"base\"\n")
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
        let error = Settings::from_str(&format!("{}stdoot = true\n", minimal()))
            .expect_err("a typo must not be ignored")
            .to_string();
        assert!(error.contains("stdoot"), "the error names the key: {error}");
    }

    /// A settings file from before the bus was removed still names `[bus]`. It must fail
    /// naming the table, not start a pipeline that ignores a transport it was told to use.
    #[test]
    fn the_removed_bus_table_is_rejected() {
        let error = Settings::from_str(&format!("{}\n[bus]\nkind = \"kafka\"\n", minimal()))
            .expect_err("a removed table must not be ignored")
            .to_string();
        assert!(error.contains("bus"), "the error names the table: {error}");
    }

    /// The backend is the table, so a backend this build does not have is named by a
    /// table that does not parse — a startup error rather than a silent fallback.
    #[test]
    fn an_unknown_sink_backend_is_rejected() {
        let error = Settings::from_str(&format!(
            "{}\n[sink.clickhouse]\nurl = \"y\"\n",
            ingest_only()
        ))
        .expect_err("an unknown backend must not fall back to duckdb")
        .to_string();
        assert!(error.contains("clickhouse"), "{error}");
    }

    /// A backend's own keys live in the backend's own table, so a misspelled one is
    /// caught inside the table that owns it rather than being silently accepted.
    #[test]
    fn a_misspelled_backend_key_is_rejected() {
        let error = Settings::from_str(&format!("{}\npah = \"/tmp/x.duckdb\"\n", minimal()))
            .expect_err("a typo inside the backend must not be ignored")
            .to_string();
        assert!(error.contains("pah"), "the error names the key: {error}");
    }

    /// `DuckDB`'s engine settings nest under its own table, so a key that only `DuckDB`
    /// understands has nowhere else to sit, and the runtime reads them from the backend
    /// that was named.
    #[test]
    fn duckdb_engine_settings_nest_under_the_duckdb_table() {
        let settings = Settings::from_str(&format!(
            r#"{}
path = "/tmp/custom.duckdb"

[sink.duckdb.settings]
threads = "4"
"#,
            minimal()
        ))
        .expect("settings parse");

        let Sink::DuckDb(duckdb) = &settings.sink else {
            panic!("the table names the backend: {settings:?}");
        };
        assert_eq!(duckdb.path, PathBuf::from("/tmp/custom.duckdb"));
        assert_eq!(duckdb.settings.get("threads"), Some(&"4".to_owned()));
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

[sink.duckdb]
path = "/tmp/custom.duckdb"
batch_records = 50
"#,
        )
        .expect("settings parse");

        assert_eq!(settings.ingest.chain, "ethereum");
        let Sink::DuckDb(duckdb) = &settings.sink else {
            panic!("the table names the backend: {settings:?}");
        };
        assert_eq!(duckdb.path, PathBuf::from("/tmp/custom.duckdb"));
        assert_eq!(duckdb.batch_records, 50);
    }
}
