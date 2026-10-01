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
//! because a present empty value — `chain = ""` — is not absence and would otherwise
//! start a process that stamps every event with a blank chain. A defaulted field carries
//! `#[serde(default)]`: the value's `Default` when that is the right one (`PathBuf`,
//! `BTreeMap`, an `Option` that means "unset"), or a `default_*` function when it is not.
//!
//! Required means no value could be right by accident. An endpoint or a chain name that
//! is guessed produces a process that starts, looks healthy, and indexes the wrong
//! thing, so the field is simply absent from the defaults.
//!
//! # Grouping
//!
//! A table names what owns its fields, not where a field was first needed:
//!
//! - `[ingest]` — the chain to follow and its endpoints. Required: with nothing to
//!   follow there is nothing to run.
//! - `[runtime]` — how storage commits: the most records one commit may cover.
//! - `[storage.<kind>]` — one backend's own settings, chosen by `[storage] kind`.
//!   Backend-specific keys stay out of the generic table so a second backend is an
//!   addition rather than an ambiguity about which `database` is meant.
//! - `[decode]` — the contract registry decode uses.
//!
//! # Example
//!
//! ```toml
//! [ingest]
//! chain = "base"
//! http_url = "https://base-rpc.publicnode.com"
//! ws_url = "wss://base-rpc.publicnode.com"
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
    /// How storage commits.
    #[serde(default)]
    pub runtime: RuntimeSettings,
    /// Where records are persisted.
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
/// rather than a deployment setting: it changes where the stream goes, not what the
/// process is.
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
    /// Print the stream to stdout instead of storing it, for watching what the indexer
    /// would write. No store is opened.
    #[serde(default)]
    pub stdout: bool,
}

/// How storage commits.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RuntimeSettings {
    /// The most records one store commit may cover.
    ///
    /// Storage receives one block at a time and commits each as it arrives. When it has
    /// fallen behind, it folds the blocks already waiting into one commit until this many
    /// records are reached, so a stalled store catches up in fewer, larger transactions.
    /// A block is never split, so a commit can run past the bound by up to one block.
    pub batch_records: usize,
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self {
            batch_records: DEFAULT_BATCH_RECORDS,
        }
    }
}

/// Where decoded records are stored, and which backend writes them.
///
/// The backend-specific settings live under the backend's own table, so this table stays
/// generic. [`StorageKind`] is the tag: exactly one backend is selected, and adding
/// `ClickHouse` or Postgres is a variant there plus its own table here, not another
/// top-level `database` whose owner a reader has to guess.
#[derive(Debug, Default, Deserialize)]
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
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DuckDbSettings {
    /// The `DuckDB` database file to write. Defaults to `indexer.duckdb`.
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

impl std::str::FromStr for Settings {
    type Err = anyhow::Error;

    /// Parses settings from TOML text, filling defaults and rejecting what is required
    /// but absent.
    ///
    /// This is the in-memory form of [`Settings::from_file`], for a caller that has the
    /// text already; prefer reading a file.
    fn from_str(text: &str) -> anyhow::Result<Self> {
        Ok(toml::from_str(text)?)
    }
}

impl Settings {
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
}

/// The default `DuckDB` path.
pub(crate) const DEFAULT_DATABASE: &str = "indexer.duckdb";

/// The default for [`RuntimeSettings::batch_records`].
pub(crate) const DEFAULT_BATCH_RECORDS: usize = 500;

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::str::FromStr as _;

    use super::{DEFAULT_BATCH_RECORDS, DEFAULT_DATABASE, Settings};

    /// The minimum a file needs: a chain and its endpoints.
    fn minimal() -> &'static str {
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

        assert!(!settings.ingest.stdout, "stdout is off unless asked for");
        assert_eq!(settings.runtime.batch_records, DEFAULT_BATCH_RECORDS);
        assert_eq!(settings.storage.kind, super::StorageKind::Duckdb);
        assert_eq!(
            settings.storage.duckdb.path,
            std::path::PathBuf::from(DEFAULT_DATABASE)
        );
        assert!(settings.decode.registry.is_none(), "nothing is decoded");
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
        assert_eq!(settings.runtime.batch_records, DEFAULT_BATCH_RECORDS);
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
            Some(std::path::PathBuf::from("registry.toml"))
        );
    }

    /// Ingest is where the pipeline starts, so a file without it is an error rather than
    /// a process with nothing to do.
    #[test]
    fn a_file_without_ingest_is_an_error() {
        let error = Settings::from_str("[storage]\nkind = \"duckdb\"\n")
            .expect_err("ingest is required")
            .to_string();
        assert!(error.contains("ingest"), "the error names it: {error}");
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

    /// The store's kind is a tagged choice, and a backend this build does not have is a
    /// startup error rather than a silent fallback to the default.
    #[test]
    fn an_unknown_storage_kind_is_rejected() {
        let error = Settings::from_str(&format!(
            "{}\n[storage]\nkind = \"clickhouse\"\n",
            minimal()
        ))
        .expect_err("an unknown backend must not fall back to duckdb")
        .to_string();
        assert!(error.contains("clickhouse"), "{error}");
    }

    /// A backend's settings live under its own table: an engine key is not a top-level
    /// store key, and the path is not a generic `database`.
    #[test]
    fn duckdb_settings_live_under_the_duckdb_table() {
        let settings = Settings::from_str(&format!(
            r#"{}
[storage]
kind = "duckdb"

[storage.duckdb]
path = "/tmp/custom.duckdb"

[storage.duckdb.settings]
threads = "4"
"#,
            minimal()
        ))
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
stdout = true

[runtime]
batch_records = 50

[storage.duckdb]
path = "/tmp/custom.duckdb"
"#,
        )
        .expect("settings parse");

        assert_eq!(settings.ingest.chain, "ethereum");
        assert!(settings.ingest.stdout);
        assert_eq!(settings.runtime.batch_records, 50);
        assert_eq!(
            settings.storage.duckdb.path,
            std::path::PathBuf::from("/tmp/custom.duckdb")
        );
    }
}
