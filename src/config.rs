//! The indexer's settings file, and the builders each stage takes.
//!
//! TOML, read once at startup. Three conventions carry the contract:
//!
//! - **Required means no value could be right by accident.** A field with no
//!   `#[serde(default)]` has no default, so serde refuses a document that omits it and
//!   names the field. Required `String`s also carry `deserialize_with = "non_empty"`,
//!   since `chain = ""` is present, not absent.
//! - **A typo is a startup error, not a silent default.** Every table is
//!   `deny_unknown_fields`, and an unknown [`Sink`] backend is refused rather than
//!   falling back to another one.
//! - **A secret is named, not written.** A [`Secret`] field takes `{ env = "NAME" }`, read
//!   from the environment as the file loads, so the file stays in git while the platform
//!   injects the value. A literal is accepted too, for a public endpoint or local
//!   development.
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
//! http_url = "https://base-rpc.publicnode.com"   # or { env = "INDEXER_HTTP_URL" }
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
//! protocols = "protocols"      # the protocol manifests; optional
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

/// A required, non-empty setting that is never logged: an RPC endpoint carrying an API
/// key, or a database connection string carrying a password.
///
/// Written either as the value itself or as the environment variable that holds it:
///
/// ```toml
/// http_url = "https://base-rpc.publicnode.com"   # the value, for a public or local one
/// http_url = { env = "INDEXER_HTTP_URL" }        # read from the environment at load
/// ```
///
/// The variable is read once, when the settings load, so a deployment missing one fails at
/// startup naming it rather than at first use. The settings file names the variable and
/// the platform's secret manager injects it, so the file stays in git and the indexer
/// never depends on which manager that is. `Debug` prints `[redacted]`, so the settings
/// can be logged whole.
#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    /// The value, for the one call that has to hand it to a client. Never log it.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[redacted]")
    }
}

impl<'de> Deserialize<'de> for Secret {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct FromEnv {
            env: String,
        }

        #[derive(Deserialize)]
        #[serde(
            untagged,
            expecting = "a non-empty string, or `{ env = \"NAME\" }` naming an environment variable"
        )]
        enum Written {
            Value(String),
            FromEnv(FromEnv),
        }

        let value = match Written::deserialize(deserializer)? {
            Written::Value(value) => value,
            // The variable's value is never put in an error: `VarError::NotUnicode`
            // displays the bytes it rejected, which would print the secret.
            Written::FromEnv(FromEnv { env }) => match std::env::var(&env) {
                Ok(value) if value.trim().is_empty() => {
                    return Err(de::Error::custom(format!(
                        "environment variable `{env}` is empty"
                    )));
                }
                Ok(value) => value,
                Err(std::env::VarError::NotPresent) => {
                    return Err(de::Error::custom(format!(
                        "environment variable `{env}` is not set"
                    )));
                }
                Err(std::env::VarError::NotUnicode(_)) => {
                    return Err(de::Error::custom(format!(
                        "environment variable `{env}` is not valid UTF-8"
                    )));
                }
            },
        };
        if value.trim().is_empty() {
            return Err(de::Error::custom("a secret setting cannot be empty"));
        }
        Ok(Self(value))
    }
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
    /// The JSON-RPC endpoint used for blocks, receipts, and logs. Required; a [`Secret`], since
    /// a provider's URL usually carries its API key.
    pub http_url: Secret,
    /// The WebSocket endpoint used for heads. Required; a [`Secret`], like `http_url`.
    pub ws_url: Secret,
    /// Which datasets to fetch and store. Omitted means all four.
    #[serde(default)]
    pub datasets: crate::sink::Datasets,
    /// The first height to index on an empty store. Omitted starts at the observed head,
    /// which skips earlier history; a value indexes that height and every one after it
    /// before following live heads. A value above the sampled head is a startup error
    /// rather than a silent clamp. A store that already holds accepted blocks resumes
    /// after them instead, and a value set alongside one is a startup error: remove it
    /// once the first run has committed.
    #[serde(default)]
    pub start_block: Option<u64>,
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
    /// A remote `PostgreSQL` database, using asynchronous transactional COPY.
    #[cfg(feature = "postgres")]
    Postgres(crate::sink::postgres::PostgresSettings),
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

    use super::Settings;

    /// The minimum a file needs: a chain, its endpoints, and a store.
    fn minimal() -> &'static str {
        r#"
[ingest]
chain = "base"
http_url = "https://example.invalid"
ws_url = "wss://example.invalid"

[sink.stdout]
"#
    }

    /// `indexer.example.toml` documents every key, so it must parse as shipped; it names
    /// `[sink.stdout]`, which every build has. It is the fixture rather than
    /// `indexer.toml`, which is a deployment's own settings and changes with it.
    #[test]
    fn the_example_settings_file_parses() {
        Settings::from_str(include_str!("../indexer.example.toml"))
            .expect("indexer.example.toml parses");
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
"#,
        )
        .expect("both forms parse");

        assert_eq!(
            settings.ingest.http_url.expose(),
            "https://example.invalid/v2/literal-key"
        );
        assert_eq!(settings.ingest.ws_url.expose(), path);
        let printed = format!("{settings:?}");
        assert!(
            !printed.contains("literal-key") && !printed.contains(&path),
            "{printed}"
        );
    }
}
