use std::env::VarError;

use super::{Settings, SettingsError};
use crate::sink::route::{Backend, Tables};

/// An environment with nothing set, so a test reads only the variables it stands in.
fn unset(_: &str) -> Result<String, VarError> {
    Err(VarError::NotPresent)
}

/// `text` parsed with nothing in the environment.
fn parse(text: &str) -> Result<Settings, SettingsError> {
    Settings::parse(text, unset)
}

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
/// `[sink.stdout]`, which every build has, and comments out the stores.
#[test]
fn the_example_settings_file_assembles() {
    let settings =
        parse(include_str!("../../indexer.example.toml")).expect("indexer.example.toml parses");
    crate::runtime::Pipeline::from_settings(&settings).expect("indexer.example.toml assembles");
}

/// Every settings file in the repository — each chain production runs, and any added
/// later — parses and assembles with a stand-in for every variable it names. A typo
/// in one is otherwise found only at deploy, where it exits 78 and the supervisor,
/// which does not restart on that code, leaves the chain down. Found by listing the
/// directory, so a new chain's file is covered without being named here; a
/// gitignored `*.local.toml` is left out.
#[cfg(all(feature = "postgres", feature = "delta"))]
#[test]
fn every_settings_file_in_the_repository_assembles() {
    // Every variable set, to a value any secret field takes.
    let stand_in: super::Env = |_| Ok("https://stand-in.invalid".to_owned());

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files: Vec<_> = std::fs::read_dir(root)
        .expect("read the crate root")
        .map(|entry| entry.expect("a directory entry").path())
        .filter(|path| {
            let name = path.file_name().and_then(|name| name.to_str());
            let parts: Vec<_> = name
                .map(|name| name.split('.').collect())
                .unwrap_or_default();
            matches!(parts.as_slice(), ["indexer", middle, "toml"] if *middle != "local")
        })
        .collect();
    files.sort();
    let names: Vec<_> = files
        .iter()
        .filter_map(|path| path.file_name()?.to_str())
        .collect();
    for shipped in [
        "indexer.bsc.toml",
        "indexer.ethereum.toml",
        "indexer.robinhood.toml",
    ] {
        assert!(names.contains(&shipped), "{shipped} is listed: {names:?}");
    }
    for path in &files {
        let shown = path.display();
        let settings = Settings::from_file(path, stand_in)
            .unwrap_or_else(|error| panic!("{shown} parses: {error}"));
        crate::runtime::Pipeline::from_settings(&settings)
            .unwrap_or_else(|error| panic!("{shown} assembles: {error}"));
    }
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

    let settings = Settings::from_file(&path, unset).expect("settings load");
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
        let settings = Settings::from_file(&path, unset).expect("settings load");
        let duckdb = settings.sink.duckdb.expect("a DuckDB sink");
        assert_eq!(duckdb.path, resolved);
    }
    std::fs::remove_dir_all(&dir).expect("clean up");
}

/// A local lake's directory resolves against the settings file's directory too, and a
/// URL is left as written.
#[cfg(feature = "delta")]
#[test]
fn a_local_delta_uri_resolves_against_the_settings_file() {
    let dir = std::env::temp_dir().join(format!("indexer-delta-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let path = dir.join("indexer.toml");
    for (written, resolved) in [
        ("lake", dir.join("lake").display().to_string()),
        ("/srv/lake", "/srv/lake".to_owned()),
        ("s3://bucket/prefix", "s3://bucket/prefix".to_owned()),
        ("file:///srv/lake", "file:///srv/lake".to_owned()),
    ] {
        let text = minimal().replace(
            "[sink.stdout]",
            &format!("[sink.delta]\nuri = \"{written}\""),
        );
        std::fs::write(&path, text).expect("write settings");
        let settings = Settings::from_file(&path, unset).expect("settings load");
        let delta = settings.sink.delta.expect("a Delta sink");
        assert_eq!(delta.uri, resolved, "{written}");
    }
    std::fs::remove_dir_all(&dir).expect("clean up");
}

/// Why `text` is refused, which must be a parse failure: the message names the key.
fn parse_refusal(text: &str) -> String {
    match parse(text) {
        Err(error @ SettingsError::Parse { .. }) => error.to_string(),
        other => panic!("expected a parse failure, got {other:?}"),
    }
}

/// `names` as a sink's `tables` list.
fn tables(names: &[&str]) -> Tables {
    serde_json::from_value(serde_json::json!(names)).expect("a tables list")
}

/// Every sink lists its tables, at least one sink is named, and printing runs alone.
#[test]
fn sinks_name_their_tables_and_stdout_runs_alone() {
    let settings = parse(minimal()).expect("stdout alone parses");
    assert_eq!(
        settings.sink.tables(),
        [(Backend::Stdout, &tables(&["blocks"]))]
    );

    let without_sink = minimal().replace("[sink.stdout]\ntables = [\"blocks\"]\n", "[sink]\n");
    assert!(
        matches!(parse(&without_sink), Err(SettingsError::NoSink)),
        "no sink"
    );
    for (case, text, refusal) in [
        (
            "without tables",
            minimal().replace("tables = [\"blocks\"]\n", ""),
            "missing field `tables`",
        ),
        (
            "empty tables",
            minimal().replace("[\"blocks\"]", "[]"),
            "at least one table",
        ),
        (
            "a blank table",
            minimal().replace("[\"blocks\"]", "[\" \"]"),
            "a table name",
        ),
        (
            "an unknown backend",
            format!("{}\n[sink.kafka]\ntables = [\"logs\"]\n", minimal()),
            "unknown field `kafka`",
        ),
    ] {
        let message = parse_refusal(&text);
        assert!(message.contains(refusal), "{case}: {message}");
    }
}

/// `[ingest]` with `keys` after its chain, and a sink.
fn ingest(keys: &str) -> String {
    format!("[ingest]\nchain = \"base\"\n{keys}\n[sink.stdout]\ntables = [\"blocks\"]\n")
}

const RPC: &str = "http_url = \"https://example.invalid\"\nws_url = \"wss://example.invalid\"\n";
const HYPERSYNC: &str =
    "[ingest.hypersync]\nurl = \"https://4663.hypersync.xyz\"\ntoken = \"secret-token\"\n";

/// `[ingest]` names one source: both RPC endpoints, or `HyperSync`, and only the keys
/// that source reads.
#[test]
fn ingest_names_one_source() {
    let settings = parse(&ingest(RPC)).expect("rpc alone parses");
    let super::IngestSource::Rpc {
        http_url, ws_url, ..
    } = &settings.ingest.source
    else {
        panic!("expected RPC, got {:?}", settings.ingest.source);
    };
    assert_eq!(
        (http_url.expose(), ws_url.expose()),
        ("https://example.invalid", "wss://example.invalid")
    );

    let settings = parse(&ingest(HYPERSYNC)).expect("hypersync alone parses");
    let super::IngestSource::HyperSync(source) = &settings.ingest.source else {
        panic!("expected HyperSync, got {:?}", settings.ingest.source);
    };
    assert_eq!(
        (source.url.as_str(), source.token.expose()),
        ("https://4663.hypersync.xyz", "secret-token")
    );
    assert!(!format!("{settings:?}").contains("secret-token"));

    for (case, keys, refusal) in [
        (
            "both",
            format!("{RPC}{HYPERSYNC}"),
            "replaces http_url and ws_url",
        ),
        ("neither", String::new(), "name http_url and ws_url"),
        (
            "http_url alone",
            "http_url = \"https://example.invalid\"\n".to_owned(),
            "named together",
        ),
        (
            "ws_url alone",
            "ws_url = \"wss://example.invalid\"\n".to_owned(),
            "named together",
        ),
        (
            "concurrency beside hypersync",
            format!("backfill_concurrency = 2\n{HYPERSYNC}"),
            "backfill_concurrency is for http_url",
        ),
    ] {
        let message = parse_refusal(&ingest(&keys));
        assert!(message.contains(refusal), "{case}: {message}");
    }
}

/// Each source's tuning is taken as written within its bounds, and refused outside
/// them: `finality_depth` is at most the deepest fork a run retracts, and no count may
/// be zero.
#[test]
fn ingest_tuning_is_bounded() {
    let depth = |depth: &str| ingest(&format!("finality_depth = {depth}\n{RPC}"));
    for (written, read) in [("128", 128), ("4096", 4_096)] {
        let settings = parse(&depth(written)).expect(written);
        assert_eq!(settings.ingest.finality_depth.get(), read, "{written}");
    }

    let settings =
        parse(&ingest(&format!("backfill_concurrency = 2\n{RPC}"))).expect("a tuned rpc parses");
    let super::IngestSource::Rpc {
        backfill_concurrency,
        ..
    } = settings.ingest.source
    else {
        panic!("expected RPC");
    };
    assert_eq!(backfill_concurrency.get(), 2);

    let tuned = format!("{HYPERSYNC}span = 500\nconcurrency = 4\n");
    let settings = parse(&ingest(&tuned)).expect("a tuned hypersync parses");
    let super::IngestSource::HyperSync(source) = &settings.ingest.source else {
        panic!("expected HyperSync");
    };
    assert_eq!((source.span.get(), source.concurrency.get()), (500, 4));

    for (case, text, refusal) in [
        ("finality_depth = 0", depth("0"), "nonzero"),
        ("finality_depth = 4097", depth("4097"), "at most 4,096"),
        (
            "backfill_concurrency = 0",
            ingest(&format!("backfill_concurrency = 0\n{RPC}")),
            "nonzero",
        ),
        (
            "span = 0",
            ingest(&format!("{HYPERSYNC}span = 0\n")),
            "nonzero",
        ),
        (
            "concurrency = 0",
            ingest(&format!("{HYPERSYNC}concurrency = 0\n")),
            "nonzero",
        ),
    ] {
        let message = parse_refusal(&text);
        assert!(message.contains(refusal), "{case}: {message}");
    }
}

/// Printing keeps no ledger, so it cannot resume beside a store.
#[cfg(feature = "duckdb")]
#[test]
fn stdout_beside_a_store_is_refused() {
    let both = format!("{}\n[sink.duckdb]\ntables = [\"logs\"]\n", minimal());
    assert!(matches!(
        parse(&both),
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
    let settings = parse(&text).expect("settings parse");
    let (duckdb, postgres, delta) = (
        tables(&["blocks"]),
        tables(&["contract_events"]),
        tables(&["logs", "decoded_logs"]),
    );
    assert_eq!(
        settings.sink.tables(),
        [
            (Backend::DuckDb, &duckdb),
            (Backend::Postgres, &postgres),
            (Backend::Delta, &delta)
        ]
    );
}

/// A secret is the value or the environment variable that holds it, and either way it
/// is never printed: a deployment logs its settings whole. The variable is read from
/// the environment the parse is given, and one it lacks is refused by name.
#[test]
fn a_secret_is_written_or_read_from_the_environment_and_never_printed() {
    /// Only `INDEXER_WS_URL` is set.
    fn env(name: &str) -> Result<String, VarError> {
        match name {
            "INDEXER_WS_URL" => Ok("wss://example.invalid/v2/env-key".to_owned()),
            _ => Err(VarError::NotPresent),
        }
    }
    let text = r#"
[ingest]
chain = "base"
http_url = "https://example.invalid/v2/literal-key"
ws_url = { env = "INDEXER_WS_URL" }

[sink.stdout]
tables = ["blocks"]
"#;
    let settings = Settings::parse(text, env).expect("both forms parse");
    let from_env = "wss://example.invalid/v2/env-key";

    let super::IngestSource::Rpc {
        http_url, ws_url, ..
    } = &settings.ingest.source
    else {
        panic!("expected an RPC source");
    };
    assert_eq!(http_url.expose(), "https://example.invalid/v2/literal-key");
    assert_eq!(ws_url.expose(), from_env);
    let printed = format!("{settings:?}");
    assert!(
        !printed.contains("literal-key") && !printed.contains("env-key"),
        "{printed}"
    );

    let message = parse_refusal(&text.replace("INDEXER_WS_URL", "INDEXER_UNSET"));
    assert!(
        message.contains("environment variable `INDEXER_UNSET` is not set"),
        "{message}"
    );
}
