//! Which tables each store writes, and the `Router` that hands each store its share.
//!
//! Every `[sink.<backend>]` lists its [`Tables`]: exact table names, globs over them, or
//! [`CONTRACT_EVENTS`] for every contract's decoded event tables. The list is resolved
//! against the run's [`Schema`] at startup, so a name that matches nothing is an error
//! rather than an empty table. The bookkeeping tables — `accepted_blocks`, `reorgs`,
//! `contracts` — are not listed: every store writes them (see
//! [`Table::is_bookkeeping`]).
//!
//! What the stores write is also what ingest fetches: [`datasets`] is the union, so a
//! dataset no store keeps is never fetched.
//!
//! ```text
//! DecodingSink ─▶ Router ─┬─▶ ChannelSink ═▶ delta
//!                         └─▶ ChannelSink ═▶ postgres
//! ```

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use serde::Deserialize;
use serde::de::{self, Deserializer};

use crate::ingest::source::Datasets;
use crate::sink::table::{Schema, Table, TableId};
use crate::sink::{EnvelopeSink, SinkError};
use crate::wire::envelope::Envelope;

/// The entry in [`Tables`] that names every contract's decoded event tables — one per
/// event of every protocol manifest, such as `uniswap_v3_pool_swap` — including those of
/// protocols added later.
pub const CONTRACT_EVENTS: &str = "contract_events";

/// A store's backend, which also names the store: a run writes at most one of each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// Standard output.
    Stdout,
    /// An embedded `DuckDB` database.
    DuckDb,
    /// A remote `PostgreSQL` database.
    Postgres,
    /// Delta Lake tables on object storage.
    Delta,
}

impl Backend {
    /// The backend's name, as its `[sink.<name>]` table spells it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::DuckDb => "duckdb",
            Self::Postgres => "postgres",
            Self::Delta => "delta",
        }
    }
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The tables one store writes, as its `[sink.<backend>]` lists them.
///
/// Each entry is a table's exact name (`logs`), a glob over table names with `*` for any
/// run of characters (`uniswap_v3_*`), or [`CONTRACT_EVENTS`]. Required and non-empty:
/// a store says what it holds rather than defaulting to everything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tables(Vec<String>);

impl<'de> Deserialize<'de> for Tables {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let entries = Vec::<String>::deserialize(deserializer)?;
        if entries.is_empty() {
            return Err(de::Error::invalid_length(0, &"at least one table"));
        }
        if let Some(blank) = entries.iter().find(|entry| entry.trim().is_empty()) {
            return Err(de::Error::invalid_value(
                de::Unexpected::Str(blank),
                &"a table name, a glob, or \"contract_events\"",
            ));
        }
        Ok(Self(entries))
    }
}

impl Tables {
    /// The tables `schema` holds that the entries name.
    ///
    /// # Errors
    ///
    /// Returns [`RouteError::Bookkeeping`] for an entry naming a table every store writes
    /// already, [`RouteError::Reserved`] when a table is itself named
    /// [`CONTRACT_EVENTS`], and [`RouteError::NoMatch`] for an entry that matches no
    /// table, which is how a typo, or `contract_events` with no protocols loaded, is
    /// caught.
    pub fn resolve(&self, schema: &Schema) -> Result<HashSet<TableId>, RouteError> {
        if schema
            .tables()
            .iter()
            .any(|def| def.name == CONTRACT_EVENTS)
        {
            return Err(RouteError::Reserved);
        }
        let mut selected = HashSet::new();
        for entry in &self.0 {
            if let Some(&table) = Table::ALL
                .iter()
                .find(|table| table.is_bookkeeping() && table.name() == entry)
            {
                return Err(RouteError::Bookkeeping { table });
            }
            let mut matched = false;
            for def in schema.tables() {
                let hit = match def.id {
                    TableId::Event(_) => entry == CONTRACT_EVENTS || glob(entry, &def.name),
                    TableId::Derived(_) => glob(entry, &def.name),
                    TableId::Dataset(table) => !table.is_bookkeeping() && glob(entry, &def.name),
                };
                if hit {
                    matched = true;
                    selected.insert(def.id);
                }
            }
            if !matched {
                return Err(RouteError::NoMatch {
                    entry: entry.clone(),
                });
            }
        }
        Ok(selected)
    }
}

/// Why a store's [`Tables`] could not be resolved.
#[derive(Debug, thiserror::Error)]
pub enum RouteError {
    /// An entry matches no table the run has.
    #[error("{entry:?} matches no table")]
    NoMatch {
        /// The entry as written.
        entry: String,
    },
    /// An entry names a table every store writes on its own.
    #[error("{table} is written to every store and cannot be listed")]
    Bookkeeping {
        /// The table.
        table: Table,
    },
    /// A generated table took the name of the [`CONTRACT_EVENTS`] entry.
    #[error("a table is named {CONTRACT_EVENTS:?}, which is reserved")]
    Reserved,
}

/// Whether `name` matches `pattern`, where `*` matches any run of characters, none
/// included, and everything else matches itself.
fn glob(pattern: &str, name: &str) -> bool {
    let mut parts = pattern.split('*');
    let Some(rest) = parts.next().and_then(|first| name.strip_prefix(first)) else {
        return false;
    };
    let parts: Vec<&str> = parts.collect();
    let Some((last, middle)) = parts.split_last() else {
        return rest.is_empty();
    };
    let mut rest = rest;
    for part in middle {
        let Some(at) = rest.find(part) else {
            return false;
        };
        rest = &rest[at + part.len()..];
    }
    rest.ends_with(last)
}

/// What ingest fetches so every store gets the tables it writes: blocks and
/// transactions when a store writes them, and logs when a store writes logs or anything
/// decoded or derived from them. A derived table also needs transactions, whose sender
/// and recipient its rows carry, whether or not a store writes them.
#[must_use]
pub fn datasets<'a>(selections: impl IntoIterator<Item = &'a HashSet<TableId>>) -> Datasets {
    let mut datasets = Datasets {
        blocks: false,
        transactions: false,
        logs: false,
    };
    for id in selections.into_iter().flatten() {
        match id {
            TableId::Dataset(Table::Block) => datasets.blocks = true,
            TableId::Dataset(Table::Transaction) => datasets.transactions = true,
            TableId::Dataset(Table::Log | Table::Decoded) | TableId::Event(_) => {
                datasets.logs = true;
            }
            TableId::Derived(_) => {
                datasets.logs = true;
                datasets.transactions = true;
            }
            TableId::Dataset(Table::Contract | Table::Reorg | Table::AcceptedBlock) => {}
        }
    }
    datasets
}

/// Hands each envelope to every sink that writes one of its tables.
///
/// A block, transaction, or log goes where its table is written. A decoded record goes
/// where its `decoded_logs` row or its event's own row is; each store keeps the rows it
/// writes. A derived record goes where its derived table is. The bookkeeping envelopes —
/// accepted blocks, reorgs, discovered contracts — go to every sink. An envelope is moved to the last sink that takes it and cloned
/// only for the others, so one store costs no copy.
///
/// Every sink sees every block boundary: [`flush`](EnvelopeSink::flush) reaches each,
/// including one that took nothing from the block, since its accepted-block marker went
/// everywhere.
#[derive(Debug)]
pub(crate) struct Router<K> {
    /// The run's schema, which names each decoded record's event table.
    schema: Arc<Schema>,
    /// Each sink and the tables it writes.
    sinks: Vec<(K, HashSet<TableId>)>,
}

impl<K> Router<K> {
    /// Routes to `sinks`, each writing its tables of `schema`.
    pub(crate) const fn new(schema: Arc<Schema>, sinks: Vec<(K, HashSet<TableId>)>) -> Self {
        Self { schema, sinks }
    }
}

impl<K: EnvelopeSink> EnvelopeSink for Router<K> {
    async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
        let (table, typed) = self.schema.tables_for(&envelope.event)?;
        let everywhere = matches!(table, TableId::Dataset(table) if table.is_bookkeeping());
        let takes = |written: &HashSet<TableId>| {
            everywhere
                || written.contains(&table)
                || typed.is_some_and(|typed| written.contains(&typed))
        };
        let Some(last) = self.sinks.iter().rposition(|(_, written)| takes(written)) else {
            return Ok(());
        };
        for (sink, written) in &mut self.sinks[..last] {
            if takes(written) {
                sink.publish(envelope.clone()).await?;
            }
        }
        self.sinks[last].0.publish(envelope).await
    }

    async fn flush(&mut self) -> Result<(), SinkError> {
        for (sink, _) in &mut self.sinks {
            sink.flush().await?;
        }
        Ok(())
    }

    fn observe_head(&mut self, height: u64) {
        for (sink, _) in &mut self.sinks {
            sink.observe_head(height);
        }
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use alloy_primitives::B256;

    use crate::ingest::source::Datasets;
    use crate::sink::fixtures;
    use crate::sink::table::{Schema, Table, TableId};
    use crate::sink::{EnvelopeSink, SinkError};
    use crate::wire::envelope::{BlockMeta, ChainId, Envelope, Event};

    use super::{RouteError, Router, Tables, datasets, glob};

    fn tables(entries: &[&str]) -> Tables {
        Tables(entries.iter().map(|&entry| entry.to_owned()).collect())
    }

    fn names(schema: &Schema, ids: &HashSet<TableId>) -> Vec<String> {
        let mut names: Vec<String> = schema
            .tables()
            .iter()
            .filter(|def| ids.contains(&def.id))
            .map(|def| def.name.clone())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn a_glob_matches_any_run_of_characters_and_nothing_else() {
        for (pattern, name, matches) in [
            ("logs", "logs", true),
            ("logs", "decoded_logs", false),
            ("*logs", "decoded_logs", true),
            ("uniswap_v3_*", "uniswap_v3_pool_swap", true),
            ("uniswap_*_swap", "uniswap_v3_pool_swap", true),
            ("*", "blocks", true),
            ("uniswap_*_mint", "uniswap_v3_pool_swap", false),
            // The prefix and suffix may not overlap.
            ("a*a", "a", false),
        ] {
            assert_eq!(glob(pattern, name), matches, "{pattern:?} against {name:?}");
        }
    }

    /// `contract_events` is every event table and no dataset or derived table; names and
    /// globs select from all of them.
    #[test]
    fn entries_resolve_to_tables() {
        let (schema, _) = fixtures::derived();
        let events = tables(&["contract_events"])
            .resolve(&schema)
            .expect("resolves");
        let every_event: HashSet<TableId> = schema
            .tables()
            .iter()
            .map(|def| def.id)
            .filter(|id| matches!(id, TableId::Event(_)))
            .collect();
        assert!(
            !every_event.is_empty(),
            "the shipped catalog has event tables"
        );
        assert_eq!(events, every_event);

        let raw = tables(&["blocks", "*logs"])
            .resolve(&schema)
            .expect("resolves");
        assert_eq!(names(&schema, &raw), ["blocks", "decoded_logs", "logs"]);

        let swaps = tables(&["uniswap_v3_pool_swap"])
            .resolve(&schema)
            .expect("resolves");
        assert_eq!(names(&schema, &swaps), ["uniswap_v3_pool_swap"]);

        let derived = tables(&["test_*"]).resolve(&schema).expect("resolves");
        assert_eq!(names(&schema, &derived), ["test_rows"]);
        assert!(
            !events.iter().any(|id| derived.contains(id)),
            "contract_events is decoded tables only"
        );
    }

    /// `*` selects every table a store may list, and no bookkeeping table, which every
    /// store writes already.
    #[test]
    fn a_glob_never_selects_a_bookkeeping_table() {
        let schema = fixtures::schema();
        let every = tables(&["*"]).resolve(&schema).expect("resolves");
        let listable: HashSet<TableId> = schema
            .tables()
            .iter()
            .map(|def| def.id)
            .filter(|id| !matches!(id, TableId::Dataset(table) if table.is_bookkeeping()))
            .collect();
        assert_eq!(every, listable);
        assert!(
            listable.len() < schema.tables().len(),
            "the schema has bookkeeping tables to leave out"
        );
    }

    #[test]
    fn an_entry_matching_nothing_or_naming_bookkeeping_is_refused() {
        let schema = fixtures::schema();
        assert!(matches!(
            tables(&["blocks", "trasactions"]).resolve(&schema),
            Err(RouteError::NoMatch { entry }) if entry == "trasactions"
        ));
        assert!(matches!(
            tables(&["accepted_blocks"]).resolve(&schema),
            Err(RouteError::Bookkeeping {
                table: Table::AcceptedBlock
            })
        ));
        let empty = Arc::new(Schema::new().expect("the dataset tables"));
        assert!(matches!(
            tables(&["contract_events"]).resolve(&empty),
            Err(RouteError::NoMatch { entry }) if entry == "contract_events"
        ));
    }

    #[test]
    fn an_empty_or_blank_list_does_not_parse() {
        for (json, reason) in [
            ("[]", "expected at least one table"),
            (r#"["logs", " "]"#, r#"invalid value: string " ""#),
        ] {
            let error = serde_json::from_str::<Tables>(json).expect_err(json);
            assert!(error.to_string().contains(reason), "{json}: {error}");
        }
        assert_eq!(
            serde_json::from_str::<Tables>(r#"["logs"]"#).expect("parses"),
            tables(&["logs"])
        );
    }

    /// Ingest fetches the union: logs whenever anything decoded or derived from them is
    /// written, and transactions too for a derived table.
    #[test]
    fn ingest_fetches_what_some_store_writes() {
        let (schema, _) = fixtures::derived();
        let resolve = |entries: &[&str]| tables(entries).resolve(&schema).expect("resolves");
        let fetch = |stores: &[&[&str]]| {
            let selections: Vec<_> = stores.iter().map(|entries| resolve(entries)).collect();
            datasets(&selections)
        };
        let only = |blocks, transactions, logs| Datasets {
            blocks,
            transactions,
            logs,
        };
        assert_eq!(fetch(&[&["contract_events"]]), only(false, false, true));
        assert_eq!(fetch(&[&["decoded_logs"]]), only(false, false, true));
        assert_eq!(fetch(&[&["test_rows"]]), only(false, true, true));
        assert_eq!(
            fetch(&[&["blocks", "transactions"], &["contract_events"]]),
            only(true, true, true)
        );
        assert_eq!(fetch(&[&["blocks"]]), only(true, false, false));
    }

    /// Records the dedupe keys it was handed and how many flushes it saw.
    #[derive(Debug, Default)]
    struct Seen {
        keys: Vec<String>,
        flushes: usize,
    }

    impl EnvelopeSink for Seen {
        async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
            self.keys.push(envelope.event.dedupe_key());
            Ok(())
        }

        async fn flush(&mut self) -> Result<(), SinkError> {
            self.flushes += 1;
            Ok(())
        }
    }

    fn envelope(event: Event) -> Envelope {
        Envelope::new(ChainId::new("base"), event)
    }

    /// Raw rows go where their table is, a decoded record wherever either of its tables
    /// is, a derived row where its table is, and the bookkeeping everywhere; every sink
    /// sees the flush.
    #[tokio::test]
    async fn each_sink_gets_the_envelopes_of_its_tables() {
        let (schema, row) = fixtures::derived();
        let resolve = |entries: &[&str]| tables(entries).resolve(&schema).expect("resolves");
        let mut router = Router::new(
            Arc::clone(&schema),
            vec![
                (Seen::default(), resolve(&["blocks", "logs"])),
                (Seen::default(), resolve(&["contract_events"])),
                (Seen::default(), resolve(&["transactions", "test_rows"])),
            ],
        );
        let block = Event::Block(Box::default());
        let log = Event::Log(Box::default());
        let decoded = Event::Decoded(Box::new(fixtures::decoded_swap()));
        let derived = Event::Derived(Box::new(row));
        let accepted = Event::AcceptedBlock(BlockMeta {
            height: 1,
            hash: B256::with_last_byte(1),
            parent_hash: B256::ZERO,
            timestamp: 1,
        });
        for event in [&block, &log, &decoded, &derived, &accepted] {
            router
                .publish(envelope(event.clone()))
                .await
                .expect("publish");
        }
        router.flush().await.expect("flush");

        let [raw, events, transactions] = &router.sinks[..] else {
            panic!("three sinks");
        };
        assert_eq!(
            raw.0.keys,
            [block.dedupe_key(), log.dedupe_key(), accepted.dedupe_key()]
        );
        assert_eq!(events.0.keys, [decoded.dedupe_key(), accepted.dedupe_key()]);
        assert_eq!(
            transactions.0.keys,
            [derived.dedupe_key(), accepted.dedupe_key()]
        );
        assert_eq!(
            router
                .sinks
                .iter()
                .map(|(sink, _)| sink.flushes)
                .collect::<Vec<_>>(),
            [1, 1, 1]
        );
    }
}
