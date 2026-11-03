//! Which tables each store writes, and the `Router` that hands each store its share.
//!
//! Every `[sink.<backend>]` lists its [`Tables`]: exact table names, globs over them, or
//! [`CONTRACT_EVENTS`] for every contract's decoded event tables. An entry written as
//! `{ matched = "<glob>" }` selects tables too, but takes only the rows of the pools
//! matched to Metric pools' pairs (see `crate::decode::pairs`), so a store can hold the
//! pools compared with Metric's of a protocol the lake holds whole, and an entry starting
//! with `!` takes its tables back out. The list is resolved against the run's [`Schema`]
//! at startup, so a name that matches nothing is an error rather than an empty table. The
//! bookkeeping tables, `accepted_blocks`, `reorgs`, `contracts`, `sessions`, `handoffs`,
//! and `matched_pools`, are not listed: every store writes them (see
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

use futures_util::future::try_join_all;
use serde::Deserialize;
use serde::de::{self, Deserializer};

use crate::decode::SharedKept;
use crate::sink::table::{Schema, Table, TableId};
use crate::sink::{EnvelopeSink, SinkError};
use crate::wire::datasets::Datasets;
use crate::wire::envelope::Envelope;

/// The entry in [`Tables`] that names every contract's decoded event tables — one per
/// event of every protocol manifest, such as `uniswap_v3_pool_swap` — including those of
/// protocols added later.
pub const CONTRACT_EVENTS: &str = "contract_events";

/// A sink's backend, which also names the sink: a run writes at most one of each.
///
/// What a settings file names. Everything past the settings that only a store does —
/// opening, resuming, draining — takes a [`StoreBackend`] instead, which cannot be
/// stdout.
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

/// A store's backend, which also names the store: the [`Backend`]s that keep what they
/// are handed, and so the only ones a store's open, resume, and drain are given.
///
/// Every variant exists in every build, whichever engines it has: only the
/// settings and the open are behind each engine's feature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreBackend {
    /// An embedded `DuckDB` database.
    DuckDb,
    /// A remote `PostgreSQL` database.
    Postgres,
    /// Delta Lake tables on object storage.
    Delta,
}

impl StoreBackend {
    /// The store's name, as its `[sink.<name>]` table spells it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::DuckDb => Backend::DuckDb.name(),
            Self::Postgres => Backend::Postgres.name(),
            Self::Delta => Backend::Delta.name(),
        }
    }
}

impl From<StoreBackend> for Backend {
    fn from(store: StoreBackend) -> Self {
        match store {
            StoreBackend::DuckDb => Self::DuckDb,
            StoreBackend::Postgres => Self::Postgres,
            StoreBackend::Delta => Self::Delta,
        }
    }
}

impl fmt::Display for StoreBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The tables one store writes, as its `[sink.<backend>]` lists them.
///
/// Each entry is a table's exact name (`logs`), a glob over table names with `*` for any
/// run of characters (`uniswap_v3_*`), or [`CONTRACT_EVENTS`]. Required and non-empty:
/// a store says what it holds rather than defaulting to everything.
///
/// An entry may also be `{ matched = "<name or glob>" }`: the tables it matches take only
/// the rows of the pools matched to Metric pools' pairs (see `crate::decode::pairs`).
/// Only contract event tables can be matched: theirs are the rows a matched pool's
/// history is filled into (see `crate::backfill`), so raw `logs`, `decoded_logs`, and a
/// derived table would hold a pool's rows only from when it was matched.
///
/// An entry starting with `!` takes the tables it matches back out of what the others
/// select, wherever it stands in the list, as `.gitignore`'s negation does: the store
/// writes every table some other entry selects and no `!` entry names.
///
/// ```toml
/// tables = [
///     "metric_v1_*",
///     "!metric_v1_oracle_provider_feed_updated",   # its derived table is enough
///     { matched = "uniswap_v3_pool_*" },
/// ]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tables(Vec<Entry>);

/// One entry of a [`Tables`] list.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
enum Entry {
    /// A name, a glob, or [`CONTRACT_EVENTS`]: every row of the tables it matches.
    All(String),
    /// A name or a glob whose tables take only the matched pools' rows.
    Matched {
        /// The name or glob.
        matched: String,
    },
    /// A name or a glob, written after a `!`, whose tables are taken out of the others'.
    #[serde(skip)]
    Excluded(String),
}

impl Entry {
    /// The name, glob, or [`CONTRACT_EVENTS`] as written.
    fn pattern(&self) -> &str {
        match self {
            Self::All(pattern) | Self::Matched { matched: pattern } | Self::Excluded(pattern) => {
                pattern
            }
        }
    }
}

impl<'de> Deserialize<'de> for Tables {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let entries: Vec<Entry> = Vec::<Entry>::deserialize(deserializer)?
            .into_iter()
            .map(|entry| match entry {
                Entry::All(pattern) => match pattern.strip_prefix('!') {
                    Some(excluded) => Entry::Excluded(excluded.to_owned()),
                    None => Entry::All(pattern),
                },
                entry => entry,
            })
            .collect();
        if entries.is_empty() {
            return Err(de::Error::invalid_length(0, &"at least one table"));
        }
        if !entries
            .iter()
            .any(|entry| !matches!(entry, Entry::Excluded(_)))
        {
            return Err(de::Error::invalid_value(
                de::Unexpected::Str("!…"),
                &"some table besides those taken out",
            ));
        }
        if let Some(blank) = entries
            .iter()
            .find(|entry| entry.pattern().trim().is_empty())
        {
            return Err(de::Error::invalid_value(
                de::Unexpected::Str(blank.pattern()),
                &"a table name, a glob, or \"contract_events\"",
            ));
        }
        Ok(Self(entries))
    }
}

/// What one sink takes, resolved from its [`Tables`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Selection {
    /// Every table it writes.
    pub tables: HashSet<TableId>,
    /// The tables of `tables` it takes only the matched pools' rows of: those only a `matched`
    /// entry selects. A table an entry selects whole is taken whole.
    pub matched: HashSet<TableId>,
}

impl Tables {
    /// The tables `schema` holds that the entries name and no `!` entry takes out, and
    /// those of them only `matched` entries name.
    ///
    /// # Errors
    ///
    /// Returns [`RouteError::Bookkeeping`] for an entry naming a table every store writes
    /// already, [`RouteError::Reserved`] when a table is itself named
    /// [`CONTRACT_EVENTS`], [`RouteError::NoMatch`] for an entry that matches no table,
    /// which is how a typo, or `contract_events` with no protocols loaded, is caught,
    /// and [`RouteError::NotAnEventTable`] for a `matched` entry matching a table that
    /// is not a contract event table.
    pub fn resolve(&self, schema: &Schema) -> Result<Selection, RouteError> {
        if schema
            .tables()
            .iter()
            .any(|def| def.name == CONTRACT_EVENTS)
        {
            return Err(RouteError::Reserved);
        }
        let mut whole = HashSet::new();
        let mut matched = HashSet::new();
        let mut excluded = HashSet::new();
        for entry in &self.0 {
            let pattern = entry.pattern();
            if let Some(&table) = Table::ALL
                .iter()
                .find(|table| table.is_bookkeeping() && table.name() == pattern)
            {
                return Err(RouteError::Bookkeeping { table });
            }
            let mut hits = false;
            for def in schema.tables() {
                let hit = match def.id {
                    TableId::Event(_) => pattern == CONTRACT_EVENTS || glob(pattern, &def.name),
                    TableId::Derived(_) => glob(pattern, &def.name),
                    TableId::Dataset(table) => !table.is_bookkeeping() && glob(pattern, &def.name),
                };
                if !hit {
                    continue;
                }
                hits = true;
                match entry {
                    Entry::All(_) => whole.insert(def.id),
                    Entry::Excluded(_) => excluded.insert(def.id),
                    Entry::Matched { .. } if matches!(def.id, TableId::Event(_)) => {
                        matched.insert(def.id)
                    }
                    Entry::Matched { .. } => {
                        return Err(RouteError::NotAnEventTable {
                            entry: pattern.to_owned(),
                            table: def.name.clone(),
                        });
                    }
                };
            }
            if !hits {
                return Err(RouteError::NoMatch {
                    entry: match entry {
                        Entry::Excluded(_) => format!("!{pattern}"),
                        Entry::All(_) | Entry::Matched { .. } => pattern.to_owned(),
                    },
                });
            }
        }
        matched.retain(|id| !whole.contains(id) && !excluded.contains(id));
        whole.retain(|id| !excluded.contains(id));
        whole.extend(&matched);
        Ok(Selection {
            tables: whole,
            matched,
        })
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
    /// A `matched` entry matches a table that is not a contract event table, the only
    /// tables a matched pool's history is filled into.
    #[error("{entry:?} is matched, but {table} is not a contract event table")]
    NotAnEventTable {
        /// The entry as written.
        entry: String,
        /// The table it matched.
        table: String,
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
pub fn datasets<'a>(selections: impl IntoIterator<Item = &'a Selection>) -> Datasets {
    let mut datasets = Datasets {
        blocks: false,
        transactions: false,
        logs: false,
    };
    for id in selections
        .into_iter()
        .flat_map(|selection| &selection.tables)
    {
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
            TableId::Dataset(
                Table::Contract
                | Table::Reorg
                | Table::AcceptedBlock
                | Table::Session
                | Table::Handoff
                | Table::Matched,
            ) => {}
        }
    }
    datasets
}

/// Hands each envelope to every sink that writes one of its tables.
///
/// A block, transaction, or log goes where its table is written. A decoded record goes
/// where its `decoded_logs` row or its event's own row is; each store keeps the rows it
/// writes. A derived record goes where its derived table is. The bookkeeping envelopes —
/// accepted blocks, reorgs, discovered contracts — go to every sink. An envelope is moved
/// to the last sink that takes it and cloned only for the others, so one store costs no
/// copy.
///
/// A log, decoded record, or derived row that is not a matched pool's goes to no sink
/// that keeps any of its tables to the matched pools' rows, so a decoded record is
/// left out of `decoded_logs` there too.
///
/// Every sink sees every block boundary: [`flush`](EnvelopeSink::flush) reaches each,
/// including one that took nothing from the block, since its accepted-block marker went
/// everywhere.
#[derive(Debug)]
pub(crate) struct Router<K> {
    /// The run's schema, which names each decoded record's event table.
    schema: Arc<Schema>,
    /// The matched pools, whose rows the matched tables keep, as the decode stage
    /// changes them.
    kept: SharedKept,
    /// Each sink and what it takes.
    sinks: Vec<(K, Selection)>,
}

impl<K> Router<K> {
    /// Routes to `sinks`, each writing its selection of `schema`, the matched tables only
    /// the rows of the pools `kept` holds.
    pub(crate) const fn new(
        schema: Arc<Schema>,
        kept: SharedKept,
        sinks: Vec<(K, Selection)>,
    ) -> Self {
        Self {
            schema,
            kept,
            sinks,
        }
    }
}

impl<K: EnvelopeSink> EnvelopeSink for Router<K> {
    async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
        let (table, typed) = self.schema.tables_for(&envelope.event)?;
        let everywhere = matches!(table, TableId::Dataset(table) if table.is_bookkeeping());
        let tables = || std::iter::once(table).chain(typed);
        // Read only when some sink keeps a table of the envelope to the matched pools.
        let mut kept = None;
        let mut keeps = || {
            *kept.get_or_insert_with(|| {
                self.kept
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .keeps(&envelope.event)
            })
        };
        // One bit per sink: a run has at most one sink of each backend.
        let mut takes = 0_u32;
        for (position, (_, selection)) in self.sinks.iter().enumerate() {
            let routed = everywhere
                || tables().any(|id| selection.tables.contains(&id))
                    && (!tables().any(|id| selection.matched.contains(&id)) || keeps());
            if routed {
                takes |= 1 << position;
            }
        }
        let Some(last) = takes.checked_ilog2().map(|last| last as usize) else {
            return Ok(());
        };
        for (position, (sink, _)) in self.sinks[..last].iter_mut().enumerate() {
            if takes & (1 << position) != 0 {
                sink.publish(envelope.clone()).await?;
            }
        }
        self.sinks[last].0.publish(envelope).await
    }

    /// Flushes every sink at once, so one whose channel is full — a store behind on its
    /// commits — does not hold the block back from the others.
    async fn flush(&mut self) -> Result<(), SinkError> {
        try_join_all(self.sinks.iter_mut().map(|(sink, _)| sink.flush())).await?;
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

    use crate::sink::fixtures;
    use crate::sink::table::{Schema, Table, TableId};
    use crate::sink::{EnvelopeSink, SinkError};
    use crate::wire::datasets::Datasets;
    use crate::wire::envelope::{BlockMeta, ChainId, Envelope, Event};

    use super::{Entry, RouteError, Router, Selection, Tables, datasets, glob};
    use crate::decode::{Kept, SharedKept};
    use crate::wire::datasets::evm::Decoded;

    fn tables(entries: &[&str]) -> Tables {
        Tables(
            entries
                .iter()
                .map(|&entry| Entry::All(entry.to_owned()))
                .collect(),
        )
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
            .expect("resolves")
            .tables;
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
            .expect("resolves")
            .tables;
        assert_eq!(names(&schema, &raw), ["blocks", "decoded_logs", "logs"]);

        let swaps = tables(&["uniswap_v3_pool_swap"])
            .resolve(&schema)
            .expect("resolves")
            .tables;
        assert_eq!(names(&schema, &swaps), ["uniswap_v3_pool_swap"]);

        let derived = tables(&["test_*"])
            .resolve(&schema)
            .expect("resolves")
            .tables;
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
        let every = tables(&["*"]).resolve(&schema).expect("resolves").tables;
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
            SharedKept::default(),
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

    /// A sink whose flush waits on a shared gate, or opens it.
    struct Gate {
        opens: bool,
        gate: Arc<tokio::sync::Semaphore>,
    }

    impl EnvelopeSink for Gate {
        async fn publish(&mut self, _: Envelope) -> Result<(), SinkError> {
            Ok(())
        }

        async fn flush(&mut self) -> Result<(), SinkError> {
            if self.opens {
                self.gate.add_permits(1);
            } else {
                let _permit = self.gate.acquire().await.expect("gate stays open");
            }
            Ok(())
        }
    }

    /// A `matched` entry selects its tables like any other, keeps them to the matched
    /// pools' rows unless an entry selects them whole, and cannot name a table of rows no
    /// contract emits.
    #[test]
    fn matched_entries_keep_their_tables_to_the_matched_pools() {
        /// A sink's settings, as a settings file writes its `tables`.
        #[derive(serde::Deserialize)]
        struct Sink {
            tables: Tables,
        }

        let schema = fixtures::schema();
        let parse = |json: &str| serde_json::from_str::<Tables>(json).expect(json);
        let matched = parse(r#"["logs", {"matched": "uniswap_v3_pool_*"}]"#)
            .resolve(&schema)
            .expect("resolves");
        let pool = names(&schema, &matched.matched);
        assert!(
            pool.contains(&"uniswap_v3_pool_swap".to_owned())
                && pool.iter().all(|name| name.starts_with("uniswap_v3_pool_")),
            "{pool:?}"
        );
        assert!(matched.matched.is_subset(&matched.tables));
        assert_eq!(
            matched.tables.len(),
            matched.matched.len() + 1,
            "and logs whole"
        );

        let whole = parse(r#"[{"matched": "uniswap_v3_pool_*"}, "uniswap_v3_pool_swap"]"#)
            .resolve(&schema)
            .expect("resolves");
        assert!(
            !names(&schema, &whole.matched).contains(&"uniswap_v3_pool_swap".to_owned()),
            "a table an entry selects whole is taken whole"
        );

        assert!(matches!(
            parse(r#"[{"matched": "blocks"}]"#).resolve(&schema),
            Err(RouteError::NotAnEventTable { table, .. }) if table == "blocks"
        ));
        for table in ["logs", "decoded_logs", "test_rows"] {
            let (schema, _) = fixtures::derived();
            let json = format!(r#"[{{"matched": "{table}"}}]"#);
            assert!(
                matches!(
                    parse(&json).resolve(&schema),
                    Err(RouteError::NotAnEventTable { .. })
                ),
                "{table} has no matched history to fill"
            );
        }
        assert!(matches!(
            parse(r#"[{"matched": "uniswap_v9_*"}]"#).resolve(&schema),
            Err(RouteError::NoMatch { .. })
        ));
        assert!(
            serde_json::from_str::<Tables>(r#"[{"matched": "logs", "only": "x"}]"#).is_err(),
            "an unknown key is refused"
        );

        let sink: Sink =
            toml::from_str(r#"tables = ["metric_v1_*", { matched = "uniswap_v3_pool_*" }]"#)
                .expect("a settings file's form parses");
        assert_eq!(
            sink.tables,
            Tables(vec![
                Entry::All("metric_v1_*".to_owned()),
                Entry::Matched {
                    matched: "uniswap_v3_pool_*".to_owned()
                },
            ])
        );
    }

    /// A `!` entry takes its tables out of what the others select, wherever it stands,
    /// matched ones too; one matching nothing is refused as any entry is, and a list of
    /// nothing but `!` entries does not parse.
    #[test]
    fn a_negated_entry_takes_its_tables_out() {
        let (schema, _) = fixtures::derived();
        let parse = |json: &str| serde_json::from_str::<Tables>(json).expect(json);
        let selected = parse(r#"["!uniswap_v3_pool_swap", "uniswap_v3_pool_*", "test_*"]"#)
            .resolve(&schema)
            .expect("resolves");
        let names = names(&schema, &selected.tables);
        assert!(names.contains(&"uniswap_v3_pool_mint".to_owned()));
        assert!(names.contains(&"test_rows".to_owned()));
        assert!(
            !names.contains(&"uniswap_v3_pool_swap".to_owned()),
            "taken out, though it came first"
        );

        let matched = parse(r#"[{"matched": "uniswap_v3_pool_*"}, "!*_swap"]"#)
            .resolve(&schema)
            .expect("resolves");
        assert!(
            !super::tests::names(&schema, &matched.tables)
                .iter()
                .any(|name| name.ends_with("_swap")),
            "a matched table is taken out too"
        );
        assert!(matched.matched.is_subset(&matched.tables));

        assert!(matches!(
            parse(r#"["logs", "!uniswap_v9_*"]"#).resolve(&schema),
            Err(RouteError::NoMatch { entry }) if entry == "!uniswap_v9_*"
        ));
        assert!(matches!(
            parse(r#"["logs", "!accepted_blocks"]"#).resolve(&schema),
            Err(RouteError::Bookkeeping { .. })
        ));
        assert!(serde_json::from_str::<Tables>(r#"["!logs"]"#).is_err());
        assert!(serde_json::from_str::<Tables>(r#"["logs", "!"]"#).is_err());
    }

    /// A sink keeping a table to the matched pools' rows takes a matched pool's decoded
    /// record and no other contract's; a sink taking the same tables whole takes both,
    /// and the bookkeeping reaches each. A pool matched later is kept from then on.
    #[tokio::test]
    async fn a_matched_sink_takes_only_the_matched_pools_rows() {
        let schema = fixtures::schema();
        let matched =
            serde_json::from_str::<Tables>(r#"["logs", {"matched": "uniswap_v3_pool_*"}]"#)
                .expect("parses")
                .resolve(&schema)
                .expect("resolves");
        let whole = tables(&["logs", "contract_events"])
            .resolve(&schema)
            .expect("resolves");
        let pool = fixtures::decoded_swap();
        let mut other = pool.clone();
        other.address = alloy_primitives::Address::repeat_byte(0x0e);
        other.log_index += 1;
        let kept = SharedKept::new(std::sync::RwLock::new(Kept::of(&[pool.address], &[])));
        let mut router = Router::new(
            Arc::clone(&schema),
            Arc::clone(&kept),
            vec![(Seen::default(), matched), (Seen::default(), whole)],
        );
        let log = |address, log_index| {
            Event::Log(Box::new(crate::wire::datasets::evm::Log {
                log_index,
                address,
                ..Default::default()
            }))
        };
        let events = [
            log(pool.address, 0),
            log(other.address, 1),
            Event::Decoded(Box::new(pool)),
            Event::Decoded(Box::new(other.clone())),
            Event::AcceptedBlock(BlockMeta {
                height: 1,
                hash: B256::with_last_byte(1),
                parent_hash: B256::ZERO,
                timestamp: 1,
            }),
        ];
        for event in &events {
            router
                .publish(envelope(event.clone()))
                .await
                .expect("publish");
        }
        *kept.write().expect("not poisoned") =
            Kept::of(&[events_address(&events[0]), other.address], &[]);
        let later = Event::Decoded(Box::new(Decoded {
            log_index: other.log_index + 1,
            ..other
        }));
        router
            .publish(envelope(later.clone()))
            .await
            .expect("publish");
        let key = |at: usize| events[at].dedupe_key();
        let [matched, whole] = &router.sinks[..] else {
            panic!("two sinks");
        };
        // Logs are taken whole; of the pool tables, only the matched pools' records.
        assert_eq!(
            matched.0.keys,
            [key(0), key(1), key(2), key(4), later.dedupe_key()]
        );
        assert_eq!(
            whole.0.keys,
            [key(0), key(1), key(2), key(3), key(4), later.dedupe_key()]
        );
    }

    /// The contract `event`, a log, came from.
    fn events_address(event: &Event) -> alloy_primitives::Address {
        match event {
            Event::Log(log) => log.address,
            _ => panic!("a log"),
        }
    }

    /// A sink stuck in its flush, as a store with a full channel is, does not hold the
    /// block back from the sinks after it.
    #[tokio::test]
    async fn a_blocked_sink_does_not_hold_back_the_others_flush() {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let sink = |opens| {
            let gate = Arc::clone(&gate);
            (Gate { opens, gate }, Selection::default())
        };
        let mut router = Router::new(
            fixtures::schema(),
            SharedKept::default(),
            vec![sink(false), sink(true)],
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), router.flush())
            .await
            .expect("the second sink's flush opens the first's gate")
            .expect("flush");
    }
}
