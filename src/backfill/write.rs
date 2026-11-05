//! What a backfill writes with: decoding a commit's logs as the
//! running indexer does, deriving their rows, the commits they are written in, and the
//! running indexer's database, filled beside it.

use std::collections::{BTreeSet, HashMap};
use std::ops::{Range, RangeInclusive};

use alloy_primitives::{Address, B256};
use futures_util::stream::{self, StreamExt as _};
use tracing::{info, warn};

use super::handover::Coverage;
use super::{BackfillError, BackfillSettings, Backfilled, COMMIT_BLOCKS, COMMIT_LOGS, READ_AHEAD};
use crate::decode::{Decoder, Record, Sent};
use crate::sink::delta::{DecodedTables, LogReader, Placement, RunLock};
use crate::sink::table::{Row, Schema};
use crate::wire::datasets::evm::Log;
use crate::wire::envelope::{ChainId, Event};

/// The running indexer's `PostgreSQL` store, when the settings name one: filled beside
/// it with the decoded records, upserted, which takes no lock since the backfill writes
/// only rows below the bound, which it never deletes.
pub(super) struct Database {
    #[cfg(feature = "postgres")]
    store: Option<crate::sink::PostgresSink>,
}

impl Database {
    /// Opens the store the settings name, if any, with the tables of `schema` its `tables`
    /// route (see [`routed`]). A backfill writing another lake (`out`) is a trial, and
    /// fills no database.
    #[cfg_attr(
        not(feature = "postgres"),
        expect(
            clippy::unused_async,
            reason = "the same signature as the `postgres` build's, which connects"
        )
    )]
    pub(super) async fn open(
        settings: &BackfillSettings,
        schema: &Schema,
        chain: &ChainId,
    ) -> Result<Self, BackfillError> {
        #[cfg(feature = "postgres")]
        if let Some(postgres) = settings
            .postgres
            .as_ref()
            .filter(|_| settings.run.out.is_none())
        {
            let tables = routed(&postgres.tables, &settings.decoded, chain, schema)?;
            let store = crate::sink::PostgresSink::open(
                postgres,
                std::sync::Arc::new(schema.select(&tables)),
                chain.as_str(),
                crate::sink::postgres::Writer::Beside,
            )
            .await?;
            return Ok(Self { store: Some(store) });
        }
        let _ = (settings, schema, chain);
        Ok(Self {
            #[cfg(feature = "postgres")]
            store: None,
        })
    }

    /// From which block the running indexer's current run decodes in the store, as its
    /// newest session says, checked as `coverage` asks (see [`Coverage::decodes`]);
    /// `None` when there is no store.
    #[cfg_attr(
        not(feature = "postgres"),
        expect(
            clippy::unused_async,
            reason = "the same signature as the `postgres` build's, which reads"
        )
    )]
    pub(super) async fn restart(
        &mut self,
        coverage: &Coverage<'_>,
    ) -> Result<Option<u64>, BackfillError> {
        #[cfg(feature = "postgres")]
        if let Some(store) = &mut self.store {
            let session = store.session().await?;
            return coverage.decodes("database", session.as_ref()).map(Some);
        }
        let _ = coverage;
        Ok(None)
    }

    /// Upserts `events`, decoded and derived records, into the tables the store routes, in
    /// one transaction.
    #[cfg_attr(
        not(feature = "postgres"),
        expect(
            clippy::unused_async,
            reason = "the same signature as the `postgres` build's, which writes"
        )
    )]
    pub(super) async fn fill(
        &mut self,
        chain: &ChainId,
        events: Vec<Event>,
    ) -> Result<(), BackfillError> {
        #[cfg(feature = "postgres")]
        if let Some(store) = &mut self.store {
            use crate::sink::EnvelopeSink as _;
            for event in events {
                store
                    .publish(crate::wire::envelope::Envelope::new(chain.clone(), event))
                    .await
                    .map_err(|source| BackfillError::Database { source })?;
            }
            store
                .flush()
                .await
                .map_err(|source| BackfillError::Database { source })?;
        }
        #[cfg(not(feature = "postgres"))]
        let _ = (chain, events);
        Ok(())
    }
}

/// What a backfill writes with once it knows what to write: the decoder, the lake's
/// tables and the database it fills beside the running indexer, under its lease.
pub(super) struct Writer<'a> {
    pub(super) chain: &'a ChainId,
    pub(super) schema: &'a Schema,
    pub(super) protocols: &'a [String],
    pub(super) lock: &'a RunLock,
    pub(super) decoder: Decoder,
    pub(super) tables: DecodedTables,
    pub(super) database: Database,
    /// Whether the manifests derive rows, whose senders are read from `transactions`.
    pub(super) derives: bool,
    /// The Parquet streams each read takes.
    pub(super) streams: usize,
}

impl Writer<'_> {
    /// Decodes the logs `addresses` emitted in `blocks`, as `reader` holds them, and
    /// appends their rows, counting what it did in `done`. Reads [`READ_AHEAD`] ranges at
    /// once, so the next is read while one is decoded and written.
    ///
    /// # Errors
    ///
    /// Stops on a store that cannot be read or written, a lease taken over, and a
    /// decoder or derivation that breaks an invariant.
    pub(super) async fn write(
        &mut self,
        reader: &LogReader,
        addresses: &[Address],
        blocks: RangeInclusive<u64>,
        done: &mut Backfilled,
    ) -> Result<(), BackfillError> {
        let (chain, protocols, streams) = (self.chain, self.protocols, self.streams);
        let (from, to) = blocks.into_inner();
        let reads = reader.reads(from, to);
        done.from = reads.first().map_or(to, |read| *read.start());
        done.to = to;
        info!(%chain, ?protocols, contracts = addresses.len(), from = done.from, to,
            reads = reads.len(), "backfilling from the lake's logs");
        let mut logs = stream::iter(reads.clone())
            .map(|blocks| tokio::spawn(reader.read(blocks, addresses, streams)))
            .buffered(READ_AHEAD);
        for read in reads {
            let Some(logs) = logs.next().await else {
                break;
            };
            let logs = logs.unwrap_or_else(|join| std::panic::resume_unwind(join.into_panic()))?;
            let (mut records, mut appended) = (0, 0);
            for (blocks, positions) in parts(&logs, &read) {
                let (decoded, added) = self
                    .commit(reader, blocks, &logs[positions], Placement::ByHeight)
                    .await?;
                records += decoded;
                appended += added;
                done.commits += 1;
            }
            done.reads += 1;
            done.logs += logs.len();
            done.decoded += records;
            done.appended += appended;
            info!(%chain, from = read.start(), to = read.end(), logs = logs.len(),
                decoded = records, appended, resume_from = read.end().saturating_add(1),
                "range backfilled");
        }
        Ok(())
    }

    /// Decodes and appends `logs`, sorted, which are from below the lake's first block, in
    /// commits of at most [`COMMIT_LOGS`] logs, counting what it did in `done`. These
    /// logs are sparse across a long history, so a commit spans as many heights as its
    /// logs do, where one per [`COMMIT_BLOCKS`] heights would make thousands of commits,
    /// and they go in one partition rather than a small file in each they fall in (see
    /// [`Placement::Together`]).
    ///
    /// # Errors
    ///
    /// As [`write`](Self::write).
    pub(super) async fn write_before_lake(
        &mut self,
        reader: &LogReader,
        logs: &[Log],
        done: &mut Backfilled,
    ) -> Result<(), BackfillError> {
        for part in logs.chunks(COMMIT_LOGS) {
            let (Some(first), Some(last)) = (part.first(), part.last()) else {
                continue;
            };
            let blocks = first.block_number..=last.block_number;
            let (decoded, appended) = self
                .commit(reader, blocks, part, Placement::Together)
                .await?;
            done.commits += 1;
            done.logs += part.len();
            done.decoded += decoded;
            done.appended += appended;
            done.before_lake += decoded;
        }
        Ok(())
    }

    /// Decodes `logs` and appends their rows, which `blocks` holds, in one commit to each
    /// table, placed as `placement` says, and fills the database with them. Returns the records decoded and the rows
    /// appended.
    async fn commit(
        &mut self,
        reader: &LogReader,
        blocks: RangeInclusive<u64>,
        logs: &[Log],
        placement: Placement,
    ) -> Result<(usize, usize), BackfillError> {
        let chain = self.chain;
        let found = decode_all(&mut self.decoder, chain, logs)?;
        let decoded = found.len();
        let events = derive(
            &self.decoder,
            reader,
            chain,
            found,
            self.derives,
            self.streams,
        )
        .await?;
        self.lock.check()?;
        let rows = rows(chain, self.schema, &events)?;
        let appended = self
            .tables
            .fill(blocks, self.protocols, rows, placement)
            .await?;
        self.database.fill(chain, events).await?;
        Ok((decoded, appended))
    }
}

/// The tables of `schema`, a backfill's, that `tables` routes to the running indexer's
/// database. `tables` names the tables of every manifest the running indexer decodes,
/// `decoded`, so it is resolved against those, where a typo is caught as at its startup,
/// and its tables taken from `schema` by name: an entry naming another protocol's is
/// none of the backfill's.
#[cfg(feature = "postgres")]
pub(super) fn routed(
    tables: &crate::sink::route::Tables,
    decoded: &std::path::Path,
    chain: &ChainId,
    schema: &Schema,
) -> Result<std::collections::HashSet<crate::sink::table::TableId>, BackfillError> {
    let all = crate::decode::Catalog::load(decoded, chain)?;
    let all = all.schema();
    let routed = tables
        .resolve(all)
        .map_err(|source| BackfillError::Route { source })?;
    let names: std::collections::HashSet<&str> = all
        .tables()
        .iter()
        .filter(|def| routed.contains(&def.id))
        .map(|def| def.name.as_str())
        .collect();
    Ok(schema
        .tables()
        .iter()
        .filter(|def| names.contains(def.name.as_str()))
        .map(|def| def.id)
        .collect())
}

/// How many threads the machine runs at once.
pub(super) fn cores() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// The commits `logs`, sorted, of `read` are written in, each with the positions of its
/// logs: together they cover the read's blocks from the first log on. A commit never
/// crosses a multiple of [`COMMIT_BLOCKS`], nor splits a block, and holds at most
/// [`COMMIT_LOGS`] logs unless one block alone holds more.
pub(super) fn parts(
    logs: &[Log],
    read: &RangeInclusive<u64>,
) -> Vec<(RangeInclusive<u64>, Range<usize>)> {
    let window = |height: u64| height / COMMIT_BLOCKS;
    let mut positions = Vec::new();
    let mut begin = 0;
    for end in 1..=logs.len() {
        let ends_here = logs.get(end).is_none_or(|log| {
            let last = &logs[end - 1];
            window(last.block_number) != window(log.block_number)
                || (end - begin >= COMMIT_LOGS && last.block_number != log.block_number)
        });
        if ends_here {
            positions.push(begin..end);
            begin = end;
        }
    }
    let mut parts = Vec::with_capacity(positions.len());
    for (index, logs_in) in positions.iter().enumerate() {
        let height = logs[logs_in.start].block_number;
        let start = match index.checked_sub(1).map(|previous| &positions[previous]) {
            Some(previous) if window(logs[previous.start].block_number) == window(height) => height,
            _ => (height - height % COMMIT_BLOCKS).max(*read.start()),
        };
        let end = match positions.get(index + 1) {
            Some(next) if window(logs[next.start].block_number) == window(height) => {
                logs[next.start].block_number - 1
            }
            _ => (height - height % COMMIT_BLOCKS)
                .saturating_add(COMMIT_BLOCKS - 1)
                .min(*read.end()),
        };
        parts.push((start..=end, logs_in.clone()));
    }
    parts
}

/// The rows `events` write: each decoded record's event-table and `decoded_logs` rows,
/// and each derived record's in its derived table.
pub(super) fn rows(
    chain: &ChainId,
    schema: &Schema,
    events: &[Event],
) -> Result<Vec<Row>, BackfillError> {
    let mut rows = Vec::with_capacity(2 * events.len());
    for event in events {
        if let Event::Decoded(record) = event {
            rows.push(schema.event_row(chain, record)?.ok_or_else(|| {
                BackfillError::UnknownEvent {
                    protocol: record.protocol.clone(),
                    contract: record.contract.clone(),
                    event: record.name.clone(),
                }
            })?);
        }
        rows.push(schema.row(chain, event)?);
    }
    Ok(rows)
}

/// `found`, the records decoded from one commit's logs, as the events the running
/// indexer publishes after their logs: each record, then the rows its derivations compute
/// from it. When the manifests `derive`, each row carries its transaction's sender and
/// recipient, read from the lake's `transactions` for just these records; a transaction
/// the lake does not hold leaves them empty, as a log whose transaction was not fetched
/// does.
pub(super) async fn derive(
    decoder: &Decoder,
    reader: &LogReader,
    chain: &ChainId,
    found: Vec<Record>,
    derives: bool,
    streams: usize,
) -> Result<Vec<Event>, BackfillError> {
    let first = found.iter().map(|record| record.decoded.block_number).min();
    let last = found.iter().map(|record| record.decoded.block_number).max();
    let senders = match (derives, first.zip(last)) {
        // Only the blocks the records are in, which a busy contract's commit spans and a
        // quiet one's does not.
        (true, Some((first, last))) => {
            let hashes: BTreeSet<B256> = found
                .iter()
                .map(|record| record.decoded.transaction_hash)
                .collect();
            let hashes: Vec<B256> = hashes.into_iter().collect();
            reader.senders(&(first..=last), &hashes, streams).await?
        }
        _ => HashMap::new(),
    };
    let mut events = Vec::with_capacity(found.len());
    let mut unsent = 0_usize;
    for Record { decoded, event } in found {
        let rows = if derives {
            let sent = senders
                .get(&decoded.transaction_hash)
                .map(|(from, to, input)| Sent {
                    hash: decoded.transaction_hash,
                    from: *from,
                    to: *to,
                    input: input.clone(),
                });
            let rows = decoder.derive(chain, event, &decoded, sent.as_ref())?;
            if sent.is_none() && !rows.is_empty() {
                unsent += 1;
            }
            rows
        } else {
            Vec::new()
        };
        events.push(Event::Decoded(Box::new(decoded)));
        events.extend(rows.into_iter().map(|row| Event::Derived(Box::new(row))));
    }
    if unsent > 0 {
        warn!(%chain, from = first, to = last, records = unsent,
            "transactions the lake does not hold; their derived rows carry no sender");
    }
    Ok(events)
}

/// The records `logs` decode to, each with the event it decoded as, as the running
/// indexer decodes them: a contract one creates decodes from its creation on, and a log
/// that does not decode is logged and skipped. Decodes on the calling worker thread, as
/// Tokio's multi-threaded runtime allows.
///
/// # Errors
///
/// Returns [`BackfillError::Decode`] when the decoder breaks an invariant.
pub(super) fn decode_all(
    decoder: &mut Decoder,
    chain: &ChainId,
    logs: &[Log],
) -> Result<Vec<Record>, BackfillError> {
    tokio::task::block_in_place(|| {
        let mut records = Vec::new();
        for log in logs {
            let Some((record, claims)) = decoder.decode(log) else {
                continue;
            };
            decoder.admit(claims);
            match record {
                Ok(record) => records.push(record),
                Err(error) if error.is_invariant() => return Err(error.into()),
                Err(error) => {
                    warn!(%chain, address = %log.address, block = log.block_number,
                        transaction = %log.transaction_hash, log_index = log.log_index,
                        selector = ?log.topic0, ?error, "log decode failed; skipped");
                }
            }
        }
        Ok(records)
    })
}
