//! What a backfill and a matched pools' fill write with: decoding a commit's logs as the
//! running indexer does, deriving their rows, the commits they are written in, and the
//! running indexer's database, filled beside it.

use std::collections::{BTreeSet, HashMap};
use std::ops::{Range, RangeInclusive};

use alloy_primitives::B256;
use tracing::warn;

use super::{BackfillError, BackfillSettings, COMMIT_BLOCKS, COMMIT_LOGS};
use crate::decode::{Decoder, Record, Sent};
use crate::sink::delta::LogReader;
use crate::sink::table::{Row, Schema, SessionEvent};
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
    /// Opens the store the settings name, if any, with the tables its `tables` route of
    /// `schema`, and checks its session as the lake's is.
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
        wanted: &[SessionEvent],
        protocols: &[String],
        to: u64,
    ) -> Result<Self, BackfillError> {
        #[cfg(feature = "postgres")]
        {
            let Some(postgres) = &settings.postgres else {
                return Ok(Self::none());
            };
            let routed = postgres
                .tables
                .resolve(schema)
                .map_err(|source| BackfillError::Route { source })?;
            // A table kept to the pools matched to Metric's pairs is the running indexer's
            // to fill, which knows the pools (see `crate::decode::pairs`).
            let whole: std::collections::HashSet<_> =
                routed.tables.difference(&routed.matched).copied().collect();
            let mut database = Self::beside(postgres, schema, &whole, chain).await?;
            if let Some(store) = &mut database.store {
                let session = store.session().await?;
                super::covers("database", session.as_ref(), wanted, protocols, to)?;
            }
            Ok(database)
        }
        #[cfg(not(feature = "postgres"))]
        {
            let _ = (settings, schema, chain, wanted, protocols, to);
            Ok(Self::none())
        }
    }

    /// No database: what is filled into it is dropped.
    pub(super) const fn none() -> Self {
        Self {
            #[cfg(feature = "postgres")]
            store: None,
        }
    }

    /// The running indexer's database that `postgres` names, opened beside it with
    /// `tables` of `schema`: it takes no lock, and writes no ledger.
    #[cfg(feature = "postgres")]
    pub(super) async fn beside(
        postgres: &crate::sink::PostgresSettings,
        schema: &Schema,
        tables: &std::collections::HashSet<crate::sink::table::TableId>,
        chain: &ChainId,
    ) -> Result<Self, BackfillError> {
        let store = crate::sink::PostgresSink::open(
            postgres,
            std::sync::Arc::new(schema.select(tables)),
            chain.as_str(),
            crate::sink::postgres::Writer::Beside,
        )
        .await?;
        Ok(Self { store: Some(store) })
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
