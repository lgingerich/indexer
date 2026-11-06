//! `indexer backfill`: decodes protocols added after their blocks were indexed, from the
//! logs the lake already holds, so adding a manifest does not mean indexing the chain
//! again. Nothing is fetched from a node; only the factories' logs from before the lake's
//! first block are fetched from `HyperSync`, when `[backfill.hypersync]` names it (see
//! "Contracts created before the lake's first block").
//!
//! ```bash
//! indexer backfill indexer.ethereum.toml
//! ```
//!
//! A backfill reads the running indexer's own settings file (see [`BackfillSettings`]):
//! its `[backfill]` names the manifests to backfill, a subtree of the running
//! indexer's, so the tables they declare are the ones it writes, and the chain, the lake,
//! and the database are the running indexer's `[ingest]`, `[sink.delta]`, and
//! `[sink.postgres]`.
//!
//! # Writing beside the running indexer
//!
//! The backfill writes into the running indexer's own tables without stopping it, and
//! never collides with it:
//!
//! - **Appends only.** It appends to `decoded_logs` and the event tables only the rows
//!   they do not hold yet, by key (see [`DecodedTables`]). In Delta a commit that only
//!   appends conflicts with no commit of the running store's, and a rerun, or a range the
//!   running indexer decoded too, writes each record once. In `PostgreSQL` it upserts.
//! - **Below the bound.** It writes no block above the lake's tip less
//!   four ledger windows (see [`LogReader::bound`]), which no delete of the running store
//!   reaches.
//! - **One at a time.** It holds the lake's backfill lease throughout (see
//!   [`RunLock`]).
//!
//! # Handing over without a gap
//!
//! Every store records a session with the first block it writes after a restart: that
//! block, and every event definition its run decodes (see
//! [`Session`](crate::sink::table::Session)). Before writing to a store, the backfill
//! checks its newest session: the running indexer decodes every protocol being
//! backfilled, with these manifests' event definitions, from a block at or below the
//! backfill's last. Then the backfill covers every block below its restart and the
//! running indexer every block from it, with the same keys. So:
//!
//! 1. Deploy the manifests to the running indexer.
//! 2. Run the backfill. A refusal names what is missing.
//!
//! Without `to`, a backfill started before the lake's tip is four ledger windows past the
//! restart hands over what it discovers at once, then waits until it is, and past every
//! handed-over contract's record, and fills through the bound then. A `to` below the
//! restart is refused.
//!
//! The backfill reads `logs` a few hundred megabytes of files at a time (see
//! [`LogReader`]), so each file is read once, and writes in commits of at most
//! [`COMMIT_BLOCKS`] heights and [`COMMIT_LOGS`] logs. A stopped run is run again, or
//! resumed by setting `from` to the `resume_from` its last read's log line names.
//!
//! A protocol feeding a derived table fills it too, as the running indexer's decode stage
//! does: each record's rows carry its transaction's sender and recipient, read from the
//! lake's `transactions` for just the records decoded (see [`LogReader::senders`]).
//!
//! # Contracts discovered below the restart
//!
//! A protocol that discovers its contracts (`created_by`) decodes, in the running
//! indexer, only the contracts created from its restart on. Those created below it the
//! backfill finds first: it reads its factories' logs below the restart, and their
//! children's when those create others in turn, until a pass finds no new factory. It
//! hands what it found to the running indexer, through a queue in the lake (see
//! [`HandoffQueue`](crate::sink::delta::HandoffQueue)), which adds each to its decoder at a block it records in its
//! `handoffs`, and waits for those records. The running indexer decodes each contract
//! from its block on, and the backfill below it; `only` narrows the contracts handed
//! over and backfilled to some.
//!
//! The records are near the tip, above the bound, so a backfill without `to` waits until
//! the bound passes them too, and fills each handed-over contract up to its record in the
//! one run. One with `to` below a record fills through `to` and says from where to run
//! again once the bound has passed it; a rerun hands over nothing it handed before.
//!
//! # Contracts created before the lake's first block
//!
//! Those are not in the lake to discover. With `[backfill.hypersync]`, the backfill
//! first asks `HyperSync` for the factories' logs below the lake's first block, a few
//! requests over the chain's whole history, and adds what they created to what it
//! hands over and backfills. Their logs from the lake's first block on are in the lake,
//! so they are decoded from there like any other. Without it, they are not found, and a
//! warning says so.
//!
//! What such a contract's lake history cannot say comes from a manifest's `before_lake`
//! (see [`Catalog`]): the events it names, of its listed contracts, such as the
//! `PoolCreated` naming a Uniswap V3 pool's tokens, or the `Initialize` naming a V4
//! pool's. The backfill fetches them from below the lake's first block over `HyperSync`,
//! and writes them to the event tables, `decoded_logs`, and `PostgreSQL` like any other
//! record, by key, so a rerun writes each once. In the lake they share one partition,
//! the first, rather than a small file in each of the thousands they would fall in by
//! height (see [`Placement::Together`](crate::sink::delta::Placement::Together)), so a
//! read of them filters on the block number, not the partition. `logs` stays as it was,
//! so the lake's first block does not move, and `only` does not narrow them, as they
//! describe contracts rather than belong to the ones handed over.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use alloy_primitives::Address;
use tracing::{info, warn};

use crate::config::BackfillRun;
use crate::decode::{Catalog, Decoder};
use crate::secret::Secret;
use crate::sink::delta::{DecodedTables, LogReader, RunLock};
use crate::wire::envelope::ChainId;

mod handover;
mod settings;
mod write;

use self::handover::{Coverage, adopt, discover, events_before_lake, handover, select};
pub use self::settings::{BackfillError, BackfillSettings, Backfilled};
use self::write::{Database, Writer, cores};

/// How the subcommand is called.
pub const USAGE: &str = "indexer backfill <indexer.<chain>.toml>";

/// How many reads run at once: the next while one is decoded and written. More were no
/// faster, since a read already spreads over every core, and each holds its logs until
/// written.
const READ_AHEAD: usize = 2;

/// How long a backfill waits for the running indexer to record the contracts it handed
/// over: its watch reads the queue every ten seconds while a backfill holds the lake, and
/// each record lands with the store's next commit.
const HANDOFF_WAIT: std::time::Duration = std::time::Duration::from_mins(15);

/// How often it reads the records while it waits.
const HANDOFF_POLL: std::time::Duration = if cfg!(test) {
    std::time::Duration::from_millis(100)
} else {
    std::time::Duration::from_secs(10)
};

/// How often a backfill without `to` reads the lake's tip again while it waits for the
/// bound to pass the running indexer's restart and records (see [`settle`]).
const BOUND_POLL: std::time::Duration = if cfg!(test) {
    std::time::Duration::from_millis(100)
} else {
    std::time::Duration::from_mins(1)
};

/// The most heights one commit covers, aligned to multiples of it.
pub const COMMIT_BLOCKS: u64 = 10_000;

/// The most logs one commit is decoded from, unless a single block holds more, so a
/// commit's rows stay a few hundred megabytes however busy the contract.
pub const COMMIT_LOGS: usize = 50_000;

/// Runs the subcommand: loads the settings at `path`, then backfills.
///
/// # Errors
///
/// As [`backfill`], and [`BackfillError::Settings`] and [`BackfillError::Missing`] for
/// the settings.
pub async fn run(path: &Path) -> Result<Backfilled, BackfillError> {
    backfill(&BackfillSettings::from_file(
        path,
        crate::secret::process_env,
    )?)
    .await
}

/// Decodes the logs of `settings`' manifests' contracts in its lake from `from` (the
/// lake's first block when absent) through `to`, and appends each decoded row its table
/// does not hold yet: in the lake, or in `out` when given, and in the `PostgreSQL` store
/// when one is named.
///
/// Holds the lake's backfill lease throughout, so no other backfill writes beside it.
/// Before writing to the running indexer's stores, checks each one's newest session:
/// its current run decodes the protocols, with these manifests' event definitions, from
/// a block at or below `to`, so the backfill and the running indexer leave no block
/// between them. Without `to`, hands over the contracts it discovers first, then waits
/// until the bound (see [`LogReader::bound`]) reaches that block and each handed-over
/// contract's record, and fills through the bound then. Decodes on the calling worker
/// thread, as Tokio's multi-threaded runtime allows.
///
/// # Errors
///
/// Refuses manifests that do not load or list no contract on the chain; an empty ledger;
/// a `to` above the bound, or below the running indexer's restart; a store whose session
/// does not decode the protocols as these manifests do (see the module docs), also when
/// a restart while it waits changes that; and a lake another backfill holds. Stops on a
/// store that cannot be read or written, or a lease taken over.
pub async fn backfill(settings: &BackfillSettings) -> Result<Backfilled, BackfillError> {
    let chain = ChainId::new(&settings.chain);
    let catalog = Catalog::load(&settings.run.protocols, &chain)?;
    let addresses: Vec<Address> = catalog.seeds.iter().map(|&(address, _)| address).collect();
    if addresses.is_empty() {
        return Err(BackfillError::NoContracts { chain });
    }
    let protocols: Vec<String> = catalog
        .entries
        .iter()
        .map(|entry| entry.protocol.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let lake = &settings.lake;
    let target = settings.run.out.as_deref().unwrap_or(&lake.uri);
    let storage = match settings.run.out {
        Some(_) => BTreeMap::new(),
        None => lake.storage.clone(),
    };
    let run = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_secs())
    );
    let lock =
        RunLock::acquire(target, &storage, chain.as_str(), &run, &protocols.join(",")).await?;
    let filled = fill(
        settings, &chain, catalog, &addresses, &protocols, &lock, &storage,
    )
    .await;
    let released = lock.release().await;
    let done = filled?;
    released?;
    Ok(done)
}

/// [`backfill`]'s work, under the lease `lock`, writing to the lake reached with
/// `storage`.
async fn fill(
    settings: &BackfillSettings,
    chain: &ChainId,
    catalog: Catalog,
    addresses: &[Address],
    protocols: &[String],
    lock: &RunLock,
    storage: &BTreeMap<String, Secret>,
) -> Result<Backfilled, BackfillError> {
    let schema = catalog.schema().clone();
    let derives = catalog.derives();
    let discovers = catalog.discovers();
    let before_lake = catalog.before_lake();
    let mut decoder = Decoder::new(catalog, []);
    let (lake, run) = (&settings.lake, &settings.run);
    let open = || LogReader::open(lake, &schema, chain.as_str(), derives);
    let mut reader = open().await?;
    let bound = reader.bound().ok_or(BackfillError::EmptyLedger)?;
    if let Some(to) = run.to
        && to > bound
    {
        return Err(BackfillError::ToAboveBound { to, bound });
    }
    let wanted = schema.decoded_events();
    let coverage = Coverage {
        wanted: &wanted,
        protocols,
    };
    let mut database = Database::open(settings, &schema, chain).await?;
    // Where the running indexer's current run decodes from. With `out` there is none: the
    // running indexer writes the chain's own lake, not this one.
    let restart = running_restart(run, &coverage, &reader, &mut database).await?;
    // The last block, when it is known before handing over: `to`, or with `out`, where
    // nothing is handed over or waited for, the bound.
    let fixed = run.to.or_else(|| run.out.as_ref().map(|_| bound));
    if let (Some(from), Some(to)) = (restart, fixed)
        && from > to
    {
        return Err(BackfillError::session_above_to(from, to, lake.undo_window));
    }
    let target = run.out.as_deref().unwrap_or(&lake.uri);
    let tables =
        DecodedTables::open(target, storage, &schema, chain.as_str(), reader.width()).await?;

    let from = run.from.unwrap_or(0);
    // Each read owns a copy of the reader, so the next is read while one is decoded and
    // written. They share the cores unless the settings say otherwise: each Parquet
    // stream holds a row group of up to a million rows while it reads, so a stream per
    // core per read would hold gigabytes.
    let streams = streams_of(run);
    let mut done = Backfilled::default();
    let mut addresses = addresses.to_vec();
    // The highest block the fill must reach to leave none between it and the running
    // indexer: its restart, and below each handed-over contract's record.
    let mut needed = restart.unwrap_or(0);
    if discovers {
        // The running indexer discovers what is created from its restart on. Handing over
        // needs only that it decodes these manifests, not the bound, so a backfill run
        // right after a deploy hands over at once, and the contracts are decoded live
        // from then on.
        let session = run.out.is_none().then(|| reader.session()).flatten();
        let below = session.map_or(bound, |session| session.block_number.saturating_sub(1));
        let blocks = from..=fixed.map_or(below, |to| below.min(to));
        let hypersync = run.hypersync.as_ref();
        let found = discover(&mut decoder, &reader, hypersync, blocks, streams).await?;
        done.discovered = found.len();
        let handed = match fixed.filter(|_| run.out.is_some()) {
            None => {
                handover(
                    settings,
                    &schema,
                    chain,
                    protocols,
                    found,
                    reader.width(),
                    lock,
                )
                .await?
            }
            // Nothing decodes them above `to`, so there is nothing to wait for.
            Some(to) => select(run.only.as_deref(), found)?
                .into_iter()
                .map(|contract| (contract, to.saturating_add(1)))
                .collect(),
        };
        done.handed = handed.len();
        let restart = session.map(|session| session.block_number);
        let through = adopt(&mut decoder, &mut addresses, &handed, restart);
        needed = needed.max(through.unwrap_or(0));
    }
    let to = if let Some(to) = fixed {
        done.rerun_through = Some(needed).filter(|&through| through > to);
        to
    } else {
        let settled = settle(chain, &coverage, open, reader, &mut database, needed, lock);
        let to;
        (reader, to) = settled.await?;
        to
    };
    let older = events_before_lake(
        &before_lake,
        run.hypersync.as_ref(),
        &(from..=to),
        reader.first(),
    )
    .await?;
    let mut writer = Writer {
        chain,
        schema: &schema,
        protocols,
        lock,
        decoder,
        tables,
        database,
        derives,
        streams,
    };
    writer.write_before_lake(&reader, &older, &mut done).await?;
    writer
        .write(&reader, &addresses, from..=to, &mut done)
        .await?;
    report(chain, &done);
    Ok(done)
}

/// The Parquet streams each read takes: as `run` says, or half the machine's cores.
fn streams_of(run: &BackfillRun) -> usize {
    run.streams
        .map_or_else(|| cores().div_ceil(READ_AHEAD), std::num::NonZeroUsize::get)
}

/// Where the running indexer's current run decodes from, as `coverage` reads it from the
/// lake and `database`; `None` when `run` writes to another lake.
async fn running_restart(
    run: &BackfillRun,
    coverage: &Coverage<'_>,
    reader: &LogReader,
    database: &mut Database,
) -> Result<Option<u64>, BackfillError> {
    match &run.out {
        Some(out) => {
            info!(out, "writing to another lake; the chain's is only read");
            Ok(None)
        }
        None => Ok(Some(coverage.restart(reader, database).await?)),
    }
}

/// Waits, holding the lease `lock`, until the lake's bound reaches `needed` and where the
/// running indexer's current run decodes from, as `coverage` reads it from the lake and
/// `database`, reading the lake again with `open` every [`BOUND_POLL`]. Returns the lake
/// as last read, and its bound, the block to fill through. A restart of the running
/// indexer while it waits moves where it decodes from on, which it then waits for.
///
/// # Errors
///
/// As [`Coverage::restart`], when the running indexer no longer decodes the protocols
/// as the backfill does; [`BackfillError::Lock`] for a lease taken over; and
/// [`BackfillError::Store`] for a lake that cannot be read.
async fn settle<F>(
    chain: &ChainId,
    coverage: &Coverage<'_>,
    open: impl Fn() -> F,
    mut reader: LogReader,
    database: &mut Database,
    needed: u64,
    lock: &RunLock,
) -> Result<(LogReader, u64), BackfillError>
where
    F: Future<Output = Result<LogReader, crate::sink::StoreError>>,
{
    loop {
        let bound = reader.bound().ok_or(BackfillError::EmptyLedger)?;
        let needed = needed.max(coverage.restart(&reader, database).await?);
        if bound >= needed {
            return Ok((reader, bound));
        }
        info!(%chain, bound, needed, blocks = needed - bound,
            "waiting for the lake's tip to pass the running indexer's restart and records by \
             four ledger windows");
        lock.check()?;
        tokio::time::sleep(BOUND_POLL).await;
        reader = open().await?;
    }
}

/// Logs what a backfill did, and from where to run it again if it is not done.
fn report(chain: &ChainId, done: &Backfilled) {
    info!(%chain, from = done.from, to = done.to, reads = done.reads, commits = done.commits,
        logs = done.logs, decoded = done.decoded, appended = done.appended,
        before_lake = done.before_lake,
        discovered = done.discovered, handed = done.handed, "backfill done");
    if let Some(through) = done.rerun_through {
        warn!(%chain, from = done.to.saturating_add(1), through,
            "the running indexer decodes some handed-over contracts only above `to`: run \
             again from here once the bound reaches `through`");
    }
}

#[cfg(test)]
mod tests;
