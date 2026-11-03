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
//! 2. Wait until the lake's tip is four ledger windows past its restart.
//! 3. Run the backfill. A refusal names what is missing.
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
//! The records are near the tip, above the bound, so a first run fills each handed-over
//! contract only through the bound, and says from where to run again once the tip has
//! passed them by the lag; a rerun hands over nothing it handed before.
//!
//! # Contracts created before the lake's first block
//!
//! Those are not in the lake to discover. With `[backfill.hypersync]`, the backfill
//! first asks `HyperSync` for the factories' logs below the lake's first block, a few
//! requests over the chain's whole history, and adds what they created to what it
//! hands over and backfills. Their logs from the lake's first block on are in the lake,
//! so they are decoded from there like any other. Without it, they are not found, and a
//! warning says so.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use alloy_primitives::Address;
use futures_util::stream::{self, StreamExt as _};
use tracing::{info, warn};

use crate::decode::{Catalog, Decoder};
use crate::secret::Secret;
use crate::sink::delta::{DecodedTables, LogReader, RunLock};
use crate::wire::envelope::ChainId;

mod handover;
mod settings;
mod write;

use self::handover::{adopt, covers, discover, handover, select};
pub use self::settings::{BackfillError, BackfillSettings, Backfilled};
use self::write::{Database, cores, decode_all, derive, parts, rows};

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
/// lake's first block when absent) through `to` (the bound when absent, see
/// [`LogReader::bound`]), and appends each decoded row its table does not hold yet: in
/// the lake, or in `out` when given, and in the `PostgreSQL` store when one is named.
///
/// Holds the lake's backfill lease throughout, so no other backfill writes beside it.
/// Before writing to the running indexer's stores, checks each one's newest session:
/// its current run decodes the protocols, with these manifests' event definitions, from
/// a block at or below `to`, so the backfill and the running indexer leave no block
/// between them. Decodes on the calling worker thread, as Tokio's multi-threaded runtime
/// allows.
///
/// # Errors
///
/// Refuses manifests that do not load or list no contract on the chain; an empty ledger; a `to` above the bound; a store whose session
/// does not cover the protocols (see the module docs); and a lake another backfill
/// holds. Stops on a store that cannot be read or written, or a lease taken over.
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
    let mut decoder = Decoder::new(catalog, []);
    let lake = &settings.lake;
    let reader =
        LogReader::open(&lake.uri, &lake.storage, &schema, chain.as_str(), derives).await?;
    let bound = reader.bound().ok_or(BackfillError::EmptyLedger)?;
    let to = match settings.run.to {
        Some(to) if to > bound => return Err(BackfillError::ToAboveBound { to, bound }),
        Some(to) => to,
        None => bound,
    };
    let wanted = schema.decoded_events();
    match &settings.run.out {
        // The running indexer writes the chain's own lake, not this one, so its session
        // says nothing about what this one holds.
        Some(out) => info!(out, "writing to another lake; the chain's is only read"),
        None => covers("lake", reader.session(), &wanted, protocols, to)?,
    }
    let target = settings.run.out.as_deref().unwrap_or(&lake.uri);
    let mut tables =
        DecodedTables::open(target, storage, &schema, chain.as_str(), reader.width()).await?;
    let mut database = Database::open(settings, &schema, chain, &wanted, protocols, to).await?;

    let from = settings.run.from.unwrap_or(0);
    // Each read owns a copy of the reader, so the next is read while one is decoded and
    // written. They share the cores: each Parquet stream holds a row group of up to a
    // million rows while it reads, so a stream per core per read would hold gigabytes.
    let streams = cores().div_ceil(READ_AHEAD);
    let mut done = Backfilled::default();
    let mut addresses = addresses.to_vec();
    if discovers {
        // The running indexer discovers what is created from its restart on; with `out`,
        // there is no running indexer to hand anything to.
        let run = &settings.run;
        let restart = run.out.is_none().then(|| reader.session()).flatten();
        let below = restart.map_or(to, |session| session.block_number.saturating_sub(1));
        let blocks = from..=below.min(to);
        let hypersync = run.hypersync.as_ref();
        let found = discover(&mut decoder, &reader, hypersync, blocks, streams).await?;
        done.discovered = found.len();
        let handed = match &settings.run.out {
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
            Some(_) => select(settings.run.only.as_deref(), found)?
                .into_iter()
                .map(|contract| (contract, to.saturating_add(1)))
                .collect(),
        };
        done.handed = handed.len();
        let restart = restart.map(|session| session.block_number);
        done.rerun_through = adopt(&mut decoder, &mut addresses, &handed, restart, to);
    }
    let addresses = &addresses[..];

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
            let found = decode_all(&mut decoder, chain, &logs[positions])?;
            records += found.len();
            let events = derive(&decoder, &reader, chain, found, derives, streams).await?;
            lock.check()?;
            appended += tables
                .fill(blocks, protocols, rows(chain, &schema, &events)?)
                .await?;
            database.fill(chain, events).await?;
            done.commits += 1;
        }
        done.reads += 1;
        done.logs += logs.len();
        done.decoded += records;
        done.appended += appended;
        info!(%chain, from = read.start(), to = read.end(), logs = logs.len(), decoded = records,
            appended, resume_from = read.end().saturating_add(1), "range backfilled");
    }
    report(chain, &done);
    Ok(done)
}

/// Logs what a backfill did, and from where to run it again if it is not done.
fn report(chain: &ChainId, done: &Backfilled) {
    info!(%chain, from = done.from, to = done.to, reads = done.reads, commits = done.commits,
        logs = done.logs, decoded = done.decoded, appended = done.appended,
        discovered = done.discovered, handed = done.handed, "backfill done");
    if let Some(through) = done.rerun_through {
        warn!(%chain, from = done.to.saturating_add(1), through,
            "the running indexer decodes some handed-over contracts only above `to`: run \
             again from here once the bound reaches `through`");
    }
}

#[cfg(test)]
mod tests;
