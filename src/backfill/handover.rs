//! Finding the contracts a backfill's factories created below the running indexer's
//! restart, handing them to it, and checking that it decodes what the backfill writes.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::RangeInclusive;

use alloy_primitives::{Address, B256};
use tracing::{info, warn};

use super::{BackfillError, BackfillSettings, HANDOFF_POLL, HANDOFF_WAIT};
use crate::config::HyperSyncEndpoint;
use crate::decode::Decoder;
use crate::ingest::source::LogSearch;
use crate::sink::delta::{HandoffQueue, LogReader, RunLock};
use crate::sink::table::{Schema, Session, SessionEvent};
use crate::wire::datasets::evm::{Contract, Log};
use crate::wire::envelope::ChainId;

/// Finds the contracts the decoder's factories created in `blocks`: first those created
/// below the lake's first block, from their logs over `HyperSync` when `hypersync` names it
/// (from the chain's first block, whatever `blocks` starts at, since the lake has no
/// record of which it searched before),
/// then those in the lake. Each reads the factories' logs, adding each child they create,
/// then the logs of any child that creates others in turn, until a pass finds no new
/// factory. Returns them in creation order.
pub(super) async fn discover(
    decoder: &mut Decoder,
    reader: &LogReader,
    hypersync: Option<&HyperSyncEndpoint>,
    blocks: RangeInclusive<u64>,
    streams: usize,
) -> Result<Vec<Contract>, BackfillError> {
    let mut found = Vec::new();
    let first = reader.first().unwrap_or_default();
    if *blocks.start() < first && !blocks.is_empty() {
        let through = first.saturating_sub(1).min(*blocks.end());
        if let Some(hypersync) = hypersync {
            before_lake(decoder, hypersync, through, &mut found).await?;
        } else {
            warn!(
                first,
                "no [backfill.hypersync]: contracts the factories created \
                below the lake's first block are not found"
            );
        }
    }
    let mut read = HashSet::new();
    loop {
        let parents = unread(decoder, &mut read);
        if parents.is_empty() || blocks.is_empty() {
            break;
        }
        for range in reader.reads(*blocks.start(), *blocks.end()) {
            let logs = reader.read(range, &parents, streams).await?;
            tokio::task::block_in_place(|| admit(decoder, &logs, &mut found))?;
        }
        info!(
            factories = parents.len(),
            found = found.len(),
            from = blocks.start(),
            to = blocks.end(),
            "discovered contracts below the restart"
        );
    }
    found.sort_by_key(|contract| (contract.block_number, contract.log_index));
    Ok(found)
}

/// Adds to `decoder` and `found` the contracts its factories created through block
/// `through`, below the lake, from their logs over `hypersync`.
async fn before_lake(
    decoder: &mut Decoder,
    hypersync: &HyperSyncEndpoint,
    through: u64,
    found: &mut Vec<Contract>,
) -> Result<(), BackfillError> {
    let search = LogSearch::new(&hypersync.url, hypersync.token.expose())?;
    let mut searched = HashSet::new();
    loop {
        let parents = unread(decoder, &mut searched);
        if parents.is_empty() {
            return Ok(());
        }
        let logs = search.logs(&parents, through).await?;
        tokio::task::block_in_place(|| admit(decoder, &logs, found))?;
        info!(
            factories = parents.len(),
            found = found.len(),
            through,
            "discovered contracts below the lake's first block"
        );
    }
}

/// The decoder's factories not in `read` yet, now added to it.
fn unread(decoder: &Decoder, read: &mut HashSet<Address>) -> Vec<Address> {
    decoder
        .parents()
        .into_iter()
        .filter(|&parent| read.insert(parent))
        .collect()
}

/// Decodes `logs`, adding each contract they create to `decoder` and `found`.
fn admit(
    decoder: &mut Decoder,
    logs: &[Log],
    found: &mut Vec<Contract>,
) -> Result<(), BackfillError> {
    for log in logs {
        let Some((record, claims)) = decoder.decode(log) else {
            continue;
        };
        if let Err(error) = record
            && error.is_invariant()
        {
            return Err(BackfillError::from(error));
        }
        found.extend(claims.contracts().cloned());
        decoder.admit(claims);
    }
    Ok(())
}

/// Adds `handed`, each with the first block its record names, to `decoder`, for one an
/// earlier run handed over that this run's discovery did not find, and to the
/// `addresses` the fill reads. Returns the highest block below where the running
/// indexer decodes any of them from, its record or `restart` if later, when it is above
/// `to`.
pub(super) fn adopt(
    decoder: &mut Decoder,
    addresses: &mut Vec<Address>,
    handed: &[(Contract, u64)],
    restart: Option<u64>,
    to: u64,
) -> Option<u64> {
    let mut decoded_from = None;
    for (contract, first) in handed {
        decoder.adopt(contract);
        addresses.push(contract.address);
        decoded_from =
            decoded_from.max(Some(restart.map_or(*first, |restart| restart.max(*first))));
    }
    addresses.sort_unstable();
    addresses.dedup();
    decoded_from
        .map(|first| first.saturating_sub(1))
        .filter(|&through| through > to)
}

/// `contracts`, narrowed to those `only` names when it names any.
///
/// # Errors
///
/// Returns [`BackfillError::NotDiscovered`] for an address `only` names that is not
/// among them.
pub(super) fn select(
    only: Option<&[Address]>,
    contracts: Vec<Contract>,
) -> Result<Vec<Contract>, BackfillError> {
    let Some(only) = only else {
        return Ok(contracts);
    };
    let mut by_address: BTreeMap<Address, Contract> = contracts
        .into_iter()
        .map(|contract| (contract.address, contract))
        .collect();
    only.iter()
        .map(|&address| {
            by_address
                .remove(&address)
                .ok_or(BackfillError::NotDiscovered { address })
        })
        .collect()
}

/// Hands the running indexer `found`, with what earlier runs handed over of `protocols`,
/// narrowed to `only`, through the lake's queue, and waits until it records each.
/// Returns each with the first block it decodes it in.
pub(super) async fn handover(
    settings: &BackfillSettings,
    schema: &Schema,
    chain: &ChainId,
    protocols: &[String],
    found: Vec<Contract>,
    width: u64,
    lock: &RunLock,
) -> Result<Vec<(Contract, u64)>, BackfillError> {
    let lake = &settings.lake;
    let mut queue =
        HandoffQueue::open(&lake.uri, &lake.storage, schema, chain.as_str(), width).await?;
    let mut candidates: BTreeMap<Address, Contract> = queue
        .read()
        .await?
        .into_iter()
        .filter(|contract| protocols.contains(&contract.protocol))
        .map(|contract| (contract.address, contract))
        .collect();
    for contract in found {
        candidates.entry(contract.address).or_insert(contract);
    }
    let handed = select(
        settings.run.only.as_deref(),
        candidates.into_values().collect(),
    )?;
    lock.check()?;
    let queued = queue.append(schema, chain, &handed).await?;
    info!(%chain, handed = handed.len(), queued, "contracts handed to the running indexer");
    let started = tokio::time::Instant::now();
    loop {
        let recorded =
            crate::sink::delta::recorded(&lake.uri, &lake.storage, schema, chain.as_str()).await?;
        let mut first: HashMap<Address, u64> = HashMap::new();
        for handoff in recorded {
            first
                .entry(handoff.address)
                .and_modify(|block| *block = (*block).min(handoff.block_number))
                .or_insert(handoff.block_number);
        }
        let missing: Vec<Address> = handed
            .iter()
            .map(|contract| contract.address)
            .filter(|address| !first.contains_key(address))
            .collect();
        let Some(&missed) = missing.first() else {
            return Ok(handed
                .into_iter()
                .map(|contract| {
                    let block = first[&contract.address];
                    (contract, block)
                })
                .collect());
        };
        if started.elapsed() >= HANDOFF_WAIT {
            return Err(BackfillError::NotHandedOver {
                handed: handed.len(),
                recorded: handed.len() - missing.len(),
                first: missed,
                seconds: HANDOFF_WAIT.as_secs(),
            });
        }
        info!(%chain, waiting = missing.len(), "waiting for the running indexer to record the handed-over contracts");
        lock.check()?;
        tokio::time::sleep(HANDOFF_POLL).await;
    }
}

/// Refuses a backfill through `to` unless `store`'s newest `session` decodes each of
/// `protocols` with the event definitions of `wanted`, the backfill's, since a block at
/// or below `to`. Then the running indexer decodes every block above `to`, and the
/// backfill every one at or below it, with the same keys.
pub(super) fn covers(
    store: &'static str,
    session: Option<&Session>,
    wanted: &[SessionEvent],
    protocols: &[String],
    to: u64,
) -> Result<(), BackfillError> {
    let session = session.ok_or(BackfillError::NoSession { store })?;
    for protocol in protocols {
        let events = |events: &[SessionEvent]| -> BTreeSet<(String, B256)> {
            events
                .iter()
                .filter(|event| &event.protocol == protocol)
                .map(|event| (event.contract.clone(), event.event_id))
                .collect()
        };
        let running = events(&session.events);
        if running.is_empty() {
            return Err(BackfillError::NotDecoding {
                store,
                protocol: protocol.clone(),
                from: session.block_number,
            });
        }
        if running != events(wanted) {
            return Err(BackfillError::EventsDiffer {
                store,
                protocol: protocol.clone(),
            });
        }
    }
    if session.block_number > to {
        return Err(BackfillError::SessionAboveBound {
            store,
            from: session.block_number,
            bound: to,
        });
    }
    Ok(())
}
