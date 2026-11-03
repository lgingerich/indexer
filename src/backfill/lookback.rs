//! Filling matched pools' history from the lake, beside the running indexer (see
//! `crate::decode::pairs`): the rows before the block the run started keeping a pool
//! from, decoded from the logs the lake holds, as a backfill does.
//!
//! A [`Fill`] names the pools one block recorded and the block before it. The task
//! waits until the lake's bound (see [`LogReader::bound`]) reaches that block, so it
//! writes nothing the running store may still delete, looking at the lake's ledger alone
//! while it waits. It then takes the backfill lease (see [`RunLock`]) and reads:
//!
//! - a pool contract's logs, from the lake's first block: its rows into the lake's
//!   decoded tables, which the run never decoded before it matched it, and into the
//!   stores keeping matched pools from the pool's `from_timestamp`;
//! - a contract holding many pools, Uniswap V4's `PoolManager`, from shortly before the
//!   earliest `from_timestamp` of its pools, and only its pools' logs, by the topic
//!   naming the pool: their rows into the stores keeping matched pools. The lake has
//!   them already, since the run decodes every one of that contract's records.
//!
//! It writes in the backfill's commits (see [`super::COMMIT_BLOCKS`]), and stops between
//! two when the run is stopped, letting go of the lease. A failure is logged and the
//! fill tried again a minute later; the decode stage hears of a fill only once it is done,
//! and records it so.

use std::collections::{HashMap, HashSet};
use std::ops::RangeInclusive;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use tokio::sync::mpsc;
use tracing::{info, warn};

use super::{BackfillError, Database, cores, decode, parts, rows};
use crate::decode::{Catalog, Decoder, Fill, PoolIds, pool_contract};
use crate::sink::delta::{
    DecodedTables, DeltaSettings, LedgerEnds, LockError, LogReader, RunLock, ledger_ends,
};
use crate::sink::stop::Stop;
use crate::sink::table::Schema;
use crate::wire::datasets::evm::{Contract, Decoded, Log};
use crate::wire::envelope::{ChainId, Event};

/// How long a fill waits before looking again for the lake's bound to reach it, or for
/// another backfill to let go of the lake, or after it failed.
const RETRY: Duration = if cfg!(test) {
    Duration::from_millis(50)
} else {
    Duration::from_mins(1)
};

/// How much further back than its pools' earliest `from_timestamp` a read of a contract
/// holding many starts, in quarters: the ledger's recent pace is only an estimate of the
/// chain's over a month.
const MARGIN_QUARTERS: u64 = 5;

/// What a fill writes to and decodes with.
#[derive(Debug)]
pub(crate) struct Lookback {
    /// The chain.
    pub(crate) chain: ChainId,
    /// The manifests the running indexer decodes with.
    pub(crate) catalog: Catalog,
    /// The chain's lake.
    pub(crate) lake: DeltaSettings,
    /// The store keeping matched pools, and what it is routed.
    #[cfg(feature = "postgres")]
    pub(crate) postgres: Option<(crate::sink::PostgresSettings, crate::sink::route::Selection)>,
}

/// One fill's pools, by how their logs are found.
#[derive(Debug, Default)]
struct Pools {
    /// Pool contracts, each with the time it is kept from.
    contracts: HashMap<Address, u64>,
    /// Contracts holding many pools: how each names a pool, and its pools with the time
    /// each is kept from.
    managers: HashMap<Address, (PoolIds, HashMap<B256, u64>)>,
    /// The protocols of the pool contracts, whose `decoded_logs` keys the lake reads.
    protocols: Vec<String>,
}

impl Pools {
    /// The time `record`'s pool is kept from, if it is one of these.
    fn from(&self, record: &Decoded) -> Option<u64> {
        if let Some(&from) = self.contracts.get(&record.address) {
            return Some(from);
        }
        let (ids, pools) = self.managers.get(&record.address)?;
        pools.get(&ids.read(record)?).copied()
    }

    /// Whether `log`, of a contract holding many, names one of these pools.
    fn names(&self, log: &Log) -> bool {
        self.managers
            .get(&log.address)
            .is_some_and(|(ids, pools)| ids.of_log(log).is_some_and(|id| pools.contains_key(&id)))
    }
}

impl Lookback {
    /// Makes each fill `fills` sends, one at a time, and sends each on to `filled` once it
    /// is done, until either channel closes or the run is stopped.
    pub(crate) async fn run(
        self,
        mut fills: mpsc::Receiver<Fill>,
        filled: mpsc::Sender<Fill>,
        stop: Stop,
    ) {
        while let Some(fill) = fills.recv().await {
            loop {
                if stop.requested() {
                    return;
                }
                match self.fill(&fill, &stop).await {
                    Ok(true) => break,
                    Ok(false) => {}
                    Err(error) => warn!(chain = %self.chain, through = fill.through, %error,
                        "filling matched pools failed; trying again"),
                }
                tokio::time::sleep(RETRY).await;
            }
            if filled.send(fill).await.is_err() {
                return;
            }
        }
    }

    /// Makes `fill`, returning `false` when it must wait, the lake's bound being below it
    /// or another backfill holding the lake, or when the run stopped part-way.
    pub(super) async fn fill(&self, fill: &Fill, stop: &Stop) -> Result<bool, BackfillError> {
        let schema = self.catalog.schema().clone();
        let lake = &self.lake;
        let chain = self.chain.as_str();
        let Some(ends) = ledger_ends(&lake.uri, &lake.storage, &schema, chain).await? else {
            return Ok(false);
        };
        if ends.bound < fill.through {
            info!(%chain, through = fill.through, bound = ends.bound,
                "matched pools wait for the lake's bound to fill their history");
            return Ok(false);
        }
        let run = format!("matched-{}-{}", std::process::id(), fill.through);
        let lock = match RunLock::acquire(&lake.uri, &lake.storage, chain, &run, "matched").await {
            Ok(lock) => lock,
            Err(LockError::Held { .. }) => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        let written = self.write(fill, &schema, ends, &lock, stop).await;
        let released = lock.release().await;
        let done = written?;
        released?;
        Ok(done)
    }

    /// Writes `fill`'s rows under the lease `lock`, `ends` the lake's ledger as it was
    /// when the fill began, returning `false` when `stop` cut it short.
    async fn write(
        &self,
        fill: &Fill,
        schema: &Schema,
        ends: LedgerEnds,
        lock: &RunLock,
        stop: &Stop,
    ) -> Result<bool, BackfillError> {
        let chain = &self.chain;
        let mut decoder = Decoder::new(self.catalog.clone(), []);
        let pools = pools(&mut decoder, fill);
        let lake = &self.lake;
        let reader =
            LogReader::open(&lake.uri, &lake.storage, schema, chain.as_str(), false).await?;
        let mut tables = if pools.contracts.is_empty() {
            None
        } else {
            Some(
                DecodedTables::open(
                    &lake.uri,
                    &lake.storage,
                    schema,
                    chain.as_str(),
                    reader.width(),
                )
                .await?,
            )
        };
        let mut database = self.database(schema).await?;
        let mut written = Written::default();
        // Pool contracts from the lake's first block, their decoded rows too; contracts
        // holding many from shortly before their pools' earliest time, only their pools'
        // logs.
        let contracts: Vec<Address> = pools.contracts.keys().copied().collect();
        let managers: Vec<Address> = pools.managers.keys().copied().collect();
        let earliest = pools
            .managers
            .values()
            .flat_map(|(_, pools)| pools.values())
            .min()
            .copied();
        let mut passes: Vec<(Vec<Address>, Vec<RangeInclusive<u64>>)> = Vec::new();
        if !contracts.is_empty() {
            passes.push((contracts, reader.reads(0, fill.through)));
        }
        if let Some(earliest) = earliest {
            let back = ends.newest.timestamp.saturating_sub(earliest) * MARGIN_QUARTERS / 4;
            passes.push((
                managers,
                reader.reads(ends.height_before(back), fill.through),
            ));
        }
        let streams = cores().div_ceil(2);
        for (addresses, reads) in passes {
            for read in reads {
                if stop.requested() {
                    return Ok(false);
                }
                let logs: Vec<Log> = reader
                    .read(read.clone(), &addresses, streams)
                    .await?
                    .into_iter()
                    .filter(|log| !pools.managers.contains_key(&log.address) || pools.names(log))
                    .collect();
                for (blocks, positions) in parts(&logs, &read) {
                    let records = tokio::task::block_in_place(|| {
                        logs[positions]
                            .iter()
                            .map(|log| decode(&mut decoder, chain, log))
                            .filter_map(Result::transpose)
                            .map(|found| found.map(|(record, _)| record))
                            .collect::<Result<Vec<Decoded>, _>>()
                    })?;
                    lock.check()?;
                    if let Some(tables) = &mut tables {
                        let owned = events(
                            records
                                .iter()
                                .filter(|record| pools.contracts.contains_key(&record.address)),
                        );
                        if !owned.is_empty() {
                            written.lake += tables
                                .fill(blocks, &pools.protocols, rows(chain, schema, &owned)?)
                                .await?;
                        }
                    }
                    written.database += self.kept(&mut database, &pools, &records).await?;
                }
            }
        }
        info!(%chain, pools = fill.pools.len(), through = fill.through, lake_rows = written.lake,
            database_rows = written.database, "matched pools' history filled");
        Ok(true)
    }

    /// Upserts the records of `records` the stores keeping matched pools keep, each from
    /// its pool's time, returning how many.
    async fn kept(
        &self,
        database: &mut Database,
        pools: &Pools,
        records: &[Decoded],
    ) -> Result<usize, BackfillError> {
        let kept: Vec<&Decoded> = records
            .iter()
            .filter(|record| {
                pools
                    .from(record)
                    .is_some_and(|from| record.block_timestamp >= from)
            })
            .collect();
        let count = kept.len();
        database.fill(&self.chain, events(kept.into_iter())).await?;
        Ok(count)
    }

    /// The store keeping matched pools, opened beside the running one, when there is one.
    #[cfg_attr(
        not(feature = "postgres"),
        expect(
            clippy::unused_async,
            reason = "the same signature as the `postgres` build's, which connects"
        )
    )]
    async fn database(&self, schema: &Schema) -> Result<Database, BackfillError> {
        #[cfg(feature = "postgres")]
        if let Some((settings, selection)) = &self.postgres {
            let store = crate::sink::PostgresSink::open(
                settings,
                std::sync::Arc::new(schema.select(&selection.tables)),
                self.chain.as_str(),
                crate::sink::postgres::Writer::Beside,
            )
            .await?;
            return Ok(Database { store: Some(store) });
        }
        let _ = schema;
        Ok(Database {
            #[cfg(feature = "postgres")]
            store: None,
        })
    }
}

/// How many rows a fill wrote, to the lake and to the stores keeping matched pools.
#[derive(Debug, Default)]
struct Written {
    lake: usize,
    database: usize,
}

/// `fill`'s pools, the pool contracts added to `decoder`, which knows only the catalog's
/// listed contracts.
fn pools(decoder: &mut Decoder, fill: &Fill) -> Pools {
    let mut pools = Pools::default();
    for pool in &fill.pools {
        if let Some(id) = pool.pool_id {
            let Some(ids) = PoolIds::of(&pool.protocol) else {
                continue;
            };
            pools
                .managers
                .entry(pool.contract)
                .or_insert_with(|| (ids, HashMap::new()))
                .1
                .insert(id, pool.from_timestamp);
            continue;
        }
        let Some(name) = pool_contract(&pool.protocol) else {
            continue;
        };
        decoder.adopt(&Contract {
            protocol: pool.protocol.clone(),
            name: name.to_owned(),
            address: pool.contract,
            factory_address: Address::ZERO,
            transaction_hash: B256::ZERO,
            transaction_index: 0,
            log_index: 0,
            block_number: pool.created_block,
            block_hash: B256::ZERO,
            block_timestamp: 0,
        });
        pools.contracts.insert(pool.contract, pool.from_timestamp);
        pools.protocols.push(pool.protocol.clone());
    }
    let protocols: HashSet<String> = pools.protocols.drain(..).collect();
    pools.protocols = protocols.into_iter().collect();
    pools.protocols.sort_unstable();
    pools
}

/// `records` as the events a store writes.
fn events<'a>(records: impl Iterator<Item = &'a Decoded>) -> Vec<Event> {
    records
        .map(|record| Event::Decoded(Box::new(record.clone())))
        .collect()
}
