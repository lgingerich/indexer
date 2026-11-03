//! The fetches of pools' creation logs the decode stage asks for (see
//! `crate::decode::pairs`), from the chain's `HyperSync` endpoint, on a task of their own
//! so ingest never waits on them.

use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::decode::Query;
use crate::ingest::source::LogSearch;
use crate::wire::datasets::evm::Log;
use crate::wire::envelope::ChainId;

/// What a run matching pools to Metric's pairs needs beside the catalog (see
/// `crate::decode::pairs`).
#[derive(Debug)]
pub(crate) struct Pairs {
    /// How far before the first Metric pool on a pair a pool's rows are kept from, in
    /// seconds.
    pub(crate) lookback: u64,
    /// Where pools' creation logs are searched for.
    pub(crate) search: LogSearch,
    /// Where the fills write, once the stores are known: `None` without a lake.
    #[cfg(feature = "delta")]
    pub(crate) fill: Option<crate::backfill::Lookback>,
}

impl Pairs {
    /// What `settings`' `[decode.pairs]` asks for, `None` when it is absent: the search
    /// of its `HyperSync` endpoint, and the fills, into the first lake `routes` name and,
    /// when it keeps some table to the matched pools, its database, decoded with `catalog`.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::Source`] when the endpoint is not a URL or its client
    /// cannot be built.
    pub(super) fn from_settings(
        settings: &crate::config::Settings,
        routes: &super::Routes,
        catalog: &crate::decode::Catalog,
    ) -> Result<Option<Self>, super::RuntimeError> {
        let Some(pairs) = &settings.decode.pairs else {
            return Ok(None);
        };
        let search = LogSearch::new(&pairs.hypersync_url, pairs.hypersync_token.expose())?;
        #[cfg(feature = "delta")]
        let fill = match routes {
            super::Routes::Store(stores) => stores
                .iter()
                .find_map(|store| store.settings.delta())
                .map(|lake| crate::backfill::Lookback {
                    chain: ChainId::new(&settings.ingest.chain),
                    catalog: catalog.clone(),
                    lake: lake.clone(),
                    // One that keeps no table to the matched pools has none of their
                    // history to fill.
                    #[cfg(feature = "postgres")]
                    postgres: stores
                        .iter()
                        .filter(|store| !store.tables.matched.is_empty())
                        .find_map(|store| {
                            store
                                .settings
                                .postgres()
                                .map(|postgres| (postgres.clone(), store.tables.clone()))
                        }),
                }),
            super::Routes::Print(_) => None,
        };
        #[cfg(not(feature = "delta"))]
        let _ = (routes, catalog);
        Ok(Some(Self {
            lookback: pairs.lookback(),
            search,
            #[cfg(feature = "delta")]
            fill,
        }))
    }
}

/// How many batches wait on each channel between the decode stage and the tasks that
/// fetch and fill matched pools. The decode stage never waits on one: what does not fit
/// is sent again at the next marker.
pub(crate) const QUEUE: usize = 64;

/// Answers each batch of queries `queries` sends with the logs they find, each with its
/// block's timestamp, on `found`, until either channel closes. A batch's queries run one
/// at a time, each waiting out the endpoint's refusals rather than giving up.
pub(crate) async fn lookup(
    search: LogSearch,
    chain: ChainId,
    mut queries: mpsc::Receiver<Vec<Query>>,
    found: mpsc::Sender<Vec<Log>>,
) {
    while let Some(batch) = queries.recv().await {
        // One at a time: each pages through the chain's whole history, and the endpoint
        // limits how fast one token asks.
        let mut searched = Vec::with_capacity(batch.len());
        for query in &batch {
            searched.push(search_retrying(&search, &chain, query).await);
        }
        let logs: Vec<Log> = searched.into_iter().flatten().collect();
        info!(%chain, logs = logs.len(), "creation logs found");
        if !logs.is_empty() && found.send(logs).await.is_err() {
            return;
        }
    }
}

/// The logs `query` finds; none, logged, when it failed for good: the search waits out a
/// refused rate or an endpoint down for as long as it takes (see [`LogSearch::logs`]), so
/// only an error asking again cannot fix, such as a refused token, ends one.
async fn search_retrying(search: &LogSearch, chain: &ChainId, query: &Query) -> Vec<Log> {
    match search
        .logs(&query.addresses, &query.topics, query.through)
        .await
    {
        Ok(logs) => logs,
        Err(error) => {
            warn!(%chain, ?query, %error, "searching for creation logs failed; skipped");
            Vec::new()
        }
    }
}
