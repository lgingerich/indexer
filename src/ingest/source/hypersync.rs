//! A backfill source over [Envio HyperSync]: blocks, transactions with their receipts,
//! and logs, thousands of heights per request.
//!
//! `HyperSync` is a hosted copy of a chain's history served through one filtering query
//! endpoint, so a range that costs an RPC node one call per height comes back in a
//! single response. It has no live heads, so it does not
//! [follow them](BlockSource::follows_heads): a run on it backfills the buried heights and
//! stops, and following the chain is a restart on an RPC source, which resumes after what
//! this run committed.
//!
//! # Wire format
//!
//! The query endpoint answers JSON or Arrow. This source reads JSON, as the RPC source
//! does: it needs no client library, and the official one carries Arrow, Parquet, and
//! Cap'n Proto for a format this indexer converts away from at once. Two shapes in the
//! JSON are not what an RPC node sends, and both are decoded here:
//!
//! - Quantities are `0x` hex with leading zeros (`"0x01"`), or bare numbers for block
//!   numbers and indexes; both are read.
//! - Nested lists arrive as bytes. `uncles` and `blob_versioned_hashes` are the 32-byte
//!   hashes concatenated; `access_list` and `authorization_list` are their bincode
//!   encoding.
//!
//! # What a response is checked against
//!
//! Every block in the requested range is asked for (`include_all_blocks`), so a height
//! with nothing selected still has its identity, and the pipeline still links it to its
//! parent. Within a block, transactions must be in index order with no gap, and logs in
//! `log_index` order, each naming the block it was returned for. Two guards catch a
//! backend that answers short. Logs are required when the header's `logsBloom` is set,
//! by the same check the RPC source runs; that check, the ordering checks, and the event
//! order live in the `assemble` module both sources share. And wherever transactions are
//! read, the last one's cumulative gas must be the block's `gas_used`: the RPC source
//! counts a receipt per transaction instead, which cannot fail here, where each row is a
//! transaction joined to its receipt, so this is the check that a trailing transaction
//! was not dropped.
//!
//! [Envio HyperSync]: https://docs.envio.dev/docs/HyperSync/overview

use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use alloy_primitives::{Address, B64, B256, Bloom, Bytes, TxHash, U256};
use alloy_rpc_types_eth::{
    AccessList, AccessListItem, Authorization, SignedAuthorization, Withdrawal,
};
use futures_util::stream;
use serde::de::{self, DeserializeOwned};
use serde::{Deserialize, Deserializer, Serialize};

use super::assemble::{
    BlockRows, check_height, check_log_order, check_logs_present, check_no_leftovers,
};
use super::retry::{Attempt, Backoff, os_seed, transient_status};
use super::{
    BlockSource, Datasets, FetchedBlock, HeadStream, SourceError, inconsistent, malformed,
};
use crate::wire::datasets::evm::{Block, Log, Transaction};
use crate::wire::envelope::{BlockMeta, ChainId};

/// The context every error from this source names.
const CONTEXT: &str = "hypersync";

/// How long one request may take, transfer included. The server stops a query after
/// about five seconds of work, but its answer can be tens of megabytes.
const REQUEST_TIMEOUT: Duration = Duration::from_mins(2);

/// A block's identity, parent link, and the two fields the response is checked against.
const IDENTITY_FIELDS: &[&str] = &[
    "number",
    "hash",
    "parent_hash",
    "timestamp",
    "logs_bloom",
    "gas_used",
];

/// A transaction's place in its block: what a block row lists, and the cumulative gas
/// that shows none is missing.
const TRANSACTION_POSITION_FIELDS: &[&str] = &[
    "block_hash",
    "block_number",
    "transaction_index",
    "hash",
    "cumulative_gas_used",
];

/// A log's place in its block: enough to count a transaction's logs.
const LOG_POSITION_FIELDS: &[&str] = &[
    "block_hash",
    "block_number",
    "transaction_index",
    "log_index",
];

/// The columns one query selects, decided once from the dataset selection.
///
/// This is the whole fetch policy; the query, the checks, and the projection read it
/// rather than the selection, so the table below is the one place the choice is made:
///
/// | Selected | Block columns | Transaction columns | Log columns |
/// | --- | --- | --- | --- |
/// | nothing (a header read) | identity | — | — |
/// | `logs` | identity | — | all |
/// | `blocks` | all | position | — |
/// | `blocks`, `logs` | all | position | all |
/// | `transactions`, with any others | all with `blocks`, else identity | all | all with `logs`, else position |
///
/// "All" is every field of [`RawBlock`], [`RawTransaction`], or [`RawLog`]. Every block
/// of the range is read whatever is selected, so each height has its identity and the
/// pipeline links it. A block row lists its transactions' hashes, so `blocks` alone
/// still reads each transaction's position, with the cumulative gas that checks none is
/// missing; a transaction row counts its logs, so `transactions` alone still reads each
/// log's position.
#[derive(Debug, Clone, Copy)]
struct Plan {
    /// The rows projected.
    datasets: Datasets,
    /// The columns queried for them.
    fields: FieldSelection,
    /// Whether the query reads transactions, which are then checked for a gap and
    /// against the block's gas.
    reads_transactions: bool,
    /// Whether the query reads logs, which are then checked against the block's bloom.
    reads_logs: bool,
}

impl Plan {
    /// # Errors
    ///
    /// Returns [`SourceError::Malformed`] when a row's columns cannot be read from its
    /// struct; see [`columns`].
    fn new(datasets: Datasets) -> Result<Self, SourceError> {
        let Datasets {
            blocks,
            transactions,
            logs,
        } = datasets;
        let reads_transactions = transactions || blocks;
        let reads_logs = logs || transactions;
        let fields = FieldSelection {
            block: if blocks {
                columns::<RawBlock>()?
            } else {
                IDENTITY_FIELDS
            },
            transaction: if transactions {
                columns::<RawTransaction>()?
            } else if reads_transactions {
                TRANSACTION_POSITION_FIELDS
            } else {
                &[]
            },
            log: if logs {
                columns::<RawLog>()?
            } else if reads_logs {
                LOG_POSITION_FIELDS
            } else {
                &[]
            },
        };
        Ok(Self {
            datasets,
            fields,
            reads_transactions,
            reads_logs,
        })
    }

    /// A header read: each block's identity, and no rows.
    const fn headers() -> Self {
        Self {
            datasets: Datasets {
                blocks: false,
                transactions: false,
                logs: false,
            },
            fields: FieldSelection {
                block: IDENTITY_FIELDS,
                transaction: &[],
                log: &[],
            },
            reads_transactions: false,
            reads_logs: false,
        }
    }
}

/// The fields `T` deserializes, read from its derived `Deserialize`: a row's struct is
/// the one list of the columns a full selection asks for, so a field added to it is
/// selected, and none is selected that it lacks.
///
/// # Errors
///
/// Returns [`SourceError::Malformed`] when `T` does not deserialize as a plain struct
/// with fields, so a change to a row's shape fails the source's construction rather
/// than selecting no columns.
fn columns<T: DeserializeOwned>() -> Result<&'static [&'static str], SourceError> {
    /// Answers nothing, and records the field names a struct asks it for.
    struct Columns<'a>(&'a mut &'static [&'static str]);

    impl<'de> Deserializer<'de> for Columns<'_> {
        type Error = de::value::Error;

        fn deserialize_struct<V: de::Visitor<'de>>(
            self,
            _name: &'static str,
            fields: &'static [&'static str],
            _visitor: V,
        ) -> Result<V::Value, Self::Error> {
            *self.0 = fields;
            Err(de::Error::custom("columns recorded"))
        }

        fn deserialize_any<V: de::Visitor<'de>>(
            self,
            _visitor: V,
        ) -> Result<V::Value, Self::Error> {
            Err(de::Error::custom("not a struct"))
        }

        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes
            byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct map enum
            identifier ignored_any
        }
    }

    let mut fields: &'static [&'static str] = &[];
    // Always an error: the deserializer only records what it is asked for.
    drop(T::deserialize(Columns(&mut fields)));
    if fields.is_empty() {
        return Err(malformed(
            "HyperSync columns",
            format!(
                "{} does not deserialize as a struct with fields",
                std::any::type_name::<T>()
            ),
        ));
    }
    Ok(fields)
}

/// A source that backfills one EVM chain from a `HyperSync` endpoint.
pub struct HyperSyncSource {
    chain: ChainId,
    ranges: HyperSyncRanges,
    finality_depth: NonZeroU64,
    client: reqwest::Client,
    query_url: reqwest::Url,
    height_url: reqwest::Url,
    /// The API token, sent as a bearer token. Never logged: `Debug` leaves it out.
    token: String,
    plan: Plan,
    backoff: Backoff,
}

/// How backfill cuts the chain for this source: ranges of `span` heights, `concurrency`
/// of them in flight at once (see [`BlockSource::backfill_span`]).
///
/// One response covers anywhere from a few dozen heights on a busy stretch to tens of
/// thousands on a quiet one, so a range is paged through as many requests as it takes,
/// and is held whole until it is accepted. Each range in flight, or answered and waiting
/// for the one below it, is one more held: memory is about `span` × `concurrency` blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HyperSyncRanges {
    /// Heights per range.
    pub span: NonZeroU64,
    /// Ranges in flight at once.
    pub concurrency: NonZeroUsize,
}

impl HyperSyncSource {
    /// Builds a source for `chain` that reads `datasets` from the `HyperSync` endpoint at
    /// `url` (such as `https://4663.hypersync.xyz`), authorized by `token`, and backfills
    /// in `ranges` through the heights more than `finality_depth` under the head.
    ///
    /// # Errors
    ///
    /// Returns a typed error when `url` is not a URL, the HTTP client cannot be built, or
    /// a row's columns cannot be read from its struct.
    pub fn new(
        chain: impl Into<ChainId>,
        url: &str,
        token: &str,
        datasets: Datasets,
        ranges: HyperSyncRanges,
        finality_depth: NonZeroU64,
    ) -> Result<Self, SourceError> {
        let base = reqwest::Url::parse(url).map_err(|error| {
            malformed(
                "HyperSync endpoint",
                format!("{url:?} is not a URL: {error}"),
            )
        })?;
        let endpoint = |path: &str| {
            base.join(path).map_err(|error| {
                malformed(
                    "HyperSync endpoint",
                    format!("{url:?} cannot take {path}: {error}"),
                )
            })
        };
        Ok(Self {
            chain: chain.into(),
            ranges,
            finality_depth,
            client: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()?,
            query_url: endpoint("query")?,
            height_url: endpoint("height")?,
            token: token.to_owned(),
            plan: Plan::new(datasets)?,
            backoff: Backoff::new(os_seed()),
        })
    }

    /// Queries the heights `from ..= to` for what `plan` selects. The response may stop
    /// short of `to`.
    async fn query(&self, from: u64, to: u64, plan: Plan) -> Result<Response, SourceError> {
        let query = Query {
            from_block: from,
            to_block: to.saturating_add(1),
            include_all_blocks: true,
            join_mode: "JoinNothing",
            transactions: plan.reads_transactions.then_some([All {}]),
            logs: plan.reads_logs.then_some([All {}]),
            field_selection: plan.fields,
        };
        self.send(|client| client.post(self.query_url.clone()).json(&query))
            .await
    }

    /// Sends a request and decodes the answer, retrying a refused rate, an overloaded
    /// backend, or a dropped connection through the source's [`Backoff`], as the RPC
    /// source retries its calls. After the last retry, the last error is returned as it
    /// was.
    async fn send<T: DeserializeOwned>(
        &self,
        request: impl Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    ) -> Result<T, SourceError> {
        let request = &request;
        self.backoff
            .run(CONTEXT, || self.attempt(request), judge)
            .await
    }

    /// One try at a request.
    async fn attempt<T: DeserializeOwned>(
        &self,
        request: &impl Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    ) -> Result<T, SourceError> {
        let response = request(&self.client)
            .bearer_auth(&self.token)
            .send()
            .await?;
        let status = response.status();
        // The delay-seconds form only, as alloy reads it for the RPC source; a date is
        // rare, and the backoff covers it.
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse().ok())
            .map(Duration::from_secs);
        let body = response.bytes().await?;
        if !status.is_success() {
            return Err(SourceError::Status {
                context: CONTEXT,
                status: status.as_u16(),
                detail: String::from_utf8_lossy(&body).chars().take(500).collect(),
                retry_after,
            });
        }
        serde_json::from_slice(&body).map_err(|source| SourceError::Json {
            context: CONTEXT,
            source,
        })
    }

    /// The header at `height`, read alone.
    async fn header_at(&self, height: u64) -> Result<BlockMeta, SourceError> {
        let headers = Plan::headers();
        let response = self.query(height, height, headers).await?;
        assemble(height, height, response, headers)?
            .pop()
            .map(|block| block.meta)
            .ok_or_else(|| malformed(CONTEXT, format!("no block at {height}")))
    }
}

/// What `result` came to: whether it is worth sending again, and how it moves the pace.
///
/// The same rule the RPC source's retry layer applies: a transient status, or a request
/// the HTTP client failed in flight — a timeout, a refused or reset connection, one that
/// closed mid-body — is sent again. A request that could not be sent as built, a status
/// such as a refused token, and an answer that does not decode are not: asking again
/// gets the same answer. Every query is a read, so a retry cannot apply anything twice.
fn judge<T>(result: &Result<T, SourceError>) -> Attempt {
    match result {
        Ok(_) => Attempt::Served,
        Err(
            error @ SourceError::Status {
                status,
                retry_after,
                ..
            },
        ) if transient_status(*status) => Attempt::Transient {
            cause: error.to_string(),
            wait: *retry_after,
        },
        Err(SourceError::Http(error)) if error.transient() => Attempt::Transient {
            cause: format!("network error: {error}"),
            wait: None,
        },
        Err(_) => Attempt::Failed,
    }
}

impl fmt::Debug for HyperSyncSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HyperSyncSource")
            .field("chain", &self.chain)
            .field("query_url", &self.query_url.as_str())
            .field("datasets", &self.plan.datasets)
            .finish_non_exhaustive()
    }
}

/// Renders the source as its chain, for log fields.
impl fmt::Display for HyperSyncSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "hypersync[{}]", self.chain)
    }
}

impl BlockSource for HyperSyncSource {
    fn chain(&self) -> &ChainId {
        &self.chain
    }

    fn follows_heads(&self) -> bool {
        false
    }

    /// Ends at once: `HyperSync` announces no heads, and the pipeline never subscribes to
    /// a source that does not follow them.
    async fn subscribe_heads(&self) -> Result<HeadStream, SourceError> {
        Ok(Box::pin(stream::empty()))
    }

    async fn fetch_block(
        &self,
        height: u64,
        _head: Option<&BlockMeta>,
    ) -> Result<FetchedBlock, SourceError> {
        self.fetch_blocks(height, height)
            .await?
            .pop()
            .ok_or_else(|| malformed(CONTEXT, format!("no block at {height}")))
    }

    /// Fetches every height `from ..= to`, with the selected datasets: one request after
    /// another, each starting where the last response stopped.
    async fn fetch_blocks(&self, from: u64, to: u64) -> Result<Vec<FetchedBlock>, SourceError> {
        let mut blocks = Vec::new();
        let mut next = from;
        while next <= to {
            let response = self.query(next, to, self.plan).await?;
            let page = assemble(next, to, response, self.plan)?;
            next = page
                .last()
                .map_or(to, |block| block.meta.height)
                .saturating_add(1);
            blocks.extend(page);
        }
        Ok(blocks)
    }

    fn backfill_span(&self) -> u64 {
        self.ranges.span.get()
    }

    /// The ranges in flight the settings ask for, while one is accepted the next are
    /// fetched: with one, the run stopped for every range's download, minutes at a time.
    fn backfill_concurrency(&self) -> usize {
        self.ranges.concurrency.get()
    }

    /// How far under the head the backfill stops, where the run on an RPC source that
    /// follows the chain takes over.
    fn finality_depth(&self) -> u64 {
        self.finality_depth.get()
    }

    async fn fetch_header(&self, height: Option<u64>) -> Result<BlockMeta, SourceError> {
        let height = match height {
            Some(height) => height,
            None => {
                self.send::<Height>(|client| client.get(self.height_url.clone()))
                    .await?
                    .height
            }
        };
        self.header_at(height).await
    }
}

/// One query's body. See the `HyperSync` query reference for every key.
#[derive(Debug, Serialize)]
struct Query {
    from_block: u64,
    /// Exclusive.
    to_block: u64,
    /// Every block in the range, matched or not, so every height has its identity.
    include_all_blocks: bool,
    /// Only what the selections match, so logs do not pull in their transactions.
    join_mode: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    transactions: Option<[All; 1]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    logs: Option<[All; 1]>,
    field_selection: FieldSelection,
}

/// A selection with no filter, which matches every row.
#[derive(Debug, Serialize)]
struct All {}

/// The columns a query returns, per table.
#[derive(Debug, Clone, Copy, Serialize)]
struct FieldSelection {
    block: &'static [&'static str],
    transaction: &'static [&'static str],
    log: &'static [&'static str],
}

/// The answer to `GET /height`.
#[derive(Debug, Deserialize)]
struct Height {
    height: u64,
}

/// One query's answer: rows in batches, and where the next query should start.
#[derive(Debug, Deserialize)]
struct Response {
    data: Vec<Batch>,
    /// The height after the last one this response covers.
    next_block: u64,
}

/// One batch of rows. A response's batches are in height order.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Batch {
    blocks: Vec<RawBlock>,
    transactions: Vec<RawTransaction>,
    logs: Vec<RawLog>,
}

/// A block as `HyperSync` returns it. Everything but identity is optional, because a
/// query selects only some columns.
#[derive(Debug, Deserialize)]
struct RawBlock {
    number: Quantity,
    hash: B256,
    parent_hash: B256,
    timestamp: Quantity,
    logs_bloom: Bloom,
    gas_used: Quantity,
    #[serde(default)]
    nonce: Option<B64>,
    #[serde(default)]
    sha3_uncles: Option<B256>,
    #[serde(default)]
    transactions_root: Option<B256>,
    #[serde(default)]
    state_root: Option<B256>,
    #[serde(default)]
    receipts_root: Option<B256>,
    #[serde(default)]
    withdrawals_root: Option<B256>,
    #[serde(default)]
    miner: Option<Address>,
    #[serde(default)]
    difficulty: Option<Quantity>,
    #[serde(default)]
    total_difficulty: Option<Quantity>,
    #[serde(default)]
    size: Option<Quantity>,
    #[serde(default)]
    extra_data: Option<Bytes>,
    #[serde(default)]
    gas_limit: Option<Quantity>,
    #[serde(default)]
    base_fee_per_gas: Option<Quantity>,
    #[serde(default)]
    blob_gas_used: Option<Quantity>,
    #[serde(default)]
    excess_blob_gas: Option<Quantity>,
    #[serde(default)]
    parent_beacon_block_root: Option<B256>,
    #[serde(default)]
    mix_hash: Option<B256>,
    /// Bincode; see [`withdrawals`].
    #[serde(default)]
    withdrawals: Option<Bytes>,
    #[serde(default)]
    l1_block_number: Option<Quantity>,
    #[serde(default)]
    send_count: Option<Quantity>,
    #[serde(default)]
    send_root: Option<B256>,
    /// The ommers' hashes, concatenated.
    #[serde(default)]
    uncles: Option<Bytes>,
}

impl RawBlock {
    fn meta(&self) -> Result<BlockMeta, SourceError> {
        Ok(BlockMeta {
            height: self.number.to_u64("block number")?,
            hash: self.hash,
            parent_hash: self.parent_hash,
            timestamp: self.timestamp.to_u64("block timestamp")?,
        })
    }

    /// The `blocks` row, with the block's transaction hashes in index order.
    fn into_block(
        self,
        meta: BlockMeta,
        transaction_hashes: Vec<TxHash>,
    ) -> Result<Block, SourceError> {
        Ok(Block {
            number: meta.height,
            hash: meta.hash,
            parent_hash: meta.parent_hash,
            timestamp: meta.timestamp,
            // As the RPC source: a block without a nonce has a zero one.
            nonce: self.nonce.unwrap_or_default(),
            ommers_hash: required(self.sha3_uncles, "sha3_uncles")?,
            transactions_root: required(self.transactions_root, "transactions_root")?,
            state_root: required(self.state_root, "state_root")?,
            receipts_root: required(self.receipts_root, "receipts_root")?,
            withdrawals_root: self.withdrawals_root,
            logs_bloom: self.logs_bloom,
            miner: required(self.miner, "miner")?,
            difficulty: self
                .difficulty
                .map_or(U256::ZERO, |difficulty| difficulty.0),
            total_difficulty: self.total_difficulty.map(|difficulty| difficulty.0),
            size: self.size.map(|size| size.0),
            extra_data: required(self.extra_data, "extra_data")?,
            gas_limit: required(self.gas_limit, "gas_limit")?.to_u64("gas_limit")?,
            gas_used: self.gas_used.to_u64("gas_used")?,
            transaction_count: transaction_hashes.len() as u64,
            base_fee_per_gas: optional_u64(self.base_fee_per_gas, "base_fee_per_gas")?,
            blob_gas_used: optional_u64(self.blob_gas_used, "blob_gas_used")?,
            excess_blob_gas: optional_u64(self.excess_blob_gas, "excess_blob_gas")?,
            parent_beacon_block_root: self.parent_beacon_block_root,
            mix_hash: self.mix_hash,
            withdrawals: self
                .withdrawals
                .as_ref()
                .map(|bytes| withdrawals(bytes))
                .transpose()?,
            // `HyperSync` serves no `requestsHash`, so a backfilled Ethereum block has none.
            requests_hash: None,
            l1_block_number: optional_u64(self.l1_block_number, "l1_block_number")?,
            send_count: optional_u64(self.send_count, "send_count")?,
            send_root: self.send_root,
            ommers: hashes(self.uncles.as_ref(), "uncles")?,
            transaction_hashes,
        })
    }
}

/// A transaction joined to its receipt, as `HyperSync` returns it.
#[derive(Debug, Deserialize)]
struct RawTransaction {
    block_hash: B256,
    block_number: Quantity,
    transaction_index: Quantity,
    hash: TxHash,
    #[serde(default)]
    nonce: Option<Quantity>,
    #[serde(default)]
    from: Option<Address>,
    #[serde(default)]
    to: Option<Address>,
    #[serde(default)]
    value: Option<Quantity>,
    #[serde(default)]
    gas: Option<Quantity>,
    #[serde(default)]
    gas_price: Option<Quantity>,
    #[serde(default)]
    max_fee_per_gas: Option<Quantity>,
    #[serde(default)]
    max_priority_fee_per_gas: Option<Quantity>,
    #[serde(default)]
    max_fee_per_blob_gas: Option<Quantity>,
    #[serde(default)]
    input: Option<Bytes>,
    #[serde(default, rename = "type")]
    transaction_type: Option<Quantity>,
    #[serde(default)]
    chain_id: Option<Quantity>,
    /// Bincode; see [`access_list`].
    #[serde(default)]
    access_list: Option<Bytes>,
    /// Concatenated hashes.
    #[serde(default)]
    blob_versioned_hashes: Option<Bytes>,
    /// Bincode; see [`authorization_list`].
    #[serde(default)]
    authorization_list: Option<Bytes>,
    #[serde(default)]
    r: Option<Quantity>,
    #[serde(default)]
    s: Option<Quantity>,
    /// As the node reports it: a legacy transaction's EIP-155 value, a typed one's parity.
    #[serde(default)]
    v: Option<Quantity>,
    #[serde(default)]
    y_parity: Option<Quantity>,
    #[serde(default)]
    status: Option<Quantity>,
    /// The pre-Byzantium post-state root a receipt carries instead of a status.
    #[serde(default)]
    root: Option<B256>,
    #[serde(default)]
    gas_used: Option<Quantity>,
    #[serde(default)]
    cumulative_gas_used: Option<Quantity>,
    #[serde(default)]
    effective_gas_price: Option<Quantity>,
    #[serde(default)]
    contract_address: Option<Address>,
    #[serde(default)]
    logs_bloom: Option<Bloom>,
    #[serde(default)]
    blob_gas_used: Option<Quantity>,
    #[serde(default)]
    blob_gas_price: Option<Quantity>,
    #[serde(default)]
    gas_used_for_l1: Option<Quantity>,
    #[serde(default)]
    l1_block_number: Option<Quantity>,
}

impl RawTransaction {
    /// The `transactions` row.
    fn into_transaction(
        self,
        meta: BlockMeta,
        transaction_index: u64,
        log_count: u64,
    ) -> Result<Transaction, SourceError> {
        Ok(Transaction {
            hash: self.hash,
            nonce: required(self.nonce, "nonce")?.to_u64("nonce")?,
            transaction_index,
            from: required(self.from, "from")?,
            to: self.to,
            value: required(self.value, "value")?.0,
            gas: required(self.gas, "gas")?.to_u64("gas")?,
            gas_price: optional_u128(self.gas_price, "gas_price")?,
            max_fee_per_gas: optional_u128(self.max_fee_per_gas, "max_fee_per_gas")?,
            max_priority_fee_per_gas: optional_u128(
                self.max_priority_fee_per_gas,
                "max_priority_fee_per_gas",
            )?,
            max_fee_per_blob_gas: optional_u128(self.max_fee_per_blob_gas, "max_fee_per_blob_gas")?,
            input: required(self.input, "input")?,
            transaction_type: u8::try_from(
                required(self.transaction_type, "type")?.to_u64("type")?,
            )
            .map_err(|_| malformed(CONTEXT, "transaction type is above 255"))?,
            chain_id: optional_u64(self.chain_id, "chain_id")?,
            access_list: self
                .access_list
                .as_ref()
                .map(|bytes| access_list(bytes))
                .transpose()?,
            blob_versioned_hashes: self
                .blob_versioned_hashes
                .as_ref()
                .map(|bytes| hashes(Some(bytes), "blob_versioned_hashes"))
                .transpose()?,
            authorization_list: self
                .authorization_list
                .as_ref()
                .map(|bytes| authorization_list(bytes))
                .transpose()?,
            r: self.r.map(|r| r.0),
            s: self.s.map(|s| s.0),
            v: optional_u128(self.v, "v")?,
            y_parity: optional_u64(self.y_parity, "y_parity")?
                .map(|parity| {
                    u8::try_from(parity).map_err(|_| malformed(CONTEXT, "y_parity is above 255"))
                })
                .transpose()?,
            // As alloy reads an RPC receipt: a status when there is one, and success for a
            // pre-Byzantium receipt, which has a state root instead.
            receipt_status: match (self.status, self.root) {
                (Some(status), _) => status.to_u64("status")? == 1,
                (None, Some(_)) => true,
                (None, None) => return Err(missing("status")),
            },
            receipt_gas_used: required(self.gas_used, "gas_used")?.to_u64("gas_used")?,
            receipt_cumulative_gas_used: required(self.cumulative_gas_used, "cumulative_gas_used")?
                .to_u64("cumulative_gas_used")?,
            receipt_effective_gas_price: required(self.effective_gas_price, "effective_gas_price")?
                .to_u128("effective_gas_price")?,
            receipt_contract_address: self.contract_address,
            receipt_logs_bloom: required(self.logs_bloom, "logs_bloom")?,
            receipt_blob_gas_used: optional_u64(self.blob_gas_used, "blob_gas_used")?,
            receipt_blob_gas_price: optional_u128(self.blob_gas_price, "blob_gas_price")?,
            receipt_gas_used_for_l1: optional_u64(self.gas_used_for_l1, "gas_used_for_l1")?,
            receipt_l1_block_number: optional_u64(self.l1_block_number, "l1_block_number")?,
            log_count,
            block_timestamp: meta.timestamp,
            block_number: meta.height,
            block_hash: meta.hash,
        })
    }
}

/// A log as `HyperSync` returns it, its topics already flattened.
#[derive(Debug, Deserialize)]
struct RawLog {
    block_hash: B256,
    block_number: Quantity,
    transaction_index: Quantity,
    log_index: Quantity,
    #[serde(default)]
    transaction_hash: Option<TxHash>,
    #[serde(default)]
    address: Option<Address>,
    #[serde(default)]
    data: Option<Bytes>,
    #[serde(default)]
    topic0: Option<B256>,
    #[serde(default)]
    topic1: Option<B256>,
    #[serde(default)]
    topic2: Option<B256>,
    #[serde(default)]
    topic3: Option<B256>,
    #[serde(default)]
    removed: Option<bool>,
}

impl RawLog {
    /// The `logs` row.
    fn into_log(
        self,
        meta: BlockMeta,
        transaction_index: u64,
        log_index: u64,
    ) -> Result<Log, SourceError> {
        Ok(Log {
            log_index,
            transaction_hash: required(self.transaction_hash, "transaction_hash")?,
            transaction_index,
            address: required(self.address, "address")?,
            topic0: self.topic0,
            topic1: self.topic1,
            topic2: self.topic2,
            topic3: self.topic3,
            data: required(self.data, "data")?,
            removed: self.removed.unwrap_or(false),
            block_number: meta.height,
            block_hash: meta.hash,
            block_timestamp: meta.timestamp,
        })
    }
}

/// Splits one response into a block per height, from `from` to its `next_block`.
///
/// Rows come back sorted by block, then by index; each block takes the next run of rows
/// carrying its number, and a row left over at the end, or out of order, is an error.
/// Events are ordered as every source orders them; see [`BlockRows::into_events`].
fn assemble(
    from: u64,
    to: u64,
    response: Response,
    plan: Plan,
) -> Result<Vec<FetchedBlock>, SourceError> {
    let Response { data, next_block } = response;
    if next_block <= from {
        return Err(inconsistent(
            CONTEXT,
            format!("made no progress past {from}; its archive may not reach that height yet"),
        ));
    }
    let last = next_block - 1;
    if last > to {
        return Err(malformed(
            CONTEXT,
            format!("answered through {last} for heights {from}..={to}"),
        ));
    }
    let mut blocks = Vec::new();
    let mut transactions = Vec::new();
    let mut logs = Vec::new();
    for batch in data {
        blocks.extend(batch.blocks);
        transactions.extend(batch.transactions);
        logs.extend(batch.logs);
    }
    let expected = usize::try_from(next_block - from).unwrap_or(usize::MAX);
    if blocks.len() != expected {
        return Err(malformed(
            CONTEXT,
            format!("{} blocks for heights {from}..={last}", blocks.len()),
        ));
    }

    let mut transactions = transactions.into_iter().peekable();
    let mut logs = logs.into_iter().peekable();
    let mut fetched = Vec::with_capacity(blocks.len());
    for (height, block) in (from..=last).zip(blocks) {
        let meta = block.meta()?;
        check_height(CONTEXT, height, meta.height)?;
        let own_transactions = take_block(&mut transactions, meta, |row| {
            (row.block_number, row.block_hash)
        })?;
        let own_logs = take_block(&mut logs, meta, |row| (row.block_number, row.block_hash))?;
        let events = project(block, meta, own_transactions, own_logs, plan)?.into_events();
        fetched.push(FetchedBlock { meta, events });
    }
    let leftover = transactions.next().is_some() || logs.next().is_some();
    check_no_leftovers(CONTEXT, from, last, leftover)?;
    Ok(fetched)
}

/// Takes the next run of `rows` at `meta`'s height, each checked to name its block.
fn take_block<T>(
    rows: &mut std::iter::Peekable<std::vec::IntoIter<T>>,
    meta: BlockMeta,
    identity: impl Fn(&T) -> (Quantity, B256),
) -> Result<Vec<T>, SourceError> {
    let mut own = Vec::new();
    while let Some(row) = rows.peek() {
        let (number, hash) = identity(row);
        if number.to_u64("block_number")? != meta.height {
            break;
        }
        if hash != meta.hash {
            return Err(inconsistent(
                CONTEXT,
                format!(
                    "a row at {} belongs to block {hash}, not {}",
                    meta.height, meta.hash
                ),
            ));
        }
        own.extend(rows.next());
    }
    Ok(own)
}

/// A log row with its two indexes read.
struct PlacedLog {
    log_index: u64,
    transaction_index: u64,
    row: RawLog,
}

/// Reads a block's logs' indexes, refusing logs out of `log_index` order.
fn place_logs(logs: Vec<RawLog>, meta: BlockMeta) -> Result<Vec<PlacedLog>, SourceError> {
    let placed = logs
        .into_iter()
        .map(|row| {
            Ok(PlacedLog {
                log_index: row.log_index.to_u64("log_index")?,
                transaction_index: row.transaction_index.to_u64("transaction_index")?,
                row,
            })
        })
        .collect::<Result<Vec<_>, SourceError>>()?;
    check_log_order(CONTEXT, meta.height, placed.iter().map(|log| log.log_index))?;
    Ok(placed)
}

/// Checks a block's rows against each other and its header: logs present when the bloom
/// is set, transactions in index order with no gap, the last one's cumulative gas the
/// block's, and every log naming a transaction the block has.
fn check_block(
    block: &RawBlock,
    meta: BlockMeta,
    transactions: &[RawTransaction],
    placed: &[PlacedLog],
    plan: Plan,
) -> Result<(), SourceError> {
    if plan.reads_logs {
        check_logs_present(CONTEXT, meta.height, !placed.is_empty(), &block.logs_bloom)?;
    }
    for (position, transaction) in transactions.iter().enumerate() {
        if transaction.transaction_index.to_u64("transaction_index")? != position as u64 {
            return Err(malformed(
                CONTEXT,
                format!(
                    "transaction {position} of block {} is missing or out of order",
                    meta.height
                ),
            ));
        }
    }
    if plan.reads_transactions {
        let cumulative = match transactions.last() {
            Some(last) => required(last.cumulative_gas_used, "cumulative_gas_used")?
                .to_u64("cumulative_gas_used")?,
            None => 0,
        };
        let gas_used = block.gas_used.to_u64("gas_used")?;
        if cumulative != gas_used {
            return Err(inconsistent(
                CONTEXT,
                format!(
                    "block {} used {gas_used} gas, but its receipts add to {cumulative}",
                    meta.height
                ),
            ));
        }
    }
    if plan.reads_transactions
        && let Some(log) = placed
            .iter()
            .find(|log| log.transaction_index >= transactions.len() as u64)
    {
        return Err(malformed(
            CONTEXT,
            format!(
                "a log of block {} names transaction {}, which it does not have",
                meta.height, log.transaction_index
            ),
        ));
    }
    Ok(())
}

/// One block's rows, from its header and the rows that carry its number.
fn project(
    block: RawBlock,
    meta: BlockMeta,
    transactions: Vec<RawTransaction>,
    logs: Vec<RawLog>,
    plan: Plan,
) -> Result<BlockRows, SourceError> {
    let placed = place_logs(logs, meta)?;
    check_block(&block, meta, &transactions, &placed, plan)?;
    let datasets = plan.datasets;

    let mut rows = BlockRows::default();
    if datasets.blocks {
        let hashes = transactions
            .iter()
            .map(|transaction| transaction.hash)
            .collect();
        rows.block = Some(block.into_block(meta, hashes)?);
    }
    if datasets.transactions {
        let mut placed = placed.into_iter().peekable();
        for (position, transaction) in transactions.into_iter().enumerate() {
            let index = position as u64;
            let mut count = 0;
            let mut own = Vec::new();
            while let Some(log) = placed.next_if(|log| log.transaction_index == index) {
                count += 1;
                if datasets.logs {
                    own.push(log.row.into_log(meta, index, log.log_index)?);
                }
            }
            rows.transactions
                .push((transaction.into_transaction(meta, index, count)?, own));
        }
        if placed.next().is_some() {
            return Err(malformed(
                CONTEXT,
                format!(
                    "logs of block {} are not grouped by transaction",
                    meta.height
                ),
            ));
        }
    } else if datasets.logs {
        for log in placed {
            rows.logs.push(
                log.row
                    .into_log(meta, log.transaction_index, log.log_index)?,
            );
        }
    }
    Ok(rows)
}

/// An unsigned integer as `HyperSync` writes it: a JSON number, or `0x` hex that may carry
/// leading zeros (`"0x01"`), which an RPC quantity may not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Quantity(U256);

impl Quantity {
    fn to_u64(self, field: &str) -> Result<u64, SourceError> {
        u64::try_from(self.0)
            .map_err(|_| malformed(CONTEXT, format!("{field} {} overflows u64", self.0)))
    }

    fn to_u128(self, field: &str) -> Result<u128, SourceError> {
        u128::try_from(self.0)
            .map_err(|_| malformed(CONTEXT, format!("{field} {} overflows u128", self.0)))
    }
}

impl<'de> Deserialize<'de> for Quantity {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl de::Visitor<'_> for Visitor {
            type Value = Quantity;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an unsigned integer, or `0x` hex")
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Quantity, E> {
                Ok(Quantity(U256::from(value)))
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Quantity, E> {
                parse_quantity(value).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

/// Parses `0x` hex, leading zeros allowed.
fn parse_quantity(text: &str) -> Result<Quantity, String> {
    let digits = text
        .strip_prefix("0x")
        .filter(|digits| !digits.is_empty())
        .ok_or_else(|| format!("{text:?} is not 0x hex"))?;
    U256::from_str_radix(digits, 16)
        .map(Quantity)
        .map_err(|error| format!("{text:?} is not a 256-bit quantity: {error}"))
}

/// 32-byte hashes, concatenated. Absent is none.
fn hashes(bytes: Option<&Bytes>, field: &str) -> Result<Vec<B256>, SourceError> {
    let Some(bytes) = bytes else {
        return Ok(Vec::new());
    };
    if bytes.len() % 32 != 0 {
        return Err(malformed(
            CONTEXT,
            format!(
                "{field} is {} bytes, not a whole number of hashes",
                bytes.len()
            ),
        ));
    }
    Ok(bytes.chunks_exact(32).map(B256::from_slice).collect())
}

/// An access list, from `HyperSync`'s bincode: a list of items, each an optional address
/// and an optional list of storage keys, every value its `0x` hex string.
fn access_list(bytes: &[u8]) -> Result<AccessList, SourceError> {
    let mut reader = Bincode::new(bytes, "access_list");
    let count = reader.len()?;
    let mut items = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let address = if reader.option()? {
            reader.parse::<Address>()?
        } else {
            return Err(malformed(CONTEXT, "access_list item has no address"));
        };
        let mut storage_keys = Vec::new();
        if reader.option()? {
            for _ in 0..reader.len()? {
                storage_keys.push(reader.parse::<B256>()?);
            }
        }
        items.push(AccessListItem {
            address,
            storage_keys,
        });
    }
    reader.finish()?;
    Ok(AccessList(items))
}

/// An authorization list, from `HyperSync`'s bincode: a list of authorizations, each its
/// chain id, address, nonce, y parity, `r`, and `s` as `0x` hex strings.
fn authorization_list(bytes: &[u8]) -> Result<Vec<SignedAuthorization>, SourceError> {
    let mut reader = Bincode::new(bytes, "authorization_list");
    let count = reader.len()?;
    let mut authorizations = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let chain_id = reader.quantity()?.0;
        let address = reader.parse::<Address>()?;
        let nonce = reader.quantity()?.to_u64("authorization nonce")?;
        let y_parity = u8::try_from(reader.quantity()?.to_u64("authorization y_parity")?)
            .map_err(|_| malformed(CONTEXT, "authorization y_parity is above 255"))?;
        let r = reader.quantity()?.0;
        let s = reader.quantity()?.0;
        authorizations.push(SignedAuthorization::new_unchecked(
            Authorization {
                chain_id,
                address,
                nonce,
            },
            y_parity,
            r,
            s,
        ));
    }
    reader.finish()?;
    Ok(authorizations)
}

/// Validator withdrawals, from `HyperSync`'s bincode: a list of withdrawals, each its
/// index, validator index, address, and amount in gwei, every one an optional `0x` hex
/// string.
fn withdrawals(bytes: &[u8]) -> Result<Vec<Withdrawal>, SourceError> {
    let mut reader = Bincode::new(bytes, "withdrawals");
    let count = reader.len()?;
    let mut withdrawals = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let index = reader.required_quantity("index")?;
        let validator_index = reader.required_quantity("validator_index")?;
        if !reader.option()? {
            return Err(reader.error("has a withdrawal with no address"));
        }
        let address = reader.parse::<Address>()?;
        let amount = reader.required_quantity("amount")?;
        withdrawals.push(Withdrawal {
            index,
            validator_index,
            address,
            amount,
        });
    }
    reader.finish()?;
    Ok(withdrawals)
}

/// A reader over bincode's default encoding, the one `HyperSync` writes nested fields in:
/// lengths are little-endian `u64`, an option is a `0` or `1` byte before its value,
/// and a string is its length and its UTF-8 bytes.
struct Bincode<'a> {
    bytes: &'a [u8],
    field: &'static str,
}

impl<'a> Bincode<'a> {
    const fn new(bytes: &'a [u8], field: &'static str) -> Self {
        Self { bytes, field }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], SourceError> {
        if self.bytes.len() < count {
            return Err(self.error("ends early"));
        }
        let (taken, rest) = self.bytes.split_at(count);
        self.bytes = rest;
        Ok(taken)
    }

    fn len(&mut self) -> Result<usize, SourceError> {
        let mut word = [0; 8];
        word.copy_from_slice(self.take(8)?);
        usize::try_from(u64::from_le_bytes(word)).map_err(|_| self.error("has a length past usize"))
    }

    fn option(&mut self) -> Result<bool, SourceError> {
        match self.take(1)? {
            [0] => Ok(false),
            [1] => Ok(true),
            _ => Err(self.error("has an option tag that is neither 0 nor 1")),
        }
    }

    fn string(&mut self) -> Result<&'a str, SourceError> {
        let len = self.len()?;
        std::str::from_utf8(self.take(len)?)
            .map_err(|_| self.error("has a string that is not UTF-8"))
    }

    fn parse<T: std::str::FromStr>(&mut self) -> Result<T, SourceError> {
        let text = self.string()?;
        T::from_str(text).map_err(|_| self.error(&format!("has {text:?}, which does not parse")))
    }

    fn quantity(&mut self) -> Result<Quantity, SourceError> {
        let text = self.string()?;
        parse_quantity(text).map_err(|detail| self.error(&detail))
    }

    /// An optional quantity that must be present, as a `u64`.
    fn required_quantity(&mut self, name: &str) -> Result<u64, SourceError> {
        if !self.option()? {
            return Err(self.error(&format!("has an item with no {name}")));
        }
        self.quantity()?.to_u64(name)
    }

    fn finish(&self) -> Result<(), SourceError> {
        if self.bytes.is_empty() {
            Ok(())
        } else {
            Err(self.error("has bytes past its end"))
        }
    }

    fn error(&self, detail: &str) -> SourceError {
        malformed(CONTEXT, format!("{} {detail}", self.field))
    }
}

/// A selected column the response left out.
fn missing(field: &str) -> SourceError {
    malformed(CONTEXT, format!("missing {field}"))
}

fn required<T>(value: Option<T>, field: &str) -> Result<T, SourceError> {
    value.ok_or_else(|| missing(field))
}

fn optional_u64(value: Option<Quantity>, field: &str) -> Result<Option<u64>, SourceError> {
    value.map(|value| value.to_u64(field)).transpose()
}

fn optional_u128(value: Option<Quantity>, field: &str) -> Result<Option<u128>, SourceError> {
    value.map(|value| value.to_u128(field)).transpose()
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use super::{
        HyperSyncSource, IDENTITY_FIELDS, LOG_POSITION_FIELDS, Plan, RawBlock, RawLog,
        RawTransaction, TRANSACTION_POSITION_FIELDS, assemble, authorization_list, columns,
        parse_quantity, withdrawals,
    };
    use crate::ingest::source::evm::{http_client, ws_connect};
    use crate::ingest::source::{BlockSource, Datasets, EvmSource};
    use crate::wire::envelope::Event;

    /// The ranges the live comparisons fetch in: one at a time, as many heights as a test
    /// asks for at once.
    const RANGES: super::HyperSyncRanges = super::HyperSyncRanges {
        span: std::num::NonZeroU64::MAX,
        concurrency: std::num::NonZeroUsize::MIN,
    };

    /// The default finality depth: the comparisons fetch fixed heights, so it is unused.
    const DEPTH: std::num::NonZeroU64 = std::num::NonZeroU64::MIN;

    /// `HyperSync`'s rows, converted, are the RPC source's rows for the same blocks: every
    /// dataset and field, across the transaction types each chain carries.
    ///
    /// Some fields differ for reasons outside this source, and are let through:
    ///
    /// - `max_fee_per_gas` on Robinhood's Arbitrum transaction types (`0x69`, `0x6a`):
    ///   the node returns one, and the RPC source drops it reading an unknown type.
    /// - `total_difficulty` on a pre-merge Ethereum block: the public node no longer
    ///   serves the deprecated field, and `HyperSync` does.
    /// - `size` on BSC: the node's own answer changes with whether full transactions
    ///   were asked for, so neither number is the block's.
    /// - `requests_hash` on a block past Prague, on any chain: `HyperSync` serves none.
    /// - `y_parity` on a Base deposit transaction (`0x7e`): op-geth reports 0 for the
    ///   unsigned transaction, and `HyperSync` reports none.
    ///
    /// Live: it reads both services, so it needs the network and an Envio token in
    /// `INDEXER_ENVIO_HYPERSYNC_TOKEN`.
    #[tokio::test]
    #[ignore = "network: reads HyperSync and public RPC nodes; needs INDEXER_ENVIO_HYPERSYNC_TOKEN"]
    async fn hypersync_rows_match_the_rpc_source() {
        let token = std::env::var("INDEXER_ENVIO_HYPERSYNC_TOKEN").expect("a HyperSync token");
        let cases: &[(&str, &str, &str, &[u64])] = &[
            (
                "robinhood",
                "https://4663.hypersync.xyz",
                "https://rpc.mainnet.chain.robinhood.com",
                &[1, 40_000_000, 85_000_000],
            ),
            (
                "ethereum",
                "https://eth.hypersync.xyz",
                "https://eth.drpc.org",
                // A pre-Byzantium receipt root, legacy transactions, then blob and
                // EIP-7702 transactions.
                &[1_000_000, 5_000_000, 22_500_000, 22_500_002],
            ),
            (
                "base",
                "https://base.hypersync.xyz",
                "https://base.drpc.org",
                &[30_000_000],
            ),
            (
                "bsc",
                "https://bsc.hypersync.xyz",
                "https://bsc-dataseed.bnbchain.org",
                &[50_000_000],
            ),
        ];
        let all = Datasets::all();
        let logs_only = Datasets {
            blocks: false,
            transactions: false,
            logs: true,
        };
        let mut differences = Vec::new();
        for &(chain, hypersync, rpc, heights) in cases {
            // The public BSC node refuses the RPC source's ranged log batch.
            let selections: &[Datasets] = if chain == "bsc" {
                &[all]
            } else {
                &[all, logs_only]
            };
            for &datasets in selections {
                let hypersync =
                    HyperSyncSource::new(chain, hypersync, &token, datasets, RANGES, DEPTH)
                        .expect("hypersync");
                let rpc = EvmSource::new(
                    chain,
                    http_client(rpc).expect("rpc"),
                    ws_connect(rpc),
                    datasets,
                    std::num::NonZeroUsize::MIN,
                    std::num::NonZeroU64::MAX,
                );
                for &height in heights {
                    let expected = rpc.fetch_block(height, None).await.expect("rpc block");
                    let mut got = hypersync
                        .fetch_blocks(height, height)
                        .await
                        .expect("hypersync block");
                    let got = got.pop().expect("one block");
                    assert_eq!(got.meta, expected.meta, "{chain} {height} meta");
                    assert_eq!(
                        got.events.len(),
                        expected.events.len(),
                        "{chain} {height} {datasets} event count"
                    );
                    let at = format!("{chain} {height} [{datasets}]");
                    for (index, (got, expected)) in
                        got.events.iter().zip(&expected.events).enumerate()
                    {
                        differences.extend(differ(
                            chain,
                            &format!("{at} event {index}"),
                            got,
                            expected,
                        ));
                    }
                }
            }
        }
        assert!(differences.is_empty(), "{}", differences.join("\n"));
    }

    /// The fields of one event that differ between the two sources, less the ones the
    /// test's documentation explains for `chain`.
    fn differ(chain: &str, at: &str, got: &Event, expected: &Event) -> Vec<String> {
        let got = serde_json::to_value(got).expect("json");
        let expected = serde_json::to_value(expected).expect("json");
        let got_fields = got.as_object().expect("an object");
        let expected_fields = expected.as_object().expect("an object");
        let fields = expected_fields.keys().chain(
            got_fields
                .keys()
                .filter(|field| !expected_fields.contains_key(*field)),
        );
        fields
            .filter(|field| {
                !matches!(
                    (chain, field.as_str()),
                    ("robinhood", "max_fee_per_gas")
                        | ("ethereum", "total_difficulty")
                        | ("bsc", "size")
                        // `HyperSync` serves no `requestsHash`.
                        | (_, "requests_hash")
                        // Nor an OP deposit's `yParity`, which op-geth reports as 0.
                        | ("base", "y_parity")
                )
            })
            .filter(|field| got.get(*field) != expected.get(*field))
            .map(|field| {
                format!(
                    "{at} ({}) {field}: hypersync {:?}, rpc {:?}",
                    expected["type"],
                    got.get(field),
                    expected.get(field)
                )
            })
            .collect()
    }

    /// A range wider than one response is paged to its end: every height, once, linked.
    #[tokio::test]
    #[ignore = "network: reads HyperSync; needs INDEXER_ENVIO_HYPERSYNC_TOKEN"]
    async fn a_range_is_paged_to_its_end() {
        let token = std::env::var("INDEXER_ENVIO_HYPERSYNC_TOKEN").expect("a HyperSync token");
        let logs_only = Datasets {
            blocks: false,
            transactions: false,
            logs: true,
        };
        let source = HyperSyncSource::new(
            "robinhood",
            "https://4663.hypersync.xyz",
            &token,
            logs_only,
            RANGES,
            DEPTH,
        )
        .expect("hypersync");
        let blocks = source
            .fetch_blocks(1_000_000, 1_049_999)
            .await
            .expect("range");
        assert_eq!(blocks.len(), 50_000);
        for (height, pair) in (1_000_001..).zip(blocks.windows(2)) {
            assert_eq!(pair[1].meta.height, height);
            assert_eq!(pair[1].meta.parent_hash, pair[0].meta.hash);
        }
    }

    #[test]
    fn a_quantity_takes_leading_zeros_but_not_bare_digits() {
        assert_eq!(
            parse_quantity("0x01").expect("hex").0,
            alloy_primitives::U256::from(1)
        );
        assert_eq!(
            parse_quantity("0x0000").expect("zero").0,
            alloy_primitives::U256::ZERO
        );
        for bare in ["0x", "12"] {
            assert_eq!(
                parse_quantity(bare).err(),
                Some(format!("{bare:?} is not 0x hex"))
            );
        }
    }

    /// An authorization list as `HyperSync` encoded Ethereum's
    /// `0x60c1fb63a6a350008912ae671e6b6630eca2711a24f671d3075bf38f3dbbafcd`.
    #[test]
    fn an_authorization_list_decodes_from_bincode() {
        let bytes = alloy_primitives::hex::decode(
            "010000000000000003000000000000003078312a0000000000000030783561376663313133393765396138616434316266313062663133663232623061363366393666366403000000000000003078300300000000000000307831420000000000000030783534376134663966313238353263393865326330623661343935646239626432653138373430366639396437376635623461623966616631623434313534396541000000000000003078633734343831316537623532323935323235306234386466613436323131386230313661343137373665613230616533313166336230383264653131343964",
        )
        .expect("hex");
        let list = authorization_list(&bytes).expect("decodes");
        assert_eq!(list.len(), 1);
        let authorization = &list[0];
        assert_eq!(authorization.chain_id, alloy_primitives::U256::from(1));
        assert_eq!(
            authorization.address,
            "0x5a7fc11397e9a8ad41bf10bf13f22b0a63f96f6d"
                .parse::<alloy_primitives::Address>()
                .expect("address")
        );
        assert_eq!(authorization.nonce, 0);
        assert_eq!(authorization.y_parity(), 1);
        assert!(
            authorization_list(&bytes[..bytes.len() - 1]).is_err(),
            "a truncated list"
        );
    }

    /// The first of Ethereum block 23,000,000's withdrawals, as `HyperSync` encodes them.
    #[test]
    fn withdrawals_decode_from_bincode() {
        let bytes = alloy_primitives::hex::decode(
            "010000000000000001090000000000000030783562303465656201070000000000000030786237633430012a00000000000000307862396437393334383738623566623936313062336665386135653434316538666164376532393366010900000000000000307831323764626564",
        )
        .expect("hex");
        let list = withdrawals(&bytes).expect("decodes");
        assert_eq!(list.len(), 1);
        assert_eq!(
            (list[0].index, list[0].validator_index, list[0].amount),
            (0x05b0_4eeb, 0x000b_7c40, 0x0127_dbed)
        );
        assert_eq!(
            list[0].address,
            "0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f"
                .parse::<alloy_primitives::Address>()
                .expect("address")
        );
        assert!(
            withdrawals(&bytes[..bytes.len() - 1]).is_err(),
            "a truncated list"
        );
    }

    /// A block's identity row: `number`'s hash is `0x..number`, and its parent the one
    /// below. `bloom` sets the logs bloom, and `gas` its gas used.
    fn block(number: u64, bloom: bool, gas: u64) -> serde_json::Value {
        serde_json::json!({
            "number": number,
            "hash": hash(number),
            "parent_hash": hash(number - 1),
            "timestamp": "0x064",
            "logs_bloom": format!("0x{}{}", if bloom { "01" } else { "00" }, "00".repeat(255)),
            "gas_used": format!("0x{gas:x}"),
        })
    }

    fn hash(number: u64) -> String {
        format!("0x{number:064x}")
    }

    fn log(number: u64, transaction_index: u64, log_index: u64) -> serde_json::Value {
        serde_json::json!({
            "block_hash": hash(number),
            "block_number": number,
            "transaction_index": transaction_index,
            "log_index": log_index,
            "transaction_hash": hash(1000 + transaction_index),
            "address": "0x00000000000000000000000000000000000000aa",
            "data": "0x",
            "topic0": hash(7),
        })
    }

    fn logs_only() -> Datasets {
        Datasets {
            blocks: false,
            transactions: false,
            logs: true,
        }
    }

    fn respond(
        next_block: u64,
        blocks: &[serde_json::Value],
        logs: &[serde_json::Value],
    ) -> super::Response {
        serde_json::from_value(serde_json::json!({
            "data": [{ "blocks": blocks, "logs": logs }],
            "next_block": next_block,
        }))
        .expect("response")
    }

    /// Every height in the response becomes a block, empty ones included, each with the
    /// logs that name it, in order.
    #[test]
    fn a_response_splits_into_a_block_per_height() {
        let response = respond(
            13,
            &[block(10, true, 0), block(11, false, 0), block(12, true, 0)],
            &[log(10, 0, 0), log(10, 1, 1), log(12, 0, 0)],
        );
        let blocks =
            assemble(10, 20, response, Plan::new(logs_only()).expect("plan")).expect("assembles");
        let shape: Vec<_> = blocks
            .iter()
            .map(|block| (block.meta.height, block.events.len()))
            .collect();
        assert_eq!(shape, [(10, 2), (11, 0), (12, 1)]);
        let Event::Log(second) = &blocks[0].events[1] else {
            panic!("a log");
        };
        assert_eq!(
            (
                second.log_index,
                second.transaction_index,
                second.block_timestamp
            ),
            (1, 1, 100)
        );
    }

    /// A response that skips a height, answers a log for another block, leaves logs out
    /// of a block whose bloom says it has some, misorders them, runs past the range, or
    /// makes no progress, is refused: as inconsistent when asking again can help, and as
    /// malformed when it cannot.
    #[test]
    fn a_short_or_mismatched_response_is_refused() {
        let mut foreign = log(10, 0, 0);
        foreign["block_hash"] = hash(99).into();
        let empty: super::Response = serde_json::from_value(serde_json::json!({
            "data": [], "next_block": 10, "archive_height": 9
        }))
        .expect("response");
        for (case, to, response, expected) in [
            (
                "a skipped height",
                20,
                respond(12, &[block(10, false, 0)], &[]),
                "malformed data from hypersync: 1 blocks for heights 10..=11".to_owned(),
            ),
            (
                "a log of another block",
                20,
                respond(11, &[block(10, true, 0)], &[foreign]),
                format!(
                    "inconsistent data from hypersync: a row at 10 belongs to block {}, not {}",
                    hash(99),
                    hash(10)
                ),
            ),
            (
                "no logs under a set bloom",
                20,
                respond(11, &[block(10, true, 0)], &[]),
                "inconsistent data from hypersync: no logs returned for block 10, whose logsBloom is set"
                    .to_owned(),
            ),
            (
                "misordered logs",
                20,
                respond(11, &[block(10, true, 0)], &[log(10, 0, 1), log(10, 0, 0)]),
                "malformed data from hypersync: logs of block 10 are not in ascending logIndex order"
                    .to_owned(),
            ),
            (
                "past the end",
                10,
                respond(12, &[block(10, false, 0), block(11, false, 0)], &[]),
                "malformed data from hypersync: answered through 11 for heights 10..=10".to_owned(),
            ),
            (
                "no progress",
                20,
                empty,
                "inconsistent data from hypersync: made no progress past 10; its archive may not reach that height yet"
                    .to_owned(),
            ),
        ] {
            let error = assemble(10, to, response, Plan::new(logs_only()).expect("plan"))
                .expect_err(case);
            assert_eq!(error.to_string(), expected, "{case}");
        }
    }

    /// With transactions read, the last receipt's cumulative gas must be the block's: for
    /// the `transactions` dataset, and for `blocks` alone, whose row lists the hashes.
    #[test]
    fn a_dropped_transaction_fails_the_gas_check() {
        let transaction = |index: u64, cumulative: u64| {
            serde_json::json!({
                "block_hash": hash(10),
                "block_number": 10,
                "transaction_index": index,
                "hash": hash(1000 + index),
                "nonce": "0x0", "from": "0x00000000000000000000000000000000000000bb",
                "value": "0x0", "gas": "0x5208", "input": "0x", "type": 0,
                "status": "0x1", "gas_used": "0x5208",
                "cumulative_gas_used": format!("0x{cumulative:x}"),
                "effective_gas_price": "0x1",
                "logs_bloom": format!("0x{}", "00".repeat(256)),
            })
        };
        let transactions = Datasets {
            blocks: false,
            transactions: true,
            logs: false,
        };
        let response = |rows: &[serde_json::Value]| -> super::Response {
            serde_json::from_value(serde_json::json!({
                "data": [{ "blocks": [block(10, false, 42_000)], "transactions": rows }],
                "next_block": 11,
            }))
            .expect("response")
        };
        let whole = [transaction(0, 21_000), transaction(1, 42_000)];
        let blocks = assemble(
            10,
            10,
            response(&whole),
            Plan::new(transactions).expect("plan"),
        )
        .expect("assembles");
        let kinds: Vec<&str> = blocks[0].events.iter().map(Event::kind).collect();
        assert_eq!(kinds, ["transaction", "transaction"]);
        let dropped = "inconsistent data from hypersync: block 10 used 42000 gas, but its receipts add to 21000";
        let error = assemble(
            10,
            10,
            response(&whole[..1]),
            Plan::new(transactions).expect("plan"),
        )
        .expect_err("transactions");
        assert_eq!(error.to_string(), dropped);

        // `blocks` alone reads only the transactions' positions, and checks them the same.
        let positions: Vec<_> = whole
            .iter()
            .map(|row| {
                let mut row = row.clone();
                row.as_object_mut()
                    .expect("an object")
                    .retain(|field, _| TRANSACTION_POSITION_FIELDS.contains(&field.as_str()));
                row
            })
            .collect();
        let blocks_only = Datasets {
            blocks: true,
            transactions: false,
            logs: false,
        };
        let error = assemble(
            10,
            10,
            response(&positions[..1]),
            Plan::new(blocks_only).expect("plan"),
        )
        .expect_err("blocks");
        assert_eq!(error.to_string(), dropped);
    }

    /// A full selection asks for every field of a row's struct, by its wire name, and the
    /// partial ones only for fields the structs have. A type that is not a struct with
    /// fields is refused, rather than selecting nothing.
    #[test]
    fn a_full_selection_is_every_field_of_the_row() {
        let blocks = columns::<RawBlock>().expect("blocks");
        let transactions = columns::<RawTransaction>().expect("transactions");
        let logs = columns::<RawLog>().expect("logs");
        assert!(
            transactions.contains(&"type"),
            "the renamed field's wire name"
        );
        for (partial, full) in [
            (IDENTITY_FIELDS, blocks),
            (TRANSACTION_POSITION_FIELDS, transactions),
            (LOG_POSITION_FIELDS, logs),
        ] {
            for field in partial {
                assert!(full.contains(field), "{field}");
            }
        }
        assert_eq!(
            columns::<u64>().err().map(|error| error.to_string()),
            Some(
                "malformed data from HyperSync columns: u64 does not deserialize as a struct with fields"
                    .to_owned()
            )
        );
    }
}
