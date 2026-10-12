//! A backfill source over [Envio HyperSync]: blocks, transactions with their receipts,
//! and logs, thousands of heights per request.
//!
//! `HyperSync` is a hosted copy of a chain's history served through one filtering query
//! endpoint, so a range that costs an RPC node one call per height comes back in a
//! single response. It has no live heads: a run on it backfills the buried heights and
//! stops (see [`crate::ingest::pipeline::Machine::backfill_buried`]), and following the chain
//! is a restart on an RPC source, which resumes after what this run committed.
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
//! `log_index` order, each naming the block it was returned for. The same two guards the
//! RPC source applies catch a backend that answers short: logs are required when the
//! header's `logsBloom` is set, and with transactions read, the last receipt's cumulative
//! gas must be the block's `gas_used`, so a dropped transaction cannot pass unseen.
//!
//! [Envio HyperSync]: https://docs.envio.dev/docs/`HyperSync`/overview

use std::fmt;
use std::num::NonZeroUsize;
use std::time::Duration;

use alloy_primitives::{Address, B64, B256, Bloom, Bytes, TxHash, U256};
use alloy_rpc_types_eth::{
    AccessList, AccessListItem, Authorization, SignedAuthorization, Withdrawal,
};
use serde::de::{self, DeserializeOwned};
use serde::{Deserialize, Deserializer, Serialize};
use tracing::warn;

use super::{BlockSource, Datasets, FetchedBlock, HeadStream, SourceError};
use crate::wire::datasets::evm::{Block, Log, Transaction};
use crate::wire::envelope::{BlockMeta, ChainId, Event};

/// The context every error from this source names.
const CONTEXT: &str = "hypersync";

/// Retries after the first attempt, for a refused rate, an overloaded backend, or a
/// dropped connection. The waits add up to a few minutes, as the RPC source's do.
const MAX_RETRIES: u32 = 10;
/// The wait after a first failure, doubled on each one after.
const MIN_DELAY: Duration = Duration::from_millis(250);
/// The longest wait.
const MAX_DELAY: Duration = Duration::from_secs(30);
/// Heights backfill asks for in one [`BlockSource::fetch_blocks`] call. One response
/// covers anywhere from a few dozen heights on a busy stretch to tens of thousands on a
/// quiet one, so a range is paged through as many requests as it takes; this bounds how
/// much one range holds in memory before it is accepted, while leaving each request room
/// to return a full response.
const SPAN: u64 = 10_000;
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

/// Every field the `blocks` dataset carries.
const BLOCK_FIELDS: &[&str] = &[
    "number",
    "hash",
    "parent_hash",
    "timestamp",
    "nonce",
    "sha3_uncles",
    "transactions_root",
    "state_root",
    "receipts_root",
    "withdrawals_root",
    "logs_bloom",
    "miner",
    "difficulty",
    "total_difficulty",
    "size",
    "extra_data",
    "gas_limit",
    "gas_used",
    "base_fee_per_gas",
    "blob_gas_used",
    "excess_blob_gas",
    "parent_beacon_block_root",
    "mix_hash",
    "withdrawals",
    "l1_block_number",
    "send_count",
    "send_root",
    "uncles",
];

/// A transaction's place in its block: what a block row lists, and nothing else.
const TRANSACTION_HASH_FIELDS: &[&str] =
    &["block_hash", "block_number", "transaction_index", "hash"];

/// Every field the `transactions` dataset carries, receipt fields included.
const TRANSACTION_FIELDS: &[&str] = &[
    "block_hash",
    "block_number",
    "transaction_index",
    "hash",
    "nonce",
    "from",
    "to",
    "value",
    "gas",
    "gas_price",
    "max_fee_per_gas",
    "max_priority_fee_per_gas",
    "max_fee_per_blob_gas",
    "input",
    "type",
    "chain_id",
    "access_list",
    "blob_versioned_hashes",
    "authorization_list",
    "r",
    "s",
    "v",
    "y_parity",
    "status",
    "root",
    "gas_used",
    "cumulative_gas_used",
    "effective_gas_price",
    "contract_address",
    "logs_bloom",
    "blob_gas_used",
    "blob_gas_price",
    "gas_used_for_l1",
    "l1_block_number",
];

/// A log's place in its block: enough to count a transaction's logs.
const LOG_POSITION_FIELDS: &[&str] = &[
    "block_hash",
    "block_number",
    "transaction_index",
    "log_index",
];

/// Every field the `logs` dataset carries.
const LOG_FIELDS: &[&str] = &[
    "block_hash",
    "block_number",
    "transaction_index",
    "transaction_hash",
    "log_index",
    "address",
    "data",
    "topic0",
    "topic1",
    "topic2",
    "topic3",
    "removed",
];

/// A source that backfills one EVM chain from a `HyperSync` endpoint.
pub struct HyperSyncSource {
    chain: ChainId,
    client: reqwest::Client,
    query_url: reqwest::Url,
    height_url: reqwest::Url,
    /// The API token, sent as a bearer token. Never logged: `Debug` leaves it out.
    token: String,
    datasets: Datasets,
    /// Backfill ranges in flight at once.
    concurrency: NonZeroUsize,
}

impl HyperSyncSource {
    /// Builds a source for `chain` that reads `datasets` from the `HyperSync` endpoint at
    /// `url` (such as `https://4663.hypersync.xyz`), authorized by `token`. Backfill keeps
    /// `concurrency` ranges in flight, each its own run of requests.
    ///
    /// # Errors
    ///
    /// Returns a typed error when `url` is not a URL or the HTTP client cannot be built.
    pub fn new(
        chain: impl Into<ChainId>,
        url: &str,
        token: &str,
        datasets: Datasets,
        concurrency: NonZeroUsize,
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
            client: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()?,
            query_url: endpoint("query")?,
            height_url: endpoint("height")?,
            token: token.to_owned(),
            datasets,
            concurrency,
        })
    }

    /// What one query asks for, from the selected datasets.
    ///
    /// A block row lists its transactions' hashes, so `blocks` alone still reads each
    /// transaction's hash; a transaction row counts its logs, so `transactions` alone
    /// still reads each log's position.
    fn selection(&self) -> FieldSelection {
        let Datasets {
            blocks,
            transactions,
            logs,
        } = self.datasets;
        FieldSelection {
            block: if blocks {
                BLOCK_FIELDS
            } else {
                IDENTITY_FIELDS
            },
            transaction: if transactions {
                TRANSACTION_FIELDS
            } else if blocks {
                TRANSACTION_HASH_FIELDS
            } else {
                &[]
            },
            log: if logs {
                LOG_FIELDS
            } else if transactions {
                LOG_POSITION_FIELDS
            } else {
                &[]
            },
        }
    }

    /// Queries the heights `from ..= to`. The response may stop short of `to`.
    async fn query(
        &self,
        from: u64,
        to: u64,
        field_selection: FieldSelection,
    ) -> Result<Response, SourceError> {
        let query = Query {
            from_block: from,
            to_block: to.saturating_add(1),
            include_all_blocks: true,
            join_mode: "JoinNothing",
            transactions: (!field_selection.transaction.is_empty()).then_some([All {}]),
            logs: (!field_selection.log.is_empty()).then_some([All {}]),
            field_selection,
        };
        self.send(|client| client.post(self.query_url.clone()).json(&query))
            .await
    }

    /// Sends a request, retrying a refused rate, an overloaded backend, or a dropped
    /// connection with a doubling wait, and decodes the answer.
    async fn send<T: DeserializeOwned>(
        &self,
        request: impl Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    ) -> Result<T, SourceError> {
        let mut delay = MIN_DELAY;
        let mut retries = 0;
        loop {
            let cause = match self.attempt(&request).await {
                Attempt::Done(value) => return Ok(value),
                Attempt::Failed(error) => return Err(error),
                Attempt::Transient(cause) => cause,
            };
            if retries == MAX_RETRIES {
                return Err(malformed(
                    CONTEXT,
                    format!("gave up after {MAX_RETRIES} retries: {cause}"),
                ));
            }
            retries += 1;
            warn!(
                retry = retries,
                max_retries = MAX_RETRIES,
                delay_ms = delay.as_millis(),
                %cause,
                "hypersync request failed transiently; backing off"
            );
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(MAX_DELAY);
        }
    }

    /// One try at a request.
    async fn attempt<T: DeserializeOwned>(
        &self,
        request: &impl Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    ) -> Attempt<T> {
        let response = match request(&self.client).bearer_auth(&self.token).send().await {
            Ok(response) => response,
            Err(error) => return Attempt::from_reqwest(error),
        };
        let status = response.status();
        let body = match response.bytes().await {
            Ok(body) => body,
            Err(error) => return Attempt::from_reqwest(error),
        };
        if status.is_success() {
            return match serde_json::from_slice(&body) {
                Ok(value) => Attempt::Done(value),
                Err(source) => Attempt::Failed(SourceError::Json {
                    context: CONTEXT,
                    source,
                }),
            };
        }
        let detail = String::from_utf8_lossy(&body)
            .chars()
            .take(500)
            .collect::<String>();
        if matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504) {
            Attempt::Transient(format!("http status {status}: {detail}"))
        } else {
            Attempt::Failed(SourceError::Status {
                context: CONTEXT,
                status: status.as_u16(),
                detail,
            })
        }
    }

    /// The header at `height`, read alone.
    async fn header_at(&self, height: u64) -> Result<BlockMeta, SourceError> {
        let only_headers = FieldSelection {
            block: IDENTITY_FIELDS,
            transaction: &[],
            log: &[],
        };
        let block = self
            .query(height, height, only_headers)
            .await?
            .data
            .into_iter()
            .flat_map(|batch| batch.blocks)
            .next()
            .ok_or_else(|| malformed(CONTEXT, format!("no block at {height}")))?;
        let meta = block.meta()?;
        if meta.height != height {
            return Err(malformed(
                CONTEXT,
                format!("requested block {height}, received {}", meta.height),
            ));
        }
        Ok(meta)
    }
}

/// How one try at a request ended.
enum Attempt<T> {
    /// The decoded answer.
    Done(T),
    /// Worth asking again: why it failed.
    Transient(String),
    /// Asking again would fail the same way.
    Failed(SourceError),
}

impl<T> Attempt<T> {
    /// A connection that failed or timed out is transient; anything else, such as a
    /// request that could not be built, is not.
    fn from_reqwest(error: reqwest::Error) -> Self {
        let error = error.without_url();
        if error.is_timeout() || error.is_connect() || error.is_request() || error.is_body() {
            Self::Transient(format!("network error: {error}"))
        } else {
            Self::Failed(SourceError::Http(error))
        }
    }
}

impl fmt::Debug for HyperSyncSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HyperSyncSource")
            .field("chain", &self.chain)
            .field("query_url", &self.query_url.as_str())
            .field("datasets", &self.datasets)
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

    async fn subscribe_heads(&self) -> Result<HeadStream, SourceError> {
        Err(SourceError::Unsupported {
            context: CONTEXT,
            detail: "HyperSync has no live heads; restart on an RPC source to follow the chain",
        })
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
        let selection = self.selection();
        let mut blocks = Vec::new();
        let mut next = from;
        while next <= to {
            let response = self.query(next, to, selection).await?;
            let page = assemble(next, to, response, self.datasets)?;
            next = page
                .last()
                .map_or(to, |block| block.meta.height)
                .saturating_add(1);
            blocks.extend(page);
        }
        Ok(blocks)
    }

    fn backfill_span(&self) -> u64 {
        SPAN
    }

    fn backfill_concurrency(&self) -> usize {
        self.concurrency.get()
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
/// Events are ordered as the RPC source orders them: the block row, then each
/// transaction followed by its logs, or the logs alone when transactions are not
/// selected.
fn assemble(
    from: u64,
    to: u64,
    response: Response,
    datasets: Datasets,
) -> Result<Vec<FetchedBlock>, SourceError> {
    let Response { data, next_block } = response;
    if next_block <= from {
        return Err(inconsistent(format!(
            "made no progress past {from}; its archive may not reach that height yet"
        )));
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
        if meta.height != height {
            return Err(malformed(
                CONTEXT,
                format!("requested block {height}, received {}", meta.height),
            ));
        }
        let own_transactions = take_block(&mut transactions, meta, |row| {
            (row.block_number, row.block_hash)
        })?;
        let own_logs = take_block(&mut logs, meta, |row| (row.block_number, row.block_hash))?;
        let events = project(block, meta, own_transactions, own_logs, datasets)?;
        fetched.push(FetchedBlock { meta, events });
    }
    if transactions.next().is_some() || logs.next().is_some() {
        return Err(malformed(
            CONTEXT,
            format!("rows outside the blocks {from}..={last}, or out of order"),
        ));
    }
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
            return Err(inconsistent(format!(
                "a row at {} belongs to block {hash}, not {}",
                meta.height, meta.hash
            )));
        }
        own.extend(rows.next());
    }
    Ok(own)
}

/// A block's logs in `log_index` order, each with its transaction's index.
type Placed = Vec<(u64, u64, RawLog)>;

/// Orders a block's logs by `log_index`, refusing a misordered or duplicate one.
fn place_logs(logs: Vec<RawLog>, meta: BlockMeta) -> Result<Placed, SourceError> {
    let mut placed: Placed = Vec::with_capacity(logs.len());
    for log in logs {
        let log_index = log.log_index.to_u64("log_index")?;
        if placed
            .last()
            .is_some_and(|(previous, _, _)| *previous >= log_index)
        {
            return Err(malformed(
                CONTEXT,
                format!(
                    "logs of block {} are not in ascending logIndex order",
                    meta.height
                ),
            ));
        }
        placed.push((
            log_index,
            log.transaction_index.to_u64("transaction_index")?,
            log,
        ));
    }
    Ok(placed)
}

/// Checks a block's rows against each other and its header: logs present when the bloom
/// is set, transactions in index order with no gap, the receipts' gas adding to the
/// block's, and every log naming a transaction the block has.
fn check_block(
    block: &RawBlock,
    meta: BlockMeta,
    transactions: &[RawTransaction],
    placed: &Placed,
    datasets: Datasets,
) -> Result<(), SourceError> {
    let reads_transactions = datasets.blocks || datasets.transactions;
    let reads_logs = datasets.logs || datasets.transactions;
    if reads_logs && placed.is_empty() && block.logs_bloom != Bloom::ZERO {
        return Err(inconsistent(format!(
            "no logs returned for block {}, whose logsBloom is set",
            meta.height
        )));
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
    if datasets.transactions {
        let cumulative = match transactions.last() {
            Some(last) => required(last.cumulative_gas_used, "cumulative_gas_used")?
                .to_u64("cumulative_gas_used")?,
            None => 0,
        };
        let gas_used = block.gas_used.to_u64("gas_used")?;
        if cumulative != gas_used {
            return Err(inconsistent(format!(
                "block {} used {gas_used} gas, but its receipts add to {cumulative}",
                meta.height
            )));
        }
    }
    if reads_transactions
        && let Some((_, index, _)) = placed
            .iter()
            .find(|(_, index, _)| *index >= transactions.len() as u64)
    {
        return Err(malformed(
            CONTEXT,
            format!(
                "a log of block {} names transaction {index}, which it does not have",
                meta.height
            ),
        ));
    }
    Ok(())
}

/// One block's events, from its header and the rows that carry its number.
fn project(
    block: RawBlock,
    meta: BlockMeta,
    transactions: Vec<RawTransaction>,
    logs: Vec<RawLog>,
    datasets: Datasets,
) -> Result<Vec<Event>, SourceError> {
    let placed = place_logs(logs, meta)?;
    check_block(&block, meta, &transactions, &placed, datasets)?;

    let mut events = Vec::new();
    if datasets.blocks {
        let hashes = transactions
            .iter()
            .map(|transaction| transaction.hash)
            .collect();
        events.push(Event::Block(Box::new(block.into_block(meta, hashes)?)));
    }
    if datasets.transactions {
        let mut placed = placed.into_iter().peekable();
        for (position, transaction) in transactions.into_iter().enumerate() {
            let index = position as u64;
            let mut own = Vec::new();
            while let Some((log_index, _, log)) = placed.next_if(|(_, tx, _)| *tx == index) {
                own.push((log_index, log));
            }
            events.push(Event::Transaction(Box::new(transaction.into_transaction(
                meta,
                index,
                own.len() as u64,
            )?)));
            if datasets.logs {
                for (log_index, log) in own {
                    events.push(Event::Log(Box::new(log.into_log(meta, index, log_index)?)));
                }
            }
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
        for (log_index, transaction_index, log) in placed {
            events.push(Event::Log(Box::new(log.into_log(
                meta,
                transaction_index,
                log_index,
            )?)));
        }
    }
    Ok(events)
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

fn malformed(context: &str, detail: impl fmt::Display) -> SourceError {
    SourceError::Malformed {
        context: context.to_owned(),
        detail: detail.to_string(),
    }
}

fn inconsistent(detail: impl fmt::Display) -> SourceError {
    SourceError::Inconsistent {
        context: CONTEXT.to_owned(),
        detail: detail.to_string(),
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::num::NonZeroUsize;

    use super::{HyperSyncSource, assemble, authorization_list, parse_quantity, withdrawals};
    use crate::ingest::source::evm::{http_client, ws_connect};
    use crate::ingest::source::{BlockSource, Datasets, EvmSource, SourceError};
    use crate::wire::envelope::Event;

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
                    HyperSyncSource::new(chain, hypersync, &token, datasets, NonZeroUsize::MIN)
                        .expect("hypersync");
                let rpc = EvmSource::new(
                    chain,
                    http_client(rpc).expect("rpc"),
                    ws_connect(rpc),
                    datasets,
                    NonZeroUsize::MIN,
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
            NonZeroUsize::MIN,
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
        assert!(parse_quantity("0x").is_err());
        assert!(parse_quantity("12").is_err());
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
        let blocks = assemble(10, 20, response, logs_only()).expect("assembles");
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
    /// of a block whose bloom says it has some, or misorders them, is refused.
    #[test]
    fn a_short_or_mismatched_response_is_refused() {
        let skipped = respond(12, &[block(10, false, 0)], &[]);
        assert!(
            assemble(10, 20, skipped, logs_only()).is_err(),
            "a skipped height"
        );

        let mut foreign = log(10, 0, 0);
        foreign["block_hash"] = hash(99).into();
        let foreign = respond(11, &[block(10, true, 0)], &[foreign]);
        assert!(matches!(
            assemble(10, 20, foreign, logs_only()),
            Err(SourceError::Inconsistent { .. })
        ));

        let bloomed = respond(11, &[block(10, true, 0)], &[]);
        assert!(matches!(
            assemble(10, 20, bloomed, logs_only()),
            Err(SourceError::Inconsistent { .. })
        ));

        let misordered = respond(11, &[block(10, true, 0)], &[log(10, 0, 1), log(10, 0, 0)]);
        assert!(
            assemble(10, 20, misordered, logs_only()).is_err(),
            "misordered logs"
        );

        let past = respond(12, &[block(10, false, 0), block(11, false, 0)], &[]);
        assert!(assemble(10, 10, past, logs_only()).is_err(), "past the end");
    }

    /// With transactions read, the last receipt's cumulative gas must be the block's.
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
        let whole = serde_json::from_value(serde_json::json!({
            "data": [{
                "blocks": [block(10, false, 42_000)],
                "transactions": [transaction(0, 21_000), transaction(1, 42_000)],
            }],
            "next_block": 11,
        }))
        .expect("response");
        let blocks = assemble(10, 10, whole, transactions).expect("assembles");
        assert_eq!(blocks[0].events.len(), 2);

        let short = serde_json::from_value(serde_json::json!({
            "data": [{
                "blocks": [block(10, false, 42_000)],
                "transactions": [transaction(0, 21_000)],
            }],
            "next_block": 11,
        }))
        .expect("response");
        assert!(matches!(
            assemble(10, 10, short, transactions),
            Err(SourceError::Inconsistent { .. })
        ));
    }

    #[test]
    fn an_empty_response_is_no_progress() {
        let response = serde_json::from_value(serde_json::json!({
            "data": [], "next_block": 10, "archive_height": 9
        }))
        .expect("response");
        assert!(assemble(10, 20, response, Datasets::all()).is_err());
    }
}
