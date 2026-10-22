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

mod assemble;
mod bincode;
mod wire;

use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use futures_util::stream;
use serde::Deserializer;
use serde::de::{self, DeserializeOwned};

use super::retry::{Attempt, Backoff, os_seed, transient_status};
use super::{BlockSource, Datasets, FetchedBlock, HeadStream, SourceError, malformed};
use crate::wire::envelope::{BlockMeta, ChainId};

use assemble::assemble;
use wire::{All, FieldSelection, Height, Query, RawBlock, RawLog, RawTransaction, Response};

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

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::ingest::source::evm::{http_client, ws_connect};
    use crate::ingest::source::{BlockSource, Datasets, EvmSource};
    use crate::wire::envelope::Event;

    /// The ranges the live comparisons fetch in: one at a time, as many heights as a test
    /// asks for at once.
    const RANGES: HyperSyncRanges = HyperSyncRanges {
        span: NonZeroU64::MAX,
        concurrency: NonZeroUsize::MIN,
    };

    /// The default finality depth: the comparisons fetch fixed heights, so it is unused.
    const DEPTH: NonZeroU64 = NonZeroU64::MIN;

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
                    NonZeroUsize::MIN,
                    NonZeroU64::MAX,
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
