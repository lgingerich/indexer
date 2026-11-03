//! A search of a chain's history for the logs some contracts emitted, over `HyperSync`:
//! what finds the contracts a factory created among hundreds of millions of blocks in a
//! few hundred requests, where a node's `eth_getLogs` may take a few hundred heights a
//! request.

use alloy_primitives::{Address, B256};
use serde::{Deserialize, Serialize};

use crate::ingest::source::{SourceError, malformed};
use crate::wire::datasets::evm::Log;
use crate::wire::envelope::BlockMeta;

use super::wire::{Quantity, RawLog};
use super::{CONTEXT, REQUEST_TIMEOUT, answer, judge};
use crate::ingest::source::retry::{Attempt, Backoff, os_seed};

/// The log columns a search returns: every column of a `logs` row.
const LOG_FIELDS: &[&str] = &[
    "block_number",
    "block_hash",
    "transaction_index",
    "log_index",
    "transaction_hash",
    "address",
    "data",
    "topic0",
    "topic1",
    "topic2",
    "topic3",
];

/// The block columns a search returns: those placing its logs in time.
const BLOCK_FIELDS: &[&str] = &["number", "hash", "timestamp"];

/// How long a search waits before asking again for a page its backoff gave up on, the
/// endpoint refusing or failing longer than a request's retries last.
const PAGE_PAUSE: std::time::Duration = if cfg!(test) {
    std::time::Duration::from_millis(10)
} else {
    std::time::Duration::from_mins(5)
};

/// A chain's `HyperSync` endpoint, searched for logs.
pub struct LogSearch {
    client: reqwest::Client,
    query_url: reqwest::Url,
    /// Never logged: it is not in `Debug`.
    token: String,
    /// The pace and retries every page is sent with, as the `HyperSync` source sends its
    /// queries.
    backoff: Backoff,
}

impl std::fmt::Debug for LogSearch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogSearch")
            .field("query_url", &self.query_url.as_str())
            .finish_non_exhaustive()
    }
}

/// A search's body.
#[derive(Debug, Serialize)]
struct Query<'a> {
    from_block: u64,
    /// Exclusive.
    to_block: u64,
    logs: [Selection<'a>; 1],
    field_selection: Fields,
}

/// The logs a search matches: emitted by any of `address`.
#[derive(Debug, Serialize)]
struct Selection<'a> {
    address: &'a [Address],
}

#[derive(Debug, Serialize)]
struct Fields {
    block: &'static [&'static str],
    log: &'static [&'static str],
}

/// A search's answer: rows in batches, where the next request starts, and the highest
/// block the endpoint holds.
#[derive(Debug, Deserialize)]
struct Response {
    data: Vec<Batch>,
    next_block: u64,
    #[serde(default)]
    archive_height: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct Batch {
    blocks: Vec<Header>,
    logs: Vec<RawLog>,
}

#[derive(Debug, Deserialize)]
struct Header {
    number: Quantity,
    hash: B256,
    timestamp: Quantity,
}

impl LogSearch {
    /// The search of the chain at `url`, such as `https://1.hypersync.xyz`, authorized by
    /// `token`.
    ///
    /// # Errors
    ///
    /// Returns a typed error when `url` is not a URL, or the HTTP client cannot be built.
    pub fn new(url: &str, token: &str) -> Result<Self, SourceError> {
        let query_url = reqwest::Url::parse(url)
            .and_then(|base| base.join("query"))
            .map_err(|error| {
                malformed(
                    "HyperSync endpoint",
                    format!("{url:?} is not a URL: {error}"),
                )
            })?;
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()?,
            query_url,
            token: token.to_owned(),
            backoff: Backoff::new(os_seed()),
        })
    }

    /// Every log `addresses` emitted from the first block through `through`, in block and
    /// log order, each with its block's timestamp. An endpoint not yet at `through` is
    /// asked again every few minutes until it is, keeping what it answered: a search
    /// never returns with the blocks above the endpoint's height missing.
    ///
    /// # Errors
    ///
    /// Returns the first request's error, or a row that is not what was selected.
    pub async fn logs(&self, addresses: &[Address], through: u64) -> Result<Vec<Log>, SourceError> {
        let to_block = through.saturating_add(1);
        let mut logs = Vec::new();
        let mut from_block = 0;
        let mut pages = 0_u64;
        while from_block < to_block {
            let query = Query {
                from_block,
                to_block,
                logs: [Selection { address: addresses }],
                field_selection: Fields {
                    block: BLOCK_FIELDS,
                    log: LOG_FIELDS,
                },
            };
            let response = self.page(&query).await?;
            pages += 1;
            if pages.is_multiple_of(25) {
                tracing::info!(
                    pages,
                    from_block,
                    to_block,
                    logs = logs.len(),
                    "searching history"
                );
            }
            for batch in response.data {
                logs.extend(rows(batch)?);
            }
            // An endpoint below `through` answers up to its own height: what it answered
            // is kept, and the rest asked for once it has caught up.
            if let Some(height) = response
                .archive_height
                .filter(|&height| height < through && response.next_block > height)
            {
                tracing::warn!(
                    height,
                    through,
                    "HyperSync is behind the search; asking again later"
                );
                from_block = from_block.max(height.saturating_add(1));
                tokio::time::sleep(PAGE_PAUSE).await;
                continue;
            }
            if response.next_block <= from_block {
                return Err(malformed(CONTEXT, "a search answered without moving on"));
            }
            from_block = response.next_block;
        }
        logs.sort_unstable_by_key(|log| (log.block_number, log.log_index));
        Ok(logs)
    }

    /// One page of a search, sent through the source's backoff, which honors the
    /// endpoint's `Retry-After`. A page still refused, or failing in flight, once the
    /// backoff gives up is asked again after [`PAGE_PAUSE`], for as long as it takes: a long
    /// search keeps what it read, and none is given up on while the endpoint is only slow
    /// or down. Any other error, such as a refused token, is returned.
    async fn page(&self, query: &Query<'_>) -> Result<Response, SourceError> {
        loop {
            let answer = self
                .backoff
                .run(CONTEXT, || self.request(query), judge)
                .await;
            match &answer {
                Err(error) if matches!(judge(&answer), Attempt::Transient { .. }) => {
                    tracing::warn!(%error, from_block = query.from_block,
                        "a search page is still refused; asking again later");
                    tokio::time::sleep(PAGE_PAUSE).await;
                }
                _ => return answer,
            }
        }
    }

    async fn request(&self, query: &Query<'_>) -> Result<Response, SourceError> {
        answer(
            self.client.post(self.query_url.clone()).json(query),
            &self.token,
        )
        .await
    }
}

/// `batch`'s logs, each placed in its block.
fn rows(batch: Batch) -> Result<Vec<Log>, SourceError> {
    let mut blocks = std::collections::HashMap::new();
    for header in batch.blocks {
        blocks.insert(
            header.hash,
            BlockMeta {
                height: header.number.to_u64("block number")?,
                hash: header.hash,
                parent_hash: B256::ZERO,
                timestamp: header.timestamp.to_u64("block timestamp")?,
            },
        );
    }
    batch
        .logs
        .into_iter()
        .map(|log| {
            let meta = *blocks
                .get(&log.block_hash)
                .ok_or_else(|| malformed(CONTEXT, "a log's block was not returned"))?;
            let transaction_index = log.transaction_index.to_u64("transaction_index")?;
            let log_index = log.log_index.to_u64("log_index")?;
            log.into_log(meta, transaction_index, log_index)
        })
        .collect()
}

/// A stand-in `HyperSync` endpoint, for tests here and elsewhere.
#[cfg(test)]
#[expect(clippy::expect_used)]
pub(crate) mod testing {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    /// An endpoint answering each request with the next of `answers`, a status and a
    /// body, and the requests' bodies it was sent.
    pub(crate) async fn endpoint(
        answers: Vec<(u16, String)>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bound");
        let url = format!("http://{}/", listener.local_addr().expect("an address"));
        let served = tokio::spawn(async move {
            let mut bodies = Vec::new();
            for (status, body) in answers {
                let (mut stream, _) = listener.accept().await.expect("a request");
                let mut read = Vec::new();
                let mut buffer = [0; 4096];
                loop {
                    let n = stream.read(&mut buffer).await.expect("read");
                    read.extend_from_slice(&buffer[..n]);
                    let text = String::from_utf8_lossy(&read).to_string();
                    if let Some(end) = text.find("\r\n\r\n") {
                        let length = text
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .map(str::to_owned)
                            })
                            .and_then(|length| length.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        if read.len() >= end + 4 + length {
                            bodies.push(text[end + 4..].to_owned());
                            break;
                        }
                    }
                }
                let reply = format!(
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(reply.as_bytes()).await.expect("written");
            }
            bodies
        });
        (url, served)
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{Address, B256};

    use super::LogSearch;
    use super::testing::endpoint;

    /// One page's answer: a log of `address` in block `height`, and where the next starts.
    fn page(height: u64, next: u64, archive: u64) -> String {
        let hash = format!(
            "{:#x}",
            B256::with_last_byte(u8::try_from(height).expect("small"))
        );
        format!(
            r#"{{"data":[{{"blocks":[{{"number":{height},"hash":"{hash}","timestamp":"0x10"}}],
            "logs":[{{"block_hash":"{hash}","block_number":{height},"transaction_index":0,
            "log_index":1,"transaction_hash":"{hash}","address":"{:#x}","data":"0x",
            "topic0":"{hash}"}}]}}],"next_block":{next},"archive_height":{archive}}}"#,
            Address::repeat_byte(7)
        )
    }

    /// A refused rate is waited out on the same page; the pages are read in turn, each log
    /// placed in its block; and an endpoint short of `through` is asked again once it has
    /// caught up, for the blocks above the height it answered to, rather than the search
    /// ending without them.
    #[tokio::test]
    async fn a_search_waits_out_refusals_and_an_endpoint_behind_it() {
        let (url, served) = endpoint(vec![
            (429, "slow down".to_owned()),
            (200, page(5, 50, 99)),
            (200, page(60, 100, 99)),
            (200, page(150, 201, 300)),
        ])
        .await;
        let search = LogSearch::new(&url, "token").expect("built");
        let logs = search
            .logs(&[Address::repeat_byte(7)], 200)
            .await
            .expect("searched");
        assert_eq!(
            logs.iter()
                .map(|log| (log.block_number, log.block_timestamp))
                .collect::<Vec<_>>(),
            [(5, 16), (60, 16), (150, 16)]
        );
        let bodies = served.await.expect("served");
        assert_eq!(
            bodies.len(),
            4,
            "the refused page asked again, then the next"
        );
        assert!(bodies[2].contains(r#""from_block":50"#), "{}", bodies[2]);
        assert!(bodies[3].contains(r#""from_block":100"#), "{}", bodies[3]);
        assert!(
            !format!("{search:?}").contains("token"),
            "the token is never shown"
        );
    }

    /// A refused token is not asked again: the search returns its error.
    #[tokio::test]
    async fn a_refused_token_ends_the_search() {
        let (url, _served) = endpoint(vec![(401, "no".to_owned())]).await;
        let search = LogSearch::new(&url, "token").expect("built");
        assert!(search.logs(&[Address::ZERO], 10).await.is_err());
    }
}
