//! Adaptive pacing and retries for the HTTP JSON-RPC transport, and for the `HyperSync`
//! source, which sends its requests through the same [`Backoff`].
//!
//! Providers publish their limits in incompatible units, if at all, so the rate is
//! learned rather than configured: back off fast, recover slowly, as TCP does. One
//! delay is shared by every request. A transient failure doubles it, and the request is
//! retried after it. Each request the provider serves shrinks it by an eighth until it
//! reaches zero; one that fails for good leaves it as it is, since a refused method or a
//! range too large says nothing about the rate. A provider that names its own wait, in a
//! `Retry-After` header, is waited for at least that long, up to the longest delay.
//!
//! The delay doubles once per burst, as TCP halves its window once per round trip: a
//! failure of a request sent before the last doubling does not double it again. Requests
//! in flight together fail together — one dropped connection, one refused second — and
//! counting each would take the delay to its cap from a single event.
//! Unthrottled, requests go back to back; throttled, the delay settles near the
//! provider's limit. After [`MAX_RETRIES`] the last error is returned as it was.
//!
//! Transient means a rate-limit refusal (HTTP `429`, or a provider's own JSON-RPC code
//! in a `200`, as recognized by alloy's [`ErrorPayload::is_retry_err`]), an overloaded
//! or restarting backend, a timeout, or a dropped connection. Every call this source
//! makes is a read, so a retry cannot apply anything twice.
//!
//! Two failures are not transient for an `eth_getLogs` over more than one height: a
//! timeout, and Infura's `-32005`, which there means the result is too large rather than
//! a rate limit. Resending the same range would only fail again, so both are returned at
//! once for the source to retry smaller. So is a batch over several heights that comes
//! back partly answered and partly refused for rate: that is a provider's cap on calls
//! per batch, which every resend of the batch hits again, not a refused second, which
//! refuses the whole batch. It still slows the pace, in case it was the second.
//!
//! An error saying the node has not reached the block asked for — Infura's "header not
//! found", which alloy counts as a rate limit — is not transient either: it is a backend
//! behind the one that announced the block, which the source reports as inconsistent
//! and the pipeline asks about again within a fraction of a second. Waiting out a backoff
//! of up to [`MAX_DELAY_MS`] here would only delay that.
//!
//! A failure to reach the provider at all is a [`NetworkError`], whatever transport
//! raised it: the layer converts the HTTP client's own errors into one, so the
//! classification below never depends on which client sits underneath, and a simulated
//! transport fails the same way a real one does.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use alloy_json_rpc::{RequestPacket, ResponsePacket, RpcError, SerializedRequest};
use alloy_transport::{TransportError, TransportErrorKind, TransportFut};
use serde::Deserialize;
use thiserror::Error;
use tower::{Layer, Service};
use tracing::warn;

use crate::metrics;

/// Infura's code for a refused rate, also answered to a log query whose result is too
/// large.
const LIMIT_EXCEEDED: i64 = -32005;

/// Retries after the first attempt. The waits add up to at most a few minutes: long
/// enough to ride out a provider's rate window, short enough that a dead endpoint
/// still stops the indexer.
const MAX_RETRIES: u32 = 10;
/// The delay after a first failure, in milliseconds, doubled on each one after.
const MIN_DELAY_MS: u64 = 250;
/// The longest delay, in milliseconds.
const MAX_DELAY_MS: u64 = 30_000;

/// The pause before each request, in milliseconds, shared by every request through one
/// layer, and how many times it has doubled. Atomics rather than a lock: each change is
/// a single read-modify-write, and nothing else is ordered by them.
#[derive(Debug, Clone, Default)]
struct Pacing {
    delay_ms: Arc<AtomicU64>,
    doublings: Arc<AtomicU64>,
}

impl Pacing {
    fn current(&self) -> u64 {
        self.delay_ms.load(Ordering::Relaxed)
    }

    /// Marks a request about to be sent, for [`slow_down`](Self::slow_down).
    fn sent(&self) -> u64 {
        self.doublings.load(Ordering::Relaxed)
    }

    /// Doubles the delay, within its bounds, for a request [`sent`](Self::sent) at
    /// `mark`, unless it has doubled since: then the failure belongs to a burst already
    /// counted. Returns the delay either way.
    fn slow_down(&self, mark: u64) -> u64 {
        if self
            .doublings
            .compare_exchange(
                mark,
                mark.wrapping_add(1),
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_err()
        {
            return self.current();
        }
        self.adjust(|ms| ms.saturating_mul(2).clamp(MIN_DELAY_MS, MAX_DELAY_MS))
    }

    /// Shrinks the delay by an eighth, to zero once it is negligible.
    fn speed_up(&self) {
        self.adjust(|ms| {
            let shrunk = ms - ms / 8;
            if shrunk < MIN_DELAY_MS / 16 {
                0
            } else {
                shrunk
            }
        });
    }

    fn adjust(&self, change: impl Fn(u64) -> u64) -> u64 {
        // The closure always answers `Some`, so this never fails; either way it carries
        // the value `change` was applied to.
        let previous = self
            .delay_ms
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |ms| Some(change(ms)))
            .unwrap_or_else(|ms| ms);
        change(previous)
    }
}

/// The random spread added to each backoff, from a seed.
///
/// `SplitMix64`: one atomic word of state, so every request through one layer draws from
/// one sequence without a lock, and a fixed seed replays the same delays. Jitter needs
/// spread, not cryptographic quality.
#[derive(Debug, Clone)]
struct Jitter(Arc<AtomicU64>);

impl Jitter {
    fn new(seed: u64) -> Self {
        Self(Arc::new(AtomicU64::new(seed)))
    }

    fn next(&self) -> u64 {
        const GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut z = self
            .0
            .fetch_add(GAMMA, Ordering::Relaxed)
            .wrapping_add(GAMMA);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// "Equal jitter": half of `delay_ms`, plus a random share of the other half, so
    /// clients that failed together do not come back together.
    fn delay(&self, delay_ms: u64) -> Duration {
        let half = delay_ms / 2;
        Duration::from_millis(half + self.next() % (half + 1))
    }
}

/// A request the HTTP client failed, rather than one the provider answered with an
/// error. Transient unless the request could not be sent as built; a timeout is
/// transient too, except for a log query over several heights, which goes back to be
/// fetched smaller.
///
/// Every HTTP client error either source sees becomes one of these, through its
/// `From<reqwest::Error>`, the one place a request URL is stripped: a provider's URL
/// usually holds its API key, and these errors are logged. It keeps only the message,
/// so a URL cannot come back through it.
#[derive(Debug, Error)]
#[error("{detail}")]
pub struct NetworkError {
    failure: Failure,
    detail: String,
}

/// How a [`NetworkError`]'s request failed, which decides whether to send it again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Failure {
    /// No answer arrived in time.
    Timeout,
    /// The request was sent and no whole answer came back: the connection was refused
    /// or reset, its TLS handshake failed, or it closed mid-response.
    Connection,
    /// The request could not be sent as built — a header its token cannot form, a
    /// redirect loop — so sending it again fails the same way.
    Request,
}

impl NetworkError {
    /// The provider did not answer in time: what the simulation's node fails with, as a
    /// real client's timeout becomes through `From<reqwest::Error>`.
    #[cfg(all(test, feature = "duckdb"))]
    pub(crate) fn timeout(detail: impl Into<String>) -> Self {
        Self {
            failure: Failure::Timeout,
            detail: detail.into(),
        }
    }

    /// Whether sending the request again can succeed: anything but a request that could
    /// not be sent as built.
    pub(crate) fn transient(&self) -> bool {
        self.failure != Failure::Request
    }

    /// Whether the request ran out of time.
    fn timed_out(&self) -> bool {
        self.failure == Failure::Timeout
    }

    /// The HTTP client's error inside a transport error as a [`NetworkError`], or the
    /// error unchanged when it came from anywhere else.
    pub(crate) fn from_transport(error: TransportError) -> TransportError {
        let RpcError::Transport(TransportErrorKind::Custom(error)) = error else {
            return error;
        };
        let error = match error.downcast::<reqwest::Error>() {
            Ok(error) => Box::new(Self::from(*error)),
            Err(error) => error,
        };
        RpcError::Transport(TransportErrorKind::Custom(error))
    }
}

/// Strips the request URL, and keeps the message with every cause under it, which is
/// what says a connection was refused rather than only that sending failed.
///
/// reqwest reports a connection that closes while the body is read as a decode error,
/// since `bytes()` decodes the body; it is a connection failure like any other dropped
/// connection. Only a builder or redirect error is a request that could not be sent.
impl From<reqwest::Error> for NetworkError {
    fn from(error: reqwest::Error) -> Self {
        let error = error.without_url();
        let failure = if error.is_timeout() {
            Failure::Timeout
        } else if error.is_builder() || error.is_redirect() {
            Failure::Request
        } else {
            Failure::Connection
        };
        let mut detail = error.to_string();
        let mut cause = std::error::Error::source(&error);
        while let Some(source) = cause {
            detail.push_str(": ");
            detail.push_str(&source.to_string());
            cause = source.source();
        }
        Self { failure, detail }
    }
}

/// What one attempt at a request came to, which decides the pacing and whether to send it
/// again. See the [module docs](self).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Attempt {
    /// The provider served it: the shared delay shrinks.
    Served,
    /// It failed for a reason that says nothing about the rate, and that sending it again
    /// as it is would not fix: it is returned, and the delay left as it is.
    Failed,
    /// The provider refused it in a way only a smaller request gets past: it is returned
    /// for the source to split, and the delay doubles, as for any refusal.
    Refused,
    /// Refused for load, or lost on the way: the delay doubles and it is sent again,
    /// after at least `wait` when the provider named one.
    Transient {
        /// Why, for the log.
        cause: String,
        /// The provider's `Retry-After`, if it sent one.
        wait: Option<Duration>,
    },
}

/// One source's shared delay and jitter, and the loop every request is retried in: what
/// [`RetryLayer`] wraps a JSON-RPC transport in, and what the `HyperSync` source sends
/// its requests through. See the [module docs](self).
#[derive(Debug, Clone)]
pub(crate) struct Backoff {
    pacing: Pacing,
    jitter: Jitter,
}

impl Backoff {
    /// Backoff whose jitter is drawn from `seed`. A deployment seeds it from the OS; a
    /// simulation passes its own, so a run replays.
    pub(crate) fn new(seed: u64) -> Self {
        Self {
            pacing: Pacing::default(),
            jitter: Jitter::new(seed),
        }
    }

    /// Sends a request through `attempt` until it settles: after the shared delay each
    /// time, and again while `judge` finds it [`Attempt::Transient`], up to
    /// [`MAX_RETRIES`] retries. The last answer is returned as it was. `source` names who
    /// is retrying, in the log and the retry metric.
    pub(crate) async fn run<T, F: Future<Output = T>>(
        &self,
        source: &'static str,
        mut attempt: impl FnMut() -> F,
        judge: impl Fn(&T) -> Attempt,
    ) -> T {
        let mut retries = 0;
        // The wait the provider named after the last refusal, if it named one.
        let mut named = Duration::ZERO;
        loop {
            let delay = self.pacing.current();
            if delay > 0 || !named.is_zero() {
                tokio::time::sleep(self.jitter.delay(delay).max(named)).await;
            }
            let mark = self.pacing.sent();
            let result = attempt().await;
            let (cause, wait) = match judge(&result) {
                Attempt::Served => {
                    self.pacing.speed_up();
                    return result;
                }
                Attempt::Failed => return result,
                Attempt::Refused => {
                    self.pacing.slow_down(mark);
                    return result;
                }
                Attempt::Transient { cause, wait } => (cause, wait),
            };
            let delay = self.pacing.slow_down(mark);
            if retries == MAX_RETRIES {
                return result;
            }
            named = wait.map_or(Duration::ZERO, |wait| {
                wait.min(Duration::from_millis(MAX_DELAY_MS))
            });
            retries += 1;
            metrics::source_retry(source);
            warn!(
                source,
                retry = retries,
                max_retries = MAX_RETRIES,
                delay_ms = delay,
                %cause,
                "request failed transiently; backing off"
            );
        }
    }
}

/// A deployment's jitter seed. Jitter only has to differ between processes, so the
/// OS-keyed hasher seeds it.
pub(crate) fn os_seed() -> u64 {
    std::hash::BuildHasher::hash_one(&std::hash::RandomState::new(), ())
}

/// Whether an HTTP status is worth asking again: a timeout, a refused rate, or an
/// overloaded or restarting backend.
pub(crate) const fn transient_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
}

/// A tower layer that paces and retries requests. See the [module docs](self).
#[derive(Debug, Clone)]
pub(crate) struct RetryLayer {
    backoff: Backoff,
}

impl RetryLayer {
    /// A layer whose backoff jitter is drawn from `seed`. A deployment seeds it from the
    /// OS; a simulation passes its own, so a run replays.
    pub(crate) fn new(seed: u64) -> Self {
        Self {
            backoff: Backoff::new(seed),
        }
    }
}

impl<S> Layer<S> for RetryLayer {
    type Service = RetryService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RetryService {
            inner,
            backoff: self.backoff.clone(),
        }
    }
}

/// The service [`RetryLayer`] wraps a transport in.
#[derive(Debug, Clone)]
pub(crate) struct RetryService<S> {
    inner: S,
    backoff: Backoff,
}

impl<S> Service<RequestPacket> for RetryService<S>
where
    S: Service<
            RequestPacket,
            Response = ResponsePacket,
            Error = TransportError,
            Future = TransportFut<'static>,
        > + Clone
        + Send
        + 'static,
{
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: RequestPacket) -> Self::Future {
        // Tower's contract: the service polled ready takes the call, so keep it for this
        // request and leave a fresh clone behind.
        let clone = self.inner.clone();
        let mut polled = Some(std::mem::replace(&mut self.inner, clone));
        // Each retry sends through a clone of its own, polled ready first.
        let retries = self.inner.clone();
        let backoff = self.backoff.clone();
        Box::pin(async move {
            let request = &request;
            let ranged = request.requests().iter().any(spans_heights);
            backoff
                .run(
                    "rpc",
                    move || {
                        let (mut inner, ready) = match polled.take() {
                            Some(inner) => (inner, true),
                            None => (retries.clone(), false),
                        };
                        let request = request.clone();
                        async move {
                            if !ready {
                                std::future::poll_fn(|cx| inner.poll_ready(cx)).await?;
                            }
                            inner
                                .call(request)
                                .await
                                .map_err(NetworkError::from_transport)
                        }
                    },
                    |result| {
                        // A ranged log query that timed out goes back to be split.
                        if ranged && timed_out(result) {
                            return Attempt::Failed;
                        }
                        judge(request, result)
                    },
                )
                .await
        })
    }
}

/// What `result` came to; see the [module docs](self).
///
/// A batch is retried whole when any call in it was refused for load, unless it covers
/// several heights and other calls were answered: then it is a cap on the batch, and goes
/// back to be split. One whose errors are all permanent, like a `-32601` for
/// `eth_getBlockReceipts`, passes through for the source to handle, as do a ranged log
/// query's [`LIMIT_EXCEEDED`] and an error saying the node has not reached the block.
fn judge(request: &RequestPacket, result: &Result<ResponsePacket, TransportError>) -> Attempt {
    match result {
        Ok(response) => {
            let ranged: Vec<_> = request
                .requests()
                .iter()
                .filter(|call| spans_heights(call))
                .map(SerializedRequest::id)
                .collect();
            let mut errors = response
                .responses()
                .iter()
                .filter_map(|response| Some((&response.id, response.payload.as_error()?)))
                .peekable();
            if errors.peek().is_none() {
                return Attempt::Served;
            }
            let Some((_, error)) = errors.find(|(id, error)| {
                error.is_retry_err()
                    && !not_reached(&error.message)
                    && !(error.code == LIMIT_EXCEEDED && ranged.contains(id))
            }) else {
                return Attempt::Failed;
            };
            let answered = response
                .responses()
                .iter()
                .any(|response| response.payload.is_success());
            if answered && spans_range(request) {
                return Attempt::Refused;
            }
            Attempt::Transient {
                cause: format!("rpc error {}: {}", error.code, error.message),
                wait: None,
            }
        }
        Err(RpcError::Transport(kind)) => match kind {
            TransportErrorKind::MissingBatchResponse(_) => Attempt::Transient {
                cause: "batch response missing a call".to_owned(),
                wait: None,
            },
            TransportErrorKind::Custom(error)
                if error
                    .downcast_ref::<NetworkError>()
                    .is_some_and(NetworkError::transient) =>
            {
                Attempt::Transient {
                    cause: format!("network error: {error}"),
                    wait: None,
                }
            }
            _ => match kind.as_http_error() {
                Some(error) if transient_status(error.status) => Attempt::Transient {
                    cause: format!("http status {}", error.status),
                    wait: kind.retry_after(),
                },
                _ => Attempt::Failed,
            },
        },
        Err(_) => Attempt::Failed,
    }
}

/// Whether a node's error message says it has not reached the height asked for, in the
/// words the providers this indexer runs on use. Geth answers an `eth_getLogs` whose
/// `toBlock` is past its head with `-32602` "block range extends beyond current head
/// block", which on a load-balanced provider is a lagging backend, not a range too large:
/// halving it would only ask the same backend for heights it lacks.
pub(crate) fn not_reached(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    [
        "greater than latest",
        "header not found",
        "block not found",
        "unknown block",
        "beyond current head",
    ]
    .iter()
    .any(|phrase| message.contains(phrase))
}

/// Whether `request` covers several heights, which the source fetches in halves when it
/// fails: a log query over a range, or headers at more than one height.
fn spans_range(request: &RequestPacket) -> bool {
    let calls = request.requests();
    calls.iter().any(spans_heights)
        || calls
            .iter()
            .filter(|call| call.method() == "eth_getBlockByNumber")
            .count()
            > 1
}

/// Whether `result` is a request that ran out of time.
fn timed_out(result: &Result<ResponsePacket, TransportError>) -> bool {
    matches!(
        result,
        Err(RpcError::Transport(TransportErrorKind::Custom(error)))
            if error.downcast_ref::<NetworkError>().is_some_and(NetworkError::timed_out)
    )
}

/// Whether `call` is an `eth_getLogs` over more than one height. A query pinned to a
/// block hash, or bounded by one height, is not.
fn spans_heights(call: &SerializedRequest) -> bool {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Range {
        from_block: Option<String>,
        to_block: Option<String>,
    }
    call.method() == "eth_getLogs"
        && call
            .params()
            .and_then(|params| serde_json::from_str::<(Range,)>(params.get()).ok())
            .is_some_and(|(range,)| range.from_block != range.to_block)
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};

    use alloy_json_rpc::{Id, Request, RequestPacket, ResponsePacket, RpcError};
    use alloy_transport::{TransportError, TransportErrorKind, TransportFut};
    use serde_json::{Value, json};
    use tower::{Layer as _, ServiceExt as _};

    use std::time::Duration;

    use super::{
        MAX_DELAY_MS, MAX_RETRIES, MIN_DELAY_MS, NetworkError, Pacing, RetryLayer, not_reached,
        timed_out,
    };

    fn response(body: &str) -> ResponsePacket {
        serde_json::from_str(body).expect("response")
    }

    /// Sends one `eth_blockNumber` through `layer` over a transport that answers its
    /// `n`th call with `reply(n)`, and reports the result and how many calls were made.
    async fn send(
        layer: &RetryLayer,
        reply: impl Fn(u32) -> Result<ResponsePacket, TransportError> + Send + Sync + 'static,
    ) -> (Result<ResponsePacket, TransportError>, u32) {
        send_call(layer, "eth_blockNumber", Value::Null, reply).await
    }

    /// [`send`] for any one call.
    async fn send_call(
        layer: &RetryLayer,
        method: &'static str,
        params: Value,
        reply: impl Fn(u32) -> Result<ResponsePacket, TransportError> + Send + Sync + 'static,
    ) -> (Result<ResponsePacket, TransportError>, u32) {
        let request = Request::new(method, Id::Number(1), params);
        let request = RequestPacket::Single(request.serialize().expect("request"));
        send_packet(layer, request, reply).await
    }

    /// [`send`] for any request: one call, or a batch.
    async fn send_packet(
        layer: &RetryLayer,
        request: RequestPacket,
        reply: impl Fn(u32) -> Result<ResponsePacket, TransportError> + Send + Sync + 'static,
    ) -> (Result<ResponsePacket, TransportError>, u32) {
        let calls = Arc::new(AtomicU32::new(0));
        let (counter, reply) = (Arc::clone(&calls), Arc::new(reply));
        let transport = tower::service_fn(move |_: RequestPacket| -> TransportFut<'static> {
            let (n, reply) = (counter.fetch_add(1, Ordering::SeqCst), Arc::clone(&reply));
            Box::pin(async move { reply(n) })
        });
        let result = layer.layer(transport).oneshot(request).await;
        (result, calls.load(Ordering::SeqCst))
    }

    /// A layer whose shared delay has doubled once, so a test can see what a request
    /// does to it.
    fn slowed() -> RetryLayer {
        let layer = RetryLayer::new(0);
        layer.backoff.pacing.slow_down(layer.backoff.pacing.sent());
        assert_eq!(layer.backoff.pacing.current(), MIN_DELAY_MS);
        layer
    }

    /// A request that fails for good — a method the node lacks, a ranged log query too
    /// large to answer, a block the node has not reached — is returned after one call,
    /// and leaves the shared delay as it was: it says nothing about the rate. Infura's
    /// "header not found", which alloy counts as a rate limit, is among them, so the
    /// source can ask again soon rather than after a backoff.
    #[tokio::test(start_paused = true)]
    async fn a_permanent_error_neither_retries_nor_moves_the_pace() {
        let ranged = json!([{"fromBlock": "0x64", "toBlock": "0x6d"}]);
        for (case, method, params, body) in [
            (
                "a missing method",
                "eth_getBlockReceipts",
                json!(["0x64"]),
                r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"not found"}}"#,
            ),
            (
                "a ranged log query too large",
                "eth_getLogs",
                ranged,
                r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"query returned more than 10000 results"}}"#,
            ),
            (
                "a block not reached",
                "eth_getBlockByNumber",
                json!(["0x64", false]),
                r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"header not found"}}"#,
            ),
        ] {
            let layer = slowed();
            let (result, calls) =
                send_call(&layer, method, params, move |_| Ok(response(body))).await;
            assert!(result.expect(case).first_error_code().is_some(), "{case}");
            assert_eq!(calls, 1, "{case}");
            assert_eq!(layer.backoff.pacing.current(), MIN_DELAY_MS, "{case}");
        }
    }

    /// Each provider's way of saying it has not reached a height is recognized, geth's
    /// answer to a log range past its head among them, while the ways of saying a range
    /// is too large are not: those go back to be split, not asked again as they are.
    #[test]
    fn not_reached_tells_a_lagging_node_from_a_range_too_large() {
        for message in [
            "Invalid params: requested block 18000000 is greater than latest block 17999999",
            "header not found",
            "block not found",
            "unknown block",
            "block range extends beyond current head block",
        ] {
            assert!(not_reached(message), "{message}");
        }
        for message in [
            "exceed maximum block range 10000",
            "query returned more than 10000 results",
            "query exceeds the maximum range",
            "execution aborted",
        ] {
            assert!(!not_reached(message), "{message}");
        }
    }

    /// A provider's `Retry-After` is waited out before the retry, when it asks for longer
    /// than the backoff would wait, and no longer than the longest delay.
    #[tokio::test(start_paused = true)]
    async fn a_named_wait_is_honoured_up_to_the_longest_delay() {
        for (asked, least, most) in [(5, 5_000, 5_000), (600, MAX_DELAY_MS, MAX_DELAY_MS)] {
            let start = tokio::time::Instant::now();
            let (result, calls) = send(&RetryLayer::new(0), move |n| match n {
                0 => Err(TransportErrorKind::http_error_with_retry_after(
                    429,
                    String::new(),
                    Some(Duration::from_secs(asked)),
                )),
                _ => Ok(response(r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#)),
            })
            .await;
            result.expect("succeeds");
            assert_eq!(calls, 2, "asked {asked} s");
            let waited = u64::try_from(start.elapsed().as_millis()).expect("millis");
            assert!(
                (least..=most).contains(&waited),
                "asked {asked} s, waited {waited} ms"
            );
        }
    }

    /// A batch over several heights that comes back partly answered and partly refused
    /// for rate is a cap on the batch: it goes back at once to be split, and still slows
    /// the pace. A one-height batch refused the same way is retried, since the source
    /// cannot split it.
    #[tokio::test(start_paused = true)]
    async fn a_partly_refused_range_is_returned_to_be_split() {
        let call = |id: u64, method: &'static str, params: Value| {
            Request::new(method, Id::Number(id), params)
                .serialize()
                .expect("request")
        };
        for (case, second, expected) in [
            (
                "headers at two heights",
                call(2, "eth_getBlockByNumber", json!(["0x65", false])),
                1,
            ),
            (
                "a block and its receipts",
                call(2, "eth_getBlockReceipts", json!(["0x64"])),
                2,
            ),
        ] {
            let first = call(1, "eth_getBlockByNumber", json!(["0x64", false]));
            let request = RequestPacket::Batch(vec![first, second]);
            let layer = RetryLayer::new(0);
            let (result, calls) = send_packet(&layer, request, |n| {
                Ok(response(if n == 0 {
                    r#"[{"jsonrpc":"2.0","id":1,"result":null},{"jsonrpc":"2.0","id":2,"error":{"code":429,"message":"too many requests"}}]"#
                } else {
                    r#"[{"jsonrpc":"2.0","id":1,"result":null},{"jsonrpc":"2.0","id":2,"result":null}]"#
                }))
            })
            .await;
            result.expect(case);
            assert_eq!(calls, expected, "{case}");
            if expected == 1 {
                assert_eq!(layer.backoff.pacing.current(), MIN_DELAY_MS, "{case}");
            }
        }
    }

    /// A refusal — an HTTP `429`, or a provider's code in a `200` — is retried after the
    /// doubled delay, and the slowdown it caused wears off as requests succeed.
    #[tokio::test(start_paused = true)]
    async fn a_refusal_is_retried_and_the_slowdown_recovers() {
        let layer = RetryLayer::new(0);
        let start = tokio::time::Instant::now();
        let (result, calls) = send(&layer, |n| match n {
            0 => Err(TransportErrorKind::http_error(429, String::new())),
            1 => Ok(response(
                r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"rate limited"}}"#,
            )),
            _ => Ok(response(r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#)),
        })
        .await;
        let waited = start.elapsed();
        let answer = result.expect("succeeds");
        assert_eq!(answer.first_error_code(), None);
        assert_eq!(calls, 3);
        // The retries waited out the jittered 250 ms, then 500 ms: each between half and
        // all of its delay. Paused time makes the clock exact.
        let (least, most) = (MIN_DELAY_MS / 2 + MIN_DELAY_MS, MIN_DELAY_MS * 3);
        assert!(
            (least..=most).contains(&u64::try_from(waited.as_millis()).expect("millis")),
            "waited {waited:?}"
        );
        // Doubled twice, then an eighth off for the success.
        let doubled = MIN_DELAY_MS * 2;
        assert_eq!(layer.backoff.pacing.current(), doubled - doubled / 8);
        for _ in 0..40 {
            let (result, _) = send(&layer, |_| {
                Ok(response(r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#))
            })
            .await;
            result.expect("succeeds");
        }
        assert_eq!(layer.backoff.pacing.current(), 0);
    }

    /// Retrying ends, and the provider's own last error is what the caller sees.
    #[tokio::test(start_paused = true)]
    async fn retries_stop_and_return_the_last_error() {
        let (result, calls) = send(&RetryLayer::new(0), |_| {
            Err(TransportErrorKind::http_error(503, String::new()))
        })
        .await;
        let error = result.expect_err("gives up");
        assert!(
            matches!(&error, RpcError::Transport(TransportErrorKind::HttpError(http)) if http.status == 503),
            "{error}"
        );
        assert_eq!(calls, MAX_RETRIES + 1);
    }

    /// A real HTTP client timeout: a request to a listener that never answers.
    async fn timeout() -> TransportError {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let url = format!("http://{}", listener.local_addr().expect("address"));
        let error = reqwest::Client::builder()
            .timeout(Duration::from_millis(1))
            .build()
            .expect("client")
            .get(url)
            .send()
            .await
            .expect_err("times out");
        assert!(error.is_timeout(), "{error}");
        TransportErrorKind::custom(error)
    }

    /// A log query over several heights that times out goes back at once, so the source
    /// can retry a smaller range; one height's, or a hash-pinned one's, is retried like
    /// any network failure.
    #[tokio::test(start_paused = true)]
    async fn only_a_ranged_log_query_timeout_is_returned_at_once() {
        for (filter, expected) in [
            (json!({"fromBlock": "0x64", "toBlock": "0x6d"}), 1),
            (
                json!({"fromBlock": "0x64", "toBlock": "0x64"}),
                MAX_RETRIES + 1,
            ),
            (
                json!({"blockHash": format!("0x{}", "ab".repeat(32))}),
                MAX_RETRIES + 1,
            ),
        ] {
            let mut timeouts = Vec::new();
            for _ in 0..=MAX_RETRIES {
                timeouts.push(timeout().await);
            }
            let timeouts = Mutex::new(timeouts);
            let layer = RetryLayer::new(0);
            let (result, calls) = send_call(&layer, "eth_getLogs", json!([filter]), move |_| {
                Err(timeouts
                    .lock()
                    .expect("lock")
                    .pop()
                    .expect("a timeout per call"))
            })
            .await;
            assert!(timed_out(&result), "{filter}: {result:?}");
            assert_eq!(calls, expected, "{filter}");
        }
    }

    /// `-32005` is returned at once for a ranged log query, and retried otherwise.
    #[tokio::test(start_paused = true)]
    async fn only_a_ranged_log_query_limit_exceeded_is_returned_at_once() {
        for (method, filter, expected, code) in [
            (
                "eth_getLogs",
                json!([{"fromBlock": "0x64", "toBlock": "0x6d"}]),
                1,
                Some(-32005),
            ),
            (
                "eth_getLogs",
                json!([{"fromBlock": "0x64", "toBlock": "0x64"}]),
                2,
                None,
            ),
            ("eth_blockNumber", Value::Null, 2, None),
        ] {
            let (result, calls) = send_call(&RetryLayer::new(0), method, filter.clone(), |n| {
                Ok(response(if n == 0 {
                    r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"query returned more than 10000 results"}}"#
                } else {
                    r#"{"jsonrpc":"2.0","id":1,"result":[]}"#
                }))
            })
            .await;
            let answer = result.expect(method);
            assert_eq!(answer.first_error_code(), code, "{method} {filter}");
            assert_eq!(calls, expected, "{method} {filter}");
        }
    }

    /// An HTTP client error loses its URL, which holds the API key, and keeps the cause
    /// under it. A refused connection is worth sending again; a request that cannot be
    /// built is not.
    #[tokio::test]
    async fn a_client_error_drops_its_url_and_is_transient_only_if_sent() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("address");
        drop(listener);
        let client = reqwest::Client::new();
        let refused = client
            .get(format!("http://{address}/secret-key"))
            .send()
            .await
            .expect_err("nothing listens");
        let unbuildable = client
            .get(format!("http://{address}/secret-key"))
            .header("authorization", "Bearer secret-key\n")
            .send()
            .await
            .expect_err("an invalid header");
        for (case, error, transient) in [
            ("refused", refused, true),
            ("unbuildable", unbuildable, false),
        ] {
            let error = NetworkError::from(error);
            let printed = format!("{error} {error:?}");
            assert!(!printed.contains("secret-key"), "{case}: {printed}");
            assert_eq!(error.transient(), transient, "{case}: {printed}");
        }
    }

    /// Requests that were in flight together and failed together double the delay once,
    /// not once each; a failure of one sent after that doubling doubles it again.
    #[test]
    fn a_burst_of_failures_doubles_the_delay_once() {
        let pacing = Pacing::default();
        let mark = pacing.sent();
        for _ in 0..16 {
            assert_eq!(pacing.slow_down(mark), MIN_DELAY_MS);
        }
        let later = pacing.sent();
        assert_eq!(pacing.slow_down(later), MIN_DELAY_MS * 2);
        assert_eq!(pacing.slow_down(mark), MIN_DELAY_MS * 2);
    }
}
