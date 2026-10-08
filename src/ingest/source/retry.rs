//! Adaptive pacing and retries for the HTTP JSON-RPC transport.
//!
//! Providers publish their limits in incompatible units, if at all, so the rate is
//! learned rather than configured: back off fast, recover slowly, as TCP does. One
//! delay is shared by every request. A transient failure doubles it, and the request is
//! retried after it. Each success shrinks it by an eighth until it reaches zero.
//! Unthrottled, requests go back to back; throttled, the delay settles near the
//! provider's limit. After [`MAX_RETRIES`] the last error is returned as it was.
//!
//! Transient means a rate-limit refusal (HTTP `429`, or a provider's own JSON-RPC code
//! in a `200`, as recognized by alloy's [`ErrorPayload::is_retry_err`]), an overloaded
//! or restarting backend, a timeout, or a dropped connection. Every call this source
//! makes is a read, so a retry cannot apply anything twice.

use std::hash::{BuildHasher as _, RandomState};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use alloy_json_rpc::{RequestPacket, ResponsePacket, RpcError};
use alloy_transport::{TransportError, TransportErrorKind, TransportFut};
use tower::{Layer, Service};
use tracing::warn;

/// Retries after the first attempt. The waits add up to at most a few minutes: long
/// enough to ride out a provider's rate window, short enough that a dead endpoint
/// still stops the indexer.
const MAX_RETRIES: u32 = 10;
/// The delay after a first failure, in milliseconds, doubled on each one after.
const MIN_DELAY_MS: u64 = 250;
/// The longest delay, in milliseconds.
const MAX_DELAY_MS: u64 = 30_000;

/// The pause before each request, in milliseconds, shared by every request through one
/// layer. One number, so an atomic rather than a lock: each change is a single
/// read-modify-write, and nothing else is ordered by it.
#[derive(Debug, Clone, Default)]
struct Pacing(Arc<AtomicU64>);

impl Pacing {
    fn current(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Doubles the delay, within its bounds, and returns the new one.
    fn slow_down(&self) -> u64 {
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
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |ms| Some(change(ms)))
            .unwrap_or_else(|ms| ms);
        change(previous)
    }
}

/// A tower layer that paces and retries requests. See the [module docs](self).
#[derive(Debug, Clone, Default)]
pub(super) struct RetryLayer {
    pacing: Pacing,
}

impl<S> Layer<S> for RetryLayer {
    type Service = RetryService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RetryService {
            inner,
            pacing: self.pacing.clone(),
        }
    }
}

/// The service [`RetryLayer`] wraps a transport in.
#[derive(Debug, Clone)]
pub(super) struct RetryService<S> {
    inner: S,
    pacing: Pacing,
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
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let pacing = self.pacing.clone();
        Box::pin(async move {
            let mut retries = 0;
            loop {
                let delay = pacing.current();
                if delay > 0 {
                    tokio::time::sleep(jitter(delay)).await;
                }
                let result = inner.call(request.clone()).await;
                let Some(cause) = transient(&result) else {
                    pacing.speed_up();
                    return result;
                };
                let delay = pacing.slow_down();
                if retries == MAX_RETRIES {
                    return result;
                }
                retries += 1;
                warn!(
                    retry = retries,
                    max_retries = MAX_RETRIES,
                    delay_ms = delay,
                    %cause,
                    "rpc request failed transiently; backing off"
                );
                std::future::poll_fn(|cx| inner.poll_ready(cx)).await?;
            }
        })
    }
}

/// Why `result` is worth retrying, or `None` when it succeeded or failed for good.
///
/// A batch is retried whole when any call in it was refused for load. One whose errors
/// are all permanent, like a `-32601` for `eth_getBlockReceipts`, passes through for
/// the source to handle.
fn transient(result: &Result<ResponsePacket, TransportError>) -> Option<String> {
    match result {
        Ok(response) => response
            .iter_errors()
            .find(|error| error.is_retry_err())
            .map(|error| format!("rpc error {}: {}", error.code, error.message)),
        Err(RpcError::Transport(TransportErrorKind::HttpError(error))) => {
            matches!(error.status, 408 | 429 | 500 | 502 | 503 | 504)
                .then(|| format!("http status {}", error.status))
        }
        Err(RpcError::Transport(TransportErrorKind::MissingBatchResponse(_))) => {
            Some("batch response missing a call".to_owned())
        }
        // The HTTP transport's connect, send, and body-read failures. The error's
        // message carries the URL, and with it the API key, so it is not logged.
        Err(RpcError::Transport(TransportErrorKind::Custom(error)))
            if error.is::<reqwest::Error>() =>
        {
            Some("network error".to_owned())
        }
        _ => None,
    }
}

/// "Equal jitter": half of `delay_ms`, plus a random share of the other half, so clients
/// that failed together do not come back together.
fn jitter(delay_ms: u64) -> Duration {
    let half = delay_ms / 2;
    // Each `RandomState` is keyed afresh, so hashing nothing with one yields a random
    // `u64` without an RNG dependency. Jitter needs spread, not cryptographic quality.
    let random = RandomState::new().hash_one(());
    Duration::from_millis(half + random % (half + 1))
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use alloy_json_rpc::{Id, Request, RequestPacket, ResponsePacket};
    use alloy_transport::{TransportError, TransportErrorKind, TransportFut};
    use tower::{Layer as _, ServiceExt as _};

    use super::{MAX_RETRIES, RetryLayer};

    fn response(body: &str) -> ResponsePacket {
        serde_json::from_str(body).expect("response")
    }

    /// Sends one request through `layer` over a transport that answers its `n`th call
    /// with `reply(n)`, and reports the result and how many calls were made.
    async fn send(
        layer: &RetryLayer,
        reply: impl Fn(u32) -> Result<ResponsePacket, TransportError> + Send + Sync + 'static,
    ) -> (Result<ResponsePacket, TransportError>, u32) {
        let calls = Arc::new(AtomicU32::new(0));
        let (counter, reply) = (Arc::clone(&calls), Arc::new(reply));
        let transport = tower::service_fn(move |_: RequestPacket| -> TransportFut<'static> {
            let (n, reply) = (counter.fetch_add(1, Ordering::SeqCst), Arc::clone(&reply));
            Box::pin(async move { reply(n) })
        });
        let request = Request::new("eth_blockNumber", Id::Number(1), ());
        let request = RequestPacket::Single(request.serialize().expect("request"));
        let result = layer.layer(transport).oneshot(request).await;
        (result, calls.load(Ordering::SeqCst))
    }

    /// A refusal — an HTTP `429`, or a provider's code in a `200` — is retried, and
    /// the slowdown it caused wears off as requests succeed.
    #[tokio::test(start_paused = true)]
    async fn a_refusal_is_retried_and_the_slowdown_recovers() {
        let layer = RetryLayer::default();
        let (result, calls) = send(&layer, |n| match n {
            0 => Err(TransportErrorKind::http_error(429, String::new())),
            1 => Ok(response(
                r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"rate limited"}}"#,
            )),
            _ => Ok(response(r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#)),
        })
        .await;
        assert!(result.expect("succeeds").as_error().is_none());
        assert_eq!(calls, 3);
        assert!(layer.pacing.current() > 0);
        for _ in 0..40 {
            let (result, _) = send(&layer, |_| {
                Ok(response(r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#))
            })
            .await;
            result.expect("succeeds");
        }
        assert_eq!(layer.pacing.current(), 0);
    }

    /// A permanent error, like the `-32601` the receipt fallback depends on, is
    /// handed back after one call.
    #[tokio::test(start_paused = true)]
    async fn a_permanent_error_is_returned_at_once() {
        let (result, calls) = send(&RetryLayer::default(), |_| {
            Ok(response(
                r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"not found"}}"#,
            ))
        })
        .await;
        assert_eq!(result.expect("a response").first_error_code(), Some(-32601));
        assert_eq!(calls, 1);
    }

    /// Retrying ends, and the provider's own last error is what the caller sees.
    #[tokio::test(start_paused = true)]
    async fn retries_stop_and_return_the_last_error() {
        let (result, calls) = send(&RetryLayer::default(), |_| {
            Err(TransportErrorKind::http_error(503, String::new()))
        })
        .await;
        let error = result.expect_err("gives up");
        assert!(error.to_string().contains("503"), "{error}");
        assert_eq!(calls, MAX_RETRIES + 1);
    }
}
