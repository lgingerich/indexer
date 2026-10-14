//! A request to stop the run, which ingest honors at once and each store's drain between
//! commits.
//!
//! Its own module, outside `channel`, because a build without a store stops on it too.

use tokio::sync::watch;

/// A request to stop, which the storage drain honors between commits and ingest at once.
///
/// Every clone sees the one request. A stop whose sender is gone can no longer be
/// requested, and waiting on it waits forever.
#[derive(Debug, Clone)]
pub(crate) struct Stop(watch::Receiver<bool>);

impl Stop {
    /// A stop, and the sender that requests it with `true`.
    pub(crate) fn new() -> (watch::Sender<bool>, Self) {
        let (sender, receiver) = watch::channel(false);
        (sender, Self(receiver))
    }

    /// A stop nothing can request: a run that ends only when ingest does.
    #[cfg(all(test, feature = "store"))]
    pub(crate) fn never() -> Self {
        Self::new().1
    }

    /// Whether the stop has been requested.
    pub(crate) fn requested(&self) -> bool {
        *self.0.borrow()
    }

    /// Waits until the stop is requested; forever, if it no longer can be.
    pub(crate) async fn wait(&mut self) {
        if self.0.wait_for(|stop| *stop).await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}
