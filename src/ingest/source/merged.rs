//! Draining two envelope streams as one.
//!
//! Storage reads the raw topic and the decoded topic, and it must read both at once: a
//! stage that drains one and then the other leaves the second topic unread while it
//! works, which against a bounded transport stalls the publisher and every stage behind
//! it. Interleaving them keeps pace, and on a durable broker it also means a live run
//! sees `decoded` records rather than only ever reaching them after `raw` ends — which,
//! for a live stream, is never.
//!
//! Ends only when *both* inputs end, so a topic that finishes first does not truncate
//! what the other is still delivering.

use anyhow::Result;

use crate::connectors::EnvelopeSource;
use crate::wire::envelope::Envelope;

/// One source backed by two, yielding from whichever produces next.
#[derive(Debug)]
pub struct Merged<S1, S2> {
    first: Option<S1>,
    second: Option<S2>,
}

/// Which input a poll came from, so one that ends can be retired without retiring the
/// other.
enum Side {
    First,
    Second,
}

impl<S1, S2> Merged<S1, S2> {
    /// Interleaves `first` and `second`.
    #[must_use]
    pub const fn new(first: S1, second: S2) -> Self {
        Self {
            first: Some(first),
            second: Some(second),
        }
    }
}

impl<S1: EnvelopeSource, S2: EnvelopeSource> EnvelopeSource for Merged<S1, S2> {
    async fn next(&mut self) -> Result<Option<Envelope>> {
        loop {
            match (self.first.as_mut(), self.second.as_mut()) {
                (None, None) => return Ok(None),
                // One input has ended; drain the rest of the other.
                (Some(first), None) => return first.next().await,
                (None, Some(second)) => return second.next().await,
                (Some(first), Some(second)) => {
                    // Without `biased`, `select!` picks a ready input at random, so a
                    // busy topic does not starve a quieter one.
                    let (side, result) = tokio::select! {
                        result = first.next() => (Side::First, result),
                        result = second.next() => (Side::Second, result),
                    };
                    if let Some(envelope) = result? {
                        return Ok(Some(envelope));
                    }
                    match side {
                        Side::First => self.first = None,
                        Side::Second => self.second = None,
                    }
                }
            }
        }
    }

    async fn commit(&mut self) -> Result<()> {
        // Only the live inputs have a checkpoint to advance; one that ended has none.
        if let Some(first) = self.first.as_mut() {
            first.commit().await?;
        }
        if let Some(second) = self.second.as_mut() {
            second.commit().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use anyhow::Result;

    use crate::connectors::EnvelopeSource;
    use crate::wire::envelope::{ChainId, Envelope, Event, Finalized};

    use super::Merged;

    /// Yields `count` envelopes with the given chain, then ends.
    struct Counting {
        chain: &'static str,
        left: u64,
    }

    impl EnvelopeSource for Counting {
        async fn next(&mut self) -> Result<Option<Envelope>> {
            if self.left == 0 {
                return Ok(None);
            }
            self.left -= 1;
            Ok(Some(Envelope::new(
                ChainId::new(self.chain),
                self.left,
                Event::Finalized(Finalized {
                    height: self.left,
                    hash: alloy_primitives::B256::ZERO,
                }),
            )))
        }
    }

    /// Both inputs are drained; neither one's records are lost to the other finishing.
    #[tokio::test]
    async fn both_inputs_are_drained() {
        let mut merged = Merged::new(
            Counting {
                chain: "raw",
                left: 3,
            },
            Counting {
                chain: "decoded",
                left: 2,
            },
        );

        let mut seen = Vec::new();
        while let Some(envelope) = merged.next().await.expect("read") {
            seen.push(envelope.chain.as_str().to_owned());
        }
        assert_eq!(seen.iter().filter(|chain| *chain == "raw").count(), 3);
        assert_eq!(seen.iter().filter(|chain| *chain == "decoded").count(), 2);
    }

    /// The merge ends only when both inputs do: one ending early must not cut the other
    /// short.
    #[tokio::test]
    async fn a_finished_input_does_not_truncate_the_other() {
        let mut merged = Merged::new(
            Counting {
                chain: "raw",
                left: 0,
            },
            Counting {
                chain: "decoded",
                left: 2,
            },
        );

        let first = merged.next().await.expect("read").expect("a record");
        assert_eq!(first.chain.as_str(), "decoded");
        let second = merged.next().await.expect("read").expect("a record");
        assert_eq!(second.chain.as_str(), "decoded");
        assert!(merged.next().await.expect("read").is_none(), "both ended");
    }
}
