//! Drops dataset events the deployment did not select.
//!
//! The source still leads every block with a header marker, because the pipeline
//! reads parent linkage from it. This sink removes that marker, and any other
//! dataset row, when it is not one of the selected datasets. Reorg and finality
//! markers always pass.

use serde::de::{self, Unexpected};
use serde::{Deserialize, Deserializer};

use crate::sink::{EnvelopeSink, SinkError};
use crate::wire::envelope::{Envelope, Event};

/// Which on-chain datasets a deployment fetches and stores.
///
/// Absent from the settings file means all four. An empty list is rejected:
/// a run that stores nothing is not a deployment. The flags are independent:
/// any combination is valid, so this is not a state machine.
#[expect(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Datasets {
    /// Store block headers.
    pub block: bool,
    /// Store transactions. The node is asked for full transaction objects.
    pub transaction: bool,
    /// Store receipts. Logs selected alongside receipts are taken from them.
    pub receipt: bool,
    /// Store logs.
    pub log: bool,
}

impl Default for Datasets {
    fn default() -> Self {
        Self::all()
    }
}

impl Datasets {
    /// Every dataset. This is the fetch the source used before selection existed.
    #[must_use]
    pub const fn all() -> Self {
        Self {
            block: true,
            transaction: true,
            receipt: true,
            log: true,
        }
    }

    /// Whether this event is stored.
    ///
    /// A decoded record is kept only when logs are, because it is produced from a log.
    /// Reorg and finality always pass.
    #[must_use]
    pub const fn keeps(self, event: &Event) -> bool {
        match event {
            Event::Block(_) => self.block,
            Event::Transaction(_) => self.transaction,
            Event::Receipt(_) => self.receipt,
            Event::Log(_) | Event::Decoded(_) => self.log,
            Event::Reorg(_) | Event::Finalized(_) => true,
        }
    }
}

impl std::fmt::Display for Datasets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut names = Vec::new();
        if self.block {
            names.push("block");
        }
        if self.transaction {
            names.push("transaction");
        }
        if self.receipt {
            names.push("receipt");
        }
        if self.log {
            names.push("log");
        }
        write!(f, "{}", names.join(","))
    }
}

impl<'de> Deserialize<'de> for Datasets {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let names = Vec::<String>::deserialize(deserializer)?;
        if names.is_empty() {
            return Err(de::Error::invalid_value(
                Unexpected::Seq,
                &"one or more of `block`, `transaction`, `receipt`, `log`",
            ));
        }
        let mut datasets = Self {
            block: false,
            transaction: false,
            receipt: false,
            log: false,
        };
        for name in &names {
            let slot = match name.as_str() {
                "block" => &mut datasets.block,
                "transaction" => &mut datasets.transaction,
                "receipt" => &mut datasets.receipt,
                "log" => &mut datasets.log,
                other => {
                    return Err(de::Error::unknown_variant(
                        other,
                        &["block", "transaction", "receipt", "log"],
                    ));
                }
            };
            if *slot {
                return Err(de::Error::custom(format!("duplicate dataset `{name}`")));
            }
            *slot = true;
        }
        Ok(datasets)
    }
}

/// Forwards only the events [`Datasets`] keeps.
#[derive(Debug)]
pub struct SelectingSink<K> {
    datasets: Datasets,
    inner: K,
}

impl<K> SelectingSink<K> {
    /// Wraps `inner`, dropping dataset events `datasets` does not name.
    #[must_use]
    pub const fn new(datasets: &Datasets, inner: K) -> Self {
        Self {
            datasets: *datasets,
            inner,
        }
    }
}

impl<K: EnvelopeSink> EnvelopeSink for SelectingSink<K> {
    async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
        if self.datasets.keeps(&envelope.event) {
            self.inner.publish(envelope).await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), SinkError> {
        self.inner.flush().await
    }

    fn observe_head(&mut self, height: u64) {
        self.inner.observe_head(height);
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::B256;

    use super::{Datasets, SelectingSink};
    use crate::sink::{EnvelopeSink as _, SinkError};
    use crate::wire::envelope::{ChainId, Envelope, Event, Finalized, Log, Reorg};

    struct Mem(Vec<&'static str>);

    impl crate::sink::EnvelopeSink for Mem {
        async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
            self.0.push(envelope.event.kind());
            Ok(())
        }
    }

    #[tokio::test]
    async fn unselected_datasets_are_dropped_and_control_events_pass() {
        let datasets: Datasets = serde_json::from_str(r#"["log"]"#).expect("datasets");
        let mut sink = SelectingSink::new(&datasets, Mem(Vec::new()));
        let chain = ChainId::new("base");
        for event in [
            Event::Block(Box::default()),
            Event::Log(Box::new(Log {
                block_number: 1,
                block_hash: B256::ZERO,
                ..Log::default()
            })),
            Event::Reorg(Reorg {
                height: 1,
                new_head_hash: B256::ZERO,
                orphaned_hashes: vec![],
            }),
            Event::Finalized(Finalized {
                height: 1,
                hash: B256::ZERO,
            }),
        ] {
            sink.publish(Envelope::new(chain.clone(), event))
                .await
                .expect("publish");
        }
        assert_eq!(sink.inner.0, ["log", "reorg", "finalized"]);
    }
}
