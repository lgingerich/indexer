//! Typed configuration for each stage.
//!
//! Every stage takes a value built here rather than reading `std::env` for itself. That
//! is what makes a stage testable: a builder can be constructed in a test with no
//! environment at all, and a misconfiguration is a type error or a build error rather
//! than a string looked up at runtime.
//!
//! # Why builders rather than a `from_env` on each stage
//!
//! The stages used to read their own environment, so their configuration was spread
//! across their `main` functions, untyped, and only checkable by running them. A
//! builder collects it into one place and makes the shape explicit — and if a
//! configuration should come from the environment or from flags later, that becomes a
//! single adapter rather than a change in every stage.

/// Where a Kafka-protocol client connects, and which topics it uses.
///
/// Shared by every stage that talks to a broker, because a broker address and topic
/// names are not per-stage concerns: a stage consumes one topic and produces to another,
/// and both ends must name the same cluster.
#[cfg(feature = "kafka")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaConfig {
    /// Comma-separated `host:port` bootstrap servers.
    pub brokers: String,
    /// The consumer group this stage's offsets are committed under.
    ///
    /// An offset is per group, so a stage that reads two topics needs two groups; the
    /// builders take a prefix and derive them.
    pub group: String,
    /// The topic this stage reads.
    pub input_topic: String,
    /// The topic this stage writes, where it has one.
    pub output_topic: Option<String>,
}

#[cfg(feature = "kafka")]
impl KafkaConfig {
    /// Starts a configuration with the broker address.
    #[must_use]
    pub fn builder(brokers: impl Into<String>) -> KafkaConfigBuilder {
        KafkaConfigBuilder::new(brokers)
    }
}

/// Builds a [`KafkaConfig`].
#[cfg(feature = "kafka")]
#[derive(Debug, Clone)]
pub struct KafkaConfigBuilder {
    brokers: String,
    group: Option<String>,
    input_topic: Option<String>,
    output_topic: Option<String>,
}

#[cfg(feature = "kafka")]
impl KafkaConfigBuilder {
    /// Starts a build against `brokers`.
    #[must_use]
    pub fn new(brokers: impl Into<String>) -> Self {
        Self {
            brokers: brokers.into(),
            group: None,
            input_topic: None,
            output_topic: None,
        }
    }

    /// Sets the consumer group.
    #[must_use]
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Sets the topic this stage reads.
    #[must_use]
    pub fn input_topic(mut self, topic: impl Into<String>) -> Self {
        self.input_topic = Some(topic.into());
        self
    }

    /// Sets the topic this stage writes.
    #[must_use]
    pub fn output_topic(mut self, topic: impl Into<String>) -> Self {
        self.output_topic = Some(topic.into());
        self
    }

    /// Finishes the build.
    ///
    /// # Errors
    ///
    /// Returns an error when a required field is unset, naming it. A stage that started
    /// with a missing group id would fail later and less clearly, from inside the
    /// client.
    pub fn build(self) -> anyhow::Result<KafkaConfig> {
        Ok(KafkaConfig {
            brokers: self.brokers,
            group: self
                .group
                .ok_or_else(|| anyhow::anyhow!("kafka group is required"))?,
            input_topic: self
                .input_topic
                .ok_or_else(|| anyhow::anyhow!("kafka input_topic is required"))?,
            output_topic: self.output_topic,
        })
    }
}

/// How a stage batches before flushing.
///
/// Shared by every stage on the bus, because the trade is the same everywhere: a larger
/// batch amortizes the transport's per-request cost, and the time bound is what stops a
/// quiet topic from leaving records unflushed — which matters because an unflushed
/// record is an uncommitted offset, and so a record that will be replayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchConfig {
    /// Records to accumulate before flushing.
    pub records: usize,
    /// How long to wait before flushing a partial batch.
    pub every: std::time::Duration,
}

impl BatchConfig {
    /// A batch of `records` or `every`, whichever comes first.
    #[must_use]
    pub const fn new(records: usize, every: std::time::Duration) -> Self {
        Self { records, every }
    }
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self::new(500, std::time::Duration::from_secs(1))
    }
}
