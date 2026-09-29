//! Running the decode stage: raw records in, decoded records and control signals out.
//!
//! The stage is a loop over a source and a sink, both handed in. It builds no clients
//! and names no broker, so it runs over anything that implements the two traits — which
//! is what makes it testable without Kafka, and what lets the transport change without
//! touching the decoder.
//!
//! [`crate::ingest`] works the same way, and for the same reason. The loop itself is
//! [`crate::connectors::run`], shared with the store's own drain so the two cannot
//! drift on the one ordering that matters: publish, then flush, then commit.
//!
//! # Delivery
//!
//! At-least-once. The transform is a pure function of a record and the registry, so a
//! redelivered record decodes to the same output with the same `dedupe_key`, and a
//! consumer that upserts on that key is idempotent.
//!
//! The commit happens **after** the sink's flush, so a crash between the two replays a
//! batch rather than losing one. That ordering is why the stage takes the source by
//! mutable reference rather than owning it: it must advance the checkpoint, but the
//! caller keeps the handle.

use std::time::Duration;

use anyhow::Result;
use tracing::{debug, info, warn};

use crate::config::BatchConfig;
use crate::connectors::{EnvelopeSink, EnvelopeSource};
use crate::decode::Transform;
use crate::decode::registry::ContractRegistry;

/// Builds and runs the decode stage.
#[derive(Debug)]
pub struct Decode {
    batch: BatchConfig,
    drain: Option<Duration>,
    registry: ContractRegistry,
}

impl Decode {
    /// Starts a build.
    #[must_use]
    pub fn builder() -> DecodeBuilder {
        DecodeBuilder::new()
    }

    /// Reads from `source`, decoding each record into `sink` until the input ends.
    ///
    /// # Errors
    ///
    /// Returns an error when a record cannot be read, when a decoded record cannot be
    /// published, or when the checkpoint cannot be advanced. A single record that does
    /// not *decode* is logged and produces no output; the raw log is already on the
    /// input topic, so a corrected ABI recovers it.
    pub async fn run<S, K>(self, source: &mut S, sink: &mut K) -> Result<()>
    where
        S: EnvelopeSource,
        K: EnvelopeSink,
    {
        let contracts = self.registry.len();
        // The registry is owned here, not inside the transform: discovery mutates it
        // between records, and the transform reads the current snapshot per call.
        let mut registry = self.registry;

        info!(
            contracts,
            batch_records = self.batch.records,
            "decode started"
        );

        // Each registered log expands to its decoded record; everything else — raw
        // datasets, unregistered logs — is dropped, since the raw topic already carries
        // it. A log that fails to decode is reported but produces no record.
        //
        // Registration happens inline, in the same sequential pass: a factory's creating
        // log always precedes its child's own logs, so a child is registered before it
        // emits anything. No lookahead, no network — the child's ABI is already loaded.
        crate::connectors::run(source, sink, self.batch, self.drain, |envelope, out| {
            let chain = envelope.chain.clone();
            let applied = Transform::apply(&registry, envelope);
            if let Some(error) = applied.error {
                // A log that matched an ABI but did not decode usually means the ABI
                // is the wrong version for this height. It must not stall the stream,
                // and it must not be silent either — the raw log is already upstream,
                // so a corrected ABI recovers it.
                warn!(%error, "a log did not decode and produced no record");
            }
            if let Some(discovery) = applied.discovery {
                debug!(child = %discovery.child, protocol = %discovery.protocol, "registered a discovered contract");
                registry.register_discovered(&chain, discovery);
            }
            out.extend(applied.output);
        })
        .await?;
        Ok(())
    }
}

/// Builds a [`Decode`].
#[derive(Debug)]
pub struct DecodeBuilder {
    batch: BatchConfig,
    drain: Option<Duration>,
    registry: ContractRegistry,
}

impl DecodeBuilder {
    /// Starts a build.
    #[must_use]
    pub fn new() -> Self {
        Self {
            batch: BatchConfig::default(),
            drain: None,
            registry: ContractRegistry::default(),
        }
    }

    /// Sets how many records to consume before flushing and committing.
    #[must_use]
    pub const fn batch(mut self, batch: BatchConfig) -> Self {
        self.batch = batch;
        self
    }

    /// Stops once the input has been idle for `drain`.
    ///
    /// For a bounded run — a backfill, a one-shot drain — not for a live stream, where a
    /// quiet input is normal and stopping is a fault. Without it a bounded run has no way
    /// to finish, which is why the store's drain takes the same bound.
    #[must_use]
    pub const fn drain(mut self, drain: Duration) -> Self {
        self.drain = Some(drain);
        self
    }

    /// Sets the contract registry this stage decodes with.
    #[must_use]
    pub fn registry(mut self, registry: ContractRegistry) -> Self {
        self.registry = registry;
        self
    }

    /// Finishes the build.
    ///
    /// # Errors
    ///
    /// Never fails. It returns a `Result` so a required setting can be reported the way
    /// the other builders report theirs, rather than changing every call site then.
    pub fn build(self) -> Result<Decode> {
        if self.registry.is_empty() {
            warn!("no contracts registered; every log will be dropped undecoded");
        }
        Ok(Decode {
            batch: self.batch,
            drain: self.drain,
            registry: self.registry,
        })
    }
}

impl Default for DecodeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::sync::{Arc, Mutex};

    use alloy_primitives::{Address, B256, TxHash};

    use crate::connectors::{EnvelopeSink, EnvelopeSource};
    use crate::wire::envelope::{ChainId, Envelope, Event, Finalized, Log};

    use super::Decode;

    /// The order the stage drove its source and sink, shared between the two fakes.
    ///
    /// The loop's one load-bearing invariant is the order — publish, then flush, then
    /// commit — so a test has to observe it rather than trust the doc. A shared journal
    /// is the only way two separately-borrowed trait objects can report their relative
    /// timing.
    #[derive(Clone, Default)]
    struct Order(Arc<Mutex<Vec<&'static str>>>);

    impl Order {
        fn push(&self, step: &'static str) {
            self.0.lock().expect("lock").push(step);
        }

        fn steps(&self) -> Vec<&'static str> {
            self.0.lock().expect("lock").clone()
        }
    }

    /// A source over a fixed list, which is the point of the split: the stage runs
    /// without a broker, so its wiring is testable.
    #[derive(Default)]
    struct FakeSource {
        queued: Vec<Envelope>,
        commits: usize,
        order: Order,
    }

    impl EnvelopeSource for FakeSource {
        async fn next(&mut self) -> anyhow::Result<Option<Envelope>> {
            Ok(self.queued.pop())
        }

        async fn commit(&mut self) -> anyhow::Result<()> {
            self.commits += 1;
            self.order.push("commit");
            Ok(())
        }
    }

    /// Collects what the stage published, and counts flushes.
    #[derive(Default)]
    struct CollectSink {
        seen: Mutex<Vec<Envelope>>,
        flushes: Mutex<usize>,
        order: Order,
    }

    impl EnvelopeSink for CollectSink {
        async fn publish(&mut self, envelope: &Envelope) -> anyhow::Result<()> {
            self.seen.lock().expect("lock").push(envelope.clone());
            self.order.push("publish");
            Ok(())
        }

        async fn flush(&mut self) -> anyhow::Result<()> {
            *self.flushes.lock().expect("lock") += 1;
            self.order.push("flush");
            Ok(())
        }
    }

    /// A log no registry entry covers: it has no decoded form, so the stage drops it.
    fn unregistered_log(sequence: u64) -> Envelope {
        Envelope::new(
            ChainId::new("base"),
            sequence,
            Event::Log(Box::new(Log {
                log_index: sequence,
                transaction_hash: TxHash::from([0x11; 32]),
                address: Address::from([0xaa; 20]),
                block_number: 100 + sequence,
                block_hash: B256::from([0x02; 32]),
                block_timestamp: 1_700_000_000,
                ..Log::default()
            })),
        )
    }

    /// A control signal: it must be forwarded, because a store retracts and compacts on
    /// it.
    fn finalized(sequence: u64) -> Envelope {
        Envelope::new(
            ChainId::new("base"),
            sequence,
            Event::Finalized(Finalized {
                height: sequence,
                hash: B256::from([0x03; 32]),
            }),
        )
    }

    /// A 32-byte big-endian word holding `value`, the form an ABI integer takes.
    fn word(value: u64) -> B256 {
        let mut word = [0u8; 32];
        word[24..].copy_from_slice(&value.to_be_bytes());
        B256::from(word)
    }

    /// A 32-byte word holding an address in its low 20 bytes.
    fn address_word(address: Address) -> B256 {
        let mut word = [0u8; 32];
        word[12..].copy_from_slice(address.as_slice());
        B256::from(word)
    }

    /// A real Uniswap V3 `PoolCreated` log: `token0`, `token1`, and `fee` are indexed,
    /// and the pool address is the *second word of `data`*, not a topic. The selector is
    /// derived from the factory ABI rather than pinned, so a signature typo fails here.
    fn pool_created_log(factory: &str, signature: &str) -> Envelope {
        const POOL: &str = "0xd0b53D9277642d899DF5C87A3966A349A798F224";
        let selector =
            crate::decode::abi::Abi::from_json(include_str!("../../abis/uniswap_v3_factory.json"))
                .expect("factory ABI")
                .selector(signature)
                .expect("declares PoolCreated");

        let child: Address = POOL.parse().expect("a pool address");
        let mut data = Vec::new();
        data.extend_from_slice(word(60).as_slice()); // tickSpacing: int24
        data.extend_from_slice(address_word(child).as_slice()); // pool: address

        Envelope::new(
            ChainId::new("base"),
            0,
            Event::Log(Box::new(Log {
                log_index: 0,
                transaction_hash: TxHash::from([0x01; 32]),
                address: factory.parse().expect("a factory address"),
                topic0: Some(selector),
                topic1: Some(address_word(Address::from([0x11; 20]))),
                topic2: Some(address_word(Address::from([0x22; 20]))),
                topic3: Some(word(3_000)),
                data: data.into(),
                block_number: 100,
                block_hash: B256::from([0x02; 32]),
                block_timestamp: 1_700_000_000,
                ..Log::default()
            })),
        )
    }

    /// The stage runs over anything implementing the traits, with no broker and no
    /// client construction, and publishes only what belongs on the decoded topic: a
    /// control signal survives, an unregistered raw log is dropped.
    #[tokio::test]
    async fn the_stage_forwards_control_signals_and_drops_raw_logs() {
        let mut source = FakeSource {
            queued: vec![unregistered_log(2), finalized(1)],
            commits: 0,
            ..Default::default()
        };
        let mut sink = CollectSink::default();

        Decode::builder()
            .registry(crate::decode::registry::ContractRegistry::default())
            .build()
            .expect("builds")
            .run(&mut source, &mut sink)
            .await
            .expect("the stage runs");

        let seen = sink.seen.lock().expect("lock");
        assert_eq!(
            seen.len(),
            1,
            "only the control signal belongs on the decoded topic"
        );
        assert!(matches!(
            seen.first().map(|e| &e.event),
            Some(Event::Finalized(_))
        ));
    }

    /// The loop's one ordering: everything published is flushed durable, and only then
    /// is the input checkpoint advanced. A commit before the flush would lose a batch on
    /// a crash between the two, so the journal is asserted and not just counted.
    #[tokio::test]
    async fn the_checkpoint_advances_after_the_sink_is_flushed() {
        let order = Order::default();
        let mut source = FakeSource {
            queued: vec![finalized(1)],
            commits: 0,
            order: order.clone(),
        };
        let mut sink = CollectSink {
            order: order.clone(),
            ..Default::default()
        };

        Decode::builder()
            .registry(crate::decode::registry::ContractRegistry::default())
            .build()
            .expect("builds")
            .run(&mut source, &mut sink)
            .await
            .expect("the stage runs");

        let steps = order.steps();
        assert!(source.commits > 0, "the checkpoint advanced");
        assert!(*sink.flushes.lock().expect("lock") > 0);
        assert_eq!(
            steps,
            ["publish", "flush", "commit"],
            "the flush must precede the commit"
        );
        // The same order holds at the end of the run, when the stage flushes and
        // commits whatever the last deadline left pending.
        let publish = steps.iter().rposition(|step| *step == "publish");
        let last_flush = steps.iter().rposition(|step| *step == "flush");
        let last_commit = steps.iter().rposition(|step| *step == "commit");
        assert!(
            publish < last_flush && last_flush < last_commit,
            "{steps:?}"
        );
    }

    /// An empty source ends cleanly rather than looping, which is what makes the stage
    /// terminate on a drained input.
    #[tokio::test]
    async fn an_empty_source_ends_the_run() {
        let mut source = FakeSource::default();
        let mut sink = CollectSink::default();
        Decode::builder()
            .registry(crate::decode::registry::ContractRegistry::default())
            .build()
            .expect("builds")
            .run(&mut source, &mut sink)
            .await
            .expect("an empty run is not an error");
        assert!(sink.seen.lock().expect("lock").is_empty());
    }

    /// The runtime's discovery promise: a factory's creation event registers its child
    /// *before* the child's own logs are read, so the child decodes in the same pass.
    ///
    /// This is the branch `run` owns and the transform cannot: it mutates the registry
    /// between records. The registry below does **not** list the pool — it only knows the
    /// factory — so the swap on the second record can only decode because the first
    /// record's discovery was applied. If `run` dropped that call, only the factory log
    /// would publish.
    ///
    /// The registration is scoped to this run: the registry is moved into the stage and
    /// not handed back, so a restart re-discovers from the same factory logs.
    #[tokio::test]
    async fn the_stage_applies_discovery_between_records() {
        const SIGNATURE: &str = "PoolCreated(address,address,uint24,int24,address)";
        const FACTORY: &str = "0x1F98431c8aD98523631AE4a59f267346ea31F984";
        const POOL: &str = "0xd0b53D9277642d899DF5C87A3966A349A798F224";
        let abi_dir = std::path::PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/abis"));

        let registry = crate::decode::registry::ContractRegistry::load(
            &[
                crate::decode::registry::AbiEntry {
                    name: "uniswap_v3_factory".to_owned(),
                    path: "uniswap_v3_factory.json".into(),
                },
                crate::decode::registry::AbiEntry {
                    name: "uniswap_v3_pool".to_owned(),
                    path: "uniswap_v3_pool.json".into(),
                },
            ],
            &[crate::decode::registry::ContractEntry {
                chain: "base".to_owned(),
                address: FACTORY.to_owned(),
                abi: "uniswap_v3_factory".to_owned(),
            }],
            &[crate::decode::registry::DiscoveryEntry {
                chain: "base".to_owned(),
                address: FACTORY.to_owned(),
                event: SIGNATURE.to_owned(),
                child: "pool".to_owned(),
                abi: "uniswap_v3_pool".to_owned(),
            }],
            abi_dir,
        )
        .expect("the registry loads");

        // `queued` pops from the back, so the factory's creating log is read first and
        // the pool's swap second — the order the chain emits them.
        let swap: Envelope = serde_json::from_str(
            include_str!("../../examples/fixtures/uniswap_v3_swaps.ndjson")
                .lines()
                .next()
                .expect("a fixture line"),
        )
        .expect("the fixture is a published envelope");
        let mut source = FakeSource {
            queued: vec![swap, pool_created_log(FACTORY, SIGNATURE)],
            ..Default::default()
        };
        let mut sink = CollectSink::default();

        Decode::builder()
            .registry(registry)
            .build()
            .expect("builds")
            .run(&mut source, &mut sink)
            .await
            .expect("the stage runs");

        let seen = sink.seen.lock().expect("lock");
        assert_eq!(
            seen.len(),
            2,
            "the factory log and the swap both decoded; the swap needs the registration"
        );
        let kinds: Vec<&str> = seen.iter().map(Envelope::kind).collect();
        assert_eq!(kinds, ["decoded", "decoded"]);
        // The first record is the factory's own event, the second the newly known pool's.
        let Event::Decoded(created) = &seen[0].event else {
            panic!("the first record must be the decoded factory event");
        };
        assert_eq!(created.name, "PoolCreated");
        let Event::Decoded(swap) = &seen[1].event else {
            panic!("the second record must be the decoded swap");
        };
        assert_eq!(swap.name, "Swap");
        assert_eq!(swap.protocol, "uniswap_v3_pool");
        assert_eq!(
            swap.address,
            POOL.parse::<Address>().expect("the pool address")
        );
    }
}
