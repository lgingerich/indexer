//! Running decode: a sink that decodes on the way through.
//!
//! [`DecodingSink`] wraps another [`EnvelopeSink`]. Every envelope is forwarded as it
//! arrived, and a log that decodes is followed by its decoded record. There is no queue
//! and no task of its own: ingest calls `publish`, the decode happens in that call, and
//! the inner sink is called next. Decoding a block is far cheaper than the block time,
//! so it needs no decoupling from ingest; the one hop that does need it — a slow store —
//! is the channel behind this sink. See `crate::sink::channel`.
//!
//! # Delivery
//!
//! The transform is a pure function of a record and the registry, so decoding the same
//! log again produces the same record with the same `dedupe_key`, and a reader that
//! deduplicates on that key is idempotent. A log that does not decode is reported and
//! produces no record; the raw log is stored regardless, so a corrected ABI recovers it
//! by re-decoding rather than re-fetching.

use anyhow::Result;
use tracing::{debug, warn};

use crate::decode::Transform;
use crate::decode::registry::ContractRegistry;
use crate::sink::EnvelopeSink;
use crate::wire::envelope::Envelope;

/// An [`EnvelopeSink`] that decodes each envelope and forwards both to `inner`.
#[derive(Debug)]
pub struct DecodingSink<K> {
    /// Owned here, not inside the transform: discovery mutates it between records, and
    /// the transform reads the current snapshot per call.
    registry: ContractRegistry,
    inner: K,
}

impl<K: EnvelopeSink> DecodingSink<K> {
    /// Decodes with `registry`, forwarding to `inner`.
    ///
    /// An empty registry is allowed and decodes nothing, which is said here rather than
    /// left to look like a quiet chain.
    #[must_use]
    pub fn new(registry: ContractRegistry, inner: K) -> Self {
        if registry.is_empty() {
            warn!("no contracts registered; no log will decode");
        }
        Self { registry, inner }
    }
}

impl<K: EnvelopeSink> EnvelopeSink for DecodingSink<K> {
    /// Forwards `envelope`, then its decoded record if it has one.
    ///
    /// # Errors
    ///
    /// Returns an error when the inner sink rejects an envelope. A log that fails to
    /// *decode* is not one: it is logged and produces no record.
    async fn publish(&mut self, envelope: Envelope) -> Result<()> {
        let applied = Transform::apply(&self.registry, &envelope);
        if let Some(error) = applied.error {
            // A log that matched an ABI but did not decode usually means the ABI is the
            // wrong version for this height. It must not stall the stream, and it must
            // not be silent either.
            warn!(%error, "a log did not decode and produced no record");
        }
        // Registration happens inline, before the next record: a factory's creating log
        // always precedes its child's own logs, so a child is registered before it emits
        // anything. No lookahead, no network — the child's ABI is already loaded.
        if let Some(discovery) = applied.discovery {
            debug!(child = %discovery.child, protocol = %discovery.protocol, "registered a discovered contract");
            self.registry
                .register_discovered(&envelope.chain, discovery);
        }
        self.inner.publish(envelope).await?;
        if let Some(decoded) = applied.output {
            self.inner.publish(decoded).await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.inner.flush().await
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{Address, B256, TxHash};

    use crate::sink::EnvelopeSink;
    use crate::wire::envelope::{ChainId, Envelope, Event, Finalized, Log};

    use super::DecodingSink;

    /// Collects what reached the inner sink, in order, and counts flushes.
    #[derive(Default)]
    struct CollectSink {
        seen: Vec<Envelope>,
        flushes: usize,
    }

    impl EnvelopeSink for CollectSink {
        async fn publish(&mut self, envelope: Envelope) -> anyhow::Result<()> {
            self.seen.push(envelope);
            Ok(())
        }

        async fn flush(&mut self) -> anyhow::Result<()> {
            self.flushes += 1;
            Ok(())
        }
    }

    fn empty_registry() -> crate::decode::registry::ContractRegistry {
        crate::decode::registry::ContractRegistry::default()
    }

    /// A log no registry entry covers: it has no decoded form.
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

    /// A control marker: it is already in the stream, so it must be forwarded once.
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

    /// Everything published reaches the inner sink exactly once, in order: an
    /// unregistered log adds no record, and a control marker is not duplicated into a
    /// second copy.
    #[tokio::test]
    async fn envelopes_are_forwarded_once_and_unregistered_logs_add_nothing() {
        let mut sink = DecodingSink::new(empty_registry(), CollectSink::default());

        sink.publish(unregistered_log(2)).await.expect("publish");
        sink.publish(finalized(3)).await.expect("publish");

        let kinds: Vec<&str> = sink.inner.seen.iter().map(Envelope::kind).collect();
        assert_eq!(kinds, ["log", "finalized"]);
    }

    /// The flush is the block boundary and must reach the sink that batches on it.
    #[tokio::test]
    async fn flush_reaches_the_inner_sink() {
        let mut sink = DecodingSink::new(empty_registry(), CollectSink::default());
        sink.flush().await.expect("flush");
        assert_eq!(sink.inner.flushes, 1);
    }

    /// The discovery promise: a factory's creation event registers its child *before*
    /// the child's own logs are read, so the child decodes in the same pass, and each
    /// decoded record follows the raw log it came from.
    ///
    /// The registry below does **not** list the pool — it only knows the factory — so
    /// the swap can only decode because the first log's discovery was applied. If the
    /// sink dropped that call, the swap would forward as a bare log.
    ///
    /// The registration is scoped to the sink: the registry is moved in and not handed
    /// back, so a restart re-discovers from the same factory logs.
    #[tokio::test]
    async fn discovery_is_applied_between_records() {
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

        // The factory's creating log is published first and the pool's swap second —
        // the order the chain emits them.
        let swap: Envelope = serde_json::from_str(
            include_str!("../../examples/fixtures/uniswap_v3_swaps.ndjson")
                .lines()
                .next()
                .expect("a fixture line"),
        )
        .expect("the fixture is a published envelope");
        let mut sink = DecodingSink::new(registry, CollectSink::default());
        sink.publish(pool_created_log(FACTORY, SIGNATURE))
            .await
            .expect("publish");
        sink.publish(swap).await.expect("publish");

        let seen = &sink.inner.seen;
        let kinds: Vec<&str> = seen.iter().map(Envelope::kind).collect();
        assert_eq!(kinds, ["log", "decoded", "log", "decoded"]);
        let Event::Decoded(created) = &seen[1].event else {
            panic!("the factory log's record must follow it");
        };
        assert_eq!(created.name, "PoolCreated");
        let Event::Decoded(swap) = &seen[3].event else {
            panic!("the swap's record must follow it");
        };
        assert_eq!(swap.name, "Swap");
        assert_eq!(swap.protocol, "uniswap_v3_pool");
        assert_eq!(
            swap.address,
            POOL.parse::<Address>().expect("the pool address")
        );
    }
}
