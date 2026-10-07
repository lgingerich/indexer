//! Live decode stays inline before the bounded storage channel.

use tracing::warn;

use crate::decode::{ContractRegistry, DecodeError, Decoder};
use crate::sink::{EnvelopeSink, SinkError};
use crate::wire::envelope::{Envelope, Event};

/// Forwards every raw record and adds matching decoded logs.
///
/// Registrations are immutable. Reorg markers are forwarded unchanged;
/// this stage does not claim that append-only stored records are canonical.
pub struct DecodingSink<K> {
    decoder: Decoder,
    inner: K,
}

impl<K> std::fmt::Debug for DecodingSink<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodingSink")
            .field("registrations", &self.decoder.registrations())
            .finish_non_exhaustive()
    }
}

impl<K: EnvelopeSink> DecodingSink<K> {
    /// Creates the live stage using the same decoder as historical replay.
    #[must_use]
    pub fn new(registry: ContractRegistry, inner: K) -> Self {
        if registry.is_empty() {
            warn!("no contracts registered; no log will decode");
        }
        Self {
            decoder: Decoder::new(registry),
            inner,
        }
    }
}

impl<K: EnvelopeSink> EnvelopeSink for DecodingSink<K> {
    async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
        let decoded = if let Event::Log(log) = &envelope.event {
            match self.decoder.decode(&envelope.chain, log) {
                Ok(decoded) => decoded,
                Err(error @ DecodeError::Shape) => return Err(SinkError::Decode(error)),
                Err(error) => {
                    warn!(chain = %envelope.chain, address = %log.address,
                        block = log.block_number, block_hash = %log.block_hash,
                        transaction = %log.transaction_hash, log_index = log.log_index,
                        abi_id = ?self.decoder.abi_id(&envelope.chain, log), selector = ?log.topic0,
                        error = ?error, "log decode failed; preserving raw record");
                    None
                }
            }
        } else {
            None
        };
        let output = decoded.map(|decoded| {
            Envelope::new(envelope.chain.clone(), Event::Decoded(Box::new(decoded)))
        });
        self.inner.publish(envelope).await?;
        if let Some(output) = output {
            self.inner.publish(output).await?;
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
    use super::*;
    use crate::wire::envelope::{ChainId, Log};

    #[derive(Default)]
    struct Collect {
        records: Vec<Envelope>,
        flushes: usize,
    }
    impl EnvelopeSink for Collect {
        async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
            self.records.push(envelope);
            Ok(())
        }
        async fn flush(&mut self) -> Result<(), SinkError> {
            self.flushes += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn misses_and_control_markers_pass_through_once() {
        let mut sink = DecodingSink::new(ContractRegistry::default(), Collect::default());
        for event in [
            Event::Log(Box::default()),
            Event::Reorg(crate::wire::envelope::Reorg {
                height: 1,
                new_head_hash: alloy_primitives::B256::ZERO,
                orphaned_hashes: vec![],
            }),
        ] {
            sink.publish(Envelope::new(ChainId::new("base"), event))
                .await
                .expect("publish");
        }
        sink.flush().await.expect("flush");
        assert_eq!(sink.inner.records.len(), 2);
        assert_eq!(sink.inner.flushes, 1);
    }

    #[tokio::test]
    async fn matching_logs_add_records_and_bad_logs_keep_raw() {
        let registry = ContractRegistry::load(
            &crate::decode::RegistryConfig {
                abi: vec![crate::decode::AbiEntry {
                    name: "pool".into(),
                    path: "abis/uniswap_v3_pool.json".into(),
                }],
                contract: vec![crate::decode::ContractEntry {
                    chain: "base".into(),
                    address: "0xd0b53D9277642d899DF5C87A3966A349A798F224".into(),
                    abi: "pool".into(),
                    protocol: "uniswap_v3".into(),
                    from_block: 0,
                    to_block: None,
                }],
            },
            env!("CARGO_MANIFEST_DIR"),
        )
        .expect("registry");
        let mut source: Envelope = serde_json::from_str(
            include_str!("../../examples/fixtures/uniswap_v3_swaps.ndjson")
                .lines()
                .next()
                .expect("line"),
        )
        .expect("fixture");
        let Event::Log(log) = &mut source.event else {
            panic!("log");
        };
        log.address = "0xd0b53D9277642d899DF5C87A3966A349A798F224"
            .parse()
            .expect("address");
        let mut bad = source.clone();
        let Event::Log(log) = &mut bad.event else {
            panic!("log");
        };
        log.data = alloy_primitives::Bytes::new();
        let mut sink = DecodingSink::new(registry, Collect::default());
        sink.publish(source).await.expect("publish");
        sink.publish(bad).await.expect("bad log is nonfatal");
        assert_eq!(
            sink.inner
                .records
                .iter()
                .map(Envelope::kind)
                .collect::<Vec<_>>(),
            ["log", "decoded", "log"]
        );
        let Event::Log(log) = &sink.inner.records[2].event else {
            panic!("raw log");
        };
        assert_eq!(log.data, Log::default().data);
    }
}
