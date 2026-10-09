//! Live decode stays inline before the bounded storage channel.

use tracing::{info, warn};

use crate::decode::{DecodeError, Decoder};
use crate::sink::{EnvelopeSink, SinkError};
use crate::wire::envelope::{ChainId, Envelope, Event, Log};

/// Forwards every raw record and adds decoded logs and discovered contracts.
///
/// After a log it can decode, it publishes the decoded record and then any contract the
/// log created. A reorg marker is forwarded unchanged and retracts the contracts created
/// in the orphaned blocks.
pub struct DecodingSink<K> {
    decoder: Decoder,
    inner: K,
}

impl<K> std::fmt::Debug for DecodingSink<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodingSink")
            .field("contracts", &self.decoder.contracts())
            .finish_non_exhaustive()
    }
}

impl<K: EnvelopeSink> DecodingSink<K> {
    /// Creates the decode stage in front of `inner`.
    #[must_use]
    pub fn new(decoder: Decoder, inner: K) -> Self {
        if decoder.contracts() == 0 {
            warn!("no contracts registered; no log will decode");
        }
        Self { decoder, inner }
    }

    /// Decodes one log into the records to publish after it.
    ///
    /// Ordinary decode failures are logged and skipped, leaving the raw log; an internal
    /// invariant failure stops the run.
    fn decode(&mut self, chain: &ChainId, log: &Log) -> Result<Vec<Event>, SinkError> {
        match self.decoder.decode(log) {
            Ok(Some(decoding)) => {
                let mut events = Vec::with_capacity(1 + decoding.discovered.len());
                events.push(Event::Decoded(Box::new(decoding.decoded)));
                events.extend(
                    decoding
                        .discovered
                        .into_iter()
                        .map(|contract| Event::Contract(Box::new(contract))),
                );
                Ok(events)
            }
            Ok(None) => Ok(Vec::new()),
            Err(error @ DecodeError::Shape) => Err(SinkError::Decode(Box::new(error))),
            Err(error) => {
                warn!(%chain, address = %log.address,
                    block = log.block_number, block_hash = %log.block_hash,
                    transaction = %log.transaction_hash, log_index = log.log_index,
                    event_id = ?self.decoder.event_id(log), selector = ?log.topic0,
                    error = ?error, "log decode failed; preserving raw record");
                Ok(Vec::new())
            }
        }
    }
}

impl<K: EnvelopeSink> EnvelopeSink for DecodingSink<K> {
    async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
        let added = match &envelope.event {
            Event::Log(log) => self.decode(&envelope.chain, log)?,
            Event::Reorg(reorg) => {
                let retracted = self.decoder.retract(&reorg.orphaned_hashes);
                if retracted > 0 {
                    info!(chain = %envelope.chain, retracted,
                        "retracted contracts created in orphaned blocks");
                }
                Vec::new()
            }
            _ => Vec::new(),
        };
        let chain = envelope.chain.clone();
        self.inner.publish(envelope).await?;
        for event in added {
            self.inner
                .publish(Envelope::new(chain.clone(), event))
                .await?;
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
    use std::path::Path;

    use alloy_dyn_abi::DynSolValue;
    use alloy_primitives::{Address, B256, keccak256};

    use super::*;
    use crate::decode::Catalog;
    use crate::wire::envelope::Reorg;

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

    fn chain() -> ChainId {
        ChainId::new("base")
    }

    fn sink() -> DecodingSink<Collect> {
        let catalog = Catalog::load(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("protocols"),
            &chain(),
        )
        .expect("catalog");
        DecodingSink::new(Decoder::new(catalog), Collect::default())
    }

    fn pool_created(pool: Address, block: B256) -> Log {
        Log {
            address: "0x33128a8fC17869897dcE68Ed026d694621f6FDfD"
                .parse()
                .expect("factory"),
            topic0: Some(keccak256(
                "PoolCreated(address,address,uint24,int24,address)",
            )),
            topic1: Some(B256::with_last_byte(1)),
            topic2: Some(B256::with_last_byte(2)),
            topic3: Some(B256::with_last_byte(3)),
            data: DynSolValue::Tuple(vec![
                DynSolValue::Int(alloy_primitives::I256::try_from(60).expect("int"), 24),
                DynSolValue::Address(pool),
            ])
            .abi_encode_params()
            .into(),
            block_hash: block,
            ..Log::default()
        }
    }

    fn swap(pool: Address) -> Log {
        let source: Envelope = serde_json::from_str(
            include_str!("../../examples/fixtures/uniswap_v3_swaps.ndjson")
                .lines()
                .next()
                .expect("line"),
        )
        .expect("fixture");
        let Event::Log(mut log) = source.event else {
            panic!("log");
        };
        log.address = pool;
        *log
    }

    fn log(log: Log) -> Envelope {
        Envelope::new(chain(), Event::Log(Box::new(log)))
    }

    fn reorg(orphaned: B256) -> Envelope {
        Envelope::new(
            chain(),
            Event::Reorg(Reorg {
                height: 10,
                new_head_hash: B256::ZERO,
                orphaned_hashes: vec![orphaned],
            }),
        )
    }

    fn kinds(sink: &DecodingSink<Collect>) -> Vec<&'static str> {
        sink.inner.records.iter().map(Envelope::kind).collect()
    }

    #[tokio::test]
    async fn misses_and_control_markers_pass_through_once() {
        let mut sink = sink();
        sink.publish(log(Log::default())).await.expect("publish");
        sink.publish(reorg(B256::ZERO)).await.expect("publish");
        sink.flush().await.expect("flush");
        assert_eq!(kinds(&sink), ["log", "reorg"]);
        assert_eq!(sink.inner.flushes, 1);
    }

    /// A creation log is followed by its decoded record and the contract; the child's
    /// swap decodes at once; a malformed swap keeps only its raw log; and once a reorg
    /// orphans the creating block, the child stops decoding.
    #[tokio::test]
    async fn a_creation_publishes_the_contract_and_a_reorg_retracts_it() {
        let pool = Address::from([0xd0; 20]);
        let block = B256::with_last_byte(9);
        let mut sink = sink();
        sink.publish(log(pool_created(pool, block)))
            .await
            .expect("creation");
        sink.publish(log(swap(pool))).await.expect("swap");
        let mut bad = swap(pool);
        bad.data = alloy_primitives::Bytes::new();
        sink.publish(log(bad)).await.expect("a bad log is nonfatal");
        assert_eq!(
            kinds(&sink),
            ["log", "decoded", "contract", "log", "decoded", "log"]
        );
        let Event::Contract(contract) = &sink.inner.records[2].event else {
            panic!("contract");
        };
        assert_eq!((contract.address, contract.block_hash), (pool, block));

        sink.publish(reorg(block)).await.expect("reorg");
        sink.publish(log(swap(pool))).await.expect("swap");
        assert_eq!(kinds(&sink)[6..], ["reorg", "log"]);
    }
}
