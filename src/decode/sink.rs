//! Live decode stays inline before the bounded storage channel.

use tracing::{info, warn};

use super::derive::{Derivations, Sent};
use super::{Claims, Decoder, Record};
use crate::metrics;
use crate::sink::{EnvelopeSink, SinkError};
use crate::wire::datasets::evm::Log;
use crate::wire::envelope::{ChainId, Envelope, Event};

/// Forwards every raw record and adds decoded logs, their derived rows, and discovered
/// contracts.
///
/// After a log it can decode, it publishes the decoded record, then the rows its
/// derivations compute from it, then any contract the log created. A reorg marker is
/// forwarded unchanged and retracts the contracts created in the orphaned blocks.
///
/// The contract set changes only once the inner sink has taken what changes it: a
/// discovered contract joins it once the log and everything after it are published, and
/// a reorg retracts once its marker is. So when the inner sink fails, the set is as it
/// was before the envelope, and the same envelope published again publishes the same
/// records, its contracts among them.
///
/// A derived row carries its transaction's sender and recipient. Ingest publishes each
/// transaction just before its logs, so the sink remembers the last one it forwarded in
/// the current block; a log whose transaction was not fetched derives rows without them.
pub struct DecodingSink<K> {
    decoder: Decoder,
    /// The derivations the catalog bound, run on every record decoded.
    derivations: Derivations,
    /// The last transaction forwarded in the current block, `None` before its first. A
    /// block's batch ends with its accepted-block marker, and a reorg marker starts
    /// another branch, so either clears it.
    transaction: Option<Sent>,
    /// Logs decoded since the last flush, recorded once a block rather than per log, so
    /// the ingest task pays no metric call per envelope.
    decoded: u64,
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
        Self {
            derivations: decoder.catalog().derivations.clone(),
            decoder,
            transaction: None,
            decoded: 0,
            inner,
        }
    }

    /// Decodes one log into the records to publish after it, and the claims on the
    /// contracts among them, to admit once they are published.
    ///
    /// Ordinary decode failures are logged and skipped, leaving the raw log and any
    /// contract the log still discovered; an internal invariant failure stops the run.
    fn decode(
        &mut self,
        chain: &ChainId,
        log: &Log,
    ) -> Result<(Vec<Event>, Option<Claims>), SinkError> {
        let Some((decoded, claims)) = self.decoder.decode(log) else {
            return Ok((Vec::new(), None));
        };
        let mut events = Vec::with_capacity(2 + claims.contracts().len());
        match decoded {
            Ok(Record { decoded, event }) => {
                self.decoded += 1;
                let sent = self
                    .transaction
                    .filter(|sent| sent.hash == log.transaction_hash);
                let derived = self
                    .derivations
                    .rows(chain, event, &decoded, sent)
                    .map_err(|error| SinkError::Decode(Box::new(error)))?;
                events.push(Event::Decoded(Box::new(decoded)));
                events.extend(derived.into_iter().map(|row| Event::Derived(Box::new(row))));
            }
            Err(error) if error.is_invariant() => {
                return Err(SinkError::Decode(Box::new(error)));
            }
            Err(error) => {
                metrics::decode_failed();
                warn!(%chain, address = %log.address,
                    block = log.block_number, block_hash = %log.block_hash,
                    transaction = %log.transaction_hash, log_index = log.log_index,
                    event_id = ?self.decoder.event_id(log), selector = ?log.topic0,
                    error = ?error, "log decode failed; preserving raw record");
            }
        }
        events.extend(
            claims
                .contracts()
                .map(|contract| Event::Contract(Box::new(contract.clone()))),
        );
        Ok((events, Some(claims)))
    }
}

impl<K: EnvelopeSink> EnvelopeSink for DecodingSink<K> {
    async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
        let (added, claims) = match &envelope.event {
            Event::Transaction(transaction) => {
                self.transaction = Some(Sent {
                    hash: transaction.hash,
                    from: transaction.from,
                    to: transaction.to,
                });
                (Vec::new(), None)
            }
            Event::Log(log) => self.decode(&envelope.chain, log)?,
            Event::AcceptedBlock(_) | Event::Reorg(_) => {
                self.transaction = None;
                (Vec::new(), None)
            }
            _ => (Vec::new(), None),
        };
        let chain = envelope.chain.clone();
        let orphaned = match &envelope.event {
            Event::Reorg(reorg) => Some(reorg.orphaned_hashes.clone()),
            _ => None,
        };
        self.inner.publish(envelope).await?;
        for event in added {
            self.inner
                .publish(Envelope::new(chain.clone(), event))
                .await?;
        }
        if let Some(claims) = claims {
            let admitted = self.decoder.admit(claims);
            if admitted > 0 {
                metrics::contracts_discovered(admitted);
            }
        }
        if let Some(orphaned) = orphaned {
            let retracted = self.decoder.retract(&orphaned);
            if retracted > 0 {
                info!(%chain, retracted, "retracted contracts created in orphaned blocks");
            }
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), SinkError> {
        if self.decoded > 0 {
            metrics::logs_decoded(std::mem::take(&mut self.decoded));
        }
        self.inner.flush().await
    }

    fn observe_head(&mut self, height: u64) {
        self.inner.observe_head(height);
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{Address, B256};

    use super::*;
    use crate::decode::fixtures::uniswap_v3::{pool_created, swap};
    use crate::decode::fixtures::{metric, shipped};
    use crate::wire::envelope::Reorg;

    #[derive(Default)]
    struct Collect {
        records: Vec<Envelope>,
        flushes: usize,
        /// The kind of the next envelope to refuse, once, as a closed channel would.
        refuse: Option<&'static str>,
    }
    impl EnvelopeSink for Collect {
        async fn publish(&mut self, envelope: Envelope) -> Result<(), SinkError> {
            if self.refuse == Some(envelope.event.kind()) {
                self.refuse = None;
                return Err(SinkError::StorageClosed);
            }
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
        DecodingSink::new(Decoder::new(shipped("base"), []), Collect::default())
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
        sink.inner
            .records
            .iter()
            .map(|envelope| envelope.event.kind())
            .collect()
    }

    /// A log no contract decodes and a control marker are each forwarded once, with
    /// nothing decoded after them, and a flush reaches the inner sink once.
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

    /// A creation log whose contract the inner sink refused leaves the contract set as it
    /// was: the child does not decode, and the same log published again publishes the
    /// contract. A reorg the inner sink refused retracts nothing until it is published.
    #[tokio::test]
    async fn a_refused_publish_leaves_the_contract_set_as_it_was() {
        let pool = Address::from([0xd0; 20]);
        let block = B256::with_last_byte(9);
        let mut sink = sink();
        sink.inner.refuse = Some("contract");
        sink.publish(log(pool_created(pool, block)))
            .await
            .expect_err("the contract is refused");
        sink.publish(log(swap(pool))).await.expect("swap");
        assert_eq!(kinds(&sink), ["log", "decoded", "log"]);

        sink.publish(log(pool_created(pool, block)))
            .await
            .expect("the creation again");
        sink.publish(log(swap(pool))).await.expect("swap");
        assert_eq!(
            kinds(&sink)[3..],
            ["log", "decoded", "contract", "log", "decoded"]
        );

        sink.inner.refuse = Some("reorg");
        sink.publish(reorg(block))
            .await
            .expect_err("the reorg is refused");
        sink.publish(log(swap(pool))).await.expect("swap");
        assert_eq!(kinds(&sink)[8..], ["log", "decoded"]);
        sink.publish(reorg(block)).await.expect("the reorg again");
        sink.publish(log(swap(pool))).await.expect("swap");
        assert_eq!(kinds(&sink)[10..], ["reorg", "log"]);
    }

    /// A record a derivation reads is followed by its derived row, before anything else
    /// the log produced. The row carries the sender and recipient of the transaction
    /// forwarded before the log, and neither when that transaction was not fetched.
    #[tokio::test]
    async fn a_derived_row_follows_its_decoded_record_with_its_transaction() {
        use alloy_primitives::{TxHash, U256};

        use crate::wire::datasets::evm::Transaction;

        let mut sink = DecodingSink::new(metric::decoder(), Collect::default());
        let mut swap = metric::swap_log(U256::ZERO, U256::ZERO, U256::ZERO);
        swap.transaction_hash = TxHash::repeat_byte(0x11);
        let transaction = Transaction {
            hash: swap.transaction_hash,
            from: Address::repeat_byte(0xf0),
            to: Some(Address::repeat_byte(0xf1)),
            ..Transaction::default()
        };
        sink.publish(Envelope::new(
            chain(),
            Event::Transaction(Box::new(transaction)),
        ))
        .await
        .expect("transaction");
        sink.publish(log(swap.clone())).await.expect("swap");
        assert_eq!(kinds(&sink), ["transaction", "log", "decoded", "derived"]);
        let Event::Derived(derived) = &sink.inner.records[3].event else {
            panic!("the swap's row");
        };
        assert_eq!(derived.table, "metric_v1_swaps");
        assert_eq!(
            (derived.log_index, derived.block_hash),
            (swap.log_index, swap.block_hash)
        );
        assert_eq!(
            (
                derived.transaction_from_address,
                derived.transaction_to_address
            ),
            (
                Some(Address::repeat_byte(0xf0)),
                Some(Address::repeat_byte(0xf1))
            )
        );

        swap.transaction_hash = TxHash::repeat_byte(0x22);
        sink.publish(log(swap))
            .await
            .expect("a swap of an unfetched transaction");
        let Some(Event::Derived(derived)) = sink.inner.records.last().map(|e| &e.event) else {
            panic!("the second swap's row");
        };
        assert_eq!(
            (
                derived.transaction_from_address,
                derived.transaction_to_address
            ),
            (None, None)
        );
    }
}
