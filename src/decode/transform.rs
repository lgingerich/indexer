//! The decode transform: a raw log in, its decoded record out.
//!
//! This is the whole of the second stage. It is a pure function of its input and a
//! [`AbiRegistry`], with no I/O and no state of its own, so it can be tested
//! exhaustively and replayed safely.
//!
//! # What lands on the decoded topic
//!
//! The stage republishes exactly two things and drops everything else:
//!
//! - **Decoded records.** A log whose address is registered and whose selector the
//!   ABI declares becomes a [`Decoded`] record. The raw log is *not* republished —
//!   it is already on the raw topic, and [`Decoded::source_key`] joins back to it —
//!   so the decoded topic is the decode *output*, not a second copy of the raw stream.
//! - **Control signals.** [`Event::Reorg`] and [`Event::Finalized`] are forwarded
//!   verbatim, because a store retracts orphaned rows from the reorg's
//!   `orphaned_hashes` and compacts below the finality watermark.
//!
//! A block, transaction, or receipt has no decoded form; a log with no registered ABI,
//! or one whose data does not decode, has nothing to publish. All of them are dropped,
//! since the raw topic already carries them.
//!
//! # The invariants it must preserve
//!
//! Whatever decoding is added later, [`Transform::apply`] may never break these:
//!
//! 1. **Control signals pass through verbatim.** [`Event::Reorg`] and
//!    [`Event::Finalized`] come out exactly as they went in, with their `chain` and
//!    `sequence` intact. Losing either makes the decoded stream quietly wrong.
//! 2. **A failed decode is reported, not silent.** A log whose data does not decode
//!    produces no record, but the failure rides on [`Applied::error`] so a caller logs
//!    it. A miss — no ABI for the address, or no selector on the ABI — is not a
//!    failure: the raw log is already upstream, so a later ABI fix is a replay rather
//!    than a re-fetch.
//! 3. **Identity is preserved.** Every published envelope carries the input's `chain`
//!    and `sequence`, and a decoded record's on-chain identity comes from the raw log,
//!    so it traces to the exact log it came from. `sequence` is never renumbered.
//!
//! [`Decoded`]: crate::wire::envelope::Event::Decoded
//! [`Decoded::source_key`]: crate::wire::envelope::Decoded::source_key
//! [`Event::Reorg`]: crate::wire::envelope::Event::Reorg
//! [`Event::Finalized`]: crate::wire::envelope::Event::Finalized

use crate::wire::envelope::{ChainId, Decoded, Envelope, Event, Log};
use alloy_primitives::B256;

use crate::decode::registry::{AbiRegistry, Contract, RegistryError};

/// What one input envelope produced, and whether anything went wrong.
///
/// Usually nothing: most events are raw datasets with no decoded form, and most logs
/// are unregistered. [`Transform::apply`] never returns `Err` for a bad log — a failed
/// decode is reported on [`error`](Self::error) so a caller logs it and moves on.
#[derive(Debug)]
pub struct Applied {
    /// The envelope to republish: the decoded record, or a control signal forwarded
    /// verbatim. `None` when the input has no decoded form.
    pub output: Option<Envelope>,
    /// The decode failure, if a log matched an ABI but did not decode against it.
    ///
    /// Independent of [`output`](Self::output): a failed decode produces no record, so
    /// this is what a caller logs instead.
    pub error: Option<RegistryError>,
}

/// Decodes one raw envelope into the envelopes to republish.
///
/// Stateless by construction: it holds no undo ring and assigns no sequence numbers,
/// so it cannot disagree with the pipeline about ordering, and replaying a record
/// produces the same output.
#[derive(Debug)]
pub struct Transform<R> {
    registry: R,
}

impl<R: AbiRegistry> Transform<R> {
    /// Builds a transform over a registry.
    #[must_use]
    pub const fn new(registry: R) -> Self {
        Self { registry }
    }

    /// The envelope to republish for one raw input envelope, if any.
    ///
    /// Returns an [`Applied`] rather than a `Result`, because a log that fails to
    /// decode is not an error the pipeline can act on: it produces no record and the
    /// failure rides alongside for a caller to log. A miss and a non-log are silent —
    /// there is nothing to publish and nothing to report.
    pub fn apply(&self, envelope: Envelope) -> Applied {
        let Envelope {
            chain,
            sequence,
            event,
            ..
        } = envelope;
        match event {
            // A log is the only thing an event ABI decodes. A block, transaction, or
            // receipt carries calldata an ABI *could* decode, but this transform does
            // not do that yet; with no decoded form it is dropped, since the raw topic
            // already carries it.
            Event::Log(log) => match self.decode(&chain, &log) {
                Ok(Some(record)) => Applied {
                    output: Some(Envelope::new(
                        chain,
                        sequence,
                        Event::Decoded(Box::new(record)),
                    )),
                    error: None,
                },
                Ok(None) => Applied {
                    output: None,
                    error: None,
                },
                Err(error) => Applied {
                    output: None,
                    error: Some(error),
                },
            },
            // A control signal must survive to drive a store's retraction and
            // compaction, so it is forwarded exactly as it arrived.
            forwarded @ (Event::Reorg(_) | Event::Finalized(_)) => Applied {
                output: Some(Envelope::new(chain, sequence, forwarded)),
                error: None,
            },
            // A raw dataset with no decoded form: the raw topic already carries it.
            Event::Block(_) | Event::Transaction(_) | Event::Receipt(_) | Event::Decoded(_) => {
                Applied {
                    output: None,
                    error: None,
                }
            }
        }
    }

    /// Decodes a log against the registry, and assembles the published record from
    /// the decoded event and the raw log it came from.
    ///
    /// A registry miss and an ABI that does not declare the log's selector are the
    /// same answer here: `None`, which the caller drops — the raw log is already on
    /// the raw topic.
    fn decode(&self, chain: &ChainId, log: &Log) -> Result<Option<Decoded>, RegistryError> {
        let Some(Contract { abi, protocol }) =
            self.registry.contract(chain, log.address, log.block_number)
        else {
            return Ok(None);
        };

        // The wire flattens a log's topics into `topic0..topic3`, so they have to be
        // packed back into the contiguous list the decoder expects. Topics are
        // contiguous from `topic0` by construction, so the first gap ends the list.
        let mut flattened = [log.topic0, log.topic1, log.topic2, log.topic3];
        let topics: Vec<B256> = flattened.iter_mut().map_while(Option::take).collect();

        let Some(event) = abi.decode_log(&topics, &log.data)? else {
            return Ok(None);
        };

        // The identity comes from the raw log, not the decoded event: the event is a
        // list of arguments, and where it sat on chain is the log's fact. Assembling
        // it here is what keeps [`Decoded`] pointing at the exact log it came from.
        Ok(Some(Decoded {
            name: event.name,
            address: log.address,
            protocol: protocol.to_owned(),
            selector: event.selector,
            signature: event.signature,
            anonymous: event.anonymous,
            transaction_hash: log.transaction_hash,
            transaction_index: log.transaction_index,
            log_index: log.log_index,
            indexed: event.indexed,
            body: event.body,
            block_number: log.block_number,
            block_hash: log.block_hash,
            block_timestamp: log.block_timestamp,
        }))
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use crate::wire::envelope::{Block, ChainId, Envelope, Event, Finalized, Log, Receipt, Reorg};
    use alloy_primitives::{Address, B256, TxHash, U256};

    use crate::decode::registry::{Abi, Contract};

    use super::Transform;

    fn hash(byte: u8) -> B256 {
        B256::from([byte; 32])
    }

    fn envelope(event: Event) -> Envelope {
        Envelope::new(ChainId::new("base"), 17, event)
    }

    /// A registry that answers for one address on one chain, which is enough to
    /// drive the transform without a backing store.
    struct OneAbi {
        abi: Abi,
    }

    impl crate::decode::registry::AbiRegistry for OneAbi {
        fn contract(&self, chain: &ChainId, address: Address, _block: u64) -> Option<Contract<'_>> {
            (chain.as_str() == "base" && address == Address::from([0xaa; 20])).then_some(Contract {
                abi: &self.abi,
                protocol: "uniswap_v3",
            })
        }
    }

    /// An empty registry: every log is a miss, which is the pre-decoding behavior.
    struct NoAbi;

    impl crate::decode::registry::AbiRegistry for NoAbi {
        fn contract(
            &self,
            _chain: &ChainId,
            _address: Address,
            _block: u64,
        ) -> Option<Contract<'_>> {
            None
        }
    }

    /// One of every event kind, so the contract below is checked against all of them
    /// rather than against the one shape that happens to be handy.
    fn every_kind() -> Vec<Event> {
        vec![
            Event::Block(Box::new(Block {
                number: 5,
                hash: hash(9),
                parent_hash: hash(8),
                timestamp: 1_700_000_000,
                ..Block::default()
            })),
            Event::Transaction(Box::new(crate::wire::envelope::Transaction {
                hash: TxHash::from([0x11; 32]),
                from: Address::from([0x22; 20]),
                block_number: 5,
                ..Default::default()
            })),
            Event::Receipt(Box::new(Receipt {
                transaction_hash: TxHash::from([0x11; 32]),
                block_number: 5,
                ..Receipt::default()
            })),
            Event::Log(Box::new(Log {
                log_index: 1,
                transaction_hash: TxHash::from([0x11; 32]),
                block_number: 5,
                topic0: Some(hash(0x07)),
                ..Log::default()
            })),
            Event::Reorg(Reorg {
                height: 4,
                new_head_hash: hash(0x20),
                orphaned_hashes: vec![hash(5), hash(6)],
            }),
            Event::Finalized(Finalized {
                height: 3,
                hash: hash(3),
            }),
        ]
    }

    /// Invariant 1: a reorg survives decoding with its retraction list intact. If
    /// this ever breaks, a store keeps orphaned rows forever.
    #[test]
    fn a_reorg_passes_through_with_its_orphaned_hashes() {
        let source = envelope(Event::Reorg(Reorg {
            height: 4,
            new_head_hash: hash(0x20),
            orphaned_hashes: vec![hash(5), hash(6)],
        }));
        let applied = Transform::new(NoAbi).apply(source.clone());
        assert!(
            applied.error.is_none(),
            "a control signal is not a decode error"
        );

        let output = applied.output.expect("a reorg is forwarded");
        assert_eq!(output, source);
        let Event::Reorg(reorg) = &output.event else {
            panic!("a reorg must come out a reorg");
        };
        assert_eq!(reorg.height, 4);
        assert_eq!(reorg.orphaned_hashes, vec![hash(5), hash(6)]);
    }

    /// Invariant 1: a finality watermark survives, because it is what lets a store
    /// compact below it.
    #[test]
    fn a_finalized_watermark_passes_through() {
        let source = envelope(Event::Finalized(Finalized {
            height: 3,
            hash: hash(3),
        }));
        let applied = Transform::new(NoAbi).apply(source.clone());
        assert!(applied.error.is_none());
        assert_eq!(applied.output, Some(source));
    }

    /// Invariant 1: every control signal is forwarded exactly as it arrived, keeping its
    /// chain and sequence. Losing a reorg's retraction list makes a store keep orphaned
    /// rows forever; losing a finality watermark stops it compacting.
    #[test]
    fn a_control_signal_is_forwarded_unchanged_with_its_identity() {
        for event in every_kind().into_iter().filter(|event| !event.is_dataset()) {
            let kind = event.kind();
            let source = envelope(event);
            let applied = Transform::new(NoAbi).apply(source.clone());

            assert!(applied.error.is_none(), "{kind} reported an error");
            let forwarded = applied.output.expect("a control signal is forwarded");
            assert_eq!(forwarded.chain, source.chain, "{kind} lost its chain");
            assert_eq!(forwarded.sequence, source.sequence, "{kind} was renumbered");
            assert_eq!(
                forwarded.event.dedupe_key(),
                source.event.dedupe_key(),
                "{kind} changed its dedupe key"
            );
            assert_eq!(forwarded, source, "{kind} changed across the transform");
        }
    }

    /// A raw dataset has no decoded form, so it is dropped: the raw topic already carries
    /// it, and the decoded topic is the decode output rather than a second copy of the
    /// stream. Every dataset kind is checked, not just the handy one.
    #[test]
    fn a_raw_dataset_is_dropped_because_the_raw_topic_carries_it() {
        for event in every_kind().into_iter().filter(Event::is_dataset) {
            let kind = event.kind();
            let applied = Transform::new(NoAbi).apply(envelope(event));
            assert!(applied.error.is_none(), "{kind} reported an error");
            assert!(
                applied.output.is_none(),
                "{kind} was published, but only a decoded record belongs on the topic"
            );
        }
    }

    /// The decode path: a registered log whose selector matches becomes a decoded
    /// record. The raw log is not republished — it is already on the raw topic, and the
    /// decoded record joins back to it by `source_key`.
    #[test]
    fn a_matching_log_becomes_a_decoded_record() {
        const ERC20: &str = r#"[{
            "type": "event",
            "name": "Transfer",
            "anonymous": false,
            "inputs": [
                {"name": "from", "type": "address", "indexed": true},
                {"name": "to", "type": "address", "indexed": true},
                {"name": "value", "type": "uint256", "indexed": false}
            ]
        }]"#;

        let mut from = [0u8; 32];
        from[12..].copy_from_slice(&[0x11; 20]);
        let mut to = [0u8; 32];
        to[12..].copy_from_slice(&[0x22; 20]);
        let value = U256::from(5).to_be_bytes::<32>();

        let log = Log {
            log_index: 3,
            transaction_hash: TxHash::from([0x01; 32]),
            address: Address::from([0xaa; 20]),
            topic0: Some(
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                    .parse()
                    .expect("selector parses"),
            ),
            topic1: Some(B256::from(from)),
            topic2: Some(B256::from(to)),
            topic3: None,
            data: value.to_vec().into(),
            block_number: 5,
            block_hash: hash(5),
            ..Log::default()
        };
        let source = envelope(Event::Log(Box::new(log)));

        let registry = OneAbi {
            abi: Abi::from_json(ERC20).expect("ABI loads"),
        };
        let applied = Transform::new(registry).apply(source.clone());
        assert!(applied.error.is_none(), "the log decodes");

        let decoded = applied.output.expect("a decoded record");
        assert_eq!(decoded.chain, source.chain);
        assert_eq!(decoded.sequence, source.sequence);
        let Event::Decoded(record) = &decoded.event else {
            panic!("the output must be the decoded record");
        };
        assert_eq!(record.name, "Transfer");
        // The protocol comes from the same lookup that found the ABI.
        assert_eq!(record.protocol, "uniswap_v3");
        assert_eq!(record.address, Address::from([0xaa; 20]));
        assert_eq!(
            record.source_key(),
            format!("5:{}:3", TxHash::from([0x01; 32]))
        );
        assert_eq!(record.indexed.len(), 2);
        assert_eq!(record.body.len(), 1);
    }

    /// Invariant 2: a log whose data does not decode produces no record, but the failure
    /// is reported rather than swallowed. The raw log is already on the raw topic, so a
    /// corrected ABI recovers it — nothing is lost by not republishing it here.
    #[test]
    fn a_log_that_fails_to_decode_reports_and_produces_no_record() {
        const ERC20: &str = r#"[{
            "type": "event",
            "name": "Transfer",
            "anonymous": false,
            "inputs": [
                {"name": "from", "type": "address", "indexed": true},
                {"name": "to", "type": "address", "indexed": true},
                {"name": "value", "type": "uint256", "indexed": false}
            ]
        }]"#;

        let log = Log {
            log_index: 3,
            transaction_hash: TxHash::from([0x01; 32]),
            address: Address::from([0xaa; 20]),
            topic0: Some(
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                    .parse()
                    .expect("selector parses"),
            ),
            // The selector matches but `data` is too short for the `uint256` the event
            // declares, so the decode fails.
            data: vec![0u8; 16].into(),
            block_number: 5,
            block_hash: hash(5),
            ..Log::default()
        };
        let source = envelope(Event::Log(Box::new(log)));

        let registry = OneAbi {
            abi: Abi::from_json(ERC20).expect("ABI loads"),
        };
        let applied = Transform::new(registry).apply(source);

        assert!(applied.error.is_some(), "a bad log reports its failure");
        assert!(
            applied.output.is_none(),
            "a failed decode produces no record"
        );
    }
}
