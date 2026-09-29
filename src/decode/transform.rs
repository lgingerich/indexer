//! The decode transform: raw envelopes in, decoded envelopes out.
//!
//! This is the whole of the second stage. It is a pure function of its input and a
//! [`AbiRegistry`], with no I/O and no state of its own, so it can be tested
//! exhaustively and replayed safely.
//!
//! # The invariants it must preserve
//!
//! Whatever decoding is added later, [`Transform::apply`] may never break these:
//!
//! 1. **Control signals pass through verbatim.** [`Event::Reorg`] and
//!    [`Event::Finalized`] come out exactly as they went in. A store retracts
//!    orphaned rows from the reorg's `orphaned_hashes` and compacts below the
//!    finality watermark, so losing either makes the decoded stream quietly wrong.
//! 2. **Nothing is dropped for lack of an ABI, or for a failed decode.** An event
//!    with no matching ABI is forwarded unchanged, and so is a log whose data did not
//!    decode: the raw log is always the first output, so the decoded topic is a
//!    lossless superset of the raw one and a later ABI fix is a replay rather than a
//!    re-fetch. A failed decode is reported on [`Applied`], not by withholding the log.
//! 3. **Identity is preserved.** Every output carries its source envelope's `chain`
//!    and `dedupe_key`. `sequence` orders the stream and is never renumbered.
//!
//! A decoded record is emitted *alongside* the log it came from, not instead of it:
//! step 2 is what makes that the only defensible choice, since a consumer that wants
//! the raw log must not have to reconstruct it from a decoded row.
//!
//! [`Event::Reorg`]: crate::wire::envelope::Event::Reorg
//! [`Event::Finalized`]: crate::wire::envelope::Event::Finalized

use crate::wire::envelope::{ChainId, Decoded, Envelope, Event, Log};
use alloy_primitives::B256;

use crate::decode::registry::{AbiRegistry, Contract, RegistryError};

/// What one input envelope produced, and whether anything went wrong.
///
/// The log is always in [`outputs`](Self::outputs) — a failed decode is reported, not
/// withheld — so a caller forwards the raw log and decides for itself how loud a
/// failure is. [`Transform::apply`] never returns `Err` for a bad log.
#[derive(Debug)]
pub struct Applied {
    /// The envelopes to republish: the source, and its decoded record if it decoded.
    pub outputs: Vec<Envelope>,
    /// The decode failure, if a log matched an ABI but did not decode against it.
    ///
    /// Present alongside a pass-through of the raw log, so a caller logs it and moves on.
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

    /// The envelopes to republish for one raw input envelope.
    ///
    /// Returns an [`Applied`] rather than a `Result`, because a log that fails to
    /// decode still has to be forwarded: the raw log is the first output whether or
    /// not a decoded record follows it, and the failure rides alongside it. That keeps
    /// invariant 2 — the decoded stream stays a superset of the raw one — while still
    /// surfacing the failure a caller may want to act on.
    pub fn apply(&self, envelope: Envelope) -> Applied {
        let (decoded, error) = match &envelope.event {
            // A log is the only thing an event ABI decodes. A block, transaction, or
            // receipt carries calldata an ABI *could* decode, but this transform does
            // not do that yet; they pass through unchanged.
            Event::Log(log) => match self.decode(&envelope.chain, log) {
                Ok(decoded) => (decoded, None),
                Err(error) => (None, Some(error)),
            },
            _ => (None, None),
        };

        let mut output = Vec::with_capacity(2);
        let sequence = envelope.sequence;
        let chain = envelope.chain.clone();
        output.push(envelope);
        if let Some(record) = decoded {
            output.push(Envelope::new(
                chain,
                sequence,
                Event::Decoded(Box::new(record)),
            ));
        }
        Applied {
            outputs: output,
            error,
        }
    }

    /// Decodes a log against the registry, and assembles the published record from
    /// the decoded event and the raw log it came from.
    ///
    /// A registry miss and an ABI that does not declare the log's selector are the
    /// same answer here: the log is forwarded undecoded.
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

        let output = applied.outputs;
        assert_eq!(output, vec![source]);
        let Some(Envelope {
            event: Event::Reorg(reorg),
            ..
        }) = output.first()
        else {
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
        assert_eq!(applied.outputs, vec![source]);
    }

    /// Invariants 2 and 3: with no ABI registered, every kind is forwarded, never
    /// dropped, and each output keeps its source's identity.
    #[test]
    fn every_kind_is_forwarded_unchanged_with_its_identity() {
        for event in every_kind() {
            let kind = event.kind();
            let source = envelope(event);
            let applied = Transform::new(NoAbi).apply(source.clone());

            assert!(applied.error.is_none(), "{kind} reported an error");
            let output = applied.outputs;
            assert_eq!(output.len(), 1, "{kind} was dropped or duplicated");
            let forwarded = output.first().expect("one output");
            assert_eq!(forwarded.chain, source.chain, "{kind} lost its chain");
            assert_eq!(
                forwarded.event.dedupe_key(),
                source.event.dedupe_key(),
                "{kind} changed its dedupe key"
            );
            assert_eq!(forwarded.sequence, source.sequence, "{kind} was renumbered");
            assert_eq!(*forwarded, source, "{kind} changed across the transform");
        }
    }

    /// The decode path: a log that matches a registered ABI comes out *with* its
    /// decoded record, and the raw log is still there — the decoded stream is a
    /// superset, never a replacement.
    #[test]
    fn a_matching_log_is_emitted_with_its_decoded_record() {
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
        let output = applied.outputs;

        assert_eq!(output.len(), 2, "the raw log and its decoded record");
        // The raw log is forwarded first and unchanged.
        assert_eq!(output.first(), Some(&source));
        // The decoded record follows, carrying the same chain and sequence, and its
        // own identity is derived from the log it came from.
        let decoded = output.get(1).expect("a decoded record");
        assert_eq!(decoded.chain, source.chain);
        assert_eq!(decoded.sequence, source.sequence);
        let Event::Decoded(record) = &decoded.event else {
            panic!("the second output must be the decoded record");
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

    /// Invariant 2: a log whose data does not decode is still forwarded, so the
    /// decoded stream stays a superset of the raw one. The failure is reported on the
    /// result rather than by withholding the log — the regression was a `continue` in
    /// the stage that dropped the pass-through, breaking the superset guarantee.
    #[test]
    fn a_log_that_fails_to_decode_is_still_forwarded() {
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
        let applied = Transform::new(registry).apply(source.clone());

        assert!(applied.error.is_some(), "a bad log reports its failure");
        assert_eq!(
            applied.outputs,
            vec![source],
            "the raw log is forwarded even when it does not decode"
        );
    }
}
