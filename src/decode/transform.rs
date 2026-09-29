//! The decode transform: a raw log in, its decoded record out.
//!
//! A pure function of its input and the current registry snapshot, with no I/O and no
//! state of its own, so it can be tested exhaustively and replayed safely. The registry
//! is passed per call rather than owned, so the caller can register a discovered
//! contract between records — which is why `apply` takes `&R`.
//!
//! # What it produces
//!
//! One thing: a [`Decoded`] record for a log whose address is registered and whose
//! selector the ABI declares. The raw log is not replaced — it is published as it was,
//! and [`Decoded::source_key`] joins the record back to it — so the decoded record is an
//! addition to the stream, not a second copy of it.
//!
//! Everything else has nothing to add: a block, transaction, or receipt has no decoded
//! form; a log with no registered ABI, or one whose data does not decode, has nothing to
//! publish; and a [`Reorg`](Event::Reorg) or [`Finalized`](Event::Finalized) marker
//! already travels in the stream the caller is forwarding, so repeating it would only
//! duplicate it.
//!
//! # The invariants it must preserve
//!
//! Whatever decoding is added later, [`Transform::apply`] may never break these:
//!
//! 1. **A failed decode is reported, not silent.** A log whose data does not decode
//!    produces no record, but the failure rides on [`Applied::error`] so a caller logs
//!    it. A miss — no ABI for the address, or no selector on the ABI — is not a failure:
//!    the raw log is already stored, so a later ABI fix is a re-decode, not a re-fetch.
//! 2. **Identity is preserved.** A decoded envelope carries the input's `chain` and
//!    `sequence`, and a record's on-chain identity comes from the raw log, so it traces
//!    to the exact log it came from. `sequence` is never renumbered.
//!
//! [`Decoded`]: crate::wire::envelope::Event::Decoded
//! [`Decoded::source_key`]: crate::wire::envelope::Decoded::source_key

use crate::decode::abi::DecodeError;
use crate::decode::registry::{AbiRegistry, Discovery};
use crate::wire::envelope::{ChainId, Decoded, Envelope, Event, Log};

/// What one input envelope produced, and whether anything went wrong.
///
/// Usually nothing: most events are raw datasets with no decoded form, and most logs are
/// unregistered.
#[derive(Debug)]
pub struct Applied {
    /// The decoded record, to publish alongside the raw envelope. `None` when the input
    /// has no decoded form.
    pub output: Option<Envelope>,
    /// The decode failure, if a log matched an ABI but did not decode against it.
    pub error: Option<DecodeError>,
    /// A contract the decoded record revealed, for the caller to register.
    ///
    /// The transform cannot register it — it does not own the registry — so it surfaces
    /// the effect and the stage applies it. `None` unless the record came from a
    /// registered discovery rule.
    pub discovery: Option<Discovery>,
}

/// Decodes one raw envelope into its decoded record, if it has one.
///
/// Stateless by construction: it holds no registry, no undo ring, and assigns no
/// sequence numbers, so it cannot disagree with the pipeline about ordering, and
/// replaying a record produces the same output.
pub struct Transform;

impl Transform {
    /// The decoded record for one raw input envelope, if it has one.
    ///
    /// Returns an [`Applied`] rather than a `Result`, because a log that fails to decode
    /// is not an error the pipeline can act on: it produces no record and the failure
    /// rides alongside for a caller to log. A miss and a non-log are silent.
    pub fn apply<R: AbiRegistry>(registry: &R, envelope: &Envelope) -> Applied {
        match &envelope.event {
            // A log is the only thing an event ABI decodes. A block, transaction, or
            // receipt carries calldata an ABI *could* decode, but this transform does not
            // do that yet.
            Event::Log(log) => Self::decode(registry, &envelope.chain, envelope.sequence, log),
            // Nothing to add: a raw dataset with no decoded form, a marker the stream
            // already carries, or a record that is already decoded.
            Event::Block(_)
            | Event::Transaction(_)
            | Event::Receipt(_)
            | Event::Decoded(_)
            | Event::Reorg(_)
            | Event::Finalized(_) => Applied {
                output: None,
                error: None,
                discovery: None,
            },
        }
    }

    /// Decodes a log against the registry, and assembles the decoded record from the
    /// decoded event and the raw log it came from.
    ///
    /// A registry miss and an ABI that does not declare the log's selector are the same
    /// answer: `None`, which the caller drops.
    fn decode<R: AbiRegistry>(registry: &R, chain: &ChainId, sequence: u64, log: &Log) -> Applied {
        let Some(contract) = registry.contract(chain, log.address, log.block_number) else {
            return Applied {
                output: None,
                error: None,
                discovery: None,
            };
        };
        match contract.abi.decode_log(log) {
            Ok(Some(event)) => {
                // A discovery rule, if one is registered for this factory and selector,
                // reads the child address out of the decoded event. Resolved here so the
                // record and its effect travel together.
                let discovery = registry.discovery(chain, log.address, event.selector, &event);
                // The identity comes from the raw log, not the decoded event: the event is
                // a list of arguments, and where it sat on chain is the log's fact.
                // Assembling it here keeps [`Decoded`] pointing at the exact log it came
                // from.
                let record = Decoded {
                    name: event.name,
                    address: log.address,
                    protocol: contract.protocol,
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
                };
                Applied {
                    output: Some(Envelope::new(
                        chain.clone(),
                        sequence,
                        Event::Decoded(Box::new(record)),
                    )),
                    error: None,
                    discovery,
                }
            }
            Ok(None) => Applied {
                output: None,
                error: None,
                discovery: None,
            },
            Err(error) => Applied {
                output: None,
                error: Some(error),
                discovery: None,
            },
        }
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are allowed
// them per the repository test style, since a failed expectation there means the fixture
// or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use alloy_primitives::{Address, B256, TxHash};

    use crate::decode::abi::Abi;
    use crate::decode::registry::{AbiRegistry, Contract};
    use crate::wire::datasets::evm::{Block, Log, Receipt};
    use crate::wire::envelope::{ChainId, Envelope, Event, Finalized, Reorg, Transaction};

    use super::Transform;

    fn hash(byte: u8) -> B256 {
        B256::from([byte; 32])
    }

    fn envelope(event: Event) -> Envelope {
        Envelope::new(ChainId::new("base"), 17, event)
    }

    /// A registry that answers for one address on one chain, which is enough to drive
    /// the transform without a settings file.
    struct OneAbi {
        abi: Arc<Abi>,
    }

    impl AbiRegistry for OneAbi {
        fn contract(&self, chain: &ChainId, address: Address, _block: u64) -> Option<Contract> {
            (chain.as_str() == "base" && address == Address::from([0xaa; 20])).then(|| Contract {
                abi: Arc::clone(&self.abi),
                protocol: "uniswap_v3_pool".to_owned(),
            })
        }
    }

    /// An empty registry: every log is a miss, which is the pre-decoding behavior.
    struct NoAbi;

    impl AbiRegistry for NoAbi {
        fn contract(&self, _chain: &ChainId, _address: Address, _block: u64) -> Option<Contract> {
            None
        }
    }

    /// One of every event kind, so the contract below is checked against all of them
    /// rather than the one shape that happens to be handy.
    fn every_kind() -> Vec<Event> {
        vec![
            Event::Block(Box::new(Block {
                number: 5,
                hash: hash(9),
                parent_hash: hash(8),
                timestamp: 1_700_000_000,
                ..Block::default()
            })),
            Event::Transaction(Box::new(Transaction {
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

    /// Only a log can decode. Every other kind — the datasets, and the control markers
    /// the stream already carries — produces nothing, so a marker is never duplicated
    /// into the stored stream. Every kind is checked, not just the handy one.
    #[test]
    fn only_a_log_can_produce_a_record() {
        for event in every_kind()
            .into_iter()
            .filter(|event| !matches!(event, Event::Log(_)))
        {
            let kind = event.kind();
            let applied = Transform::apply(&NoAbi, &envelope(event));
            assert!(applied.error.is_none(), "{kind} reported an error");
            assert!(
                applied.output.is_none(),
                "{kind} produced a record, but only a log decodes"
            );
        }
    }

    /// The decode path: a registered log whose selector matches becomes a decoded
    /// record, and the record joins back to the raw log by
    /// `source_key`.
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
        let abi = Abi::from_json(ERC20).expect("ABI loads");

        let mut from = [0u8; 32];
        from[12..].copy_from_slice(&[0x11; 20]);
        let mut to = [0u8; 32];
        to[12..].copy_from_slice(&[0x22; 20]);
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
            data: [0u8; 32].to_vec().into(),
            block_number: 5,
            block_hash: hash(5),
            ..Log::default()
        };
        let source = envelope(Event::Log(Box::new(log)));

        let registry = OneAbi { abi: Arc::new(abi) };
        let applied = Transform::apply(&registry, &source);
        assert!(applied.error.is_none(), "the log decodes");

        let decoded = applied.output.expect("a decoded record");
        assert_eq!(decoded.chain, source.chain);
        assert_eq!(decoded.sequence, source.sequence);
        let Event::Decoded(record) = &decoded.event else {
            panic!("the output must be the decoded record");
        };
        assert_eq!(record.name, "Transfer");
        // The protocol comes from the same lookup that found the ABI.
        assert_eq!(record.protocol, "uniswap_v3_pool");
        assert_eq!(
            record.source_key(),
            format!("5:{}:3", TxHash::from([0x01; 32]))
        );
        assert_eq!(record.indexed.len(), 2);
        assert_eq!(record.body.len(), 1);
    }

    /// Invariant 1: a log whose data does not decode produces no record, but the failure
    /// is reported rather than swallowed. The raw log is already stored, so a
    /// corrected ABI recovers it.
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
        let registry = OneAbi {
            abi: Arc::new(Abi::from_json(ERC20).expect("ABI loads")),
        };
        let applied = Transform::apply(&registry, &envelope(Event::Log(Box::new(log))));
        assert!(applied.error.is_some(), "a bad log reports its failure");
        assert!(
            applied.output.is_none(),
            "a failed decode produces no record"
        );
    }
}
