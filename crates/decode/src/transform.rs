//! The decode transform: raw envelopes in, decoded envelopes out.
//!
//! This is the whole of the second stage. It is deliberately a pure function with
//! no I/O and no state, so it can be tested exhaustively and replayed safely.
//!
//! # The invariants it must preserve
//!
//! Whatever decoding is added later, [`Transform::apply`] may never break these:
//!
//! 1. **Control signals pass through verbatim.** [`Event::Reorg`] and
//!    [`Event::Finalized`] come out exactly as they went in. A store retracts
//!    orphaned rows from the reorg's `orphaned_hashes` and compacts below the
//!    finality watermark, so losing either makes the decoded stream quietly wrong.
//! 2. **Nothing is dropped for lack of an ABI.** An event with no matching ABI is
//!    forwarded unchanged, so the decoded topic is a lossless superset of the raw
//!    one and a later ABI addition is a replay rather than a re-fetch.
//! 3. **Identity is preserved.** Every output carries its source envelope's `chain`
//!    and `dedupe_key`. `sequence` orders the stream and is never renumbered.
//!
//! [`Event::Reorg`]: wire::envelope::Event::Reorg
//! [`Event::Finalized`]: wire::envelope::Event::Finalized

use wire::envelope::Envelope;

/// Decodes one raw envelope into the envelopes to republish.
///
/// Stateless by construction: it holds no undo ring and assigns no sequence
/// numbers, so it cannot disagree with the pipeline about ordering, and replaying a
/// record produces the same output.
///
/// Not built yet: the ABI registry and the actual decoding. Until then `apply` is
/// the identity, which is the correct floor — the transform's shape and invariants
/// are pinned by tests before any decoding logic exists.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Transform;

impl Transform {
    /// Builds the identity transform.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// The envelopes to republish for one raw input envelope.
    ///
    /// Returns a `Vec` rather than a single envelope because one input record can
    /// yield several decoded records — one log with an ABI the consumer cares about
    /// may decode into more than one row. Today it always returns exactly one,
    /// unchanged.
    #[must_use]
    pub fn apply(&self, envelope: Envelope) -> Vec<Envelope> {
        // No ABI registry yet, so every event takes the no-match path: forwarded
        // unchanged. This is invariant 2, and it stays the fallback once decoding
        // lands.
        vec![envelope]
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{Address, B256, TxHash};
    use wire::envelope::{Block, ChainId, Envelope, Event, Finalized, Log, Receipt, Reorg};

    use super::Transform;

    fn hash(byte: u8) -> B256 {
        B256::from([byte; 32])
    }

    fn envelope(event: Event) -> Envelope {
        Envelope::new(ChainId::new("base"), 17, event)
    }

    /// One of every event kind, so the contract below is checked against all of
    /// them rather than against the one shape that happens to be handy.
    fn every_kind() -> Vec<Event> {
        vec![
            Event::Block(Box::new(Block {
                number: 5,
                hash: hash(9),
                parent_hash: hash(8),
                timestamp: 1_700_000_000,
                ..Block::default()
            })),
            Event::Transaction(Box::new(wire::envelope::Transaction {
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
        let output = Transform::new().apply(source.clone());

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
        let output = Transform::new().apply(source.clone());
        assert_eq!(output, vec![source]);
    }

    /// Invariants 2 and 3: every kind is forwarded, never dropped, and each output
    /// keeps its source's identity.
    #[test]
    fn every_kind_is_forwarded_unchanged_with_its_identity() {
        for event in every_kind() {
            let kind = event.kind();
            let source = envelope(event);
            let output = Transform::new().apply(source.clone());

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
}
