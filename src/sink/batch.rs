//! What a store has been handed since its last commit, buffered the same way by every
//! store: rows rendered from envelopes, the blocks a `reorg` retracted, and the last
//! copy of each row.
//!
//! # Blocks a store already holds
//!
//! A restart resumes ingest from the slowest store's tip, so a faster store is handed
//! again blocks it committed before the stop. [`Batch::reconcile`] names them, before
//! the first envelope, and each is settled when its `accepted_block` marker arrives, the
//! last envelope of its block:
//!
//! - the same hash at that height: the store holds this block, so its rows are dropped
//!   from the buffer and nothing is written for it;
//! - another hash: the store's copy was orphaned while it was ahead, so the batch
//!   records a retraction of it — a `reorgs` row, and its rows deleted at the commit,
//!   before the canonical block's are written — as a reorg at the tip would.
//!
//! A store ahead on the canonical chain therefore deletes nothing on a restart, which
//! matters for a database serving an app: see `sink::resume`.

use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use alloy_primitives::B256;

use crate::sink::table::{Row, Schema, TableId, Value};
use crate::sink::{SinkError, StoreError, hex};
use crate::wire::envelope::{BlockMeta, ChainId, Envelope, Event, Reorg};

/// What a store has been handed since its last commit: the rows to write, and the
/// blocks a buffered `reorg` retracted.
///
/// A decoded record is two rows: its generic `decoded_logs` row, and its event's typed
/// row, from the store's [`Schema`]. A row of a table the store does not write is not
/// kept, so a store routed only one of the two keeps only that one.
///
/// A store holds only the canonical chain. A `reorg` orphans blocks that are either
/// already committed or still in this buffer — blocks are published in order and the
/// storage channel is FIFO, so an orphaned block can never arrive after its `reorg`.
/// [`push`](Self::push) drops the buffered ones at once, and the store deletes the
/// committed ones when it commits the buffered rows, before writing them.
///
/// A store holds one chain, the one it was opened for, and so does its batch: an
/// envelope of another chain is refused rather than written beside it.
#[derive(Debug)]
pub(super) struct Batch {
    /// The chain the store was opened for.
    chain: ChainId,
    /// The rows published up to the last `accepted_block` marker, in publish order.
    rows: Vec<Row>,
    /// The rows published since the last marker, in publish order: those of the block
    /// being buffered, and any published between two blocks, such as a `reorg`'s. They
    /// join `rows` at the next marker, except the rows of a block the store holds, which
    /// are dropped instead.
    block: Vec<Row>,
    /// The blocks buffered `reorg`s orphaned, to delete from the store at the commit;
    /// `None` when no `reorg` is buffered that orphans any.
    pub(super) orphaned: Option<Orphaned>,
    /// Roughly how many bytes `rows` and `block` hold in memory.
    bytes: usize,
    /// The blocks the store committed above where ingest resumed, each settled as its
    /// height arrives (see the module docs). Kept across commits.
    held: Held,
    /// Every table the run writes, which renders each decoded record's typed row.
    schema: Arc<Schema>,
}

/// The blocks a batch's `reorg`s orphaned.
#[derive(Debug)]
pub(super) struct Orphaned {
    /// The lowest buffered `reorg` height, which none of them is below.
    pub(super) floor: u64,
    /// Their hashes, as `0x` hex.
    pub(super) hashes: BTreeSet<String>,
}

/// The blocks a store committed above where ingest resumed, which ingest hands it again.
///
/// They can be named only before the first envelope: a block handed earlier would have
/// been settled against nothing, so a later [`Batch::reconcile`] is refused rather than
/// applied to some heights and not others.
#[derive(Debug)]
enum Held {
    /// Nothing has been pushed or named yet.
    Unnamed,
    /// What is left to settle, by height: the blocks [`Batch::reconcile`] named, or none
    /// when the first envelope came before any were. Each is forgotten once ingest hands
    /// its height or one above it.
    Settling(BTreeMap<u64, B256>),
}

/// How a block ingest hands relates to what the store holds at its height.
#[derive(Debug, PartialEq, Eq)]
enum Arrival {
    /// The store holds no block there, so this one is written.
    New,
    /// The store holds this very block, so its rows are dropped and nothing is written.
    Same,
    /// The store holds this other block there, on a branch that lost while the store was
    /// ahead, so it is retracted before this one is written.
    Replaces(B256),
}

impl Held {
    /// Closes naming: from the first envelope on, the store holds only what was named.
    fn seal(&mut self) {
        if matches!(self, Self::Unnamed) {
            *self = Self::Settling(BTreeMap::new());
        }
    }

    /// Settles `block`, which ingest is handing: what the store holds at its height, which
    /// is then forgotten. Ingest hands heights in order, so a held height below this one
    /// was passed over and is never handed: it is forgotten too, rather than kept for the
    /// rest of the run.
    fn arrive(&mut self, block: &BlockMeta) -> Arrival {
        let Self::Settling(held) = self else {
            return Arrival::New;
        };
        let mut above = held.split_off(&block.height);
        let at = above.remove(&block.height);
        *held = above;
        match at {
            None => Arrival::New,
            Some(hash) if hash == block.hash => Arrival::Same,
            Some(hash) => Arrival::Replaces(hash),
        }
    }
}

impl Batch {
    /// An empty batch of `schema`'s tables, for a store of `chain`.
    pub(super) fn new(schema: Arc<Schema>, chain: ChainId) -> Self {
        Self {
            chain,
            rows: Vec::new(),
            block: Vec::new(),
            orphaned: None,
            bytes: 0,
            held: Held::Unnamed,
            schema,
        }
    }

    /// The chain the store was opened for, and the only one its batch takes.
    pub(super) const fn chain(&self) -> &ChainId {
        &self.chain
    }

    /// Names the blocks the store already holds that ingest will hand it again: those
    /// above the block every store holds, where ingest resumes. Each is settled when its
    /// height arrives; see the module docs.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::LateReconcile`] once an envelope has been pushed or the
    /// blocks were named already: settling only the heights still to come would leave
    /// the others unchecked.
    pub(super) fn reconcile(&mut self, held: &[BlockMeta]) -> Result<(), StoreError> {
        let Held::Unnamed = self.held else {
            return Err(StoreError::LateReconcile);
        };
        self.held = Held::Settling(
            held.iter()
                .map(|block| (block.height, block.hash))
                .collect(),
        );
        Ok(())
    }

    /// Buffers one envelope's rows. A `reorg` first drops every buffered row of the
    /// blocks it orphans and records them for deletion; its own row is kept as the
    /// record of the retraction. A block the store already holds is settled at its
    /// `accepted_block` marker; see the module docs.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::OtherChain`] for an envelope of another chain than the
    /// store's, and [`SinkError::Table`] for a decoded record no event table holds, which
    /// means it was decoded against a different catalog than the store opened with, and
    /// when a row cannot be built.
    pub(super) fn push(&mut self, envelope: &Envelope) -> Result<(), SinkError> {
        if envelope.chain != self.chain {
            return Err(StoreError::OtherChain {
                store: self.chain.to_string(),
                envelope: envelope.chain.to_string(),
            }
            .into());
        }
        self.held.seal();
        let Event::AcceptedBlock(block) = &envelope.event else {
            return self.buffer(envelope);
        };
        match self.held.arrive(block) {
            Arrival::New => {}
            Arrival::Same => {
                self.drop_block(block.hash);
                return Ok(());
            }
            // The store's copy of this height is on a branch that lost while it was
            // ahead: retract it as a reorg at the tip would, before this block lands.
            Arrival::Replaces(held) => self.buffer(&Envelope::new(
                envelope.chain.clone(),
                Event::Reorg(Reorg {
                    height: block.height,
                    new_head_hash: block.hash,
                    orphaned_hashes: vec![held],
                }),
            ))?,
        }
        self.buffer(envelope)?;
        self.rows.append(&mut self.block);
        Ok(())
    }

    /// Drops the rows of the block being buffered, `hash`, which the store already
    /// holds, and closes it. Its rows all came after the last marker, so only those are
    /// looked at; any other row among them, such as a `reorg`'s, is kept.
    fn drop_block(&mut self, hash: B256) {
        let hash = hex(&hash);
        for row in std::mem::take(&mut self.block) {
            if row.block_hash() == Some(hash.as_str()) {
                self.bytes = self.bytes.saturating_sub(row_size(&row));
            } else {
                self.rows.push(row);
            }
        }
    }

    /// Buffers one envelope's rows, as [`push`](Self::push) describes, once any block the
    /// store holds is settled.
    fn buffer(&mut self, envelope: &Envelope) -> Result<(), SinkError> {
        if let Event::Reorg(reorg) = &envelope.event
            && !reorg.orphaned_hashes.is_empty()
        {
            let hashes: BTreeSet<String> = reorg.orphaned_hashes.iter().map(hex).collect();
            let kept = |row: &Row| !row.block_hash().is_some_and(|h| hashes.contains(h));
            self.rows.retain(kept);
            self.block.retain(kept);
            self.bytes = self.rows().map(row_size).sum();
            let orphaned = self.orphaned.get_or_insert(Orphaned {
                floor: reorg.height,
                hashes: BTreeSet::new(),
            });
            orphaned.floor = orphaned.floor.min(reorg.height);
            orphaned.hashes.extend(hashes);
        }
        // Only the rows of tables this store writes are rendered.
        let (table, typed) = self.schema.tables_for(&envelope.event)?;
        if let Event::Decoded(decoded) = &envelope.event
            && typed.is_some_and(|typed| self.schema.writes(typed))
            && let Some(row) = self.schema.event_row(&envelope.chain, decoded)?
        {
            self.keep(row);
        }
        if self.schema.writes(table) {
            let row = self.schema.row(&envelope.chain, &envelope.event)?;
            self.keep(row);
        }
        Ok(())
    }

    fn keep(&mut self, row: Row) {
        self.bytes += row_size(&row);
        self.block.push(row);
    }

    /// Every buffered row, in publish order.
    pub(super) fn rows(&self) -> impl DoubleEndedIterator<Item = &Row> {
        self.rows.iter().chain(&self.block)
    }

    /// How many rows are buffered.
    pub(super) fn len(&self) -> usize {
        self.rows.len() + self.block.len()
    }

    /// The rows each table should write: the last buffered copy of each `dedupe_key`, in
    /// publish order. Every row is of the batch's one chain, so the key alone tells two
    /// copies of a row apart.
    ///
    /// A merge must not see a key twice — both SQL engines refuse to update one conflict
    /// row twice in a statement — and "last" means last published, which only the buffer
    /// knows; a staging table's physical order does not promise it.
    #[cfg(any(feature = "duckdb", feature = "postgres"))]
    pub(super) fn by_table(&self) -> HashMap<TableId, Vec<&Row>> {
        grouped(self.rows().zip(last_copies(self.rows(), self.len())))
    }

    /// [`by_table`](Self::by_table)'s rows, owned: taken out of the batch, so whoever
    /// writes them can free each as soon as it is written rather than all at the end.
    #[cfg(feature = "delta")]
    pub(super) fn take_tables(&mut self) -> HashMap<TableId, Vec<Row>> {
        let last = last_copies(self.rows(), self.len());
        let rows = std::mem::take(&mut self.rows);
        let block = std::mem::take(&mut self.block);
        grouped(rows.into_iter().chain(block).zip(last))
    }

    /// Whether there is nothing to commit. A `reorg` always buffers its own row, so a
    /// batch with deletions is never empty.
    pub(super) fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.block.is_empty()
    }

    /// Roughly how many bytes the buffered rows hold in memory.
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    /// Forgets what was buffered, once a commit has made it durable. The blocks still
    /// to reconcile are kept: ingest has not reached them yet.
    pub(super) fn clear(&mut self) {
        self.rows.clear();
        self.block.clear();
        self.orphaned = None;
        self.bytes = 0;
    }
}

/// Whether each of the `len` `rows` is the last copy of its `(table, dedupe_key)`, by
/// position: what both [`Batch::by_table`] and [`Batch::take_tables`] keep.
fn last_copies<'a>(rows: impl DoubleEndedIterator<Item = &'a Row>, len: usize) -> Vec<bool> {
    let mut last = vec![false; len];
    // Sized up front: growing a set of half a million keys rehashes it again and again.
    let mut seen = HashSet::with_capacity(len);
    for (position, row) in (0..len).rev().zip(rows.rev()) {
        last[position] = seen.insert((row.table().id, row.dedupe_key()));
    }
    last
}

/// The `rows` marked to keep, by table, each table's in the order given.
fn grouped<R: Borrow<Row>>(rows: impl Iterator<Item = (R, bool)>) -> HashMap<TableId, Vec<R>> {
    let mut tables: HashMap<TableId, Vec<R>> = HashMap::new();
    for (row, keep) in rows {
        if keep {
            tables.entry(row.borrow().table().id).or_default().push(row);
        }
    }
    tables
}

/// Roughly how many bytes `row` holds in memory: the row with its own copies of the
/// chain and key, every value's slot, and what each value holds on the heap.
fn row_size(row: &Row) -> usize {
    size_of::<Row>()
        + row.chain().len()
        + row.dedupe_key().len()
        + row.values().iter().map(value_size).sum::<usize>()
}

fn value_size(value: &Value) -> usize {
    size_of::<Value>()
        + match value {
            Value::Text(text) | Value::Document(text) => text.capacity(),
            Value::List(items) => items.iter().map(value_size).sum(),
            Value::Null
            | Value::Uint(_)
            | Value::Int(_)
            | Value::BigInt { .. }
            | Value::Bool(_)
            | Value::Timestamp(_) => 0,
        }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::B256;

    use std::collections::HashSet;
    use std::sync::Arc;

    use super::{Batch, Held};
    use crate::sink::table::{Schema, Table, TableError, TableId};
    use crate::sink::{SinkError, StoreError};
    use crate::wire::datasets::evm::{Block, Contract, Log, Transaction};
    use crate::wire::envelope::{BlockMeta, ChainId, Envelope, Event, Reorg};

    fn meta(height: u64, tag: u8) -> BlockMeta {
        BlockMeta {
            height,
            hash: B256::repeat_byte(tag),
            parent_hash: B256::ZERO,
            timestamp: height,
        }
    }

    /// A block's envelopes as ingest publishes them: its row, then its marker.
    fn block(batch: &mut Batch, meta: BlockMeta) {
        let chain = ChainId::new("base");
        let row = Event::Block(Box::new(Block {
            number: meta.height,
            hash: meta.hash,
            ..Block::default()
        }));
        for event in [row, Event::AcceptedBlock(meta)] {
            batch
                .push(&Envelope::new(chain.clone(), event))
                .expect("buffered");
        }
    }

    /// An empty batch of `schema`'s tables, for a store of `base`.
    fn on_base(schema: Arc<Schema>) -> Batch {
        Batch::new(schema, ChainId::new("base"))
    }

    fn tables(batch: &Batch) -> Vec<TableId> {
        batch.rows().map(|row| row.table().id).collect()
    }

    /// A block the store already holds, handed again with the same hash, leaves nothing
    /// to write, and the block after it is written as usual.
    #[test]
    fn a_held_block_handed_again_is_not_written() {
        let mut batch = on_base(crate::sink::fixtures::schema());
        batch.reconcile(&[meta(5, 0xa0)]).expect("named first");
        block(&mut batch, meta(5, 0xa0));
        assert!(batch.is_empty(), "{:?}", tables(&batch));
        assert!(batch.orphaned.is_none());

        block(&mut batch, meta(6, 0xa1));
        assert_eq!(
            tables(&batch),
            [Table::Block.into(), Table::AcceptedBlock.into()]
        );
    }

    /// A held height handed with another hash retracts the store's copy: a `reorgs` row,
    /// and the old hash to delete at the commit, before the new block's rows.
    #[test]
    fn a_held_block_whose_hash_changed_is_retracted() {
        let mut batch = on_base(crate::sink::fixtures::schema());
        batch.reconcile(&[meta(5, 0xa0)]).expect("named first");
        block(&mut batch, meta(5, 0xb0));
        let orphaned = batch.orphaned.as_ref().expect("the old copy is orphaned");
        assert_eq!(orphaned.floor, 5);
        assert_eq!(
            orphaned.hashes.iter().collect::<Vec<_>>(),
            [&format!("{:#x}", B256::repeat_byte(0xa0))]
        );
        assert_eq!(
            tables(&batch),
            [
                Table::Block.into(),
                Table::Reorg.into(),
                Table::AcceptedBlock.into()
            ]
        );
    }

    /// A decoded record keeps only the rows of the tables the store writes, and one whose
    /// event no table holds is refused, whether or not the store writes event tables.
    #[test]
    fn a_decoded_record_keeps_the_rows_its_store_writes() {
        let schema = crate::sink::fixtures::schema();
        let swap = crate::sink::fixtures::decoded_swap();
        let typed = schema.event_table(&swap).expect("the swap's table").id;
        let only_decoded = HashSet::from([Table::Decoded.into()]);
        for (written, expected) in [
            (HashSet::from([typed]), typed),
            (only_decoded, Table::Decoded.into()),
        ] {
            let mut batch = on_base(Arc::new(schema.select(&written)));
            let envelope =
                Envelope::new(ChainId::new("base"), Event::Decoded(Box::new(swap.clone())));
            batch.push(&envelope).expect("buffered");
            assert_eq!(tables(&batch), [expected]);

            let mut unknown = swap.clone();
            unknown.event_id = B256::repeat_byte(0xee);
            let unknown = Envelope::new(ChainId::new("base"), Event::Decoded(Box::new(unknown)));
            assert!(matches!(
                batch.push(&unknown),
                Err(SinkError::Table(TableError::UnknownEvent { .. }))
            ));
        }
    }

    /// What is still to reconcile outlives a commit: the drain commits on its own
    /// schedule, long before ingest reaches every held height.
    #[test]
    fn held_blocks_outlive_a_commit() {
        let mut batch = on_base(crate::sink::fixtures::schema());
        batch
            .reconcile(&[meta(5, 0xa0), meta(6, 0xa1)])
            .expect("named first");
        block(&mut batch, meta(5, 0xa0));
        batch.clear();
        block(&mut batch, meta(6, 0xa1));
        assert!(batch.is_empty(), "{:?}", tables(&batch));
        assert!(batch.orphaned.is_none());
    }

    /// Every row a held block publishes names it, whatever its table — the dataset
    /// tables, a decoded record's two, a discovered contract's, a derived row's — so all
    /// of them are dropped with it. Only a `reorg` belongs to no block, and one published
    /// before the block's rows is kept, as the record of a retraction.
    #[test]
    fn every_row_of_a_held_block_is_dropped() {
        let (schema, mut derived) = crate::sink::fixtures::derived();
        let mut batch = on_base(schema);
        let held = meta(5, 0xa0);
        batch.reconcile(&[held]).expect("named first");
        let mut decoded = crate::sink::fixtures::decoded_swap();
        decoded.block_number = held.height;
        decoded.block_hash = held.hash;
        derived.block_number = held.height;
        derived.block_hash = held.hash;
        let events = [
            Event::Reorg(Reorg {
                height: held.height,
                new_head_hash: held.hash,
                orphaned_hashes: vec![B256::repeat_byte(0xcc)],
            }),
            Event::Block(Box::new(Block {
                number: held.height,
                hash: held.hash,
                ..Block::default()
            })),
            Event::Transaction(Box::new(Transaction {
                block_number: held.height,
                block_hash: held.hash,
                ..Transaction::default()
            })),
            Event::Log(Box::new(Log {
                block_number: held.height,
                block_hash: held.hash,
                ..Log::default()
            })),
            Event::Decoded(Box::new(decoded)),
            Event::Contract(Box::new(Contract {
                protocol: "uniswap_v3".to_owned(),
                name: "UniswapV3Pool".to_owned(),
                address: alloy_primitives::Address::repeat_byte(0xc0),
                factory_address: alloy_primitives::Address::repeat_byte(0xf0),
                transaction_hash: B256::repeat_byte(0x11),
                transaction_index: 0,
                log_index: 0,
                block_number: held.height,
                block_hash: held.hash,
                block_timestamp: held.timestamp,
            })),
            Event::Derived(Box::new(derived)),
        ];
        for event in events {
            batch
                .push(&Envelope::new(ChainId::new("base"), event))
                .expect("buffered");
        }
        assert_eq!(batch.len(), 8, "a decoded record is two rows");
        batch
            .push(&Envelope::new(
                ChainId::new("base"),
                Event::AcceptedBlock(held),
            ))
            .expect("settled");
        assert_eq!(tables(&batch), [Table::Reorg.into()]);
        assert_eq!(
            batch.bytes,
            batch.rows().map(super::row_size).sum::<usize>()
        );
    }

    /// A `reorg` part-way through a held block drops rows from before the block as well
    /// as from it, and the block is still dropped whole at its marker.
    #[test]
    fn a_reorg_inside_a_held_block_leaves_it_droppable() {
        let mut batch = on_base(crate::sink::fixtures::schema());
        batch.reconcile(&[meta(6, 0xa1)]).expect("named first");
        block(&mut batch, meta(5, 0xa0));
        let chain = ChainId::new("base");
        for event in [
            Event::Block(Box::new(Block {
                number: 6,
                hash: B256::repeat_byte(0xa1),
                ..Block::default()
            })),
            Event::Reorg(Reorg {
                height: 5,
                new_head_hash: B256::repeat_byte(0xb0),
                orphaned_hashes: vec![B256::repeat_byte(0xa0)],
            }),
            Event::AcceptedBlock(meta(6, 0xa1)),
        ] {
            batch
                .push(&Envelope::new(chain.clone(), event))
                .expect("buffered");
        }
        assert_eq!(tables(&batch), [Table::Reorg.into()]);
    }

    /// The blocks a store holds are named before its first envelope, once: naming them
    /// later would leave the heights already handed unchecked.
    #[test]
    fn a_late_reconcile_is_refused() {
        let mut batch = on_base(crate::sink::fixtures::schema());
        block(&mut batch, meta(5, 0xa0));
        assert!(matches!(
            batch.reconcile(&[meta(6, 0xa1)]),
            Err(StoreError::LateReconcile)
        ));

        let mut batch = on_base(crate::sink::fixtures::schema());
        batch.reconcile(&[]).expect("named first");
        assert!(matches!(
            batch.reconcile(&[meta(6, 0xa1)]),
            Err(StoreError::LateReconcile)
        ));
    }

    /// A held height below the one ingest hands is never handed, so it is forgotten
    /// rather than kept for the rest of the run.
    #[test]
    fn held_heights_ingest_passes_over_are_forgotten() {
        let mut batch = on_base(crate::sink::fixtures::schema());
        batch
            .reconcile(&[meta(5, 0xa0), meta(6, 0xa1), meta(7, 0xa2)])
            .expect("named first");
        block(&mut batch, meta(6, 0xa1));
        let Held::Settling(held) = &batch.held else {
            panic!("an envelope seals the held blocks");
        };
        assert_eq!(held.keys().copied().collect::<Vec<_>>(), [7]);
        assert!(batch.is_empty(), "{:?}", tables(&batch));
    }

    /// A store holds one chain: an envelope of another is refused, not written beside
    /// the store's own, and nothing of it is buffered.
    #[test]
    fn an_envelope_of_another_chain_is_refused() {
        let mut batch = on_base(crate::sink::fixtures::schema());
        let other = Envelope::new(ChainId::new("ethereum"), Event::AcceptedBlock(meta(1, 1)));
        assert!(matches!(
            batch.push(&other),
            Err(SinkError::Store(StoreError::OtherChain { store, envelope }))
                if store == "base" && envelope == "ethereum"
        ));
        assert!(batch.is_empty());
    }
}
