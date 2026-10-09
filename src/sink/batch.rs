//! What a store has been handed since its last commit, buffered the same way by every
//! store: rows rendered from envelopes, the blocks a `reorg` retracted, and the last
//! copy of each row.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use crate::sink::SinkError;
#[cfg(feature = "delta")]
use crate::sink::table::Value;
use crate::sink::table::{Row, Schema, TableId};
use crate::wire::envelope::{Envelope, Event};

/// What a store has been handed since its last commit: the rows to write, and the
/// blocks a buffered `reorg` retracted.
///
/// A decoded record is two rows: its generic `decoded_logs` row, and its event's typed
/// row, from the run's [`Schema`].
///
/// A store holds only the canonical chain. A `reorg` orphans blocks that are either
/// already committed or still in this buffer — blocks are published in order and the
/// storage channel is FIFO, so an orphaned block can never arrive after its `reorg`.
/// [`push`](Self::push) drops the buffered ones at once, and the store deletes the
/// committed ones when it commits [`rows`](Self::rows), before writing them.
#[derive(Debug)]
pub(super) struct Batch {
    /// Rows to write, in publish order.
    pub(super) rows: Vec<Row>,
    /// Orphaned block hashes to delete from the store, as `0x` hex, by chain, with the
    /// lowest buffered `reorg` height, which none of them is below.
    pub(super) orphaned: BTreeMap<String, (u64, BTreeSet<String>)>,
    /// Roughly how many bytes `rows` hold in memory.
    #[cfg(feature = "delta")]
    bytes: usize,
    /// Every table the run writes, which renders each decoded record's typed row.
    schema: Arc<Schema>,
}

impl Batch {
    pub(super) fn new(schema: Arc<Schema>) -> Self {
        Self {
            rows: Vec::new(),
            orphaned: BTreeMap::new(),
            #[cfg(feature = "delta")]
            bytes: 0,
            schema,
        }
    }

    /// Buffers one envelope's rows. A `reorg` first drops every buffered row of the
    /// blocks it orphans and records them for deletion; its own row is kept as the
    /// record of the retraction.
    ///
    /// # Errors
    ///
    /// Returns [`SinkError::UnknownEvent`] for a decoded record no event table holds,
    /// which means it was decoded against a different catalog than the store opened with,
    /// and [`SinkError::Table`] when a row cannot be built.
    pub(super) fn push(&mut self, envelope: &Envelope) -> Result<(), SinkError> {
        if let Event::Reorg(reorg) = &envelope.event
            && !reorg.orphaned_hashes.is_empty()
        {
            let chain = envelope.chain.as_str();
            let hashes: BTreeSet<String> = reorg
                .orphaned_hashes
                .iter()
                .map(|hash| format!("{hash:#x}"))
                .collect();
            self.rows.retain(|row| {
                row.chain() != chain || !row.block_hash().is_some_and(|h| hashes.contains(h))
            });
            #[cfg(feature = "delta")]
            {
                self.bytes = self.rows.iter().map(row_size).sum();
            }
            let (height, orphaned) = self
                .orphaned
                .entry(chain.to_owned())
                .or_insert((reorg.height, BTreeSet::new()));
            *height = (*height).min(reorg.height);
            orphaned.extend(hashes);
        }
        if let Event::Decoded(decoded) = &envelope.event {
            let row = self
                .schema
                .event_row(&envelope.chain, decoded)?
                .ok_or_else(|| SinkError::UnknownEvent {
                    protocol: decoded.protocol.clone(),
                    contract: decoded.contract.clone(),
                    event: decoded.name.clone(),
                })?;
            self.keep(row);
        }
        let row = self.schema.row(&envelope.chain, &envelope.event)?;
        self.keep(row);
        Ok(())
    }

    fn keep(&mut self, row: Row) {
        #[cfg(feature = "delta")]
        {
            self.bytes += row_size(&row);
        }
        self.rows.push(row);
    }

    /// The rows each table should write: the last buffered copy of each
    /// `(chain, dedupe_key)`, in publish order.
    ///
    /// A merge must not see a key twice — both SQL engines refuse to update one conflict
    /// row twice in a statement — and "last" means last published, which only the buffer
    /// knows; a staging table's physical order does not promise it.
    pub(super) fn by_table(&self) -> HashMap<TableId, Vec<&Row>> {
        let mut seen = HashSet::new();
        let mut tables: HashMap<TableId, Vec<&Row>> = HashMap::new();
        for row in self.rows.iter().rev() {
            let id = row.table().id;
            if seen.insert((id, row.chain(), row.dedupe_key())) {
                tables.entry(id).or_default().push(row);
            }
        }
        for rows in tables.values_mut() {
            rows.reverse();
        }
        tables
    }

    /// Whether there is nothing to commit. A `reorg` always buffers its own row, so a
    /// batch with deletions is never empty.
    pub(super) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Roughly how many bytes the buffered rows hold in memory.
    #[cfg(feature = "delta")]
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    /// Forgets everything, once a commit has made it durable.
    pub(super) fn clear(&mut self) {
        self.rows.clear();
        self.orphaned.clear();
        #[cfg(feature = "delta")]
        {
            self.bytes = 0;
        }
    }
}

/// Roughly how many bytes `row` holds in memory: the row with its own copies of the
/// chain and key, every value's slot, and what each value holds on the heap.
#[cfg(feature = "delta")]
fn row_size(row: &Row) -> usize {
    size_of::<Row>()
        + row.chain().len()
        + row.dedupe_key().len()
        + row.values().iter().map(value_size).sum::<usize>()
}

#[cfg(feature = "delta")]
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
