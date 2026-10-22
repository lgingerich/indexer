//! What opening reads back, and what it repairs: the ledger's newest blocks, the
//! contracts, and the rows a stopped commit left behind (see the ledger's rules in the
//! module docs).

use std::collections::HashSet;

use alloy_primitives::B256;
use deltalake::arrow::datatypes::DataType as ArrowType;
use deltalake::datafusion::prelude::{cast, ident, lit};
use futures_util::stream::{self, StreamExt as _, TryStreamExt as _};
use tokio::time::Instant;
use tracing::{info, warn};

use super::arrow::PARTITION;
use super::expr::{at_or_above, in_texts, of_blocks, parse, texts, texts_of, uint, uints};
use super::{CONCURRENT_TABLES, DeltaSink, hex, millis};
use crate::ingest::pipeline::LEDGER_WINDOW;
use crate::sink::store_error::{InvalidStoredValue, StoreError};
use crate::sink::table::{Table, TableId};
use crate::wire::envelope::{BlockMeta, StoredContract};

/// The `contracts` columns a restore reads back.
pub(super) const CONTRACT_COLUMNS: [&str; 6] = [
    "protocol",
    "name",
    "address",
    "block_hash",
    "block_number",
    "log_index",
];

/// The `accepted_blocks` columns opening the store reads back.
pub(super) const LEDGER_COLUMNS: [&str; 4] = ["height", "hash", "parent_hash", "timestamp"];

/// The `reorgs` columns opening the store reads back.
pub(super) const REORG_COLUMNS: [&str; 3] = ["height", "dedupe_key", "orphaned_hashes"];

impl DeltaSink {
    /// The contracts discovered on this chain that the lake holds, every one canonical:
    /// one created in an orphaned block was deleted with it, and one the ledger does not
    /// name was deleted when the store opened.
    pub(super) async fn contracts(&self) -> Result<Vec<StoredContract>, StoreError> {
        let batches = self
            .lake(Table::Contract)
            .read(|frame| {
                // Ordered, so a restore reads the rows the same way every time.
                frame
                    .sort(vec![ident("dedupe_key").sort(true, false)])?
                    .select(texts_of(&CONTRACT_COLUMNS))
            })
            .await?;
        let mut contracts = Vec::new();
        for batch in &batches {
            let protocol = texts(batch, 0, "contracts.protocol")?;
            let name = texts(batch, 1, "contracts.name")?;
            let address = texts(batch, 2, "contracts.address")?;
            let block_hash = texts(batch, 3, "contracts.block_hash")?;
            let block_number = texts(batch, 4, "contracts.block_number")?;
            let log_index = texts(batch, 5, "contracts.log_index")?;
            for row in 0..batch.num_rows() {
                contracts.push(StoredContract {
                    protocol: protocol.value(row).to_owned(),
                    name: name.value(row).to_owned(),
                    address: parse("contracts.address", address.value(row))?,
                    block_hash: parse("contracts.block_hash", block_hash.value(row))?,
                    block_number: parse("contracts.block_number", block_number.value(row))?,
                    log_index: parse("contracts.log_index", log_index.value(row))?,
                });
            }
        }
        Ok(contracts)
    }

    /// The newest [`LEDGER_WINDOW`] accepted blocks, oldest first.
    ///
    /// Read from the ledger's newest two partitions alone, which hold at least the window
    /// (a partition is never narrower), so a restart does not scan every file the
    /// ledger ever wrote.
    pub(super) async fn read_ledger(&self) -> Result<Vec<BlockMeta>, StoreError> {
        let lake = self.lake(Table::AcceptedBlock);
        let newest = lake.newest_partition()?;
        let [height, hash, parent_hash, timestamp] = LEDGER_COLUMNS;
        let batches = lake
            .read(|frame| {
                let frame = match newest {
                    Some(newest) => frame.filter(ident(PARTITION).gt_eq(lit(newest - 1)))?,
                    None => frame,
                };
                let seconds = cast(
                    cast(ident(timestamp), ArrowType::Int64) / lit(1_000_000_i64),
                    ArrowType::UInt64,
                );
                frame
                    .sort(vec![ident(height).sort(false, false)])?
                    .limit(0, Some(LEDGER_WINDOW))?
                    .select(vec![
                        cast(ident(height), ArrowType::UInt64),
                        cast(ident(hash), ArrowType::Utf8),
                        cast(ident(parent_hash), ArrowType::Utf8),
                        seconds,
                    ])
            })
            .await?;
        let mut ledger = Vec::new();
        for batch in &batches {
            let heights = uints(batch, 0, "accepted_blocks.height")?;
            let hashes = texts(batch, 1, "accepted_blocks.hash")?;
            let parents = texts(batch, 2, "accepted_blocks.parent_hash")?;
            let times = uints(batch, 3, "accepted_blocks.timestamp")?;
            for row in 0..batch.num_rows() {
                ledger.push(BlockMeta {
                    height: heights.value(row),
                    hash: parse("accepted_blocks.hash", hashes.value(row))?,
                    parent_hash: parse("accepted_blocks.parent_hash", parents.value(row))?,
                    timestamp: times.value(row),
                });
            }
        }
        ledger.sort_by_key(|block| block.height);
        Ok(ledger)
    }

    /// Deletes the rows of blocks `ledger` does not name, at or above its oldest height:
    /// what a commit that stopped part-way appended before its ledger entry. With no
    /// ledger at all, every block row is such a row.
    ///
    /// Every block table is read, so the reads overlap; a stray row is rare, and its
    /// delete runs once they are done.
    ///
    /// Returns the contracts the lake keeps once repaired, for a restore: they are read
    /// beside the scan, and again only if the repair deleted some of them.
    pub(super) async fn repair(
        &mut self,
        ledger: &[BlockMeta],
    ) -> Result<Vec<StoredContract>, StoreError> {
        let scanned = Instant::now();
        let named: HashSet<String> = ledger.iter().map(|block| hex(&block.hash)).collect();
        let floor = ledger.first().map(|block| block.height);
        let ledger_id = TableId::Dataset(Table::AcceptedBlock);
        let scan = stream::iter(self.lakes.iter().enumerate())
            .filter(|(_, lake)| std::future::ready(lake.def.id != ledger_id))
            .filter_map(|(position, lake)| {
                std::future::ready(
                    lake.block_columns()
                        .map(|columns| (position, lake, columns)),
                )
            })
            .map(|(position, lake, (hash, number))| {
                let named = &named;
                async move {
                    let batches = lake
                        .read(|frame| {
                            let frame = match floor {
                                Some(floor) => {
                                    frame.filter(at_or_above(number, floor, lake.width))?
                                }
                                None => frame,
                            };
                            frame.select(texts_of(&[hash]))?.distinct()
                        })
                        .await?;
                    let mut stray = Vec::new();
                    for batch in &batches {
                        let hashes = texts(batch, 0, "block_hash")?;
                        stray.extend(
                            hashes
                                .iter()
                                .flatten()
                                .filter(|block| !named.contains(*block))
                                .map(str::to_owned),
                        );
                    }
                    Ok::<_, StoreError>((position, stray))
                }
            })
            .boxed()
            .buffered(CONCURRENT_TABLES)
            .try_filter(|(_, stray)| std::future::ready(!stray.is_empty()))
            .try_collect::<Vec<(usize, Vec<String>)>>();
        let (strays, mut contracts) = tokio::try_join!(scan, self.contracts())?;
        let scanned = scanned.elapsed();
        let contract_position = self.position(Table::Contract);
        let contracts_repaired = strays
            .iter()
            .any(|(position, _)| *position == contract_position);
        let deleted = Instant::now();
        let tables = strays.len();
        for (position, stray) in strays {
            let lake = &mut self.lakes[position];
            warn!(
                table = lake.name(),
                blocks = stray.len(),
                "deleting rows a stopped commit left behind"
            );
            let Some(columns) = lake.block_columns() else {
                continue;
            };
            let predicate = of_blocks(columns, &stray, floor, lake.width);
            lake.delete(&self.writer, predicate).await?;
        }
        if contracts_repaired {
            contracts = self.contracts().await?;
        }
        let deleted = deleted.elapsed();
        let reorgs = Instant::now();
        self.repair_reorgs(ledger).await?;
        info!(
            stray_tables = tables,
            scan_ms = millis(scanned),
            delete_ms = millis(deleted),
            reorgs_ms = millis(reorgs.elapsed()),
            "lake checked for rows a stopped commit left behind"
        );
        Ok(contracts)
    }

    /// Deletes the `reorgs` rows of retractions that never finished: ones whose orphaned
    /// blocks `ledger` still names. A commit writes a retraction's row before it takes the
    /// orphans out of the ledger, so a stop in between leaves such a row; the restart
    /// resumes on top of the orphans, detects the reorg again, and records it once.
    ///
    /// A block orphaned and later canonical again is named by the ledger too, so its
    /// first retraction's row is deleted here as well. That costs an audit row, never a
    /// block's data.
    async fn repair_reorgs(&mut self, ledger: &[BlockMeta]) -> Result<(), StoreError> {
        let Some(floor) = ledger.first().map(|block| block.height) else {
            return Ok(());
        };
        let named: HashSet<B256> = ledger.iter().map(|block| block.hash).collect();
        let position = self.positions[&TableId::Dataset(Table::Reorg)];
        let [height, dedupe_key, orphaned_hashes] = REORG_COLUMNS;
        // A retraction's height is its lowest orphan's, so an unfinished one is at or
        // above the ledger's oldest block.
        let batches = self.lakes[position]
            .read(|frame| {
                frame
                    .filter(ident(height).gt_eq(uint(floor)))?
                    .select(texts_of(&[dedupe_key, orphaned_hashes]))
            })
            .await?;
        let mut unfinished = Vec::new();
        for batch in &batches {
            let keys = texts(batch, 0, "reorgs.dedupe_key")?;
            let orphans = texts(batch, 1, "reorgs.orphaned_hashes")?;
            for row in 0..batch.num_rows() {
                let orphans: Vec<B256> =
                    serde_json::from_str(orphans.value(row)).map_err(|_| {
                        InvalidStoredValue::new("reorgs.orphaned_hashes", orphans.value(row))
                    })?;
                if orphans.iter().any(|hash| named.contains(hash)) {
                    unfinished.push(keys.value(row).to_owned());
                }
            }
        }
        if unfinished.is_empty() {
            return Ok(());
        }
        warn!(
            rows = unfinished.len(),
            "deleting retractions a stopped commit left unfinished"
        );
        self.lakes[position]
            .delete(&self.writer, in_texts(dedupe_key, &unfinished))
            .await
    }
}
