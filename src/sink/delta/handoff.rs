//! The contracts a backfill hands the running indexer: a small Delta table,
//! `_backfill_contracts`, beside the chain's tables, with the `contracts` table's
//! columns.
//!
//! A backfill that discovers contracts below the running indexer's restart appends them
//! here under its lease ([`HandoffQueue`]). The running indexer watches the lease, and
//! reads the queue while a backfill holds it and once when it is released, and once at
//! startup for what it missed while stopped ([`HandoffWatch`]). Each contract it reads
//! joins its decoder at a block it records as a `handoffs` row, from which the backfill
//! knows where its own decoding of the contract may stop (see `backfill`).
//!
//! The watch costs the running indexer one listing of the lease's log a minute while no
//! backfill runs, on a task of its own: ingest never waits on it.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::Address;
use deltalake::arrow::datatypes::DataType as ArrowType;
use deltalake::datafusion::prelude::{cast, ident};
use deltalake::parquet::basic::{Compression, ZstdLevel};
use deltalake::parquet::file::properties::WriterProperties;
use tokio::sync::{Semaphore, mpsc};
use tracing::{info, warn};

use super::expr::{seconds, texts, texts_of, uints};
use super::lake::{CONCURRENT_CHECKPOINTS, Lake};
use super::lock::LeaseWatch;
use super::open::{Missing, Storage, open_existing, open_table};
use super::settings::DeltaSettings;
use crate::secret::Secret;
use crate::sink::store_error::{InvalidStoredValue, Operation, StoreError};
use crate::sink::table::{HANDOFF_COLUMNS, Schema, Table};
use crate::wire::datasets::evm::Contract;
use crate::wire::envelope::{ChainId, Event, Handoff};

/// The queue's table, under `<uri>/<chain>/`.
const TABLE: &str = "_backfill_contracts";

/// How often the running indexer asks whether a backfill holds the lake.
const IDLE_POLL: Duration = if cfg!(test) {
    Duration::from_millis(50)
} else {
    Duration::from_mins(1)
};

/// How often it reads the queue while one does.
const ACTIVE_POLL: Duration = if cfg!(test) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(10)
};

/// The queue, as a backfill writes it and the running indexer reads it.
#[derive(Debug)]
pub struct HandoffQueue {
    lake: Lake,
    writer: WriterProperties,
}

impl HandoffQueue {
    /// Opens the queue under `<uri>/<chain>/`, reached with the object store options
    /// `storage`, creating it `width` heights to a partition when there is none.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when the table cannot be opened or created, and
    /// [`StoreError::Drift`] when it does not match the `contracts` table's columns.
    pub async fn open(
        uri: &str,
        storage: &BTreeMap<String, Secret>,
        schema: &Schema,
        chain: &str,
        width: u64,
    ) -> Result<Self, StoreError> {
        let root = uri.trim_end_matches('/');
        let storage = Storage::new(root, storage)?;
        let uri = format!("{root}/{chain}/{TABLE}");
        let checkpoints = Arc::new(Semaphore::new(CONCURRENT_CHECKPOINTS));
        let missing = Missing::Create { width };
        let def = schema.dataset(Table::Contract);
        Ok(Self::of(
            open_table(&storage, uri, def, missing, checkpoints).await?,
        ))
    }

    /// The queue in `lake`.
    fn of(lake: Lake) -> Self {
        Self {
            lake,
            writer: WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::default()))
                .build(),
        }
    }

    /// Appends the `contracts` of `chain` the queue does not hold yet, by key, as one
    /// commit, and returns how many it appended.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when the queue cannot be read or written, and
    /// [`StoreError::Table`] when a row cannot be built.
    pub async fn append(
        &mut self,
        schema: &Schema,
        chain: &ChainId,
        contracts: &[Contract],
    ) -> Result<usize, StoreError> {
        let held: HashSet<String> = self
            .read()
            .await?
            .iter()
            .map(Contract::dedupe_key)
            .collect();
        let rows = contracts
            .iter()
            .filter(|contract| !held.contains(&contract.dedupe_key()))
            .map(|contract| schema.row(chain, &Event::Contract(Box::new(contract.clone()))))
            .collect::<Result<Vec<_>, _>>()?;
        if rows.is_empty() {
            return Ok(0);
        }
        let batch = self.lake.record_batch(&rows.iter().collect::<Vec<_>>())?;
        // Not `committed`: a backfill starts no checkpoints.
        self.lake.table = self.lake.append(&self.writer, vec![batch]).await?;
        Ok(rows.len())
    }

    /// Every contract the queue holds, by key.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Engine`] when the read fails and [`StoreError::Restore`] when
    /// a stored value does not parse.
    pub async fn read(&self) -> Result<Vec<Contract>, StoreError> {
        const TEXTS: [&str; 6] = [
            "protocol",
            "name",
            "address",
            "factory_address",
            "transaction_hash",
            "block_hash",
        ];
        let batches = self
            .lake
            .read(|frame| {
                let mut columns = texts_of(&TEXTS);
                columns.extend(
                    ["transaction_index", "log_index", "block_number"]
                        .map(|column| cast(ident(column), ArrowType::UInt64).alias(column)),
                );
                columns.push(seconds("block_timestamp"));
                frame
                    .sort(vec![ident("dedupe_key").sort(true, false)])?
                    .select(columns)
            })
            .await?;
        let mut contracts = Vec::new();
        for batch in &batches {
            let protocol = texts(batch, 0, "_backfill_contracts.protocol")?;
            let name = texts(batch, 1, "_backfill_contracts.name")?;
            let address = texts(batch, 2, "_backfill_contracts.address")?;
            let factory = texts(batch, 3, "_backfill_contracts.factory_address")?;
            let transaction = texts(batch, 4, "_backfill_contracts.transaction_hash")?;
            let block_hash = texts(batch, 5, "_backfill_contracts.block_hash")?;
            let transaction_index = uints(batch, 6, "_backfill_contracts.transaction_index")?;
            let log_index = uints(batch, 7, "_backfill_contracts.log_index")?;
            let block_number = uints(batch, 8, "_backfill_contracts.block_number")?;
            let block_timestamp = uints(batch, 9, "_backfill_contracts.block_timestamp")?;
            for row in 0..batch.num_rows() {
                contracts.push(Contract {
                    protocol: protocol.value(row).to_owned(),
                    name: name.value(row).to_owned(),
                    address: InvalidStoredValue::parse(
                        "_backfill_contracts.address",
                        address.value(row),
                    )?,
                    factory_address: InvalidStoredValue::parse(
                        "_backfill_contracts.factory_address",
                        factory.value(row),
                    )?,
                    transaction_hash: InvalidStoredValue::parse(
                        "_backfill_contracts.transaction_hash",
                        transaction.value(row),
                    )?,
                    transaction_index: transaction_index.value(row),
                    log_index: log_index.value(row),
                    block_number: block_number.value(row),
                    block_hash: InvalidStoredValue::parse(
                        "_backfill_contracts.block_hash",
                        block_hash.value(row),
                    )?,
                    block_timestamp: block_timestamp.value(row),
                });
            }
        }
        Ok(contracts)
    }

    /// Reads the table again, for commits made since it was opened or last read.
    async fn refresh(&mut self) -> Result<bool, StoreError> {
        let before = self.lake.table.version();
        self.lake
            .table
            .update_state()
            .await
            .map_err(StoreError::engine(Operation::Read, Some(TABLE)))?;
        Ok(self.lake.table.version() != before)
    }
}

/// The `handoffs` the running indexer recorded in the lake under `<uri>/<chain>/`,
/// reached with the object store options `storage`: each contract a backfill handed it,
/// with the first block it decoded with it. None before a run that records them opened
/// the lake.
///
/// # Errors
///
/// Returns [`StoreError::Engine`] when the table cannot be read, [`StoreError::Drift`]
/// when it does not match its definition, and [`StoreError::Restore`] when a stored
/// value does not parse.
pub async fn recorded(
    uri: &str,
    storage: &BTreeMap<String, Secret>,
    schema: &Schema,
    chain: &str,
) -> Result<Vec<Handoff>, StoreError> {
    let root = uri.trim_end_matches('/');
    let storage = Storage::new(root, storage)?;
    let def = schema.dataset(Table::Handoff);
    let uri = format!("{root}/{chain}/{}", def.name);
    match open_existing(&storage, uri, def).await? {
        Some(lake) => handoffs(&lake).await,
        None => Ok(Vec::new()),
    }
}

/// The rows of `lake`, a `handoffs` table.
pub(super) async fn handoffs(lake: &Lake) -> Result<Vec<Handoff>, StoreError> {
    let [protocol, name, address, number, hash, timestamp] = HANDOFF_COLUMNS;
    let batches = lake
        .read(|frame| {
            let mut columns = texts_of(&[protocol, name, address, hash]);
            columns.push(cast(ident(number), ArrowType::UInt64).alias(number));
            columns.push(seconds(timestamp));
            frame
                .sort(vec![ident("dedupe_key").sort(true, false)])?
                .select(columns)
        })
        .await?;
    let mut handoffs = Vec::new();
    for batch in &batches {
        let protocols = texts(batch, 0, "handoffs.protocol")?;
        let names = texts(batch, 1, "handoffs.name")?;
        let addresses = texts(batch, 2, "handoffs.address")?;
        let hashes = texts(batch, 3, "handoffs.block_hash")?;
        let numbers = uints(batch, 4, "handoffs.block_number")?;
        let timestamps = uints(batch, 5, "handoffs.block_timestamp")?;
        for row in 0..batch.num_rows() {
            handoffs.push(Handoff {
                protocol: protocols.value(row).to_owned(),
                name: names.value(row).to_owned(),
                address: InvalidStoredValue::parse("handoffs.address", addresses.value(row))?,
                block_number: numbers.value(row),
                block_hash: InvalidStoredValue::parse("handoffs.block_hash", hashes.value(row))?,
                block_timestamp: timestamps.value(row),
            });
        }
    }
    Ok(handoffs)
}

/// The running indexer's watch on the queue: reads it once at startup, then asks every
/// minute whether a backfill holds the lake, reading the queue every ten seconds while
/// one does and once more when it lets go, and hands each contract it has not handed
/// before to the decode stage.
#[derive(Debug, Clone)]
pub struct HandoffWatch {
    uri: String,
    storage: BTreeMap<String, Secret>,
}

impl HandoffWatch {
    /// Watches the lake `settings` names.
    #[must_use]
    pub fn new(settings: &DeltaSettings) -> Self {
        Self {
            uri: settings.uri.trim_end_matches('/').to_owned(),
            storage: settings.storage.clone(),
        }
    }

    /// Watches `chain`'s queue until `handed` is dropped, sending what it reads there.
    /// A failed check is logged and tried again at the next: the watch never stops the
    /// run, and what it missed it reads at the next check.
    pub async fn run(
        self,
        schema: Arc<Schema>,
        chain: ChainId,
        handed: mpsc::Sender<Vec<Contract>>,
    ) {
        let storage = match Storage::new(&self.uri, &self.storage) {
            Ok(storage) => storage,
            Err(error) => {
                warn!(%chain, %error, "backfill handoff watch cannot reach the lake; not watching");
                return;
            }
        };
        let mut lease = LeaseWatch::new(&self.uri, chain.as_str());
        let mut queue: Option<HandoffQueue> = None;
        let mut sent: HashSet<Address> = HashSet::new();
        // At startup, for what was queued while the run was stopped.
        let mut read = true;
        let mut held = false;
        loop {
            if read {
                match self.read(&storage, &schema, &chain, &mut queue).await {
                    Ok(contracts) => {
                        let new: Vec<Contract> = contracts
                            .into_iter()
                            .filter(|contract| sent.insert(contract.address))
                            .collect();
                        if !new.is_empty() {
                            info!(%chain, contracts = new.len(), "contracts handed over by a backfill");
                            if handed.send(new).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(error) => {
                        warn!(%chain, %error, "backfill handoff queue unreadable; trying again");
                    }
                }
            }
            tokio::select! {
                () = handed.closed() => return,
                () = tokio::time::sleep(if held { ACTIVE_POLL } else { IDLE_POLL }) => {}
            }
            let holds = match lease.held(&storage).await {
                Ok(holds) => holds,
                Err(error) => {
                    warn!(%chain, %error, "backfill lease unreadable; trying again");
                    held
                }
            };
            // Once more after a release, for what was queued since the last read.
            read = holds || held;
            held = holds;
        }
    }

    /// The queue's contracts, opening it once it exists, and reading it only when it
    /// changed since the last read.
    async fn read(
        &self,
        storage: &Storage,
        schema: &Schema,
        chain: &ChainId,
        queue: &mut Option<HandoffQueue>,
    ) -> Result<Vec<Contract>, StoreError> {
        if let Some(queue) = queue {
            if !queue.refresh().await? {
                return Ok(Vec::new());
            }
            return queue.read().await;
        }
        let uri = format!("{}/{chain}/{TABLE}", self.uri);
        let Some(lake) = open_existing(storage, uri, schema.dataset(Table::Contract)).await? else {
            return Ok(Vec::new());
        };
        let opened = HandoffQueue::of(lake);
        let contracts = opened.read().await?;
        *queue = Some(opened);
        Ok(contracts)
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use alloy_primitives::{Address, B256};
    use tokio::sync::mpsc;

    use super::{HandoffQueue, HandoffWatch, LeaseWatch, Storage};
    use crate::sink::delta::{DeltaSettings, RunLock};
    use crate::sink::table::Schema;
    use crate::wire::datasets::evm::Contract;
    use crate::wire::envelope::ChainId;

    const CHAIN: &str = "base";

    /// A fresh directory for one test's lake, removed when the guard drops.
    struct Dir(PathBuf);

    impl Dir {
        fn new() -> Self {
            static NEXT: AtomicU32 = AtomicU32::new(0);
            Self(std::env::temp_dir().join(format!(
                "indexer-handoff-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )))
        }

        fn uri(&self) -> String {
            self.0.display().to_string()
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn pool(byte: u8) -> Contract {
        Contract {
            protocol: "uniswap_v3".to_owned(),
            name: "UniswapV3Pool".to_owned(),
            address: Address::repeat_byte(byte),
            factory_address: Address::repeat_byte(0xfa),
            transaction_hash: B256::repeat_byte(byte),
            transaction_index: 1,
            log_index: 2,
            block_number: 3,
            block_hash: B256::repeat_byte(0x0b),
            block_timestamp: 1_700_000_000,
        }
    }

    /// The queue reads back every column of what was appended, once per key however
    /// often it is appended.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_queue_holds_each_contract_once() {
        let dir = Dir::new();
        let schema = Schema::new().expect("the dataset tables");
        let chain = ChainId::new(CHAIN);
        let mut queue = HandoffQueue::open(&dir.uri(), &BTreeMap::new(), &schema, CHAIN, 100_000)
            .await
            .expect("opens");
        assert_eq!(
            queue
                .append(&schema, &chain, &[pool(1)])
                .await
                .expect("appended"),
            1
        );
        assert_eq!(
            queue
                .append(&schema, &chain, &[pool(1), pool(2)])
                .await
                .expect("appended"),
            1
        );
        assert_eq!(queue.read().await.expect("read"), [pool(1), pool(2)]);
    }

    /// No backfill holds a lake whose lease was never taken, or was released; one does
    /// while it holds the lease.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_backfill_holds_the_lake_only_while_it_holds_the_lease() {
        let dir = Dir::new();
        let storage = Storage::new(&dir.uri(), &BTreeMap::new()).expect("storage");
        let mut lease = LeaseWatch::new(&dir.uri(), CHAIN);
        assert!(!lease.held(&storage).await.expect("checked"));
        let lock = RunLock::acquire(&dir.uri(), &BTreeMap::new(), CHAIN, "run", "uniswap_v3")
            .await
            .expect("acquired");
        assert!(lease.held(&storage).await.expect("checked"));
        assert!(
            lease.held(&storage).await.expect("checked"),
            "unchanged, still held"
        );
        lock.release().await.expect("released");
        assert!(!lease.held(&storage).await.expect("checked"));
    }

    /// The next batch the watch hands over, within ten seconds.
    async fn received(handing: &mut mpsc::Receiver<Vec<Contract>>) -> Vec<Contract> {
        tokio::time::timeout(Duration::from_secs(10), handing.recv())
            .await
            .expect("in time")
            .expect("a batch")
    }

    /// The watch hands over what was queued before the run started, then what a backfill
    /// queues while it holds the lake, each contract once, and ends when the decode stage
    /// drops its end of the channel.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_watch_hands_over_what_backfills_queue() {
        let dir = Dir::new();
        let schema = Arc::new(Schema::new().expect("the dataset tables"));
        let chain = ChainId::new(CHAIN);
        let mut queue = HandoffQueue::open(&dir.uri(), &BTreeMap::new(), &schema, CHAIN, 100_000)
            .await
            .expect("opens");
        queue
            .append(&schema, &chain, &[pool(1)])
            .await
            .expect("appended");

        let settings: DeltaSettings =
            toml::from_str(&format!("tables = ['*']\nuri = '{}'", dir.uri())).expect("settings");
        let (handed, mut handing) = mpsc::channel(4);
        let watching = tokio::spawn(HandoffWatch::new(&settings).run(
            Arc::clone(&schema),
            chain.clone(),
            handed,
        ));
        assert_eq!(
            received(&mut handing).await,
            [pool(1)],
            "queued while stopped"
        );

        let lock = RunLock::acquire(&dir.uri(), &BTreeMap::new(), CHAIN, "run", "uniswap_v3")
            .await
            .expect("acquired");
        queue
            .append(&schema, &chain, &[pool(1), pool(2)])
            .await
            .expect("appended");
        assert_eq!(received(&mut handing).await, [pool(2)], "only what is new");
        lock.release().await.expect("released");

        drop(handing);
        tokio::time::timeout(Duration::from_secs(10), watching)
            .await
            .expect("the watch ends")
            .expect("joined");
    }
}
