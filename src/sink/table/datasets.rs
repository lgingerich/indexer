//! The dataset tables, each declared once as columns read from its record's fields.

use std::sync::Arc;

use crate::wire::envelope::{
    Block, BlockMeta, ChainId, Contract, Decoded, Event, Log, Reorg, Transaction,
};

use super::{
    Cell, Column, Row, Table, TableBuilder, TableDef, TableError, TableId, Timestamp, Value,
};

/// Reads one column's value from a record.
type Read<R> = Box<dyn Fn(&R) -> Result<Value, serde_json::Error> + Send + Sync>;

/// A dataset table: its definition, and how each column reads a record of type `R`.
struct Dataset<R> {
    def: Arc<TableDef>,
    reads: Vec<Read<R>>,
}

impl<R> Dataset<R> {
    /// The record's row, keyed by `chain` and the record's `key`.
    fn row(&self, record: &R, chain: &ChainId, key: String) -> Result<Row, TableError> {
        let values = self
            .reads
            .iter()
            .map(|read| read(record))
            .collect::<Result<_, _>>()
            .map_err(|source| TableError::Json {
                table: self.def.name.clone(),
                source,
            })?;
        Row::new(&self.def, values, chain.as_str(), key)
    }
}

/// Declares a dataset table one field at a time. A column's type and nullability are the
/// field's own, through [`Cell`].
struct DatasetBuilder<R> {
    table: TableBuilder,
    reads: Vec<Read<R>>,
}

impl<R: 'static> DatasetBuilder<R> {
    fn new(table: Table) -> Self {
        Self {
            table: TableDef::builder(TableId::Dataset(table), table.name()),
            reads: Vec::new(),
        }
    }

    /// A column holding a field read by value: a number, a hash, an address.
    fn col<C: Cell + 'static>(mut self, name: &'static str, read: fn(&R) -> C) -> Self {
        self.table = self.table.column(Column::new(name, C::TYPE, C::NULLABLE));
        self.reads
            .push(Box::new(move |record| read(record).value()));
        self
    }

    /// A column holding a field read by reference: bytes, text, a list.
    fn col_ref<C: Cell + 'static>(mut self, name: &'static str, read: fn(&R) -> &C) -> Self {
        self.table = self.table.column(Column::new(name, C::TYPE, C::NULLABLE));
        self.reads
            .push(Box::new(move |record| read(record).value()));
        self
    }

    fn reorg_by(mut self, hash: &'static str, number: &'static str) -> Self {
        self.table = self.table.reorg_by(hash, number);
        self
    }

    fn build(self) -> Result<Dataset<R>, TableError> {
        Ok(Dataset {
            def: Arc::new(self.table.build()?),
            reads: self.reads,
        })
    }
}

fn blocks() -> Result<Dataset<Block>, TableError> {
    DatasetBuilder::<Block>::new(Table::Block)
        .col("number", |b| b.number)
        .col("hash", |b| b.hash)
        .col("parent_hash", |b| b.parent_hash)
        .col("timestamp", |b| Timestamp(b.timestamp))
        .col("nonce", |b| b.nonce)
        .col("ommers_hash", |b| b.ommers_hash)
        .col("transactions_root", |b| b.transactions_root)
        .col("state_root", |b| b.state_root)
        .col("receipts_root", |b| b.receipts_root)
        .col("withdrawals_root", |b| b.withdrawals_root)
        .col("logs_bloom", |b| b.logs_bloom)
        .col("miner", |b| b.miner)
        .col("difficulty", |b| b.difficulty)
        .col("total_difficulty", |b| b.total_difficulty)
        .col("size", |b| b.size)
        .col_ref("extra_data", |b| &b.extra_data)
        .col("gas_limit", |b| b.gas_limit)
        .col("gas_used", |b| b.gas_used)
        .col("transaction_count", |b| b.transaction_count)
        .col("base_fee_per_gas", |b| b.base_fee_per_gas)
        .col("blob_gas_used", |b| b.blob_gas_used)
        .col("excess_blob_gas", |b| b.excess_blob_gas)
        .col("parent_beacon_block_root", |b| b.parent_beacon_block_root)
        .col_ref("ommers", |b| &b.ommers)
        .col_ref("transaction_hashes", |b| &b.transaction_hashes)
        .reorg_by("hash", "number")
        .build()
}

fn transactions() -> Result<Dataset<Transaction>, TableError> {
    DatasetBuilder::<Transaction>::new(Table::Transaction)
        .col("hash", |t| t.hash)
        .col("nonce", |t| t.nonce)
        .col("transaction_index", |t| t.transaction_index)
        .col("from_address", |t| t.from)
        .col("to_address", |t| t.to)
        .col("value", |t| t.value)
        .col("gas", |t| t.gas)
        .col("gas_price", |t| t.gas_price)
        .col("max_fee_per_gas", |t| t.max_fee_per_gas)
        .col("max_priority_fee_per_gas", |t| t.max_priority_fee_per_gas)
        .col("max_fee_per_blob_gas", |t| t.max_fee_per_blob_gas)
        .col_ref("input", |t| &t.input)
        .col("transaction_type", |t| t.transaction_type)
        .col("chain_id", |t| t.chain_id)
        .col_ref("access_list", |t| &t.access_list)
        .col_ref("blob_versioned_hashes", |t| &t.blob_versioned_hashes)
        .col_ref("authorization_list", |t| &t.authorization_list)
        .col("receipt_status", |t| t.receipt_status)
        .col("receipt_gas_used", |t| t.receipt_gas_used)
        .col("receipt_cumulative_gas_used", |t| {
            t.receipt_cumulative_gas_used
        })
        .col("receipt_effective_gas_price", |t| {
            t.receipt_effective_gas_price
        })
        .col("receipt_contract_address", |t| t.receipt_contract_address)
        .col("receipt_logs_bloom", |t| t.receipt_logs_bloom)
        .col("receipt_blob_gas_used", |t| t.receipt_blob_gas_used)
        .col("receipt_blob_gas_price", |t| t.receipt_blob_gas_price)
        .col("log_count", |t| t.log_count)
        .col("block_timestamp", |t| Timestamp(t.block_timestamp))
        .col("block_number", |t| t.block_number)
        .col("block_hash", |t| t.block_hash)
        .reorg_by("block_hash", "block_number")
        .build()
}

fn logs() -> Result<Dataset<Log>, TableError> {
    DatasetBuilder::<Log>::new(Table::Log)
        .col("log_index", |l| l.log_index)
        .col("transaction_hash", |l| l.transaction_hash)
        .col("transaction_index", |l| l.transaction_index)
        .col("address", |l| l.address)
        .col("topic0", |l| l.topic0)
        .col("topic1", |l| l.topic1)
        .col("topic2", |l| l.topic2)
        .col("topic3", |l| l.topic3)
        .col_ref("data", |l| &l.data)
        .col("removed", |l| l.removed)
        .col("block_number", |l| l.block_number)
        .col("block_hash", |l| l.block_hash)
        .col("block_timestamp", |l| Timestamp(l.block_timestamp))
        .reorg_by("block_hash", "block_number")
        .build()
}

/// What identifies a decoded record is typed columns, so a store keys, joins, and filters
/// on them without parsing; the arguments, which vary per event, are documents in their
/// raw ABI form. Each event's own table has them typed.
fn decoded_logs() -> Result<Dataset<Decoded>, TableError> {
    DatasetBuilder::<Decoded>::new(Table::Decoded)
        .col_ref("name", |d| &d.name)
        .col("address", |d| d.address)
        .col_ref("protocol", |d| &d.protocol)
        .col_ref("contract", |d| &d.contract)
        .col("selector", |d| d.selector)
        .col("event_id", |d| d.event_id)
        .col_ref("signature", |d| &d.signature)
        .col("anonymous", |d| d.anonymous)
        .col("transaction_hash", |d| d.transaction_hash)
        .col("transaction_index", |d| d.transaction_index)
        .col("log_index", |d| d.log_index)
        .col_ref("indexed", |d| &d.indexed)
        .col_ref("body", |d| &d.body)
        .col("block_number", |d| d.block_number)
        .col("block_hash", |d| d.block_hash)
        .col("block_timestamp", |d| Timestamp(d.block_timestamp))
        .reorg_by("block_hash", "block_number")
        .build()
}

/// The provenance columns of Allium's `dex.pools`, generalized past pools: what was
/// created, by which factory, and where its creation log sits.
fn contracts() -> Result<Dataset<Contract>, TableError> {
    DatasetBuilder::<Contract>::new(Table::Contract)
        .col_ref("protocol", |c| &c.protocol)
        .col_ref("name", |c| &c.name)
        .col("address", |c| c.address)
        .col("factory_address", |c| c.factory_address)
        .col("transaction_hash", |c| c.transaction_hash)
        .col("transaction_index", |c| c.transaction_index)
        .col("log_index", |c| c.log_index)
        .col("block_number", |c| c.block_number)
        .col("block_hash", |c| c.block_hash)
        .col("block_timestamp", |c| Timestamp(c.block_timestamp))
        .reorg_by("block_hash", "block_number")
        .build()
}

/// A reorg belongs to no block: its rows are the record of what was retracted.
fn reorgs() -> Result<Dataset<Reorg>, TableError> {
    DatasetBuilder::<Reorg>::new(Table::Reorg)
        .col("height", |r| r.height)
        .col("new_head_hash", |r| r.new_head_hash)
        .col_ref("orphaned_hashes", |r| &r.orphaned_hashes)
        .build()
}

/// The identity and linkage a restart needs, and nothing else.
fn accepted_blocks() -> Result<Dataset<BlockMeta>, TableError> {
    DatasetBuilder::<BlockMeta>::new(Table::AcceptedBlock)
        .col("height", |a| a.height)
        .col("hash", |a| a.hash)
        .col("parent_hash", |a| a.parent_hash)
        .col("timestamp", |a| Timestamp(a.timestamp))
        .reorg_by("hash", "height")
        .build()
}

/// Every dataset table, declared once per run.
pub(super) struct DatasetTables {
    blocks: Dataset<Block>,
    transactions: Dataset<Transaction>,
    logs: Dataset<Log>,
    decoded_logs: Dataset<Decoded>,
    contracts: Dataset<Contract>,
    reorgs: Dataset<Reorg>,
    accepted_blocks: Dataset<BlockMeta>,
}

impl std::fmt::Debug for DatasetTables {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatasetTables").finish_non_exhaustive()
    }
}

impl DatasetTables {
    /// Declares every dataset table.
    pub(super) fn new() -> Result<Self, TableError> {
        Ok(Self {
            blocks: blocks()?,
            transactions: transactions()?,
            logs: logs()?,
            decoded_logs: decoded_logs()?,
            contracts: contracts()?,
            reorgs: reorgs()?,
            accepted_blocks: accepted_blocks()?,
        })
    }

    /// A dataset table's definition.
    pub(super) const fn def(&self, table: Table) -> &Arc<TableDef> {
        match table {
            Table::Block => &self.blocks.def,
            Table::Transaction => &self.transactions.def,
            Table::Log => &self.logs.def,
            Table::Decoded => &self.decoded_logs.def,
            Table::Contract => &self.contracts.def,
            Table::Reorg => &self.reorgs.def,
            Table::AcceptedBlock => &self.accepted_blocks.def,
        }
    }

    /// An event's row in its dataset table.
    pub(super) fn row(&self, chain: &ChainId, event: &Event) -> Result<Row, TableError> {
        let key = event.dedupe_key();
        match event {
            Event::Block(b) => self.blocks.row(b, chain, key),
            Event::Transaction(t) => self.transactions.row(t, chain, key),
            Event::Log(l) => self.logs.row(l, chain, key),
            Event::Decoded(d) => self.decoded_logs.row(d, chain, key),
            Event::Contract(c) => self.contracts.row(c, chain, key),
            Event::Reorg(r) => self.reorgs.row(r, chain, key),
            Event::AcceptedBlock(a) => self.accepted_blocks.row(a, chain, key),
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use alloy_primitives::{Address, B256, TxHash, U256};

    use crate::wire::envelope::{
        Block, BlockMeta, ChainId, Contract, Decoded, Event, Log, Reorg, Transaction,
    };

    use std::sync::Arc;

    use crate::sink::table::{ColumnType, Row, Schema, Table, TableDef, TableId, Value};

    fn row_for(chain: &ChainId, event: &Event) -> Row {
        Schema::new()
            .expect("the dataset tables")
            .row(chain, event)
            .expect("a row")
    }

    fn def(table: Table) -> Arc<TableDef> {
        Arc::clone(Schema::new().expect("the dataset tables").dataset(table))
    }

    fn hash(byte: u8) -> B256 {
        B256::from([byte; 32])
    }

    fn chain() -> ChainId {
        ChainId::new("ethereum")
    }

    fn every_kind() -> Vec<Event> {
        vec![
            Event::Block(Box::new(Block {
                number: 100,
                hash: hash(0x01),
                // Populated, not defaulted: an empty list still renders a document, but
                // a real one is what a mismatched variant shows up against.
                ommers: vec![hash(0x02)],
                transaction_hashes: vec![TxHash::from([0x11; 32])],
                ..Block::default()
            })),
            Event::Transaction(Box::new(Transaction {
                hash: TxHash::from([0x11; 32]),
                block_number: 100,
                block_hash: hash(0x01),
                ..Transaction::default()
            })),
            Event::Log(Box::new(Log {
                log_index: 7,
                transaction_hash: TxHash::from([0x11; 32]),
                block_number: 100,
                block_hash: hash(0x01),
                ..Log::default()
            })),
            Event::Decoded(Box::new(Decoded {
                name: "Swap".to_owned(),
                address: Address::from([0xd0; 20]),
                protocol: "uniswap_v3".to_owned(),
                contract: "UniswapV3Pool".to_owned(),
                event_id: hash(0x08),
                selector: hash(0x07),
                signature: "Swap(address)".to_owned(),
                anonymous: false,
                transaction_hash: TxHash::from([0x11; 32]),
                transaction_index: 3,
                log_index: 7,
                indexed: Vec::new(),
                body: Vec::new(),
                block_number: 100,
                block_hash: hash(0x01),
                block_timestamp: 1_700_000_000,
            })),
            Event::Contract(Box::new(Contract {
                protocol: "uniswap_v3".to_owned(),
                name: "UniswapV3Pool".to_owned(),
                address: Address::from([0xd0; 20]),
                factory_address: Address::from([0xfa; 20]),
                transaction_hash: TxHash::from([0x11; 32]),
                transaction_index: 3,
                log_index: 6,
                block_number: 100,
                block_hash: hash(0x01),
                block_timestamp: 1_700_000_000,
            })),
            Event::Reorg(Reorg {
                height: 100,
                new_head_hash: hash(0x01),
                orphaned_hashes: vec![hash(0x02)],
            }),
            Event::AcceptedBlock(BlockMeta {
                height: 100,
                hash: hash(0x01),
                parent_hash: hash(0x02),
                timestamp: 1_700_000_000,
            }),
        ]
    }

    /// Every event is a row, so a store has no variant to handle and nothing is silently
    /// dropped. A control signal is a row like any other.
    #[test]
    fn every_event_becomes_exactly_one_row() {
        let events = every_kind();
        assert_eq!(events.len(), Table::ALL.len(), "one fixture per table");
        for event in &events {
            let row = row_for(&chain(), event);
            assert_eq!(row.values().len(), row.table().columns.len());
            assert_eq!(row.chain(), "ethereum");
            assert_eq!(row.dedupe_key(), event.dedupe_key());
        }
    }

    /// Every table but `reorgs` names its block by a required text column, so a reorg
    /// retracts its rows. A table that named none by mistake would keep an orphaned branch
    /// forever.
    #[test]
    fn every_table_but_reorg_names_its_block() {
        for event in every_kind() {
            let row = row_for(&chain(), &event);
            let TableId::Dataset(table) = row.table().id else {
                panic!("a dataset event is a dataset row");
            };
            if table == Table::Reorg {
                assert_eq!(row.block_hash(), None, "a reorg belongs to no block");
                continue;
            }
            let def = def(table);
            let column = def
                .block_hash
                .as_deref()
                .expect("every other table belongs to a block");
            let column = &def.columns[def.position(column).expect("the column exists")];
            assert_eq!((column.kind, column.nullable), (ColumnType::Text, false));
            assert_eq!(
                row.block_hash(),
                Some(format!("{:#x}", hash(0x01)).as_str())
            );
        }
    }

    /// A price above `u64::MAX` and a value at `U256::MAX` survive exactly. `u64::MAX` wei
    /// is 18.4 ETH, which a gas price spike can pass.
    #[test]
    fn wide_values_round_trip_exactly() {
        let price = u128::from(u64::MAX) + 1;
        let transaction = Transaction {
            gas_price: Some(price),
            value: U256::MAX,
            ..Transaction::default()
        };
        let row = row_for(&chain(), &Event::Transaction(Box::new(transaction)));
        assert_eq!(
            row.value("gas_price").expect("a column"),
            &Value::unsigned(U256::from(price))
        );
        assert_eq!(
            row.value("value").expect("a column"),
            &Value::unsigned(U256::MAX)
        );
    }

    /// A field the chain does not have is null, never zero: `withdrawals_root` before
    /// EIP-4895, or the fee cap of a legacy transaction.
    #[test]
    fn an_absent_field_is_null_and_not_zero() {
        let row = row_for(&chain(), &Event::Block(Box::default()));
        assert_eq!(
            row.value("withdrawals_root").expect("a column"),
            &Value::Null
        );
        let row = row_for(&chain(), &Event::Transaction(Box::default()));
        assert_eq!(
            row.value("max_fee_per_gas").expect("a column"),
            &Value::Null
        );
    }
}
