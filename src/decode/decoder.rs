//! Log decoding and factory discovery for ingestion.
//!
//! The decoder holds the catalog and the *contract set*: every address it decodes, mapped
//! to its catalog entry. Seeds come from the manifests; discovered contracts join the set the
//! moment their creation log decodes, so a child that emits later in the same block —
//! even the same transaction, as a pool's `Initialize` follows its `PoolCreated` — is
//! decoded too. A reorg retracts the children created in orphaned blocks; the replacement
//! block's creation log re-adds them.

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use alloy_primitives::{Address, B256};
use tracing::warn;

use super::abi::DecodeError;
use super::catalog::{Catalog, EntryId};
use crate::wire::envelope::{Contract, Decoded, Log, TypedValue};

/// What one log produced: its decoded record, and any contracts its event created.
#[derive(Debug)]
pub struct Decoding {
    /// The decoded record.
    pub decoded: Decoded,
    /// The contracts this log created, in rule order.
    pub discovered: Vec<Contract>,
}

/// A contract a previous run discovered, as a store reads it back: only what is needed
/// to decode it again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredContract {
    /// The protocol its manifest declares.
    pub protocol: String,
    /// Its contract name within that protocol, for example `UniswapV3Pool`.
    pub name: String,
    /// The contract.
    pub address: Address,
    /// The hash of the block whose creation log discovered it, so a reorg that orphans
    /// that block after a restart still retracts it.
    pub block_hash: B256,
}

/// One address in the contract set.
#[derive(Debug, Clone, Copy)]
struct Member {
    entry: EntryId,
    /// The block hash of the creation log, which a reorg can orphan. `None` for a seed.
    /// A restored contract keeps its stored block hash: a restart resumes with the
    /// previous run's undo window, so a reorg can still orphan the block that created it.
    created: Option<B256>,
}

/// A log decoder over a catalog and a growing set of contracts.
#[derive(Debug)]
pub struct Decoder {
    catalog: Catalog,
    contracts: HashMap<Address, Member>,
}

impl Decoder {
    /// Starts with the catalog's seed addresses.
    #[must_use]
    pub fn new(catalog: Catalog) -> Self {
        let contracts = catalog
            .seeds
            .iter()
            .map(|&(address, entry)| {
                (
                    address,
                    Member {
                        entry,
                        created: None,
                    },
                )
            })
            .collect();
        Self { catalog, contracts }
    }

    /// Adds contracts a previous run discovered, returning how many were new.
    ///
    /// A contract whose `protocol.name` the catalog no longer declares is skipped with a
    /// warning: its manifest or ABI file was removed or renamed, so it has nothing to
    /// decode with.
    pub fn restore(&mut self, contracts: impl IntoIterator<Item = StoredContract>) -> usize {
        let mut added = 0;
        for contract in contracts {
            let Some(entry) = self.catalog.entry(&contract.protocol, &contract.name) else {
                warn!(protocol = %contract.protocol, name = %contract.name,
                    address = %contract.address, "stored contract is no longer declared");
                continue;
            };
            added += usize::from(self.insert(contract.address, entry, Some(contract.block_hash)));
        }
        added
    }

    /// How many addresses decode: seeds plus discovered contracts.
    #[must_use]
    pub fn contracts(&self) -> usize {
        self.contracts.len()
    }

    /// The identity of the event a log would decode as, for caller-owned diagnostics.
    #[must_use]
    pub fn event_id(&self, log: &Log) -> Option<B256> {
        let member = self.contracts.get(&log.address)?;
        self.catalog.entries[member.entry].abi.event_id(log)
    }

    /// Decodes a log from a contract in the set, adding any contract it creates.
    ///
    /// `None` means the address is not in the set or its ABI has no matching event.
    ///
    /// # Errors
    /// Returns the concrete decoding or schema invariant failure. Callers may skip bad
    /// logs while preserving raw records, but must stop on internal invariant failures.
    pub fn decode(&mut self, log: &Log) -> Result<Option<Decoding>, DecodeError> {
        let Some(member) = self.contracts.get(&log.address) else {
            return Ok(None);
        };
        let entry = &self.catalog.entries[member.entry];
        let Some(event) = entry.abi.decode_log(log)? else {
            return Ok(None);
        };
        let mut children = Vec::new();
        if let Some(rules) = entry.rules.get(&(event.selector, event.indexed.len() + 1)) {
            for rule in rules {
                let address = event
                    .indexed
                    .iter()
                    .chain(&event.body)
                    .find(|argument| argument.position == rule.position)
                    .and_then(|argument| match argument.value {
                        TypedValue::Address { value } => Some(value),
                        _ => None,
                    })
                    .ok_or(DecodeError::Shape)?;
                children.push((rule.child, address));
            }
        }
        let decoded = Decoded {
            name: event.name,
            address: log.address,
            protocol: entry.protocol.clone(),
            contract: entry.name.clone(),
            event_id: event.id,
            selector: event.selector,
            signature: event.signature,
            anonymous: false,
            transaction_hash: log.transaction_hash,
            transaction_index: log.transaction_index,
            log_index: log.log_index,
            indexed: event.indexed,
            body: event.body,
            block_number: log.block_number,
            block_hash: log.block_hash,
            block_timestamp: log.block_timestamp,
        };
        let discovered = children
            .into_iter()
            .map(|(child, address)| {
                self.insert(address, child, Some(log.block_hash));
                let entry = &self.catalog.entries[child];
                Contract {
                    protocol: entry.protocol.clone(),
                    name: entry.name.clone(),
                    address,
                    factory_address: log.address,
                    transaction_hash: log.transaction_hash,
                    transaction_index: log.transaction_index,
                    log_index: log.log_index,
                    block_number: log.block_number,
                    block_hash: log.block_hash,
                    block_timestamp: log.block_timestamp,
                }
            })
            .collect();
        Ok(Some(Decoding {
            decoded,
            discovered,
        }))
    }

    /// Drops every contract created in one of `orphaned`, returning how many.
    ///
    /// Seeds are never dropped. A scan over the set is fine:
    /// reorgs are rare and the set is a hash map of a few hundred thousand entries at most.
    pub fn retract(&mut self, orphaned: &[B256]) -> usize {
        let before = self.contracts.len();
        self.contracts
            .retain(|_, member| member.created.is_none_or(|hash| !orphaned.contains(&hash)));
        before - self.contracts.len()
    }

    /// Adds `address` as an instance of `entry`, returning whether it was new.
    ///
    /// An address already in the set keeps its first entry: a seed stays a seed, and a
    /// contract a second rule also names keeps the entry it was first discovered as.
    fn insert(&mut self, address: Address, entry: EntryId, created: Option<B256>) -> bool {
        match self.contracts.entry(address) {
            Entry::Vacant(vacant) => {
                vacant.insert(Member { entry, created });
                true
            }
            Entry::Occupied(occupied) => {
                if occupied.get().entry != entry {
                    warn!(%address, kept = %self.catalog.label(occupied.get().entry),
                        ignored = %self.catalog.label(entry), "contract already registered as another contract");
                }
                false
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use std::path::Path;

    use alloy_dyn_abi::DynSolValue;
    use alloy_primitives::{Address, B256, keccak256};

    use super::{Decoder, StoredContract};
    use crate::decode::Catalog;
    use crate::wire::envelope::{ChainId, Envelope, Event, Log};

    const FACTORY: &str = "0x33128a8fC17869897dcE68Ed026d694621f6FDfD";

    fn decoder() -> Decoder {
        Decoder::new(
            Catalog::load(
                Path::new(env!("CARGO_MANIFEST_DIR")).join("protocols"),
                &ChainId::new("base"),
            )
            .expect("shipped protocols load"),
        )
    }

    /// A Uniswap V3 `PoolCreated` from the Base factory naming `pool`, in `block`.
    fn pool_created(pool: Address, block: B256) -> Log {
        Log {
            address: FACTORY.parse().expect("factory"),
            topic0: Some(keccak256(
                "PoolCreated(address,address,uint24,int24,address)",
            )),
            topic1: Some(B256::with_last_byte(1)),
            topic2: Some(B256::with_last_byte(2)),
            topic3: Some(B256::left_padding_from(&[0x0b, 0xb8])),
            data: DynSolValue::Tuple(vec![
                DynSolValue::Int(alloy_primitives::I256::try_from(60).expect("int"), 24),
                DynSolValue::Address(pool),
            ])
            .abi_encode_params()
            .into(),
            log_index: 4,
            block_number: 10,
            block_hash: block,
            ..Log::default()
        }
    }

    /// A real Uniswap V3 `Swap`, re-addressed to `pool`.
    fn swap(pool: Address) -> Log {
        let envelope: Envelope = serde_json::from_str(
            include_str!("../../examples/fixtures/uniswap_v3_swaps.ndjson")
                .lines()
                .next()
                .expect("fixture"),
        )
        .expect("envelope");
        let Event::Log(mut log) = envelope.event else {
            panic!("log");
        };
        log.address = pool;
        *log
    }

    fn stored(address: Address) -> StoredContract {
        StoredContract {
            protocol: "uniswap_v3".to_owned(),
            name: "UniswapV3Pool".to_owned(),
            address,
            block_hash: B256::with_last_byte(3),
        }
    }

    /// The creation log names the child and adds it at once, so the child's next log —
    /// in the same block — decodes, stamped with the child's protocol.
    #[test]
    fn a_creation_event_discovers_its_child_immediately() {
        let mut decoder = decoder();
        let pool = Address::from([0xd0; 20]);
        assert!(decoder.decode(&swap(pool)).expect("decode").is_none());

        let created = decoder
            .decode(&pool_created(pool, B256::with_last_byte(1)))
            .expect("decode")
            .expect("the factory is a seed");
        assert_eq!(created.decoded.name, "PoolCreated");
        let [contract] = &created.discovered[..] else {
            panic!("one child");
        };
        assert_eq!(
            (contract.address, contract.name.as_str()),
            (pool, "UniswapV3Pool")
        );
        assert_eq!(
            contract.factory_address,
            FACTORY.parse::<Address>().expect("factory")
        );
        assert_eq!((contract.block_number, contract.log_index), (10, 4));

        let swap = decoder
            .decode(&swap(pool))
            .expect("decode")
            .expect("now known");
        assert_eq!(
            (swap.decoded.name.as_str(), swap.decoded.protocol.as_str()),
            ("Swap", "uniswap_v3")
        );
        assert!(swap.discovered.is_empty());
    }

    /// A reorg drops children created in orphaned blocks and nothing else; seeds and
    /// children of other blocks stay. A restored contract is retracted like one this run
    /// discovered, since its creating block may sit in the resumed undo window.
    #[test]
    fn a_reorg_retracts_children_of_orphaned_blocks() {
        let mut decoder = decoder();
        let (kept, dropped, restored, restored_orphan) = (
            Address::from([0x01; 20]),
            Address::from([0x02; 20]),
            Address::from([0x03; 20]),
            Address::from([0x04; 20]),
        );
        let (canonical, orphaned) = (B256::with_last_byte(1), B256::with_last_byte(2));
        decoder.restore([
            stored(restored),
            StoredContract {
                block_hash: orphaned,
                ..stored(restored_orphan)
            },
        ]);
        decoder
            .decode(&pool_created(kept, canonical))
            .expect("decode");
        decoder
            .decode(&pool_created(dropped, orphaned))
            .expect("decode");
        let before = decoder.contracts();

        assert_eq!(decoder.retract(&[orphaned, B256::ZERO]), 2);
        assert_eq!(decoder.contracts(), before - 2);
        for (pool, decodes) in [
            (kept, true),
            (dropped, false),
            (restored, true),
            (restored_orphan, false),
        ] {
            assert_eq!(
                decoder.decode(&swap(pool)).expect("decode").is_some(),
                decodes
            );
        }
    }

    /// Restored contracts decode; one no longer declared is skipped, and
    /// restoring twice adds nothing.
    #[test]
    fn restored_contracts_decode() {
        let mut decoder = decoder();
        let pool = Address::from([0xd0; 20]);
        let retired = StoredContract {
            protocol: "retired".to_owned(),
            ..stored(pool)
        };
        assert_eq!(decoder.restore([stored(pool), retired]), 1);
        assert!(decoder.decode(&swap(pool)).expect("decode").is_some());
        assert_eq!(decoder.restore([stored(pool)]), 0);
    }
}
