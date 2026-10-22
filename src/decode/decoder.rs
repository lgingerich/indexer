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
use super::derive::Derivation;
use crate::wire::envelope::{Contract, Decoded, Log, StoredContract, TypedValue};

/// What one log produced: its decoded record, and any contracts its event created.
#[derive(Debug)]
pub struct Decoding {
    /// The decoded record.
    pub decoded: Decoded,
    /// The contracts this log created, in rule order.
    pub discovered: Vec<Contract>,
}

/// One address in the contract set.
#[derive(Debug, Clone)]
struct Member {
    entry: EntryId,
    /// The block hashes of its creation logs, which a reorg can orphan; it leaves the set
    /// once all are. `None` for a seed. A restored contract keeps its stored block hashes:
    /// a restart resumes with the previous run's undo window, so a reorg can still orphan
    /// a block that created it.
    created: Option<Vec<B256>>,
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

    /// Adds contracts a previous run discovered, returning how many addresses were new.
    ///
    /// They are added in the order the run discovered them, by creation block and log, so
    /// when two claim one address the first keeps it, as it did in that run, whatever
    /// order the stores read them back in.
    ///
    /// A contract whose `protocol.name` the catalog no longer declares is skipped with a
    /// warning: its manifest or ABI file was removed or renamed, so it has nothing to
    /// decode with.
    pub fn restore(&mut self, contracts: impl IntoIterator<Item = StoredContract>) -> usize {
        let mut contracts: Vec<StoredContract> = contracts.into_iter().collect();
        contracts.sort_by_key(|contract| (contract.block_number, contract.log_index));
        let before = self.contracts.len();
        for contract in contracts {
            let Some(entry) = self.catalog.entry(&contract.protocol, &contract.name) else {
                warn!(protocol = %contract.protocol, name = %contract.name,
                    address = %contract.address, "stored contract is no longer declared");
                continue;
            };
            self.insert(contract.address, entry, contract.block_hash);
        }
        self.contracts.len() - before
    }

    /// The derivations the catalog feeds, which run on every record decoded.
    pub(super) fn derivations(&self) -> &[Derivation] {
        &self.catalog.derivations
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
        // Only a claim the set records is published: one naming a seed, or an address
        // already registered as another contract, or named twice by this log, would be a
        // row restoring nothing, or a second row under one key.
        children.retain(|&(child, address)| self.insert(address, child, log.block_hash));
        let discovered = children
            .into_iter()
            .map(|(child, address)| {
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

    /// Forgets creations in `orphaned` blocks, dropping contracts left with none, and
    /// returns how many were dropped.
    ///
    /// Seeds are never dropped. A scan over the set is fine:
    /// reorgs are rare and the set is a hash map of a few hundred thousand entries at most.
    pub fn retract(&mut self, orphaned: &[B256]) -> usize {
        let before = self.contracts.len();
        self.contracts.retain(|_, member| {
            member.created.as_mut().is_none_or(|created| {
                created.retain(|hash| !orphaned.contains(hash));
                !created.is_empty()
            })
        });
        before - self.contracts.len()
    }

    /// Adds `address` as an instance of `entry`, created in block `created`, returning
    /// whether the set recorded it: a new address, or a new creation block of a
    /// discovered one, which a reorg of either block must see.
    ///
    /// An address already in the set keeps its first entry: a seed stays a seed, and a
    /// contract a second rule also names keeps the entry it was first discovered as. A
    /// claim as another entry records nothing.
    fn insert(&mut self, address: Address, entry: EntryId, created: B256) -> bool {
        match self.contracts.entry(address) {
            Entry::Vacant(vacant) => {
                vacant.insert(Member {
                    entry,
                    created: Some(vec![created]),
                });
                true
            }
            Entry::Occupied(mut occupied) => {
                let member = occupied.get_mut();
                if member.entry != entry {
                    warn!(%address, kept = %self.catalog.label(member.entry),
                        ignored = %self.catalog.label(entry), "contract already registered as another contract");
                    return false;
                }
                match &mut member.created {
                    Some(blocks) if !blocks.contains(&created) => {
                        blocks.push(created);
                        true
                    }
                    _ => false,
                }
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

    use super::Decoder;
    use crate::decode::Catalog;
    use crate::wire::envelope::StoredContract;
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
            block_number: 3,
            log_index: 0,
        }
    }

    /// A protocol `p` on Base: a seeded `Parent` whose `Made(address child)` names a
    /// `Child` by two rules and an `Other` by a third, and `Child` seeded at [`SEEDED`].
    fn claims() -> Decoder {
        let root = std::env::temp_dir().join(format!("indexer-claims-{}", std::process::id()));
        let dir = root.join("p");
        std::fs::create_dir_all(&dir).expect("create protocol dir");
        let made = r#"[{"type":"event","name":"Made","anonymous":false,"inputs":[
            {"name":"child","type":"address","indexed":false}]}]"#;
        let rule = r#"{ contract = "Parent", event = "Made", param = "child" }"#;
        for (file, contents) in [
            ("Parent.json", made.to_owned()),
            ("Child.json", "[]".to_owned()),
            ("Other.json", "[]".to_owned()),
            (
                "protocol.toml",
                format!(
                    "protocol = \"p\"\n\
                     [[contract]]\nabi = [\"Parent.json\"]\naddresses = {{ base = [\"{PARENT}\"] }}\n\
                     [[contract]]\nabi = [\"Child.json\"]\naddresses = {{ base = [\"{SEEDED}\"] }}\n\
                     created_by = [{rule}, {rule}]\n\
                     [[contract]]\nabi = [\"Other.json\"]\ncreated_by = [{rule}]\n"
                ),
            ),
        ] {
            std::fs::write(dir.join(file), contents).expect("write file");
        }
        let catalog = Catalog::load(&root, &ChainId::new("base"));
        std::fs::remove_dir_all(&root).expect("clean up");
        Decoder::new(catalog.expect("the protocol loads"))
    }

    const PARENT: Address = Address::repeat_byte(0x0a);
    const SEEDED: Address = Address::repeat_byte(0x0b);

    /// `Parent`'s `Made` naming `child`.
    fn made(child: Address) -> Log {
        Log {
            address: PARENT,
            topic0: Some(keccak256("Made(address)")),
            data: DynSolValue::Address(child).abi_encode().into(),
            block_hash: B256::with_last_byte(7),
            ..Log::default()
        }
    }

    /// A claim is published only when the set records it: three rules naming one new
    /// address publish it once, as the first rule's contract, and a claim on a seed
    /// publishes nothing.
    #[test]
    fn only_a_recorded_claim_is_published() {
        let mut decoder = claims();
        let child = Address::repeat_byte(0xc0);
        let created = decoder
            .decode(&made(child))
            .expect("decode")
            .expect("the parent is a seed");
        let published: Vec<_> = created
            .discovered
            .iter()
            .map(|contract| (contract.address, contract.name.as_str()))
            .collect();
        assert_eq!(published, [(child, "Child")]);

        let seed = decoder
            .decode(&made(SEEDED))
            .expect("decode")
            .expect("the parent is a seed");
        assert!(seed.discovered.is_empty(), "{:?}", seed.discovered);
    }

    /// Two stored claims on one address restore as the run kept them: the first by
    /// creation block and log, whichever order the stores read them back in.
    #[test]
    fn a_restore_keeps_the_first_claim_on_an_address() {
        let address = Address::repeat_byte(0xc1);
        let claim = |name: &str, block_number, log_index| StoredContract {
            protocol: "p".to_owned(),
            name: name.to_owned(),
            address,
            block_hash: B256::with_last_byte(1),
            block_number,
            log_index,
        };
        let first = [claim("Other", 4, 0), claim("Child", 3, 9)];
        for stored in [first.clone(), [first[1].clone(), first[0].clone()]] {
            let mut decoder = claims();
            assert_eq!(decoder.restore(stored), 1);
            let kept = decoder.contracts[&address].entry;
            assert_eq!(decoder.catalog.label(kept), "p.Child");
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

    /// A reorg drops children created only in orphaned blocks; seeds and children also
    /// created elsewhere stay. A restored contract is retracted like one this run
    /// discovered, since its creating block may sit in the resumed undo window.
    #[test]
    fn a_reorg_retracts_children_of_orphaned_blocks() {
        let mut decoder = decoder();
        let (kept, dropped, restored, restored_orphan, twice) = (
            Address::from([0x01; 20]),
            Address::from([0x02; 20]),
            Address::from([0x03; 20]),
            Address::from([0x04; 20]),
            Address::from([0x05; 20]),
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
        for block in [orphaned, canonical] {
            decoder.decode(&pool_created(twice, block)).expect("decode");
        }
        let before = decoder.contracts();

        assert_eq!(decoder.retract(&[orphaned, B256::ZERO]), 2);
        assert_eq!(decoder.contracts(), before - 2);
        for (case, pool, decodes) in [
            ("created in a canonical block", kept, true),
            ("created in an orphaned block", dropped, false),
            ("restored from a canonical block", restored, true),
            ("restored from an orphaned block", restored_orphan, false),
            ("created in both", twice, true),
        ] {
            assert_eq!(
                decoder.decode(&swap(pool)).expect("decode").is_some(),
                decodes,
                "{case}"
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
