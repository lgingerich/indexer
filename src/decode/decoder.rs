//! Log decoding and factory discovery for ingestion.
//!
//! The decoder holds the catalog and the *contract set*: every address it decodes, mapped
//! to its catalog entry. Seeds come from the manifests, and the contracts a previous run
//! discovered are restored as the decoder is built, before it decodes anything. A
//! discovered contract joins the set once its creation log is published, so a child that
//! emits later in the same block — even the same transaction, as a pool's `Initialize`
//! follows its `PoolCreated` — is decoded too. A reorg retracts the children created in
//! orphaned blocks; the replacement block's creation log re-adds them.
//!
//! A child is read from its creation log's address argument alone, so an unrelated
//! argument that fails to decode still discovers it, and only that log's record is lost.

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use alloy_primitives::{Address, B256};
use tracing::warn;

use super::abi::{DecodeError, EventKey, event_key};
use super::catalog::{Catalog, EntryId};
#[cfg(feature = "delta")]
use super::derive::{DeriveError, Sent};
use crate::sink::StoredContract;
#[cfg(feature = "delta")]
use crate::wire::datasets::evm::Derived;
use crate::wire::datasets::evm::{Contract, Decoded, Log};
#[cfg(feature = "delta")]
use crate::wire::envelope::ChainId;

/// What [`Decoder::adopt`] did with a handed-over contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Adopted {
    /// It joined the set.
    Added,
    /// The set already decoded it as that contract: a seed, one this run discovered, or
    /// one a previous run's stores restored.
    Known,
    /// It is not decoded: the catalog does not declare it, or the address is another
    /// contract.
    Refused,
}

/// The catalog entry and event a record decoded as, which its derivations are bound to.
pub(crate) type DecodedAs = (EntryId, EventKey);

/// A log's decoded record, with the catalog entry and event it decoded as.
#[derive(Debug)]
pub struct Record {
    /// The record.
    pub decoded: Decoded,
    /// The catalog entry and event the record decoded as, which its derivations are bound
    /// to.
    pub(crate) event: DecodedAs,
}

/// The contracts one creation log discovered, not yet in the set.
///
/// [`Decoder::decode`] leaves the set as it was, so a log whose records are never
/// published, because the sink after it failed, discovers nothing: the same log seen
/// again claims its contracts again and publishes them. [`Decoder::admit`] records them
/// once they are published.
#[derive(Debug)]
#[must_use = "a discovered contract decodes only once its claim is admitted"]
pub struct Claims {
    /// Each contract the log created, in rule order, with the catalog entry it decodes
    /// as.
    contracts: Vec<(EntryId, Contract)>,
}

impl Claims {
    /// The contracts claimed, in rule order: the rows to publish before
    /// [`Decoder::admit`].
    #[must_use]
    pub fn contracts(&self) -> impl ExactSizeIterator<Item = &Contract> {
        self.contracts.iter().map(|(_, contract)| contract)
    }
}

/// One address in the contract set.
#[derive(Debug, Clone)]
struct Member {
    entry: EntryId,
    origin: Origin,
}

/// How an address joined the contract set.
#[derive(Debug, Clone)]
enum Origin {
    /// A manifest lists it. A seed is never retracted.
    Seed,
    /// A creation log discovered it, in this run or a previous one. A restored contract
    /// keeps its stored block hashes: a restart resumes with the previous run's undo
    /// window, so a reorg can still orphan a block that created it.
    ///
    /// It holds the block hashes of its creation logs, which a reorg can orphan: never
    /// empty, since [`Decoder::retract`] drops the contract once all are orphaned.
    Created(Vec<B256>),
}

/// A log decoder over a catalog and a growing set of contracts.
#[derive(Debug)]
pub struct Decoder {
    catalog: Catalog,
    contracts: HashMap<Address, Member>,
}

impl Decoder {
    /// Starts with the catalog's seed addresses and the contracts a previous run
    /// discovered, `restored`, as the stores read them back.
    ///
    /// The restored contracts are added in the order the run discovered them, by creation
    /// block and log, so when two claim one address the first keeps it, as it did in that
    /// run, whatever order the stores read them back in. They are added here, before the
    /// decoder decodes anything, so no claim this run makes can come before them.
    ///
    /// A contract whose `protocol.name` the catalog no longer declares is skipped with a
    /// warning: its manifest or ABI file was removed or renamed, so it has nothing to
    /// decode with.
    #[must_use]
    pub fn new(catalog: Catalog, restored: impl IntoIterator<Item = StoredContract>) -> Self {
        let contracts = catalog
            .seeds
            .iter()
            .map(|&(address, entry)| {
                (
                    address,
                    Member {
                        entry,
                        origin: Origin::Seed,
                    },
                )
            })
            .collect();
        let mut decoder = Self { catalog, contracts };
        let mut restored: Vec<StoredContract> = restored.into_iter().collect();
        restored.sort_by_key(|contract| (contract.block_number, contract.log_index));
        for contract in restored {
            let Some(entry) = decoder.catalog.entry(&contract.protocol, &contract.name) else {
                warn!(protocol = %contract.protocol, name = %contract.name,
                    address = %contract.address, "stored contract is no longer declared");
                continue;
            };
            decoder.insert(contract.address, entry, contract.block_hash);
        }
        decoder
    }

    /// Adds `contract`, which a backfill discovered below this run's restart and handed
    /// over, so its logs decode from the next one on.
    ///
    /// One whose `protocol.name` the catalog does not declare is refused, as is an address
    /// the set holds as another contract.
    pub(crate) fn adopt(&mut self, contract: &Contract) -> Adopted {
        let Some(entry) = self.catalog.entry(&contract.protocol, &contract.name) else {
            warn!(protocol = %contract.protocol, name = %contract.name,
                address = %contract.address, "handed-over contract is not declared; not decoded");
            return Adopted::Refused;
        };
        match self.contracts.get(&contract.address) {
            Some(member) if member.entry == entry => Adopted::Known,
            Some(member) => {
                conflict(&self.catalog, contract.address, member.entry, entry);
                Adopted::Refused
            }
            None => {
                self.insert(contract.address, entry, contract.block_hash);
                Adopted::Added
            }
        }
    }

    /// Every address in the set whose contract's events discover others: the factories,
    /// seeds or discovered, whose creation logs a backfill reads to find their children.
    #[cfg(feature = "delta")]
    pub(crate) fn parents(&self) -> Vec<Address> {
        let mut parents: Vec<Address> = self
            .contracts
            .iter()
            .filter(|(_, member)| !self.catalog.entries[member.entry].rules.is_empty())
            .map(|(&address, _)| address)
            .collect();
        parents.sort_unstable();
        parents
    }

    /// The catalog it decodes with.
    pub(super) const fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// The rows `decoded` derives, as the running indexer's decode stage derives them:
    /// `event` is the [`Record::event`] it decoded as, and `sent` its transaction, whose
    /// sender and recipient each row carries, `None` when it is not known.
    ///
    /// # Errors
    ///
    /// Returns [`DeriveError::Columns`] when a derivation computes a row that does not
    /// match its table, a bug in it that stops the run.
    #[cfg(feature = "delta")]
    pub(crate) fn derive(
        &self,
        chain: &ChainId,
        event: DecodedAs,
        decoded: &Decoded,
        sent: Option<&Sent>,
    ) -> Result<Vec<Derived>, DeriveError> {
        self.catalog.derivations.rows(chain, event, decoded, sent)
    }

    /// How many addresses decode: seeds plus discovered contracts.
    #[must_use]
    pub fn contracts(&self) -> usize {
        self.contracts.len()
    }

    /// How many of [`Self::contracts`] were discovered rather than seeded.
    #[must_use]
    pub fn discovered(&self) -> usize {
        self.contracts
            .values()
            .filter(|member| matches!(member.origin, Origin::Created(_)))
            .count()
    }

    /// The identity of the event a log would decode as, for caller-owned diagnostics.
    #[must_use]
    pub fn event_id(&self, log: &Log) -> Option<B256> {
        let member = self.contracts.get(&log.address)?;
        self.catalog.entries[member.entry].abi.event_id(log)
    }

    /// Decodes a log from a contract in the set into its record, or why it did not
    /// decode, and the claims on any contract it creates.
    ///
    /// `None` means the address is not in the set or its ABI has no matching event. A
    /// log that matches an event but does not decode still discovers the contracts it
    /// names, since each is read from its own argument. A caller may skip a log that did
    /// not decode and keep its raw record, but must stop on an internal invariant failure.
    ///
    /// The set is left as it was: a discovered contract decodes once the caller has
    /// published [`Claims::contracts`] and passed the claims to [`Self::admit`].
    ///
    /// A child's logs from before its creation log, such as those its constructor emits,
    /// are never decoded: the child joins the set only once that log is admitted, after
    /// them.
    pub fn decode(&self, log: &Log) -> Option<(Result<Record, DecodeError>, Claims)> {
        let id = self.contracts.get(&log.address)?.entry;
        let entry = &self.catalog.entries[id];
        let decoded = entry.abi.decode_log(log).transpose()?;
        // The rules' key is the log's own, not the decoded event's, so a log that does not
        // decode still discovers its children. The zero address is no contract: an event
        // names it where it has none to name, as a multiplier oracle's passthrough feed
        // names no token.
        let named = event_key(log)
            .and_then(|key| entry.rules.get(&key))
            .into_iter()
            .flatten()
            .filter_map(|rule| Some((rule.child, rule.slot.read(log)?)))
            .filter(|(_, address)| !address.is_zero());
        // Only a claim the set would record is published: one naming a seed, or an
        // address already registered as another contract, or named twice by this log,
        // would be a row restoring nothing, or a second row under one key.
        let mut contracts: Vec<(EntryId, Contract)> = Vec::new();
        for (child, address) in named {
            let claimed = contracts
                .iter()
                .find(|(_, contract)| contract.address == address);
            let recorded = match claimed {
                Some(&(kept, _)) => {
                    if kept != child {
                        conflict(&self.catalog, address, kept, child);
                    }
                    false
                }
                None => self.records(address, child, log.block_hash),
            };
            if recorded {
                let entry = &self.catalog.entries[child];
                let contract = Contract {
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
                };
                contracts.push((child, contract));
            }
        }
        let decoded = decoded.map(|event| Record {
            event: (id, event.key),
            decoded: Decoded {
                name: event.name,
                address: log.address,
                protocol: entry.protocol.clone(),
                contract: entry.name.clone(),
                event_id: event.id,
                selector: event.key.0,
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
            },
        });
        Some((decoded, Claims { contracts }))
    }

    /// Adds the contracts a decoded log discovered, once they are published, returning
    /// how many the set recorded.
    pub fn admit(&mut self, claims: Claims) -> usize {
        claims
            .contracts
            .into_iter()
            .filter(|(entry, contract)| self.insert(contract.address, *entry, contract.block_hash))
            .count()
    }

    /// Forgets creations in `orphaned` blocks, dropping contracts left with none, and
    /// returns how many were dropped.
    ///
    /// Seeds are never dropped. A scan over the set is fine:
    /// reorgs are rare and the set is a hash map of a few hundred thousand entries at most.
    pub fn retract(&mut self, orphaned: &[B256]) -> usize {
        let before = self.contracts.len();
        self.contracts.retain(|_, member| match &mut member.origin {
            Origin::Seed => true,
            Origin::Created(created) => {
                created.retain(|block| !orphaned.contains(block));
                !created.is_empty()
            }
        });
        before - self.contracts.len()
    }

    /// Whether [`Self::insert`] would record `address` as an instance of `entry`, created
    /// in block `created`: a new address, or a new creation block of a discovered one,
    /// which a reorg of either block must see.
    ///
    /// An address already in the set keeps its first entry: a seed stays a seed, and a
    /// contract a second rule also names keeps the entry it was first discovered as. A
    /// claim as another entry records nothing.
    fn records(&self, address: Address, entry: EntryId, created: B256) -> bool {
        let Some(member) = self.contracts.get(&address) else {
            return true;
        };
        if member.entry != entry {
            conflict(&self.catalog, address, member.entry, entry);
            return false;
        }
        match &member.origin {
            Origin::Seed => false,
            Origin::Created(creations) => !creations.contains(&created),
        }
    }

    /// Adds `address` as an instance of `entry`, created in block `created`, by the rules
    /// [`Self::records`] states, returning whether the set recorded it.
    fn insert(&mut self, address: Address, entry: EntryId, created: B256) -> bool {
        let member = match self.contracts.entry(address) {
            Entry::Vacant(vacant) => {
                vacant.insert(Member {
                    entry,
                    origin: Origin::Created(vec![created]),
                });
                return true;
            }
            Entry::Occupied(occupied) => occupied.into_mut(),
        };
        if member.entry != entry {
            conflict(&self.catalog, address, member.entry, entry);
            return false;
        }
        match &mut member.origin {
            Origin::Created(creations) if !creations.contains(&created) => {
                creations.push(created);
                true
            }
            Origin::Seed | Origin::Created(_) => false,
        }
    }
}

/// Warns that a claim on `address` as `ignored` is refused, since the set holds it as
/// `kept`.
fn conflict(catalog: &Catalog, address: Address, kept: EntryId, ignored: EntryId) {
    warn!(%address, kept = %catalog.label(kept),
        ignored = %catalog.label(ignored), "contract already registered as another contract");
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use alloy_dyn_abi::DynSolValue;
    use alloy_primitives::{Address, B256, keccak256};

    use super::{Decoder, Record};
    use crate::decode::DecodeError;
    use crate::decode::fixtures::uniswap_v3::{FACTORY, pool_created, swap};
    use crate::decode::fixtures::{protocol, shipped};
    use crate::sink::StoredContract;
    use crate::wire::datasets::evm::{Contract, Log};

    fn decoder() -> Decoder {
        restored([])
    }

    /// A decoder over the shipped Base catalog that restored `stored`.
    fn restored(stored: impl IntoIterator<Item = StoredContract>) -> Decoder {
        Decoder::new(shipped("base"), stored)
    }

    /// Decodes `log` and admits the contracts it discovered, as the sink does once it
    /// has published them.
    fn publish(
        decoder: &mut Decoder,
        log: &Log,
    ) -> Option<(Result<Record, DecodeError>, Vec<Contract>)> {
        let (outcome, claims) = decoder.decode(log)?;
        let published = claims.contracts().cloned().collect();
        decoder.admit(claims);
        Some((outcome, published))
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
    fn claims(stored: impl IntoIterator<Item = StoredContract>) -> Decoder {
        let made = r#"[{"type":"event","name":"Made","anonymous":false,"inputs":[
            {"name":"child","type":"address","indexed":false}]}]"#;
        let rule = r#"{ contract = "Parent", event = "Made", param = "child" }"#;
        let manifest = format!(
            "protocol = \"p\"\n\
             [[contract]]\nabi = [\"Parent.json\"]\naddresses = {{ base = [\"{PARENT}\"] }}\n\
             [[contract]]\nabi = [\"Child.json\"]\naddresses = {{ base = [\"{SEEDED}\"] }}\n\
             created_by = [{rule}, {rule}]\n\
             [[contract]]\nabi = [\"Other.json\"]\ncreated_by = [{rule}]\n"
        );
        let catalog = protocol(&[
            ("Parent.json", made),
            ("Child.json", "[]"),
            ("Other.json", "[]"),
            ("protocol.toml", &manifest),
        ]);
        Decoder::new(catalog.expect("the protocol loads"), stored)
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
        let mut decoder = claims([]);
        let child = Address::repeat_byte(0xc0);
        let (_, created) = publish(&mut decoder, &made(child)).expect("the parent is a seed");
        let published: Vec<_> = created
            .iter()
            .map(|contract| (contract.address, contract.name.as_str()))
            .collect();
        assert_eq!(published, [(child, "Child")]);

        let (_, seed) = decoder.decode(&made(SEEDED)).expect("the parent is a seed");
        assert_eq!(seed.contracts().len(), 0, "{seed:?}");
        let (_, again) = decoder.decode(&made(child)).expect("the parent is a seed");
        assert_eq!(again.contracts().len(), 0, "{again:?}");
    }

    /// The zero address is never claimed: a parent naming none discovers nothing.
    #[test]
    fn the_zero_address_is_not_claimed() {
        let decoder = claims([]);
        let (_, none) = decoder
            .decode(&made(Address::ZERO))
            .expect("the parent is a seed");
        assert_eq!(none.contracts().len(), 0, "{none:?}");
    }

    /// A decode leaves the set as it was: until its claims are admitted, the child does
    /// not decode, and the same log claims it again.
    #[test]
    fn a_claim_joins_the_set_only_once_admitted() {
        let mut decoder = decoder();
        let pool = Address::from([0xd0; 20]);
        let creation = pool_created(pool, B256::with_last_byte(1));
        let (_, unpublished) = decoder.decode(&creation).expect("the factory is a seed");
        assert_eq!(unpublished.contracts().len(), 1);
        drop(unpublished);
        assert!(decoder.decode(&swap(pool)).is_none());

        let (_, retried) = decoder.decode(&creation).expect("the factory is a seed");
        assert_eq!(retried.contracts().len(), 1);
        assert_eq!(decoder.admit(retried), 1);
        assert!(decoder.decode(&swap(pool)).is_some());
        assert_eq!(decoder.discovered(), 1);
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
            let decoder = claims(stored);
            assert_eq!(decoder.discovered(), 1);
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
        assert!(decoder.decode(&swap(pool)).is_none());

        let (outcome, created) =
            publish(&mut decoder, &pool_created(pool, B256::with_last_byte(1)))
                .expect("the factory is a seed");
        assert_eq!(outcome.expect("decodes").decoded.name, "PoolCreated");
        let [contract] = &created[..] else {
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

        let (outcome, claims) = decoder.decode(&swap(pool)).expect("now known");
        let record = outcome.expect("decodes").decoded;
        assert_eq!(
            (record.name.as_str(), record.protocol.as_str()),
            ("Swap", "uniswap_v3")
        );
        assert_eq!(claims.contracts().len(), 0);
    }

    /// A creation log whose unrelated argument fails validation still discovers its
    /// child, read from the address argument alone; only the log's own record is lost.
    #[test]
    fn a_creation_log_that_does_not_decode_still_discovers_its_child() {
        let mut decoder = decoder();
        let pool = Address::from([0xd0; 20]);
        let mut created = pool_created(pool, B256::with_last_byte(1));
        // `tickSpacing`, an `int24`, holding a word past 24 bits.
        created.data = DynSolValue::Tuple(vec![
            DynSolValue::Int(alloy_primitives::I256::MAX, 256),
            DynSolValue::Address(pool),
        ])
        .abi_encode_params()
        .into();
        let (outcome, discovered) = publish(&mut decoder, &created).expect("the factory is a seed");
        assert!(
            matches!(outcome, Err(DecodeError::Value { .. })),
            "{outcome:?}"
        );
        let [contract] = &discovered[..] else {
            panic!("one child: {discovered:?}");
        };
        assert_eq!(contract.address, pool);
        assert!(
            decoder.decode(&swap(pool)).is_some(),
            "the pool now decodes"
        );
    }

    /// A reorg drops children created only in orphaned blocks; seeds and children also
    /// created elsewhere stay. A restored contract is retracted like one this run
    /// discovered, since its creating block may sit in the resumed undo window.
    #[test]
    fn a_reorg_retracts_children_of_orphaned_blocks() {
        let (kept, dropped, restored_canonical, restored_orphan, twice) = (
            Address::from([0x01; 20]),
            Address::from([0x02; 20]),
            Address::from([0x03; 20]),
            Address::from([0x04; 20]),
            Address::from([0x05; 20]),
        );
        let (canonical, orphaned) = (B256::with_last_byte(1), B256::with_last_byte(2));
        let mut decoder = restored([
            stored(restored_canonical),
            StoredContract {
                block_hash: orphaned,
                ..stored(restored_orphan)
            },
        ]);
        publish(&mut decoder, &pool_created(kept, canonical));
        publish(&mut decoder, &pool_created(dropped, orphaned));
        for block in [orphaned, canonical] {
            publish(&mut decoder, &pool_created(twice, block));
        }
        let before = decoder.contracts();

        assert_eq!(decoder.retract(&[orphaned, B256::ZERO]), 2);
        assert_eq!(decoder.contracts(), before - 2);
        for (case, pool, decodes) in [
            ("created in a canonical block", kept, true),
            ("created in an orphaned block", dropped, false),
            ("restored from a canonical block", restored_canonical, true),
            ("restored from an orphaned block", restored_orphan, false),
            ("created in both", twice, true),
        ] {
            assert_eq!(decoder.decode(&swap(pool)).is_some(), decodes, "{case}");
        }
    }

    /// Restored contracts decode; one no longer declared is skipped, and one stored
    /// twice is restored once.
    #[test]
    fn restored_contracts_decode() {
        let pool = Address::from([0xd0; 20]);
        let retired = StoredContract {
            protocol: "retired".to_owned(),
            address: Address::from([0xd1; 20]),
            ..stored(pool)
        };
        let decoder = restored([stored(pool), retired, stored(pool)]);
        assert_eq!(decoder.discovered(), 1);
        assert!(decoder.decode(&swap(pool)).is_some());
    }
}
