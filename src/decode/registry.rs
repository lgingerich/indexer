//! The ABI registry: which contract's ABI applies to a log at a height.
//!
//! Lookup is keyed by `(chain, contract address, block)` and not by address alone.
//! A contract's ABI is valid over a *block range*: a proxy upgrades, a new
//! implementation appears, and the same address answers different selectors at
//! different heights. An implicit "latest" would silently decode an old log with a
//! new ABI, which is the failure mode that produces plausible but wrong rows — so
//! the height is part of the key and there is no default for it.
//!
//! # Static and dynamic ABIs
//!
//! The two sources are complementary rather than alternatives:
//!
//! - A **compile-time** ABI is checked code. `sol!`-generated bindings, or a JSON
//!   ABI load, give a decoder that cannot silently mis-type a field.
//! - A **runtime** ABI is data. No compile-time set can cover an open-ended set of
//!   contracts deployed by third parties, and a proxy's ABI is not knowable at build
//!   time at all.
//!
//! [`Abi`] is the runtime path; a compile-time binding would be converted into one
//! for a uniform call site. [`AbiRegistry`] is the seam either way, so the transform
//! does not know which answered — and so a registry backed by a table of ABI
//! versions can be swapped in without touching the decoder.

use std::collections::{BTreeMap, HashMap};

use crate::wire::envelope::{ChainId, Decoded, DecodedArg};
use alloy_dyn_abi::{DynSolValue, EventExt as _};
use alloy_json_abi::{Event, JsonAbi};
use alloy_primitives::{Address, B256, TxHash};

use crate::decode::convert::{self, ConversionError};

/// The parts of a raw log a decoder needs.
///
/// A borrowed view rather than the whole [`Log`](crate::wire::datasets::evm::Log) record, so
/// the seal is explicit: a decoder reads a log and produces a record, and it has no
/// business seeing the rest of the envelope.
#[derive(Debug, Clone, Copy)]
pub struct RawLog<'a> {
    /// The chain the log came from.
    pub chain: &'a ChainId,
    /// The contract that emitted the log.
    pub address: Address,
    /// The log's topics, `topic0` first.
    pub topics: &'a [B256],
    /// The log's unindexed data.
    pub data: &'a [u8],
    /// The transaction that emitted the log.
    pub transaction_hash: TxHash,
    /// The emitting transaction's position in its block.
    pub transaction_index: u64,
    /// The log's position within its block.
    pub log_index: u64,
    /// Height of the block containing the log.
    pub block_number: u64,
    /// Hash of the block containing the log.
    pub block_hash: B256,
    /// Timestamp of the block containing the log.
    pub block_timestamp: u64,
}

/// One loaded contract ABI, able to decode a log against its events.
///
/// Owns a parsed [`JsonAbi`] rather than a source path, because the lookup path must
/// not do I/O. A registry that reads a file per log is a registry that stalls the
/// pipeline under load.
#[derive(Debug, Clone, Default)]
pub struct Abi {
    abi: JsonAbi,
    /// Selector to event, built once so the per-log lookup is a map hit rather than
    /// a linear scan over the ABI's events.
    by_selector: BTreeMap<B256, Event>,
}

impl Abi {
    /// Loads an ABI from its JSON form.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::Abi`] if the JSON is not a valid ABI.
    pub fn from_json(json: &str) -> Result<Self, RegistryError> {
        let abi: JsonAbi = serde_json::from_str(json).map_err(|error| RegistryError::Abi {
            detail: error.to_string(),
        })?;
        Ok(Self::from_abi(abi))
    }

    /// Indexes an already-parsed ABI by event selector.
    ///
    /// Anonymous events are excluded: they carry no selector in `topic0`, so they
    /// cannot be found by one, and pretending otherwise would match the wrong event
    /// on an unrelated log.
    #[must_use]
    pub fn from_abi(abi: JsonAbi) -> Self {
        let by_selector = abi
            .events()
            .filter(|event| !event.anonymous)
            .map(|event| (event.selector(), event.clone()))
            .collect();
        Self { abi, by_selector }
    }

    /// The parsed ABI, for a caller that needs more than event lookup.
    #[must_use]
    pub const fn json_abi(&self) -> &JsonAbi {
        &self.abi
    }

    /// Decodes one log, if this ABI declares an event with that topic.
    ///
    /// Returns `Ok(None)` when no event in the ABI has the log's selector. That is a
    /// normal miss, not an error: a contract emits events outside any ABI the
    /// consumer cares about, and the transform forwards the log undecoded.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::Decode`] when an event *did* match and its data does
    /// not decode against it. That is worth surfacing rather than skipping: a
    /// mismatch usually means the ABI is the wrong version for this height, and
    /// silently dropping the log would hide exactly that.
    pub fn decode_log(&self, log: RawLog<'_>) -> Result<Option<Decoded>, RegistryError> {
        self.decode_log_as(log, "", "")
    }

    /// Decodes a log, stamping the protocol and dataset it belongs to.
    ///
    /// The caller knows these because it found the ABI through a registry entry that
    /// carries them; an [`Abi`] alone does not, which is why they are parameters rather
    /// than a lookup here.
    ///
    /// # Errors
    ///
    /// As [`Abi::decode_log`].
    pub fn decode_log_as(
        &self,
        log: RawLog<'_>,
        protocol: &str,
        dataset: &str,
    ) -> Result<Option<Decoded>, RegistryError> {
        let Some(selector) = log.topics.first() else {
            return Ok(None);
        };
        let Some(event) = self.by_selector.get(selector) else {
            return Ok(None);
        };

        let decoded = event
            .decode_log_parts(log.topics.iter().copied(), log.data)
            .map_err(|error| RegistryError::Decode {
                selector: *selector,
                detail: error.to_string(),
            })?;

        // `decode_log_parts` splits the event's inputs into indexed and non-indexed
        // exactly as the ABI declares them, so the names come from the same split.
        let (indexed_params, body_params): (Vec<_>, Vec<_>) =
            event.inputs.iter().partition(|param| param.indexed);

        Ok(Some(Decoded {
            name: event.name.clone(),
            address: log.address,
            protocol: protocol.to_owned(),
            dataset: dataset.to_owned(),
            selector: *selector,
            signature: event.signature(),
            anonymous: event.anonymous,
            transaction_hash: log.transaction_hash,
            transaction_index: log.transaction_index,
            log_index: log.log_index,
            indexed: typed_args(&indexed_params, &decoded.indexed)?,
            body: typed_args(&body_params, &decoded.body)?,
            block_number: log.block_number,
            block_hash: log.block_hash,
            block_timestamp: log.block_timestamp,
        }))
    }
}

/// Converts decoded values to named arguments, pairing each with its ABI parameter.
///
/// The decoder returns values in ABI order with no names, so the names come from the
/// event's own input list — the same split the decoder used. A length mismatch would
/// mean the decoder disagreed with the ABI about its own shape, which is a bug rather
/// than bad input, so it is an error rather than a truncation.
fn typed_args(
    params: &[&alloy_json_abi::EventParam],
    values: &[DynSolValue],
) -> Result<Vec<DecodedArg>, RegistryError> {
    if params.len() != values.len() {
        return Err(RegistryError::Shape {
            decoded: values.len(),
            declared: params.len(),
        });
    }

    params
        .iter()
        .zip(values)
        .map(|(param, value)| {
            Ok(DecodedArg {
                name: param.name.clone(),
                value: convert::value(value)?,
            })
        })
        .collect::<Result<Vec<_>, ConversionError>>()
        .map_err(RegistryError::Conversion)
}

/// Where an ABI comes from, so the transform does not know.
///
/// The height is part of the key because an ABI is valid over a block range, not
/// forever. Implement this over a table of `(address, block_range) -> ABI` for a
/// proxy-aware registry.
pub trait AbiRegistry {
    /// The ABI for `address` on `chain` as of `block`, or `None` if unknown.
    ///
    /// Must not do I/O on the hot path: a miss returns `None`, and the caller
    /// forwards the log undecoded rather than stalling the pipeline behind a lookup.
    fn abi(&self, chain: &ChainId, address: Address, block: u64) -> Option<&Abi>;

    /// What the registry knows about `address`: its protocol and its dataset.
    ///
    /// Defaults to nothing, so a registry that only answers ABIs — a test double, or a
    /// minimal implementation — stays valid. A record decoded through such a registry
    /// carries empty strings, which is honest: nothing knows what the contract is.
    fn describe(&self, _chain: &ChainId, _address: Address) -> Option<(&str, &str)> {
        None
    }
}

/// Why a log could not be decoded.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    /// The ABI could not be loaded.
    #[error("invalid ABI: {detail}")]
    Abi {
        /// What was wrong with it.
        detail: String,
    },
    /// An event matched the log's selector but the data did not decode against it.
    #[error("log {selector} does not decode against its ABI: {detail}")]
    Decode {
        /// The event selector that matched.
        selector: B256,
        /// The decoder's own explanation.
        detail: String,
    },
    /// The decoder returned a different number of values than the ABI declares.
    #[error("ABI declares {declared} arguments but {decoded} were decoded")]
    Shape {
        /// How many values the decoder produced.
        decoded: usize,
        /// How many the ABI declares.
        declared: usize,
    },
    /// A decoded value could not be published.
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    /// A registration was not `chain:address:path`.
    #[error("registration {entry:?} is not chain:address:path")]
    Registration {
        /// The entry as written.
        entry: String,
    },
    /// A registration's address did not parse.
    #[error("registration {entry:?} has an invalid address")]
    Address {
        /// The entry as written.
        entry: String,
    },
    /// An ABI file's name did not say what it decodes.
    #[error(
        "ABI file {name:?} is not {{chain}}.{{address}}.json, so nothing says which \
         contract it applies to"
    )]
    AbiName {
        /// The offending file name.
        name: String,
    },
    /// Two registry entries claim one `(chain, address)`.
    #[error("both {chain}.{address} are registered; one address decodes with one ABI")]
    Duplicate {
        /// The chain, as written.
        chain: String,
        /// The address, as written.
        address: String,
    },
    /// The ABI directory could not be read.
    #[error("cannot read ABI directory {path}: {detail}")]
    AbiDirectory {
        /// The directory.
        path: String,
        /// The underlying error.
        detail: String,
    },
}

/// A registry built from every ABI in a directory.
///
/// An ABI file is named for what it decodes: `{chain}.{address}.json`. So
/// `base.0xd0b53d9277642d899df5c87a3966a349a798f224.json` is the Base ABI for that
/// contract. The directory is scanned at startup and no list is configured, which is
/// what makes adding a contract a matter of dropping a file in.
///
/// # Why the name carries the tag
///
/// An ABI does not say which chain or contract it belongs to — the file is just the JSON
/// `cast interface --json` or Etherscan produces, byte for byte. The tag has to live
/// somewhere, and the name is the one place that needs no wrapper, so an ABI is never
/// edited to be registered.
///
/// It also means a mis-tagged ABI is a *startup* error rather than a runtime surprise. A
/// wrong ABI decodes a log into plausible values, which is worse than failing, so the
/// filename is parsed strictly and a file that does not match is refused.
///
/// # Known limitation
///
/// One ABI per `(chain, address)`, applying at every height. A proxy that upgrades
/// changes its ABI at a height, and this cannot express that. The [`AbiRegistry`] seam is
/// what a table-backed registry — keyed by `(chain, address, block_range)` — replaces
/// without the decoder changing.
///
/// # Address spelling
///
/// The key is the typed [`Address`], not its rendering, so the checksummed
/// capitalization in a filename does not matter: an address has several spellings, and
/// comparing strings silently missed every lookup until this was a typed key.
#[derive(Debug, Default)]
pub struct FileRegistry {
    entries: HashMap<(ChainId, Address), Abi>,
}

impl FileRegistry {
    /// Scans `directory` for `{chain}.{address}.json` ABIs.
    ///
    /// A missing directory yields an empty registry rather than an error, because
    /// running without decoding is legitimate: the logs pass through undecoded, and the
    /// caller says so at startup.
    ///
    /// # Errors
    ///
    /// Returns an error when a file's name is not `{chain}.{address}.json`, when its
    /// address does not parse, or when its contents are not an ABI. None of those are
    /// skipped: a file in the ABI directory that cannot be used is a mistake worth
    /// reporting, not a log worth quietly not decoding.
    pub fn from_dir(directory: impl AsRef<std::path::Path>) -> Result<Self, RegistryError> {
        let directory = directory.as_ref();
        let entries = match std::fs::read_dir(directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(error) => {
                return Err(RegistryError::AbiDirectory {
                    path: directory.display().to_string(),
                    detail: error.to_string(),
                });
            }
        };

        let mut registry = Self::default();
        for entry in entries {
            let path = entry
                .map_err(|error| RegistryError::AbiDirectory {
                    path: directory.display().to_string(),
                    detail: error.to_string(),
                })?
                .path();
            // Only `.json`, so a README or an editor's backup in the directory is not
            // mistaken for a misnamed ABI.
            if path.extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            registry.load_one(&path)?;
        }
        Ok(registry)
    }

    /// Loads one ABI from its path, taking the chain and address from the filename.
    fn load_one(&mut self, path: &std::path::Path) -> Result<(), RegistryError> {
        let named = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_owned();
        let (chain, address) = parse_abi_name(&named).ok_or_else(|| RegistryError::AbiName {
            name: named.clone(),
        })?;
        let address: Address = address.parse().map_err(|_| RegistryError::Address {
            entry: named.clone(),
        })?;
        let json = std::fs::read_to_string(path).map_err(|error| RegistryError::Abi {
            detail: format!("read {}: {error}", path.display()),
        })?;
        let abi = Abi::from_json(&json)?;
        self.entries.insert((ChainId::new(chain), address), abi);
        Ok(())
    }

    /// Whether any ABI is registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// How many ABIs are registered, for logging what was picked up.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// The `(chain, address)` pairs this registry answers for.
    pub fn entries(&self) -> impl Iterator<Item = (&ChainId, Address)> {
        self.entries
            .keys()
            .map(|(chain, address)| (chain, *address))
    }
}

/// Splits a `{chain}.{address}.json` filename into its chain and address.
///
/// Returns `None` for anything else. The address must contain a `0x` prefix, which is
/// what keeps a chain name containing a dot — and there are such chains — from being
/// split in the wrong place.
fn parse_abi_name(name: &str) -> Option<(&str, &str)> {
    let stem = name.strip_suffix(".json")?;
    // The address is the part from the last `0x`; the chain is everything before the dot
    // that precedes it.
    let address_at = stem.rfind("0x")?;
    let (chain, address) = stem.split_at(address_at);
    let chain = chain.strip_suffix('.')?;
    if chain.is_empty() || !address.starts_with("0x") {
        return None;
    }
    Some((chain, address))
}

impl AbiRegistry for FileRegistry {
    fn abi(&self, chain: &ChainId, address: Address, _block: u64) -> Option<&Abi> {
        self.entries.get(&(chain.clone(), address))
    }
}

#[cfg(test)]
// The crate denies `expect`/`unwrap` to keep production paths honest; tests are
// allowed them per the repository test style, since a failed expectation there
// means the fixture or setup is wrong.
#[expect(clippy::expect_used)]
mod tests {
    use std::sync::LazyLock;

    use alloy_primitives::{Address, B256, I256, TxHash, U256};

    use super::{Abi, AbiRegistry, FileRegistry, RawLog, RegistryError, parse_abi_name};
    use crate::wire::envelope::{ChainId, DecodedArg};
    use crate::wire::typed::TypedValue;

    /// One chain for every fixture; a registry keyed by chain is tested below.
    static CHAIN: LazyLock<ChainId> = LazyLock::new(|| ChainId::new("base"));

    /// The canonical ERC-20 event, as an ABI JSON would spell it.
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

    fn address(byte: u8) -> Address {
        Address::from([byte; 20])
    }

    /// `keccak256("Transfer(address,address,uint256)")`. Pinned as a constant rather
    /// than computed, so a change in how the selector is derived shows up as a
    /// failure here rather than as a silent mismatch against real chain data.
    fn transfer_selector() -> B256 {
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
            .parse()
            .expect("selector parses")
    }

    /// A real `Transfer` log: two indexed addresses as topics, the amount as data.
    fn transfer_log() -> (Vec<B256>, Vec<u8>) {
        let mut from = [0u8; 32];
        from[12..].copy_from_slice(&[0x11; 20]);
        let mut to = [0u8; 32];
        to[12..].copy_from_slice(&[0x22; 20]);
        let value = U256::from(1_000_000_000_000_000_000u64).to_be_bytes::<32>();
        (
            vec![transfer_selector(), B256::from(from), B256::from(to)],
            value.to_vec(),
        )
    }

    fn raw_log<'a>(topics: &'a [B256], data: &'a [u8]) -> RawLog<'a> {
        RawLog {
            chain: &CHAIN,
            address: address(0xaa),
            topics,
            data,
            transaction_hash: TxHash::from([0x01; 32]),
            transaction_index: 1,
            log_index: 3,
            block_number: 100,
            block_hash: B256::from([0x02; 32]),
            block_timestamp: 1_700_000_000,
        }
    }

    /// The happy path: a real log decodes into named, typed arguments, and the
    /// record links back to the exact raw log it came from.
    #[test]
    fn a_transfer_log_decodes_into_typed_arguments() {
        let abi = Abi::from_json(ERC20).expect("ABI loads");
        let (topics, data) = transfer_log();
        let decoded = abi
            .decode_log(raw_log(&topics, &data))
            .expect("log decodes")
            .expect("an event matched");

        assert_eq!(decoded.name, "Transfer");
        assert_eq!(decoded.address, address(0xaa));
        assert_eq!(decoded.selector, transfer_selector());
        assert_eq!(decoded.signature, "Transfer(address,address,uint256)");
        assert_eq!(decoded.indexed.len(), 2);
        assert_eq!(decoded.body.len(), 1);
        // Each argument carries the ABI's own name, so a store can address it rather
        // than count positions.
        assert_eq!(
            decoded.indexed,
            vec![
                DecodedArg {
                    name: "from".to_owned(),
                    value: TypedValue::Address {
                        value: address(0x11)
                    },
                },
                DecodedArg {
                    name: "to".to_owned(),
                    value: TypedValue::Address {
                        value: address(0x22)
                    },
                },
            ]
        );
        assert_eq!(
            decoded.body,
            vec![DecodedArg {
                name: "value".to_owned(),
                value: TypedValue::Uint {
                    value: U256::from(1_000_000_000_000_000_000u64),
                    bits: 256,
                },
            }]
        );
        // The source is the raw log's natural key, so a store can join back to it.
        assert_eq!(
            decoded.source_key(),
            format!("100:{}:3", TxHash::from([0x01; 32]))
        );
        assert_eq!(decoded.block_number, 100);
        assert_eq!(decoded.block_timestamp, 1_700_000_000);
    }

    /// A log whose selector the ABI does not declare is a miss, not an error: the
    /// transform forwards it undecoded rather than failing the batch.
    #[test]
    fn an_unknown_selector_is_a_miss_not_an_error() {
        let abi = Abi::from_json(ERC20).expect("ABI loads");
        let topics = vec![B256::from([0x99; 32])];
        assert!(
            abi.decode_log(raw_log(&topics, &[]))
                .expect("a miss is not an error")
                .is_none()
        );
    }

    /// A log with no topics at all, which is legal for an anonymous event, is also a
    /// miss rather than a panic.
    #[test]
    fn a_log_with_no_topics_is_a_miss() {
        let abi = Abi::from_json(ERC20).expect("ABI loads");
        assert!(
            abi.decode_log(raw_log(&[], &[]))
                .expect("a miss is not an error")
                .is_none()
        );
    }

    /// A selector that matches but data that is too short to decode is an error,
    /// because the likely cause is an ABI from the wrong block range and hiding it
    /// would publish the wrong contract's values.
    #[test]
    fn a_selector_that_matches_with_undecodable_data_is_an_error() {
        let abi = Abi::from_json(ERC20).expect("ABI loads");
        let (topics, _) = transfer_log();
        // Half a word where the event declares a `uint256`.
        let short = [0u8; 16];
        assert!(matches!(
            abi.decode_log(raw_log(&topics, &short)),
            Err(RegistryError::Decode { .. })
        ));
    }

    /// An ABI that is not JSON fails loudly at load, not at first decode.
    #[test]
    fn malformed_abi_json_is_rejected_at_load() {
        assert!(matches!(
            Abi::from_json("not an abi"),
            Err(RegistryError::Abi { .. })
        ));
    }

    /// A regression test against a real Uniswap V3 `Swap` log captured from Base.
    ///
    /// Every other test here builds its own bytes, so they would all still pass if
    /// the decoder agreed with itself about a layout the chain does not use. This one
    /// pins the layout to the chain: the `int256` amounts are signed, the pool's
    /// `uint160` price is read at 160 bits, and the trailing `int24` tick is not
    /// silently widened.
    #[test]
    fn a_real_uniswap_v3_swap_log_decodes_with_signed_amounts() {
        let abi = Abi::from_json(include_str!("../../abis/uniswap_v3_pool.json"))
            .expect("the pool ABI loads");
        let topic0: B256 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
            .parse()
            .expect("selector parses");
        let topic1: B256 = "0x0000000000000000000000006ff5693b99212da76ad316178a184ab56d299b43"
            .parse()
            .expect("topic parses");
        let data = hex_bytes(
            "fffffffffffffffffffffffffffffffffffffffffffffffffff4b34627fb9302\
             0000000000000000000000000000000000000000000000000000000000830544\
             00000000000000000000000000000000000000000003678007a6bbf505d858fa\
             00000000000000000000000000000000000000000000000012fb062ae6731f9d\
             fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcfd3b",
        );
        let log_topics = vec![topic0, topic1, topic1];

        let decoded = abi
            .decode_log(RawLog {
                chain: &ChainId::new("base"),
                address: address(0xd0),
                topics: &log_topics,
                data: &data,
                transaction_hash: TxHash::from([0x2a; 32]),
                transaction_index: 12,
                log_index: 767,
                block_number: 51_913_794,
                block_hash: B256::from([0xd4; 32]),
                block_timestamp: 1_700_000_000,
            })
            .expect("the log decodes")
            .expect("the ABI declares Swap");

        assert_eq!(decoded.name, "Swap");
        assert_eq!(
            decoded.signature,
            "Swap(address,address,int256,int256,uint160,uint128,int24)"
        );
        // Every argument is named from the ABI, which is what lets a store map
        // `amount0` to a column instead of counting positions.
        let names: Vec<&str> = decoded
            .indexed
            .iter()
            .chain(&decoded.body)
            .map(|arg| arg.name.as_str())
            .collect();
        assert_eq!(
            names,
            [
                "sender",
                "recipient",
                "amount0",
                "amount1",
                "sqrtPriceX96",
                "liquidity",
                "tick"
            ]
        );

        // `amount0` is negative: this swap sold token0, and reading the two's
        // complement word as unsigned would produce 1.15e77 instead.
        assert_eq!(
            decoded.body.first().map(|arg| &arg.value),
            Some(&TypedValue::Int {
                value: I256::try_from(-3_180_585_820_646_654_i64).expect("fits"),
                bits: 256,
            })
        );
        assert_eq!(
            decoded.body.get(1).map(|arg| &arg.value),
            Some(&TypedValue::Int {
                value: I256::try_from(8_586_564_i64).expect("fits"),
                bits: 256,
            })
        );
        // `sqrtPriceX96` is `uint160`, not `uint256`.
        assert_eq!(
            decoded.body.get(2).and_then(rebuild_type),
            Some(alloy_dyn_abi::DynSolType::Uint(160))
        );
        assert_eq!(
            decoded.body.get(4).and_then(rebuild_type),
            Some(alloy_dyn_abi::DynSolType::Int(24))
        );
        // The link back to the raw log is its natural key.
        assert_eq!(
            decoded.source_key(),
            format!("51913794:{}:767", TxHash::from([0x2a; 32]))
        );
    }

    /// The Solidity type a published argument declares, for asserting a width.
    fn rebuild_type(arg: &DecodedArg) -> Option<alloy_dyn_abi::DynSolType> {
        crate::decode::convert::dyn_type(&arg.value).ok()
    }

    /// Decodes a hex string with whitespace, so a long fixture stays readable.
    fn hex_bytes(hex: &str) -> Vec<u8> {
        let compact: String = hex.chars().filter(|c| !c.is_whitespace()).collect();
        alloy_primitives::hex::decode(compact).expect("fixture is valid hex")
    }

    /// The registry is keyed by chain as well as address, so the same address on two
    /// chains does not resolve to one ABI.
    #[test]
    fn the_registry_is_keyed_by_chain_and_address() {
        struct OneAbi {
            base: Abi,
        }

        impl AbiRegistry for OneAbi {
            fn abi(&self, chain: &ChainId, contract: Address, _block: u64) -> Option<&Abi> {
                (chain.as_str() == "base" && contract == address(0xaa)).then_some(&self.base)
            }
        }

        let registry = OneAbi {
            base: Abi::from_json(ERC20).expect("ABI loads"),
        };
        let base = ChainId::new("base");
        let other = ChainId::new("ethereum");

        assert!(registry.abi(&base, address(0xaa), 100).is_some());
        assert!(registry.abi(&base, address(0xbb), 100).is_none());
        assert!(registry.abi(&other, address(0xaa), 100).is_none());
    }

    /// An ABI file is named for what it decodes, and a chain name containing a dot must
    /// not be split at the wrong one.
    #[test]
    fn an_abi_filename_yields_its_chain_and_address() {
        assert_eq!(
            parse_abi_name("base.0xd0b53d9277642d899df5c87a3966a349a798f224.json"),
            Some(("base", "0xd0b53d9277642d899df5c87a3966a349a798f224"))
        );
        // A dotted chain name still splits at the address, which is why the parse looks
        // for the last `0x` rather than the first dot.
        assert_eq!(
            parse_abi_name("arbitrum.nova.0x1111111111111111111111111111111111111111.json"),
            Some((
                "arbitrum.nova",
                "0x1111111111111111111111111111111111111111"
            ))
        );
    }

    /// A file in the ABI directory that does not say what it decodes is refused rather
    /// than skipped. A wrong ABI produces plausible values, which is worse than not
    /// decoding, so a misnamed file must be a startup error.
    #[test]
    fn a_misnamed_abi_file_is_refused() {
        assert_eq!(parse_abi_name("uniswap_v3_pool.json"), None);
        assert_eq!(parse_abi_name("base.json"), None);
        assert_eq!(
            parse_abi_name("base.0xnothex.json"),
            Some(("base", "0xnothex"))
        );
        assert_eq!(parse_abi_name("base.0xabc.txt"), None);
        assert_eq!(parse_abi_name(".0xabc.json"), None);
        assert_eq!(parse_abi_name("base.abc.json"), None);
    }

    /// Discovery loads every tagged ABI in a directory, so adding a contract is dropping
    /// a file in rather than editing a list.
    #[test]
    fn a_directory_is_discovered_without_a_list() {
        let dir = std::env::temp_dir().join(format!("abi-dir-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        // Two ABIs for one contract on two chains, plus a non-JSON file that must be
        // ignored rather than treated as a misnamed ABI.
        std::fs::write(
            dir.join("base.0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json"),
            ERC20,
        )
        .expect("write base ABI");
        std::fs::write(
            dir.join("ethereum.0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.json"),
            ERC20,
        )
        .expect("write ethereum ABI");
        std::fs::write(dir.join("README.md"), "not an abi").expect("write readme");

        let registry = FileRegistry::from_dir(&dir).expect("the directory is discovered");
        assert_eq!(registry.len(), 2);
        // The checksummed spelling in the second filename resolves to the same address
        // as the lowercase one, because the key is the typed address.
        assert!(
            registry
                .abi(&ChainId::new("base"), address(0xaa), 1)
                .is_some()
        );
        assert!(
            registry
                .abi(&ChainId::new("ethereum"), address(0xaa), 1)
                .is_some()
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A missing directory is an empty registry, not an error: running without decoding
    /// is legitimate, and the caller says so at startup.
    #[test]
    fn a_missing_directory_is_an_empty_registry() {
        let registry = FileRegistry::from_dir("/nonexistent/abi/dir").expect("missing is empty");
        assert!(registry.is_empty());
    }
}
