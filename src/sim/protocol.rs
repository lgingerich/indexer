//! A small synthetic protocol the simulated chain emits and the catalog decodes.
//!
//! A seeded factory announces pools with `PoolCreated(address indexed poolAddress)`, and
//! pools emit `Swap`. The pools are a handful of fixed addresses, so a pool is created on
//! one branch and not another, created twice, or swaps before it is created, which is
//! when its swaps must not decode. The manifest is written under the name `metric_v1`
//! with a contract named `MetricOmmPool` whose `Swap` has the exact signature the
//! `metric_v1_swaps` derivation pins, so the catalog declares that derived table too and
//! every swap derives a row: decode, discovery, its retraction on a reorg, and one
//! derivation all run.

use std::path::{Path, PathBuf};

use alloy_primitives::{Address, B256, Bytes, keccak256};

use super::Rng;
use super::chain::SimLog;

/// The protocol's name, as the manifest gives it and decoded records carry it.
pub(super) const PROTOCOL: &str = "metric_v1";

/// The factory, seeded for the simulated chain.
pub(super) const FACTORY: Address = Address::repeat_byte(0xfa);

/// Every address a pool can have.
const POOLS: [Address; 3] = [
    Address::repeat_byte(0xa1),
    Address::repeat_byte(0xa2),
    Address::repeat_byte(0xa3),
];

/// The factory's `PoolCreated` topic.
pub(super) fn created() -> B256 {
    keccak256("PoolCreated(address)")
}

/// A pool's `Swap` topic, the signature `metric_v1_swaps` reads.
pub(super) fn swap() -> B256 {
    keccak256("Swap(address,address,uint256,uint256,uint256)")
}

const FACTORY_ABI: &str = r#"[{"type":"event","name":"PoolCreated","anonymous":false,
"inputs":[{"name":"poolAddress","type":"address","indexed":true}]}]"#;

const POOL_ABI: &str = r#"[{"type":"event","name":"Swap","anonymous":false,"inputs":[
{"name":"sender","type":"address","indexed":true},
{"name":"recipient","type":"address","indexed":true},
{"name":"details","type":"uint256","indexed":false},
{"name":"amountDeltas","type":"uint256","indexed":false},
{"name":"platformFees","type":"uint256","indexed":false}]}]"#;

/// Writes the protocol's manifest and ABIs under `dir`, returning the directory the
/// catalog loads.
pub(super) fn write(dir: &Path) -> PathBuf {
    let protocol = dir.join("protocols");
    std::fs::create_dir_all(&protocol).expect("protocol directory");
    let manifest = format!(
        "protocol = \"{PROTOCOL}\"\n\n\
         [[contract]]\nabi = [\"Factory.json\"]\naddresses = {{ {chain} = [\"{FACTORY:#x}\"] }}\n\n\
         [[contract]]\nabi = [\"MetricOmmPool.json\"]\ntable = \"pool\"\n\
         created_by = [{{ contract = \"Factory\", event = \"PoolCreated\", param = \"poolAddress\" }}]\n",
        chain = super::CHAIN,
    );
    for (file, text) in [
        ("protocol.toml", manifest.as_str()),
        ("Factory.json", FACTORY_ABI),
        ("MetricOmmPool.json", POOL_ABI),
    ] {
        std::fs::write(protocol.join(file), text).expect("protocol file");
    }
    protocol
}

/// A log of the protocol, or `None` for one of the chain's other logs.
pub(super) fn log(rng: &mut Rng, transaction_hash: B256) -> Option<SimLog> {
    let pool = POOLS[usize::try_from(rng.below(POOLS.len() as u64)).unwrap_or(0)];
    match rng.below(10) {
        0 => Some(SimLog {
            address: FACTORY,
            topics: vec![created(), pool.into_word()],
            data: Bytes::new(),
            transaction_hash,
        }),
        1..=3 => {
            let mut data = Vec::with_capacity(96);
            for _ in 0..3 {
                data.extend_from_slice(rng.b256().as_slice());
            }
            Some(SimLog {
                address: pool,
                topics: vec![
                    swap(),
                    Address::repeat_byte(0x5e).into_word(),
                    Address::repeat_byte(0x7e).into_word(),
                ],
                data: data.into(),
                transaction_hash,
            })
        }
        _ => None,
    }
}
