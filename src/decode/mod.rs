//! Schema-aware ABI event decoding with immutable, predefined registrations.
//!
//! ABIs are validated and prepared at startup. The registry selects an ABI by chain,
//! address and original block height; protocol identity is separate from ABI identity.
//! [`Decoder`] is shared by live ingestion and append-only stored-log replay.
//!
//! [`DecodingSink`] runs before the storage channel and forwards raw records regardless
//! of ordinary decode failures. Dynamic discovery, automatic ABI resolution, within-block
//! upgrades and canonical-chain/reorg handling are deliberately not implemented.

mod abi;
mod decoder;
mod registry;
mod sink;

pub use abi::{Abi, AbiError, DecodeError, DecodedEvent};
pub use decoder::Decoder;
pub use registry::{
    AbiEntry, Contract, ContractEntry, ContractRegistry, RegistryConfig, RegistryError,
};
pub use sink::DecodingSink;
