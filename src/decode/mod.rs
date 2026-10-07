//! Schema-aware ABI event decoding with factory discovery.
//!
//! Protocol manifests (see [`Catalog`]) name each protocol's contract kinds, their ABIs,
//! their seed addresses per chain, and the creation events that discover more. The
//! [`Decoder`] decodes a log only when its address is a known contract — a seed or a
//! discovered child — and stamps the record with that contract's protocol. Decoding is
//! *attributed*: a log whose signature matches a known event but whose address is not a
//! known contract is not decoded, because anyone can deploy a contract that emits a
//! lookalike event.
//!
//! [`DecodingSink`] runs before the storage channel and forwards raw records regardless
//! of ordinary decode failures. Discovered contracts are published as their own dataset,
//! so the store holds them in the same transaction as the block that created them and a
//! restart reads them back.

mod abi;
mod catalog;
mod decoder;
mod sink;

pub use abi::{Abi, AbiError, DecodeError, DecodedEvent};
pub use catalog::{Catalog, CatalogError};
pub use decoder::{Decoder, Decoding, StoredContract};
pub use sink::DecodingSink;
