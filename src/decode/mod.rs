//! Decoding a log against a contract ABI.
//!
//! The second layer: it sits between ingest and storage as a wrapper around a sink,
//! adding a decoded record after each raw log it can decode. It runs inline — ingest's
//! call to publish an envelope is the call that decodes it — so there is no queue
//! between the two. This module is the whole layer.
//!
//! # Architecture
//!
//! Four parts, each a separate concern:
//!
//! - [`abi`] — **one contract's ABI, decoding one log.** Turns a log's topics and data
//!   into a [`DecodedEvent`]: the event's name and its named, typed arguments. Knows
//!   nothing about the wire beyond the log itself. This is also the only place in the
//!   crate that knows alloy's value model, converting it to the wire's
//!   [`TypedValue`](crate::wire::typed::TypedValue).
//! - [`registry`] — **which ABI applies where.** Maps `(chain, address)` to a shared
//!   [`Abi`], and holds the discovery rules that learn new addresses at runtime. This is
//!   where the [settings file](crate::config) and the [registry file](ContractRegistry::from_file)
//!   become data.
//! - [`transform`] — **the pure function.** One envelope in, its decoded record or a
//!   forwarded control signal out. No I/O, no state; the registry is passed per call.
//! - [`sink`] — **the runtime.** A wrapper around another sink that drives the transform
//!   on each envelope, applies any discovery the transform surfaces between records, and
//!   forwards the raw envelope and its decoded record to the sink it wraps.
//!
//! The direction is one-way: `sink` drives `transform`, which reads `registry`, which holds
//! `abi`. Nothing below knows about the layer above it — `Abi` cannot see a chain, and the
//! transform cannot open a socket.
//!
//! # Flow
//!
//! One raw envelope goes through the stage like this:
//!
//! ```text
//!                      envelope (from ingest)
//!                           │
//!                    ┌──────▼───────┐
//!                    │  sink        │  owns the registry, forwards every envelope
//!                    └──────┬───────┘
//!                           │  &envelope
//!                    ┌──────▼───────┐
//!                    │  transform   │
//!                    └──────┬───────┘
//!         not a Log ────────┤
//!         (block, tx,       │  a Log
//!          receipt,   ┌─────▼──────────────┐
//!          marker)    │ registry.contract  │──── miss ──▶ nothing added
//!         nothing     └─────┬──────────────┘
//!         added             │  Contract { abi, protocol }
//!                     ┌─────▼──────────────┐
//!                     │ abi.decode_log     │──── no selector ──▶ nothing added
//!                     └─────┬──────────────┘
//!                           │  DecodedEvent
//!                     ┌─────▼──────────────┐
//!                     │ registry.discovery │──── no rule ──▶ none
//!                     └─────┬──────────────┘
//!                           │  Discovery { child, abi, protocol }
//!                           ▼
//!                  Decoded record ──▶ inner sink, right after the raw log
//!                  Discovery      ──▶ registry.register_discovered  (in sink, next record)
//! ```
//!
//! Three outcomes for a log, and only the first adds a record:
//!
//! 1. **Decoded** — the address is registered, the ABI declares the log's selector, and
//!    the data decodes. The record follows the raw log; a discovery effect, if the event
//!    matched a rule, is handed back to `sink`.
//! 2. **Missed** — no ABI for the address, or no such event on the ABI. Nothing is added,
//!    silently: the raw log is forwarded regardless, so nothing is lost.
//! 3. **Failed** — an event matched but the data did not decode. Nothing is added, with
//!    the failure reported on [`Applied::error`] so `sink` logs it. Almost always an ABI
//!    from the wrong block range.
//!
//! Every envelope is forwarded as it arrived, so decode never removes anything from the
//! stream: a reorg or finality marker reaches the store once, from ingest, and a block,
//! transaction, or receipt — which has no decoded form — passes through untouched.
//!
//! # Discovery, in one paragraph
//!
//! A pool created by a factory is not in the registry file, because it did not exist
//! when that file was written. A discovery rule closes that gap: when a factory's
//! creation event decodes, the rule names the argument holding the new child address and
//! what ABI it decodes with. `sink` applies the registration immediately, in the same
//! sequential pass. That is deterministic — the child's ABI is already loaded, so no
//! network is involved — and correct because a factory emits its creation event before
//! the child emits anything, so the child is registered before its first log is read.
//!
//! # What this deliberately does not do
//!
//! It produces *facts*: an argument's name and its typed value, straight off the ABI. It
//! does not reshape them into a dataset, because that needs context this stage does not
//! have — which token a pool trades, how many decimals it has, which table a swap belongs
//! to. Those are joins against reference data, so the projection belongs where the joins
//! are.
//!
//! That is why there is no per-protocol mapper here. A decoder that knew what `amount0`
//! *means* would be a decoder that has to know every protocol.
//!
//! # Dependency direction
//!
//! This module may depend on [`crate::sink`] and [`crate::wire`]. It deliberately
//! knows nothing about [`crate::ingest`]: the ordering and reorg state machine lives
//! there, and a decode that needed it would be a decode that has to reimplement it.
//! The layers are modules, so nothing enforces that direction; it is on a reviewer.

pub mod abi;
pub mod registry;
pub mod sink;
pub mod transform;

pub use abi::{Abi, DecodeError, DecodedEvent};
pub use registry::{
    AbiEntry, AbiRegistry, Contract, ContractEntry, ContractRegistry, Discovery, DiscoveryEntry,
    RegistryConfig, RegistryError,
};
pub use sink::DecodingSink;
pub use transform::{Applied, Transform};
