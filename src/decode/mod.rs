//! Decoding a log against a contract ABI.
//!
//! The second stage: it consumes raw records from `raw.chain` and publishes the decoded
//! ones to `decoded.chain`. This module is the whole stage.
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
//! - [`run`] — **the runtime.** A loop over a source and a sink, driving the transform and
//!   applying any discovery the transform surfaces, between records.
//!
//! The direction is one-way: `run` drives `transform`, which reads `registry`, which holds
//! `abi`. Nothing below knows about the layer above it — `Abi` cannot see a chain, and the
//! transform cannot open a socket.
//!
//! # Flow
//!
//! One raw envelope goes through the stage like this:
//!
//! ```text
//!                       raw.chain
//!                           │
//!                    ┌──────▼───────┐
//!                    │  run         │  owns the registry, drives the loop
//!                    └──────┬───────┘
//!                           │  envelope
//!                    ┌──────▼───────┐
//!                    │  transform   │
//!                    └──────┬───────┘
//!         not a Log ────────┤
//!         (block, tx,       │  a Log
//!          receipt)   ┌─────▼──────────────┐
//!         dropped     │ registry.contract  │──── miss ──▶ dropped
//!                     └─────┬──────────────┘
//!                           │  Contract { abi, protocol }
//!                     ┌─────▼──────────────┐
//!                     │ abi.decode_log     │──── no selector ──▶ dropped
//!                     └─────┬──────────────┘
//!                           │  DecodedEvent
//!                     ┌─────▼──────────────┐
//!                     │ registry.discovery │──── no rule ──▶ none
//!                     └─────┬──────────────┘
//!                           │  Discovery { child, abi, protocol }
//!                           ▼
//!                  Decoded record ──▶ decoded.chain
//!                  Discovery      ──▶ registry.register_discovered  (in run, next record)
//! ```
//!
//! Three outcomes for a log, and only the first publishes:
//!
//! 1. **Decoded** — the address is registered, the ABI declares the log's selector, and
//!    the data decodes. The record is published; a discovery effect, if the event matched
//!    a rule, is handed back to `run`.
//! 2. **Missed** — no ABI for the address, or no such event on the ABI. Dropped silently:
//!    the raw log is already on `raw.chain`, so nothing is lost.
//! 3. **Failed** — an event matched but the data did not decode. Dropped, with the failure
//!    reported on [`Applied::error`] so `run` logs it. Almost always an ABI from the wrong
//!    block range.
//!
//! Control signals ([`Reorg`](crate::wire::envelope::Event::Reorg) and
//! [`Finalized`](crate::wire::envelope::Event::Finalized)) are forwarded verbatim, because
//! a store retracts orphaned rows and compacts below the watermark on them. Everything
//! else — block, transaction, receipt — has no decoded form and is dropped; the raw topic
//! is its home.
//!
//! # Discovery, in one paragraph
//!
//! A pool created by a factory is not in the registry file, because it did not exist
//! when that file was written. A discovery rule closes that gap: when a factory's
//! creation event decodes, the rule names the argument holding the new child address and
//! what ABI it decodes with. `run` applies the registration immediately, in the same
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
//! This module may depend on [`crate::connectors`] and [`crate::wire`]. It deliberately
//! knows nothing about [`crate::ingest`]: the ordering and reorg state machine lives
//! there, and a decode that needed it would be a decode that has to reimplement it.
//! The layers are modules, so nothing enforces that direction; it is on a reviewer.

pub mod abi;
pub mod registry;
pub mod run;
pub mod transform;

pub use abi::{Abi, DecodeError, DecodedEvent};
pub use registry::{
    AbiEntry, AbiRegistry, Contract, ContractEntry, ContractRegistry, Discovery, DiscoveryEntry,
    RegistryConfig, RegistryError,
};
pub use run::{Decode, DecodeBuilder};
pub use transform::{Applied, Transform};
