# indexer

A low-latency, chain-agnostic blockchain indexer.

It reads a chain's live tip, parses each block into ordered events, decodes the logs
it has ABIs for, and stores everything in a local database. Chain-specific knowledge sits
behind one trait and where events go behind another, so new chains and new stores are
additions rather than rewrites.

## Architecture

```
chain (RPC) ─▶ ingest ─▶ decode ─▶ channel ═══▶ drain ─▶ DuckDB
              └────────── task 1 ──────────┘     └──── task 2 ────┘
                                        bounded, in memory,
                                        one block per message
```

Each block takes the same path:

1. **Ingest** hears a new head over WebSocket, fetches the block with its transactions,
   receipts and logs in one batched request, and turns it into ordered events. It gives
   every event a per-chain `sequence`, checks parent-hash linkage against a bounded undo
   ring, and emits a `reorg` marker when the chain forks and a `finalized` marker when
   finality advances.
2. **Decode** sees each envelope as ingest publishes it. A log whose address is in the
   contract registry is decoded against its ABI, and the decoded record is forwarded right
   after the raw log. Everything else passes through untouched.
3. **The channel** collects the block's envelopes and sends them to storage as one message
   when ingest flushes at the block boundary.
4. **Storage** writes the block's raw and decoded rows to DuckDB in one commit.

Three choices shape it:

**Ingest and decode are direct calls, not a queue.** `DecodingSink` wraps another sink:
ingest publishes an envelope, and the same call decodes it and forwards both the raw
envelope and its decoded record. Decoding a block takes a small fraction of a block time,
so there is nothing to decouple.

**The channel is the one queue, and it exists for the store.** A store stalls — a
checkpoint, a slow fsync — and ingest must not stop reading heads while it does. The
channel holds a few dozen blocks; when it is full, ingest waits, so lag is felt instead
of buffered. A block travels as one message, so its raw rows and decoded rows commit
together. When the store has fallen behind it folds the waiting blocks into one commit
(`runtime.batch_records`), which is how it catches up.

**The channel is not durable, and does not need to be.** The chain is the record upstream
and the store is the record downstream; a crash loses only what was in flight, and the
store's high-water mark says where to resume. Resuming from it is not built yet, and is the
first thing that needs to be.

## Layout

One crate, one binary, four layers as modules. What runs is not hardcoded: the settings
file states it, and `runtime` assembles the pipeline — ingest follows the configured chain,
decode uses the configured registry, storage writes the configured store. `main` is only
the process boundary. The layers share one definition of the stream through `wire`.

```
src/
├── main.rs         the process boundary: logging, the settings path, the exit code
├── runtime.rs      assembles the pipeline the settings describe, and runs it
├── config.rs       typed settings — layer configuration, not string lookups
├── wire/           the wire contract: envelope, events, dataset records. Pure data.
├── ingest/         block sources and the reorg-aware pipeline.
├── decode/         the stateless ABI decode transform, its registry, and the sink that applies it.
└── sink/           where envelopes go: the sink trait, the decode→storage channel, the store, stdout.
```

`wire` depends on `alloy` and `serde` and on nothing else in the tree. `decode`
knows nothing about `ingest`, so it cannot reach into the pipeline's ordering and reorg
state machine.

The layers are modules, so their one-way dependency direction is a convention a
reviewer checks rather than one the compiler enforces. At this size, one owner reads
all of it, and each module documents the direction it may depend in. CI checks the one
that matters most: `decode` must not depend on `ingest`.

## What works

- **EVM ingestion.** Live heads over WebSocket (`eth_subscribe`/`newHeads`), and
  each block fetched over JSON-RPC with full transactions, receipts, and logs in
  one batched request. Nothing the node returns is dropped. See
  `src/ingest/source/evm.rs`.
- **One event per dataset.** A `block` event, then for each transaction a
  `transaction` event, its `receipt` event, and its `log` events. Each dataset is a
  normalized table — a block references its transactions by hash, a receipt carries
  only the count of its logs — so no field is published twice and a row maps to a
  persistence row. See `src/wire/datasets/evm.rs`.
- **Ordered events.** Every event gets a per-chain monotonic `sequence`; every
  dataset exposes a `dedupe_key` derived from its natural key. See
  `src/wire/envelope.rs`.
- **A decode stage.** `src/decode` is a stateless transform over an ABI registry, run
  inline by `DecodingSink`. It decodes each registered log against the ABI for its
  `(chain, address)` and stores the decoded record right after the raw log. Everything is
  forwarded as it arrived — decode adds records and removes none — so a reorg or finality
  marker reaches the store exactly once, from ingest. It reimplements no ordering or reorg
  logic, so decoding a log again is safe: the same log gives the same record and the same
  `dedupe_key`.
- **A local store.** The process writes every envelope, raw and decoded, into a local
  `DuckDB` database: one append-only `events` table.
- **Finality watermark.** A `finalized` event says a block and everything below
  it are permanent. Its height comes from the node's own `finalized` tag, so each
  chain's rules apply with no confirmation count to tune; on Base it trails the
  tip by about 600 blocks. It is published only when it advances.
- **Reorg retraction.** Parent-hash linkage is checked on every block. A mismatch
  publishes a `reorg` event whose `orphaned_hashes` say what was retracted, and
  reclaims the sequence numbers those blocks had used. The undo window is bounded
  (128 blocks by default, and finalized blocks are dropped first). See
  `src/ingest/pipeline.rs`.
- **NDJSON to stdout.** See `src/sink/stdout.rs`; `[ingest] stdout = true` prints the
  stream instead of storing it.
- **A local `DuckDB` store**, behind the `duckdb` feature (on by default). Embedded and
  single-writer, so it is the archive; anything downstream reads from it rather than from
  the live stream.

`duckdb` is on by default because the pipeline needs a store to run. No broker is involved
anywhere, so `cargo run` alone runs the pipeline.

## Not built yet

Resume from the store's high-water mark (and rebuilding the undo ring from it), backfill
and the backfill-to-live handoff, mempool, filtered subscriptions, derived state
(balances/nonces), the aggregation layer, and the Parquet/GCS archiver. The crate is a
walking skeleton: it indexes forward from whatever the chain does next, and a restart
begins at the live tip again rather than where it stopped.

## Run it

One binary runs the pipeline the settings describe: ingest follows the chain, decode adds
a record for each log it has an ABI for, and storage writes both into the store. Ingest and
decode run together in one task and storage in another; a part that ends for good stops the
process, because continuing without it would leave a stream that looks alive but is not.

- **`[ingest]` is required** — it names the chain to follow.
- **Decode uses `[decode] registry`.** An absent or empty registry decodes nothing, which
  is a legitimate way to run and is said at startup.
- **Storage is the `[storage]` backend.** With `[ingest] stdout = true` no store is opened
  and the stream is printed instead.

Settings come from a TOML file, named by the first argument or defaulting to
`indexer.toml`. `indexer.toml` in the repository is a working example. What decode decodes
lives in a separate registry file, named by `[decode] registry` and defaulting to nothing —
`registry.toml` is a working example; see [The contract registry](#the-contract-registry).

```bash
RUST_LOG=info cargo run --release -- indexer.toml
```

```toml
[ingest]
chain = "base"
http_url = "https://base-rpc.publicnode.com"
ws_url = "wss://base-rpc.publicnode.com"

[storage]
kind = "duckdb"

[storage.duckdb]
path = "indexer.duckdb"

[decode]
registry = "registry.toml"
```

Each table names what owns its fields, not where a field was first needed:

- `[ingest]` — the chain to follow and its endpoints.
- `[runtime]` — how storage commits: the most records one commit may cover.
- `[storage.<kind>]` — one backend's own settings. `[storage] kind` selects it, so a
  second backend (ClickHouse, Postgres) is a variant there plus its own table, not
  another top-level `database` whose owner a reader has to guess.
- `[decode]` — the contract registry.

**Required** — no value could be right by accident, so each errors at startup naming
the field:

| Key | Meaning |
| --- | --- |
| `ingest.chain` | Chain id stamped on every event |
| `ingest.http_url` | JSON-RPC endpoint for blocks and receipts |
| `ingest.ws_url` | WebSocket endpoint for `newHeads` |

**Defaulted** — correct for a standard deployment; override for a non-standard one:

| Key | Default | Meaning |
| --- | --- | --- |
| `ingest.stdout` | `false` | Print the stream instead of storing it; no store is opened |
| `runtime.batch_records` | `500` | Most records one store commit may cover; a backlog of blocks is folded into one commit up to this, and a block is never split |
| `storage.kind` | `duckdb` | The store backend |
| `storage.duckdb.path` | `indexer.duckdb` | Path to the store |

**Optional tables:**

| Key | Meaning |
| --- | --- |
| `decode.registry` | Path to the contract registry file, relative to the settings file. Absent means nothing is decoded |
| `storage.duckdb.settings` | Any other DuckDB setting, passed straight through |

`RUST_LOG` is still read from the environment, because a log filter is not deployment
configuration.

An unknown key is a startup error naming the line and the key, so a misspelling is
caught rather than silently leaving a setting at its default. An unknown `storage.kind`
is likewise an error rather than a silent fallback to the default backend.

### The contract registry

What decode decodes lives in its own file, named by `[decode] registry` — `registry.toml`
in the repository is a working example. It is deliberately not part of `indexer.toml`: the
settings file is deployment topology (endpoints, paths), while the registry is a
catalog of contracts that grows on its own schedule. Splitting them keeps a new protocol
from churning the deployment diff.

The registry file holds three lists, all data:

```toml
[[abi]]
name = "uniswap_v3_pool"
path = "abis/uniswap_v3_pool.json"

[[contract]]
chain = "base"
address = "0x1F98431c8aD98523631AE4a59f267346ea31F984"   # V3 factory
abi = "uniswap_v3_factory"

[[discovery]]
chain = "base"
address = "0x1F98431c8aD98523631AE4a59f267346ea31F984"   # the V3 factory
event = "PoolCreated(address,address,uint24,int24,address)"
child = "pool"           # the decoded argument holding the new pool address
abi = "uniswap_v3_pool"  # what the child decodes with, and its protocol tag
```

**Why an ABI is named once.** A pool protocol like Uniswap V3 has thousands of pools
sharing one ABI. The ABI is loaded once and shared; an address is a line.

**Discovery.** A pool created at runtime is not in the registry file, so it cannot be a
`[[contract]]`. A rule closes that: when the factory's creation event decodes, the named
argument holds the child's address, and the child is registered with the named ABI.
Registration is deterministic — the child's ABI is already loaded, so no network is
involved — and a factory emits its creation event before the child emits anything, so a
sequential pass registers a child before its first log.

A protocol that does not put its pools at an address needs no rule: **Uniswap V4's
`PoolManager` is a single `[[contract]]`**, because a V4 pool is a `bytes32` id (`keccak256`
of the `PoolKey`), not a deployed contract. The `Initialize` record it emits *is* the pool's
metadata.

Every decoded record carries `protocol` — the ABI's name — so a consumer can group rows
without knowing any address.

Two entries claiming one address, a dangling ABI name, a malformed address, and a rule for
an event its factory's ABI does not declare are each a startup error rather than a silent
no-op: a registry that decodes less than it was told to looks like a quiet chain.

ABI paths resolve relative to the registry file, not the working directory, so the file
and its `abis/` directory move together.

One ABI per address applies at every height. A proxy that upgrades changes its ABI at a
height, which this cannot express — the `AbiRegistry` seam is what a table-backed
registry keyed by `(chain, address, block_range)` replaces.

### What decode does not do

**It does not project a record into a dataset.** Decoding produces *facts* — an
argument's name and its typed value, straight off the ABI:

```json
{"name": "amount0", "value": {"type": "int", "value": "-3180585820646654", "bits": 256}}
```

Turning that into a `dex.trades` row needs three things the decoder does not have:

| Needed | Source | Have it? |
| --- | --- | --- |
| What the arguments mean | the ABI | yes |
| Which contract this is | the registry | yes |
| Which tokens, and their decimals and symbols | **reference data** | not yet |

Token metadata is the gap. A Uniswap `Swap` event names neither token — only the pool
address — so `token0`/`token1` come from calling the pool, and decimals and symbols from
calling the tokens. None of that is derivable from a log, which is why the projection
cannot live here.

So `protocol` is a fact and travels on the record; a *dataset* is a modeling choice and
belongs where the joins are. Two payoffs from keeping it that way:

- **No per-protocol code in the decoder.** A mapper that knew what `amount0` means would
  be a mapper that has to know every protocol, and a new DEX would be a new Rust file.
- **A new DEX is rows in reference tables**, not code.

When stream processing lands, the decoded records and the reference tables are what it
joins. `protocol` and the argument names are the join keys, which is why they are on the
wire rather than re-derived downstream.

### Other client settings

The store takes settings this file does not restate, passed straight through and
validated by the engine:

```toml
[storage.duckdb.settings]
threads = "4"
max_memory = "1GB"
```

Anything unrecognized is an error from DuckDB naming the setting, so a typo is caught at
startup rather than silently ignored. Every envelope lands in an append-only `events` table
with the envelope as JSON beside the columns a query filters on.

Configuration is a typed struct in `src/config.rs`, not a string lookup scattered through
each layer, so a layer can be constructed in a test with no environment at all.
`src/runtime.rs` turns those settings into the pipeline, so the wiring is testable without
a network.

## Event shape

```json
{"chain":"base","sequence":18174,"v":1,
 "type":"log","log_index":0,"transaction_hash":"0x…","transaction_index":3,
 "address":"0x…","topic0":"0x…","topic1":null,"topic2":null,"topic3":null,
 "data":"0x…","removed":false,"block_number":51883702,"block_hash":"0x…"}
```

`v` is the wire shape's version. It is a field on the envelope rather than a transport
header because one of the sinks is a local database: a header survives no hop into
`DuckDB`, a file, or a pipe, so a consumer reading those could not tell two shapes
apart. It is bumped only for a breaking change — a field's type or meaning changing,
or a field or variant being removed. Adding an optional field or a new event variant
does not bump it, which is why consumers must skip an unknown `type` and ignore
unknown fields.

Events fall into three kinds. **Datasets** — `block`, `transaction`, `receipt`, `log`
— are durable on-chain records, defined per chain in `src/wire/datasets/evm.rs`. Each
is a normalized table with a natural key and fully deconstructed fields, the same
decomposition of the chain's datasets, so a row maps straight to a
persistence row; children are referenced by scalar key, never embedded. **Derived**
records — `decoded` — are what the decode stage produces from a dataset, carrying
typed ABI arguments; the type of every argument travels with it, so a consumer can
rebuild a typed column without reading the ABI. **Control
signals** — `reorg`, `finalized` — drive a consumer's state machine and carry no
payload; `Event::is_dataset` tells them apart. The line is one flat object: `chain`,
`sequence`, and the event's fields under its `type` tag. Consumers deduplicate on
each event's `dedupe_key`, not `sequence`: a sequence can be reused after a reorg or
a restart, and the key is scoped to the stream. Each dataset's key comes from its
natural key, so a transaction and its receipt (both keyed by the transaction hash)
stay distinct.

Identity uses `alloy_primitives::{B256, BlockHash, TxHash}`, encoded as lowercase
`0x` hex, and quantity fields encode as `0x` hex via `alloy-serde`. The wire shape
is pinned by tests in `src/wire/envelope.rs`: `every_variant_round_trips_through_json`
covers every event kind, `the_wire_object_carries_only_the_envelope_and_event_fields`
pins the exact key set, and
`the_schema_version_is_stamped_and_absent_v_parses` pins both the `v` stamp and
backward compatibility with a line written without the field. Every sink
renders through the same encoding, so stdout and the local
store cannot drift apart. A chain whose
identity does not fit that shape — Solana's base58 blockhash and 64-byte signature
are the expected case — gets its own module beside `src/datasets/evm.rs` plus a
variant in `Event`, rather than a shared type widened to fit both.

## Examples

```bash
cargo run --example decode_logs
```

Decodes two real Uniswap V3 swap logs captured from a Base pool, using the same
`Transform` the decode layer runs. No arguments and no network: the capture and the ABI
are embedded, and each input is printed as its decoded record — which is what the
pipeline stores beside the raw log.

Worth reading the output for two things: `amount0` is negative, because the amount is an
`int256` and the sign says which way the pool sent that token; and `sqrtPriceX96` carries
`bits: 160` while `tick` carries `bits: 24`, because a store needs the declared width to
pick a column and a width is not recoverable from a number.

To point it at other contracts, capture real input by setting `stdout = true` in the
`[ingest]` table, which prints the stream instead of storing it, and change the address, chain, ABI, and fixture the example names.

## Benchmarks

```bash
cargo bench
```

`benches/hot_path.rs` measures the CPU work between socket and sink — decoding
the node's batch response and serialising envelopes — since end-to-end latency is dominated by the
network and is not reproducible off-line. It is hand-rolled and dependency-free so
it can report percentiles rather than means, and it calls `std::hint::black_box`
explicitly because that is the only way to stop `lto` and `codegen-units = 1` from
deleting the work being measured.

Measured on an Apple Silicon laptop, release profile, ~1,000 samples each:

| Workload | response | events | decode p50 | decode p99 | serialise p50 |
| --- | --- | --- | --- | --- | --- |
| 100 txs × 2 logs | 281 KiB | 401 | 725µs | 921µs | 572µs |
| 500 txs × 4 logs | 1.9 MiB | 3,001 | 4.76ms | 5.48ms | 4.71ms |
| 2,000 txs × 5 logs | 8.6 MiB | 14,001 | 21.6ms | 25.3ms | 23.0ms |

Decode is linear in response size at roughly 2.5µs per KiB. Payloads are still
never *decoded* into ABI values — the node's JSON is deserialized into alloy's RPC
types and then projected field by field — so the cost remains mostly memory traffic
and per-event allocation.

Against the budget: an average Base block in a live run had about 260
transactions and 880 logs, which falls between the first two rows, so decode
plus serialise is a few milliseconds of CPU per block. The larger cost is the
network: the indexer publishes several MB per Base block, because every dataset is
published with its fields spelled out rather than as one opaque `raw` string.

## Checks

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps --all-features
cargo nextest run --all-targets --all-features
```

## Compatibility policy

The published shape is versioned by `wire::envelope::SCHEMA_VERSION`, stamped on
every line as `v`. It is bumped only for a **breaking** change — a field's type or
meaning changing, or a field or variant being removed. Adding an optional field or a
new event variant does not bump it, so a consumer must skip an unknown `type` and
ignore unknown fields. A breaking change needs a coexistence window in the store,
because a reader reading across the change sees both shapes interleaved.
