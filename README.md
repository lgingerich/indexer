# indexer

A low-latency, chain-agnostic blockchain indexer.

It reads a chain's live tip, parses each block into ordered events, and publishes
them as newline-delimited JSON. Chain-specific knowledge sits behind one trait and
egress behind another, so new chains and new brokers are additions rather than
rewrites.

## Layout

One crate, one binary, four layers as modules. What runs is not hardcoded: the settings
file states it, and `runtime` assembles the pipeline — ingest iff a chain is configured,
decode iff a registry has entries, storage always — on whichever bus and store the
settings chose. `main` is only the process boundary. The layers share one definition of
the stream through `wire`, without sharing a process.

```
src/
├── main.rs         the process boundary: logging, the settings path, the exit code
├── runtime.rs      assembles the pipeline the settings describe, and runs it
├── config.rs       typed settings — stage configuration, not string lookups
├── wire/           the wire contract: envelope, events, dataset records. Pure data.
├── connectors/     the traits, the transports (kafka, memory, stdout, duckdb), the drain loop.
├── ingest/         block sources and the reorg-aware pipeline.
└── decode/         the stateless ABI decode transform and its registry.
```

`wire` depends on `alloy` and `serde` and on nothing else in the tree. `decode`
knows nothing about `ingest`, so it cannot reach into the pipeline's ordering and reorg
state machine.

`connectors` holds no domain logic: it is the traits, the transports, and the shared
drain loop the stages drive (`connectors/mod.rs`). The loop lives there rather than in a
module of its own because it is the sink's contract — the publish → flush → commit order
exists once, so storage and decode cannot drift on it.

The layers are modules, so their one-way dependency direction is a convention a
reviewer checks rather than one the compiler enforces. At this size, one owner reads
all of it, and each module documents the direction it may depend in.

The pipeline is `ingest` → `raw.chain` → `decode` → `decoded.chain` → storage.
Aggregation and windowing are not built.

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
- **A decode stage.** `src/decode` is a stateless transform over an ABI registry.
  It consumes `raw.chain`, decodes each registered log against the ABI for its
  `(chain, address)`, and produces the decoded record to `decoded.chain`. Raw datasets
  have no decoded form, so they are dropped there — the raw topic is their home — and
  only decoded records and control signals land on the decoded topic. Reorgs and
  finality watermarks are forwarded because a store retracts and compacts on them. It
  reimplements no ordering or reorg logic, so replaying a record is safe. Offsets are
  committed only after the producer's flush, so a crash replays a batch rather than
  losing one.
- **A local store.** The process drains `raw.chain` and `decoded.chain` into a local
  `DuckDB` database, one consumer group per topic and one append-only `events` table.
- **Finality watermark.** A `finalized` event says a block and everything below
  it are permanent. Its height comes from the node's own `finalized` tag, so each
  chain's rules apply with no confirmation count to tune; on Base it trails the
  tip by about 600 blocks. It is published only when it advances.
- **Reorg retraction.** Parent-hash linkage is checked on every block. A mismatch
  publishes a `reorg` event whose `orphaned_hashes` say what was retracted, and
  reclaims the sequence numbers those blocks had used. The undo window is bounded
  (128 blocks by default, and finalized blocks are dropped first). See
  `src/ingest/pipeline.rs`.
- **NDJSON to stdout.** See `src/connectors/stdout.rs`.
- **Two buses, one pipeline.** The stages share a topic bus: a Kafka-protocol transport
  (behind the `kafka` feature) whose sink is keyed by chain so a chain's stream keeps its
  `sequence` order on one partition, or an in-process transport (`kind = "memory"`) that
  runs the whole pipeline with no broker. The stages and the publish → flush → commit
  order are identical either way; only durability differs.
- **A local `DuckDB` store**, behind the `duckdb` feature (on by default). Embedded and
  single-writer, so it is an archive/analytics endpoint, not the fan-out.

`duckdb` is on by default because the pipeline needs a store to run; `kafka` is off by
default so a plain `cargo build` does not compile the C `librdkafka`. A memory-bus run
needs no broker, so `cargo run` alone runs the pipeline.

## Not built yet

Backfill-to-live handoff, checkpoint resume, mempool, filtered subscriptions, derived
state (balances/nonces), the aggregation layer, and the Parquet/GCS archiver. The
crate is a walking skeleton: it indexes forward from whatever the chain does next and
does not fill gaps that predate startup.

## Run it

One binary runs the pipeline the settings describe: ingest follows the chain, decode reads
`raw.chain` and writes `decoded.chain`, and storage drains both into the store. What runs
is not hardcoded — the settings decide it. Stages run concurrently and stop together: a
stage that ends for good stops the process, because continuing without it would leave a
stream that looks alive but is not.

- **Ingest runs iff `[ingest]` is present** — configuring a chain is the statement that it
  should be followed.
- **Decode runs iff `[decode] registry` names a registry with entries.** Decode is purely
  additive: with none it would drop every raw dataset and forward only the control
  signals, which ingest already publishes to the topic storage reads, so skipping it
  loses nothing.
- **Storage always runs**, reading the raw topic and (when decode ran) the decoded topic.

Settings come from a TOML file, named by the first argument or defaulting to
`indexer.toml`. `indexer.toml` in the repository is a working example. What decode decodes
lives in a separate registry file, named by `[decode] registry` and defaulting to nothing —
`registry.toml` is a working example; see [The contract registry](#the-contract-registry).

```bash
# With a broker (the kafka feature compiles librdkafka):
RUST_LOG=info cargo run --release --features kafka -- indexer.toml

# Or no broker at all: an in-process bus, one process, no message queue.
RUST_LOG=info cargo run --release -- indexer.toml
```

```toml
[ingest]
chain = "base"
http_url = "https://base-rpc.publicnode.com"
ws_url = "wss://base-rpc.publicnode.com"

[bus]
kind = "kafka"          # or "memory" for a single, broker-less process

[bus.kafka]
brokers = "localhost:9092"

[runtime]
drain_secs = 5   # omit for a live indexer

[storage]
kind = "duckdb"

[storage.duckdb]
path = "indexer.duckdb"

[decode]
registry = "registry.toml"
```

Each table names what owns its fields, not where a field was first needed:

- `[bus]` — the transport (`kind`), the topics, and the group prefix. A topic is a bus
  concept, not a Kafka one, so the same names apply under either transport. The broker's
  own settings live under `[bus.kafka]`, needed only when `kind = "kafka"`.
- `[runtime]` — how every stage drains: the batch size and time bound, and the drain
  bound. Transport-independent, so it is not under `[bus]`.
- `[storage.<kind>]` — one backend's own settings. `[storage] kind` selects it, so a
  second backend (ClickHouse, Postgres) is a variant there plus its own table, not
  another top-level `database` whose owner a reader has to guess.

**Required** — no value could be right by accident, so each errors at startup naming
the field:

| Key | Meaning |
| --- | --- |
| `bus.kafka.brokers` | Bootstrap servers, when `bus.kind = "kafka"` |
| `ingest.chain` | Chain id stamped on every event |
| `ingest.http_url` | JSON-RPC endpoint for blocks and receipts |
| `ingest.ws_url` | WebSocket endpoint for `newHeads` |

**Defaulted** — correct for a standard deployment; override for a non-standard one:

| Key | Default | Meaning |
| --- | --- | --- |
| `bus.kind` | `kafka` | The transport: `kafka` or `memory` |
| `bus.raw_topic` | `raw.chain` | Ingest's output and decode's input |
| `bus.decoded_topic` | `decoded.chain` | Decode's output |
| `bus.group_prefix` | `indexer` | Consumer group prefix, per stage (Kafka only) |
| `ingest.stdout` | `false` | Print instead of publishing |
| `runtime.batch_records` | `500` | Records per flush |
| `runtime.batch_ms` | `1000` | Time bound on a batch |
| `runtime.drain_secs` | unset | Stop a topic after this idle; bounds decode's input too. **Omit for a live indexer** |
| `storage.kind` | `duckdb` | The store backend |
| `storage.duckdb.path` | `indexer.duckdb` | Path to the store |

**Optional tables:**

| Key | Meaning |
| --- | --- |
| `decode.registry` | Path to the contract registry file, relative to the settings file. Absent means nothing is decoded |
| `bus.kafka.properties` | Any other librdkafka property, passed straight through |
| `storage.duckdb.settings` | Any other DuckDB setting, passed straight through |

Omit `[ingest]` entirely to run decode and storage against a topic filled elsewhere.
`RUST_LOG` is still read from the environment, because a log filter is not deployment
configuration.

An unknown key is a startup error naming the line and the key, so a misspelling is
caught rather than silently leaving a setting at its default. An unknown `storage.kind`
is likewise an error rather than a silent fallback to the default backend.

### The contract registry

What decode decodes lives in its own file, named by `[decode] registry` — `registry.toml`
in the repository is a working example. It is deliberately not part of `indexer.toml`: the
settings file is deployment topology (brokers, endpoints, paths), while the registry is a
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

When stream processing lands, the decoded stream and the reference tables are what it
joins. `protocol` and the argument names are the join keys, which is why they are on the
wire rather than re-derived downstream.

### Other client settings

Both clients take settings this file does not restate, passed straight through and
validated by the engine:

```toml
[bus.kafka.properties]
"compression.codec" = "gzip"   # quote keys: most contain dots
"fetch.max.bytes" = "1048576"

[storage.duckdb.settings]
threads = "4"
max_memory = "1GB"
```

Anything unrecognized is an error from librdkafka or DuckDB naming the property, so a
typo is caught at startup rather than silently ignored. `bus.kafka.properties` applies to
every client the process builds, so a property that means different things to a producer
and a consumer — `auto.offset.reset` is the usual one — is better left out.

On Kafka, each topic-consumer pair uses its own consumer group, named `<prefix>-<stage>`,
because an offset is per group: one group spanning two topics would commit a single
position across both. Every envelope lands in an append-only `events` table with the
envelope as JSON beside the columns a query filters on. A topic with nothing more to read
is drained and the run stops.

Configuration is a typed builder in `src/config.rs`, not a string lookup scattered
through each stage, so a stage can be constructed in a test with no environment at all.
`src/runtime.rs` turns those settings into the pipeline, so the wiring is testable without
a broker.

## Event shape

```json
{"chain":"base","sequence":18174,"v":1,
 "type":"log","log_index":0,"transaction_hash":"0x…","transaction_index":3,
 "address":"0x…","topic0":"0x…","topic1":null,"topic2":null,"topic3":null,
 "data":"0x…","removed":false,"block_number":51883702,"block_hash":"0x…"}
```

`v` is the wire shape's version. It is a field on the envelope rather than a broker
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
backward compatibility with a line written without the field. Every connector
renders through the same encoding, so stdout, the broker, and the local
store cannot drift apart. A chain whose
identity does not fit that shape — Solana's base58 blockhash and 64-byte signature
are the expected case — gets its own module beside `src/datasets/evm.rs` plus a
variant in `Event`, rather than a shared type widened to fit both.

## Examples

```bash
cargo run --example decode_logs
```

Decodes two real Uniswap V3 swap logs captured from a Base pool, using the same
`Transform` the decode stage runs. No arguments and no broker: the capture and the ABI
are embedded, and each input is printed as its decoded record — which is what the stage
publishes, since a raw dataset has no decoded form and only the record reaches the
decoded topic.

Worth reading the output for two things: `amount0` is negative, because the amount is an
`int256` and the sign says which way the pool sent that token; and `sqrtPriceX96` carries
`bits: 160` while `tick` carries `bits: 24`, because a store needs the declared width to
pick a column and a width is not recoverable from a number.

To point it at other contracts, capture real input by setting `stdout = true` in the
`[ingest]` table, which prints what the indexer would publish instead of sending it to a
broker, and change the address, chain, ABI, and fixture the example names.

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
ignore unknown fields. A breaking change needs a coexistence window on the topic,
because one consumer group reading across the change sees both shapes interleaved.
