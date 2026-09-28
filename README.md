# indexer

A low-latency, chain-agnostic blockchain indexer.

It reads a chain's live tip, parses each block into ordered events, and publishes
them as newline-delimited JSON. Chain-specific knowledge sits behind one trait and
egress behind another, so new chains and new brokers are additions rather than
rewrites.

## Layout

A Cargo workspace. The split exists so that the process that produces events and
any process that consumes them share one definition of the stream without sharing a
One crate, one binary, four layers as modules. Started as a process, every stage built
explicitly in `main`, so what runs is visible in one place.

```
src/
├── main.rs         builds every stage from the environment and runs them together
├── config.rs       typed builders — stage configuration, not string lookups
├── wire/           the wire contract: envelope, events, dataset records. Pure data.
├── connectors/     the traits, the concrete connectors, and the storage runtime.
├── ingest/         block sources and the reorg-aware pipeline.
└── decode/         the stateless ABI decode transform and its registry.
```

`wire` depends on `alloy` and `serde` and on nothing else in the tree. `decode`
knows nothing about `ingest`, so it cannot reach into the pipeline's ordering and reorg
state machine.

`connectors` holds no domain logic: it is the traits, the transports, and the stages
whose whole job is wiring — the storage drain lives there rather than in a module of its
own, because it is the sink and the process that drives it, and it would be the only
stage module with nothing of its own in it.

The layers were separate crates, which enforced that direction with the compiler. They
are modules now, so it is a convention a reviewer checks. The trade: breaking the
`decode` → `ingest` cycle was what forced the shared types into their own crate and the
bus into another — four manifests, feature forwarding between them, and a compiler
guarantee worth about one edge. At this size, one owner reads all of it.

The pipeline is `ingest` → `raw.chain` → `decode` → `decoded.chain` → storage.
Aggregation and windowing are not built.

## What works today

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
  It consumes `raw.chain`, decodes each log against the ABI registered for its
  `(chain, address)`, and produces to `decoded.chain`. It emits the decoded record
  *alongside* the raw log, so the decoded stream is a lossless superset, and forwards
  everything else — including reorgs and finality watermarks, which must survive for a
  store to retract and compact. It reimplements no ordering or reorg logic, so
  replaying a record is safe. Offsets are committed only after the producer's flush,
  so a crash replays a batch rather than losing one.
- **A storage stage.** It drains `raw.chain` and `decoded.chain` into a local `DuckDB`
  database, one consumer group per topic and one append-only `events` table.
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
- **Kafka-protocol connectors** both ways, behind the `kafka` feature: a sink keyed by
  chain so a chain's stream keeps its `sequence` order on one partition, and a source
  that yields the same envelopes back.
- **A local `DuckDB` store**, behind the `duckdb` feature. Embedded and
  single-writer, so it is an archive/analytics endpoint, not the fan-out.

Both optional connectors are off by default so a plain `cargo build` compiles neither C
`librdkafka` nor the `DuckDB` C++ engine; enable them with `--features kafka,duckdb`.

## Not built yet

Backfill-to-live handoff, checkpoint resume, mempool, filtered
subscriptions, derived state (balances/nonces), the aggregation layer, and the
Parquet/GCS archiver. The
crate is a walking skeleton: it indexes forward from
whatever the chain does next and does not fill gaps that predate startup.

## Run it

One binary runs every stage: ingest follows the chain, decode reads `raw.chain` and
writes `decoded.chain`, and storage drains both into `DuckDB`. They run concurrently
and stop together — a stage that ends for good stops the process, because continuing
without it would leave a stream that looks alive but is not.

Settings come from a TOML file, named by the first argument or defaulting to
`indexer.toml`. `indexer.toml` in the repository is a working example.

```bash
RUST_LOG=info cargo run --release --features kafka,duckdb -- indexer.toml
```

```toml
[ingest]
chain = "base"
http_url = "https://base-rpc.publicnode.com"
ws_url = "wss://base-rpc.publicnode.com"

[kafka]
brokers = "localhost:9092"

[storage]
database = "indexer.duckdb"
```

**Required** — no value could be right by accident, so each errors at startup naming
the field:

| Key | Meaning |
| --- | --- |
| `kafka.brokers` | Bootstrap servers |
| `ingest.chain` | Chain id stamped on every event |
| `ingest.http_url` | JSON-RPC endpoint for blocks and receipts |
| `ingest.ws_url` | WebSocket endpoint for `newHeads` |

**Defaulted** — correct for a standard deployment; override for a non-standard one:

| Key | Default | Meaning |
| --- | --- | --- |
| `ingest.raw_topic` | `raw.chain` | Ingest's output and decode's input |
| `ingest.stdout` | `false` | Print instead of publishing |
| `kafka.decoded_topic` | `decoded.chain` | Decode's output |
| `kafka.group_prefix` | `indexer` | Consumer group prefix, per stage |
| `kafka.batch_records` | `500` | Records per flush |
| `kafka.batch_ms` | `1000` | Time bound on a batch |
| `storage.database` | `indexer.duckdb` | Path to the store |
| `storage.drain_secs` | unset | Stop a topic after this idle. **Omit for a live indexer** |
| `storage.abis` | `[]` | `chain:address:path`, one ABI each |

Omit `[ingest]` entirely to run decode and storage against a topic filled elsewhere.
`RUST_LOG` is still read from the environment, because a log filter is not deployment
configuration.

An unknown key is a startup error naming the line and the key, so a misspelling is
caught rather than silently leaving a setting at its default.

`storage.abis` applies one ABI per address at every height, which is the honest
limitation of a file-backed registry: a proxy that upgrades changes its ABI at a
height, and that needs a table-backed registry behind the same seam.

Storage uses one consumer group per topic, named `<group>-<topic>`, because an offset
is per group: one group spanning two topics would commit a single position across both.
Every envelope lands in an append-only `events` table with the envelope as JSON beside
the columns a query filters on. A topic with nothing more to read is drained and the
run stops.

Configuration is a typed builder in `src/config.rs`, not a string lookup scattered
through each stage, so a stage can be constructed in a test with no environment at all.

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
`the_schema_version_is_stamped_and_old_lines_still_parse` pins both the `v` stamp and
backward compatibility with a line written before the field existed. Every connector
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
are embedded, and each input is printed followed by its decoded record — which is what
the stage publishes, since the decoded stream keeps the raw log rather than replacing it.

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
network: the indexer publishes several MB per Base block, since every dataset is
now published with its fields spelled out rather than as one opaque `raw` string.

## Checks

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps --all-features
cargo test
```

## Compatibility policy

The published shape is versioned by `wire::envelope::SCHEMA_VERSION`, stamped on
every line as `v`. It is bumped only for a **breaking** change — a field's type or
meaning changing, or a field or variant being removed. Adding an optional field or a
new event variant does not bump it, so a consumer must skip an unknown `type` and
ignore unknown fields. A breaking change needs a coexistence window on the topic,
because one consumer group reading across the change sees both shapes interleaved.
