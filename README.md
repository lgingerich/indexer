# indexer

A low-latency, chain-agnostic blockchain indexer.

It reads a chain's live tip, parses each block into ordered events, and publishes
them as newline-delimited JSON. Chain-specific knowledge sits behind one trait and
egress behind another, so new chains and new brokers are additions rather than
rewrites.

## Layout

A Cargo workspace. The split exists so that the process that produces events and
any process that consumes them share one definition of the stream without sharing a
runtime — the dependency graph is the boundary, and it is enforced by the compiler
rather than by convention.

```
crates/
├── wire/         the wire contract: envelope, events, dataset records. Pure data.
├── connectors/   the bus boundary: EventSink, EnvelopeSource, and the connectors.
├── indexer/      ingestion: block sources, the reorg-aware pipeline, bin `ingest`.
├── decode/       the stateless ABI decode transform, bin `decode`.
└── materialize/  decoded events into typed tables, bin `materialize`.
```

`wire` depends on `alloy` and `serde` and on nothing else in the tree. `decode`
depends on `wire` and **not** on `indexer`, so the decode stage structurally cannot
reach into the pipeline's ordering and reorg state machine. That missing edge is the
whole point of the split.

`materialize` is separate from `decode` for the same reason one level down: a new ABI
is a decode concern, and a new protocol's meaning is a modeling concern. Keeping them
apart is what lets decode stay stateless and replayable.

## What works today

- **EVM ingestion.** Live heads over WebSocket (`eth_subscribe`/`newHeads`), and
  each block fetched over JSON-RPC with full transactions, receipts, and logs in
  one batched request. Nothing the node returns is dropped. See
  `crates/indexer/src/source/evm.rs`.
- **One event per dataset.** A `block` event, then for each transaction a
  `transaction` event, its `receipt` event, and its `log` events. Each dataset is a
  normalized table — a block references its transactions by hash, a receipt carries
  only the count of its logs — so no field is published twice and a row maps to a
  persistence row. See `crates/wire/src/datasets/evm.rs`.
- **Ordered events.** Every event gets a per-chain monotonic `sequence`; every
  dataset exposes a `dedupe_key` derived from its natural key. See
  `crates/wire/src/envelope.rs`.
- **A decode stage.** `crates/decode` is a stateless transform over an ABI registry,
  and it runs as a process: `cargo run -p decode --bin decode --features kafka`
  consumes `raw.chain`, decodes each log against the ABI registered for its
  `(chain, address)`, and produces to `decoded.chain`. It emits the decoded record
  *alongside* the raw log, so the decoded stream is a lossless superset, and forwards
  everything else — including reorgs and finality watermarks, which must survive for a
  store to retract and compact. It reimplements no ordering or reorg logic, so
  replaying a record is safe. Offsets are committed only after the producer's flush,
  so a crash replays a batch rather than losing one.
- **Typed tables from decoded events.** `crates/materialize` turns a decoded record
  into table rows in two layers: one faithful row per event with every argument under
  its ABI name, and one semantic row in the shape Allium calls `dex.trades`. An ABI
  integer that fits an `i64` becomes one; anything wider — a `uint160` price, a
  `uint256` wei amount — becomes exact decimal text rather than a rounded float.
- **Finality watermark.** A `finalized` event says a block and everything below
  it are permanent. Its height comes from the node's own `finalized` tag, so each
  chain's rules apply with no confirmation count to tune; on Base it trails the
  tip by about 600 blocks. It is published only when it advances.
- **Reorg retraction.** Parent-hash linkage is checked on every block. A mismatch
  publishes a `reorg` event whose `orphaned_hashes` say what was retracted, and
  reclaims the sequence numbers those blocks had used. The undo window is bounded
  (128 blocks by default, and finalized blocks are dropped first). See
  `crates/indexer/src/pipeline.rs`.
- **NDJSON to stdout.** See `crates/connectors/src/stdout.rs`.
- **Kafka-protocol connectors** both ways, behind the `kafka` feature: a sink keyed by
  chain so a chain's stream keeps its `sequence` order on one partition, and a source
  that yields the same envelopes back.
- **A local `DuckDB` store**, behind the `duckdb` feature. Embedded and
  single-writer, so it is an archive/analytics endpoint, not the fan-out.

Both optional connectors are off by default so a plain `cargo build` compiles neither C
`librdkafka` nor the `DuckDB` C++ engine; enable them with `--features kafka,duckdb`.

## Not built yet

Backfill-to-live handoff, checkpoint resume, mempool, filtered
subscriptions, derived state (balances/nonces), the stream-processing layer, and the
Parquet/GCS archiver. The
crate is a walking skeleton: it indexes forward from
whatever the chain does next and does not fill gaps that predate startup.

## Run it

```bash
EVM_CHAIN=base \
EVM_HTTP_URL=https://base-rpc.publicnode.com \
EVM_WS_URL=wss://base-rpc.publicnode.com \
RUST_LOG=info \
cargo run -p indexer --bin ingest --release > events.ndjson
```

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `EVM_CHAIN` | yes | — | Chain id stamped on every event |
| `EVM_HTTP_URL` | yes | — | JSON-RPC endpoint for blocks, receipts, and the finalized block |
| `EVM_WS_URL` | yes | — | WebSocket endpoint for `newHeads` |
| `RUST_LOG` | no | `info` | Log filter |

Logs go to stderr; events go to stdout, so the two streams never interleave. The
features above are on by default for the binary.

### Run the decode stage

```bash
KAFKA_BROKERS=127.0.0.1:9092 \
DECODE_ABIS="base:0xd0b53D9277642d899DF5C87A3966A349A798F224:crates/decode/abi/uniswap_v3_pool.json" \
cargo run -p decode --bin decode --features kafka
```

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `KAFKA_BROKERS` | yes | — | Bootstrap servers |
| `DECODE_ABIS` | no | — | `chain:address:abi.json`, comma-separated, repeatable |
| `DECODE_GROUP` | no | `indexer-decode` | Consumer group |
| `DECODE_INPUT_TOPIC` | no | `raw.chain` | Topic to consume |
| `DECODE_OUTPUT_TOPIC` | no | `decoded.chain` | Topic to produce |
| `DECODE_BATCH` | no | `500` | Records per flush |
| `DECODE_BATCH_MS` | no | `1000` | Time bound on a batch |

A batch is `DECODE_BATCH` records or `DECODE_BATCH_MS` milliseconds, whichever comes
first. `DECODE_ABIS` applies one ABI per address at every height, which is the honest
limitation of a file-backed registry: a proxy that upgrades changes its ABI at a
height, and that needs a table-backed registry behind the same seam.

### Run the materialize stage

```bash
KAFKA_BROKERS=127.0.0.1:9092 \
MATERIALIZE_OUTPUT_DIR=tables \
cargo run -p materialize --bin materialize --features kafka
```

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `KAFKA_BROKERS` | yes | — | Bootstrap servers |
| `MATERIALIZE_INPUT_TOPIC` | no | `decoded.chain` | Topic to consume |
| `MATERIALIZE_OUTPUT_DIR` | no | `tables` | Directory for one NDJSON file per table |
| `MATERIALIZE_GROUP` | no | `indexer-materialize` | Consumer group |
| `MATERIALIZE_BATCH` | no | `500` | Rows per flush |
| `MATERIALIZE_BATCH_MS` | no | `1000` | Time bound on a batch |

Output is one file per table — `Swap.ndjson`, `dex.trades.ndjson` — each line a row
with its identity columns first, so the two layers are joinable. The JSON-lines sink
appends and does not deduplicate, so it is for inspecting output; a real store upserts
on the identity columns instead.

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
— are durable on-chain records, defined per chain in `crates/wire/src/datasets/evm.rs`. Each
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
is pinned by tests in `crates/wire/src/envelope.rs`: `every_variant_round_trips_through_json`
covers every event kind, `the_wire_object_carries_only_the_envelope_and_event_fields`
pins the exact key set, and
`the_schema_version_is_stamped_and_old_lines_still_parse` pins both the `v` stamp and
backward compatibility with a line written before the field existed. Every connector
renders through the same encoding, so stdout, the broker, and the local
store cannot drift apart. A chain whose
identity does not fit that shape — Solana's base58 blockhash and 64-byte signature
are the expected case — gets its own module beside `src/datasets/evm.rs` plus a
variant in `Event`, rather than a shared type widened to fit both.

## Benchmarks

```bash
cargo bench
```

`crates/indexer/benches/hot_path.rs` measures the CPU work between socket and sink — decoding
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
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --no-deps --workspace --all-features
cargo test --workspace
```

## Compatibility policy

The published shape is versioned by `wire::envelope::SCHEMA_VERSION`, stamped on
every line as `v`. It is bumped only for a **breaking** change — a field's type or
meaning changing, or a field or variant being removed. Adding an optional field or a
new event variant does not bump it, so a consumer must skip an unknown `type` and
ignore unknown fields. A breaking change needs a coexistence window on the topic,
because one consumer group reading across the change sees both shapes interleaved.
