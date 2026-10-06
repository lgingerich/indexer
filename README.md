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

stdout backend:  ingest ─▶ decode ─▶ stdout        no channel, no store
```

Ingest and decode are direct calls in one task. The channel is the only queue, and it
exists so a stalled store does not stall the head subscription.

### One block

```
WebSocket head
      │
      ▼
JSON-RPC batch: block + receipts          one request, nothing the node returns is dropped
      │
      ▼
ordered events, each with its dataset's natural key
      │
      │   block
      │   transaction, receipt, log, decoded, log, …
      │   transaction, receipt, …
      │
      ▼  flush, once
one channel message
      │
      ▼
one DuckDB commit                         raw rows and decoded rows together
```

A `reorg` marker, when there is one, is the first envelope of that message: the fork is
recorded before the replacement block.

### What decode adds

Decode never removes or rewrites an envelope. It only inserts a record after a log it
can decode.

```
envelope
   │
   ├─ block, transaction, receipt, reorg ──▶ forwarded as it arrived
   │
   └─ log
        ├─ no ABI for (chain, address), or no such event ──▶ the log, nothing added
        ├─ ABI matches, data does not decode ──────────────▶ the log, error logged
        └─ decodes ─────────────────────────────────────────▶ the log, then its decoded record
```

An immutable `Decoder` decodes logs during ingestion. The registry selects
an ABI and protocol by `(chain, address, block)` using predefined half-open block ranges.
There is no factory discovery, network lookup, or runtime registry mutation.

### A reorg

Height and parent-hash linkage are checked before publishing each block. Ingestion
retains the unfinalized tail plus a finalized linkage anchor, with a fixed 4,096-block
memory budget; insufficient capacity is an error rather than silent eviction.

```
published     1 ─── 2 ─── 3
new head           └─── 2'          2' builds on 1, not on 2

appended, in order
  block 1, block 2, block 3
  reorg { height: 2, orphaned_hashes: [3, 2] }
  block 2', and its transactions, receipts, logs, and decoded rows
```

The store appends every one of those rows. It does not delete the orphaned branch, mark
it noncanonical, or hide it from a dataset query. `block`, `transaction`, `receipt`,
`log`, and `decoded` contain both branches. The `reorg` table records which block hashes
stopped being canonical, and that is the only place that fact is stored.

A downstream read that wants the current chain has to exclude every block hash named by
`reorg.orphaned_hashes`. A read that does not apply those markers returns orphaned rows
alongside their replacements. The same rule applies to every dataset row that carries one
of those block hashes, not only to the `block` table.

A row's key carries the block's hash, so orphaned block 2 and replacement block 2' are
different rows rather than one row overwritten. A fork older than the ring is an error
rather than an empty retraction.

### When the store stalls

Caught up, one block arrives and leaves as its own commit:

```
ingest ──▶ [ block ] ──▶ one commit
```

Behind, the blocks already waiting share a commit, up to `sink.duckdb.batch_records`. A
block is never split across commits:

```
ingest ──▶ [ block | block | block ] ──▶ one commit
```

Full — 32 blocks waiting and the store still flushing — the next flush waits. Ingest
stops taking heads until there is room. That wait is the backpressure; nothing is
dropped:

```
ingest ── flush waits ──▶ [ ████ | ████ | ████ ] ◀── store still flushing
```

### A crash

```
chain (replayable) ──▶ in flight on the channel ──▶ committed in DuckDB
                              lost                        kept
```

The channel holds no durable log. The chain is the record upstream and the store is
the record downstream, so a restart should continue from the last committed height.
That resume is not built: a restart replays from the node's current finalized anchor. See
[Not built yet](#not-built-yet).

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
├── wire/           the wire contract: envelope, events, dataset records, and the rows a
│                   store persists. Pure data.
├── ingest/         block sources and the reorg-aware pipeline.
├── decode/         the immutable log decoder, historical registry, and live decoding sink.
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
  each block fetched over JSON-RPC in one batched request. `[ingest] datasets`
  chooses which of block, transaction, receipt, and log are fetched and stored;
  omitted, all four are. Logs without receipts use one `eth_getLogs` per height,
  and `[ingest] log_addresses` limits that call to those contracts. A header is
  still read for every height so linkage and finality keep working. See
  `src/ingest/source/evm.rs`.
- **One event per dataset.** A `block` event, then for each transaction a
  `transaction` event, its `receipt` event, and its `log` events. Each dataset is a
  normalized table — a block references its transactions by hash, a receipt carries
  only the count of its logs — so no field is published twice and a row maps to a
  persistence row. See `src/wire/datasets/evm.rs`.
- **A key per event.** Every event exposes a `dedupe_key` derived from its natural key:
  the hashes that identify the row plus a dataset tag. A key that descends from a block
  carries the block's *hash*, not its number — so a log in an orphaned block and the same
  transaction's log in the replacement are different rows, not one row written twice. The
  height is a column, not part of the key, because it is recoverable and a key that
  restates it is saying the same thing twice. See `src/wire/envelope.rs`.
- **A decode stage.** `src/decode::Decoder` prepares ABI layouts once and decodes each
  registered log against the ABI for its `(chain, address, block)`, inline in
  `DecodingSink`. Everything is forwarded as it arrived — decode adds records and removes
  none — so a reorg or finality marker reaches the store exactly once, from ingest.
  Decoded records carry the ABI content hash, protocol, and each argument's original
  position and complete ABI schema. Raw byte strings remain lossless, with readable
  text only when valid UTF-8.
- **A local store.** The process writes every envelope, raw and decoded, into a local
  `DuckDB` database: one typed table per dataset, with real columns rather than a JSON
  blob. What a dataset's columns *are* lives in `src/wire/row.rs`, not in the store, so a
  second store reuses the mapping instead of re-deriving it.
- **Finality watermark.** Ingestion reads the node's `finalized` tag and keeps that
  boundary in memory. On Base it trails the tip by about 600 blocks. Ingestion will not
  reorganize that height, and it drops finalized identities from the undo window except
  for one linkage anchor. The boundary is not written to the store.
- **Reorg retraction.** Parent-hash linkage is checked on every block. A mismatch
  appends a `reorg` event whose `orphaned_hashes` name the block hashes that stopped
  being canonical, then appends the replacement branch under its own keys. Dataset tables
  keep both branches; a reader applies the `reorg` rows to see only the current chain.
  The undo window retains the full unfinalized tail up to a fixed 4,096-block budget, and
  finalized entries are discarded except for one linkage anchor. See `src/ingest/pipeline.rs`.
- **Finalized backfill and catch-up.** The library can index earlier finalized history;
  production starts at the source-finalized anchor and catches up before following live
  heads. Decode runs on that path, so a historical log is stored raw and, when it matches,
  decoded. Alloy keeps the WebSocket subscription independent of fetches and sink
  delivery, and reconnects and resubscribes with bounded retries. Notifications are
  wake-up hints: HTTP reconciliation fills gaps and resolves reorgs, with a 30-second
  fallback when the subscription is silent. HTTP failures and exhausted subscription
  recovery are terminal. A process restart still starts over at the current finalized
  anchor.
- **NDJSON to stdout.** See `src/sink/stdout.rs`; the `[sink.stdout]` backend prints the
  stream instead of storing it, and opens no store.
- **A local `DuckDB` store**, behind the `duckdb` feature (on by default). Embedded and
  single-writer, so it is the archive; anything downstream reads from it rather than from
  the live stream.

`duckdb` is on by default because the pipeline needs a store to run. No broker is involved
anywhere, so `cargo run` alone runs the pipeline.

## Not built yet

The running process starts at the node's finalized anchor, catches up through decode,
and appends every row. These are the gaps that leaves.

Durability is one chain. The others stand alone.

```
idempotent writes ─┬─▶ resume from the store
                   └─▶ stored-log replay
streaming aggregation
Avro for a serialized envelope
ranged log backfill
```

### Resume from the store

The chain can replay any block. The store is what should say how far that replay has
already been committed. It does not. Startup always samples the node's current
finalized anchor and continues from there. The channel is not a checkpoint: accepting
a block there means the storage task has the batch, not that a commit landed.

```
startup
   │
   ├─ read the last committed finalized identity
   ├─ rebuild the undo ring from the stored rows at and above it
   └─ continue from the next height
```

Without the rebuilt ring, a reorg in blocks this process no longer remembers cannot
be retracted, and rows that run left behind stay. The durability TODO in
`src/ingest/pipeline.rs` is this item: restore the last committed finalized identity
and reconcile the stored unfinalized suffix before replay.

### Idempotent writes

Resume and backfill both replay a block that may already be stored. Every row carries
`chain` and `dedupe_key`, and both stores index that pair. The index is not unique,
and the write is an append (`DuckDB` appender, `PostgreSQL` `COPY`).

```
today     append the row                          a replay is a second row

planned   upsert on (chain, dedupe_key)           a replay is the same row
```

Upsert is a replay tool, not a reorg tool. A reorg's two branches have different keys
by design, so an upsert leaves both rows — which is what the marking rule above
needs.

### Stored-log replay

Re-decoding logs already in the store is not implemented. That path would read
retained raw logs, run them through the same decoder, and replace or deduplicate the
decoded rows. The idempotent write above is what makes the replacement safe.

### Streaming aggregation

Decode emits facts. A swap's tokens, decimals, and windows over those facts are
joins and derived calculations, and they belong in a streaming layer after the
store. See [What decode does not do](#what-decode-does-not-do).

```
raw logs + decoded records ──▶ aggregation ──▶ trades, balances, windows
                │                    ▲
                └──── reference data ┘     tokens, decimals, symbols
```

### Avro for a serialized envelope

Inside the process an envelope is a Rust value on the channel, and the stores write
one typed table per dataset. The only serialization is stdout, which writes
newline-delimited JSON. Avro is the planned encoding for that serialized envelope —
stdout and anything downstream of it — so those hops share one schema.

### Ranged log backfill

`[ingest] log_addresses` limits each `eth_getLogs` to those contracts. That call
runs when logs are selected and receipts are not; with receipts, logs come from
the receipts and an address list is refused at startup. Backfill still walks one
height at a time. A wider `eth_getLogs` range for historical blocks is not built.
A `logs` subscription is not the log source: it has no end-of-block marker.

Also not built, and not on the path above: mempool ingestion and a Parquet archive.

## Run it

One binary runs the pipeline the settings describe: ingest follows the chain, decode adds
a record for each log it has an ABI for, and storage writes both into the store. Ingest and
decode run together in one task and storage in another; a part that ends for good stops the
process, because continuing without it would leave a stream that looks alive but is not.

- **`[ingest]` is required** — it names the chain to follow.
- **Decode uses `[decode] registry`.** An absent or empty registry decodes nothing, which
  is a legitimate way to run and is said at startup.
- **Storage is the `[sink.<backend>]` table.** With `[sink.stdout]` no store is
  opened and the stream is printed instead.

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

[sink.duckdb]
path = "indexer.duckdb"

[decode]
registry = "registry.toml"
```

Each table names what owns its fields, not where a field was first needed:

- `[ingest]` — the chain to follow and its endpoints.
- `[sink.<backend>]` — where records go, and by what. Exactly one backend table may be
  present, and it is required: a run with no store and the next with one are different
  deployments, and there is no default right for both. Naming a backend is naming its
  table, so a second backend (ClickHouse, Postgres) is a new table rather than another
  top-level `database` whose owner a reader has to guess — and a key only that backend
  understands has nowhere else to sit. Commit batching is part of this, because it is a
  property of the store: `DuckDB` folds a backlog into larger transactions, where a
  remote backend would batch unconditionally.
- `[decode]` — the contract registry.

**Required** — no value could be right by accident, so each errors at startup naming
the field:

| Key | Meaning |
| --- | --- |
| `ingest.chain` | Chain id stamped on every event |
| `ingest.http_url` | JSON-RPC endpoint for blocks and receipts |
| `ingest.ws_url` | WebSocket endpoint for `newHeads` |
| one `[sink.<backend>]` table | Which backend writes. `duckdb` persists; `stdout` prints and opens no store |

**Defaulted** — correct for a standard deployment; override for a non-standard one:

| Key | Default | Meaning |
| --- | --- | --- |
| `ingest.datasets` | block, transaction, receipt, log | Which datasets are fetched and stored. The header is still read for every height |
| `ingest.log_addresses` | none | Contracts passed to `eth_getLogs`. Empty fetches every log. Valid only when `log` is selected and `receipt` is not |
| `sink.duckdb.batch_records` | `500` | Most records one store commit may cover; a backlog of blocks is folded into one commit up to this, and a block is never split |
| `sink.duckdb.path` | `indexer.duckdb` | Path to the store |

**Optional tables:**

| Key | Meaning |
| --- | --- |
| `decode.registry` | Path to the contract registry file, relative to the settings file. Absent means nothing is decoded |
| `sink.duckdb.settings` | Any other DuckDB setting, passed straight through |

`RUST_LOG` is still read from the environment, because a log filter is not deployment
configuration.

An unknown key is a startup error naming the line and the key, so a misspelling is
caught rather than silently leaving a setting at its default. An unknown backend is
likewise an error rather than a silent fallback to another one, and naming two backends at
once is an error rather than picking one.

### The contract registry

What decode decodes lives in its own file, named by `[decode] registry` — `registry.toml`
in the repository is a working example. It is deliberately not part of `indexer.toml`: the
settings file is deployment topology (endpoints, paths), while the registry is a
catalog of contracts that grows on its own schedule. Splitting them keeps a new protocol
from churning the deployment diff.

The registry file holds two lists, all data:

```toml
[[abi]]
name = "uniswap_v3_pool"
path = "abis/uniswap_v3_pool.json"

[[contract]]
chain = "base"
address = "0xd0b53D9277642d899DF5C87A3966A349A798F224"
abi = "uniswap_v3_pool"
protocol = "uniswap_v3"
from_block = 0
# to_block = 60000000     # optional exclusive bound
```

**Why an ABI is named once.** A pool protocol like Uniswap V3 has thousands of pools
sharing one ABI. The ABI is loaded and prepared once, content-addressed by `abi_id`,
and shared; an address is a registration.

**Historical registrations.** `protocol` is required; `from_block` defaults to zero.
An omitted `to_block` means no upper bound. Disjoint ranges for one `(chain, address)` support proxy
upgrades: a historical log always uses its original block's registration. Overlapping
ranges, invalid bounds, dangling ABI names, and malformed addresses are startup errors.
ABI paths resolve relative to the registry file, not the working directory.

Registrations are predefined and immutable. Factories do not discover or register child
contracts during decoding; add each child's registration explicitly. Uniswap V4's
`PoolManager` is one address registration because logical pools are `bytes32` ids,
not deployed contracts.

Every decoded record carries the registration's explicit `protocol` and the ABI's
content identity. The protocol is not inferred from an ABI filename.

### What decode does not do

**It does not project a record into a dataset.** Decoding produces *facts* — an
argument's name and its typed value, straight off the ABI:

```json
{"name": "amount0", "position": 2, "abi_type": {"kind": "int256"}, "value": {"type": "int", "value": "-3180585820646654", "bits": 256}}
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

That join is the streaming aggregation layer in [Not built yet](#not-built-yet).
`protocol` and the argument names are the join keys, which is why they are on the
wire rather than re-derived downstream.

### PostgreSQL 18 sink

Build with `cargo run --release --no-default-features --features postgres -- indexer.toml`
(or add `--features postgres` to the default build to retain DuckDB support). Replace
`[sink.duckdb]` with exactly one PostgreSQL sink table:

```toml
[sink.postgres]
connection_string = "host=localhost port=5432 user=indexer dbname=indexer sslmode=require"
batch_records = 500
```

The connection string accepts PostgreSQL URL or keyword syntax. TLS uses the platform
certificate store; use `sslmode=disable` only for trusted local connections. Connection
strings are redacted from startup logs. The database must already exist, and the role
needs permission to create and write the dataset tables in its configured search path.

The sink creates the same six typed tables as DuckDB. Columns the chain always
provides are `NOT NULL`; fields it can omit stay nullable. Each table has a non-unique
index on `(chain, dedupe_key)`. A flush bulk-loads with binary `COPY` in one transaction.
Unsigned 64-bit fields use `NUMERIC(20,0)`, hex values use `TEXT`, booleans use
`BOOLEAN`, and documents use `JSONB`. Existing tables are not migrated. Duplicates are
preserved, and failed batches remain buffered. A lost connection during commit can leave
the outcome unknown; retrying is not exactly-once. Restart recovery remains unimplemented,
as with the DuckDB sink.

The PostgreSQL integration check creates a private schema on a PostgreSQL 18 server:

```bash
INDEXER_TEST_POSTGRES_URL='host=localhost user=indexer dbname=indexer sslmode=disable' \
  cargo nextest run --all-features --run-ignored only -E 'test(sink::postgres::tests::copy_is_atomic)'
```

### Other client settings

The store takes settings this file does not restate, passed straight through and
validated by the engine:

```toml
[sink.duckdb.settings]
threads = "4"
max_memory = "1GB"
```

They live under the `DuckDB` table because they are DuckDB's, and no other backend would
know what to do with them. Anything unrecognized is an error from DuckDB naming the
setting, so a typo is caught at startup rather than silently ignored. Every envelope
lands in its dataset's own typed table — `block`, `transaction`, `receipt`, `log`,
`decoded`, `reorg` — with real columns rather than a JSON blob, so a
consumer filters and joins on values. The schema is generated from the row headers in
`src/wire/row.rs`, so a column added to a dataset appears without anyone editing the
DDL.

Configuration is a typed struct in `src/config.rs`, not a string lookup scattered through
each layer, so a layer can be constructed in a test with no environment at all.
`src/runtime.rs` turns those settings into the pipeline, so the wiring is testable without
a network.

## Event shape

```json
{"chain":"base","v":1,
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
signal** — `reorg` — drives a consumer's state machine and carries no
payload. The line is one flat object: `chain`,
`v`, and the event's fields under its `type` tag. Consumers deduplicate on
each event's `dedupe_key`. Each dataset's key comes from its
natural key, so a transaction and its receipt (both keyed by the transaction hash)
stay distinct — and a key that descends from a block carries the block hash too, so the
two branches of a reorg are separate rows.

`src/wire/row.rs` renders a dataset as the rows a store persists: the table, its columns
with their types, and the values in that order. The mapping is one answer for every
store, so a `ClickHouse` sink and a `DuckDB` one read the same headers and produce
different DDL rather than each deciding what a log is.

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
`Decoder` the decode layer runs. No arguments and no network: the capture and the ABI
are embedded, and each input is printed as its decoded record — which is what the
pipeline stores beside the raw log.

Worth reading the output for two things: `amount0` is negative, because the amount is an
`int256` and the sign says which way the pool sent that token; and `sqrtPriceX96` carries
`bits: 160` while `tick` carries `bits: 24`, because a store needs the declared width to
pick a column and a width is not recoverable from a number.

To point it at other contracts, capture real input by using the `[sink.stdout]`
backend, which prints the stream instead of storing it, and change the address, chain, ABI, and fixture the example names.

## Benchmarks

```bash
cargo bench
```

`benches/hot_path.rs` measures typed block/receipt projection and envelope
serialisation. Alloy's transport and JSON-RPC decoding are not included; typed inputs
are prepared outside the timed projection. End-to-end latency is dominated by the
network and is not reproducible off-line. It is hand-rolled and dependency-free so
it can report percentiles rather than means, and it calls `std::hint::black_box`
explicitly because that is the only way to stop `lto` and `codegen-units = 1` from
deleting the work being measured.

Historical measurements of the previous parse-and-project benchmark on an Apple
Silicon laptop, release profile, ~1,000 samples each (not comparable to the current
projection-only timing):

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
