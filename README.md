# indexer

A low-latency, chain-agnostic blockchain indexer.

It reads a chain's live tip, parses each block into ordered events, decodes the logs of
the contracts its protocol manifests name — including pools a factory creates while it
runs — and stores everything in a local database. Chain-specific knowledge sits
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
      │   accepted_block                  the block's identity, always last
      │
      ▼  flush, once
one channel message
      │
      ▼
one DuckDB commit                         raw rows and decoded rows together
```

A `reorg` marker, when there is one, is the first envelope of that message: the fork is
recorded before the replacement block. An `accepted_block` marker is always the last: the
block's height, hash, parent hash, and timestamp, published for every block — empty ones
included, whatever datasets are selected — so it commits with the block's rows and is
what a restart resumes from.

### What decode adds

Decode never removes or rewrites an envelope. It only inserts records after a log it
can decode.

```
envelope
   │
   ├─ block, transaction, receipt ─────────▶ forwarded as it arrived
   ├─ reorg ───────────────────────────────▶ forwarded; contracts created in the
   │                                          orphaned blocks stop decoding
   └─ log
        ├─ address not a known contract, or no such event ─▶ the log, nothing added
        ├─ event matches, data does not decode ────────────▶ the log, error logged
        ├─ decodes ────────────────────────────────────────▶ the log, then its decoded record
        └─ decodes, and is a creation event ──────────────▶ the log, its decoded record,
                                                             then the contract it created
```

The `Decoder` holds the *contract set*: every address it decodes, mapped to a protocol
and a contract. Seeds come from the protocol manifests; a factory's creation event
— Uniswap V3's `PoolCreated` — adds the pool it names the moment it decodes, so the
pool's own logs later in the same block, even the same transaction, decode too. See
[Protocol manifests](#protocol-manifests).

### A reorg

Height and parent-hash linkage are checked before publishing each block. Ingestion
retains the most recent 4,096 accepted block identities as a sliding window, plus one
predecessor kept only as a linkage floor. The window is a bounded recovery budget, not a
finality claim: a fork deeper than it stops with an error rather than a guessed retraction.

```
published     1 ─── 2 ─── 3
new head           └─── 2'          2' builds on 1, not on 2

published, in order
  block 1, block 2, block 3
  reorg { height: 2, orphaned_hashes: [3, 2] }
  block 2', and its transactions, receipts, logs, and decoded rows

stored
  block 1, block 2'                 and each one's rows
  reorg { height: 2, orphaned_hashes: [3, 2] }
```

The store holds only the current chain. A `reorg` deletes every row of the blocks it
orphans — from `blocks`, `transactions`, `receipts`, `logs`, `decoded_logs`, `contracts`, and
`accepted_blocks` — in the same transaction that writes the `reorgs` row and the
replacements, so no commit ever shows both branches or neither. A dataset query needs no
filter. The `reorgs` row stays as the record of what was retracted.

Each table names the column its block is found by (`Table::block_hash_column`: `hash`
for `blocks` and `accepted_blocks`, `block_hash` elsewhere), and the delete goes by that
column, not by `dedupe_key`. An orphaned block is either already committed or still
buffered in the same batch — blocks are published in order and the storage channel is
FIFO — so the store drops the buffered rows when the `reorg` arrives and deletes the
committed ones before writing the batch. Deleting first is what stores a block that
returns: orphaned by one `reorg` and canonical again after a later one, its republished
rows are written after the delete. A failed commit keeps the deletions with the rows and
retries both; a retry of a commit that did land deletes nothing and upserts onto itself.

In `PostgreSQL`, `(chain, dedupe_key)` is each table's primary key — the replica
identity logical replication needs to publish a delete — and `(chain, <block hash>)` is
indexed so the delete does not scan. The index is added to an existing table at
startup; the primary key is not, so a table created before it keeps its `UNIQUE`
constraint and a store meant for logical replication should be re-created. `DuckDB`
deletes the same rows without the extra index.

A row's key carries the block's hash, so orphaned block 2 and replacement block 2' are
different rows rather than one row overwritten. A fork older than the retained window is
an error rather than an empty retraction.

A replay of a row already stored updates that row. Both stores upsert on
`(chain, dedupe_key)`: only the last published copy of a key in the batch is loaded,
then merged. A `reorg` marker is keyed by the new head, so a replay of that head updates
the one marker.

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
the record downstream, so a restart continues from the last block the store committed,
and the node serves the blocks after it.

### Resume from the store

The store's `accepted_blocks` table is a ledger of every block it committed, one row per
block, each written in the same transaction as that block's rows. At startup the newest
4,097 rows are read back — an orphaned block's row was deleted by its `reorg` — the undo window plus its
floor — and every older row for the chain is deleted, so the table stays bounded across
restarts. Only the newest parent-linked run of those rows is kept; a gap below it is
logged and the window starts above it.

```
stored ledger      … 98 ── 99 ── 100                 the restored tip is 100
node, unchanged    … 98 ── 99 ── 100 ── 101 ── …     resume at 101
node, forked       … 98 ── 99'── 100'── 101'── …     reorg { orphaned: [100, 99] },
                                                     then 99', 100', 101', …
```

The restored tip's header is re-read from the node — one call for the header alone,
since only its hash is compared. If the hash matches, indexing continues from the next
height through the same split a fresh start uses: buried heights backfill, the tail goes
through the reorg-aware path. Nothing the store already committed is fetched or written
again; only blocks that were still in flight at the crash are. If the hash differs, the
stored suffix was orphaned while nothing was running: the full block is fetched, and the
ordinary fork walk finds the common ancestor inside the restored window, appends one
`reorg` naming the stored hashes, and replays the replacements. Either way the first new
block must link to the restored tip, so coverage from there forward has no gap. A fork
deeper than the restored window stops with an error for an operator to resolve; history
is never silently cleared. The window bounds how far below the stored tip a fork may
reach, not how long the process was down: a week offline with a three-block reorg near
the old tip resumes normally. Discovered contracts
are restored with their creating block's hash, so that startup `reorg` also retracts a
pool created in an orphaned block.

How each crash point resolves:

| Crash point | Store at restart | What startup does |
| --- | --- | --- |
| A block was queued, not committed | The ledger's tip is older | Replays from the tip |
| A committed, its later `reorg` did not | A is in the ledger, with no marker | Finds A differs, appends the `reorg`, replays |
| The `reorg` committed, replacements did not | A was deleted; the tip falls back to the ancestor | Resumes after the ancestor, fetching the replacements |
| B committed, progress only in memory | The ledger includes B | Resumes after B |
| A `PostgreSQL` commit's outcome is unknown | Whatever committed | Reads it; replaying a committed row upserts onto itself |

`ingest.start_block` applies to an empty store only. Set alongside a non-empty ledger it
is a startup error rather than a choice between the two, since starting elsewhere would
recreate the gaps resume closes. `[sink.stdout]` has no store, so it always starts fresh.

Resume guarantees contiguity from the restored tip forward, not before it: gaps that
runs before resume existed left behind are not backfilled. And `MAX(block_number)` of a
dataset is not a substitute for the ledger — empty blocks write no rows, and a maximum
proves nothing about contiguity.

## Layout

One crate, one binary, four layers as modules. What runs is not hardcoded: the settings
file states it, and `runtime` assembles the pipeline — ingest follows the configured chain,
decode uses the configured protocol manifests, storage writes the configured store. `main` is only
the process boundary. The layers share one definition of the stream through `wire`.

```
src/
├── main.rs         the process boundary: logging, the settings path, the exit code
├── runtime.rs      assembles the pipeline the settings describe, and runs it
├── config.rs       typed settings — layer configuration, not string lookups
├── wire/           the wire contract: envelope, events, dataset records, the rows a
│                   store persists, and each decoded event's typed table. Pure data.
├── ingest/         block sources and the reorg-aware pipeline.
├── decode/         protocol manifests, the log decoder and its contract set, factory
│                   discovery, and the live decoding sink.
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
  and `[ingest] log_addresses` limits that call to those contracts. A live `newHeads`
  notification carries the head's identity, parent hash, and timestamp, so a logs-only
  fetch reuses them and pins `eth_getLogs` to the announced hash instead of re-reading
  the header; a block, transaction, or `receipts` dataset still fetches the body it needs.
  See `src/ingest/source/evm.rs`.
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
  log from a known contract, inline in `DecodingSink`. Everything is forwarded as it
  arrived — decode adds records and removes none — so a reorg marker reaches the store
  exactly once, from ingest. Decoded records carry the event definition's hash
  (`event_id`), the protocol, and each argument's original position and complete ABI
  schema. Raw byte strings remain lossless, with readable text only when valid UTF-8.
- **Factory discovery.** A manifest's `created_by` rule turns a factory's creation event
  into a new contract in the set, live, and publishes it as a `contract` row in the same
  commit as the block that created it. A reorg retracts contracts created in orphaned
  blocks. At startup the store's `contracts` rows are read back, so a restart keeps
  decoding every pool a previous run discovered, and resume replays the blocks missed
  while down, so pools created then are discovered too.
- **A local store.** The process writes every envelope, raw and decoded, into a local
  `DuckDB` database: one typed table per dataset, with real columns rather than a JSON
  blob, and one per decoded event, with a column per argument. What a dataset's columns *are* lives in `src/wire/row.rs`, not in the store, so a
  second store reuses the mapping instead of re-deriving it.
- **Sliding undo window, no finality assertion.** Ingestion keeps the most recent
  4,096 accepted block identities plus one predecessor as a recovery floor. It does not
  read the node's `finalized` tag and does not claim any height is irreversible. The
  window caps how deep a fork reconciliation can reach; a deeper fork stops rather than
  guessing. Each identity is also written to the store's `accepted_blocks` ledger, which
  is what a restart restores the window from.
- **Reorg retraction.** Parent-hash linkage is checked on every block. A mismatch
  appends a `reorg` event whose `orphaned_hashes` name the block hashes that stopped
  being canonical, then appends the replacement branch under its own keys. The store
  deletes the orphaned blocks' rows in the same transaction, so its tables hold only the
  current chain and the `reorgs` rows are the history of what was retracted.
  The sliding window retains up to a fixed 4,096 identities. See `src/ingest/pipeline.rs`.
- **Headless backfill, then live heads.** Startup samples the head once and captures it
  as a backfill target; on an empty store `[ingest] start_block` may request earlier
  inclusive history but defaults to the head. Backfill then reads consecutive concrete heights and makes no
  further discovery call — not even while reconciling a fork — until it reaches the
  target, at which point live heads take over. Decode runs on both paths, so a historical
  log is stored raw and, when it matches, decoded. Alloy keeps the WebSocket subscription
  independent of fetches and sink delivery, and reconnects and resubscribes with bounded
  retries. Live notifications are inputs and hints, not a replay log: a duplicate head is
  skipped before any fetch, a gap is filled from the next height, and a 30-second timer
  reconciles when the subscription is silent. HTTP failures and exhausted subscription
  recovery are terminal.
- **Resume from the store.** A restart continues after the last block the store
  committed, reconciling a fork that happened while it was down. See
  [Resume from the store](#resume-from-the-store).
- **NDJSON to stdout.** See `src/sink/stdout.rs`; the `[sink.stdout]` backend prints the
  stream instead of storing it, and opens no store.
- **A local `DuckDB` store**, behind the `duckdb` feature. Embedded and
  single-writer, so it is the archive; anything downstream reads from it rather than from
  the live stream.

The default build has only `stdout`, since it needs no engine and no store: a bare
`cargo run` compiles fast and prints the stream. Add `--features duckdb` for the store, or
`--features postgres` for a remote one. No broker is involved anywhere.

## Not built yet

The running process samples the head, backfills through it, then follows live heads and
writes every row. These are the gaps that leaves.

```
stored-log replay
streaming aggregation
Avro for a serialized envelope
ranged log backfill
signature-only decoding
event table and schema limitations
```

### Stored-log replay

Re-decoding logs already in the store is not implemented. That path would read
retained raw logs, run them through the same decoder, and replace or deduplicate the
decoded rows. The store upserts on `(chain, dedupe_key)`, which makes a replay of the
same interpretation replace the stored row. A changed event definition stays a separate
row, because its `event_id` is part of the key. A replay would also have to walk the
contract set in block order, since a contract decodes only from its creation onward.

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
height at a time — even a live notification reuses its header but fetches logs for
that one height. A wider `eth_getLogs` range covering many historical blocks, and
cross-block batching of the per-height block/receipt calls, are not built.
A `logs` subscription is not the log source: it has no end-of-block marker.

### OpenTelemetry metrics

Metrics are not exported yet. Nothing synchronous should run per envelope: a record in
`DecodingSink::publish` is paid per log on the ingest task, the one that has to stay ahead
of the chain. Height, head, and lag become observable gauges reading the atomics ingest
already writes; blocks, rows, and commit duration record once per commit in
`ChannelReceiver::drain`, on the storage task. Instruments are built once with fixed
attributes (`chain`, `sink`). Attributes that vary per block — hash, address, selector —
create a series per event and are the one way to make this expensive; `cargo bench
--bench hot_path` with the meter live is the check that indexing latency has not moved.

### Signature-only decoding

Decode is *attributed*: a log decodes only when its address is a known contract — a
seed or a discovered child — so every decoded record carries a protocol that is a fact
about its emitter. Allium also publishes a broader form, decoding any log whose `topic0`
matches a known event, preferring the contract's own ABI and falling back to a generic
one. The catalog already keys events by selector and topic count, so a
`(topic0, topic count) → event` index across every manifest is the lookup that needs;
those records would carry no protocol, since a matching signature says nothing about who
emitted it — anyone can deploy a contract that emits a lookalike `Swap`. Not built.

### Event table and schema limitations

Known gaps in the [event tables](#protocol-manifests) and the store schemas, none of them
handled yet:

- **An event declared twice under one name stops startup.** A proxy upgrade lists each
  version's ABI under one contract, and when an upgrade changes an event's argument
  types, the old and new definitions both exist, both named `Swap`, and both want the
  table `{protocol}_{contract}_swap`. That is a duplicate-name error, and there is no way
  around it but dropping one version's ABI — losing either the logs before the upgrade
  or the ones after. A Solidity overload in one ABI fails the same way. The fix, when a
  contract needs it, is a suffix on later definitions or a per-event name in the
  manifest.
- **Tables are created, never migrated.** A store creates a missing table and leaves an
  existing one alone. An ABI edited in place — a renamed or retyped argument — changes
  the columns the indexer writes, so the next write to that table fails; drop the table
  first. The same holds for dataset tables when a release changes their columns: the
  `decoded_logs` table gained a `contract` column, so a store created before that must be
  re-created.
- **Older PostgreSQL tables lack a primary key.** Tables created before `(chain,
  dedupe_key)` became the primary key keep a `UNIQUE` constraint instead, which logical
  replication does not accept as a replica identity, so a reorg's delete fails once such
  a table is in a publication. Re-create the store.
- **Long names need a manual `table` key.** A name past 63 bytes is a startup error, so
  a contract with long event names needs a `table` key chosen by hand; there is no
  automatic shortening.
- **Documents keep the wire encoding.** Array and tuple arguments, and every argument in
  `decoded_logs`, are JSON in the published form, where integers are `0x` hex strings.
  PostgreSQL cannot cast a 256-bit hex string to `NUMERIC` without a helper function, so
  arithmetic on a value inside a document is awkward. Scalar arguments have typed columns
  and are not affected.
- **A `string` argument may be `NULL`.** A string whose bytes are not valid UTF-8, or that
  holds a NUL, which PostgreSQL rejects in text, is `NULL` in its event table. The raw log
  keeps the exact bytes.
- **DuckDB's `BIGNUM` is exact only for sums and additions.** Multiplying a `BIGNUM` by
  another type returns a `DOUBLE`; cast first when precision matters. PostgreSQL's
  `NUMERIC` stays exact.

Also not built, and not on the path above: mempool ingestion and a Parquet archive.

## Run it

One binary runs the pipeline the settings describe: ingest follows the chain, decode adds
a record for each log it has an ABI for, and storage writes both into the store. Ingest and
decode run together in one task and storage in another; a part that ends for good stops the
process, because continuing without it would leave a stream that looks alive but is not.

- **`[ingest]` is required** — it names the chain to follow.
- **Decode uses `[decode] protocols`.** Leaving it out decodes nothing, which is a
  legitimate way to run and is said at startup; a directory with no manifest in it is a
  startup error.
- **Storage is the `[sink.<backend>]` table.** With `[sink.stdout]` no store is
  opened and the stream is printed instead.

Settings come from a TOML file, named by the first argument or defaulting to
`indexer.toml`. `indexer.toml` in the repository is a working example. What decode decodes
lives in a directory of protocol manifests, named by `[decode] protocols` and defaulting
to nothing — `protocols/` is a working example; see [Protocol manifests](#protocol-manifests).

```bash
RUST_LOG=info cargo run --release --features duckdb -- indexer.toml
```

```toml
[ingest]
chain = "base"
http_url = "https://base-rpc.publicnode.com"
ws_url = "wss://base-rpc.publicnode.com"

[sink.duckdb]
path = "indexer.duckdb"

[decode]
protocols = "protocols"
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
- `[decode]` — the protocol manifests.

**Required** — no value could be right by accident, so each errors at startup naming
the field:

| Key | Meaning |
| --- | --- |
| `ingest.chain` | Chain id stamped on every event |
| `ingest.http_url` | JSON-RPC endpoint for blocks and receipts. A [secret](#secrets) |
| `ingest.ws_url` | WebSocket endpoint for `newHeads`. A [secret](#secrets) |
| one `[sink.<backend>]` table | Which backend writes. `duckdb` persists; `stdout` prints and opens no store |

**Defaulted** — correct for a standard deployment; override for a non-standard one:

| Key | Default | Meaning |
| --- | --- | --- |
| `ingest.datasets` | blocks, transactions, receipts, logs | Which datasets are fetched and stored. A dataset that needs the block body still reads it; a logs-only live fetch reuses the notification header |
| `ingest.log_addresses` | none | Contracts passed to `eth_getLogs`. Empty fetches every log. Valid only when `logs` is selected and `receipts` is not |
| `ingest.start_block` | the sampled head | First height to index on an empty store. Absent starts live at the observed head; a value backfills that inclusive height forward before following live heads. A value above the sampled head is a startup error, and so is a value when the store already holds blocks: a run resumes from the store |
| `sink.duckdb.batch_records` | `500` | Most records one store commit may cover; a backlog of blocks is folded into one commit up to this, and a block is never split |
| `sink.duckdb.path` | `indexer.duckdb` | Path to the store. Tables live in a schema named for the chain: `base.logs` |

**Optional tables:**

| Key | Meaning |
| --- | --- |
| `decode.protocols` | Path to the protocol manifests directory, relative to the settings file: the whole tree, a group such as `protocols/uniswap`, or one protocol. Absent means nothing is decoded; a directory with no manifest in it is a startup error |
| `sink.duckdb.settings` | Any other DuckDB setting, passed straight through |

`RUST_LOG` is still read from the environment, because a log filter is not deployment
configuration. When it is unset, a debug build logs at `info` and a release build at
`error`, so the developer sees the run and a deployment's log volume stays the operator's
call.

An unknown key is a startup error naming the line and the key, so a misspelling is
caught rather than silently leaving a setting at its default. An unknown backend is
likewise an error rather than a silent fallback to another one, and naming two backends at
once is an error rather than picking one.

### Secrets

The RPC endpoints (`ingest.http_url`, `ingest.ws_url`) and the PostgreSQL
`connection_string` are secrets: a provider's URL usually carries its API key, and a
connection string its password. Each is written either as the value or as the
environment variable that holds it:

```toml
http_url = "https://base-rpc.publicnode.com"   # a public endpoint, written as its value
ws_url = { env = "INDEXER_WS_URL" }             # read from the environment at startup
```

The variable is read once, as the settings load, so a deployment missing one stops at
startup naming it. A secret is never logged: settings print it as `[redacted]`, the
startup log omits the endpoints, and RPC errors drop the URL reqwest attaches to them.

In production, the settings file names the variables and the platform's secret manager
injects them into the process — on Cloudflare Containers, the Worker that starts the
container passes Worker secrets or Secrets Store values as its environment. The
indexer reads only the environment, so the same file and binary run under any manager.

For local development, either:

- export the variables: copy `.env.example` to `.env` (gitignored) and load it with
  `set -a && . ./.env && set +a`, or `dotenv` in a direnv `.envrc`; or
- write the values into a gitignored local settings file — any `*.local.toml`, e.g.
  `cargo run -- indexer.local.toml`.

### Protocol manifests

What decode decodes lives in a directory named by `[decode] protocols` — `protocols/` in
the repository is a working example. It is deliberately not part of `indexer.toml`: the
settings file is deployment topology (endpoints, paths), while the manifests are a catalog
of protocols that grows on its own schedule.

Each protocol is a directory holding a `protocol.toml` and the ABI files it names. They
can sit at any depth, so versions live under their protocol; a directory without a
manifest only groups others:

```
protocols/
  uniswap/
    v3/
      protocol.toml
      UniswapV3Factory.json
      UniswapV3Pool.json
    v4/
      protocol.toml
      PoolManager.json
```

`[decode] protocols` may name the whole tree, a group (`protocols/uniswap` loads v3 and
v4), or one protocol (`protocols/uniswap/v4`); every manifest under it loads. A
manifest's `protocol` value is its name — on decoded records and in table names — so
where it sits, and which directory the setting names, never changes what it writes.

```toml
protocol = "uniswap_v3"

[[contract]]
abi = ["UniswapV3Factory.json"]
addresses = { base = ["0x33128a8fC17869897dcE68Ed026d694621f6FDfD"] }

[[contract]]
abi = ["UniswapV3Pool.json"]
table = "pool"                     # only because a default name passes 63 bytes
created_by = [{ contract = "UniswapV3Factory", event = "PoolCreated", param = "pool" }]
addresses = { base = ["0xd0b53D9277642d899DF5C87A3966A349A798F224"] }   # optional seeds
```

**A `[[contract]]` is one contract of the protocol,** named after its first ABI file:
`UniswapV3Pool.json` is `UniswapV3Pool`, so name ABI files after the contract they
describe. A rule names its parent by that name, and a stored discovery uses it to find
its ABI after a restart, so keep it stable (list a proxy's original ABI first and append
upgrades). Every decoded record carries the manifest's `protocol`. A process indexes one chain, so only that
chain's `addresses` are loaded; the rest are parsed and dropped.

**Discovery replaces listing children.** `created_by` says that when an instance of the
parent `contract` emits `event`, its `param` argument is a new instance of this one. The
event and parameter are resolved against the parent's ABI at load — Uniswap V3's
`PoolCreated.pool` is the fifth, non-indexed argument, Metric's `poolAddress` the first,
indexed one — and the parameter must be an `address`, so a typo is a startup error
rather than a rule that never fires. A discovered contract decodes from its creation log
onward, and its row in `contracts` records the factory, the creation log, and the block.
Rules chain: a discovered contract can have `created_by` children of its own.

**Restarts.** The `contracts` rows are the state: each commits in the same transaction as
the block that created it, so the store never holds a block without its discoveries or
the reverse. At startup the store's rows are read back; one created in a block a `reorg`
orphaned was deleted with that block. A run [resumes from the store](#resume-from-the-store), so the
blocks missed while the process was down are replayed and a pool created then is
discovered; a reorg while it was down retracts a pool whose creating block it orphaned.
A pool created before the first run is never seen — list those as seeds. With
`[sink.stdout]` there is no store, so each start begins from the seeds.

`ingest.log_addresses` turns discovery off, with a warning at startup. That filter is an
explicit list fixed at startup, so a discovered contract's logs would never be fetched.

**Upgrades.** List every version's ABI in `abi`. Events merge by selector and topic
count, so a log decodes as whichever version emitted it; the same selector with a
different definition is a startup error. There are no block ranges.

**Event identity.** Each decoded record's `event_id` is the hash of the one event
definition it used — name, parameter names, types, and indexed flags — not of the ABI
file, so adding or editing another event in the file leaves existing rows' keys alone.

Uniswap V4's `PoolManager` is a single seed: its pools are `bytes32` ids inside one
contract, not deployed contracts, so there is nothing to discover.

**Event tables.** Every decoded record is stored twice: in the generic `decoded_logs` table,
with its arguments as documents, and in its event's own typed table, with one column per
argument. A table is named `{protocol}_{contract}_{event}`: the manifest's `protocol`,
the contract's name in `snake_case` — or its `table` key, when it has one — and the
event's name in `snake_case`:

```
uniswap_v3_pool_swap
  address, transaction_hash, transaction_index, log_index,
  sender, recipient, amount0, amount1, sqrt_price_x96, liquidity, tick,
  block_number, block_hash, block_timestamp, chain, dedupe_key
```

| ABI type | PostgreSQL | DuckDB |
| --- | --- | --- |
| `uint8` – `uint64` | `NUMERIC(20,0)` | `UBIGINT` |
| `int8` – `int64` | `BIGINT` | `BIGINT` |
| wider integers | `NUMERIC(78,0)` | `BIGNUM` |
| `bool` | `BOOLEAN` | `BOOLEAN` |
| `address`, `bytesN`, `bytes`, `function` | `TEXT`, `0x` hex | `VARCHAR`, `0x` hex |
| `string` | `TEXT`; `NULL` when not valid UTF-8 or holding a NUL | same |
| arrays, tuples | `JSONB`, as in `decoded_logs` | `JSON`, as in `decoded_logs` |
| an indexed `string`, `bytes`, array, or tuple | its topic hash, as text | same |

Wide integers are exact, so `sum(amount0)` needs no cast. In DuckDB, `BIGNUM` sums and
adds exactly, but multiplying it by another type turns the result into a `DOUBLE`; cast
first when that matters. Argument names become `snake_case` (`sqrtPriceX96` is
`sqrt_price_x96`); an unnamed argument is `arg{n}`, and one that repeats an earlier column
gets `_{n}` appended. A row's key is the decoded record's `dedupe_key`, and a reorg
deletes it with the rest of its block.

A table name may be at most 63 bytes, PostgreSQL's limit, past which it would silently
truncate the name; a longer one is a startup error asking for a shorter `table`. So is
a name produced twice: two contracts given the same `table`, or one contract declaring
two events by one name. Tables are created at startup and never altered. See
[event table and schema limitations](#event-table-and-schema-limitations) for what that
leaves unhandled.

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
| Which contract this is | the protocol manifests | yes |
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

Build with `cargo run --release --features postgres -- indexer.toml`
(or name both features to keep DuckDB support as well). Replace
`[sink.duckdb]` with exactly one PostgreSQL sink table:

```toml
[sink.postgres]
connection_string = { env = "INDEXER_POSTGRES_URL" }
batch_records = 500
```

The connection string accepts PostgreSQL URL or keyword syntax, and is a
[secret](#secrets) since it carries the password. TLS uses the platform certificate store;
use `sslmode=disable` only for trusted local connections. The database must already
exist, and the role needs permission to create a schema in it and tables in that schema.

Tables live in a schema named for the chain — `base.logs`, `base.uniswap_v3_pool_swap` —
created at startup if missing and set as the session's `search_path`. Processes indexing
different chains can share one database without sharing tables, and a chain is dropped
or re-indexed with `DROP SCHEMA base CASCADE`. Every row still carries its `chain`
column, so rows unioned across chains stay self-describing. DuckDB does the same within
its file.

The sink creates the same tables as DuckDB: the eight dataset tables, and the
[event tables](#protocol-manifests). Columns the chain always
provides are `NOT NULL`; fields it can omit stay nullable. Each table's primary key is
`(chain, dedupe_key)`, and every table but `reorgs` is indexed on its block hash for
[reorg deletes](#a-reorg). A flush bulk-loads with binary `COPY` into a temporary
table, then upserts into the dataset table, in one transaction. Unsigned 64-bit fields
use `NUMERIC(20,0)`, hex values use `TEXT`, booleans use `BOOLEAN`, and documents use
`JSONB`. A replay updates the existing row. Columns of an existing table are not
migrated. Failed batches remain buffered. A lost connection during commit can leave
the outcome unknown; retrying is not exactly-once. Restart recovery remains unimplemented,
as with the DuckDB sink.

For logical replication (CDC) into a streaming engine, publish the chain's schema, so
event tables a new manifest adds are included as they are created:
`CREATE PUBLICATION base FOR TABLES IN SCHEMA base`, one per chain or one listing
several.

The PostgreSQL integration checks create a private schema on a PostgreSQL 18 server:

```bash
INDEXER_TEST_POSTGRES_URL='host=localhost user=indexer dbname=indexer sslmode=disable' \
  cargo nextest run --all-features --run-ignored only -E 'test(sink::postgres::tests::)'
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
lands in its dataset's own typed table — `blocks`, `transactions`, `receipts`, `logs`,
`decoded_logs`, `contracts`, `reorgs`, `accepted_blocks` — with real columns rather than a JSON blob, so a
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
records — `decoded` and `contract` — are what the decode stage produces from a log:
typed ABI arguments, whose types travel with them so a consumer can rebuild a typed
column without reading the ABI, and the contracts a factory created, modeled on the
provenance columns of Allium's `dex.pools`. **Control
signals** — `reorg` and `accepted_block` — drive a consumer's state machine and carry no
payload: a `reorg` names the hashes that stopped being canonical, and an `accepted_block`
closes each block with its identity, so a consumer sees where every block ends, even an
empty one. The line is one flat object: `chain`,
`v`, and the event's fields under its `type` tag. Consumers deduplicate on
each event's `dedupe_key`. Each dataset's key comes from its
natural key, so a transaction and its receipt (both keyed by the block hash and the
transaction hash) stay distinct by their dataset tag — and a key that descends from a
block carries the block hash, so the two branches of a reorg are separate rows.

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
