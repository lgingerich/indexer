# Streaming storage and computation design

Status: proposed, not built. Decisions marked **unverified** are gated on the spike at the end.

## Requirements

- **Arbitrary, layered, incremental computation.** Raw → derived → derived-of-derived, any depth.
  Primary input is decoded logs; any dataset may be used.
- **Reorg-correct at every layer.** An orphaned block's rows must be retracted from every
  derived table, through every layer, automatically.
- **Exact 256-bit integers end to end.** No silent rounding or truncation anywhere,
  especially in the raw lake, which is the source of truth.
- **Per-table routing to many destinations.** Any table (raw, decoded, or derived) can go to
  zero or more stores: Postgres, the lake, or others later.
- **Postgres is for app-served data only.** Bulk raw data does not go there.
- **The engine computes only.** It is never a serving database. Results are written back to
  Postgres or the lake.
- **Self-hosted, open source, single node to start.** It must scale out later without a redesign.
- **Datasets follow Allium's schemas** (see `AGENTS.md`).

## Architecture

```
chain ─▶ indexer ─▶ Redpanda: base.blocks  (1 partition, 1 message per block, ordered)
                        │
      ┌─────────────────┼────────────────────────┐
      ▼                 ▼                        ▼
 Postgres writer      router ─────────────▶ base.t.<table>        tip, per table
 (tip; reorg =        (fan-out + finality)   base.canonical_blocks keyed; tombstone = orphaned
  delete by hash)                            base.final.<table>    finalized, append-only
      │                                          │          │
      ▼                                          ▼          ▼
  Postgres  ◀───── derived (tip) ───────── RisingWave ──▶ Iceberg lake
  (app tables)                             layers 1..n    (raw: finalized, exact text;
                                           ◀── backfill ─  derived: finalized)
```

| Component | Role |
|---|---|
| indexer | Unchanged pipeline. One Kafka sink replaces the in-process channel. Resumes from the last `accepted_block` in `base.blocks`. |
| `base.blocks` | Source of truth for the tip. Days of retention. Preserves today's guarantee that a `reorg` arrives after the rows it orphans and before their replacements. |
| Postgres writer | The existing `PostgresSink`, now reading from Redpanda. Writes only the tables routed to Postgres. |
| router | Splits blocks into per-table topics. Maintains `canonical_blocks`. Holds each block until it is finalized, then emits it to `base.final.*` unless it was orphaned. |
| RisingWave | Runs the incremental views. Append-only Iceberg sinks write the raw lake from `base.final.*`. View outputs are sunk to Postgres and Iceberg. |
| Iceberg lake | Source of truth for history. Finalized only, append-only. |

The Postgres writer, router, and backfill tool live in this repo as one library-first consumer binary.

## Per-table routing

Every table has an independent list of destinations. Routing for raw and decoded tables is
config. Routing for derived tables is a RisingWave `CREATE SINK` per view.

```toml
[destination.postgres]
kind = "postgres"
finality = "tip"          # writes at the head; applies reorgs as deletes by block hash

[destination.lake]
kind = "iceberg"
finality = "finalized"    # append-only; never sees a reorg

[[route]]
tables = ["blocks", "transactions", "receipts", "logs"]
to = ["lake"]

[[route]]
tables = ["uniswap_v3_pool_*"]
to = ["postgres", "lake"]
```

Rules:

- **Each destination is its own Redpanda consumer.** It has its own offset, its own ledger of
  committed blocks, and its own batching. A slow or broken destination lags only itself.
- **A destination declares its finality.** `tip` needs a store that can delete by block hash
  (Postgres; ClickHouse would need a deletion strategy). `finalized` stores only ever append,
  which is what makes immutable formats viable.
- **Watermarks.** Each destination publishes the highest block it has committed. A query that
  spans destinations reads only up to the lowest watermark.
- **Replays are idempotent.** Every store upserts or dedupes on `(chain, dedupe_key)`, so
  restarting from any earlier offset is safe.
- **Adding a destination is a new consumer, not an indexer change.** It backfills from the lake,
  then catches up from Redpanda.

Trade-offs:

| | Gain | Cost |
|---|---|---|
| Per-table routing | Postgres stays small. The lake is cheap. Each table lives where its readers are. | No transaction across stores. Stores sit at different heights. Joins across stores are awkward. |
| Routing in consumers, not the indexer | The indexer has one sink. Destinations are independent and replayable. | Another system to run (Redpanda), and one more hop of latency. |
| Two routing surfaces (TOML for raw, SQL sinks for derived) | Each lives next to the thing it routes. | Two places to look. Could be unified into one manifest later. |

## Reorgs

- **Upstream of the engine, the data only grows.** Rows are keyed by block hash, so an orphaned
  row and its replacement are different rows.
- **`canonical_blocks`** gets a hash inserted on `accepted_block` and deleted on `reorg`.
  Every view's first layer joins its input with `canonical_blocks`. A reorg is therefore a
  few deletes on one small table, and the engine retracts everything downstream.
  Arrival order across topics does not matter.
- **Rejected: an explicit delete for every orphaned row.** The indexer would have to keep the
  rows of its last 4,096 blocks in memory.
- **Lake and finalized sinks never see reorgs.** The router holds each block until it is
  finalized, which on Base takes tens of minutes, by the node's `finalized` tag or a fixed depth.
  Days of Redpanda retention covers that wait.
- **View rules:**
  - Bucket time by `block_timestamp`, never by arrival time.
  - Never declare a source `APPEND ONLY`, and never use windows that emit once on close.
    Both drop retractions.
  - Change a view by creating `_v2` beside it, moving readers, and dropping v1.

## Exact 256-bit integers

The widest exact numeric type in each system:

| System | Widest exact type |
|---|---|
| Postgres | `NUMERIC(78,0)`, which the indexer uses today. Full range. |
| RisingWave | `rw_int256`: signed 256-bit; `sum`, `min`, `max` return `rw_int256`. Its `numeric` is only 28 digits. |
| Feldera | `DECIMAL(38)` (`FELDERA_MAX_DECIMAL_PRECISION = 38`). Nothing wider than 64-bit integers. |
| Iceberg, Delta, Parquet, DuckDB `DECIMAL` | 38 digits. DuckDB's `BIGNUM` is unbounded. |
| Arrow, Vortex | 76 digits. Still short of the 78 a `uint256` needs. |

Encoding per layer:

- **Lake: exact decimal text** under the Postgres column name. It is lossless and every
  reader can cast it (DuckDB: `::BIGNUM`). Values that fit in 64 bits stay native integers.
  Round-trip tests cover 0, `uint256` max, and `int256` min and max.
  The alternative is 32-byte big-endian binary: smaller, but nothing can do arithmetic on it.
- **Engine, signed values:** `rw_int256` directly.
- **Engine, unsigned values:** values ≥ 2^255 (e.g. `type(uint256).max` approvals) overflow
  `rw_int256`. Split them into a top and bottom 128-bit half, each its own `rw_int256`. Sums
  stay exact; recombine at output.
- **Approximations:** prices and ratios (`sqrtPriceX96` math, decimal scaling) are `DOUBLE`,
  named as approximate, and never fed back into exact sums.
- **Postgres output:** `rw_int256` → text → `NUMERIC(78,0)`.

## Decisions

### Change capture: from a log, not from Postgres

| Option | Verdict |
|---|---|
| CDC from Postgres via logical replication | Rejected. Raw data will not live in Postgres. |
| Indexer pushes directly to each destination | Rejected. Every new destination needs indexer code. A slow destination stalls ingest. There is no replay for rebuilds. |
| **Redpanda between the indexer and every destination** | **Chosen.** Single binary, Kafka API, which every engine and lake tool reads. Days of retention. |
| Engine reads the lake | Only for backfill. Minutes of latency, and painful deletes. |

### Lake file format: Parquet, not Vortex (for now)

| | Vortex | Parquet |
|---|---|---|
| Speed | About 18% faster than Parquet v2 in DuckDB's TPC-H benchmark | Baseline |
| Size | Slightly larger in the same benchmark | Baseline |
| Stability | File format stable since 0.36. Rust API changes often (0.87, Oct 2026). Linux Foundation, Apache-2.0. | Mature |
| Readers | DuckDB (core extension), DataFusion, Spark, Polars | Everything |
| Table format | None. Iceberg support "coming soon"; one experimental fork PR | Iceberg, Delta |
| Streaming engines | **None read or write it** | All |

Vortex is rejected because no streaming engine reads or writes it, and no table format manages
it. Revisit once Iceberg accepts Vortex data files. Lock-in would be low: DuckDB converts it to
Parquet in one `COPY`.

### Table format: Iceberg

| | Delta Lake | Iceberg |
|---|---|---|
| Catalog | None (the transaction log sits with the data) | Required for writes. Start with JDBC in the existing Postgres; move to Lakekeeper (a Rust REST catalog) for production. |
| Rust writer | `delta-rs`, mature | `iceberg-rust` 0.10, write path still maturing (not needed: RisingWave writes) |
| Ecosystem | Databricks-led | Multi-vendor: Snowflake, BigQuery, S3 Tables, Athena, Trino, DuckDB (read/write) |
| Engine fit | Feldera reads/writes. RisingWave appends. | RisingWave reads and writes with exactly-once. Feldera reads only. |

Iceberg is chosen because it follows from the engine choice and is the broader standard.
Delta would be the pick only with Feldera.

### Engine: RisingWave

Must-haves: retractions through multi-layer views; durable state across restarts in the open
source edition; Kafka in; Postgres and lake out; a path to exact u256 arithmetic.

| Engine | Language / license | Retractions across layers | Durable restart (OSS) | Exact u256 | Lake | Verdict |
|---|---|---|---|---|---|---|
| **RisingWave** | Rust, Apache-2.0 | Yes; cascading materialized views | Yes; checkpoints to object store | `rw_int256` (signed) | Iceberg read/write; Delta append | **Chosen** |
| Feldera | Rust (DBSP), MIT | Yes; cleanest model, Rust UDFs | **No**: checkpoints are Enterprise-only, so every restart recomputes from scratch | `DECIMAL(38)`; needs 4×64-bit limbs | Delta read/write; Iceberg read | Runner-up. Pick it if you buy Enterprise or recompute stays cheap. |
| Apache Flink | Java, Apache-2.0 | Yes; changelog semantics | Yes; RocksDB plus checkpoints | `DECIMAL(38)`; limbs or UDFs | Iceberg, Paimon | Strongest fallback. JVM and heavier to run; layers are separate jobs or one big job. |
| Materialize | Rust (differential dataflow), source-available | Yes; excellent | Yes | Numeric up to 39 digits (unverified) | Limited sinks | Community edition is capped at 24 GiB memory and 48 GiB disk, with a license key. |
| Arroyo | Rust, Apache/MIT | Partial; strongest at windowed, append-style work | Yes | No | Iceberg (via Cloudflare Pipelines) | Cloudflare-owned since 2025, pre-1.0 (v0.15, Dec 2025). Uncertain self-hosted roadmap. |
| Pathway | Rust engine (differential dataflow), Python API, BSL 1.1 | Yes | Persistence available | No | Delta, Iceberg (unverified) | Python-first, non-OSI license. |
| Spark Structured Streaming | JVM | Weak: chained streaming aggregations are limited | Yes | `DECIMAL(38)` | All | Micro-batch, not built for this. |
| Kafka Streams / ksqlDB | JVM; ksqlDB under the Confluent Community License | KTables update | Yes | No | Via Connect | A library or a license constraint; no lake story. |
| ClickHouse materialized views | C++ | **No**: they are insert triggers and never see deletes | — | `Int256`/`UInt256` | — | Wrong for reorgs. Fine as a *destination*. |
| `pg_ivm`, Epsio | Postgres-bound | Partial | — | `NUMERIC` | — | Requires the data in Postgres. |
| Timely / differential dataflow (crates) | Rust library | Yes | DIY | DIY | DIY | Maximum control; all persistence, connectors, and operations are ours. |
| Substreams (The Graph) | Rust→WASM modules, reorg-aware | Yes, via undo signals | Yes | Yes (Rust) | Postgres, ClickHouse sinks | Domain-specific prior art. Needs a Firehose source, so it would replace this indexer rather than sit beside it. |

Also noted: **Apache Paimon** (a lake format whose primary-key tables emit changelogs) and
**Apache Fluss**. Both are Flink-centric. They would replace Iceberg if Flink were chosen and
the lake had to carry updates at the tip.

## Spike (gates the decisions above)

1. RisingWave standalone with Redpanda. Kill it mid-stream; views must resume, not recompute.
2. Write raw data through RisingWave's append-only Iceberg sinks with a JDBC catalog in Postgres.
   DuckDB reads it back, and `::BIGNUM` sums match Postgres.
3. Exact math:
   - `sum(rw_int256)` on signed values
   - the two-half split on `uint256` values, including `type(uint256).max`
   - recombined totals equal `SUM` over `NUMERIC(78,0)`
4. Backfill: lake history into a table, then Kafka input on the same table, with the primary
   key absorbing the overlap. **Unverified:** whether a RisingWave table with a connector also
   accepts `INSERT`.
5. Reorg: a tombstone on `canonical_blocks` retracts through two stacked views.
6. Redpanda: raise `max.message.bytes` for full Base blocks, or compress messages. The
   `rdkafka` client goes behind a `kafka` feature (it builds a C library).

## Sources

- RisingWave: [`rw_int256`](https://docs.risingwave.com/sql/data-types/rw-int256), [Iceberg writes](https://docs.risingwave.com/iceberg/byoi/write-to-iceberg), [Iceberg support](https://docs.risingwave.com/iceberg/iceberg-feature-support), [Delta sink](https://docs.risingwave.com/integrations/destinations/delta-lake), [vs Arroyo](https://risingwave.com/blog/risingwave-vs-arroyo-rust-stream-processors/)
- Feldera: [fault tolerance](https://docs.feldera.com/pipelines/fault-tolerance/), [pricing and editions](https://www.feldera.com/pricing), [Delta input](https://docs.feldera.com/connectors/sources/delta), [connector orchestration](https://docs.feldera.com/connectors/orchestration), [SQL types](https://docs.feldera.com/sql/types)
- Vortex: [repository](https://github.com/vortex-data/vortex), [DuckDB extension and benchmark](https://duckdb.org/2026/01/23/duckdb-vortex-extension), [Vortex-in-Iceberg PR](https://github.com/AstroVela/duckdb-iceberg/pull/36)
- Iceberg and DuckDB: [DuckDB Iceberg writes](https://duckdb.org/2025/11/28/iceberg-writes-in-duckdb.html), [DuckDB catalogs](https://duckdb.org/docs/current/core_extensions/iceberg/catalogs), [Iceberg Rust 0.10](https://iceberg.apache.org/blog/apache-iceberg-rust-0.10.0-release/)
- Others: [Materialize v26 licensing](https://materialize.com/changelog/2025-11-18-v26-release/), [Cloudflare acquires Arroyo](https://blog.cloudflare.com/cloudflare-acquires-arroyo-pipelines-streaming-ingestion-beta), [Pathway](https://pypi.org/project/pathway), [Apache Paimon](https://paimon.apache.org/docs/1.4/)
