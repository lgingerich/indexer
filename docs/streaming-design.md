# Streaming storage and computation design

Status: mostly superseded. The Redpanda log, Iceberg lake, and finalized-only raw sinks this
document proposed were replaced by the Delta Lake sink (`src/sink/delta`), written at the tip,
and by routing tables to stores inside the indexer process. The full proposal, including the
engine and lake-format comparisons, is in git history at `2b750b5`.

## Requirements that still hold

- **Reorg-correct at every layer.** An orphaned block's rows must be retracted from every
  table, including any derived one.
- **Exact 256-bit integers end to end.** No silent rounding or truncation anywhere. Each
  store's encoding is documented beside it (for example `src/sink/delta/arrow.rs`).
- **Per-table routing to many destinations.** Any table can go to one or more stores.
- **Postgres is for app-served data only.** Bulk raw data does not go there.
- **Datasets follow Allium's schemas** (see `AGENTS.md`).

## Open: derived computation

Derived tables are computed inside the indexer, on the sub-second path to Postgres; see
`docs/derive-design.md`. Per-event tables are built there. State and aggregate tables
(raw → derived → derived-of-derived) are designed there and not built.

The engine evaluation that preceded that choice shortlisted RisingWave (durable restarts in open
source, `rw_int256`), Feldera (cleanest retraction model, but checkpoints are Enterprise-only),
and Apache Flink (strongest fallback, JVM). Whatever is chosen must propagate retractions
through every layer: views bucket time by `block_timestamp`, never declare a source
append-only, and never use windows that emit once on close.
