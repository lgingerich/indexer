# Architecture

Project-specific invariants live here; universal Rust workflow lives in `AGENTS.md`. Detail: `src/lib.rs` (layering), `PIPELINE.md` (ingest flow), `docs/derive-design.md` (derived tables), `README.md`.

## Purpose
A low-latency, chain-agnostic blockchain indexer: one crate, one binary (`src/main.rs`), configured by a TOML settings file. Chain data goes in through `ingest`, is decoded against contract ABIs by `decode`, crosses one in-process channel, and is persisted by one or more stores in `sink` (DuckDB, PostgreSQL, Delta Lake on S3-compatible storage, or NDJSON on stdout). `indexer backfill` (`src/backfill`, `delta` feature) decodes protocols added later from logs already in the lake. It does not do mempool, re-decoding of stored logs, streaming aggregation, or signature-only decoding (README "Not built yet").

## Module Boundaries
```text
ingest -> decode -> channel -> store      (ingest and decode are direct calls in one task)
```
- `src/wire`: the published data contract (`Envelope`, `Event`, datasets, typed ABI values). Pure data, depends on no other module.
- `src/window.rs`: the undo window and ledger of accepted blocks. Depends only on `wire`.
- `src/ingest`: `source/` (EVM JSON-RPC, HyperSync backfill) and `pipeline/` (ordering, reorg retraction; see `PIPELINE.md`). Depends on `sink`, `wire`, `window`, `metrics`; knows nothing of `decode`.
- `src/decode`: ABI decoding, factory discovery, derived tables (`derive/`), protocol manifests in `protocols/`. Depends on `sink`, `wire`, `metrics`; knows nothing of `ingest`.
- `src/sink`: `EnvelopeSink`, router, channel, resume, and the stores (`duckdb`, `postgres`, `delta`, `stdout`), each behind a cargo feature except `stdout`. Depends on `wire`, `window`, `metrics`, `config`, `secret`; knows nothing of `ingest`, and only its tests use `decode`.
- `src/config.rs`, `src/secret.rs`, `src/metrics.rs`: outside the layers; any layer may use `secret` and `metrics`.
- `src/runtime`: sits above everything and assembles the pipeline from settings. `src/backfill` and `src/sim` (test-only, `duckdb` feature) drive the real pipeline.

## Core Invariants
- Layer direction is a convention, not compiler-enforced (modules, not crates). CI greps that `decode` and `ingest` never name each other (`.github/workflows/ci.yml`); reviewers check the rest.
- Ingest and decode share one task; the bounded channel in `sink::channel` is the only queue, so a stalled store does not stall ingest. Each store runs on its own runtime (`sink::store_runtime`).
- Reorg-correct everywhere: forks are retracted within the bounded undo window (`window::MAX_UNFINALIZED_BLOCKS`, 4,096), and a restart resumes from the stores' ledgers, bringing every store to the newest block they all hold (`sink::resume`).
- Decoding is attributed: a log is decoded only if its address is a seed or discovered contract of a protocol manifest.
- `wire::envelope::SCHEMA_VERSION` is bumped for any breaking wire change, including a new `Event` variant; new optional fields are additive.
- Data model: model datasets on Allium's published schemas before inventing a shape (they cover many chains and dataset types, so a matching table almost always exists; index: <https://docs.allium.so/llms.txt>, EVM raw: <https://docs.allium.so/historical-data/supported-blockchains/evm/ethereum/raw>, EVM decoded: <https://docs.allium.so/historical-data/supported-blockchains/evm/ethereum/decoded>, Solana: <https://docs.allium.so/_llms/data-catalog/solana.md>, shared core schemas: <https://docs.allium.so/_llms/data-catalog/evm/core-schemas.md>). Conventions, as in one normalized table per dataset in `src/wire/datasets/evm.rs`, a documented natural key that becomes the `dedupe_key` (SQL stores upsert on `(chain, dedupe_key)`; Delta appends and holds replays out with its ledger), children referenced by scalar key rather than embedded, and no opaque `raw` blobs: every field present and typed. Integers are exact (U256/I256), never rounded.
- Before adding a dataset or field, search Allium for the table that already covers it (chain name + dataset name, e.g. "ethereum raw logs") and copy its column set, natural key, and normalization.
- Backfill only appends rows the tables lack, by key, and writes below the lake's tip less four ledger windows.

## Forbidden Patterns / Dependencies
- `decode` must not reference `crate::ingest`, and `ingest` must not reference `crate::decode` (CI-enforced). `sink` must not depend on `ingest`.
- `wire` must not depend on any other crate module and does no I/O.
- No `unsafe` (`unsafe_code = "deny"`).
- No secrets in logs: use `secret::Secret`.
- Store features (`duckdb`, `postgres`, `delta`) must stay optional; the no-engine build is checked in CI.
