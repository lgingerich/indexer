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

## Agent Workflow

### Simulation

Never run the deterministic simulation (`sim::simulate`, `SIM_SEED`/`SIM_SEEDS`, or
`--ignore-default-filter`) unless the user asks for it. The default nextest profile
skips it.

### Production

The runbook is `../infra/docs/production.md` §6. Read it before touching
production. The VM size, container memory caps, alerts and dashboards live in
`../infra`, not here.

- One Hetzner VM runs a systemd unit `indexer-prod@<chain>` per chain
  (`ethereum`, `bsc`, `robinhood`), each with settings from `indexer.<chain>.toml`.
- Look at production yourself instead of asking the user to paste logs:

  ```sh
  ssh root@indexer-prod   # over Tailscale
  journalctl -u indexer-prod@ethereum -n 200 --no-pager -o cat
  journalctl -u indexer-prod-sync -n 50 --no-pager -o cat
  docker stats --no-stream
  journalctl -k | grep -iE 'oom|killed process'
  ```

  SSH goes over Tailscale, so it needs Tailscale running on this machine. If it
  fails, say so once and use Grafana Cloud logs
  (`{unit=~"indexer-prod@.+"}`) instead of retrying.
- Deploy with `gh workflow run deploy.yml --ref main -f action=deploy -f chains=<chains>`,
  then `gh run watch`. Use `-f action=stop` to stop a chain. A deploy restarts
  the chain, so tell the user before running one.
- Secrets are in the Infisical project `indexer` (`prod` and `dev`). Run commands
  under `infisical run --env=<env> --`. Never print secret values.

Rules:

- Reading the production `s3://blockchain-data` lake from a local run is fine:
  queries, row counts, Delta log and file statistics, anything that writes
  nothing.
- Never point a local run, test or benchmark that writes at production Postgres
  or the lake. A second writer duplicates rows. Use `dev` and a local Postgres,
  or a `_dev`/`_benchmark` prefix in R2 the user has agreed to.
- Before changing memory buffers, batch sizes, concurrency or the VM size,
  measure the change locally under a container memory cap
  (`docker run --memory=<cap>`) and report the numbers. Change one setting per
  deploy so the effect can be attributed.

### Working Alongside Other Sessions

- Other agent sessions may be working in this checkout at the same time. Commit
  only the files this task changed; never `git add -A` or `git add .`.
- For work that spans many files or several commits, suggest a worktree before
  starting on `main`.
- End every task by stating what is committed, what is pushed, and what is left
  uncommitted.
- When a task pauses partway, end with the open items and the decisions already
  made, so a new session can resume without re-asking.
