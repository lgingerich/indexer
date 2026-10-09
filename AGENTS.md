# AGENTS.md

Guidance for AI coding agents working in this repository.

## Core Behavior

- Ask before assuming when requirements, architecture, or intent are unclear.
- Prefer the simplest solution that satisfies the request.
- Think before writing code for architecture, debugging, or non-trivial feature work. Surface the approach and tradeoffs first.
- Stay in scope. Only modify files and code directly related to the current task.
- Do not refactor, rename, reformat, or reorganize unrelated code. Mention worthwhile follow-ups instead of making them.
- Ask before big changes that restructure existing code, remove behavior, change public APIs, or alter project conventions.
- Do not preserve backwards compatibility for internal Rust APIs unless the user
  explicitly asks. When an internal API needs different semantics, update the
  existing method/type directly and migrate call sites.
- Do not add parallel helper methods such as `*_trusted`, `*_unchecked`, or
  alternate constructors merely to avoid changing existing internal callers.
  Prefer one clear method with the correct invariant boundary.
- Admit uncertainty before it costs the user. If a fact, version, API, or approach is uncertain, say so before relying on it.
- Before destructive or irreversible actions, explain the impact and wait for explicit confirmation.

## Rust Project Standards

- Use Cargo for all Rust workflows.
- Lock the tech stack to Rust, Cargo, rustfmt, Clippy, rustdoc, and cargo-nextest unless the user asks for alternatives.
- Keep this template library-first unless the user asks for binary-specific structure.
- Preserve the pinned Rust toolchain in `rust-toolchain.toml` unless asked to change it.
- Match the existing strict lint style in `Cargo.toml`.
- Prefer clear public APIs, useful documentation, and focused tests.
- Do not weaken lint levels, release profiles, or CI checks without calling it out first.

## Data Modeling

Model datasets on Allium's published schemas before inventing a shape. They cover
many chains (EVM, Solana, and others) and many dataset types (raw, decoded, assets,
DEX, NFTs, metrics), so a matching table is almost always already designed:

- Index of everything: <https://docs.allium.so/llms.txt>
- EVM raw tables: <https://docs.allium.so/historical-data/supported-blockchains/evm/ethereum/raw>
- EVM decoded tables: <https://docs.allium.so/historical-data/supported-blockchains/evm/ethereum/decoded>
- Solana tables: <https://docs.allium.so/_llms/data-catalog/solana.md>
- Core schemas shared across chains: <https://docs.allium.so/_llms/data-catalog/evm/core-schemas.md>

Before adding a dataset or a field, search Allium for the table that already covers
it (chain name + dataset name, e.g. "ethereum raw logs") and copy its column set,
natural key, and normalization. Follow the same conventions this repo already takes
from them (`src/wire/datasets/evm.rs`): one normalized table per dataset, a documented
natural key that becomes the `dedupe_key`, children referenced by scalar key rather
than embedded, and no opaque `raw` blobs — every field present and typed.

## Verification

After code changes, run the relevant checks:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps --all-features
cargo nextest run --all-targets --all-features
```

Never run the deterministic simulation (`sim::simulate`, `SIM_SEED`/`SIM_SEEDS`, or
`--ignore-default-filter`) unless the user asks for it. The default nextest profile
skips it.

## Communication

- Never open with filler phrases like "Great question", "Of course", or "Certainly". Start with the answer or action.
- Match length to the task. Be concise for small tasks and more detailed for design/debugging work.
- Summarize what changed and how it was verified.
- Mention useful follow-ups, but do not implement them without being asked.
