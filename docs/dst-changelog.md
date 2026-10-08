# DST changelog

Bugs found by the deterministic simulation (`src/sim/`), newest first. One row per bug;
the linked PR carries the cause, the fix, and how it was verified.

Replay a seed on the commit it was found on (later changes reshuffle seeds) with
`SIM_SEED=<n> SIM_TRACE=1 cargo nextest run --all-features simulate --no-capture`.

| ID | Found | Seeds | Bug | Impact | Fix |
| --- | --- | --- | --- | --- | --- |
| DST-001 | 2026-10-08, `bf9a787` | 17, 68, 172, 202 | A fork replacing the first indexed block stopped the pipeline with `UndoWindowExceeded`, again on every restart. | Liveness: stuck until an operator edits the store | |
