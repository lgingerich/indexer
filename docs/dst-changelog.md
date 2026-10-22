# DST changelog

Bugs found by the deterministic simulation (`src/sim/`), newest first. One row per bug;
the linked PR carries the cause, the fix, and how it was verified.

Replay a seed on the commit it was found on (later changes reshuffle seeds) with
`SIM_SEED=<n> SIM_TRACE=1 cargo nextest run --ignore-default-filter --all-features simulate --no-capture`.

| ID | Found | Seeds | Bug | Impact | Fix |
| --- | --- | --- | --- | --- | --- |
| DST-003 | 2026-10-22, `6f4ba5f` | 7 | A lagging backend's "not reached" answer to `eth_getLogs` stopped the run as a transport error. | Liveness: a stop and a supervisor restart on a lagging load-balanced provider | [#7](https://github.com/lgingerich/indexer/pull/7) |
| DST-002 | 2026-10-14, `3446889` | 236 | A restart whose node reported a head behind the store, below every block it handed back, sent that head to the fork walk, which stopped with `UndoWindowExceeded` although the head was the oldest stored block's parent. | Liveness: a stop and a supervisor restart after a fresh start on a lagging node | [#6](https://github.com/lgingerich/indexer/pull/6) |
| DST-001 | 2026-10-08, `bf9a787` | 17, 68, 172, 202 | A fork replacing the first indexed block stopped the pipeline with `UndoWindowExceeded`, again on every restart. | Liveness: stuck until an operator edits the store | [#1](https://github.com/lgingerich/indexer/pull/1) |
