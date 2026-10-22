# Multiple stores: open items

Known gaps in running several stores from one ingest (`src/sink/route.rs`,
`src/sink/resume.rs`), decided against or not yet done. Each says what it costs, the
options, and what to do if it starts to matter. Only one can leave a store without
blocks it should hold — a store that never committed on a fresh deployment — and none
duplicates data. Structural cleanups from the review of this change are listed last.

## A rewound store can miss a `reorgs` row

**What.** A store ahead of the others at startup normally keeps its blocks and
reconciles them as ingest hands them again (see `src/sink/resume.rs`): one it holds is
skipped, and one whose hash changed while it was ahead is retracted with a `reorgs` row
of its own. Only a store whose full ledger window starts above the block after the
common one is rewound instead, since its ledger may not name every block it holds in
between. A rewind is not a retraction, so if a reorg orphaned blocks inside that store's
lead, it never records it.

The block data is right in every store either way. Only the audit table can differ.

**Shape of a reconciled retraction.** A store that reconciles writes one `reorgs` row per
height whose hash changed, naming that one orphan, where the store that saw the reorg
live wrote one row naming every orphan. Both name the same blocks.

**When.** A rewind needs a store more than a ledger window (4,097 blocks) ahead of the
slowest one, which a stop leaves only if a lake commit of thousands of small blocks is
dropped; a missed row additionally needs a reorg inside that lead.

**Recommendation.** Accept. Record the rewind as a retraction — write a `reorgs` row for
held hashes another store's ledger contradicts at the same height — only if anything
starts reading `reorgs` as authoritative history.

## A store added later is not backfilled

**What.** A store with an empty ledger — added to a running deployment — joins where the
others resume. The blocks before that are never written into it, and `ingest.start_block`
cannot help, since it applies only when every store is empty.

**Options.** Run a separate one-off indexer against just the new store with a
`start_block`, stopping where the main run began; or add a per-store start height that
ingest backfills for that store alone. The second refetches history over RPC for one
store, which is the cost this design exists to avoid, so it would want a bounded range.

**Recommendation.** Document the one-off run when it is first needed.

## A store that never committed misses the first blocks of a fresh deployment

**What.** A new deployment of a lake and a database, with `ingest.start_block = S`. The
database commits S, S+1, … every block; the lake waits out its commit interval (30 s, or
until its buffer fills, which in a backfill can be many blocks). If the process stops
before the lake's first commit, the restart finds the lake empty, so it joins where the
database resumes, and the lake never holds [S, S+k]. The only sign is an `info` line,
"store has no accepted blocks; it joins where the others resume". Removing
`start_block`, which the restart then asks for, does not bring them back.

**Options.** Log a `warn!` when an empty store joins beside non-empty ones, naming the
height it starts from; refuse to start in that state unless the operator acknowledges it
(a setting, or emptying the other stores); or fill the gap with the one-off run above.

**Recommendation.** At least the `warn!`, so the gap is visible. Do it next.

## No warning when protocols are loaded but nothing decoded is written

**What.** Before multi-store routing, the run warned "`logs` dataset is not selected;
registered contracts will not decode". Now, when `[decode] protocols` is set but no sink
lists `logs`, `decoded_logs`, or an event table, no logs are fetched, nothing decodes,
and `contracts` stays empty — silently.

**Recommendation.** Restore a one-line `warn!` in `Pipeline::from_settings` when the
derived fetch has no logs and the decoder has contracts. Small; do it next.

## Not yet verified

- Reconciling a store that is ahead runs against DuckDB beside the lake in the
  two-store restart tests, but not against PostgreSQL: those tests need a live database
  and are ignored by default.
- **Done:** the SQL rewind, now only a fallback, is tested on both engines, including
  a store far enough ahead that its ledger was pruned. It deletes by height, as the lake
  does; it used to find blocks through the ledger, and missed those the prune had
  dropped.
- A store whose whole history lies above the slowest tip reconciles rather than being
  refused. That rule has unit tests in `resume.rs`, but no end-to-end restart test of its
  own.
- The deterministic simulation compiles against the new runtime but has not been run,
  and models one store. Extending it to two stores that fail independently is the best
  test of reconciliation.

## Structural cleanups

From the code-quality review. None changes behavior.

- **One carrier for what a store writes.** It is held twice: the store's selected
  `Schema` and the router's `HashSet<TableId>` (`Opened.tables`, `Pipeline.routes`).
  `Pipeline::route` falls back to an empty set with `unwrap_or_default`, and
  `Pipeline::new` leaves `routes` empty. Make the selected `Arc<Schema>` the only one and
  have the router ask `schema.writes(id)`; that also removes the `everywhere` case in
  `Router::publish`, since the bookkeeping tables are always in a selected schema.
- **Printing versus storing as a type.** `Backend::Stdout` can reach store-only places
  (`Opened`, `ResumeError`, `RuntimeError::OpenStore`), and `run` rescans the routes for
  it. Split a store-only backend enum, resolve the settings once into "print these
  tables" or "these stores", and `run` stops taking `&Settings` (which also closes the
  README's "`Pipeline::run` reads the settings twice").
- **Adding a backend touches five `cfg`-gated lists** — `Sinks`, `Sinks::tables`,
  `AnyStore`, its `each!` macro, and `Pipeline::open`, which repeats one open block per
  backend. Open stores through `AnyStore` from a typed view of their settings, and give
  `DuckDbSettings` and `PostgresSettings` a `batching()` like `DeltaSettings`.
- **`src/runtime.rs` is past 1,000 lines** (mostly tests): move them to
  `src/runtime/tests.rs`, as `src/sink/delta/tests.rs` does.
- **`Datasets` leftovers.** Its `Default` (all three) and settings-file docs predate the
  derived fetch; `route::datasets` belongs on the type.
- **A third hand-rolled transaction** in `SqlStore` (`rewind` beside `new` and
  `write_batch`): extract one helper.
- **Smaller:** `route.rs` mixes table selection (config) with the `Router` (hot path);
  `Opened` lives in `resume.rs` but its `batching` is only for the drain; `Sinks::tables`
  names which sinks are set; a config test asserts on error text rather than the
  variant. (`AnyStore` not forwarding `observe_head` is not a gap: no store implements
  it. The storage channel takes each sampled head, for its progress line and the
  store's lag metric, before anything reaches a store.)

## Stores further apart than the ledger window cannot resume together

**What.** A restore reads each store's newest `LEDGER_WINDOW` accepted blocks. When one
store's full window lies entirely above another's tip, they share no block in what they
read back, and startup fails with "the stores hold no block in common". That happens to
a store taken out of the settings and added back after the others moved on by more than
the window, and could happen in a fast backfill where a database commits more than a
window of blocks while the lake waits out its commit interval, if the process stops
then. (A store whose ledger is shorter than the window — one that joined recently —
simply reconciles instead; see `src/sink/resume.rs`.)

**Options.** Treat the store that is behind as new (empty its ledger by hand and let it
join), or read a store's ledger by height rather than newest-first, so the common block
can be found further back.

**Recommendation.** Keep the startup error: it is clear, and silently rewinding a store
by thousands of blocks would be worse.

## Logs are decoded even when no store writes a decoded table

**What.** When a store writes `logs` but no store writes `decoded_logs` or an event
table, decode still runs on every log, and the router drops what it produces. The
discovered contracts are still written.

**Recommendation.** Skip decoding in that case if the CPU shows up in a profile.
