# Delta Lake sink: open items

Known gaps in `src/sink/delta`, decided against for now. None loses or duplicates block
data; each says what it costs, the options, and what to do if it starts to matter.

## `reorgs` audit rows

**What.** Two gaps in the `reorgs` table, which is an audit record and holds no block data:

1. **A→B→A.** Block A is orphaned by retraction R1, then a later reorg makes A canonical
   again. On the next open, `repair_reorgs` sees R1's orphan in the ledger, takes R1 for a
   retraction a stopped commit left unfinished, and deletes its row.
2. **Duplicate keys.** The natural key is `{new_head_hash}:reorg`. A retraction recorded
   in two commits is two rows; the SQL stores upsert it into one.

**Why it is not simple.** At open, "a stopped commit left this unfinished" and "this
finished and a later reorg reversed it" look the same: its orphans are in the ledger.
`new_head_hash` does not tell them apart, since after a flip back the old new head is
out of the ledger too.

**Options.**

- *Accept* (current). The module docs say a flip back costs an audit row.
- *A commit number.* Each lake commit records an increasing number in every table's
  commit metadata (Delta's application transaction ids are made for this). A retraction
  is unfinished only if the `reorgs` table's newest commit is newer than the ledger's, and
  only rows of that last commit are considered. Exact, with no clocks, and it never
  touches an older finished retraction.
- *MERGE for `reorgs`.* delta-rs has `merge`; writing `reorgs` by merging on `dedupe_key`
  rather than appending fixes the duplicates and matches the SQL stores. The table is
  small and unpartitioned, so a merge is cheap.

**Recommendation.** Do both the commit number and the merge if anything starts reading
`reorgs` as authoritative history. Until then, accept.

## Concurrent table writes in a commit

**What.** A commit writes table by table. Its time is roughly, per table touched, one
Parquet upload and one log commit: an estimated 100–300 ms each on S3, not measured. At
the tip, 10–20 tables every 30 s is a few seconds, which the 32-block channel absorbs.
Catching up, commits are large and the drain waits on each.

**Options.** Sequential (current); every table at once; or a few at a time.

**Considerations.** The order the ledger rule needs stays fixed: retractions first, the
ledger's delete first among the deletes, the ledger's append last. Only the middle step
(other tables' deletes and appends) can run concurrently. Concurrency builds every
table's Arrow batch at once, so a commit's peak memory approaches twice the buffer
rather than the buffer plus one table. One table failing while others succeed is the
partial commit the next open already repairs.

**Recommendation.** Measure commit time against real S3 first. If catching up is too
slow, run the middle step about four tables at a time.

## `StoreError::Reserved`

**What.** The lake adds a `block_range` partition column, so a table that declares one
cannot be stored there, and the lake refuses it at open with a variant only it uses.

**Options.** Keep it (current). Reserve the name in `TableBuilder`, which removes the
variant but rejects, for every store, an event whose argument is named `block_range`. Or
rename the partition column to something no column can be, which depends on how event
argument names are sanitized.

**Recommendation.** Keep it: it fails clearly, at startup, for the one store it concerns.

## Compacting `contracts` and `reorgs`

**What.** Maintenance compacts only partitioned tables. `contracts` and `reorgs` have no
block partition, and a reorg can delete from any of their files, so a compaction there
could conflict with the writer's delete. Both grow by a file per commit that touches
them, which is a discovery or a reorg, so they stay small.

**Option if it matters.** Compact them from the writer itself, between commits, once
their file count passes a bound.

## Not tested against S3

**What.** Every test writes to a local directory. S3's conditional-put commits, the
shared object store's bucket rooting, and credential resolution are exercised only in a
deployment.

**Option.** An opt-in test (ignored by default, like the simulation) against a bucket
named by an environment variable.

## Two Arrow versions

**What.** With every feature on, the build carries Arrow 58 (DuckDB's) and 59
(delta-rs's), and two `reqwest`s. Only an upstream release can align them; CI caches the
build meanwhile.

## Splitting `delta/mod.rs`

**What.** The module is about 900 lines. The repair (`read_ledger`, `repair`,
`repair_reorgs`) and the expression and read-back helpers could move to their own files,
as `sql.rs` sits beside `store.rs`.
