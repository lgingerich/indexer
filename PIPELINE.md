# Ingest pipeline flow

How `ingest::Machine` turns a chain into an ordered stream of envelopes. The code is
`src/ingest/pipeline.rs`; this is the map.

## Entry point

The runtime builds one machine and runs it. There is no other way in.

```
runtime::Pipeline { source, start_block }
  └─ Machine::new(source, sink, ledger).run(start_block)
       ├─ sync(start_block)     startup: resume or start, backfill, fill the tail
       └─ live loop             follow heads until the subscription ends
```

- `ledger` is the accepted blocks a store read back, oldest first. It is empty for a
  fresh run, and always empty for `[sink.stdout]`.
- `start_block` is a fresh run's first height; `None` starts at the head. Giving one
  together with a non-empty ledger is refused.

## Startup: `sync`

```
sync(start)
  ├─ start + ledger? ─────────────────► Err(StartWithHistory)
  ├─ head = sample_head()
  ├─ next =
  │    ├─ ledger:  head < tip? ───────► process_head(head); return   (source is behind the store)
  │    │           else resume(tip) → tip to continue from; next = tip + 1
  │    └─ fresh:   start or head        (start > head? → Err(StartAboveHead))
  ├─ end = buried(head)                  head − 4096, saturating at 0
  ├─ while next <= end:                  ◄── backfill: buried heights, no reorg handling
  │      next = backfill_range(next, end)
  │      at end: head = sample_head(); end = buried(head)
  ├─ tip = ring.tip() or adopt the block at `next`   (a fresh run's first block has no parent)
  └─ catch_up(tip, head)                 ◄── the reorgable tail, fork-aware
```

- **Buried** heights sit a full undo window below the sampled head, so no fork reaches
  them: backfill fetches them in ranges and treats a broken link as a source fault.
- **Re-sampling at the bound.** A long backfill falls behind the chain, so at the bound
  the head is sampled again and backfill continues while more has been buried. The tail
  left for `catch_up` is then at most about a window deep.
- **`resume`** re-reads the stored tip's header. If its hash still matches, the run
  continues from it; if not, the stored suffix was orphaned while no process ran, and
  the fork walk below retracts it before anything new is published.

## Live loop

```
loop {
  select (biased) {
    head notification ──► process_head(head)
    30 s with no head ──► reconcile()  =  process_head(sample_head())
    subscription ends ──► Err(SubscriptionClosed)
  }
  on error:
    UnstableSource | Source(Inconsistent) ──► warn, skip; the next head retries
    anything else ──────────────────────────► stop the run
}
```

`process_head(head)`:

```
tip = ring.tip()                          (none? → Err(NoTip): a pipeline bug)
  ├─ head == tip, or already accepted ──► nothing to do
  ├─ head above tip ────────────────────► catch_up(tip, head)
  └─ head at or below tip ──────────────► re-read header; changed? → UnstableSource
                                          else handle_reorg(head)

catch_up(tip, target):
  for each height after tip up to target:
    fetch block (reusing the notification's metadata at the target height only)
    links to tip? ──► commit, advance
    else ───────────► re-read target header; changed? → UnstableSource
                      else handle_reorg(target)
```

## Forks: `handle_reorg`

```
lowest = target, above = []
walk down by header:
  parent of `lowest` accepted with a matching hash? ──► ancestor found
  `lowest` replaces the first block ever indexed? ────► adopt from there (nothing older is stored)
  parent below the retained window? ──────────────────► Err(UndoWindowExceeded), nothing retracted
  else: read the parent's header, check the link, above.push(lowest), lowest = parent

re-read target header; changed? ──► UnstableSource, nothing retracted
fetch the block at `lowest`           ◄── before the retraction, so a failure can't empty the window
deliver Reorg { orphaned hashes above the ancestor }; rewind the ring
commit `lowest`, then fetch and commit each block in `above`, ascending
```

- Only headers are held while walking, so a deep fork costs a few bytes per height
  rather than a full block.
- A replay failure after the retraction leaves accepted history at exactly what was
  delivered: the retraction and the replacements committed so far. The next head
  continues from there.

## Commit and the undo window

```
commit(meta, events):
  deliver(events + AcceptedBlock(meta))  ──► sink.publish each, then sink.flush
  ring.push(meta)                         only after the sink took it
```

- The `AcceptedBlock` marker is committed in the same store transaction as the block's
  rows, so the ledger a restart reads back never claims a block the store does not hold.
- The ring keeps the newest 4,096 accepted identities plus one evicted *floor*. A fork
  whose ancestor is below it stops rather than guess.

## Errors

| Error | Meaning | Live loop |
|---|---|---|
| `UnstableSource` | The node changed its view while a head was being reconciled | skip |
| `Source(Inconsistent)` | The node's answers disagreed: a reorg mid-fetch, or a lagging backend | skip |
| `Source(Transport)` | A call failed after the transport's own retries | stop |
| `Source(Malformed)`, `Source(Json)`, `IdentityMismatch`, `BrokenLink` | The node returned data that breaks an invariant | stop |
| `UndoWindowExceeded` | A fork deeper than the retained window | stop |
| `Sink(_)` | The sink refused a delivery | stop |
| `NoTip` | No accepted block to reconcile against: a pipeline bug | stop |
| `SubscriptionClosed` | The head subscription ended | stop |

Startup (`sync`) has no skip: any error stops the run, and a restart resumes from the
store's ledger.
