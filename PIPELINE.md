# Ingest pipeline flow

How `ingest::Machine` turns a chain into an ordered stream of envelopes. The code is
`src/ingest/pipeline/`, one file per section below; this is the map. The sources it
drives are `src/ingest/source/evm/` (JSON-RPC, follows the chain) and
`src/ingest/source/hypersync/` (Envio HyperSync, backfill only).

## Entry point

The runtime builds one machine and runs it. There is no other way in.

```
runtime::Pipeline::from_settings(settings) ──► (Pipeline { source, start_block, stop_block, … }, Routes)
  └─ pipeline.run(routes, …)                    opens the stores the routes name, resumes them
       └─ Machine::new(source, sink, ledger).run(start, stop)
            ├─ sync_settled(start, stop)   startup: resume or start, backfill, fill the tail
            │    stop given, or a backfill-only source? ──► return Ok
            └─ live loop                   follow heads until the subscription ends
```

- `Routes` are the sinks the settings name and the tables each writes. They are kept
  apart from `Pipeline` so only a pipeline built from settings can be run that way.
- `ledger` is the accepted blocks the stores read back, oldest first, after
  `sink::resume` brought every store to the newest block they all hold, and whether
  any store holds history below them. It is empty for a fresh run, and always empty
  for `[sink.stdout]`.
- `start` is a fresh run's first height; `None` starts at the head. With a non-empty
  ledger it is ignored when the ledger reaches it, and refused when it is more than
  one block past the ledger's tip.
- `stop` is the last height to index; `None` follows live heads. It must be at or
  below the sampled head and at or above where the run starts. A ledger whose tip is
  already at or past it returns at once.

## Startup: `sync`

`sync_settled` runs `sync`, and runs it again after 2 s when it fails with
`UnstableSource` or `Source(Inconsistent)`: a load-balanced provider's lagging backend
catches up within seconds. Accepted history matches what was delivered, so the next
attempt resumes from the tip. After 30 attempts in a row that accept nothing, the
error stops the run; an attempt that moved the tip starts the count over, so a long
backfill that meets a lagging answer now and then is not stopped by them.

```
sync(start, stop)
  ├─ source follows heads? ── no ──► buried_stop: see "Backfill-only sources" below
  ├─ start > ledger tip + 1? ─────────► Err(StartAboveHistory)   (else the start is ignored)
  ├─ ledger tip >= stop? ─────────────► return                   (nothing to index)
  ├─ head = sample_head()                (stop > head? → Err(StopAboveHead))
  ├─ next =
  │    ├─ ledger:  head < tip? ───────► process_head(head); return   (source is behind the store)
  │    │           else resume(tip) → tip to continue from; next = tip + 1
  │    └─ fresh:   start or head        (start > head? → Err(StartAboveHead);
  │                                      stop < start? → Err(StopBelowStart))
  ├─ end = buried(head)                  head − finality_depth, saturating at 0; capped at stop
  ├─ while next <= end:                  ◄── backfill: buried heights, no reorg handling
  │      fetch ranges ahead, accept them in order
  │      at end: stop reached? done; else head = sample_head(); end = buried(head)
  ├─ tip = ring.tip() or adopt the block at `next`   (a fresh run's first block has no parent)
  └─ catch_up(tip, stop's header or head)            ◄── the reorgable tail, fork-aware
```

- **`finality_depth`** is the source's, from `[ingest] finality_depth` in the chain's
  settings: 1 to 4,096 (`window::MAX_UNFINALIZED_BLOCKS`, the deepest fork a run can
  retract), and 4,096 when omitted.
- **Buried** heights sit `finality_depth` blocks or more below the sampled head, so no
  fork reaches them: backfill fetches them in ranges of `backfill_span` heights, up to
  `backfill_concurrency` ranges in flight, accepts them strictly in height order, and
  treats a broken link as a source fault. A failed range surfaces only after every
  range below it is accepted; the ranges above it are dropped unpublished.
- **Re-sampling at the bound.** A long backfill falls behind the chain, so at the bound
  the head is sampled again and backfill continues while more has been buried. The tail
  left for `catch_up` is then at most about `finality_depth` deep. A bound at the stop
  has nothing to wait for and is not resampled.
- **`resume`** re-reads the stored tip's header. If its hash still matches, the run
  continues from it; if not, the stored suffix was orphaned while no process ran, and
  the fork walk below retracts it before anything new is published.

### Backfill-only sources

A source that does not follow heads (`BlockSource::follows_heads` is false: HyperSync)
has no view of the tip, so a run on it indexes only buried heights and returns. A run
on a source that does follow heads resumes after it.

```
buried_stop(start, stop):
  ├─ fresh and no start? ─────────────► Err(BackfillWithoutStart)
  ├─ head = sample_head()                (stop > head? → Err(StopAboveHead))
  ├─ buried = head − finality_depth
  ├─ fresh: stop < start? ────────────► Err(StopBelowStart)
  │         start > head? ────────────► Err(StartAboveHead)
  │         start > buried? ──────────► Err(StartNotBuried)
  └─ stop = min(stop, buried), fixed once, then sync as with any stop
```

- The stop is not resampled as backfill reaches it: no live phase follows, and on a fast
  chain the head moves on during every pass, so a run that waited for it to stand still
  would never end.
- `resume` on such a source refuses a stored tip whose hash differs with
  `IdentityMismatch`: it cannot walk the fork that would retract it.

## Live loop

```
loop {
  select (biased) {
    head notification ──► observe it; process_head(head)
    retry wait over ────► process_head(the head to ask about again)
    30 s with no head ──► reconcile()  =  process_head(sample_head())
    subscription ends ──► Err(SubscriptionClosed)
  }
  on error:
    UnstableSource | Source(Inconsistent) ──► ask about that head again soon
    anything else ──────────────────────────► stop the run
  on success: forget the pending retry; the head reached covers it
}
```

- **Asking again.** A head the source answered inconsistently is retried after 100 ms,
  without waiting for the next head or the reconcile timer: a backend behind the one
  that announced it catches up in well under a second, and on a slow chain the next
  head is seconds away. Each retry of the same head doubles the wait, up to 2 s; a newly
  announced head starts again at 100 ms. A head arriving first covers the retry.

`process_head(head)`:

```
tip = ring.tip()                          (none? → Err(NoTip): a pipeline bug)
  ├─ head == tip, or already accepted ──► nothing to do
  ├─ head above tip ────────────────────► catch_up(tip, head)
  ├─ head below the window, and an ─────► nothing to do        (the source is behind, not forked)
  │  ancestor of its oldest block?
  └─ head at or below tip ──────────────► re-read header; changed? → UnstableSource
                                          else handle_reorg(head, head)

catch_up(tip, target):
  fetch the heights after tip through target ahead, up to backfill_concurrency in flight
  (reusing the notification's metadata at the target height only), and in height order:
    links to tip? ──► commit, advance
    else ───────────► drop the fetches ahead; re-read target header; changed? → UnstableSource
                      else handle_reorg(target, the block that did not link)
```

- **`is_ancestor_below_window`** walks headers from just under the oldest remembered
  block down to the head, at most 4,096 of them. If each is the parent the one above
  names and the last is the head, the source is only behind the store, as a
  load-balanced provider's often is. Without it such a head went to the fork walk, found
  no remembered ancestor, and stopped the run with `UndoWindowExceeded`.
- **Fetching ahead.** Backfill, `catch_up`, and the replay after a fork share one
  prefetcher (`prefetch.rs`): each request is its own task, answers are taken in height
  order, and dropping the prefetcher aborts what is still in flight.

## Forks: `handle_reorg`

```
handle_reorg(target, start):              start = target, or catch_up's first block that did not link
lowest = start, above = []
walk down by header:
  parent of `lowest` accepted with a matching hash? ──► ancestor found
  `lowest` replaces the first block ever indexed? ────► adopt from there (nothing older is stored)
  parent below the retained window? ──────────────────► Err(UndoWindowExceeded), nothing retracted
  else: read the parent's header, check the link, above.push(lowest), lowest = parent

re-read target header, and start's; changed? ──► UnstableSource, nothing retracted
fetch the block at `lowest`           ◄── before the retraction, so a failure can't empty the window
deliver Reorg { orphaned hashes above the ancestor }; rewind the ring
commit `lowest`, then fetch ahead and commit each block up to the target, ascending:
  each walked height must return the walked block, each one above must link, and the
  target must be the target; anything else is UnstableSource
```

- Only headers are held while walking, so a deep fork costs a few bytes per height
  rather than a full block.
- During catch-up the walk starts where the chain diverged, not at the target: every
  block above the first that did not link is unaccepted, and walking down to it from the
  target read each of their headers one at a time for nothing.
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
- The ring keeps the newest 4,096 (`window::MAX_UNFINALIZED_BLOCKS`) accepted
  identities plus one evicted *floor*. A fork whose ancestor is below it stops rather
  than guess. A restored ledger says whether the stores hold history below it, which
  decides whether a fork replacing its oldest block is adopted (nothing below) or stops
  the run.

## Errors

| Error | Meaning | Startup | Live loop |
|---|---|---|---|
| `UnstableSource` | The node changed its view while a head was being reconciled | retry | ask again |
| `Source(Inconsistent)` | The node's answers disagreed: a reorg mid-fetch, or a lagging backend | retry | ask again |
| `Source(Http)`, `Source(Transport)`, `Source(Status)` | A call failed after the transport's own paced retries, or was refused | stop | stop |
| `Source(Malformed)`, `Source(Json)` | The node returned data that breaks a source invariant | stop | stop |
| `WrongHeight`, `IdentityMismatch`, `BrokenLink`, `InvalidRange` | The node returned a block at the wrong height, with the wrong identity, not linking, or the wrong number for a range | stop | stop |
| `UndoWindowExceeded` | A fork deeper than the retained window | stop | stop |
| `StartAboveHistory`, `StartAboveHead`, `StartNotBuried`, `BackfillWithoutStart` | `start_block` does not fit the store, the head, or a backfill-only source | stop | — |
| `StopAboveHead`, `StopBelowStart` | `stop_block` is outside the run's bounds | stop | — |
| `Sink(_)` | The sink refused a delivery | stop | stop |
| `HeightOverflow` | The next height would overflow `u64` | stop | stop |
| `NoTip` | No accepted block to reconcile against: a pipeline bug | stop | stop |
| `SubscriptionClosed` | The head subscription ended | — | stop |

A stop leaves the store at what was delivered, and a restart resumes from its ledger.
