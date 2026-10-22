# Derived tables, computed in the indexer

Status: per-event derivations are built (`src/decode/derive`): Metric's `metric_v1_swaps`,
`metric_v1_bin_fills`, `metric_v1_liquidity`, and `metric_v1_pool_bins`. State and
aggregate tables are designed below under [Later](#later) and not built.

## Why in the indexer

App data goes straight to Postgres and must be there within one second of the indexer
seeing the block, decoded and computed. Delta Lake is the slow historical layer, which
commits about every 30 seconds, so nothing on the app path reads from it.

Derive runs as the last step of decoding a record, and writes in the same transaction as
the block's decoded events. A block's derived rows land with the block, and a reader never
sees its events without them. Feldera and RisingWave were considered and set aside for this path:
either would put a service and a network hop between ingest and Postgres, and Feldera's
open-source edition recomputes from scratch on every restart.

## What is built

```
ingest ─▶ DecodingSink (decode, then derive) ─▶ Router ─▶ stores
```

A `Derivation` reads one or more events of one protocol's contract and writes one table:
rows per decoded record, computed from that record alone. Most derive one row per
record; one that unpacks arrays (liquidity per bin, a pool's bin ladder) derives one per
entry, and may derive none.

```rust
pub struct Derivation {
    pub protocol: &'static str,            // "metric_v1"
    pub contract: &'static str,            // "MetricOmmPool"
    pub signatures: &'static [&'static str], // ["Swap(address,address,uint256,uint256,uint256)"]
    pub table: &'static str,               // "metric_v1_swaps"
    pub columns: &'static [(&'static str, ColumnType)],
    pub indexes: &'static [&'static [&'static str]], // [["chain", "address", "block_number", "log_index"]]
    pub derive: fn(&Decoded, usize) -> Result<Vec<Row>, DeriveError>, // rows, given which signature
}

type Row = Vec<(&'static str, TypedValue)>; // [("amount0_raw", value), ...]
```

A row names each value's column beside it, so two columns of one type (`delta0` and
`delta1`, `lower` and `upper`) cannot trade values unseen: a row must name the table's
`columns` in order.

- **No state.** `derive` is a plain function of one record. A derived row carries its
  log's block hash, so a `reorg` deletes it with the block's other rows, a rewind deletes
  it by height, and a restart has nothing to restore. Any store can hold it, the lake
  included.
- **The transaction.** Every derived row carries its transaction's sender and recipient,
  `transaction_from_address` and `transaction_to_address`, as Allium's `dex.trades` does:
  a swap's trader is the transaction's sender, not the event's `sender`, which is the
  router. Ingest publishes each transaction just before its logs, so the decode sink
  remembers the one in flight. Listing a derived table makes ingest fetch transactions,
  a block's body and receipts in place of one `eth_getLogs`, whether or not a store
  writes them; route `transactions` to the lake to keep them too. Both columns are
  nullable: the recipient is null for a contract deployment, and both would be for a
  log whose transaction was not fetched, which listing the table prevents.
- **The table** is the log's columns, the transaction's two, the derivation's own, then
  the block's, keyed on
  `(chain, dedupe_key)` with `dedupe_key = block_hash:transaction_hash:log_index:table:row`,
  `row` numbering a record's rows from 0 in the order the derivation computes them.
  It is declared at startup beside the event tables (`Schema::add_derived`), and a store
  lists it in `tables` like any other. `contract_events` does not include it, but a
  glob such as `metric_v1_*` or `*` does.
- **On the wire** a derived row is `Event::Derived`: the table, the log's identity, and
  its values as named `TypedValue`s. It was added without bumping `schema_version`;
  the policy in `wire::envelope` now counts a new variant as a bump, since a strict
  reader rejects an unknown `type`.
- **When it runs.** A derivation runs when the catalog declares every one of its events'
  exact signatures, and its rows go to the stores that list its table, as decoded rows do. An
  ABI that retyped an argument changes the signature, which leaves the table undeclared,
  so a store that lists it fails at startup. Arguments are read by position and name, so
  one that moved under an unchanged signature (two `uint256`s swapped) is caught at its
  first record rather than misread.
- **When a record cannot be read** (an argument missing, misnamed, or of another type),
  the derivation logs a warning and adds nothing, as a log that fails to decode keeps
  only its raw record. A row that does not fit its table is a bug in the derivation and
  stops the run: one that does not name its table's columns in order (a value too many,
  too few, or two swapped) is refused as it is derived, and
  one whose value does not fit its column, checked on the value rather than its width
  label, is refused by the store.
- **History.** A derivation covers blocks ingested while it runs. Blocks stored before
  it was enabled are not backfilled; see the README's stored-log replay.
- **Schema changes.** Changing a derived table's columns makes the store's existing table
  drift, which is a startup error, as for any table.

### `metric_v1_swaps`

One row per Metric pool `Swap`, with its three packed words unpacked as the pool's
`types/Packed.sol` lays them out. This matches the `metric.swap` view in `metric-tools`
column for column, so the two can be diffed directly.

| Column | Source |
|---|---|
| `sender`, `recipient` | the event's indexed addresses |
| `amount0_raw`, `amount1_raw` | `amountDeltas`, signed halves: token0 high, token1 low. Native units, positive when the pool received the token |
| `zero_for_one` | `amount0_raw > 0`: the pool received token0, so selling token0 for token1 |
| `exact_input` | `details` bit 120 |
| `bin_idx_after` | `details` bits 104–119, signed |
| `pos_in_bin_after` | `details` bits 0–103 |
| `platform_spread_fee_scaled`, `platform_notional_fee_scaled` | `platformFees`: spread high, notional low, in the pool's scaled units on the input token |

### `metric_v1_bin_fills`

One row per `BinSwapped`: one bin a swap filled, emitted before that swap's `Swap` log.
This matches the `metric.bin_fill` view in `metric-tools`, less its `swap_log_index`.
Linking a fill to its swap means holding fills until the `Swap` arrives, which is state.
SQL can still join them, since a fill belongs to the next `Swap` of the same pool in the
same transaction.

| Column | Source |
|---|---|
| `bin_idx` | `binIdx`, an `int256` the pool keeps as `int16`; a value outside that range is unreadable |
| `delta0_scaled`, `delta1_scaled` | `binDeltas`, signed halves: token0 high, token1 low. The bin's own balance change, positive when it gained the token; the input side includes the LP fee |
| `lp_fee_scaled`, `platform_fee_scaled` | `fees`: LP high (stays in the bin), platform low (never enters it) |

### `metric_v1_liquidity`

One row per bin of a `LiquidityAdded` or `LiquidityRemoved`, from their parallel arrays.
This matches the `metric.liquidity` view in `metric-tools`. A position is
`(pool, owner, salt)`; its shares are per bin and cannot be transferred.

| Column | Source |
|---|---|
| `direction` | `add` or `remove`, by the event |
| `owner`, `salt` | the indexed position key: `provider` and the `uint80` salt |
| `bin_idx` | `binIdxs[i]`, an `int256` the pool keeps as `int16` |
| `delta0_scaled`, `delta1_scaled` | `binBalanceDeltas[i]`, signed as emitted: negative when tokens leave the bin |
| `shares` | `shares[i]`, positive on an add and negative on a remove |

Arrays of different lengths, or a bin index outside `int16`, make the record unreadable.

### `metric_v1_pool_bins`

One row per bin of a pool's ladder as created, from the factory's `PoolCreated`, lowest
bin first. This matches `metric.pool_bin_initial` and replaces the `metric.unpack_bins`
SQL. The log is the factory's, so the pool is its own column.

| Column | Source |
|---|---|
| `pool` | `poolAddress` |
| `bin_idx` | entry `k` of `nonNegativeBinDataArray` is bin `k`; of `negativeBinDataArray`, bin `-k-1` |
| `length_e6`, `add_fee_buy_e6`, `add_fee_sell_e6` | one 48-bit bin, five to a word, least significant first: bits 0–15, 16–31, 32–47 |
| `lower_e6`, `upper_e6` | distances from the reference price: bin 0's lower edge is `curBinDistFromProvidedPriceE6`, and each bin's edges run a length on from its neighbour's |

The unpacking follows the factory's `_unpackAndValidateBinStates`: the first zero length
ends a side, dropping the rest of its word and any later words. The additional fees are
as created; `BinAdditionalFeesUpdated` changes them later, which is state.

## Removing it

Derivations stay in the indexer, as in-process Rust, for the reasons under
[Why in the indexer](#why-in-the-indexer). Should that ever change, the module doc of
`src/decode/derive` lists what to delete: the directory, its hooks in `decode`, and the
generic support for derived rows in `wire` and `sink`.

## Adding a derivation

1. Add a `Derivation` constant in `src/decode/derive/<protocol>/<table>.rs`. Pin the
   event's canonical signature, and read arguments by position and name, and packed
   words by bit field, with the helpers in `src/decode/derive/value.rs`.
2. List it in `DERIVATIONS`.
3. Test the unpacking against the contract's own vectors, and the row against the
   shipped catalog, with events built in `src/decode/fixtures.rs`.

## Later

None of this is built. It is the design for derived tables that carry state across
records, kept here so the per-event layer grows into it rather than around it.

**Three kinds of table:**

| Kind | Example | Key | Reorg handling | May route to |
|---|---|---|---|---|
| Event (built) | `metric_v1_swaps` | the log, plus the table | the store's delete by `block_hash` | any store |
| State | `metric_v1_pool_bins` | natural key, no block hash: `(pool, bin_idx)` | an undo journal | one SQL store |
| Aggregate | `metric_v1_pool_hourly` | `(pool, hour)` | an undo journal | one SQL store |

**What state and aggregate tables need:**

- **Rows hold absolute values, never increments,** so writing a row twice is harmless.
- **A keyed delete.** `Batch` gains tombstones and `SqlStore` deletes by key, because
  a row that belongs to no block cannot be deleted by block.
- **An undo journal of previous values.** Each write records `(table, key, old value)`
  for the current block, kept for the last `MAX_UNFINALIZED_BLOCKS` blocks. A `reorg`
  restores them newest first. That is correct for any logic, including step-by-step
  state such as a pool's current bin.
- **Persistence.** The journal is persisted as a block-scoped `derive_journal` table in
  the block's transaction. A restart loads state from the store. `SqlStore::rewind`
  applies the journal above the rewind height before its usual deletes, so rewind and
  reorg share one undo.
- **Rebuilds.** A `derivations` table records each derivation's version. A missing row
  or a changed version truncates the derivation's tables and replays its inputs from the
  home store's event tables before ingest resumes.
- **Routing.** Routing a state or aggregate table to Delta, which only appends, is a
  startup error.

**Metric's next tables:**

- **State:** pools with their config, bins with balances and total shares, positions and
  their per-bin shares, and extensions.
- **Event:** bin fills linked to their swap, liquidity rows per bin, reserves after each
  change, and fee collections.
- **Aggregate:** pool by hour in token units.
- **Stays in SQL:** USD, token metadata, and labels remain views in `metric-tools`, which
  then read these tables instead of replaying history.

**Things the contract sources show that this design has to handle:**

- **The oracle's quote is not in any event.** Recovering it from the largest fill needs
  bin geometry, so it is a state-table job.
- **A silent reset.** When a remove empties the current bin, the pool resets
  `curPosInBin` without an event (`MetricOmmPool.sol:226`). Today's SQL misses this.
- **The protocol/admin split of the spread fee** is settled at `collectFees`.
  Per-swap splits are an attribution; `ProtocolFeesCollected` is the ledger.
