# indexer

A low-latency, chain-agnostic blockchain indexer.

It reads a chain's live tip, parses each block into ordered events, and publishes
them as newline-delimited JSON. Chain-specific knowledge sits behind one trait and
egress behind another, so new chains and new brokers are additions rather than
rewrites.

## What works today

- **EVM ingestion.** Live heads over WebSocket (`eth_subscribe`/`newHeads`), and
  each block fetched over JSON-RPC with full transactions, receipts, and logs in
  one batched request. Nothing the node returns is dropped. See
  `src/source/evm.rs`.
- **One event per dataset.** A `block` event, then for each transaction a
  `transaction` event, its `receipt` event, and its `log` events. Each dataset is a
  normalized table — a block references its transactions by hash, a receipt carries
  only the count of its logs — so no field is published twice and a row maps to a
  persistence row. See `src/datasets/evm.rs`.
- **Ordered events.** Every event gets a per-chain monotonic `sequence`; every
  dataset exposes a `dedupe_key` derived from its natural key. See `src/envelope.rs`
  and `src/datasets/evm.rs`.
- **Finality watermark.** A `finalized` event says a block and everything below
  it are permanent. Its height comes from the node's own `finalized` tag, so each
  chain's rules apply with no confirmation count to tune; on Base it trails the
  tip by about 600 blocks. It is published only when it advances.
- **Reorg retraction.** Parent-hash linkage is checked on every block. A mismatch
  publishes a `reorg` event whose `orphaned_hashes` say what was retracted, and
  reclaims the sequence numbers those blocks had used. The undo window is bounded
  (128 blocks by default, and finalized blocks are dropped first). See
  `src/pipeline.rs`.
- **NDJSON to stdout.** See `src/sink/mod.rs`.

## Not built yet

Backfill-to-live handoff, checkpoint resume, mempool, ABI/IDL decoding, filtered
subscriptions, derived state (balances/nonces), the Redpanda sink, and the
Parquet/GCS archiver. The crate is a walking skeleton: it indexes forward from
whatever the chain does next and does not fill gaps that predate startup.

## Run it

```bash
EVM_CHAIN=base \
EVM_HTTP_URL=https://base-rpc.publicnode.com \
EVM_WS_URL=wss://base-rpc.publicnode.com \
RUST_LOG=info \
cargo run --release > events.ndjson
```

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `EVM_CHAIN` | yes | — | Chain id stamped on every event |
| `EVM_HTTP_URL` | yes | — | JSON-RPC endpoint for blocks, receipts, and the finalized block |
| `EVM_WS_URL` | yes | — | WebSocket endpoint for `newHeads` |
| `RUST_LOG` | no | `info` | Log filter |

Logs go to stderr; events go to stdout, so the two streams never interleave.

## Event shape

```json
{"chain":"base","sequence":18174,
 "type":"log","log_index":0,"transaction_hash":"0x…","transaction_index":3,
 "address":"0x…","topic0":"0x…","topic1":null,"topic2":null,"topic3":null,
 "data":"0x…","removed":false,"block_number":51883702,"block_hash":"0x…"}
```

Events fall into two kinds. **Datasets** — `block`, `transaction`, `receipt`, `log`
— are durable on-chain records, defined per chain in `src/datasets/evm.rs`. Each
is a normalized table with a natural key and fully deconstructed fields, the same
decomposition of the chain's datasets, so a row maps straight to a
persistence row; children are referenced by scalar key, never embedded. **Control
signals** — `reorg`, `finalized` — drive a consumer's state machine and carry no
payload; `Event::is_dataset` tells them apart. The line is one flat object: `chain`,
`sequence`, and the event's fields under its `type` tag. Consumers deduplicate on
each event's `dedupe_key`, not `sequence`: a sequence can be reused after a reorg or
a restart, and the key is scoped to the stream. Each dataset's key comes from its
natural key, so a transaction and its receipt (both keyed by the transaction hash)
stay distinct.

Identity uses `alloy_primitives::{B256, BlockHash, TxHash}`, encoded as lowercase
`0x` hex, and quantity fields encode as `0x` hex via `alloy-serde`. Two tests pin
the hash format as a contract, since it is what consumers parse:
`b256_wire_format_is_lowercase_0x_hex` in `src/envelope.rs`. The line the sink
writes is pinned by `render_keeps_the_envelope_fields_flat_on_one_line` in
`src/sink/mod.rs`. A chain whose
identity does not fit that shape — Solana's base58 blockhash and 64-byte signature
are the expected case — gets its own module beside `src/datasets/evm.rs` plus a
variant in `Event`, rather than a shared type widened to fit both.

## Benchmarks

```bash
cargo bench
```

`benches/hot_path.rs` measures the CPU work between socket and sink — decoding
the node's batch response and serialising envelopes — since end-to-end latency is dominated by the
network and is not reproducible off-line. It is hand-rolled and dependency-free so
it can report percentiles rather than means, and it calls `std::hint::black_box`
explicitly because that is the only way to stop `lto` and `codegen-units = 1` from
deleting the work being measured.

Measured on an Apple Silicon laptop, release profile, ~1,000 samples each:

| Workload | response | events | decode p50 | decode p99 | serialise p50 |
| --- | --- | --- | --- | --- | --- |
| 100 txs × 2 logs | 281 KiB | 401 | 725µs | 921µs | 572µs |
| 500 txs × 4 logs | 1.9 MiB | 3,001 | 4.76ms | 5.48ms | 4.71ms |
| 2,000 txs × 5 logs | 8.6 MiB | 14,001 | 21.6ms | 25.3ms | 23.0ms |

Decode is linear in response size at roughly 2.5µs per KiB. Payloads are still
never *decoded* into ABI values — the node's JSON is deserialized into alloy's RPC
types and then projected field by field — so the cost remains mostly memory traffic
and per-event allocation.

Against the budget: an average Base block in a live run had about 260
transactions and 880 logs, which falls between the first two rows, so decode
plus serialise is a few milliseconds of CPU per block. The larger cost is the
network: the indexer publishes several MB per Base block, since every dataset is
now published with its fields spelled out rather than as one opaque `raw` string.

## Checks

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps --all-features
cargo test
```
