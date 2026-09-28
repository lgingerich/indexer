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
- **One event per thing.** A `block` marker, then for each transaction a
  `transaction` event (raw transaction plus raw receipt) followed by its `log`
  events. Each field is published exactly once: the block's `raw` omits its
  `transactions` array and a receipt omits its `logs`, since those are their own
  events.
- **Ordered events.** Every event gets a per-chain monotonic `sequence` and a
  stable `dedupe_key`. See `src/envelope.rs`.
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
{"chain":"base","sequence":18174,"schema_version":3,
 "type":"log","height":51883702,"block_hash":"0x…","tx_id":"0x…","tx_index":3,
 "item_index":0,"raw":"{\"address\":\"0x…\",\"topics\":[\"0x…\"],\"data\":\"0x…\"}"}
```

`raw` (and `receipt` on transactions) is the chain's payload as a JSON-encoded
string, so a consumer decodes with whatever ABI it trusts. Attaching `schema_version` means a shape change is detectable rather than
silent. Consumers deduplicate on `dedupe_key`, not `sequence`: a sequence can be
reused after a reorg or a restart.

Block and transaction identity use `alloy_primitives::B256` and `TxHash`, encoded as
lowercase `0x` hex. Two tests pin that as a contract, since it is what consumers
parse: `b256_wire_format_is_lowercase_0x_hex` in `src/envelope.rs`. A chain whose
identity does not fit that shape — Solana's base58 blockhash and 64-byte signature
are the expected case — gets its own type plus a tagged union at the envelope
boundary, rather than a shared type widened to fit both.

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
| 100 txs × 2 logs | 275 KiB | 301 | 652µs | 824µs | 370µs |
| 500 txs × 4 logs | 1.8 MiB | 2,501 | 4.67ms | 5.86ms | 2.74ms |
| 2,000 txs × 5 logs | 8.3 MiB | 12,001 | 21.4ms | 25.0ms | 12.4ms |

Decode is linear in response size at roughly 2.5µs per KiB. Payloads are never
parsed, only scanned and copied, so the cost is mostly memory traffic.

Against the budget: an average Base block in a live run had about 260
transactions and 880 logs, which falls between the first two rows, so decode
plus serialise is a few milliseconds of CPU per block. The larger cost is the
network: the indexer publishes about 1.7 MB per Base block, roughly 0.8 MB/s.

## Checks

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps --all-features
cargo test
```
