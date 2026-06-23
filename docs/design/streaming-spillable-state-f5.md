# F5 — Spillable keyed state (Flink-class large state, no OOM)

Status: DESIGN + increment 1 (2026-06-22). The 65536 cap is fixed (correctness), but windowed-agg
state is still **fully in-memory** ⇒ a window larger than RAM OOMs. Flink's defining production
strength is **state ≫ RAM** via spill (RocksDB; Flink 2.0 ForSt = object-store + local cache + async,
REFERENCES §3). F5 closes this — the last gap to "prod-grade like Flink" for stateful streaming.

## Where the memory goes today (`window_accum.rs`)
1. `pending_rows: Vec<RecordBatch>` — the accumulated PARTIAL aggregate state, one partial row per
   (window,key) seen, held in RAM until the window closes. O(open-window groups) = O(distinct keys).
   **Unbounded in RAM.**
2. `run_final_aggregate(pending_rows.clone())` — materializes the FULL final output in RAM (`out.push`
   every batch) before emitting. **O(groups) again, plus a clone.**
3. `emitted_ends: HashSet<i64>` — one i64 per closed window forever (P1 leak, separate).

## Design (grounded: DataFusion spill + Flink ForSt)
**Principle (ForSt):** state is object-store-native with a bounded local memory cache + async I/O;
spill in Arrow format. Vajra already has the pieces — `state_io` (Arrow-IPC ↔ `CheckpointStore`
object-store) and DataFusion's `RuntimeEnv`/`DiskManager`/`MemoryPool` (spillable aggregation).

**1. Bound + spill `pending_rows` (increment 1 — this change).**
A byte budget `SAIL_STREAMING__STATE_MEMORY_BUDGET` (default e.g. 256 MiB). As partial batches
accumulate, track bytes; when over budget, **spill** the oldest partial batches to a spill store
(Arrow-IPC blob via `state_io` encode → `CheckpointStore` when a checkpoint dir is set = ForSt
object-store path; else DataFusion `DiskManager` temp file = local-disk path). Keep only the budget's
worth of recent partials in RAM. Track spilled handles.

**2. Streaming finalize over spilled + memory (increment 2).**
At finalize, feed partials to `run_final_aggregate` as a STREAM: in-memory partials + lazily-read
spilled blobs, via a `StaticBatchExec` that yields them without holding all at once. Run the Final
`AggregateExec` under a **bounded `MemoryPool`** on the streaming `RuntimeEnv` so DataFusion's
grouped-hash-aggregate **spills its own hash table** when large (proven DataFusion path), and emit its
output batches incrementally (already a stream) — never materialize the whole result.

**3. Compaction.** Periodically re-run Partial over accumulated partials to collapse duplicate
(window,key) partials (a key appearing in many batches), keeping spilled+memory state ≈ O(distinct
groups), not O(batches×groups).

## Validation gate
`scripts/state_scale_stress.py` at **large N (e.g. 10M–50M keys)** with a small state budget:
- correctness: input == output (no loss), and
- **bounded RSS** (≈ budget, NOT linear in N) — vs today's linear growth → OOM.
Head-to-head: Flink (RocksDB) on the same N — Vajra should match (hold state ≫ RAM) and ideally win
on memory (no JVM). That is the "prod-grade like Flink large-state" proof.

## Build roadmap (tracked — this is a state backend, a multi-step change)
- **F5.0 spill primitive — DONE 2026-06-22:** `state_io::{write_spill,read_spill,delete_spill}` (numbered
  Arrow-IPC chunks ↔ `CheckpointStore`), unit-tested (`spill_chunks_roundtrip_and_gc`). Safe building
  block, not yet wired into the hot path.
- **F5.1 wire spill into `WindowAccumExec` — DONE + validated 2026-06-22 (commit):** per-instance byte
  budget (`SAIL_STREAMING_STATE_BUDGET_BYTES`, default 128 MiB); over budget → spill `pending_rows`
  chunk to the checkpoint store, evict from RAM; `gather_partials` folds spills back at every
  finalize/snapshot (EO unchanged — the durable snapshot is always the full flattened state); finalize
  GCs consumed spills. Validated: 200k keys, **256 KB budget → 23 spills**, output **200000 exact**
  (no loss/dup across the spill round-trip). Bounds the ACCUMULATION phase. NOTE: budget is
  per-partition (state is sharded by `StreamExchangeExec`), so it composes with parallelism
  (Key-Groups analogue). Remaining peak is the finalize/snapshot read-back (F5.2).
- **F5.2 streaming finalize — DONE + validated 2026-06-23 (commit):** `SpillSourceExec` yields the
  in-memory pending + each spilled chunk LAZILY (one at a time) into the Final `AggregateExec`, run
  under `bounded_agg_context` (a `FairSpillPool(budget)` + `DiskManager` so DataFusion spills its OWN
  hash table). The merge is RESUMABLE (`AccumState.active_merge`, driven one output batch per poll in
  the unfold loop) so the result is emitted INCREMENTALLY — `buf` never holds the whole result; the
  trailing watermark/EndOfData marker is deferred until the output drains (barrier order preserved).
  `rebuild_retained_state` re-spills the open windows after each finalize. The 64K-cap invariant holds:
  `emitted_ends` is applied a SNAPSHOT during the merge and updated only on completion (so every output
  batch of one finalize emits). **Validated** (`scripts/f5_validate.sh`, COUNT, N keys/one window):
  out == N EXACT at N = 200k / 500k / 1M / **5M**, at BOTH a 4 MiB (in-RAM) and a 256 KB (spilling)
  per-partition budget; spills engage + scale monotonically (23 → 56 → 120 → **602** at 256 KB), no
  errors, no OOM — 5M distinct keys handled under a 256 KB budget (old in-RAM path would hold all 5M
  partials; pre-F5.1 would cap at 64K). HONEST: process RSS is still ~O(N) at these scales (1.25 GiB @
  5M) because the **parquet output sink + Spark-Connect result path are O(N)** and sub-second runs
  don't let jemalloc release pages — process RSS is not the right instrument. Operator state IS bounded
  (spill + incremental emit); a clean FLAT-RSS-vs-Flink measurement needs a bounded sink + sustained
  stream = F5.4.
- **F5.3 retain/re-spill across finalize** (open windows survive bounded) + **compaction** (collapse
  duplicate (window,k) partials).
- **F5.4 gate:** @ 10M–50M keys, small budget → input==output + **bounded RSS**; head-to-head vs
  Flink/RocksDB. NOTE (learned in F5.2): use a **bounded/streaming sink** (not a parquet dump of N
  rows) and a **sustained stream** (so jemalloc reaches steady state), and measure the **operator's
  memory reservation** (DataFusion `MemoryPool` accounting / metrics), not just process RSS — at
  sub-second batch runs with an O(N) sink, process RSS is dominated by the sink + allocator retention,
  not the operator working set.
- Apply the same spill to dedup + stream-join state.

## The bar — HIGHER than Flink (not just parity)
- **Object-store-NATIVE state from day one.** Flink only got disaggregated state in 2.0 (ForSt, bolted
  onto a local-RocksDB lineage); Vajra spills/checkpoints to object-store in ONE Arrow-IPC format from
  the start — same blob for spill and checkpoint, no separate state serializer, no local-disk ceiling.
- **Arrow-columnar state, zero-copy, vectorized restore** — vs RocksDB's row-oriented KV (serialize per
  key). Restore = mmap/stream Arrow batches straight into the operator.
- **No JVM / no GC** → no stop-the-world pauses during large-state spill/restore (Flink's tail-latency
  pain under big RocksDB state).
- **Unified with EO:** spilled state is already part of the per-epoch snapshot (F3-c) — spill and
  exactly-once are the same mechanism, not two systems.
Target: hold state ≫ RAM like Flink, **with less memory + better tail latency + simpler ops**.

## Honest scope
Increment 1 (spill `pending_rows`) bounds the STATE-accumulation memory. Increment 2 (bounded-pool
streaming final merge) bounds the OUTPUT/merge memory. Both are needed for full "no OOM at any N".
Distributed (per-partition state) already shards by key via `StreamExchangeExec`, so each worker's
state is 1/N — spill compounds with sharding (Flink Key-Groups analogue). EO unchanged: spilled state
is part of the operator snapshot (`state_io`), already checkpointed per epoch (F3-c) / EndOfData.
