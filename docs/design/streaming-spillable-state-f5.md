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

## Honest scope
Increment 1 (spill `pending_rows`) bounds the STATE-accumulation memory. Increment 2 (bounded-pool
streaming final merge) bounds the OUTPUT/merge memory. Both are needed for full "no OOM at any N".
Distributed (per-partition state) already shards by key via `StreamExchangeExec`, so each worker's
state is 1/N — spill compounds with sharding (Flink Key-Groups analogue). EO unchanged: spilled state
is part of the operator snapshot (`state_io`), already checkpointed per epoch (F3-c) / EndOfData.
