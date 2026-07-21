# Large-state backend (P0) — spillable + disaggregated, designed to beat Flink

> The streaming latency + EO + throughput + memory gaps vs Flink are now closed/won
> (`STREAMING_VS_FLINK_EKS.md`). The remaining operational gap is **large keyed state**:
> Zelox keeps streaming state **in memory** and snapshots it **whole** to the checkpoint.
> Flink handles huge state via RocksDB + incremental checkpoints + (2.0) disaggregated
> state (ForSt). This designs a Zelox state backend that **matches and then beats** Flink —
> not by porting RocksDB, but by exploiting Zelox's no-JVM, Arrow-columnar, object-store-first
> architecture.

## Where Zelox is today
- `WindowAccumExec` keeps partial-aggregate state in `AccumState.pending_rows: Vec<RecordBatch>`
  (in memory), pruned to open windows, and **full-snapshotted** to the object-store checkpoint
  on epoch/EndOfData (`state_io::stage_state`). `StreamJoinExec` / dedup hold state in memory too.
- Consequence: state must fit RAM (OOM risk at large keyspaces), and every checkpoint uploads
  the **entire** state (not the delta).

## How the proven systems do it (and what we take, not copy)
- **Flink HashMapStateBackend** (heap) — fast, but JVM-GC-bound and RAM-limited.
- **Flink RocksDBStateBackend** — embedded LSM on local disk → state ≫ RAM; **incremental
  checkpoints** upload only new/changed SST files; JNI + JVM-off-heap overhead.
- **Flink changelog state backend (FLIP-158)** — decouples a durability *log* from periodic
  materialization → faster, more predictable checkpoints.
- **Flink 2.0 disaggregated state (ForSt)** — state lives on DFS/object store, compute reads via
  a local cache → cloud elasticity, fast rescaling, cheap checkpoints (state already remote).
- **DataFusion** — `GroupedHashAggregateStream` + `MemoryPool` spill: when a memory budget is
  exceeded, spill to disk and merge — the model for *bounded-memory* operators.

## Zelox design — `StateBackend`: Arrow-columnar, spillable, disaggregated-native
A pluggable trait, Arrow-first (state is RecordBatches, not opaque bytes), used by
`WindowAccumExec` / `StreamJoinExec` / dedup instead of in-memory `Vec`s:

```text
trait StateBackend {
    // keyed columnar state: key-group -> the operator's partial-state batches
    fn get(&self, key_group: u32) -> Result<Option<StateChunk>>;     // hot-cache or load
    fn put(&mut self, key_group: u32, chunk: StateChunk);            // buffered, spillable
    fn scan(&self) -> impl Iterator<Item=(u32, StateChunk)>;          // for Final merge / emit
    fn evict(&mut self, predicate);                                   // drop closed windows (TTL)
    fn checkpoint(&mut self, epoch) -> Result<CheckpointHandle>;      // INCREMENTAL upload
    fn restore(&mut self, handle: CheckpointHandle) -> Result<()>;    // load on recovery
}
```

**Three layers, each a deliberate improvement over Flink:**

1. **Bounded memory + spill (matches RocksDB, no JNI/GC).** State chunks live in an in-memory
   Arrow map under a `MemoryPool` budget; when exceeded, **spill** the coldest key-groups to a
   local on-disk store (embedded KV such as `redb`/`rocksdb`-rust, or Arrow IPC files keyed by
   key-group). Pure Rust, no JVM heap, no GC pauses → flatter tail latency than Flink at large
   state. Reuses DataFusion's `MemoryPool` so it composes with the rest of the pipeline.

2. **Incremental + disaggregated checkpoints (matches ForSt, native).** State is keyed by
   **key-group** and content-addressed; `checkpoint(epoch)` uploads only key-groups **changed
   since the last epoch** to the object store (delta), writing one atomic manifest
   (`state/<op>/<epoch>/manifest`) — reusing Zelox's existing single-atomic-object commit (the
   same primitive the realtime EO sink uses). Because Zelox is **object-store-first already**,
   "disaggregated state" is the natural representation, not a bolt-on: the object store *is* the
   state of record; the local store is a write-back **cache**. This is the ForSt idea, native.

3. **Rescaling by key-group.** State partitioned into a fixed large number of key-groups
   (Flink's approach); on parallelism change, key-groups are reassigned to instances and each
   loads its groups from the object store — clean elastic rescaling (pairs with the parallel
   Kafka source's `% N` assignment).

## Why this beats Flink (the honest thesis)
- **No JVM** → no GC pauses on large state (Flink's large-RocksDB tail-latency pain) and no
  off-heap/JNI tax.
- **Arrow-columnar state** → vectorized merge/scan, zero-copy to the agg operators (Flink
  serializes per-key rows).
- **Object-store-first** → checkpoints are cheap (state already remote, only the delta), and
  elasticity is free — Flink retrofitted this in 2.0 (ForSt); Zelox is built that way.

## Build order (each step compiles + is chaos/scale-testable)
1. **`StateBackend` trait + in-memory impl** (parity refactor): move `WindowAccumExec` off the
   raw `Vec<RecordBatch>` onto the trait. No behavior change; sets the seam. Validate 6/6 + EO.
2. **Spill layer**: `MemoryPool`-budgeted, spill cold key-groups to a local embedded KV.
   *Acceptance:* a keyed windowed agg with **state ≫ RAM** runs stable (no OOM).
3. **Incremental + disaggregated checkpoint**: per-key-group delta upload + atomic manifest;
   restore loads from object store. *Acceptance:* checkpoint upload scales with the delta, not
   total state; EO across crash with large state holds.
4. **Rescaling**: key-group reassignment on parallelism change. *Acceptance:* change workers
   live, state redistributes, results stay correct.

## Sequencing with the DataFusion 54.0.0 upgrade
See `docs/design/datafusion-54-upgrade.md`. 54.0.0's `MemoryPool`/spill and StringView/BinaryView
maturation help layer 2; decide order after the 54 arrow-bump delta is known. Both in
`docs/PROD_GRADE_ROADMAP.md` (P0).
