# Incremental + async checkpointing (Flink ForSt-class, unified with F5 spill)

Status: DESIGN + increment 1 (2026-06-24). Grounded in REFERENCES §2/§3/§3b (Flink incremental
checkpoints, SST shared-file model, SharedStateRegistry refcount, async upload; ForSt object-store
state).

## The problem
Today `state_io` snapshots the **full** operator state every epoch: `gather_partials` reads back
ALL spilled chunks + in-RAM, re-encodes the whole thing, one `put` of `epoch-N`. After F5 (spillable
state ≫ RAM) this is actively harmful: a 50 GB state rewritten every checkpoint interval stalls the
pipeline and burns object-store bandwidth. Flink solved this with **incremental checkpoints** — only
the *changed* state is written; unchanged SST files are *referenced*, not re-uploaded (REFERENCES §3b).

## The unlock: F5 spill chunks ARE the SST-analog
Our F5 spill writes **immutable, numbered Arrow-IPC blobs** (`state/<op>/spill-<id>`). That is exactly
Flink's per-checkpoint SST-file model. So incremental checkpointing is nearly free:

- A **checkpoint = a manifest**, not a state copy: `{ epoch, meta:[i64], chunks:[id...],
  residual_key }`.
- **Spilled chunks (the bulk) are referenced, not re-uploaded** — they were written out-of-band
  during spill, OFF the barrier critical path. The only fresh bytes per checkpoint are the small
  in-RAM **residual** (the un-spilled tail) + the manifest.
- **Checkpoint cost = O(residual + chunks-spilled-since-last-ckpt)**, not O(total state). For a steady
  large state with infrequent new spills, a checkpoint is a few KB.
- **One Arrow format, object-store-native, spill = checkpoint** — vs Flink bolting ForSt onto RocksDB.
  This is the "even better than Flink" edge: the spill substrate and the checkpoint substrate are the
  same immutable-chunk store.

## Refcount / SharedStateRegistry (the lifecycle change)
F5 today GCs a chunk when it's *consumed* at finalize. For incremental checkpoints a chunk referenced
by a *committed* epoch must survive until no retained epoch references it. So chunks become
**immutable + refcounted**:
- Each epoch manifest lists the chunk-ids it references.
- A chunk is deleted only when **no retained epoch manifest references it** (= Flink SharedStateRegistry).
- On subsumption (commit epoch N ⇒ retain {N, N-1}, drop N-2): delete N-2's manifest + residual, then
  delete any chunk no longer referenced by a retained manifest.

## Async
The expensive part (chunk upload) already happens during spill, off the barrier path. The per-epoch
write is just the residual blob + manifest (small) — so checkpointing is already near-async. True
async (spawn the residual+manifest put without blocking the unfold poll, track completion before
acking the barrier) is a refinement once the manifest mechanism lands.

## Build plan
- **inc.1 (this increment) — `state_io` manifest primitive, unit-tested, NOT yet in the hot path**
  (mirrors how F5.0 landed the spill primitive first):
  - `Manifest { meta: Vec<i64>, chunks: Vec<u64>, residual: Option<blob> }` (JSON header + Arrow-IPC
    residual), keys `state/<op>/epoch-<N>/manifest` + `.../residual`.
  - `stage_epoch_incremental(ck, op, epoch, residual_batches, chunk_ids, meta)` — writes residual +
    manifest only; does NOT touch the referenced chunk blobs.
  - `restore_epoch_incremental(ck, op, epoch)` → `(full_batches, meta)` by reading manifest → residual
    + each referenced chunk.
  - `gc_unreferenced_chunks(ck, op, retained_epochs, all_chunk_ids)` — refcount over retained
    manifests; delete chunks referenced by none + drop subsumed manifests/residuals.
  - Unit test: write chunks 0,1,2; stage epoch 1 → {chunks:[0,1], residual:R1}; epoch 2 →
    {chunks:[0,1,2], residual:R2} (shares 0,1); restore both; GC with retained={2} → chunk-specific
    survival/deletion verified; restore epoch 2 still complete.
- **inc.2 — wire into `WindowAccumExec`/`StreamJoinExec`**: change the chunk lifecycle to immutable +
  refcounted (chunks not GC'd at finalize while a live epoch references them); Checkpoint{epoch}
  handler calls `stage_epoch_incremental` (residual = in-RAM pending/buffer; chunks = current spilled
  ids); GC via `gc_unreferenced_chunks` on subsumption.
- **inc.3 — async**: spawn residual+manifest put off the unfold; track completion before barrier ack.
- **inc.4 — gate**: large-state continuous run, measure per-checkpoint bytes written = O(delta) not
  O(state); recovery correctness; compare checkpoint duration vs the full-snapshot path + vs Flink.

## Honest scope
inc.1 is the safe primitive (no hot-path change, no F5 regression risk). The lifecycle change (inc.2)
is where care is needed: a chunk referenced by a committed epoch must not be GC'd. EO is preserved
because the manifest + referenced immutable chunks are a consistent point-in-time snapshot (the same
epoch the source seeks offsets for — F3-c).
