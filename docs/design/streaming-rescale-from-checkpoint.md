# Rescaling from checkpoint — keyed-state redistribution (P0-2)

**Status:** design. **Advances:** roadmap P0-2 ([flink-replacement-roadmap.md](flink-replacement-roadmap.md)).
**Builds on:** incremental checkpoint ([streaming-incremental-checkpoint.md](streaming-incremental-checkpoint.md)),
F5 spill chunks. **Grounded in:** Flink **key-groups** + rescaling, ForSt/SharedStateRegistry
(REFERENCES §2/§3b).

## Problem
Today a stateful job restores at the **same** parallelism it was checkpointed at (each operator
instance restores its own `state/<op>/...`). Prod needs to **grow/shrink** a running job — restore at a
**different** parallelism M′ ≠ M — which requires **redistributing keyed state** so each new instance
owns the right keys. This is Flink's real operational killer feature; without it you can't scale a live
pipeline. It's the one big *operability* P0 alongside throughput.

## Flink's mechanism (the reference)
Flink pre-partitions the key space into a fixed number **G of key-groups** (`maxParallelism`, default
128). Each key maps to a key-group: `kg = hash(key) % G`. An operator instance owns a **contiguous
range** of key-groups; rescale to M′ just re-assigns key-group ranges to the M′ instances. State is
physically organized by key-group so an instance reads exactly the key-groups it now owns. Cost: read +
re-serialize the RocksDB state for the moved key-groups.

## Zelox design — key-groups on the immutable-Arrow-chunk substrate

### 1. Fixed key-groups (G, default 128, configurable as max-parallelism)
`kg(key) = hash(key) % G`. The keyed exchange (`StreamExchangeExec`, already hashes by key) routes by
**key-group → instance**: instance `i` of M owns `[ i*G/M, (i+1)*G/M )`. Routing through key-groups
(not `hash % M` directly) is what makes rescale a pure re-assignment.

### 2. State physically tagged by key-group
Each operator's keyed state (window `(window,key)` accum; join per-side buffer) carries a **key-group
column** (or spills **one chunk per key-group range**). Two options:
- **(a) KG-aligned chunks (preferred):** spill so a chunk covers a contiguous KG range; the manifest
  records each chunk's KG coverage `[kg_lo, kg_hi]`. → rescale moves whole chunks, **no row rewrite**.
- **(b) KG column + filter:** chunks are arbitrary but every row has its `kg`; on restore an instance
  reads candidate chunks and **filters** to its owned KGs. Simpler to write; reads more than it keeps.
Start with (b) (correctness, minimal write-path change), evolve hot paths to (a) (the zero-rewrite win).

### 3. Manifest carries KG coverage
Extend the incremental-checkpoint manifest (`state_io::encode_manifest`) so each referenced chunk-id
also records `[kg_lo, kg_hi]` (or "unaligned"). The manifest already lists chunk-ids + meta — add a
parallel KG-range vector. Backward-compatible: absent KG info ⇒ treat chunk as covering all KGs (filter
path).

### 4. Rescale restore at M′
For new instance `i′` owning KG range `R′`:
1. Read the **union** of all M old instances' manifests for the epoch (a job restores from one logical
   checkpoint = M per-instance manifests).
2. Select chunks whose KG coverage intersects `R′`.
3. Path (a): adopt those chunks directly (re-reference in `i′`'s new manifest) — **no state rewrite**.
   Path (b): read + filter rows to `R′`.
4. Residual (in-RAM tail) is small → always read + filter.

### The Zelox differentiator
Because state is **immutable Arrow chunks referenced by a manifest** (not a mutable RocksDB instance),
KG-aligned rescale (path a) is a **manifest re-assignment with ZERO state rewrite** — each new
instance's manifest just references the chunks for its KGs (refcount via the existing
SharedStateRegistry). Flink must read + re-serialize the moved key-groups out of RocksDB. → rescale is
**cheaper + faster** on Zelox, on the same substrate that already gives O(delta) checkpoints + 6.6×
memory.

## STATUS (2026-06-29) — primitive layer DONE + PROVEN; operator wiring remains
- ✅ Steps 1–2 + 3a + auto: `key_group`/`instance_key_group_range`/`key_group_owner`; exchange routes
  by kg→owner (rescale-stable); manifest per-chunk KG-range + `chunks_for_range`; `restore_keyed_range`
  + `restore_keyed_range_auto` (discovers old M via `stage_parallelism`); proven exact
  (`rescale_redistributes_keyed_state_exactly`: M=4 → M′∈{2,8}, no loss/dup). Grounded REFERENCES §2b.
- ⬜ **Remaining 3b = operator wiring, target `WindowAccumExec`** (NOT `StreamJoinExec` — that rejects
  `partition!=0`, i.e. runs single-instance, so rescale N/A there). In the window execute: (a) tag the
  kg column at spill, hashing the group key with the SAME hasher the exchange uses (so kg matches
  routing) — or have the exchange tag `__kg` once and carry it through; (b) per-instance state key
  `window-0-<partition>` + `stage_parallelism(partition_count)`; (c) restore via
  `restore_keyed_range_auto(op_base, epoch, partition, partition_count, g, kg_col)`. Then the **rescale
  crash gate** (checkpoint at M, kill, restore at M′≠M, assert completeness/no-dup/EO) — needs a live
  Kafka run. This is deliberate surgery on the largest function + a slow gate; do it as a dedicated
  pass, not rushed.

## Build steps (incremental, each locally testable — no EKS)
1. **KG primitive + manifest extension:** `kg(key)`, add KG-range vector to encode/decode_manifest
   (back-compat). Unit test: round-trip + intersection selection. *(deterministic, like the O(delta)
   test.)*
2. **Exchange routes by key-group:** `StreamExchangeExec` maps `kg → instance` for M; unit test routing
   at M and M′.
3. **State tagged by KG (path b):** window/join spill carries `kg`; restore filters to owned KGs. Test:
   stage at M=4, restore at M′=2 and M′=8, assert each key lands in exactly one instance + full state
   recovered (no loss/dup).
4. **Rescale gate:** extend the correctness gate — checkpoint at M, kill, restore at M′≠M, assert
   completeness + no-dup + EO (the standing adversarial discipline). Reuses `inc_ckpt_gate`/`f3c`.
5. **Path (a) KG-aligned chunks:** spill per-KG-range; manifest re-assignment with zero rewrite; measure
   rescale time vs path (b) → the "beats Flink" number (EKS later).

## Risks
- Single-instance realtime EO commit (`kafka/reader.rs:279`) — rescaling the SOURCE parallelism is
  constrained by single-instance EO; rescale the **stateful downstream** (window/join) first (where
  key-groups live), keep source rescale as a separate item.
- KG count G is fixed at job start (= max parallelism), like Flink — document the limit.
- Correctness over performance: land path (b) (filter, always-correct) before path (a) (zero-rewrite).
