# Distributed EO rework — wire the EpochCoordinator into the realtime path (Flink ABS)

Branch: `streaming/distributed-eo-rework` · Status: DESIGN (2026-07-03) · Closes the crash-EO-at-scale P0
([continuous-stateful-eo-fix.md](continuous-stateful-eo-fix.md): multi-partition continuous stateful
crash-EO produces real committed dups at N=16; targeted offset-record patches were necessary-but-insufficient).

## 1. The finding (measured + code-audited)

The crash-EO dup at scale is a **globally-inconsistent checkpoint**: on crash + restart, the same window is
committed under a pre-crash AND a post-crash epoch (measured via `_spark_metadata`: real committed dups, not
a test artifact). Root architectural cause:

- **`EpochCoordinator` (Vajra's Flink checkpoint-coordinator / RisingWave meta-service) EXISTS + is
  unit-tested but is NEVER WIRED** (`grep` = zero non-test usage). It already implements the correct
  protocol: collect an `EpochAck{offsets, state_ptrs}` from EVERY task, declare epoch `e` globally complete
  only when ALL expected tasks ack, emit one atomic `GlobalCheckpoint`; recovery restores `last_committed`.
- The realtime path instead uses an **ad-hoc sink commit**: "the LAST *sink* task (sees all `num_partitions`
  markers) writes the single global commit" (`streaming_decode.rs`). This coordinates ONLY the sink's own
  tasks — NOT the N source instances' offset staging (T-EO-3 unions whatever `staged-epoch-<e>` keys happen
  to exist at commit time — incomplete under barrier skew), and NOT the M window instances' state snapshots.
- ⇒ the sink can commit epoch `e` (offsets + `_spark_metadata`) while some sources haven't staged `e` and/or
  some window instances haven't durably snapshotted `e`'s `emitted_ends`. On recovery the three parts
  (offsets / window state / sink data) are **mutually inconsistent** → re-read + re-emit already-committed
  windows → dups. At N=16 this is frequent; at N≤2 it's rare (why local PARTS≤4 nearly hid it).

## 2. The fix — route the realtime checkpoint through the EpochCoordinator (Chandy-Lamport / Flink ABS)

Grounded in REFERENCES §2 (Flink barriers + alignment), §8 (RisingWave barrier commit), the
prodgrade-practices "Exactly-once" row (barrier-aligned snapshot; offsets+state commit ATOMICALLY), and the
EpochCoordinator's own design (ABS + RisingWave). The coordinator is the missing glue:

1. **Barrier** `Checkpoint{e}` originates from the epoch clock (coordinator triggers; sources currently
   self-time — move the clock to the coordinator so all sources share epoch `e`, removing the skew).
2. **Snapshot-then-ack, per task:** on `Checkpoint{e}` each task DURABLY snapshots its slice, THEN acks:
   - **Source instance i:** stages its offsets (already: `write_staged_epoch_offsets`) → ack `{offsets_i}`.
   - **Window instance p:** durably snapshots `(emitted_ends + pending state)` for `e` (already:
     `state_io` per-epoch) → ack `{state_ptr: op/p/e}`.
   - **Sink task j:** writes its slice `<base>/<e>/part-j.parquet` (NOT `_spark_metadata` yet) → ack.
3. **Global completion:** the coordinator marks `e` complete only when EVERY expected task (all N sources +
   all M windows + all sink tasks) has acked (`EpochCoordinator::ack` → `GlobalCheckpoint`). Late/stale acks
   idempotent; committing `e` subsumes earlier epochs (monotone `last_committed`).
4. **Atomic commit (the driver, on `GlobalCheckpoint`):** write ONE atomic `realtime/committed` =
   `{epoch: e, offsets: ALL sources, state_ptrs: ALL windows}`; THEN commit `_spark_metadata/<e>` (make the
   sink slices reader-visible). Order matters: data becomes visible only AFTER the global offset+state
   commit, so a crash before `_spark_metadata` leaves `e` invisible + not committed → re-done cleanly.
5. **Recovery:** read `realtime/committed` → `last_committed = e`. Truncate/ignore any epoch `> e` (remove
   uncommitted `<base>/<e'>` + `_spark_metadata` beyond `e`). Every source seeks its committed offset; every
   window restores its `op/p/e` state (incl. `emitted_ends`). Now re-processing NEVER re-emits a committed
   window (emitted_ends restored) and NEVER re-reads committed data (offsets restored) — EO.

## 3. Why this is exactly-once (the invariant)

`realtime/committed.epoch = e` ⟺ every source's offset AND every window's state for `e` are durable AND all
sink slices for `e` are written. Recovery restores that globally-consistent line and discards everything
after it. This is Flink's Asynchronous Barrier Snapshotting; we improve on Flink by snapshotting immutable
Arrow chunks (O(delta) inc-ckpt, no RocksDB) and committing to object store (no JobManager RPC on the hot
path — RisingWave-style decoupled durable commit).

## 4. Build plan (incremental, each gated by the local PARTS=16 crash repro → dup=0)

- **W1 — coordinator wiring skeleton:** thread an `EpochCoordinator` handle (driver-side) + a task→coordinator
  ack channel (reuse the existing distributed stream_service RPC, or object-store acks for cloud-native).
  Expected-task set = N sources + M windows + sink tasks (from the plan).
- **W2 — source + window acks:** source acks offsets at barrier `e`; window acks its state ptr after durable
  snapshot. (Both already snapshot; add the ack.)
- **W3 — global commit:** driver writes `realtime/committed` (offsets + state_ptrs) atomically on
  `GlobalCheckpoint`, THEN `_spark_metadata`. Replace the ad-hoc "last sink task commits".
- **W4 — recovery truncation + restore:** on startup, truncate epochs `> last_committed`; restore all state
  + offsets from the global record. Gate: **C7 PARTS=16 crash → dup=0** (local), then PARTS=8/4 regression.
- **W5 — EKS re-validate** P1b → dup=0 at 16 partitions; then promote the claim + settle the N-reader default.

## 4b. W1 investigation (code-audited 2026-07-03)

Confirmed the per-operator ordering is ALREADY correct (Chandy-Lamport local rule):
- **Source** (`kafka/reader.rs:947-948`): `write_staged_epoch_offsets(...).await` THEN emit `Checkpoint{e}`
  — stages durably BEFORE the barrier. ✓
- **Window** (`window_accum.rs:1078-1156`): `stage_epoch_incremental/stage_epoch_state(...).await` with
  `meta = [watermark, emitted_ends...]` THEN forwards the barrier. Snapshots durably (incl. emitted_ends)
  BEFORE forwarding. ✓
- **`StreamBarrierAlignExec`**: correct N→1 Chandy-Lamport alignment (seal `e` only when every input
  reached it).

So the LOCAL ordering is right; the gap is GLOBAL COMPLETION at the sink: `RealtimeFileSinkExec` commits
`e` (offset union + `_spark_metadata`) driven by its own marker/`num_partitions` coordination, NOT by a
guarantee that ALL N sources staged `e` AND ALL M windows snapshotted `e`. The offset union (`list_rel`
of `staged-epoch-<e>`) reads whatever is present at commit time — incomplete under barrier skew at N=16
(measured: partitions missing → resume at 0). This is precisely the `EpochCoordinator`'s all-task-ack
guarantee, which is unwired. ⇒ W2/W3 (task acks → coordinator → driver atomic global commit) is the
correct, minimal change; the per-operator snapshots it needs already exist and are correct.

## 5. Risk / honesty

This is a real distributed-protocol change (F2/F3), high blast radius (the core EO path). It is gated at every
step by the local PARTS=16 crash repro (fast, no EKS) and the full correctness_gate (no regression on the
already-Flink-class no-dup/completeness/throughput/memory). Do NOT claim crash-EO exactly-once at scale until
C7 PARTS=16 is dup=0 locally AND P1b is dup=0 on EKS. The coordinator being pre-built + unit-tested de-risks
the protocol; the work is the wiring + recovery, done carefully, not rushed.
