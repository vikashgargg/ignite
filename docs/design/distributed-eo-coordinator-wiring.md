# Distributed EO rework — wire the EpochCoordinator into the realtime path (Flink ABS)

Branch: `streaming/distributed-eo-rework` · Status: DESIGN (2026-07-03) · Closes the crash-EO-at-scale P0
([continuous-stateful-eo-fix.md](continuous-stateful-eo-fix.md): multi-partition continuous stateful
crash-EO produces real committed dups at N=16; targeted offset-record patches were necessary-but-insufficient).

## 0. STANDING PRINCIPLE — no patches, structural correctness only (charter, user-directed 2026-07-03)

Zelox replaces Flink and Spark **in every way** (see [zelox_charter](../../MEMORY.md)); crash-EO exactly-once
is a **structural invariant**, not a metric to tune toward with heuristics. **Rule: do NOT patch the
symptom.** Three targeted patches on the ad-hoc sink-commit path (W3 offset-completeness gate, W4 GC-guard,
W4 recovery-truncation) each only *moved* the dup count (7407→7414→6259) without eliminating it — proof the
**ad-hoc "last sink task counts done-markers" commit is the wrong abstraction**. The credible prod-grade fix
is the one Flink/RisingWave already prove: a **checkpoint coordinator that commits epoch `e` iff EVERY task
(all N sources + all M windows + all P sinks) has durably acked `e`** — global consistency by construction,
so completeness/ordering/truncation become *consequences* of the protocol, not standalone guesses. Any change
to the EO path must advance THIS invariant and cite it; a change that only shifts the measured dup count is
rejected. Offset-completeness and recovery-truncation are kept ONLY as components of the coordinator protocol
(they are Flink-ABS-correct), never as the commit decision itself.

## 1. The finding (measured + code-audited)

The crash-EO dup at scale is a **globally-inconsistent checkpoint**: on crash + restart, the same window is
committed under a pre-crash AND a post-crash epoch (measured via `_spark_metadata`: real committed dups, not
a test artifact). Root architectural cause:

- **`EpochCoordinator` (Zelox's Flink checkpoint-coordinator / RisingWave meta-service) EXISTS + is
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

## 4c. FINAL protocol — object-store epoch acks, coordinator-gated commit (implementation, 2026-07-03)

Chosen realization (objectively-better-in-production vs an RPC coordinator: no new RPC surface, identical in
local-cluster and EKS, same durable medium as offsets/state = RisingWave decoupled-commit + the F4
object-store-atomic principle Zelox already uses). The tested [`EpochCoordinator`] state machine is the
commit decision — fed acks read from object store, it commits epoch `e` **iff every task acked**.

**Expected task set (written once each):** `epochs/expected/{src,win,snk}` = N readers / M window instances /
P sink tasks. Total expected = N+M+P leaf tasks per epoch.

**Per-epoch, per-task ack (durable-snapshot-THEN-ack, Chandy-Lamport local rule):**
- Source inst `i` on barrier `e`: stage offsets (exists) → ack `epochs/<e>/ack/src-<i>` = `{offsets_i}`.
- Window inst `p` on barrier `e`: durably snapshot `emitted_ends`+state (exists) → ack `epochs/<e>/ack/win-<p>`
  = `{state_ptr: "window/p/e"}`. **← the structurally-missing gate: today the sink commits without any
  guarantee every window durably snapshotted `e`.**
- Sink task `j` on barrier `e`: write slice `<base>/<e>/part-j.parquet` (exists) → ack `epochs/<e>/ack/snk-<j>`.

**Coordinator-gated commit (one designated finalizer task):** builds `EpochCoordinator::new(0..N+M+P)`, reads
`epochs/<e>/ack/*`, feeds each into `ack()`. Only when `ack()` returns `Some(GlobalCheckpoint)` (ALL N+M+P
acked) does it write ONE atomic `realtime/committed` = `{epoch, offsets: all src acks, state_ptrs: all win
acks}` THEN commit `_spark_metadata` (union of snk slices). No all-ack ⇒ deferred (invisible, uncommitted).
Replaces the ad-hoc "last sink counts sink done-markers + offset-count heuristic" entirely.

**Recovery:** read `realtime/committed`→`last_committed`; every source seeks its offset; every window restores
`window/p/last_committed`; **truncate every epoch > last_committed** (`_spark_metadata` + data dirs) so the
visible set == the recovery line. Truncation/completeness are now *consequences* of the all-ack invariant.

**Gate (unchanged):** `INC=0 PARTS=16 N=1000 bash scripts/inc_ckpt_gate.sh` crash×N → dup=0, then PARTS=8/4
regression, then EKS P1b → dup=0. A run that only lowers the dup count (not 0) does NOT advance the invariant.

## 4e. FIX LANDED — aligned checkpoint barriers in the exchange (2026-07-03)

`StreamExchangeExec`'s N→M receiver merge now **aligns** `Checkpoint{e}` (Flink ABS): it BUFFERS each input's
barrier and emits ONE aligned barrier downstream only when every non-ended input has reached `e` (MIN over
active inputs; a not-yet-barriered active input HOLDS). Previously `Checkpoint` fell into `Mk::Other` and only
sub-channel 0's barrier was forwarded (15 of 16 dropped) → the window snapshotted an inconsistent cut. With
alignment, a window's `watermark@e` reflects data ≤ every reader's `e` offset, so the recovery cut (offset +
watermark + `emitted_ends`) is consistent and `emitted_ends` watermark-pruning can never re-emit a committed
window. **GATE: `INC=0 PARTS=16 N=1000 scripts/inc_ckpt_gate.sh` crash ×3 → rows=6000/6000, distinct=6,
all_counts_10=True, no_dup=True, PASS ×3 (dup=0).** The three earlier patches (offset-completeness, GC-guard,
recovery-truncation) are retained as sound Flink-ABS components but were necessary-not-sufficient without the
aligned barrier. REMAINING before the claim: PARTS=8/4 + INC=1 regression, correctness_gate 6/6, then EKS P1b.
`exchange.rs`: `rewrite_checkpoint` + `Mk::Checkpoint` + the align block in `merge_output_subchannels`.

## 4d. MEASURED root cause (2026-07-03, decisive — not asserted)

Instrumented the single-sink PARTS=16 crash gate (topology confirmed:
`RealtimeFileSink(1) ← StreamBarrierAlign(8→1) ← WindowAccum(8) ← StreamExchange(16→8) ← Watermark ← Kafka(16)`).
Dumped, per crash, which committed epoch dirs hold the duplicated `(window,key)` rows AND what each window
instance restores:

- Only **2 epoch dirs commit**: epoch 0 (pre-crash, 2000 rows = 2 windows) and epoch 24 (post-restart drain,
  5004 rows). **1003 `(window,key)` pairs are in BOTH** ⇒ recovery re-emitted already-committed windows.
- On restart each window instance restores `committed_epoch=23`, **`watermark=21s`**, `emitted_ends={10s,20s}`
  (only 2 ends — the rest PRUNED), `pending_rows≈130`.
- `emitted_ends` is **pruned by watermark** (window_accum.rs:975 "P1 fix": once wm passes a window end +
  retention, drop it; safe in steady state because late data < wm is dropped at ingestion).

**The bug = a non-consistent recovery cut.** The sink commits window OUTPUT continuously (watermark-driven,
between barriers) into per-epoch data dirs, but the window STATE snapshot for the committed epoch records a
watermark/`emitted_ends` that does **not** match the output already committed. On recovery the source resumes
by OFFSET while the window restores a watermark that is LOWER than the ends of windows the sink already
committed (e.g. epoch 0's windows end above the restored 21s). Those ends were **pruned** from `emitted_ends`,
so re-read data re-aggregates and re-emits them → committed a second time under the drain epoch = the dup. In
one line: **offset, watermark, `emitted_ends`, and committed output are four views of the checkpoint that are
NOT the same cut** — exactly the globally-inconsistent checkpoint (§1), now measured end-to-end.

**Why patches can't fix it:** offset-completeness (W3), GC-guard, and recovery-truncation (W4) each address one
view; the defect is that the four views diverge. `emitted_ends` pruning is only sound when the restored
watermark and the resume offset are the SAME cut — which the self-timed multi-reader + unaligned-exchange path
does not guarantee. The fix must make the checkpoint ONE consistent cut.

**The credible fix (structural, Flink ABS):** the epoch barrier must be a true in-band marker so that for a
committed epoch `e`: the committed offset, the window watermark, the (unpruned-below-that-watermark)
`emitted_ends`, and the sink output ALL correspond to the same data boundary. Concretely: (a) the
`StreamExchange` must ALIGN barriers (forward `Checkpoint{e}` to a window instance only after `e` arrived from
every upstream input) so a window's watermark at barrier `e` reflects only data ≤ every reader's `e` offset;
(b) `emitted_ends` may be pruned in memory but the per-epoch snapshot must retain every end ≥ the committed
watermark so recovery cannot re-emit; (c) commit offset+state+output atomically for the SAME `e` via the
EpochCoordinator (§4c). Then pruning-by-watermark ⟺ pruning-by-offset and recovery is exactly-once. This is
the next implementation step; it is validated by the same gate (dup=0), not by a lower dup count.

## 5. Risk / honesty

This is a real distributed-protocol change (F2/F3), high blast radius (the core EO path). It is gated at every
step by the local PARTS=16 crash repro (fast, no EKS) and the full correctness_gate (no regression on the
already-Flink-class no-dup/completeness/throughput/memory). Do NOT claim crash-EO exactly-once at scale until
C7 PARTS=16 is dup=0 locally AND P1b is dup=0 on EKS. The coordinator being pre-built + unit-tested de-risks
the protocol; the work is the wiring + recovery, done carefully, not rushed.
