# Changelog

All notable changes to Zelox are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **DataFusion 54.0.0 + Arrow 58.3.0 upgrade** (2026-07-06): full workspace migrated. `cargo test --workspace`
  860 passed / 0 failed, `clippy --all-targets -D warnings` clean, gold data byte-identical to DF53 (map/stack
  nullability preserved). Adopted DF54's new optimizer rules (WindowTopN / TopKRepartition / HashJoinBuffering)
  and coercion improvements; migrated the distributed codec (`PhysicalPlanDecodeContext`), `Cast`/`TryCast`
  (`field: FieldRef`), `PruningStatistics::row_counts(&self)`, `ScalarValue::ListView`, `Ident`/`ObjectName`
  moves, and the inherent-downcast (`as_any` removal).

### Fixed
- **DataFusion 54 distributed scan double-count** (critical): DF54's morsel-driven scan pools all files into a
  shared work-source for in-process sibling work-stealing; an isolated distributed task has no siblings and
  drained the whole pool → **every file read once per partition (N× row duplication)**, silently, on any file
  format. Fixed with DF54's own opt-out — the per-task plan rewrite sets `partitioned_by_file_group=true` so
  each partition reads only its own file group (`WorkSource::Local`). Verified: repro 400→200, inc_ckpt crash-EO
  PASS, **T2 kind (real k8s): windowed-agg n_windows=5 / sum=5M exact, Kafka-sink 2M/2M delivered**. See
  `docs/REFERENCES.md`.
- **Build/disk hygiene**: `[profile.dev] incremental=false` + `debug="line-tables-only"` (bounds the debug
  target 37 GiB → 7.6 GiB, ends the ENOSPC build crashes); threshold-driven launchd prune watchdog replacing a
  TCC-blocked 3am cron.

### Added
- **Streaming crash-EO exactly-once at scale** (EKS-confirmed, 2026-07-04): Flink-ABS **aligned checkpoint
  barriers** in the exchange + **exact source-signaled idleness** (librdkafka `PartitionEOF` = Flink
  `WatermarkStatus.IDLE`) + a **crash-recovery emit floor**. 16-partition continuous `kill -9` → dup=0, sum
  exact, clean==crash.
- **Final-window completeness** — opt-in `ZELOX_COMPLETE_ON_END` (Flink `scan.bounded.mode` parity): flush all
  windows at end-of-input while keeping Spark availableNow semantics by default (superset). EKS: 10 windows/100M.
- **Parallel Kafka sink** (Flink `KafkaSink` parity): fixes a **15/16-partition data-loss bug** (a single sink
  task read only input partition 0) + ~300× throughput. EKS: 100M/100M delivered @ 1.67M msg/s.
- **3-tier SDLC + `kind` tier** (`docs/design/three-tier-sdlc.md`, `k8s/kind/`, `scripts/kind_*`): T1 local →
  T2 kind (real k8s, free) → T3 EKS (confirm-only). Prod-representative self-checking gates
  (`scripts/{correctness_gate,inc_ckpt_gate,completeness_gate,kafka_sink_gate,local_continuous_scale}`).
- **Spark-parity gap list + DataFusion 54 / Arrow 58.3 upgrade plan** (`docs/design/spark-parity-and-upgrade-plan.md`) referencing LakeSail v0.6.5 (DF 54.0.0 + Arrow 58.3.0) as the proven upgrade reference.
- Public GA prod-grade readiness board (`docs/design/public-ga-readiness-board.md`) — SDLC/Jira-style
  epics for distribution, supply-chain, container, Helm, observability, testing, and governance.
- `NOTICE` (Apache-2.0 fork attribution to LakeSail/Sail + Arrow/DataFusion) and this `CHANGELOG.md`.
- Production-workload benchmarks on real object storage (AWS S3), measured on Graviton EKS:
  - **P1** — Kafka → 10 s windowed aggregation → Parquet on S3, **exactly-once including hard-crash
    recovery** (`kill -9` → resume from S3 checkpoint): rows=9000, duplicates=0, sums bit-identical.
  - **P4** — batch 200M rows → write Parquet on S3 → read + aggregate, vs Apache Spark 3.5.3:
    Zelox **5.92 s / 3.44 GiB** vs Spark **36.94 s / 8.1 GiB** — ~6.2× faster, ~2.4× less memory,
    bit-identical output.

### Changed
- **Streaming vs Flink claims reconciled to the latest rigorous measurement (honest).** The
  authoritative tri-engine (Nexmark-methodology) run at ~5.3M ev/s **supersedes** an earlier lighter
  ~1.5M-ev/s run: throughput is now reported as ~1.10× *slower* than Flink 1.19 (competitive), memory
  ~1.2× less (path-dependent; batch is ~8× less). Claims are made only where measured, path-dependence
  flagged.
- **Prod-grade structured logging**: all library `eprintln!` scaffolding replaced with the `log`
  facade (`env_logger`, `RUST_LOG` filtering, timestamp/level/target), wired at every server
  entrypoint. Removed the ad-hoc `ZELOX_F5_DEBUG` env gate (log levels gate verbosity instead).

### Fixed
- Per-partition watermark (Flink `withIdleness`) prevents premature window close on multi-partition
  Kafka sources — fixes continuous exactly-once duplicate window emissions.

---

> Earlier history predates this changelog; see the git log and `STATUS.md` / `BENCHMARKS.md` for the
> measured milestones (Spark SQL 105/105 scorecard, TPC-H SF-1/SF-100, TPC-DS-99 coverage, ClickBench,
> streaming exactly-once, Delta/Iceberg support, Apple-container validation).
