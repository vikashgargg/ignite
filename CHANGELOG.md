# Changelog

All notable changes to Vajra are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Public GA prod-grade readiness board (`docs/design/public-ga-readiness-board.md`) — SDLC/Jira-style
  epics for distribution, supply-chain, container, Helm, observability, testing, and governance.
- `NOTICE` (Apache-2.0 fork attribution to LakeSail/Sail + Arrow/DataFusion) and this `CHANGELOG.md`.
- Production-workload benchmarks on real object storage (AWS S3), measured on Graviton EKS:
  - **P1** — Kafka → 10 s windowed aggregation → Parquet on S3, **exactly-once including hard-crash
    recovery** (`kill -9` → resume from S3 checkpoint): rows=9000, duplicates=0, sums bit-identical.
  - **P4** — batch 200M rows → write Parquet on S3 → read + aggregate, vs Apache Spark 3.5.3:
    Vajra **5.92 s / 3.44 GiB** vs Spark **36.94 s / 8.1 GiB** — ~6.2× faster, ~2.4× less memory,
    bit-identical output.

### Changed
- **Streaming vs Flink claims reconciled to the latest rigorous measurement (honest).** The
  authoritative tri-engine (Nexmark-methodology) run at ~5.3M ev/s **supersedes** an earlier lighter
  ~1.5M-ev/s run: throughput is now reported as ~1.10× *slower* than Flink 1.19 (competitive), memory
  ~1.2× less (path-dependent; batch is ~8× less). Claims are made only where measured, path-dependence
  flagged.
- **Prod-grade structured logging**: all library `eprintln!` scaffolding replaced with the `log`
  facade (`env_logger`, `RUST_LOG` filtering, timestamp/level/target), wired at every server
  entrypoint. Removed the ad-hoc `VAJRA_F5_DEBUG` env gate (log levels gate verbosity instead).

### Fixed
- Per-partition watermark (Flink `withIdleness`) prevents premature window close on multi-partition
  Kafka sources — fixes continuous exactly-once duplicate window emissions.

---

> Earlier history predates this changelog; see the git log and `STATUS.md` / `BENCHMARKS.md` for the
> measured milestones (Spark SQL 105/105 scorecard, TPC-H SF-1/SF-100, TPC-DS-99 coverage, ClickBench,
> streaming exactly-once, Delta/Iceberg support, Apple-container validation).
