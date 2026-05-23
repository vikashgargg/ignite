# Vajra — Build Status

> Last updated: 2026-05-23
> Tag: **v0.1.0-alpha** (Phase 1 complete)
> Branch: `main`

---

## Phase 1 — Complete ✅

### Foundation ✅
- Forked `lakehq/sail` → Vajra; binary renamed `vajra`; CLI restructured
- GitHub Actions CI: check / test / clippy / fmt on every push
- Cross-compile: Linux x86_64 + aarch64 musl via `cargo-zigbuild`; macOS universal2
- Release workflow: publishes binaries on `v*` tags
- `install.sh` for `curl | sh` install

### Spark Compatibility — 71/71 (100%) ✅

| Fix | Description |
|---|---|
| DELETE without WHERE | `lit(true)` predicate in delta-rs path |
| UPDATE SET | Copy-on-Write via `CASE WHEN` + Truncate overwrite |
| `monotonically_increasing_id()` in aggregates | Pre-projection before DataFusion Aggregate node |
| FILTER in aggregate functions | Confirmed working; stale skip removed |
| INSERT OVERWRITE | Stale skip removed |
| Managed tables | `is_external` flag; MANAGED default when no LOCATION |
| JSON PERMISSIVE / DROPMALFORMED / FAILFAST | `PermissiveJsonDecoder` streaming pipeline |
| `_corrupt_record` no-schema inference | Column injected when no schema provided |
| Arrow UDF type coercion | Correct type coercion for Arrow batch UDFs |
| HAVING-only aggregates | `find_aggregate_exprs` now includes HAVING expression |
| Map extraction key cast | Cast to match map key type for nested `getItem` |
| Partition column type inference | Int64 / Float64 / Utf8 from key=value paths |
| `describe()` field IDs | Opaque `#N` IDs resolved to column names |
| UPDATE SET NULL literals | Correct NULL handling in update expression |
| Python 3.9 UDF worker compat | `spark.py` UDF worker compatible with Python 3.9+ |

### TPC-H — 22/22 PASS ✅

All 22 queries pass on the release binary (LTO). Total: **1.515s**.

```
Q01 0.12s  Q06 0.03s  Q11 0.02s  Q16 0.04s  Q21 0.11s
Q02 0.03s  Q07 0.09s  Q12 0.07s  Q17 0.13s  Q22 0.02s
Q03 0.06s  Q08 0.07s  Q13 0.05s  Q18 0.14s
Q04 0.04s  Q09 0.09s  Q14 0.04s  Q19 0.08s
Q05 0.08s  Q10 0.10s  Q15 0.05s  Q20 0.06s
```

### Distributed Modes — All Three Verified ✅

| Mode | Status |
|---|---|
| `local` | ✅ 71/71 |
| `local-cluster` | ✅ 71/71 |
| `kubernetes-cluster` (kind) | ✅ 71/71 |

### Apple Container ✅
- `docker/apple/Dockerfile` — linux/arm64 optimised
- Layer-cache split: manifests → `cargo fetch` → build (fast incremental rebuilds)
- SIGTERM graceful shutdown handler (handles `container stop` / K8s eviction)
- HEALTHCHECK TCP probe
- `make container-build` / `make container-run` / `make container-run-cluster`

### Release ✅
- Tag: `v0.1.0-alpha`
- Binary: `./target/release/vajra` (105 MB macOS ARM64)
- Scorecard: **100% (71/71)**
- TPC-H: **22/22, 1.515s**

---

## Phase 2 — In Progress

Target: `v0.3.0` — "Distributed GA"

| Item | Status |
|---|---|
| Structured Streaming (Kafka → Delta) | Not started |
| Arrow Flight shuffle at TB scale | Not started |
| JWT / mTLS auth middleware | Not started |
| `vajra-pyspark` PyPI package | Not started |
| Unity Catalog production hardening | Not started |
| TPC-H SF-100 distributed benchmark | Not started |

See [PLAN.md](PLAN.md) for full Phase 2 breakdown.

---

## Known Limitations

- **Streaming**: `readStream` / `writeStream` not yet implemented
- **Scale**: Distributed mode tested at SF-1 only; TB-scale validation is Phase 2
- **Catalogs**: Unity Catalog and HMS have provider stubs; not production-hardened for schema evolution or ACL enforcement
- **Python UDFs**: Require `PYTHONPATH` pointing to PySpark installation on the server
- **mimalloc**: Disabled by default — must NOT be re-enabled if Python UDFs are used (causes allocator re-entrancy crash with PyO3 on Tokio worker threads)
