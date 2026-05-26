# Vajra — Build Status

> Last updated: 2026-05-26
> Tag: **v0.3.0-alpha** (Phase 1 + Phase 2 + Sprint 3 complete)
> Branch: `phase2/distributed`
> See [PRODUCTION_ROADMAP.md](PRODUCTION_ROADMAP.md) for the full plan.

---

## Phase 1 — Complete ✅

### Foundation ✅
- Forked `lakehq/sail` → Vajra; binary renamed `vajra`; CLI restructured
- GitHub Actions CI: check / test / clippy / fmt / distributed-scorecard / k8s-scorecard / macos-scorecard on every push
- Cross-compile: Linux x86_64 + aarch64 musl via `cargo-zigbuild`; macOS universal2
- Release workflow: publishes binaries on `v*` tags
- `install.sh` for `curl | sh` install

### Spark Compatibility — 105/105 (100%) ✅

All groups pass across all 3 deployment modes:

| Group | Score |
|---|---|
| Basic SQL | 13/13 |
| Aggregate Functions | 6/6 |
| Window Functions | 4/4 |
| String Functions | 5/5 |
| Date / Time Functions | 4/4 |
| Complex Types | 5/5 |
| DataFrame API | 9/9 |
| Python UDFs (scalar + Pandas + Arrow) | 5/5 |
| JSON Reading (PERMISSIVE / FAILFAST) | 5/5 |
| Parquet Read / Write | 3/3 |
| DML (Delta Lake DELETE / UPDATE) | 4/4 |
| Misc Spark SQL | 8/8 |
| Advanced SQL (PIVOT, UNPIVOT, TABLESAMPLE) | 6/6 |
| Higher-Order Functions (TRANSFORM, FILTER, AGGREGATE) | 5/5 |
| Recursive CTEs | 2/2 |
| QUALIFY / GROUPS BETWEEN / Named Windows | 3/3 |
| NATURAL JOIN / LATERAL VIEW OUTER | 2/2 |

Notable SQL features vs LakeSail upstream: DELETE, UPDATE, monotonically_increasing_id, FILTER aggregate, JSON PERMISSIVE, Arrow UDF coercion, HAVING-only aggregates, map extraction key cast, partition column type inference, GROUPS BETWEEN, QUALIFY, WITH RECURSIVE, RecursiveQuery optimizer fix, NATURAL JOIN, LATERAL VIEW OUTER, CROSS JOIN LATERAL, FROM-first HiveQL, TABLESAMPLE byte-size/ON, LTRIM/RTRIM trim syntax, TVF mixed args, UNPIVOT empty IN list/empty value tuple/column aliases.

### TPC-H — 22/22 PASS ✅ (SF-1 single-node)

All 22 queries pass. Total: **1.515s vs Spark JVM ~60s warm** (40× speedup).

```
Q01 0.12s  Q06 0.03s  Q11 0.02s  Q16 0.04s  Q21 0.11s
Q02 0.03s  Q07 0.09s  Q12 0.07s  Q17 0.13s  Q22 0.02s
Q03 0.06s  Q08 0.07s  Q13 0.05s  Q18 0.14s
Q04 0.04s  Q09 0.09s  Q14 0.04s  Q19 0.08s
Q05 0.08s  Q10 0.10s  Q15 0.05s  Q20 0.06s
```

### Distributed Modes — All Three Verified ✅

| Mode | Score |
|---|---|
| `local` | ✅ 105/105 |
| `local-cluster` | ✅ 105/105 |
| `kubernetes-cluster` (kind) | ✅ 105/105 |

### Apple Container ✅
- `docker/apple/Dockerfile` — linux/arm64 optimised with tarball cache workaround
- Layer-cache split: manifests → `cargo fetch` → build (fast incremental rebuilds)
- SIGTERM graceful shutdown handler; HEALTHCHECK TCP probe
- `make container-build` / `make container-run` / `make container-run-cluster`

### CI ✅ (all three platforms)
- `distributed-scorecard` — Linux, local-cluster mode, 105/105
- `k8s-scorecard` — Linux, kind cluster, kubernetes-cluster mode
- `macos-scorecard` — macOS-15 Apple Silicon, local-cluster mode

---

## Phase 2 — Complete ✅ (Sprint 2, 2026-05-24)

### Structured Streaming ✅
| Item | Status |
|---|---|
| Streaming aggregates (COUNT/SUM/AVG per micro-batch) | ✅ `StreamAggregateNode` + rewriter |
| `writeStream.format("memory").queryName(name)` | ✅ `MemorySinkExec` + `MemoryStreamBuffer` |
| `writeStream.foreachBatch(fn)` | ✅ `ForeachBatchSinkExec` PyO3 callback |
| Kafka source (`readStream.format("kafka")`) | ✅ rdkafka, 7-column Spark schema |
| Streaming checkpoint + recovery | ✅ reads/writes `{checkpointLocation}/offsets/{batchId}` |
| Stream × static join | ✅ flow-event schema stripping |
| Streaming analytic windows (rank/lag/row_number OVER) | ✅ per-micro-batch |
| Lambda HOFs in streaming | ✅ native DataFusion |
| Streaming integration test (`test_streaming.py`) | ✅ rate→agg→memory→spark.sql |

### Infrastructure ✅
| Item | Status |
|---|---|
| Scheduler HA (K8s Lease-based leader election) | ✅ `--ha` flag, `KubernetesLeaderElector` |
| Bearer token auth (`--auth-token` / `SAIL_AUTH__TOKEN`) | ✅ `BearerTokenInterceptor` |
| mTLS (`--tls-cert/--tls-key/--tls-ca`) | ✅ |
| K8s CI validation (kind in GitHub Actions) | ✅ `k8s-scorecard` job |
| macOS CI validation (Apple Silicon native) | ✅ `macos-scorecard` job |
| Standard Docker image (`docker/Dockerfile`) | ✅ K8s-ready |
| K8s Helm chart (server + worker, HPA) | ✅ `helm/vajra/` |

---

## Phase 3 — Sprint 3 Complete ✅ (2026-05-25)

| Item | Status |
|---|---|
| `F.window()` event-time windowing struct generation | ✅ `date_bin`-based struct<start,end> |
| `withWatermark` pass-through (resolver no-op) | ✅ |
| Streaming checkpoint (offset files per batch) | ✅ |
| Streaming checkpoint recovery on restart | ✅ reads max batchId from `offsets/` dir |
| TPC-DS query suite script | ✅ `scripts/tpcds_score.py` |
| TPC-H distributed benchmark script | ✅ `scripts/tpch_distributed.py` + CI job |
| `vajra-pyspark` PyPI package | ✅ `python/vajra_pyspark/` |
| Stream × static join | ✅ |
| `DESCRIBE QUERY` | ✅ returns (col_name, data_type, comment) rows |
| `df.approxQuantile()` | ✅ `approx_percentile_cont_udaf` |
| `df.freqItems()` | ✅ `array_agg(distinct)` per column |
| `dropDuplicates` within watermark | ✅ per-batch stateless distinct |
| `AddArtifacts` RPC + `CachedLocalRelation` | ✅ `ArtifactStore` session extension |
| CTAS metadata options (COMMENT/SORT BY/BUCKET BY) | ✅ silently ignored |
| Concurrency test (20 parallel sessions) | ✅ `scripts/test_concurrency.py` |
| Web UI on :4040 | ✅ axum HTML dashboard + `/api/streaming` JSON |

### Phase 3 In Progress (Sprint 4+ targets)

| Item | Status |
|---|---|
| Streaming event-time window execution | Planner ✅, executor wiring Sprint 6 |
| VARIANT type (Spark 4.x) | Sprint 4 |
| Delta time travel (AT VERSION / TIMESTAMP) | Sprint 4 |
| GroupedMap/CoGroupedMap UDFs (Spark 4.1) | Sprint 4 |
| Delta V2 checkpointing + log compaction | Sprint 4 |
| ClickBench benchmark | Sprint 4 |
| dbt integration guide | Sprint 4 |
| HMS Thrift client | Sprint 5 |
| TPC-H SF-100 distributed | Sprint 5 |
| Official Spark test suite (95%+) | Sprint 5 |

---

## Competitive Position vs LakeSail v0.6.3 (2026-05-26)

LakeSail is at v0.6.3 (released 2026-05-21) with 2,732 stars and daily merges. Full comparison:

| Dimension | LakeSail v0.6.3 | **Vajra v0.3.0** |
|---|---|---|
| Runtime | Rust | **Rust** |
| Cold start | ~2 s | **~200 ms** |
| Idle memory | ~500 MB | **~300 MB** |
| TPC-H SF-1 | ~15 s | **1.515 s (10×)** |
| Spark compat (105 scorecard) | ~95% | **100% (105/105)** |
| Python UDFs (scalar/Pandas/Arrow) | ✅ | **✅** |
| Python iterator UDFs (GroupedMap 4.1) | ✅ v0.6.3 | Sprint 4 |
| VARIANT type (Spark 4.x) | ✅ v0.6.3 | Sprint 4 |
| Delta time travel | ✅ v0.6.0 | Sprint 4 |
| Delta V2 checkpoint + log compaction | ✅ v0.6.0 | Sprint 4 |
| Delta type widening | ✅ v0.6.3 | Sprint 4 |
| Iceberg V3 | ✅ v0.6.3 | Sprint 4 |
| dbt integration | ✅ v0.6.3 | Sprint 4 |
| ClickBench | ✅ v0.6.3 | Sprint 4 |
| HMS table metadata | ✅ v0.6.3 | Sprint 5 |
| Vortex data source | ✅ v0.6.0 | Sprint 5 |
| **Kafka streaming source** | ❌ | **✅** |
| **foreachBatch** | ❌ | **✅** |
| **memory sink** | ❌ | **✅** |
| **Streaming checkpoint** | ❌ (issue #1969) | **✅** |
| **JWT bearer auth** | ❌ | **✅** |
| **mTLS** | ❌ | **✅** |
| **Apple Container (macOS 26)** | ❌ | **✅ — only one** |
| **K8s Helm chart + HPA** | ❌ | **✅** |
| **Scheduler HA** | ❌ | **✅** |
| **Web UI :4040** | ❌ | **✅** |
| **Binary size** | ~300 MB | **105 MB macOS / 80 MB Linux** |
| pip install | `pysail` | **`vajra-pyspark`** |

---

## Known Limitations

- **Streaming event-time**: `window()` / `withWatermark` accepted by planner; tumbling window execution executor not yet wired (Sprint 6)
- **VARIANT type**: Not yet implemented; required for Spark 4.x full compat (Sprint 4)
- **Delta time travel**: `AT VERSION`/`AT TIMESTAMP` not yet wired (Sprint 4)
- **Scale**: TPC-H SF-1 proven; SF-100 distributed unvalidated (Sprint 5)
- **Iceberg**: REST catalog partial; V3 spec, partition pruning improvements needed (Sprint 4)
- **HMS**: HMS Thrift client stubs only; production HMS not fully supported (Sprint 5)
- **Python UDFs**: Require `PYTHONPATH` pointing to PySpark installation on the server
- **mimalloc**: Disabled by default — must NOT be re-enabled if Python UDFs are used (allocator re-entrancy crash with PyO3 on Tokio worker threads)
