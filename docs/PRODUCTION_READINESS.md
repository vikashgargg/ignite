# Zelox — Road to a True Production Spark Replacement

The honest checklist of what it takes to call Zelox a **production-grade drop-in
Spark replacement** and cut a **1.0 GA**. Capability/correctness/medium-scale perf
are already proven (see the matrix below); the open work is **security, reliability
under production conditions, and release hygiene**. Every item has a **measurable
acceptance criterion** — "done" means the criterion is met and published, not
"implemented."

Status legend: ✅ done · 🟡 partial · ⬜ not started.

---

## 0. Definition of "True Spark Replacement / GA"
A user runs `pip install zelox-pyspark`, points existing PySpark at Zelox, and it
works — **correctly, fast, safely, and reliably, for days, under load**. Concretely,
all P0 items below are ✅ and published.

---

## 1. Correctness & Compatibility  — **strong, nearly done**
| Item | Status | Acceptance criterion |
|---|---|---|
| 105-test scorecard, all 4 modes | ✅ | 105/105 on local, local-cluster, Apple Container, K8s |
| Differential byte-exact vs Spark | ✅ | 124/124 byte-for-byte; CI job fails on any divergence |
| Official Apache Spark test suite | 🟡 | currently 95.01% (2492/2623); **target ≥ 97%** + published breakdown |
| `differential-spark` a **required** check | ⬜ | branch-protection on `main`; no merge can diverge from Spark |
| Spark **4.x** reference (not just 3.5.3) | ⬜ | differential + benchmarks also run vs Spark 4.x |
| TPC-DS (broad query surface) | ⬜ | TPC-DS SF-1 + SF-100 run, pass-rate + timings published |

## 2. Performance & Scale  — **proven small→100GB; parity vs LakeSail**
| Item | Status | Acceptance criterion |
|---|---|---|
| TPC-H SF-1 vs Spark (warm) | ✅ | 1.78 s vs 63.46 s (~36×), 22/22 |
| TPC-H SF-100 vs Spark (time+mem) | ✅ | 347 s / 51.7 GiB vs 1099 s / 115 GiB (~3.2×, ~2.2× less RAM) |
| ClickBench 100M distributed (EKS) | ✅ | 43/43, 377.9 s, S3 + real K8s |
| ClickBench parity vs LakeSail | ✅ | 60.11 s vs 65.50 s (0.92×) on identical c6a.4xlarge harness |
| **Re-confirm on current `phase4` build** | ⬜ | rebuild from branch, ClickBench within ±10% of the 60.11 s release number |
| Same-box Spark ClickBench reference | ⬜ | run Spark on the same box → full 3-way (Zelox/LakeSail-published/Spark) |
| Distributed TPC-H SF-100 < 60 s | ⬜ | 10-node K8s, 22/22, total < 60 s |
| Large-state (high key cardinality) | ❌ | **BUG found 2026-06-22** (`scripts/state_scale_stress.py`): streaming windowed-agg silently caps at **65536 distinct keys** (drops the rest); batch groupBy is correct at 200k. P0 correctness gap vs Flink (handles billions). Also F5: state is in-memory (no spill) ⇒ very large state OOMs. |
| Realtime streaming latency (p50/p99) | 🟡 | `scripts/stream_latency.sh` — produce→Zelox realtime Kafka→Kafka→visible per-record. Smoke 2026-06-22 (**debug** build): p50 25 ms / p99 137 ms / max 142 ms, n=440k. Release build + Flink side-by-side (the "beats Flink tail-latency" claim) pending. |

## 3. Security & Hardening  — **first audit pass done; pentest still outstanding**
> First-pass internal review done 2026-06-06 ([docs/THREAT_MODEL.md](THREAT_MODEL.md)):
> CVE gate added + 4 dependency vulns fixed, `SECURITY.md` published, and 6 code
> findings logged (F1 fixed). Still missing for GA: a real **penetration test**,
> **fuzzing**, and the **resource/DoS limits**.

| Item | Status | Acceptance criterion |
|---|---|---|
| Dependency CVE gate | ✅ | `cargo audit` + `cargo deny` in CI (`.github/workflows/security.yml`, `deny.toml`); **0 vulnerabilities** (fixed 4 in `astral-tokio-tar` via testcontainers 0.27) |
| Threat model | ✅ | [docs/THREAT_MODEL.md](THREAT_MODEL.md) — assets, trust boundaries, attacker model, findings |
| `SECURITY.md` + disclosure policy | ✅ | [SECURITY.md](../SECURITY.md) — private reporting + hardened-deployment guidance |
| Constant-time token compare (F1) | ✅ | timing side-channel removed (`subtle::ConstantTimeEq`) |
| Web UI default-localhost + token⇒TLS (F3/F4) | ✅ | Web UI defaults to `127.0.0.1` (`UiConfig`); server refuses to start with a token but no TLS |
| Reflection off in prod (F2) | ✅ | gRPC reflection disabled automatically when an auth token is configured |
| SQL parser / Connect fuzzing | ⬜ | fuzz harness runs in CI; no panics/UB on malformed input |
| Auth/TLS adversarial test | ⬜ | verified: no auth bypass, token forgery, downgrade, or weak-cipher accept |
| Resource-exhaustion / DoS limits (F6) | 🟡→✅ | inbound msg cap + per-connection stream/concurrency caps added; bounded memory pool available (recommend in prod). Per-query wall-time budget still a follow-up knob |
| Penetration test | 🟡 | first **internal** pass done (grpcurl/curl/nc — reflection gating, TLS enforcement, malformed-input, Web UI scope all verified; THREAT_MODEL.md). Third-party pentest + HTTP/2-abuse/load still required |
| Secrets handling | 🟡 | no secrets in logs; creds via env/secret store only — audit + document |

## 4. Reliability & Endurance  — **unproven under production conditions**
| Item | Status | Acceptance criterion |
|---|---|---|
| Kafka → sink soak (24 h) | 🟡 | `scripts/stream_soak_chaos.sh` — **smoke PASSED 2026-06-22** (120s/20k-s: 2.36M rows EO, flat RSS); 24 h `SOAK=1` run still pending for the GA DoD. |
| Concurrency / multi-tenant load | ⬜ | N concurrent clients sustained; latency + correctness hold, no deadlock |
| Failover / chaos | ✅ | **PASSED 2026-06-22**: `stream_soak_chaos.sh` hard-kills (`kill -9`) the server mid-stream + restarts → durable output exactly-once across the crash (2,360,000 rows, **0 loss, 0 dup, contiguous**). |
| Memory stability over time | 🟡 | **smoke PASSED**: RSS median 127 MB / max 142 MB = flat (1.11× < 1.5) across crash/restart; flat over 24 h still pending. |
| Graceful shutdown + backpressure | 🟡 | in-flight queries drain on SIGTERM; slow consumers don't unbound buffers — verify |
| Crash recovery | 🟡 | streaming checkpoint recovery ✅; batch driver restart story documented |

## 5. Operability & Observability  — **basics exist, needs SLO maturity**
| Item | Status | Acceptance criterion |
|---|---|---|
| Metrics + dashboard | 🟡 | Prometheus + Grafana exist (Phase 2); define core SLIs + a golden dashboard |
| Alerting / SLOs | ⬜ | documented SLOs (availability, query latency) + example alert rules |
| Distributed tracing | ⬜ | OpenTelemetry spans across driver→workers for a query |
| Structured logging | 🟡 | leveled logs exist; ensure JSON + correlation IDs, no PII/secrets |
| Runbooks | ⬜ | on-call runbooks: OOM, stuck query, worker loss, checkpoint corruption |

## 6. Release, Packaging & API Stability  — **still alpha**
| Item | Status | Acceptance criterion |
|---|---|---|
| `pip install zelox-pyspark` smoke | ⬜ | published to PyPI; the DoD one-liner works on a clean machine (DoD item) |
| Multi-arch release binaries | 🟡 | macOS arm64 + Linux x86_64 ship; add Linux arm64 (build-from-source only today) |
| Version / API stability policy | ⬜ | move off `v0.6.0-alpha`; semver policy + documented stability guarantees |
| Upgrade / compat matrix | ⬜ | supported Spark-client versions + Zelox upgrade path documented |
| Full CI lane green end-to-end | 🟡 | clippy ✅ + differential ✅; get fmt/test/build/scorecard/k8s/macos all green |

## 7. Documentation & Support
| Item | Status | Acceptance criterion |
|---|---|---|
| Migration guide (Spark → Zelox) | ⬜ | "point your code here + known differences" guide |
| Known-limitations page | 🟡 | exists in roadmap; promote to user-facing (PYTHONPATH, mimalloc, HMS stubs) |
| Deployment guides | 🟡 | K8s/Helm + Apple Container exist; add a hardened-prod reference deployment |

---

## Path to GA (gating)
- **GA = every P0 below is ✅ and published.**
- **P0 (blockers):** §3 security pass (CVE gate + threat model + fuzz + auth/DoS + pentest),
  §4 Kafka→Delta 24 h soak + concurrency + one failover test, §1 `differential-spark`
  required, §6 `zelox-pyspark` PyPI smoke + full CI green + drop the `-alpha`.
- **P1 (credibility):** §2 phase4 re-confirm + same-box Spark ClickBench + distributed
  SF-100 < 60 s; §1 Spark 4.x reference + TPC-DS + ≥97% suite.
- **P2 (polish → 1.0-rc):** §5 SLOs/tracing/runbooks; §6 arm64 Linux + semver; §7 docs.

## Streaming ("master of streaming" — the LakeSail differentiator)
Full capability audit + prioritized roadmap: [STREAMING.md](STREAMING.md). Summary:
event-time window aggregation, watermarks, dedup, Kafka source, foreachBatch/memory
sinks, and checkpoint+recovery all **work**; the open P0s are **stateful
stream–stream joins** (today per-micro-batch → silently incomplete), **triggers**
(`availableNow`/`once` currently ignored), and **explicit output modes**. This track
doubles as the reliability evidence (the Kafka→Delta 24 h soak in §4).

## Recommended focus (next 2–3 weeks)
Capability and speed are *already_ proven — the credibility gap is now **"is it safe
and will it survive production?"** So:
1. **Security audit (start now, no cloud needed):** `cargo audit` + `cargo deny` in CI,
   scan auth/TLS paths, add a parser fuzz target, write `SECURITY.md` + a threat model.
2. **Reliability:** stand up the Kafka→Delta 24 h soak and a concurrency load test; add
   one worker/scheduler-kill failover test.
3. **Lock the gates (cheap, high trust):** make `differential-spark` required, get the
   full CI lane green, publish + smoke-test `zelox-pyspark` on PyPI.
4. **Quick win:** the phase4 ClickBench re-confirm (~$1, ~1 h) to prove no perf
   regression since the released binary.

Everything above #1–#3 is what turns "fast and correct in benchmarks" into
"trustworthy in production." That, plus dropping the `-alpha`, is the GA line.
