# Vajra — Road to a True Production Spark Replacement

The honest checklist of what it takes to call Vajra a **production-grade drop-in
Spark replacement** and cut a **1.0 GA**. Capability/correctness/medium-scale perf
are already proven (see the matrix below); the open work is **security, reliability
under production conditions, and release hygiene**. Every item has a **measurable
acceptance criterion** — "done" means the criterion is met and published, not
"implemented."

Status legend: ✅ done · 🟡 partial · ⬜ not started.

---

## 0. Definition of "True Spark Replacement / GA"
A user runs `pip install vajra-pyspark`, points existing PySpark at Vajra, and it
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
| Same-box Spark ClickBench reference | ⬜ | run Spark on the same box → full 3-way (Vajra/LakeSail-published/Spark) |
| Distributed TPC-H SF-100 < 60 s | ⬜ | 10-node K8s, 22/22, total < 60 s |

## 3. Security & Hardening  — **biggest gap; features exist, NOT audited**
> Today Vajra has JWT bearer auth + mTLS as *features*, tested for functionality.
> There is **no penetration test, no security audit, no dependency-CVE gate, no
> fuzzing**. This is the #1 thing blocking an honest "production-ready" claim.

| Item | Status | Acceptance criterion |
|---|---|---|
| Dependency CVE gate | ⬜ | `cargo audit` + `cargo deny` in CI, zero unwaived advisories |
| Threat model | ⬜ | documented threat model for the Spark Connect / gRPC surface |
| SQL parser / Connect fuzzing | ⬜ | fuzz harness runs in CI; no panics/UB on malformed input |
| Auth/TLS adversarial test | ⬜ | verified: no auth bypass, token forgery, downgrade, or weak-cipher accept |
| Resource-exhaustion / DoS limits | ⬜ | per-query memory/time limits + connection caps; hostile query can't OOM the server |
| Penetration test | ⬜ | a real pentest (internal or third-party) with findings triaged + fixed |
| Secrets handling | 🟡 | no secrets in logs; creds via env/secret store only — audit + document |
| `SECURITY.md` + disclosure policy | ⬜ | published vulnerability-reporting process |

## 4. Reliability & Endurance  — **unproven under production conditions**
| Item | Status | Acceptance criterion |
|---|---|---|
| Kafka → Delta 24 h soak | ⬜ | runs 24 h, no OOM/restart/leak; lag stays bounded (DoD item) |
| Concurrency / multi-tenant load | ⬜ | N concurrent clients sustained; latency + correctness hold, no deadlock |
| Failover / chaos | ⬜ | kill a worker and the scheduler mid-job → job completes or fails cleanly (HA) |
| Memory stability over time | ⬜ | no unbounded growth across a long mixed workload (RSS flat) |
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
| `pip install vajra-pyspark` smoke | ⬜ | published to PyPI; the DoD one-liner works on a clean machine (DoD item) |
| Multi-arch release binaries | 🟡 | macOS arm64 + Linux x86_64 ship; add Linux arm64 (build-from-source only today) |
| Version / API stability policy | ⬜ | move off `v0.6.0-alpha`; semver policy + documented stability guarantees |
| Upgrade / compat matrix | ⬜ | supported Spark-client versions + Vajra upgrade path documented |
| Full CI lane green end-to-end | 🟡 | clippy ✅ + differential ✅; get fmt/test/build/scorecard/k8s/macos all green |

## 7. Documentation & Support
| Item | Status | Acceptance criterion |
|---|---|---|
| Migration guide (Spark → Vajra) | ⬜ | "point your code here + known differences" guide |
| Known-limitations page | 🟡 | exists in roadmap; promote to user-facing (PYTHONPATH, mimalloc, HMS stubs) |
| Deployment guides | 🟡 | K8s/Helm + Apple Container exist; add a hardened-prod reference deployment |

---

## Path to GA (gating)
- **GA = every P0 below is ✅ and published.**
- **P0 (blockers):** §3 security pass (CVE gate + threat model + fuzz + auth/DoS + pentest),
  §4 Kafka→Delta 24 h soak + concurrency + one failover test, §1 `differential-spark`
  required, §6 `vajra-pyspark` PyPI smoke + full CI green + drop the `-alpha`.
- **P1 (credibility):** §2 phase4 re-confirm + same-box Spark ClickBench + distributed
  SF-100 < 60 s; §1 Spark 4.x reference + TPC-DS + ≥97% suite.
- **P2 (polish → 1.0-rc):** §5 SLOs/tracing/runbooks; §6 arm64 Linux + semver; §7 docs.

## Recommended focus (next 2–3 weeks)
Capability and speed are *already_ proven — the credibility gap is now **"is it safe
and will it survive production?"** So:
1. **Security audit (start now, no cloud needed):** `cargo audit` + `cargo deny` in CI,
   scan auth/TLS paths, add a parser fuzz target, write `SECURITY.md` + a threat model.
2. **Reliability:** stand up the Kafka→Delta 24 h soak and a concurrency load test; add
   one worker/scheduler-kill failover test.
3. **Lock the gates (cheap, high trust):** make `differential-spark` required, get the
   full CI lane green, publish + smoke-test `vajra-pyspark` on PyPI.
4. **Quick win:** the phase4 ClickBench re-confirm (~$1, ~1 h) to prove no perf
   regression since the released binary.

Everything above #1–#3 is what turns "fast and correct in benchmarks" into
"trustworthy in production." That, plus dropping the `-alpha`, is the GA line.
