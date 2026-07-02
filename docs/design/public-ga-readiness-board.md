# Vajra — Public GA Prod-Grade Readiness Board (SDLC / Jira-style)

> Goal: make Vajra a **fully public, prod-grade drop-in replacement for Spark (batch) + Flink
> (streaming)** — anyone can `docker pull` / `helm install` / `pip install` and run it, with the
> repo, CI/CD, release, container, observability, security and governance at the bar tech giants
> (Google/Apple) and top OSS projects (Apache DataFusion, Polars, ruff/uv, Vector, ClickHouse) hold.
>
> This board is the **distribution + repo prod-grade** track. The **engine** gap roadmaps already
> exist and are NOT duplicated here: [PROD_GRADE_ROADMAP.md](../PROD_GRADE_ROADMAP.md) (streaming
> latency, large-state, recovery, adaptive exec) and [PRODUCTION_READINESS.md](../PRODUCTION_READINESS.md)
> (correctness/perf/security/reliability GA gates). This board cross-links them as **E9**.
>
> Convention: each ticket has **ID · Priority (P0 blocks public launch / P1 GA / P2 post-GA) ·
> Status (TODO / DOING / DONE / EXISTS-partial) · Acceptance criteria · OSS reference** we model on.
> Update Status + link the commit the same turn work lands (cite-don't-re-derive).

---

## Status snapshot (audited 2026-07-02)

**Already present (good foundation):** LICENSE, CONTRIBUTING, CODE_OF_CONDUCT, SECURITY; Helm chart
(`helm/vajra`); 20+ CI workflows (build/lint/tests/security/gold-data, PR-gated); release.yml +
release-binary.yml + multi-platform-build.yml + release-notes; Dockerfiles (release/apple/quickstart/dev);
dependabot + codecov; clippy `-D warnings` lane green; 260+ streaming/json unit tests; `env_logger`
structured logging wired at every server entrypoint.

**Confirmed gaps (this board):** ✗ no workflow publishes a **public pullable image** (the #1
"let people try it" blocker) · ✗ CHANGELOG.md / NOTICE / MAINTAINERS.md / GOVERNANCE.md / CODEOWNERS ·
✗ issue + PR templates · ✗ container-image vulnerability scan (Trivy/grype) + SBOM + signing (cosign) ·
✗ dead-code / unused-logic sweep · ✗ Apple-container local+cluster documented runbook + periodic gate ·
partial: Helm chart not lint/CI-tested, no published chart.

---

## E1 — Public image + artifact distribution ("so people can pull & try") · **P0**

The crown jewel of "public prod-grade." Model: DataFusion/Polars/Vector publish multi-arch images to
**GHCR** (`ghcr.io/<org>/<repo>`) on tag, plus binaries via cargo-dist; images are signed + SBOM-attested.

| ID | Pri | Status | Ticket / Acceptance criteria | OSS ref |
|---|---|---|---|---|
| **VAJRA-D1** | P0 | TODO | **Publish multi-arch (arm64+amd64) image to GHCR on release tag.** `docker/build-push-action` + `setup-buildx` + QEMU; tags `ghcr.io/vikashgargg/ignite:{version, latest, sha}`; `packages: write`; SAME arm image used on EKS + Apple container. AC: `docker pull ghcr.io/vikashgargg/ignite:latest && docker run …` works from a clean machine for both arches. | Vector `publish.yml`, DataFusion release |
| **VAJRA-D2** | P0 | TODO | **README + docs "Run in 30s" with the pullable image** (`docker run … ghcr.io/…`), plus `helm install`. AC: a new user reaches a running Spark-Connect endpoint from copy-paste. | uv/ruff READMEs |
| **VAJRA-D3** | P1 | EXISTS-partial | Binary release via `release-binary.yml` — verify it attaches signed per-platform tarballs + `install.sh` checksum-verifies. AC: `curl install.sh | sh` pins + verifies a checksum. | cargo-dist |
| **VAJRA-D4** | P1 | TODO | **Publish Helm chart** (OCI to GHCR `helm push` or gh-pages index). AC: `helm install vajra oci://ghcr.io/…/charts/vajra`. | Bitnami, Grafana charts |
| **VAJRA-D5** | P2 | TODO | Verify `vajra-pyspark` PyPI publish is wired in release (wheel + sdist, abi3). | maturin publish |

## E2 — Supply-chain security & scanning · **P0/P1** (user ask: "do scans")

Model: SLSA provenance + cosign keyless signing + Trivy image scan + cargo-deny/audit (already have CVE gate).

| ID | Pri | Status | Ticket / Acceptance criteria | OSS ref |
|---|---|---|---|---|
| **VAJRA-S1** | P1 | EXISTS-partial | cargo-audit/deny CVE gate (0 vulns) — confirm it runs on PR + schedule, fails build on new CVE. | rustsec, `security.yml` |
| **VAJRA-S2** | P0 | TODO | **Trivy (or grype) scan of the published image** in the release pipeline; fail on HIGH/CRITICAL. AC: SARIF uploaded to code-scanning; release blocked on unfixed CRITICAL. | aquasecurity/trivy-action |
| **VAJRA-S3** | P1 | TODO | **cosign keyless image signing + SBOM (syft) attestation** on publish. AC: `cosign verify` passes; SBOM downloadable. | sigstore, SLSA-3 |
| **VAJRA-S4** | P1 | TODO | Enable GitHub CodeQL (Rust via `github/codeql-action`) + secret scanning + dependency review on PR. AC: CodeQL workflow green, PRs get dependency-review. | github/codeql-action |
| **VAJRA-S5** | P2 | TODO | Pin GitHub Actions to commit SHAs (not floating tags) + `permissions:` least-privilege per workflow. | OpenSSF Scorecard |
| **VAJRA-S6** | P2 | TODO | Add **OpenSSF Scorecard** workflow + badge; target ≥7. | ossf/scorecard-action |

## E3 — Code cleanliness / dead-code sweep · **P1** (user ask: "remove unwanted, clean logic not needed")

| ID | Pri | Status | Ticket / Acceptance criteria | OSS ref |
|---|---|---|---|---|
| **VAJRA-C1** | P1 | DONE | Debug scaffolding removed; all library logging → `log` facade (commit b980c19e). | — |
| **VAJRA-C2** | P1 | TODO | **Dead-code sweep**: `cargo +nightly udeps` (unused deps) + `RUSTFLAGS="-W dead_code"` review + clippy `--all-targets`; remove unused fns/structs/env-gates/feature flags left from experiments. AC: no `dead_code`/`unused` warnings; udeps clean; no orphan scripts. | rust-lang udeps |
| **VAJRA-C3** | P2 | TODO | Sweep experiment/one-off scripts in `scripts/` — keep the standing harness (correctness_gate, tri_engine, eks_*), archive/delete throwaways; document each kept script's purpose in CODEMAP. | — |
| **VAJRA-C4** | P2 | TODO | Enforce `cargo fmt --check` + `taplo` (TOML fmt) in CI if not already. | — |

## E4 — Container: Apple `container` (local + cluster) same-arm-image · **P1**

| ID | Pri | Status | Ticket / Acceptance criteria | OSS ref |
|---|---|---|---|---|
| **VAJRA-K1** | P1 | EXISTS-partial | Apple `container` **local** smoke gate (one container, P1/P2 + EO across `container kill`) using the SAME `ghcr.io` arm image as EKS. AC: scripted, one command, green. (Validated once 2026-06-16 — needs to be a repeatable script.) | [[project_apple_container]] |
| **VAJRA-K2** | P1 | TODO | Apple `container` **cluster** gate (scheduler + N workers on 192.168.64.x bridge, Kafka dual-listener) running P1–P5 distributed. AC: scripted, green, documented runbook. | — |
| **VAJRA-K3** | P2 | TODO | Doc: "Run Vajra on Apple container" runbook (build-env gotchas: builder VM 6cpu/4gb, opt-level=1 AWS-SDK OOM). | — |

## E5 — K8s / Helm prod-grade · **P1**

| ID | Pri | Status | Ticket / Acceptance criteria | OSS ref |
|---|---|---|---|---|
| **VAJRA-H1** | P1 | TODO | `helm lint` + `helm template` + `ct` (chart-testing) + kind install-test in CI. AC: chart CI green on PR. | helm/chart-testing-action |
| **VAJRA-H2** | P1 | TODO | Chart hardening: resource requests/limits, liveness/readiness probes, PodDisruptionBudget, HPA, securityContext (non-root, RO-rootfs), ServiceAccount+RBAC, NetworkPolicy. AC: passes `kubeconform` + `polaris`/`kube-score`. | Bitnami charts |
| **VAJRA-H3** | P2 | TODO | Values documented (`README.md` in chart via `helm-docs`); example prod values. | helm-docs |

## E6 — Observability (metrics/traces, not just logs) · **P1**

| ID | Pri | Status | Ticket / Acceptance criteria | OSS ref |
|---|---|---|---|---|
| **VAJRA-O1** | P1 | EXISTS-partial | Logs = DONE (structured `log`+env_logger). **Prometheus `/metrics`** for streaming operators (throughput, watermark lag, checkpoint duration, spill bytes, backpressure) — currently a roadmap gap. AC: `/metrics` scrapeable; Grafana dashboard shipped. | Flink metrics, sail-telemetry OTEL |
| **VAJRA-O2** | P2 | TODO | OTEL traces across stages + exemplars; SLO doc (availability, p99 latency). | OpenTelemetry |

## E7 — Testing / correctness at prod-grade · **P1**

| ID | Pri | Status | Ticket / Acceptance criteria | OSS ref |
|---|---|---|---|---|
| **VAJRA-T1** | P1 | EXISTS | 260+ streaming/json unit tests; differential harness vs real Spark (124 workloads byte-exact); per-partition watermark + spill covered. | — |
| **VAJRA-T2** | P1 | EXISTS-partial | **Standing streaming correctness gate** (`scripts/correctness_gate.sh`, adversarial C1–C7) — wire into CI (self-ensures Kafka); flip XFAIL→XPASS as fixes land. AC: runs in CI, gates prod-grade claims. | Jepsen-style |
| **VAJRA-T3** | P2 | TODO | Soak/endurance (24h+) + chaos (random kill) gate; coverage report (codecov already present) with a floor. | — |

## E8 — Governance / legal / community · **P1** (public-project hygiene)

| ID | Pri | Status | Ticket / Acceptance criteria | OSS ref |
|---|---|---|---|---|
| **VAJRA-G1** | P0 | TODO | **NOTICE** file — Apache-2.0 attribution incl. the **LakeSail/Sail fork lineage** + DataFusion/Arrow. AC: legally correct attribution present. | Apache projects |
| **VAJRA-G2** | P1 | TODO | **CHANGELOG.md** (Keep a Changelog + SemVer), auto-updated by release-please or the existing release-notes script. AC: every release has an entry. | keepachangelog.com |
| **VAJRA-G3** | P1 | TODO | **CODEOWNERS** + **PR template** + **issue templates** (bug/feature/question, YAML forms). AC: new PR/issue shows the template. | github community standards |
| **VAJRA-G4** | P2 | TODO | MAINTAINERS.md + GOVERNANCE.md + support/roadmap in README; "Community Standards" 100% green. | CNCF projects |
| **VAJRA-G5** | P1 | TODO | README badges reflect reality (CI, release, license, image pull, coverage); honest claims (per the standing bar). | — |

## E9 — Engine feature-parity gaps (cross-link, not duplicated here)

The batch (Spark) side is strong; streaming (Flink) side is competitive with named gaps. Tracked in
[PROD_GRADE_ROADMAP.md](../PROD_GRADE_ROADMAP.md) §3 + [PRODUCTION_READINESS.md](../PRODUCTION_READINESS.md).
Highlights blocking a "full Flink replacement" claim: **streaming latency (P0)**, large-state backend
(P0), mid-job recovery time, adaptive batch execution, **streaming Iceberg sink** (next branch),
TPC-DS Q5/Q9 compat, throughput parse-fusion (VAJ-T7). **The streaming Iceberg sink is the next feature
branch** (`streaming/iceberg-sink`) — same checkpoint-coordinated EO substrate proven for Parquet-on-S3 (P1).

---

## Suggested execution order (fastest path to "public, pullable, prod-grade")

1. **E1 VAJRA-D1/D2** (public GHCR image + run-in-30s) + **E2 VAJRA-S2/S3** (scan+sign the image) — the
   "people can try it" milestone, all in the release pipeline.
2. **E8 VAJRA-G1/G2/G3** (NOTICE, CHANGELOG, CODEOWNERS/templates) — public-project hygiene, cheap local.
3. **E3 VAJRA-C2** (dead-code/udeps sweep) — clean tree before more eyes.
4. **E5 VAJRA-H1/H2** (Helm lint+harden) + **E4 VAJRA-K1** (Apple-container repeatable gate).
5. **E2 VAJRA-S4/S5/S6** (CodeQL, action pinning, Scorecard) + **E6 VAJRA-O1** (Prometheus metrics).
6. Then the **streaming Iceberg sink** feature branch (E9), and the deeper engine P0s (latency, large-state).

Everything local-testable is done + tested + committed to main before the Iceberg branch; cloud-gated
items (image e2e on both arches, EKS/Apple-container) are validated in their pipeline. Tear AWS to $0.
