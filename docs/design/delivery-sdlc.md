# Zelox delivery SDLC — how we ship (prod-grade, per [AIM](../AIM.md))

Follow this every task; don't reinvent. Grounded in [AIM.md](../AIM.md) + [prodgrade-practices](streaming-prodgrade-practices.md).
The bar: production-first, hyperscale, no patch work, synthesized-not-copied, "objectively better in
production or redesign". Skill: `.claude/skills/dist-streaming-test`.

## 1. Validation gates — T1 → T2 → T3 (EKS CONFIRMS, never DISCOVERS)
| Tier | Env | Cost | Proves | Tooling |
|---|---|---|---|---|
| **T1** | local process / local-cluster + MinIO | FREE | correctness + mechanism (local-cluster routes shuffle over Flight, so distributed path IS exercised) | `scripts/local_dist_coalesce_check.sh`, `correctness_gate.sh`, `inc_ckpt_gate.sh`, unit tests |
| **T2** | **kind** (real k8s in Docker) + MinIO | FREE | manifests, image pull, pod scheduling, cross-pod networking/Flight, S3 sink, Kafka, Flink-SQL | `scripts/kind_*`, `k8s/kind/*` |
| **T3** | **EKS** | $ (tear to $0) | at-scale NUMBERS only (throughput/latency/memory vs Flink/Spark) | `scripts/eks_*`, `k8s/stream/eks-*` |
- Determinism: streaming windowed COUNTS are timing-flaky under desktop load (`nm_dist_gate` flakes on
  MAIN too). Use MONOTONIC producer ts + `ZELOX_COMPLETE_ON_END=1`; check `main` before blaming a change.
- **DONE = T1+T2 green + (if a scale claim) T3 measured.** A fix isn't done until validated end-to-end.

## 2. Branching / git / CI
- Feature branch per axis/ticket (`<axis>/<ticket>`), NOT direct-on-main. Keep `main` green: `cargo test
  --workspace` + `cargo clippy --all-targets -D warnings` (workspace denies unwrap/expect/panic) BEFORE
  any commit. Small, validated commits; commit message ends `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Distributed-aware: a new physical-exec field must round-trip in `sail-execution/src/codec.rs` or be
  logged as a single-node-only gap. Merge to main when T1→T2→T3 green; update BOARD cell + link commit
  the SAME turn.
- CI (target): PR runs fmt + clippy(-D) + `cargo test --workspace` + the T1 gates; kind T2 gate on the
  streaming-touching paths. Release = image build (`docker/Dockerfile`) pushed to registry.

## 3. Packaging & deploy — all modes (Kubernetes-first per AIM)
- **Image:** `docker/Dockerfile` (release) / `.dbg` (symbols). Build fast on a throwaway arm64 EC2
  (`scripts/eks_build_image.sh TAG`, ~$0.10) → registry; a Mac-built binary is Mach-O (NOT Linux-pod
  runnable). ECR gotcha: teardown deletes the repo — `aws ecr create-repository` before each build.
- **kind (T2 local):** `scripts/kind_up.sh` + `k8s/kind/*` (kind-cluster.yaml = kafka + compute nodes;
  minio.yaml = S3). `docker pull <ECR>:TAG && docker tag ... zelox:TAG && kind load docker-image zelox:TAG`.
- **EKS (T3 cluster):** `k8s/stream/eks-stream-cluster*.yaml` (eksctl) + `k8s/stream/*` manifests; node
  instance role has S3 (object_store from_env). Tear to $0 after (no permission needed) — cluster + EC2 +
  S3 + ECR.
- **Apple-silicon container (both modes):** `docker/apple/` — validated local-cluster + EO-across-container-
  kill (memory `project_apple_container`); use for a fast local multi-worker check without kind.
- **Helm (target):** package driver+worker+kafka+minio/S3 as a chart for repeatable local/cluster deploy
  (values toggle mode: local-cluster vs kubernetes-cluster, image tag, S3 endpoint, ZELOX_* flags). Track
  in the GA-readiness board.

## 4. Dual testing — Spark (batch) AND Flink (streaming), like-for-like
Zelox is the UNIFIED engine, so it must beat BOTH — a modern columnar engine does batch AND streaming:
- **Batch vs Spark 3.5.x:** identical query + data → assert byte-identical output + throughput + peak RSS
  (`scripts/eks_batch_s3.sh`, P4 = 6.2× faster / 2.4× less mem). TPC-H, ClickBench.
- **Streaming vs Flink 1.19:** identical windowed-agg / passthrough → counts-exact + throughput + latency
  tail + memory + crash-EO dup=0 (`scripts/eks_*headtohead*`, `kind_latency_ht.sh`). Both engines measured
  in the SAME run before teardown (never claim from one side).
- Claim ONLY measured head-to-head; flag path-dependence. Inherit distilled facts from [REFERENCES.md] +
  [AIM.md] official sources; append new fetched facts to REFERENCES the same turn.

## 5. Board / sprint cadence
- [docs/BOARD.md] is the single kanban (axis → measured status vs Spark+Flink → epic/ticket → T-state →
  evidence). Each turn work lands: update the cell + link the commit. Each session: pick the next
  highest-value axis from the board's backlog (sprint = the active epic); record NEXT items so the plan
  survives context resets. "What's planned vs achieved" lives ONLY here (memory indexes it, doesn't dup).
