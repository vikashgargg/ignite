# EKS benchmark infra runbook — surprise-free tri-engine runs (prod-grade, STANDING)

> **Purpose:** the operational companion to [three-tier-sdlc.md](three-tier-sdlc.md). That doc says *why*
> T1→T2→T3; this one is the **pre-flighted, gotcha-annotated procedure** for standing up the EKS cluster and
> running the Zelox-vs-Flink / Zelox-vs-Spark scorecard **without discovering infra bugs mid-run**. Every
> item below is a failure this project actually hit — encode it, don't re-learn it. Region: `ap-south-1`.
> Standing cost law: **tear to $0 when idle** (`scripts/aws_eks_teardown.sh <cluster>`).

## 0. Pre-flight checklist (run BEFORE `eksctl create` — all must be ✅)

1. **ECR repo exists** — teardown DELETES it by default, so recreate every time:
   `aws ecr create-repository --repository-name zelox --region ap-south-1` (idempotent-check first).
2. **Image built for the CURRENT tree + tag known** — build on a throwaway EC2, never a Mac (Mach-O ≠ Linux
   pod): `PROFILE=vajra-bench-ec2 scripts/eks_build_image.sh <TAG>`; verify `arch=arm64 os=linux`.
3. **Bench AWS resources still exist** (they keep their pre-rename names — infra, not product identity):
   instance profile `vajra-bench-ec2`, SG `vajra-build-sg` (`sg-043445d6492980581`), a public subnet.
   The `eks_build_image.sh` default `PROFILE` is `zelox-bench-ec2` (renamed) which does **not** exist →
   always pass `PROFILE=vajra-bench-ec2`.
4. **Image-tag substitution is robust** — the manifests carry placeholder tags (`__ECR__/zelox:t7jp`,
   `:bf6`); scripts must substitute **any** tag, not a hard-coded `eo-multipart`. Fixed form:
   `sed -E "s#__ECR__/zelox:[A-Za-z0-9._-]+#$REG/zelox:$TAG#g"`. A stale literal silently deploys a dead tag
   → **InvalidImageName / ImagePullBackOff** (caught free at T2 kind; commit `27d2e93e`).
5. **`tri_engine_scorecard.sh` does NOT forward `TAG`** to `eks_stream_headtohead.sh` → **export TAG** in the
   environment or the head-to-head defaults to a non-existent `realtime-fix` tag.

## 1. Which cluster for which workload (they are NOT interchangeable)

| Workload | Cluster config | Compute node | Why |
|---|---|---|---|
| **Head-to-head + P1/P4 S3 + batch** (single-node `zelox-stream.yaml`) | `k8s/stream/eks-stream-cluster.yaml` | **c7g.4xlarge** (16 vCPU) | `zelox-stream` and Flink TM each request **cpu:15 / mem:24-26Gi** → need a ≥16-vCPU node. On c7g.2xlarge (8 vCPU) the pod sits **Pending: Insufficient cpu** forever. |
| **Distributed exchange / coalescer A/B** (`zelox-stream-dist.yaml` = driver + N small workers) | `k8s/stream/eks-stream-cluster-dist.yaml` | 2× c7g.2xlarge | Driver + workers are small; distributed shuffle across nodes is the point. |
| **Batch TPC-DS/TPC-H SF100** | `k8s/eks/cluster-sf100.yaml` | fat node | Different topology; a **cluster swap** — batch and streaming can't share a cluster. |

**Do not** run the single-node head-to-head on the `-dist` cluster (this session's mistake: created `-dist`,
then the 15-CPU `zelox-stream` pod was unschedulable on the 8-vCPU nodes). Match the cluster to the workload.

## 2. Capacity gotcha — c7g.4xlarge is frequently unavailable in ap-south-1

**Symptom:** managed nodegroup stuck `CREATE_IN_PROGRESS` 20+ min, `nodegroup.resources.autoScalingGroups
= None`, **zero EC2 instances** launching (`describe-instances --filters tag:eks:nodegroup-name`), health
issues empty. That is **InsufficientInstanceCapacity**, not slowness. (Confirmed 2026-07-24: c7g.2xlarge had
capacity, c7g.4xlarge did not, all 3 AZs.)

**Fix / prevention:**
- Prefer **m7g.4xlarge** (16 vCPU / 64 GB, different capacity pool) — rarely shares c7g's shortage. Same
  Arrow-columnar/Graviton profile; the extra RAM is harmless.
- Or a **capacity-flexible managed nodegroup**: `--instance-types m7g.4xlarge,c6g.4xlarge,m6g.4xlarge,c7g.4xlarge`
  (all 16-vCPU arm64) so EKS grabs whatever pool has stock.
- Give the nodegroup **all cluster subnets** (multi-AZ) — a single-subnet nodegroup pins one AZ and one
  capacity pool.
- **Diagnose before waiting:** if `autoScalingGroups == None` and no instances after ~10 min → capacity;
  delete + retry a different type rather than waiting on CFN.

## 3. IAM gotcha — CLI-added nodegroups lack S3 access

The cluster config grants S3 via `managedNodeGroups[].iam.attachPolicyARNs: [… AmazonS3FullAccess]`. A
nodegroup added later with **`eksctl create nodegroup` (CLI) gets a fresh NodeInstanceRole WITHOUT S3** →
every S3 sink workload (P1 realtime→S3, P4 batch→S3) fails with access-denied. **After adding any CLI
nodegroup, attach S3:**
```
BIGROLE=$(aws eks describe-nodegroup --cluster-name <c> --nodegroup-name <ng> --region ap-south-1 \
  --query 'nodegroup.nodeRole' --output text | sed -E 's#.*/##')
aws iam attach-role-policy --role-name "$BIGROLE" --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
```

## 4. PySpark 4.2 realtime mode (Flink-parity streaming)

- Realtime = **`.trigger(realTime="<interval>")`** (Spark 4.2 `Trigger.RealTime`). The client pod must run
  **`pyspark-client==4.2.0`** (`k8s/stream/zelox-client.yaml`).
- Zelox wires it: `real_time_batch_duration` (commands.proto field 100) → `spec::StreamTrigger::RealTime`
  (`proto/plan.rs`) → **`StreamDriver::Realtime`** (`plan_executor.rs`), the SAME event-at-a-time engine as
  the legacy `.trigger(continuous=…)`. Both are the Flink-parity realtime path; Zelox's is **stateful**
  (windowed/agg/join) — a superset of 4.2's stateless-only RTM.
- The duration is a **commit/checkpoint interval** (min 5s per 4.2), NOT a latency target — records flow
  continuously between commits, so latency is unaffected by the interval.

## 5. The run sequence (one cluster, then $0) — check each result before the next

```
export TAG=<tag>            # e.g. rename42 — MUST be exported (scorecard doesn't forward it)
# 1. Streaming scorecard vs Flink 1.19 — throughput + peak RSS (bounded windowed-agg) + realtime latency
N=100000000 LAT_RATE=20000 LAT_DUR=60 scripts/tri_engine_scorecard.sh streaming
# 2. Realtime (trigger realTime) -> S3 EO: read back exact + kill-9 (dup=0, clean==crash)
scripts/eks_continuous_eo.sh 20000000 $TAG
# 3. Batch -> S3 vs Spark 3.5.3: count/sum match + write/read timing
scripts/eks_batch_s3.sh 100000000
# 4. teardown to $0 (deletes cluster + ECR)
scripts/aws_eks_teardown.sh zelox-stream-dist ap-south-1
```
**N=100M** is the established streaming baseline (Flink 1.19 = 8.8 s → 11.36M ev/s, 8.5 GiB); use it for
comparability. Correctness proofs (P1/P4 S3) are N-independent → 20M is enough there.

**Fair-comparison law (charter):** claim only measured head-to-head with **identical resources** to both
engines; flag path-dependence (memory is bounded-vs-continuous dependent). Throughput is compared in
**bounded** windowed-agg (how the Flink baseline was set); realtime `trigger(realTime)` is compared on
**latency**, where the no-GC tail wins.

## 6. Teardown — the $ gate that must never be interrupted

`scripts/aws_eks_teardown.sh <cluster> ap-south-1` deletes the eksctl cluster (VPC/nodegroups/EC2) **and the
ECR repo** (recreate before the next build — see §0.1). Also delete any per-run S3 buckets (the P1/P4 scripts
self-clean on exit, but verify: `aws s3 ls | grep -E 'zelox-p[14]|conteo'`). Confirm `$0`: no EKS cluster,
no running EC2, no orphaned ELB/EBS.
