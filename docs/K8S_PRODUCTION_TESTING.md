# Validating Vajra as a Production Spark Replacement on Kubernetes

> How to prove Vajra is a drop-in Apache Spark replacement on a **real** Kubernetes
> cluster — at scale, with production storage, autoscaling, and HA — and how that
> goes beyond what LakeSail currently demonstrates.

This is the production validation runbook. A `kind` cluster on a laptop proves the
manifests are correct; **real** validation needs a multi-node cloud cluster with
object storage. Both paths are below.

---

## Why not just `kind` on a laptop?

`kind` (Kubernetes-in-Docker) runs every "node" as a container on one machine, so it
cannot exercise the things that matter in production:

- Real network shuffle between physically separate worker pods
- Object-store-backed shuffle spill (S3/GCS) instead of local disk
- Horizontal Pod Autoscaler reacting to real CPU/memory load
- Scheduler high-availability failover across nodes
- Node loss / pod eviction / rolling upgrade behaviour
- TB-scale data that exceeds a single machine's memory

Use `kind` as a **smoke test** (manifests apply, pods become ready, a query runs
end-to-end). Use a cloud cluster for the **real** validation.

---

## Architecture under test

```
            ┌──────────────────────────────────────────────┐
            │              Kubernetes cluster               │
            │                                                │
 PySpark    │   ┌────────────┐      ┌────────────────────┐  │
 client ────┼──▶│  vajra      │      │  HPA (2→N workers) │  │
 sc://      │   │  scheduler  │◀────▶│  scales on CPU/mem │  │
            │   │  (Deployment│      └────────────────────┘  │
            │   │   + Lease HA)│                              │
            │   └─────┬───────┘                              │
            │         │ Arrow Flight shuffle (gRPC)          │
            │   ┌─────▼──────┐  ┌──────────┐  ┌──────────┐  │
            │   │ worker pod │  │ worker   │  │ worker   │   │
            │   │            │  │ pod      │  │ pod  ... │   │
            │   └─────┬──────┘  └────┬─────┘  └────┬─────┘  │
            └─────────┼──────────────┼─────────────┼─────────┘
                      ▼              ▼             ▼
                 ┌─────────────────────────────────────┐
                 │   S3 / GCS  (data + shuffle spill)   │
                 └─────────────────────────────────────┘
```

Vajra ships all of this today (see `helm/vajra/`): scheduler + worker Deployments,
HPA, PodDisruptionBudget, RBAC, and Kubernetes Lease-based scheduler leader election.

---

## Path A — Cloud K8s production validation (the real test)

### 1. Provision a cluster

Cheapest reliable option is **AWS EKS with spot instances** (or GKE/AKS equivalents).

```sh
# EKS via eksctl — 3× m6i.2xlarge spot (8 vCPU / 32 GB each), autoscale to 10
eksctl create cluster \
  --name vajra-prod-test \
  --region us-east-1 \
  --node-type m6i.2xlarge \
  --nodes 3 --nodes-min 3 --nodes-max 10 \
  --spot \
  --with-oidc --managed
```

GKE equivalent:
```sh
gcloud container clusters create vajra-prod-test \
  --num-nodes 3 --machine-type e2-standard-8 \
  --enable-autoscaling --min-nodes 3 --max-nodes 10 \
  --region us-central1
```

### 2. Object storage for data + shuffle (production pattern)

```sh
aws s3 mb s3://vajra-prod-test-data
# Worker pods get S3 access via IRSA (IAM Roles for Service Accounts) — no static keys
eksctl create iamserviceaccount \
  --cluster vajra-prod-test --name vajra-worker --namespace vajra \
  --attach-policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess \
  --approve
```

### 3. Deploy Vajra with the Helm chart

```sh
helm install vajra ./helm/vajra \
  --namespace vajra --create-namespace \
  --set image.repository=<your-registry>/vajra \
  --set image.tag=v0.6.0-alpha \
  --set mode=kubernetes-cluster \
  --set scheduler.ha.enabled=true \
  --set worker.replicas=3 \
  --set worker.autoscaling.enabled=true \
  --set worker.autoscaling.maxReplicas=10 \
  --set serviceAccount.name=vajra-worker \
  --set storage.s3.bucket=vajra-prod-test-data
```

### 4. Run the production validation suite

```sh
kubectl port-forward -n vajra svc/vajra-spark-server 50051:50051 &

# (a) Full Spark compatibility — must be 105/105 in kubernetes-cluster mode
SPARK_REMOTE=sc://localhost:50051 python scripts/spark_compat_score.py

# (b) TPC-H SF-100 distributed across worker pods (the scale test)
SPARK_REMOTE=sc://localhost:50051 python scripts/tpch_distributed.py --scale-factor 100

# (c) TPC-DS subset for breadth
SPARK_REMOTE=sc://localhost:50051 python scripts/tpcds_score.py
```

### 5. Production-behaviour validation (what makes it a *real* Spark replacement)

| Test | Command | Pass criteria |
|---|---|---|
| **Autoscaling** | Submit a heavy job, watch `kubectl get hpa -n vajra -w` | Workers scale 3→N under load, back down after |
| **Scheduler HA** | `kubectl delete pod -n vajra -l role=scheduler` mid-query | Standby acquires the Lease, job completes |
| **Worker loss** | `kubectl delete pod -n vajra -l role=worker` mid-shuffle | Stage retried on another worker, correct result |
| **Graceful drain** | `kubectl drain <node>` | Pods reschedule, no data loss (SIGTERM handler) |
| **Rolling upgrade** | `helm upgrade` to a new tag | Zero failed queries during rollout (PDB holds quorum) |
| **24h endurance** | Kafka → Delta streaming job for 24h | No OOM, no restart, checkpoints advance |

### 6. Tear down

```sh
helm uninstall vajra -n vajra
eksctl delete cluster --name vajra-prod-test --region us-east-1
aws s3 rb s3://vajra-prod-test-data --force
```

---

## Path B — Local `kind` smoke test (manifests correctness)

For CI / pre-cloud confidence that the image + manifests are valid. This is what the
`k8s-scorecard` CI job runs on every push.

```sh
kind create cluster --name vajra
make container-build-k8s            # or: docker build -f docker/Dockerfile -t vajra:latest .
kind load docker-image vajra:latest --name vajra
kubectl apply -f k8s/sail.yaml
kubectl rollout status deployment/vajra-spark-server -n vajra --timeout=120s
kubectl port-forward -n vajra svc/vajra-spark-server 50051:50051 &
SPARK_REMOTE=sc://localhost:50051 python scripts/spark_compat_score.py   # expect 105/105
kind delete cluster --name vajra
```

> Note: building the image from source needs a builder with ≥ 8 GB RAM. On an
> 8 GB machine, build on CI (clean, 16 GB runners) or use a prebuilt image —
> see `docker/Dockerfile` low-memory build settings.

---

## Vajra vs LakeSail on Kubernetes

| Capability | LakeSail | **Vajra** |
|---|---|---|
| Helm chart | basic | **scheduler + workers + HPA + PDB + RBAC** |
| Horizontal Pod Autoscaler | ❌ | **✅ CPU/mem-based worker autoscaling** |
| Scheduler HA (leader election) | ❌ | **✅ Kubernetes Lease-based** |
| Graceful shutdown (SIGTERM) | partial | **✅ drains in-flight tasks** |
| PodDisruptionBudget | ❌ | **✅ quorum held during rollout** |
| Object-store shuffle (S3/GCS) | ✅ | **✅ via `object_store`** |
| Single-YAML quickstart | ❌ | **✅ `k8s/sail.yaml`** |
| Spark compat in k8s mode | ~95% | **100% (105/105)** |
| Distributed lambda HOFs + recursive CTE | partial | **✅ (Sprint 4.1 codec fix)** |

---

## CI coverage

The `k8s-scorecard` job in `.github/workflows/ignite-ci.yml` runs Path B (kind +
`kubernetes-cluster` mode + 105 scorecard) on every push to a clean 16 GB runner —
so the K8s deployment path is continuously validated regardless of local hardware.

For full cloud-scale validation (TPC-H SF-100, HA failover, autoscaling), run Path A
on a real cluster before each release milestone.
