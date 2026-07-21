# Zelox scale testing on AWS EKS — minimal cost, full teardown

Production-shaped distributed benchmark on real EKS, designed for **~$1–2 total**
and **guaranteed $0 after**. Default target: **full ClickBench (100M rows)
distributed** — the benchmark LakeSail publishes — with TPC-H SF-100 as an
extension. Region: `ap-south-1`.

## Cost model (ap-south-1, approximate)
| Item | Rate | A ~1.5 hr run |
|---|---|---|
| EKS control plane | $0.10/hr (fixed) | ~$0.15 |
| 3× Graviton spot (r7g/m7g.2xlarge) | ~$0.13/hr ea ≈ $0.40/hr | ~$0.60 |
| EBS gp3 (3×80 GB, ~1.5 hr) | ~$0.04/hr | ~$0.06 |
| S3 (≈14 GB ClickBench, hours) | ~$0.01 | ~$0.01 |
| ECR (small) | ~free | ~$0 |
| **No NAT gateway** (disabled in cluster.yaml) | $0 | $0 |
| **Total** | | **~$1** |

## Step 0 — guardrails (once)
```bash
aws configure            # IAM user access key/secret; region ap-south-1
aws sts get-caller-identity   # confirm
# Billing → Budgets → $5 monthly, alert 80%  (console; 2 min)
export REGION=ap-south-1
export ACCT=$(aws sts get-caller-identity --query Account --output text)
export ECR="$ACCT.dkr.ecr.$REGION.amazonaws.com"
export BUCKET="zelox-scale-$ACCT"
```

## Step 1 — push the arm64 image to ECR (~3 min)
```bash
aws ecr create-repository --repository-name zelox --region $REGION >/dev/null 2>&1 || true
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ECR
docker tag zelox:latest $ECR/zelox:latest        # zelox:latest already built (arm64)
docker push $ECR/zelox:latest
```

## Step 2 — create the cluster (~15–20 min; the long pole)
```bash
eksctl create cluster -f k8s/eks/cluster.yaml      # Graviton spot, no NAT
kubectl get nodes                                  # 3 arm64 nodes Ready
```

## Step 3 — load ClickBench data into S3 (~15 min, in-region)
A one-shot loader Job copies the public ClickHouse `hits` parquet (100 files,
~14 GB) into your bucket. Nodes have S3 access via the instance role.
```bash
aws s3 mb s3://$BUCKET --region $REGION
# loader job: see k8s/eks/clickbench-loader.yaml (curl public hits -> aws s3 cp)
sed "s#__BUCKET__#$BUCKET#g; s#__REGION__#$REGION#g" k8s/eks/clickbench-loader.yaml | kubectl apply -f -
kubectl wait --for=condition=complete job/clickbench-loader -n zelox --timeout=1800s
```

## Step 4 — deploy Zelox (kubernetes-cluster mode, ECR image)
```bash
# Point both the deployment image and the worker-pod-template image at ECR.
sed "s#image: zelox:latest#image: $ECR/zelox:latest#g; \
     s#value: zelox:latest#value: $ECR/zelox:latest#g" k8s/sail.yaml \
  | kubectl apply -f -
kubectl rollout status deployment/zelox-spark-server -n zelox --timeout=300s
kubectl port-forward -n zelox svc/zelox-spark-server 50051:50051 &
```

## Step 5 — run the distributed benchmark
```bash
# Full ClickBench (100M rows) from S3 — driver spawns worker pods across nodes.
SPARK_REMOTE=sc://localhost:50051 CLICKBENCH_DATA=s3://$BUCKET/clickbench \
  .venvs/smoke/bin/python scripts/clickbench.py | tee /tmp/clickbench_eks.log
```
Extension — **TPC-H SF-100**: bigger nodes/data; generate to `s3://$BUCKET/tpch`
via a DuckDB loader Job, then `TPCH_DATA_DIR=s3://$BUCKET/tpch TPCH_SKIP_GENERATE=1
TPCH_SF=100 scripts/tpch_distributed.py`.

## Step 6 — TEAR DOWN EVERYTHING (do immediately after) ⚠️
```bash
kill %1 2>/dev/null                     # stop port-forward
scripts/aws_eks_teardown.sh zelox-scale $REGION $BUCKET zelox
```
The script deletes the cluster (one CFN stack), ECR repo, S3 bucket, any leftover
eksctl stacks, then **verifies** no EKS/EC2/NAT/ELB/EBS/EIP remain. Confirm all
verify lines are empty/none → **$0 ongoing**. Double-check the EKS and EC2 consoles.
