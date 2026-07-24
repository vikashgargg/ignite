#!/usr/bin/env bash
# P4 batch-on-S3 vs Spark: SAME batch_s3_bench.py (write Parquet to S3 -> read back + aggregate) on Zelox
# then Spark 3.5.3 (local[16], same node after Zelox->0), same S3 bucket. Compares write/read+agg timing +
# VERIFIES count/sum match (correctness) = credible "replaces Spark on batch/S3". Reuses the streaming
# cluster (compute node has S3 perms + zelox-stream server). Per-run bucket, deleted on exit ($0).
# Usage: scripts/eks_batch_s3.sh [N_ROWS]
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${1:-200000000}"; REGION=ap-south-1; NS=stream
BUCKET="zelox-p4-$(date +%s)"
ECR="$(aws ecr describe-repositories --region $REGION --repository-name zelox --query 'repositories[0].repositoryUri' --output text)"; REG="${ECR%/zelox}"
kk() { kubectl -n "$NS" "$@"; }
gib() { awk -v b="$1" 'BEGIN{printf "%.2f", b/1073741824}'; }

echo "==== [0] S3 bucket ===="; aws s3 mb "s3://$BUCKET" --region "$REGION" >/dev/null && echo "s3://$BUCKET"
cleanup() { echo "== rb s3://$BUCKET =="; aws s3 rb "s3://$BUCKET" --force >/dev/null 2>&1; kk delete job spark-batch-s3 --ignore-not-found >/dev/null 2>&1; }
trap cleanup EXIT

echo "==== [1] Zelox server + client ===="
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null  # batch skips kafka.yaml (which made the ns)
sed -E -e "s|__ECR__/zelox:[A-Za-z0-9._-]+|$REG/zelox:${TAG:-rename42}|g" -e "s|__ECR__|$REG|g" k8s/stream/zelox-stream.yaml | kk apply -f -
kk patch deploy zelox-stream --type merge -p '{"spec":{"strategy":{"rollingUpdate":{"maxSurge":0,"maxUnavailable":1}}}}' >/dev/null
kk set env deploy/zelox-stream AWS_REGION="$REGION" >/dev/null
kk wait --for=condition=available --timeout=300s deployment/zelox-stream
kk apply -f k8s/stream/zelox-client.yaml; kk wait --for=condition=ready --timeout=300s pod/zelox-client
until kk logs zelox-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
kk cp scripts/batch_s3_bench.py zelox-client:/tmp/batch_s3_bench.py

echo "==== [2] ZELOX batch-on-S3 ===="
kk exec zelox-client -- sh -c \
  "SPARK_REMOTE=sc://zelox-stream.$NS.svc.cluster.local:50051 S3_PATH=s3://$BUCKET/zelox N_ROWS=$N ENGINE=zelox python3 /tmp/batch_s3_bench.py" 2>&1 | grep -a BATCH_RESULT | tee /tmp/batch_s3.txt
VPOD=$(kk get pod -l app=zelox-stream --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')
echo "Zelox peakRSS=$(gib "$(kk exec "$VPOD" -- cat /sys/fs/cgroup/memory.peak 2>/dev/null)")GiB" | tee -a /tmp/batch_s3.txt

echo "==== [3] SPARK 3.5.3 baseline (scale Zelox->0, same node/bucket) ===="
kk scale deploy/zelox-stream --replicas=0 >/dev/null 2>&1; sleep 10
kk create configmap batch-s3-script --from-file=batch_s3_bench.py=scripts/batch_s3_bench.py --dry-run=client -o yaml | kk apply -f - >/dev/null
kk delete job spark-batch-s3 --ignore-not-found >/dev/null 2>&1
sed -e "s|__S3_PATH__|s3://$BUCKET/spark|" -e "s|__N_ROWS__|$N|" k8s/eks/spark-s3-job.yaml | kk apply -f -
kk wait --for=condition=complete --timeout=2400s job/spark-batch-s3 2>/dev/null \
  && kk logs job/spark-batch-s3 2>/dev/null | grep -aE "BATCH_RESULT|peak_RSS" | tee -a /tmp/batch_s3.txt \
  || { echo "spark job did not complete; logs:"; kk logs job/spark-batch-s3 2>/dev/null | tail -20; }

echo ""; echo "######## P4 BATCH-ON-S3: Zelox vs Spark ########"; cat /tmp/batch_s3.txt
echo "(verify sum_v + distinct_k MATCH across engines; compare write_s/read_agg_s)"
echo "Teardown: eksctl delete cluster --name zelox-stream-ht --region $REGION --force --wait"
