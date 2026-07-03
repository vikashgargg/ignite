#!/usr/bin/env bash
# EKS like-for-like validation of the aligned checkpoint-barrier crash-EO fix
# (docs/design/distributed-eo-coordinator-wiring.md §4e). Runs the CONTINUOUS (realtime N-reader)
# windowed-agg -> Parquet-on-S3 at 16 partitions on real EKS, crashes the server mid-run, resumes from
# the S3 checkpoint, and asserts EXACTLY-ONCE (dup=0 AND clean-run == crash-run). This exercises the exact
# path (16 readers -> keyed StreamExchange -> WindowAccum -> aligned barrier -> sink) that dropped 15/16
# checkpoint barriers before the fix. Assumes cluster UP + image :aligned-eo in ECR.
# Usage: scripts/eks_continuous_eo.sh [N] [TAG]
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${1:-30000000}"; TAG="${2:-aligned-eo}"; REGION=ap-south-1; NS=stream
BUCKET="vajra-conteo-$(date +%s)"
ECR="$(aws ecr describe-repositories --region $REGION --repository-name vajra --query 'repositories[0].repositoryUri' --output text)"; REG="${ECR%/vajra}"
kk() { kubectl -n "$NS" "$@"; }
wait_ready() { kk wait --for=condition=available --timeout=300s deployment/"$1"; }

echo "==== [0] S3 bucket s3://$BUCKET ===="
aws s3 mb "s3://$BUCKET" --region "$REGION" >/dev/null && echo "bucket created"
cleanup() { echo "== emptying+deleting s3://$BUCKET =="; aws s3 rb "s3://$BUCKET" --force >/dev/null 2>&1; }
trap cleanup EXIT

echo "==== [1] Kafka + produce $N (16 partitions) ===="
kk apply -f k8s/stream/kafka.yaml >/dev/null 2>&1 || kubectl apply -f k8s/stream/kafka.yaml
wait_ready kafka
kk delete job producer --ignore-not-found >/dev/null 2>&1
sed "s|N_EVENTS\", value: \"[0-9]*\"|N_EVENTS\", value: \"$N\"|" k8s/stream/producer-job.yaml | kk apply -f -
kk wait --for=condition=complete --timeout=1800s job/producer; kk logs job/producer | grep PRODUCED

echo "==== [2] Vajra ($TAG) + client ===="
sed -e "s|__ECR__|$REG|g" -e "s|vajra:eo-multipart|vajra:$TAG|g" k8s/stream/vajra-stream.yaml | kk apply -f -
kk patch deploy vajra-stream --type merge -p '{"spec":{"strategy":{"rollingUpdate":{"maxSurge":0,"maxUnavailable":1}}}}' >/dev/null
kk set env deploy/vajra-stream AWS_REGION="$REGION" >/dev/null
wait_ready vajra-stream
kk apply -f k8s/stream/vajra-client.yaml
kk wait --for=condition=ready --timeout=300s pod/vajra-client
until kk logs vajra-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
kk cp scripts/stream_windowed_agg_continuous.py vajra-client:/tmp/wcont.py

SR="sc://vajra-stream.$NS.svc.cluster.local:50051"
run_cont() { # $1=out-subdir $2=run_secs
  kk exec vajra-client -- sh -c \
    "SPARK_REMOTE=$SR BOOT=kafka.$NS.svc.cluster.local:9092 TOPIC=events N_EVENTS=$N OUT=s3://$BUCKET/$1 CK=s3://$BUCKET/$1_ck RUN_SECS=$2 python3 /tmp/wcont.py" 2>&1
}
verify() { # $1=out-subdir -> P1_VERIFY rows/distinct/sum/dup
  kk exec vajra-client -- sh -c "SPARK_REMOTE=$SR python3 - <<PY
from pyspark.sql import SparkSession, functions as F
s=SparkSession.builder.remote('$SR').getOrCreate()
d=s.read.parquet('s3://$BUCKET/$1')
n=d.count(); distinct=d.select('window','k').distinct().count(); tot=d.agg(F.sum('count')).collect()[0][0]
print(f'P1_VERIFY rows={n} distinct_window_key={distinct} sum_count={tot} dup={n-distinct}')
PY" 2>&1 | grep -a P1_VERIFY
}

echo "==== [P1a] CONTINUOUS CLEAN run -> S3 ===="
run_cont conteo_clean 90 | grep -aoE 'VAJRA_WAGG.*' || true
CLEAN=$(verify conteo_clean); echo "$CLEAN"

echo "==== [P1b] CONTINUOUS EO-under-CRASH -> S3 ===="
run_cont conteo_crash 90 >/tmp/conteo_b.log 2>&1 &
RUNPID=$!
sleep 25
echo "== CHAOS: kill -9 vajra-stream server (mid continuous run) =="
kk delete pod -l app=vajra-stream --grace-period=0 --force >/dev/null 2>&1
wait $RUNPID 2>/dev/null; echo "(first attempt died with the server)"
wait_ready vajra-stream; until kk logs vajra-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
echo "== restart: resume continuous from S3 checkpoint =="
run_cont conteo_crash 90 | grep -aoE 'VAJRA_WAGG.*' || true
CRASH=$(verify conteo_crash); echo "$CRASH"

echo ""; echo "######## CONTINUOUS CRASH-EO RESULT (16-part, S3) ########"
echo "clean: $CLEAN"
echo "crash: $CRASH"
cdup=$(echo "$CRASH" | grep -aoE 'dup=[0-9-]+' | cut -d= -f2)
csum=$(echo "$CRASH" | grep -aoE 'sum_count=[0-9]+' | cut -d= -f2)
asum=$(echo "$CLEAN" | grep -aoE 'sum_count=[0-9]+' | cut -d= -f2)
if [ "${cdup:-x}" = "0" ] && [ -n "${csum:-}" ] && [ "${csum:-}" = "${asum:-}" ]; then
  echo "CONTINUOUS_CRASH_EO PASS (dup=0 AND crash sum == clean sum == $csum)"
else
  echo "CONTINUOUS_CRASH_EO FAIL (dup=$cdup crash_sum=$csum clean_sum=$asum)"
fi
echo "Teardown: eksctl delete cluster --name vajra-stream-ht --region $REGION --force --wait (NEVER interrupt)"
