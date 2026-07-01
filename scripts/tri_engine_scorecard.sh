#!/usr/bin/env bash
# Tri-engine scorecard (docs/design/tri-engine-benchmark-matrix.md): fair, official-methodology
# head-to-head of Vajra vs the engines it replaces — Flink (streaming) + Spark (batch).
#
# DESIGN: this script MEASURES on an already-up cluster (kubeconfig pointed at it). Cluster
# create/teardown is a SEPARATE explicit step (the $ gate that must never be interrupted — see
# the eksctl-teardown lesson). Run per phase:
#
#   # Streaming (cluster = k8s/stream/eks-stream-cluster.yaml up; baseline Flink 1.19):
#   N=100000000 scripts/tri_engine_scorecard.sh streaming
#   # Batch (cluster = k8s/eks/cluster-sf100.yaml up; baseline Spark 3.5.3):
#   TPCDS_SF=10 TPCH_SF=10 scripts/tri_engine_scorecard.sh batch
#
# Official anchors (REFERENCES §7): streaming ≈ Nexmark q5/q6 windowed-agg (throughput ev/s) + latency
# p50/p99/p999 (lat_probe.py); batch = TPC-DS-99 power test (sequential response time + total wall) +
# TPC-H + peak RSS. Full Nexmark q0–q13 = dedicated follow-on.
set -uo pipefail
PHASE="${1:-help}"; NS="${NS:-stream}"; REGION="${REGION:-ap-south-1}"
N="${N:-100000000}"; LAT_RATE="${LAT_RATE:-20000}"; LAT_DUR="${LAT_DUR:-60}"
TPCDS_SF="${TPCDS_SF:-10}"; TPCH_SF="${TPCH_SF:-10}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
kk() { kubectl -n "$NS" "$@"; }
mask() { sed -E 's/[0-9]{12}/<ACCT>/g'; }
gib() { awk -v b="$1" 'BEGIN{printf "%.2f", b/1073741824}'; }

# ---------------------------------------------------------------------------
streaming_phase() {
  echo "######## STREAMING SCORECARD (vs Flink 1.19) ########"
  REG="$(aws ecr describe-repositories --region "$REGION" --repository-name vajra --query 'repositories[0].repositoryUri' --output text 2>/dev/null | sed 's|/vajra||')"
  # S1+S2 throughput + peak memory (reuses the validated head-to-head).
  echo "==== S1+S2 throughput/memory (N=$N) ===="
  N="$N" REGION="$REGION" bash scripts/eks_stream_headtohead.sh "$N" 2>&1 | mask \
    | grep -aiE "PRODUCED|FLINK wall|VAJRA_WAGG|VAJRA peak|Flink :|Vajra :" | tee /tmp/tri_stream.txt
  # WM_PROF per-stage (if image has it)
  kk logs deploy/vajra-stream 2>/dev/null | grep -aE "WM_PROF" | tail -1 | mask || true

  # S3 latency p50/p99/p999 — SAME lat_probe for both engines (raw Kafka->Kafka passthrough).
  echo "==== S3 latency (rate=$LAT_RATE/s dur=${LAT_DUR}s) ===="
  for t in lat_in lat_out; do
    kk exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
      --create --topic "$t" --partitions 16 --replication-factor 1 >/dev/null 2>&1 || true
  done
  local BOOT="kafka.$NS.svc.cluster.local:9092"
  kk cp scripts/lat_probe.py vajra-client:/tmp/lat_probe.py

  # --- Vajra latency: passthrough query (continuous) + probe ---
  echo "-- Vajra passthrough latency --"
  kk cp scripts/stream_latency_query.py vajra-client:/tmp/lat_q.py
  kk exec vajra-client -- sh -c \
    "SPARK_REMOTE=sc://vajra-stream.$NS.svc.cluster.local:50051 BOOT=$BOOT IN_TOPIC=lat_in OUT_TOPIC=lat_out CK=/data/lat_ck python3 /tmp/lat_q.py >/tmp/lq.log 2>&1 &" || true
  sleep 12
  kk exec vajra-client -- sh -c \
    "ENGINE=vajra BOOT=$BOOT IN_TOPIC=lat_in OUT_TOPIC=lat_out RATE=$LAT_RATE DURATION_S=$LAT_DUR python3 /tmp/lat_probe.py" 2>&1 | grep -a LATENCY_RESULT | tee -a /tmp/tri_stream.txt

  # --- Flink latency: continuous passthrough job + same probe ---
  echo "-- Flink passthrough latency --"
  kk apply -f k8s/stream/flink-session.yaml >/dev/null 2>&1; kk wait --for=condition=available --timeout=300s deployment/flink-jm deployment/flink-tm 2>/dev/null
  # submit the continuous job (detached) via the JM sql-client, then probe, then cancel.
  local JM; JM=$(kk get pod -l app=flink,component=jm -o jsonpath='{.items[0].metadata.name}')
  kk cp k8s/stream/flink-sql-latency.sql "$JM":/tmp/flink-sql-latency.sql
  kk exec "$JM" -- sh -c '/opt/flink/bin/sql-client.sh -f /tmp/flink-sql-latency.sql' >/tmp/flink_lat_submit.log 2>&1 &
  sleep 20
  kk exec vajra-client -- sh -c \
    "ENGINE=flink BOOT=$BOOT IN_TOPIC=lat_in OUT_TOPIC=lat_out RATE=$LAT_RATE DURATION_S=$LAT_DUR python3 /tmp/lat_probe.py" 2>&1 | grep -a LATENCY_RESULT | tee -a /tmp/tri_stream.txt
  kk delete -f k8s/stream/flink-session.yaml --ignore-not-found >/dev/null 2>&1

  echo "######## STREAMING SCORECARD TABLE ########"; cat /tmp/tri_stream.txt | mask
  echo "NOTE: full Nexmark q0–q13 = follow-on. Teardown: scripts/aws_eks_teardown.sh vajra-stream-ht $REGION"
}

# ---------------------------------------------------------------------------
batch_phase() {
  NS="${BATCH_NS:-vajra}"   # batch runs in the `vajra` namespace (k8s/eks/vajra-sf100.yaml)
  echo "######## BATCH SCORECARD (vs Spark 3.5.3), ns=$NS ########"
  echo "Official: TPC-DS-99 power test (sequential per-query + total wall) + TPC-H + peak RSS."
  : > /tmp/tri_batch.txt
  local REG; REG="$(aws ecr describe-repositories --region "$REGION" --repository-name vajra --query 'repositories[0].repositoryUri' --output text 2>/dev/null | sed 's|/vajra||')"
  # Deploy the batch Vajra server + client (vajra-sf100 = svc/deploy/app name).
  kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null 2>&1 || true
  sed "s|__ECR__|$REG|g" k8s/eks/vajra-sf100.yaml | kk apply -f -
  kk apply -f k8s/eks/vajra-client.yaml
  kk wait --for=condition=available --timeout=300s deployment/vajra-sf100 2>/dev/null
  kk wait --for=condition=ready --timeout=300s pod/vajra-client 2>/dev/null
  until kk logs vajra-client 2>/dev/null | grep -q CLIENT_READY; do sleep 5; done
  local VSVC="vajra-sf100.${NS}.svc.cluster.local:50051"
  # TPC-DS uses createDataFrame (Arrow over Connect = cross-pod safe: client generates, Vajra receives).
  # TPC-H writes parquet to a LOCAL dir then spark.read.parquet reads it SERVER-side -> that path is on
  # the client, not Vajra -> cross-pod FAIL. TPC-H needs shared storage (future); the clean official run
  # is TPC-DS-99. (Data is client-side pandas -> keep SF small enough for the client pod, SF-1 default.)
  for bench in "tpcds_score.py TPCDS_SF=$TPCDS_SF TPC-DS-99"; do
    set -- $bench; local SCRIPT="$1" SFENV="$2" NAME="$3"
    echo "==== Vajra $NAME ($SFENV) ===="
    kk cp "scripts/$SCRIPT" vajra-client:/tmp/"$SCRIPT" 2>/dev/null
    # Capture FULL output to a log (so failures/Tracebacks are visible, not filtered away), then show it.
    kk exec vajra-client -- sh -c "SPARK_REMOTE=sc://$VSVC $SFENV python3 /tmp/$SCRIPT" >"/tmp/vbatch_$NAME.log" 2>&1 || true
    grep -aiE "Result:|TPC-DS|TPC-H|Scorecard|Q[0-9]|Error|Traceback|Exception|Generating|Data ready|refused|connect" \
      "/tmp/vbatch_$NAME.log" | tail -25 | tee -a /tmp/tri_batch.txt
    local VPOD; VPOD=$(kk get pod -l app=vajra-sf100 -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    if [ -n "$VPOD" ]; then
      local PK; PK=$(kk exec "$VPOD" -- sh -c 'cat /sys/fs/cgroup/memory.peak 2>/dev/null || cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes 2>/dev/null' 2>/dev/null)
      echo "Vajra $NAME peakRSS=$(gib "${PK:-0}") GiB" | tee -a /tmp/tri_batch.txt
    fi
  done
  # Spark baseline: same scripts, Spark 3.5.3 local[16], after scaling Vajra->0 (same node = fair).
  echo "==== Spark 3.5.3 baseline (scale Vajra->0, same node) ===="
  kk scale deploy/vajra-sf100 --replicas=0 2>/dev/null || true; sleep 10
  # TPC-DS-99 power test on Spark (data generated in-process via DuckDB at TPCDS_SF).
  kk create configmap tpcds-script --from-file=tpcds_score.py=scripts/tpcds_score.py --dry-run=client -o yaml | kk apply -f - >/dev/null
  kk delete job spark-tpcds-99 --ignore-not-found >/dev/null 2>&1
  # keep the flow-map `}` intact when injecting SF (a greedy sed ate it -> broken YAML on the first run).
  sed "s/name: TPCDS_SF, value: \"10\" }/name: TPCDS_SF, value: \"$TPCDS_SF\" }/" k8s/eks/spark-tpcds-job.yaml | kk apply -f -
  kk wait --for=condition=complete --timeout=3600s job/spark-tpcds-99 2>/dev/null \
    && kk logs job/spark-tpcds-99 2>/dev/null | grep -aiE "TOTAL|wall|peak_RSS|TPC-DS|GEOMEAN" | tail -15 | tee -a /tmp/tri_batch.txt
  # TPC-H on Spark (reuses the existing spark-bench-job for tpch_distributed.py).
  echo "  (TPC-H Spark: k8s/eks/spark-bench-job.yaml, same pattern)"
  echo "######## BATCH SCORECARD TABLE ########"; cat /tmp/tri_batch.txt | mask
  echo "Teardown: eksctl delete cluster --name <batch-cluster> --region $REGION --force --wait (NO interrupt)"
}

case "$PHASE" in
  streaming) streaming_phase ;;
  batch) batch_phase ;;
  all) streaming_phase; echo; batch_phase ;;
  *) echo "usage: $0 {streaming|batch|all}  (cluster must be UP + kubeconfig pointed; see header)"; exit 1 ;;
esac
