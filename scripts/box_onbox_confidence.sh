#!/usr/bin/env bash
# Runs ON the 32 GiB EC2 (invoked by box_confidence_100m.sh). Reuses the kind manifests validated at 2M,
# at full 100M scale + full resources. Phase 1 = Vajra realtime memory+throughput+jeprof (the priority).
# Phase 2 = Flink realtime apples-to-apples + byte-identical. Prints a RESULTS block.
set -uo pipefail
ROOT="$HOME/zelox"; cd "$ROOT"
N="${N:-100000000}"; NS=stream; CL=zelox-conf; CTX="kind-$CL"
kk(){ kubectl --context "$CTX" -n "$NS" "$@"; }
echo "############ PHASE 0: get strip=false jemalloc-prof image (pull from ECR else build+push) ############"
REGION="${REGION:-ap-south-1}"
ECR="$(aws ecr describe-repositories --region "$REGION" --repository-name vajra --query 'repositories[0].repositoryUri' --output text 2>/dev/null)"
aws ecr get-login-password --region "$REGION" 2>/dev/null | docker login --username AWS --password-stdin "${ECR%/vajra}" >/dev/null 2>&1 || true
if [ -n "$ECR" ] && docker pull "${ECR}:prof" 2>/dev/null; then
  docker tag "${ECR}:prof" vajra:prof; echo "pulled ${ECR##*/}:prof (skip build)"
else
  docker build -f docker/Dockerfile --build-arg CARGO_FEATURES=jemalloc --build-arg RELEASE_STRIP=false \
    -t vajra:prof . 2>&1 | tail -3
  [ -n "$ECR" ] && { docker tag vajra:prof "${ECR}:prof" && docker push "${ECR}:prof" >/dev/null 2>&1 && echo "pushed ${ECR##*/}:prof for reuse"; }
fi

echo "############ PHASE 0b: kind up + load ############"
kind get clusters 2>/dev/null | grep -qx "$CL" || kind create cluster --name "$CL" --config k8s/kind/kind-cluster.yaml
kind load docker-image vajra:prof --name "$CL"
kubectl --context "$CTX" create ns "$NS" 2>/dev/null || true

echo "############ PHASE 0c: deploy Kafka + MinIO (single apply, no double-rollout; 8-vCPU-safe) ############"
# Apply ONCE with scaled resources baked in (a second `set resources` caused a rollout that raced the
# produce and lost the topic last time). CPU fits 8 vCPU (kafka 2 + vajra 4). Kafka mem limit 14Gi so
# 100M (~10 GB) page-cache doesn't trip the cgroup.
sed -E 's/cpu: "[0-9]+"/cpu: "2"/g; s/memory: "16Gi"/memory: "4Gi"/g; s/memory: "26Gi"/memory: "14Gi"/g' \
  k8s/stream/kafka.yaml | kk apply -f - >/dev/null
kk apply -f k8s/kind/minio.yaml >/dev/null
kk rollout status deploy/kafka --timeout=200s; kk rollout status deploy/minio --timeout=120s
KPOD=$(kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')
kk exec "$KPOD" -- bash -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic events --partitions 8 --replication-factor 1 2>&1 | tail -1" || true

echo "############ PHASE 0d: deploy Vajra (prof, 16Gi, MinIO, MALLOC_CONF=prof) ############"
EP="http://minio.stream.svc.cluster.local:9000"
cat > /tmp/vajra-box.yaml <<YAML
apiVersion: apps/v1
kind: Deployment
metadata: { name: vajra-stream, namespace: stream }
spec:
  replicas: 1
  selector: { matchLabels: { app: vajra-stream } }
  template:
    metadata: { labels: { app: vajra-stream } }
    spec:
      containers:
        - name: server
          image: vajra:prof
          imagePullPolicy: Never
          args: ["server","--ip","0.0.0.0","--port","50051","--mode","local-cluster","--workers","4"]
          ports: [ { containerPort: 50051 } ]
          resources: { requests: { cpu: "4", memory: "10Gi" }, limits: { cpu: "5", memory: "24Gi" } }
          env:
            - { name: RUST_LOG, value: warn }
            - { name: SAIL_RUNTIME__STACK_SIZE, value: "16777216" }
            - { name: AWS_ENDPOINT, value: "$EP" }
            - { name: AWS_ENDPOINT_URL, value: "$EP" }
            - { name: AWS_ACCESS_KEY_ID, value: "minioadmin" }
            - { name: AWS_SECRET_ACCESS_KEY, value: "minioadmin" }
            - { name: AWS_ALLOW_HTTP, value: "true" }
            - { name: AWS_REGION, value: "us-east-1" }
            - { name: _RJEM_MALLOC_CONF, value: "prof:true,prof_active:true,lg_prof_sample:19,lg_prof_interval:31,prof_prefix:/tmp/jeprof,dirty_decay_ms:1000,muzzy_decay_ms:1000" }
            - { name: MALLOC_CONF, value: "prof:true,prof_active:true,lg_prof_sample:19,lg_prof_interval:31,prof_prefix:/tmp/jeprof,dirty_decay_ms:1000,muzzy_decay_ms:1000" }
            - { name: VAJRA_WM_PROF, value: "1" }
          volumeMounts: [ { name: data, mountPath: /data } ]
      volumes: [ { name: data, emptyDir: { sizeLimit: 40Gi } } ]
---
apiVersion: v1
kind: Service
metadata: { name: vajra-stream, namespace: stream }
spec: { selector: { app: vajra-stream }, ports: [ { port: 50051, targetPort: 50051 } ] }
YAML
kk apply -f /tmp/vajra-box.yaml >/dev/null
kk apply -f k8s/stream/vajra-client.yaml >/dev/null
kk rollout status deploy/vajra-stream --timeout=200s
until kk logs vajra-client 2>/dev/null | grep -q CLIENT_READY; do sleep 5; done
kk cp scripts/scale_producer.py vajra-client:/tmp/scale_producer.py
kk cp scripts/stream_realtime_drain.py vajra-client:/tmp/rt_drain.py

echo "############ PHASE 1: produce $N + Vajra realtime -> MinIO ############"
BOOT=kafka.stream.svc.cluster.local:9092
# CLOSER_TS: high-ts sentinel per partition so the watermark advances past the last window (10s windows
# over 100s of data at EPMS=1000; closer at base+200s) — else the final window never closes and the drain
# times out looking incomplete (that made the prior box run read 9/10 windows — a harness omission, not a bug).
kk exec vajra-client -- sh -c "BOOT=$BOOT TOPIC=events N=$N K=1000 EPMS=1000 NP=8 CLOSER_TS=1700000200000 python3 /tmp/scale_producer.py 2>&1 | tail -2" || true
# Re-resolve kafka pod (guard against a mid-run restart) and ASSERT the topic actually holds N.
KPOD=$(kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')
OFF=$(kk exec "$KPOD" -- bash -c "/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic events 2>/dev/null" | awk -F: '{s+=$3} END{print s+0}')
echo "kafka total offset = $OFF (expected $N)"
if [ "$OFF" -lt "$N" ]; then echo "FATAL: kafka has $OFF < $N — produce/topic lost, aborting drain (fix infra, do not trust downstream numbers)"; kk get pods; exit 3; fi
SR="sc://vajra-stream.stream.svc.cluster.local:50051"
kk exec vajra-client -- sh -c \
 "AWS_ENDPOINT=$EP AWS_ENDPOINT_URL=$EP AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_ALLOW_HTTP=true AWS_REGION=us-east-1 \
  SPARK_REMOTE=$SR BOOT=$BOOT TOPIC=events N_EVENTS=$N RT_DUR='5 seconds' MAX_SECS=600 OUT=s3://vajra/rt_out CK=s3://vajra/rt_ck \
  python3 /tmp/rt_drain.py 2>&1" | grep -aiE "VAJRA_|window|sum|EXACT|throughput|drain" | tail -8
VPOD=$(kk get pod -l app=vajra-stream -o jsonpath='{.items[0].metadata.name}')
echo "--- Vajra peak RSS ---"; kk exec "$VPOD" -- sh -c 'cat /sys/fs/cgroup/memory.peak' | awk '{printf "VAJRA_PEAK_RSS=%.2f GiB\n",$1/1073741824}'
kk exec "$VPOD" -- sh -c 'cat /sys/fs/cgroup/memory.stat | grep "^anon "' | awk '{printf "VAJRA_ANON=%.2f GiB\n",$2/1073741824}'
echo "--- WM_PROF (from_json should be 0 = T7_FUSE active) ---"; kk logs "$VPOD" 2>/dev/null | grep -a WM_PROF_PROC | tail -1

echo "############ PHASE 1b: jeprof (symbolized — names the heap) ############"
kk exec "$VPOD" -- sh -c 'ls -S /tmp/jeprof.*.heap 2>/dev/null | head -1' > /tmp/lastheap.txt
LH=$(cat /tmp/lastheap.txt)
if [ -n "$LH" ]; then
  kk cp "$VPOD:$LH" /tmp/vajra.heap
  kk cp "$VPOD:/usr/local/bin/vajra" /tmp/vajra-bin
  docker run --rm -v /tmp:/w debian:12-slim bash -c \
    'apt-get update -q >/dev/null 2>&1; apt-get install -y -q libjemalloc-dev binutils >/dev/null 2>&1; echo "=== JEPROF TOP (inuse_space) ==="; jeprof --text --inuse_space /w/vajra-bin /w/vajra.heap 2>/dev/null | head -20'
else echo "no jeprof heap (alloc < interval)"; fi

echo "############ PHASE 2: Flink realtime 100M apples-to-apples (same events topic + query) ############"
# Sequential: free RAM by removing Vajra first so Flink TM gets the box to itself (fair — Vajra also had
# it to itself in Phase 1). Same 100M events topic, same 10s tumble COUNT, same Kafka-lag/sink completeness.
kk delete deploy/vajra-stream --ignore-not-found >/dev/null 2>&1; sleep 5
KPOD=$(kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')
kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic wagg_out --partitions 8 --replication-factor 1 >/dev/null 2>&1 || true
# Scale flink-session for the box: TM 12Gi/8 slots (Vajra used 4 workers; 8 slots is a fair-to-generous
# match on 8 vCPU), JM small. parallelism 16->8 in the SQL.
sed -E 's/taskmanager.numberOfTaskSlots: 16/taskmanager.numberOfTaskSlots: 8/; s/taskmanager.memory.process.size: 24576m/taskmanager.memory.process.size: 12288m/; s/cpu: "1[0-9]"/cpu: "6"/g; s/memory: "2[0-9]Gi"/memory: "13Gi"/g; /nodeSelector/d; /role: compute/d; /role: kafka/d' \
  k8s/stream/flink-session.yaml | kk apply -f - >/dev/null
kk wait --for=condition=available --timeout=300s deployment/flink-jm >/dev/null 2>&1 || echo "WARN jm"
kk wait --for=condition=available --timeout=300s deployment/flink-tm >/dev/null 2>&1 || echo "WARN tm"
JM=$(kk get pod -l app=flink,component=jm -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
for i in $(seq 1 40); do kk exec "$JM" -- sh -c 'curl -sf localhost:8081/overview >/dev/null' 2>/dev/null && break; sleep 5; done
sed 's/parallelism.default. = .16./parallelism.default'"'"' = '"'"'8/' k8s/stream/flink-sql-realtime.sql > /tmp/flink-sql-box.sql
kk create configmap flink-sql-rt --from-file=flink-sql.sql=/tmp/flink-sql-box.sql --dry-run=client -o yaml | kk apply -f - >/dev/null
kk delete job flink-rt --ignore-not-found >/dev/null 2>&1
sed -e 's/name: flink-runner/name: flink-rt/' -e 's/name: flink-sql }/name: flink-sql-rt }/' k8s/stream/flink-runner-job.yaml | kk apply -f - >/dev/null
kk wait --for=condition=complete --timeout=300s job/flink-rt >/dev/null 2>&1 || echo "WARN submit"
t0=$(date +%s); FLINK_DRAIN=""
echo "--- poll wagg_out until 10 windows x 1000 keys = 10000 rows ---"
for i in $(seq 1 200); do
  sleep 3
  MSGS=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic wagg_out 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')
  el=$(( $(date +%s) - t0 ))
  [ $((i % 10)) -eq 0 ] && echo "  t=${el}s wagg_out_rows=${MSGS:-?}"
  [ -n "$MSGS" ] && [ "$MSGS" -ge 10000 ] 2>/dev/null && { FLINK_DRAIN=$el; break; }
done
TM=$(kk get pod -l app=flink,component=tm -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
FMEM=$(kk exec "$TM" -- sh -c 'cat /sys/fs/cgroup/memory.peak 2>/dev/null' 2>/dev/null)
[ -z "$FLINK_DRAIN" ] && FLINK_DRAIN=$(( $(date +%s) - t0 ))
awk -v d="$FLINK_DRAIN" -v n="$N" -v m="${FMEM:-0}" 'BEGIN{printf "FLINK_WAGG_COMPLETE drain_s=%d throughput=%.3fM_ev/s peakTM_RSS=%.2f GiB\n", d, n/d/1e6, m/1073741824}'

echo "############ RESULTS SUMMARY ############"
echo "See VAJRA_PEAK_RSS / VAJRA_ANON / VAJRA_REALTIME_DRAIN / JEPROF TOP / WM_PROF above."
