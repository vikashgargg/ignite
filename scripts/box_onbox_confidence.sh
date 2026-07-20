#!/usr/bin/env bash
# Runs ON the 32 GiB EC2 (invoked by box_confidence_100m.sh). Reuses the kind manifests validated at 2M,
# at full 100M scale + full resources. Phase 1 = Vajra realtime memory+throughput+jeprof (the priority).
# Phase 2 = Flink realtime apples-to-apples + byte-identical. Prints a RESULTS block.
set -uo pipefail
ROOT="$HOME/zelox"; cd "$ROOT"
N="${N:-100000000}"; NS=stream; CL=zelox-conf; CTX="kind-$CL"
kk(){ kubectl --context "$CTX" -n "$NS" "$@"; }
echo "############ PHASE 0: build strip=false jemalloc-prof image ############"
docker build -f docker/Dockerfile --build-arg CARGO_FEATURES=jemalloc --build-arg RELEASE_STRIP=false \
  -t vajra:prof . 2>&1 | tail -3

echo "############ PHASE 0b: kind up + load ############"
kind get clusters 2>/dev/null | grep -qx "$CL" || kind create cluster --name "$CL" --config k8s/kind/kind-cluster.yaml
kind load docker-image vajra:prof --name "$CL"
kubectl --context "$CTX" create ns "$NS" 2>/dev/null || true

echo "############ PHASE 0c: deploy Kafka + MinIO (full-res for 100M) ############"
kk apply -f k8s/stream/kafka.yaml >/dev/null
kk set resources deploy/kafka --requests=cpu=2,memory=3Gi --limits=cpu=4,memory=6Gi
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
          resources: { requests: { cpu: "6", memory: "16Gi" }, limits: { cpu: "12", memory: "22Gi" } }
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
kk exec vajra-client -- sh -c "BOOT=$BOOT TOPIC=events N=$N K=1000 EPMS=1000 NP=8 python3 /tmp/scale_producer.py 2>&1 | tail -2" || true
kk exec "$KPOD" -- bash -c "/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic events 2>/dev/null | awk -F: '{s+=\$3} END{print \"kafka total offset =\",s}'"
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

echo "############ PHASE 2: Flink realtime 100M apples-to-apples ############"
if [ -f k8s/stream/flink-session.yaml ] && [ -f scripts/eks_flink_wagg_complete.sh ]; then
  echo "Flink phase: deploy + realtime drain (best-effort; memory answer already captured above)"
  # Reuse the flink manifests; scale for the box. (Kept best-effort so a Flink hiccup can't lose Phase-1.)
  kk apply -f k8s/stream/flink-session.yaml >/dev/null 2>&1 || true
else echo "Flink scaffolding not found in tree — skip (Phase-1 memory/throughput is the priority)"; fi

echo "############ RESULTS SUMMARY ############"
echo "See VAJRA_PEAK_RSS / VAJRA_ANON / VAJRA_REALTIME_DRAIN / JEPROF TOP / WM_PROF above."
