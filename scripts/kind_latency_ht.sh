#!/usr/bin/env bash
# T2 REALTIME latency head-to-head on kind (LOCAL, FREE): Zelox (continuous) vs Flink 1.19 (streaming).
# THE Flink-class metric a micro-batch cannot express, run on REAL Kubernetes (pods, service networking,
# the zelox image) BEFORE any EKS spend. Both engines run the IDENTICAL Kafka lat_in -> raw value
# passthrough -> Kafka lat_out at a fixed rate; a shared in-pod loadgen embeds produce_ts and a consumer
# computes now-produce_ts per record -> p50/p99/p99.9/max (no-JVM/no-GC target = better TAIL).
# SEQUENTIAL (never concurrent): Zelox measured + torn down, THEN Flink — so both fit the 8-vCPU Docker VM.
# Same manifests/topology as EKS; resource requests scaled to a laptop via scale_kind (T2 tests
# topology/scheduling, not scale). Assumes `TAG=<img> scripts/kind_up.sh` already ran (cluster up + image loaded).
# Usage: RATE=5000 DUR=45 TAG=bf6 scripts/kind_latency_ht.sh
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
RATE="${RATE:-5000}"; DUR="${DUR:-45}"; TAG="${TAG:-bf6}"; NS=stream; CTX="${CTX:-kind-zelox-kind}"
BOOT="kafka.$NS.svc.cluster.local:9092"
kk() { kubectl --context "$CTX" -n "$NS" "$@"; }
# Scale EKS resource requests down to the kind Docker VM, keeping the SAME manifests/topology.
scale_kind() {
  sed -E \
    -e 's/cpu: "1[0-9]"/cpu: "1"/g' -e 's/cpu: "[6-9]"/cpu: "1"/g' \
    -e 's/memory: "2[0-9]Gi"/memory: "2Gi"/g' -e 's/memory: "1[0-9]Gi"/memory: "1500Mi"/g' \
    -e 's/"--workers", "4"/"--workers", "2"/g'
}
kubectl --context "$CTX" get ns "$NS" >/dev/null 2>&1 || kubectl --context "$CTX" create ns "$NS"

echo "==== [1] Kafka + topics lat_in/lat_out (16 part) ===="
scale_kind < k8s/stream/kafka.yaml | kk apply -f -
kk wait --for=condition=available --timeout=300s deployment/kafka
KPOD=$(kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')
for t in lat_in lat_out; do
  kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "$t" --partitions 16 --replication-factor 1 >/dev/null 2>&1
done
echo "topics ready"

# ---- shared in-pod loadgen+consumer (runs in zelox-client, which has confluent-kafka) ----
loadgen_consume() { # $1=label -> LATENCY_RESULT line
  kk exec zelox-client -- sh -c "BOOT=$BOOT RATE=$RATE DUR=$DUR python3 - <<'PY'
import os, time, json, threading
from confluent_kafka import Producer, Consumer
boot=os.environ['BOOT']; rate=int(os.environ['RATE']); dur=int(os.environ['DUR'])
def produce():
    p=Producer({'bootstrap.servers':boot,'linger.ms':5,'queue.buffering.max.messages':2000000})
    i,t0=0,time.time()
    while time.time()-t0<dur:
        s=time.time()
        for _ in range(rate):
            now=int(time.time()*1000)
            while True:
                try: p.produce('lat_in', value=json.dumps({'id':i,'ts':now})); break
                except BufferError: p.poll(0.01)
            i+=1
        p.poll(0)
        dt=time.time()-s
        if dt<1.0: time.sleep(1.0-dt)
    p.flush()
c=Consumer({'bootstrap.servers':boot,'group.id':f'lat-{time.time()}','auto.offset.reset':'latest','enable.auto.commit':False})
c.subscribe(['lat_out'])
th=threading.Thread(target=produce); th.start()
lat=[]; t0=time.time()
while time.time()-t0<dur+8:
    m=c.poll(0.5)
    if m is None or m.error(): continue
    try:
        v=json.loads(m.value()); lat.append(int(time.time()*1000)-int(v['ts']))
    except Exception: pass
c.close(); th.join()
lat=sorted(x for x in lat if x>=0)
pct=lambda p: lat[min(len(lat)-1,int(len(lat)*p/100))] if lat else -1
print(f'LATENCY_RESULT n={len(lat)} p50_ms={pct(50)} p99_ms={pct(99)} p999_ms={pct(99.9)} max_ms={lat[-1] if lat else -1}')
PY" 2>&1 | grep -a LATENCY_RESULT | sed "s/^/[$1] /"
}

echo "==== [2] ZELOX ($TAG) realtime passthrough ===="
sed -E -e "s#__ECR__/zelox:[A-Za-z0-9._-]+#zelox:$TAG#g" -e 's/imagePullPolicy: Always/imagePullPolicy: IfNotPresent/g' k8s/stream/zelox-stream.yaml | scale_kind | kk apply -f -
kk wait --for=condition=available --timeout=300s deployment/zelox-stream
kk apply -f k8s/stream/zelox-client.yaml
kk wait --for=condition=ready --timeout=300s pod/zelox-client
until kk logs zelox-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
kk cp scripts/stream_latency_query.py zelox-client:/tmp/lat.py
SR="sc://zelox-stream.$NS.svc.cluster.local:50051"
kk exec zelox-client -- sh -c "SPARK_REMOTE=$SR BOOT=$BOOT IN_TOPIC=lat_in OUT_TOPIC=lat_out CK=/tmp/lat_ck python3 /tmp/lat.py" >/tmp/vlat.log 2>&1 &
VQ=$!; sleep 15
ZELOX_LAT=$(loadgen_consume ZELOX)
kill $VQ 2>/dev/null
kk delete deploy zelox-stream --ignore-not-found >/dev/null 2>&1

echo "==== [3] FLINK 1.19 streaming passthrough (fair: parallelism=2 == Zelox workers=2) ===="
# CRITICAL: scale_kind scales only k8s resource *limits*; Flink's INTERNAL JVM sizing
# (taskmanager.memory.process.size, default 24576m for the EKS big node) must ALSO be scaled
# or the TM OOMKills (exitCode 137) in the small pod -> 0 slots register -> job FAILs on slot
# timeout. And parallelism/slots must drop from 16 (16-way on ~1 laptop core = thrash/stall)
# to 2 to match Zelox. flink_scale bakes both in.
flink_scale() {
  sed -E \
    -e 's/numberOfTaskSlots: 16/numberOfTaskSlots: 2/' -e 's/parallelism.default: 16/parallelism.default: 2/' \
    -e 's/taskmanager.memory.process.size: 24576m/taskmanager.memory.process.size: 1400m/' \
    -e 's/taskmanager.memory.managed.fraction: 0.5/taskmanager.memory.managed.fraction: 0.1/' \
    -e 's/cpu: "15"/cpu: "2"/g' -e 's/cpu: "16"/cpu: "2"/g' \
    -e 's/requests: \{cpu: "2", memory: "2[0-9]Gi"\}/requests: {cpu: "1", memory: "1500Mi"}/' \
    -e 's/limits: \{cpu: "2", memory: "2[0-9]Gi"\}/limits: {cpu: "2", memory: "1700Mi"}/'
}
flink_scale < k8s/stream/flink-session.yaml | kk apply -f -
kk wait --for=condition=available --timeout=600s deployment/flink-jm || echo "WARN flink-jm slow"
# Wait for the TM to actually REGISTER slots with the JM (Running != registered; OOM shows here).
JM=$(kk get pod -l app=flink,component=jm -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
for i in $(seq 1 40); do kk exec "$JM" -- sh -c 'curl -sf localhost:8081/overview >/dev/null' 2>/dev/null && { echo "flink JM REST up"; break; }; sleep 5; done
for i in $(seq 1 24); do
  TMS=$(kk exec "$JM" -- sh -c 'curl -sf localhost:8081/taskmanagers' 2>/dev/null | grep -oE '"freeSlots":[0-9]+' | head -1 | cut -d: -f2)
  [ "${TMS:-0}" -ge 2 ] 2>/dev/null && { echo "flink TM registered ${TMS} slots"; break; }
  sleep 5
done
# Fair SQL: parallelism.default 16 -> 2 (must match the TM's 2 slots, else tasks stay SCHEDULED).
sed "s|SET 'parallelism.default' = '16';|SET 'parallelism.default' = '2';|" k8s/stream/flink-sql-latency.sql > /tmp/flink-sql-lat-fair.sql
kk create configmap flink-sql-lat --from-file=flink-sql.sql=/tmp/flink-sql-lat-fair.sql --dry-run=client -o yaml | kk apply -f -
kk delete job flink-lat --ignore-not-found >/dev/null 2>&1
sed -e 's/name: flink-runner/name: flink-lat/' -e 's/name: flink-sql }/name: flink-sql-lat }/' \
    -e 's#/opt/flink/bin/sql-client.sh -f /sql/flink-sql.sql#/opt/flink/bin/sql-client.sh -f /sql/flink-sql.sql 2>\&1 | tee /tmp/sqlout; sleep 999999#' \
    k8s/stream/flink-runner-job.yaml | kk apply -f -
SUBMITTED=0
# Wait for the job's TASKS to actually be RUNNING (job-level RUNNING with tasks stuck SCHEDULED = no
# slots = n=0). Deploying tasks (not just an accepted JobGraph) is the real readiness signal.
for i in $(seq 1 60); do
  R=$(kk exec "$JM" -- sh -c 'curl -sf localhost:8081/jobs/overview' 2>/dev/null | grep -oE '"running":[0-9]+' | head -1 | cut -d: -f2)
  [ "${R:-0}" -ge 2 ] 2>/dev/null && { SUBMITTED=1; echo "flink job RUNNING ($R tasks deployed)"; break; }
  sleep 5
done
[ "$SUBMITTED" = "1" ] || echo "WARN: flink tasks never reached RUNNING (measurement will be n=0)"
sleep 10
FLINK_LAT=$(loadgen_consume FLINK)
kk delete -f k8s/stream/flink-session.yaml --ignore-not-found >/dev/null 2>&1
kk delete job flink-lat --ignore-not-found >/dev/null 2>&1

echo ""; echo "######## T2/kind REALTIME LATENCY HEAD-TO-HEAD (rate=$RATE/s dur=${DUR}s) ########"
echo "$ZELOX_LAT"
echo "$FLINK_LAT"
echo "(no-JVM/no-GC target = better p99/p999/max TAIL; kind is topology-faithful but laptop-scale)"
