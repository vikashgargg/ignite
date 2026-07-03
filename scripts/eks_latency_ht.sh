#!/usr/bin/env bash
# EKS REALTIME latency head-to-head: Vajra (continuous) vs Flink 1.19 (streaming), like-for-like.
# THE Flink-class metric micro-batch cannot express. Both run the identical Kafka lat_in -> value
# passthrough -> Kafka lat_out at a fixed rate; a shared in-pod loadgen embeds produce_ts and a consumer
# computes now-produce_ts per record, reporting p50/p99/p99.9/max (no-JVM/no-GC should win the TAIL).
# Sequential (never concurrent), same compute node, same Kafka. Assumes cluster UP + image :TAG in ECR.
# Usage: scripts/eks_latency_ht.sh [RATE] [DURATION_S] [TAG]
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
RATE="${1:-20000}"; DUR="${2:-60}"; TAG="${3:-aligned-eo}"; REGION=ap-south-1; NS=stream
ECR="$(aws ecr describe-repositories --region $REGION --repository-name vajra --query 'repositories[0].repositoryUri' --output text)"; REG="${ECR%/vajra}"
kk() { kubectl -n "$NS" "$@"; }
wait_ready() { kk wait --for=condition=available --timeout=300s deployment/"$1"; }

echo "==== [1] Kafka + topics lat_in/lat_out (16 part) ===="
kk apply -f k8s/stream/kafka.yaml >/dev/null 2>&1 || kubectl apply -f k8s/stream/kafka.yaml
wait_ready kafka
KPOD=$(kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')
for t in lat_in lat_out; do
  kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "$t" --partitions 16 --replication-factor 1 >/dev/null 2>&1
done
echo "topics ready"

# ---- shared in-pod loadgen+consumer (runs in vajra-client, which has confluent-kafka) ----
BOOT="kafka.$NS.svc.cluster.local:9092"
loadgen_consume() { # $1=label -> LATENCY_RESULT line
  kk exec vajra-client -- sh -c "BOOT=$BOOT RATE=$RATE DUR=$DUR python3 - <<'PY'
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

echo "==== [2] VAJRA ($TAG) realtime passthrough ===="
sed -e "s|__ECR__|$REG|g" -e "s|vajra:eo-multipart|vajra:$TAG|g" k8s/stream/vajra-stream.yaml | kk apply -f -
wait_ready vajra-stream
kk apply -f k8s/stream/vajra-client.yaml
kk wait --for=condition=ready --timeout=300s pod/vajra-client
until kk logs vajra-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
kk cp scripts/stream_latency_query.py vajra-client:/tmp/lat.py
SR="sc://vajra-stream.$NS.svc.cluster.local:50051"
kk exec vajra-client -- sh -c "SPARK_REMOTE=$SR BOOT=$BOOT IN_TOPIC=lat_in OUT_TOPIC=lat_out CK=s3://none/x python3 /tmp/lat.py" >/tmp/vlat.log 2>&1 &
VQ=$!; sleep 15
VAJRA_LAT=$(loadgen_consume VAJRA)
kill $VQ 2>/dev/null
VPOD=$(kk get pod -l app=vajra-stream --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')
VMEM=$(kk exec "$VPOD" -- cat /sys/fs/cgroup/memory.peak 2>/dev/null)
kk delete deploy vajra-stream --ignore-not-found >/dev/null 2>&1

echo "==== [3] FLINK 1.19 streaming passthrough (mini-batch OFF) ===="
kk apply -f k8s/stream/flink-session.yaml
wait_ready flink-jm; wait_ready flink-tm
kk create configmap flink-sql-lat --from-file=flink-sql.sql=k8s/stream/flink-sql-latency.sql --dry-run=client -o yaml | kk apply -f -
# Detached submit (unbounded passthrough): run sql-client in a bg pod, then measure, then cancel.
kk delete job flink-lat --ignore-not-found >/dev/null 2>&1
sed -e 's/name: flink-runner/name: flink-lat/' -e 's/name: flink-sql }/name: flink-sql-lat }/' \
    -e 's#/opt/flink/bin/sql-client.sh -f /sql/flink-sql.sql#sed -i "s/table.dml-sync. = .true./table.dml-sync = false/" /sql/flink-sql.sql 2>/dev/null; /opt/flink/bin/sql-client.sh -f /sql/flink-sql.sql; sleep 999999#' \
    k8s/stream/flink-runner-job.yaml | kk apply -f -
# wait for the Flink job to be RUNNING on the TM
for i in $(seq 1 40); do kk logs job/flink-lat 2>/dev/null | grep -qiE "job has been submitted|Job ID" && break; sleep 5; done
sleep 15
FLINK_LAT=$(loadgen_consume FLINK)
FTM=$(kk get pod -l app=flink,component=tm -o jsonpath='{.items[0].metadata.name}')
FMEM=$(kk exec "$FTM" -- sh -c 'cat /sys/fs/cgroup/memory.peak 2>/dev/null' 2>/dev/null)
kk delete -f k8s/stream/flink-session.yaml --ignore-not-found >/dev/null 2>&1
kk delete job flink-lat --ignore-not-found >/dev/null 2>&1

echo ""; echo "######## REALTIME LATENCY HEAD-TO-HEAD (rate=$RATE/s dur=${DUR}s) ########"
echo "$VAJRA_LAT   peakRSS_bytes=${VMEM:-?}"
echo "$FLINK_LAT   peakRSS_bytes=${FMEM:-?}"
echo "(Flink-class realtime: compare p99/p999/max tails — no-JVM/no-GC target = better tail)"
echo "Teardown: eksctl delete cluster --name vajra-stream-ht --region $REGION --force --wait (NEVER interrupt)"
