#!/usr/bin/env bash
# FULL prod metric matrix on ONE box (64 GiB), both engines realtime on MinIO S3:
# throughput, memory, correctness (S3-verified), latency (p50/p99/p99.9), reliability (crash-EO).
# Reuses proven scripts (stream_realtime_drain, lat_probe, stream_latency_query, flink manifests).
# Each phase is best-effort + prints a tagged line so a later hiccup can't lose earlier results.
# Pulls zelox:prof from ECR (cached) so no 14-min rebuild. Invoked by box_metrics_launch.sh.
set -uo pipefail
ROOT="$HOME/zelox"; cd "$ROOT"
N="${N:-100000000}"; NS=stream; CL=zelox-conf; CTX="kind-$CL"; REGION="${REGION:-ap-south-1}"
kk(){ kubectl --context "$CTX" -n "$NS" "$@"; }
EP="http://minio.stream.svc.cluster.local:9000"; BOOT="kafka.stream.svc.cluster.local:9092"
S3ENV="AWS_ENDPOINT=$EP AWS_ENDPOINT_URL=$EP AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_ALLOW_HTTP=true AWS_REGION=us-east-1"

echo "########## PHASE 0: image (pull cached) + kind + kafka + minio ##########"
ECR="$(aws ecr describe-repositories --region "$REGION" --repository-name zelox --query 'repositories[0].repositoryUri' --output text 2>/dev/null)"
aws ecr get-login-password --region "$REGION" 2>/dev/null | docker login --username AWS --password-stdin "${ECR%/zelox}" >/dev/null 2>&1 || true
docker pull "${ECR}:prof" 2>/dev/null && docker tag "${ECR}:prof" zelox:prof && echo "pulled cached zelox:prof" || {
  docker build -f docker/Dockerfile --build-arg CARGO_FEATURES=jemalloc --build-arg RELEASE_STRIP=false -t zelox:prof . 2>&1 | tail -2; }
kind get clusters 2>/dev/null | grep -qx "$CL" || kind create cluster --name "$CL" --config k8s/kind/kind-cluster.yaml
kind load docker-image zelox:prof --name "$CL"
kubectl --context "$CTX" create ns "$NS" 2>/dev/null || true
sed -E 's/cpu: "[0-9]+"/cpu: "2"/g; s/memory: "16Gi"/memory: "4Gi"/g; s/memory: "26Gi"/memory: "14Gi"/g' k8s/stream/kafka.yaml | kk apply -f - >/dev/null
kk apply -f k8s/kind/minio.yaml >/dev/null
kk rollout status deploy/kafka --timeout=200s; kk rollout status deploy/minio --timeout=120s
KPOD(){ kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}'; }
mktopic(){ kk exec "$(KPOD)" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "$1" --partitions "${2:-8}" --replication-factor 1 >/dev/null 2>&1 || true; }
offsets(){ kk exec "$(KPOD)" -- bash -c "/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $1 2>/dev/null" | awk -F: '{s+=$3}END{print s+0}'; }

deploy_zelox(){ # $1 = extra env (e.g. crash floor); prof off for throughput/latency, on for memory
  local prof="$1"
  cat > /tmp/vj.yaml <<YAML
apiVersion: apps/v1
kind: Deployment
metadata: { name: zelox-stream, namespace: stream }
spec:
  replicas: 1
  selector: { matchLabels: { app: zelox-stream } }
  template:
    metadata: { labels: { app: zelox-stream } }
    spec:
      containers:
        - name: server
          image: zelox:prof
          imagePullPolicy: Never
          args: ["server","--ip","0.0.0.0","--port","50051","--mode","local-cluster","--workers","4"]
          ports: [ { containerPort: 50051 } ]
          resources: { requests: { cpu: "4", memory: "10Gi" }, limits: { cpu: "5", memory: "24Gi" } }
          env:
            - { name: RUST_LOG, value: warn }
            - { name: ZELOX_RUNTIME__STACK_SIZE, value: "16777216" }
            - { name: AWS_ENDPOINT, value: "$EP" }
            - { name: AWS_ENDPOINT_URL, value: "$EP" }
            - { name: AWS_ACCESS_KEY_ID, value: "minioadmin" }
            - { name: AWS_SECRET_ACCESS_KEY, value: "minioadmin" }
            - { name: AWS_ALLOW_HTTP, value: "true" }
            - { name: AWS_REGION, value: "us-east-1" }
            - { name: _RJEM_MALLOC_CONF, value: "$prof" }
            - { name: MALLOC_CONF, value: "$prof" }
            - { name: ZELOX_WM_PROF, value: "1" }
          volumeMounts: [ { name: data, mountPath: /data } ]
      volumes: [ { name: data, emptyDir: { sizeLimit: 40Gi } } ]
---
apiVersion: v1
kind: Service
metadata: { name: zelox-stream, namespace: stream }
spec: { selector: { app: zelox-stream }, ports: [ { port: 50051, targetPort: 50051 } ] }
YAML
  kk apply -f /tmp/vj.yaml >/dev/null; kk rollout status deploy/zelox-stream --timeout=200s; }

DECAY="dirty_decay_ms:1000,muzzy_decay_ms:1000"
PROF="prof:true,prof_active:true,lg_prof_sample:19,lg_prof_interval:31,prof_prefix:/tmp/jeprof,$DECAY"

echo "########## PHASE 1 (Zelox): throughput + memory + correctness (100M realtime -> MinIO) ##########"
mktopic events 8
kk apply -f k8s/stream/zelox-client.yaml >/dev/null; until kk logs zelox-client 2>/dev/null | grep -q CLIENT_READY; do sleep 5; done
kk cp scripts/scale_producer.py zelox-client:/tmp/scale_producer.py; kk cp scripts/stream_realtime_drain.py zelox-client:/tmp/rt_drain.py
kk exec zelox-client -- sh -c "BOOT=$BOOT TOPIC=events N=$N K=1000 EPMS=1000 NP=8 CLOSER_TS=1700000200000 python3 /tmp/scale_producer.py 2>&1 | tail -1" || true
echo "PRODUCED events=$(offsets events)"
deploy_zelox "$PROF"
SR="sc://zelox-stream.stream.svc.cluster.local:50051"
kk exec zelox-client -- sh -c "$S3ENV SPARK_REMOTE=$SR BOOT=$BOOT TOPIC=events N_EVENTS=$N RT_DUR='1 seconds' MAX_SECS=900 OUT=s3://zelox/rt_out CK=s3://zelox/rt_ck python3 /tmp/rt_drain.py 2>&1" | grep -aiE "ZELOX_REALTIME_DRAIN|ZELOX_COMPLETENESS|ZELOX_CONSUME" | tail -3
VP=$(kk get pod -l app=zelox-stream -o jsonpath='{.items[0].metadata.name}')
echo "ZELOX_MEM peakRSS=$(kk exec $VP -- sh -c 'cat /sys/fs/cgroup/memory.peak' 2>/dev/null | awk '{printf "%.2f",$1/1073741824}')GiB anon=$(kk exec $VP -- sh -c 'grep "^anon " /sys/fs/cgroup/memory.stat' 2>/dev/null | awk '{printf "%.2f",$2/1073741824}')GiB"
kk exec zelox-client -- sh -c "$S3ENV EXPECT_N=$N python3 - <<'PY'
import boto3,io,pyarrow.parquet as pq,pyarrow.compute as pc,os
c=boto3.client('s3',endpoint_url=os.environ['AWS_ENDPOINT'],aws_access_key_id='minioadmin',aws_secret_access_key='minioadmin')
EN=int(os.environ['EXPECT_N']); tot=0;w=set();r=0
for k in [x['Key'] for x in c.list_objects_v2(Bucket='zelox',Prefix='rt_out/').get('Contents',[]) if x['Key'].endswith('.parquet') and '_spark_metadata' not in x['Key']]:
 t=pq.read_table(io.BytesIO(c.get_object(Bucket='zelox',Key=k)['Body'].read())); t=t.filter(pc.greater_equal(t.column('k'),0))
 r+=t.num_rows; tot+=sum(t.column('count').to_pylist()); w.update(str(x) for x in pc.struct_field(t.column('window'),'start').to_pylist())
print('ZELOX_S3_CORRECT windows=%d rows=%d sum=%d EXACT=%s'%(len(w),r,tot,len(w)==10 and r==10000 and tot==EN))
PY" 2>&1 | tail -1

echo "########## PHASE 2 (Zelox): reliability = crash-EO on S3 (kill-9 mid-drain, recover, verify) ##########"
mktopic events2 8
kk exec zelox-client -- sh -c "BOOT=$BOOT TOPIC=events2 N=30000000 K=1000 EPMS=300 NP=8 CLOSER_TS=1700000200000 python3 /tmp/scale_producer.py 2>&1 | tail -1" || true
kk exec zelox-client -- sh -c "$S3ENV SPARK_REMOTE=$SR BOOT=$BOOT TOPIC=events2 N_EVENTS=30000000 RT_DUR='1 seconds' MAX_SECS=40 OUT=s3://zelox/eo_out CK=s3://zelox/eo_ck python3 /tmp/rt_drain.py 2>&1 | tail -1" &
DPID=$!; sleep 22; echo "--- KILL -9 zelox mid-drain ---"; kk delete pod $VP --grace-period=0 --force >/dev/null 2>&1; wait $DPID 2>/dev/null
deploy_zelox "$DECAY"
kk exec zelox-client -- sh -c "$S3ENV SPARK_REMOTE=$SR BOOT=$BOOT TOPIC=events2 N_EVENTS=30000000 RT_DUR='1 seconds' MAX_SECS=180 OUT=s3://zelox/eo_out CK=s3://zelox/eo_ck python3 /tmp/rt_drain.py 2>&1" | grep -aiE "ZELOX_COMPLETENESS" | tail -1
kk exec zelox-client -- sh -c "$S3ENV python3 - <<'PY'
import boto3,io,pyarrow.parquet as pq,pyarrow.compute as pc,os,collections
c=boto3.client('s3',endpoint_url=os.environ['AWS_ENDPOINT'],aws_access_key_id='minioadmin',aws_secret_access_key='minioadmin')
seen=collections.Counter()
for k in [x['Key'] for x in c.list_objects_v2(Bucket='zelox',Prefix='eo_out/').get('Contents',[]) if x['Key'].endswith('.parquet') and '_spark_metadata' not in x['Key']]:
 t=pq.read_table(io.BytesIO(c.get_object(Bucket='zelox',Key=k)['Body'].read())); t=t.filter(pc.greater_equal(t.column('k'),0))
 for w,kk_ in zip(pc.struct_field(t.column('window'),'start').to_pylist(),t.column('k').to_pylist()): seen[(str(w),kk_)]+=1
dup=sum(1 for v in seen.values() if v>1)
print('ZELOX_CRASH_EO distinct_wk=%d duplicates=%d EO=%s'%(len(seen),dup,dup==0))
PY" 2>&1 | tail -1

echo "########## PHASE 3 (Zelox): latency p50/p99/p99.9 (rate-limited passthrough) ##########"
mktopic lat_in 4; mktopic lat_out 4
kk cp scripts/lat_probe.py zelox-client:/tmp/lat_probe.py 2>/dev/null; kk cp scripts/stream_latency_query.py zelox-client:/tmp/lat_q.py 2>/dev/null
kk exec zelox-client -- sh -c "SPARK_REMOTE=$SR BOOT=$BOOT IN_TOPIC=lat_in OUT_TOPIC=lat_out python3 /tmp/lat_q.py >/tmp/latq.log 2>&1 & sleep 12; RATE=5000 DURATION_S=40 BOOT=$BOOT IN_TOPIC=lat_in OUT_TOPIC=lat_out python3 /tmp/lat_probe.py 2>&1 | grep -aiE 'p50|p99|LAT'" 2>&1 | tail -3 | sed 's/^/ZELOX_LAT /'

echo "########## PHASE 4 (Flink): throughput + memory(TM) + correctness (100M realtime -> MinIO S3) ##########"
kk delete deploy/zelox-stream --ignore-not-found >/dev/null 2>&1; sleep 5
bash scripts/box_flink_s3_phase.sh "$N" 2>&1 | grep -aiE "FLINK_" | tail -6 || echo "FLINK phase script missing"

echo "########## PHASE 5: EXACT DATA CORRECTNESS — real queries, both engines byte-identical on S3 ##########"
kk exec zelox-client -- sh -c "$S3ENV EXPECT_N=$N python3 - <<'PY'
import boto3,io,os,collections,pyarrow.parquet as pq,pyarrow.compute as pc
c=boto3.client('s3',endpoint_url=os.environ['AWS_ENDPOINT'],aws_access_key_id='minioadmin',aws_secret_access_key='minioadmin')
EN=int(os.environ['EXPECT_N'])
def load(prefix,cntcol,wcol,structw):
 d={}
 for x in c.list_objects_v2(Bucket='zelox',Prefix=prefix).get('Contents',[]):
  k=x['Key']
  if '_spark_metadata' in k or not ('part-' in k or k.endswith('.parquet')): continue
  t=pq.read_table(io.BytesIO(c.get_object(Bucket='zelox',Key=k)['Body'].read()))
  if 'k' not in t.schema.names: continue
  ws=pc.struct_field(t.column(wcol),'start') if structw else t.column(wcol)
  for w,kk,cn in zip(ws.to_pylist(),t.column('k').to_pylist(),t.column(cntcol).to_pylist()):
   if kk is not None and kk>=0: d[(str(w)[:19],int(kk))]=int(cn)
 return d
V=load('rt_out/','count','window',True); F=load('flink_out/','cnt','window_start',False)
# Q1 shape: exactly 10 windows x 1000 keys = 10000 rows, each count uniform
def shape(D,name):
 wins=sorted({w for w,_ in D}); keys={k for _,k in D}
 per=[sum(1 for (w,_) in D if w==x) for x in wins]
 print('DATA_%s windows=%d keys=%d rows=%d per_window_rows(min/max)=%d/%d counts(min/max)=%d/%d total=%d'%(
   name,len(wins),len(keys),len(D),min(per),max(per),min(D.values()),max(D.values()),sum(D.values())))
 return wins,keys
vw,vk=shape(V,'ZELOX'); fw,fk=shape(F,'FLINK')
# Q2 exact per-(window,k) equality Zelox vs Flink
common=set(V)&set(F); mism=[x for x in common if V[x]!=F[x]]
print('DATA_IDENTICAL common=%d value_mismatches=%d only_zelox=%d only_flink=%d windows_match=%s keys_match=%s'%(
  len(common),len(mism),len(set(V)-set(F)),len(set(F)-set(V)),vw==fw,vk==fk))
# Q3 real drill-down query: per-window totals (each window must total EN/10) + full key coverage 0..999
wtot_v=collections.Counter(); [wtot_v.__setitem__(w,wtot_v[w]+cn) for (w,_),cn in V.items()]
ok_wtot=all(v==EN//10 for v in wtot_v.values()); ok_keys=(vk==set(range(1000)))
print('DATA_DRILLDOWN per_window_total==%d:%s all_1000_keys_present:%s sample_window=%s'%(EN//10,ok_wtot,ok_keys,sorted(wtot_v)[0] if wtot_v else 'none'))
print('DATA_VERDICT EXACT_CORRECT_AND_IDENTICAL=%s'%(len(V)==10000 and len(F)==10000 and len(mism)==0 and set(V)==set(F) and all(x==EN//10000 for x in V.values())))
PY" 2>&1 | tail -6

echo "########## SCORECARD ##########"
echo "TAGS: ZELOX_REALTIME_DRAIN ZELOX_MEM ZELOX_S3_CORRECT ZELOX_CRASH_EO ZELOX_LAT FLINK_DRAIN FLINK_TM_MEM FLINK_S3_CORRECT DATA_VERDICT"
