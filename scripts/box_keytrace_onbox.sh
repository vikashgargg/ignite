#!/usr/bin/env bash
# ONE definitive linux localization run for the realtime key-corruption bug. Runs ON a 32 GiB EC2
# (invoked by box_keytrace_launch.sh). Builds the KEY_TRACE-instrumented image, runs Zelox realtime 10M in
# kind→MinIO with ZELOX_KEY_TRACE=1, and captures the per-stage distinct-k census + the S3 per-key result.
# The FIRST stage whose distinct_k drops below 1000 = the corrupting operator (+ min/max = mechanism).
set -uo pipefail
ROOT="$HOME/zelox"; cd "$ROOT"
N="${N:-10000000}"; NS=stream; CL=zelox-kt; CTX="kind-$CL"
kk(){ kubectl --context "$CTX" -n "$NS" "$@"; }
EP="http://minio.stream.svc.cluster.local:9000"; BOOT="kafka.stream.svc.cluster.local:9092"
echo "###### build KEY_TRACE image ######"
docker build -f docker/Dockerfile -t zelox:keytrace . 2>&1 | tail -2
echo "###### kind + kafka + minio ######"
kind get clusters 2>/dev/null | grep -qx "$CL" || kind create cluster --name "$CL" --config k8s/kind/kind-cluster.yaml
kind load docker-image zelox:keytrace --name "$CL"
kubectl --context "$CTX" create ns "$NS" 2>/dev/null || true
sed -E 's/cpu: "[0-9]+"/cpu: "2"/g; s/memory: "16Gi"/memory: "4Gi"/g; s/memory: "26Gi"/memory: "12Gi"/g' k8s/stream/kafka.yaml | kk apply -f - >/dev/null
kk apply -f k8s/kind/minio.yaml >/dev/null
kk rollout status deploy/kafka --timeout=200s; kk rollout status deploy/minio --timeout=120s
KP(){ kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}'; }
kk exec "$(KP)" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic events --partitions 8 --replication-factor 1 >/dev/null 2>&1 || true
echo "###### zelox (ZELOX_KEY_TRACE=1) ######"
cat > /tmp/vjkt.yaml <<YAML
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
          image: zelox:keytrace
          imagePullPolicy: Never
          args: ["server","--ip","0.0.0.0","--port","50051","--mode","local-cluster","--workers","4"]
          ports: [ { containerPort: 50051 } ]
          resources: { requests: { cpu: "4", memory: "8Gi" }, limits: { cpu: "10", memory: "20Gi" } }
          env:
            - { name: RUST_LOG, value: warn }
            - { name: ZELOX_RUNTIME__STACK_SIZE, value: "16777216" }
            - { name: ZELOX_KEY_TRACE, value: "1" }
            - { name: AWS_ENDPOINT, value: "$EP" }
            - { name: AWS_ENDPOINT_URL, value: "$EP" }
            - { name: AWS_ACCESS_KEY_ID, value: "minioadmin" }
            - { name: AWS_SECRET_ACCESS_KEY, value: "minioadmin" }
            - { name: AWS_ALLOW_HTTP, value: "true" }
            - { name: AWS_REGION, value: "us-east-1" }
          volumeMounts: [ { name: data, mountPath: /data } ]
      volumes: [ { name: data, emptyDir: { sizeLimit: 30Gi } } ]
---
apiVersion: v1
kind: Service
metadata: { name: zelox-stream, namespace: stream }
spec: { selector: { app: zelox-stream }, ports: [ { port: 50051, targetPort: 50051 } ] }
YAML
kk apply -f /tmp/vjkt.yaml >/dev/null
kk apply -f k8s/stream/zelox-client.yaml >/dev/null
kk rollout status deploy/zelox-stream --timeout=200s
until kk logs zelox-client 2>/dev/null | grep -q CLIENT_READY; do sleep 5; done
kk cp scripts/scale_producer.py zelox-client:/tmp/scale_producer.py
kk cp scripts/stream_realtime_drain.py zelox-client:/tmp/rt_drain.py
echo "###### produce $N + drain (ZELOX_KEY_TRACE active) ######"
kk exec zelox-client -- sh -c "BOOT=$BOOT TOPIC=events N=$N K=1000 EPMS=100 NP=8 CLOSER_TS=1700000200000 python3 /tmp/scale_producer.py 2>&1 | tail -1" || true
SR="sc://zelox-stream.stream.svc.cluster.local:50051"
kk exec zelox-client -- sh -c "AWS_ENDPOINT=$EP AWS_ENDPOINT_URL=$EP AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_ALLOW_HTTP=true AWS_REGION=us-east-1 SPARK_REMOTE=$SR BOOT=$BOOT TOPIC=events N_EVENTS=$N RT_DUR='2 seconds' MAX_SECS=500 OUT=s3://zelox/rt_out CK=s3://zelox/rt_ck python3 /tmp/rt_drain.py 2>&1" | grep -aiE "ZELOX_COMPLETENESS" | tail -1
VP=$(kk get pod -l app=zelox-stream -o jsonpath='{.items[0].metadata.name}')
echo "###### KEY_TRACE CENSUS (the localization) ######"
kk logs "$VP" 2>/dev/null | grep -a "KEY_TRACE\[" | sort -u
echo "###### S3 per-key (final corruption level) ######"
kk exec zelox-client -- sh -c "AWS_ENDPOINT=$EP python3 -c \"
import boto3,io,pyarrow.parquet as pq,pyarrow.compute as pc
c=boto3.client('s3',endpoint_url='$EP',aws_access_key_id='minioadmin',aws_secret_access_key='minioadmin')
fs=[k['Key'] for k in c.list_objects_v2(Bucket='zelox',Prefix='rt_out/').get('Contents',[]) if k['Key'].endswith('.parquet') and '_spark_metadata' not in k['Key']]
allk=set();rows=0
for f in fs:
 t=pq.read_table(io.BytesIO(c.get_object(Bucket='zelox',Key=f)['Body'].read()));t=t.filter(pc.greater_equal(t.column('k'),0));rows+=t.num_rows;allk|=set(pc.unique(t.column('k')).to_pylist())
print('S3_KEYCHECK files=%d rows=%d distinct_k=%d missing=%d'%(len(fs),rows,len(allk),len(set(range(1000))-allk)))
\" 2>&1 | tail -1"
echo "###### DONE — read KEY_TRACE census: first stage distinct_k<1000 = the bug ######"
