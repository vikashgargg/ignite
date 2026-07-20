#!/usr/bin/env bash
# Root-cause the realtime windowed-agg KEY-CORRUPTION bug (10000 rows/sum correct but k column scrambled:
# distinct_k=590 not 1000). Isolates the cause across a config matrix, each a small 10M realtime drain to
# MinIO S3, verifying distinct_k==1000. Runs on the live box. Usage: box_rootcause_keys.sh
set -uo pipefail
cd "$HOME/zelox"; export KUBECONFIG=$HOME/.kube/config
CTX=kind-zelox-conf; NS=stream; N=10000000
K(){ kubectl --context "$CTX" -n "$NS" "$@"; }
EP="http://minio.stream.svc.cluster.local:9000"; BOOT="kafka.stream.svc.cluster.local:9092"
S3ENV="AWS_ENDPOINT=$EP AWS_ENDPOINT_URL=$EP AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_ALLOW_HTTP=true AWS_REGION=us-east-1"
KP(){ K get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}'; }
K delete deploy flink-jm flink-tm --ignore-not-found >/dev/null 2>&1  # free RAM

deploy(){ # $1=workers $2=t7fuse(0/1) $3=name
  cat > /tmp/rc.yaml <<YAML
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
          args: ["server","--ip","0.0.0.0","--port","50051","--mode","local-cluster","--workers","$1"]
          ports: [ { containerPort: 50051 } ]
          resources: { requests: { cpu: "3", memory: "6Gi" }, limits: { cpu: "6", memory: "16Gi" } }
          env:
            - { name: RUST_LOG, value: warn }
            - { name: SAIL_RUNTIME__STACK_SIZE, value: "16777216" }
            - { name: AWS_ENDPOINT, value: "$EP" }
            - { name: AWS_ENDPOINT_URL, value: "$EP" }
            - { name: AWS_ACCESS_KEY_ID, value: "minioadmin" }
            - { name: AWS_SECRET_ACCESS_KEY, value: "minioadmin" }
            - { name: AWS_ALLOW_HTTP, value: "true" }
            - { name: AWS_REGION, value: "us-east-1" }
            - { name: VAJRA_T7_FUSE, value: "$2" }
      volumes: []
---
apiVersion: v1
kind: Service
metadata: { name: vajra-stream, namespace: stream }
spec: { selector: { app: vajra-stream }, ports: [ { port: 50051, targetPort: 50051 } ] }
YAML
  K apply -f /tmp/rc.yaml >/dev/null; K rollout status deploy/vajra-stream --timeout=150s >/dev/null; }

checkkeys(){ # $1 = out prefix
  K exec vajra-client -- sh -c "$S3ENV OUTP=$1 python3 - <<'PY'
import boto3,io,os,pyarrow.parquet as pq,pyarrow.compute as pc
c=boto3.client('s3',endpoint_url=os.environ['AWS_ENDPOINT'],aws_access_key_id='minioadmin',aws_secret_access_key='minioadmin')
fs=[k['Key'] for k in c.list_objects_v2(Bucket='vajra',Prefix=os.environ['OUTP']).get('Contents',[]) if k['Key'].endswith('.parquet') and '_spark_metadata' not in k['Key']]
allk=set();rows=0
for f in fs:
 t=pq.read_table(io.BytesIO(c.get_object(Bucket='vajra',Key=f)['Body'].read()));t=t.filter(pc.greater_equal(t.column('k'),0));rows+=t.num_rows;allk|=set(pc.unique(t.column('k')).to_pylist())
print('distinct_k=%d rows=%d missing=%d KEYS_OK=%s'%(len(allk),rows,len(set(range(1000))-allk),len(allk)==1000))
PY" 2>&1 | tail -1; }

run(){ # $1=workers $2=fuse $3=topic $4=outprefix $5=label
  echo "===== CASE $5 (workers=$1 T7_FUSE=$2) ====="
  K exec "$(KP)" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $3 --partitions 8 --replication-factor 1 >/dev/null 2>&1 || true
  K exec vajra-client -- sh -c "BOOT=$BOOT TOPIC=$3 N=$N K=1000 EPMS=100 NP=8 CLOSER_TS=1700000200000 python3 /tmp/scale_producer.py >/dev/null 2>&1" || true
  deploy "$1" "$2" "$5"
  K exec vajra-client -- sh -c "$S3ENV SPARK_REMOTE=sc://vajra-stream.stream.svc.cluster.local:50051 BOOT=$BOOT TOPIC=$3 N_EVENTS=$N RT_DUR='2 seconds' MAX_SECS=180 OUT=s3://vajra/$4 CK=s3://vajra/${4}ck python3 /tmp/rt_drain.py 2>&1 | grep -aiE 'VAJRA_COMPLETENESS' | tail -1" || true
  echo -n "  RESULT $5: "; checkkeys "$4/"
}

# ensure client + scripts present
K get pod vajra-client >/dev/null 2>&1 || K apply -f k8s/stream/vajra-client.yaml >/dev/null
until K logs vajra-client 2>/dev/null | grep -q CLIENT_READY; do sleep 5; done
K cp scripts/scale_producer.py vajra-client:/tmp/scale_producer.py; K cp scripts/stream_realtime_drain.py vajra-client:/tmp/rt_drain.py

run 4 1 rc_a rc_out_a "A_repro_fuse1_w4"
run 4 0 rc_b rc_out_b "B_fuse0_w4"
run 1 1 rc_c rc_out_c "C_fuse1_w1"
echo "===== ROOT-CAUSE MATRIX DONE (KEYS_OK=True means that config is correct) ====="
