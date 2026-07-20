#!/usr/bin/env bash
# Flink realtime windowed-COUNT -> S3(MinIO) parquet on the box: deploy S3-sink session (MinIO endpoint
# injected), submit flink-sql-s3.sql, wait until 10 windows land, then S3-verify + TM peak RSS + drain time.
# Reuses the flow validated live (both engines byte-identical on MinIO). Usage: box_flink_s3_phase.sh [N]
set -uo pipefail
cd "$HOME/zelox"; export KUBECONFIG=$HOME/.kube/config
N="${1:-100000000}"; CTX=kind-zelox-conf; NS=stream
K(){ kubectl --context "$CTX" -n "$NS" "$@"; }
EP="http://minio.stream.svc.cluster.local:9000"
K delete deploy flink-jm flink-tm --ignore-not-found >/dev/null 2>&1; K delete job flink-s3 --ignore-not-found >/dev/null 2>&1; sleep 5
sed -E 's#taskmanager.numberOfTaskSlots: 16#taskmanager.numberOfTaskSlots: 8#; s#taskmanager.memory.process.size: 24576m#taskmanager.memory.process.size: 12288m#; s#taskmanager.memory.managed.fraction: 0.5#taskmanager.memory.managed.fraction: 0.3\n    s3.endpoint: '"$EP"'\n    s3.path.style.access: true\n    s3.access-key: minioadmin\n    s3.secret-key: minioadmin#; /nodeSelector/d; /role: kafka/d; /role: compute/d; s/cpu: "1[0-9]"/cpu: "6"/g; s/memory: "2[0-9]Gi"/memory: "13Gi"/g' \
  k8s/stream/flink-session-s3-sink.yaml | K apply -f - >/dev/null
K wait --for=condition=available --timeout=300s deployment/flink-jm >/dev/null 2>&1 || echo FLINK_WARN_jm
K wait --for=condition=available --timeout=300s deployment/flink-tm >/dev/null 2>&1 || echo FLINK_WARN_tm
JM=$(K get pod -l app=flink,component=jm -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
for i in $(seq 1 40); do K exec "$JM" -c jobmanager -- sh -c 'curl -sf localhost:8081/overview >/dev/null' 2>/dev/null && break; sleep 5; done
K create configmap flink-sql-s3 --from-file=flink-sql.sql=k8s/stream/flink-sql-s3.sql --dry-run=client -o yaml | K apply -f - >/dev/null
K delete job flink-s3 --ignore-not-found >/dev/null 2>&1
sed -e 's/name: flink-runner/name: flink-s3/' -e 's/{ name: flink-sql }/{ name: flink-sql-s3 }/g' -e 's/name: flink-sql,/name: flink-sql-s3,/g' k8s/stream/flink-runner-job-s3.yaml | K apply -f - >/dev/null
t0=$(date +%s); FDRAIN=""
countwin(){ K exec vajra-client -- python3 -c "import boto3,io,pyarrow.parquet as pq;c=boto3.client('s3',endpoint_url='$EP',aws_access_key_id='minioadmin',aws_secret_access_key='minioadmin');ks=[x['Key'] for x in c.list_objects_v2(Bucket='vajra',Prefix='flink_out/').get('Contents',[]) if 'part-' in x['Key'] and '.inprogress' not in x['Key']];w=set();r=0;s=0
for k in ks:
 t=pq.read_table(io.BytesIO(c.get_object(Bucket='vajra',Key=k)['Body'].read()));r+=t.num_rows;s+=sum(t.column('cnt').to_pylist());w.update(str(x) for x in t.column('window_start').to_pylist())
print(len(w),r,s)" 2>/dev/null; }
for i in $(seq 1 200); do
  sleep 5; read W R S <<<"$(countwin)"
  el=$(( $(date +%s) - t0 )); [ $((i % 6)) -eq 0 ] && echo "  flink t=${el}s windows=${W:-0} rows=${R:-0}"
  [ "${W:-0}" -ge 10 ] 2>/dev/null && { FDRAIN=$el; break; }
done
read W R S <<<"$(countwin)"
TM=$(K get pod -l app=flink,component=tm -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
FMEM=$(K exec "$TM" -c taskmanager -- sh -c 'cat /sys/fs/cgroup/memory.peak' 2>/dev/null)
[ -z "$FDRAIN" ] && FDRAIN=$(( $(date +%s) - t0 ))
awk -v d="$FDRAIN" -v n="$N" -v m="${FMEM:-0}" 'BEGIN{printf "FLINK_DRAIN drain_s=%d throughput=%.3fM_ev/s\n",d,n/d/1e6}'
echo "FLINK_TM_MEM peakRSS=$(awk "BEGIN{printf \"%.2f\",${FMEM:-0}/1073741824}")GiB"
echo "FLINK_S3_CORRECT windows=${W:-0} rows=${R:-0} sum=${S:-0} EXACT=$([ "${W:-0}" = 10 ] && [ "${R:-0}" = 10000 ] && [ "${S:-0}" = "$N" ] && echo YES || echo NO)"
