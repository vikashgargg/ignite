#!/usr/bin/env bash
# EPIC-M / M1: profile the realtime-passthrough memory on a throwaway arm64 EC2 box (kind can't reach the
# 13 GiB). Runs Kafka + Vajra rt43 (server) + a pyspark-4.2 realtime passthrough in Docker, then reads the
# Vajra container's cgroup memory.stat (ANON = real heap/buffers vs FILE = reclaimable page cache) +
# VAJRA_KAFKA_STATS (prefetch bytes) to ATTRIBUTE the RSS. Terminates the box. Grounds M2-M4 before any code.
# Usage: scripts/mem_profile_ec2.sh
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
REGION="${REGION:-ap-south-1}"; ITYPE="${INSTANCE_TYPE:-c7g.4xlarge}"
PROFILE="${PROFILE:-vajra-bench-ec2}"; SG="${SG:-sg-043445d6492980581}"; SUBNET="${SUBNET:-subnet-07d37405bf8df92fa}"
AMI="$(aws ssm get-parameter --region "$REGION" --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64 --query Parameter.Value --output text)"
ECR="$(aws ecr describe-repositories --region "$REGION" --repository-name vajra --query 'repositories[0].repositoryUri' --output text)"; REG="${ECR%/vajra}"
KEY=/tmp/vajra-prof-key.pem; KN="vajra-prof-key-$$"
mask(){ sed -E 's/[0-9]{12}/<ACCT>/g'; }
cleanup(){ set +e; [ -n "${IID:-}" ] && aws ec2 terminate-instances --region "$REGION" --instance-ids "$IID" >/dev/null 2>&1; aws ec2 delete-key-pair --region "$REGION" --key-name "$KN" >/dev/null 2>&1; rm -f "$KEY"; echo "profiler box terminated"; }
trap cleanup EXIT
MYIP4="$(curl -4 -s ifconfig.me)"; aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$SG" --protocol tcp --port 22 --cidr "$MYIP4/32" >/dev/null 2>&1 || true
aws ec2 create-key-pair --region "$REGION" --key-name "$KN" --query KeyMaterial --output text > "$KEY"; chmod 600 "$KEY"
IID="$(aws ec2 run-instances --region "$REGION" --image-id "$AMI" --instance-type "$ITYPE" --key-name "$KN" \
  --iam-instance-profile Name="$PROFILE" --security-group-ids "$SG" --subnet-id "$SUBNET" --associate-public-ip-address \
  --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":40,"VolumeType":"gp3"}}]' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=vajra-mem-profiler}]' --query 'Instances[0].InstanceId' --output text)"
echo "launched $IID; waiting running..."; aws ec2 wait instance-running --region "$REGION" --instance-ids "$IID"
IP="$(aws ec2 describe-instances --region "$REGION" --instance-ids "$IID" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)"
SSH="ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -i $KEY ec2-user@$IP"
for i in $(seq 1 30); do $SSH true 2>/dev/null && break; sleep 5; done

# Remote profiling script: docker net + Kafka + Vajra rt43 server + backlog + realtime passthrough,
# sampling the Vajra container cgroup memory.stat (anon=heap/buffers vs file=page cache) to ATTRIBUTE RSS.
$SSH "cat > /tmp/prof.sh" <<REMOTE
set -uo pipefail
sudo dnf install -y docker >/dev/null 2>&1 || sudo yum install -y docker >/dev/null 2>&1
sudo systemctl start docker
aws ecr get-login-password --region $REGION | sudo docker login --username AWS --password-stdin $REG >/dev/null 2>&1
sudo docker network create vnet >/dev/null 2>&1
sudo docker run -d --name kafka --network vnet -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e CLUSTER_ID=prof-cluster-000000 apache/kafka:3.8.0 >/dev/null 2>&1
sleep 25
sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic lat_in --partitions 16 --replication-factor 1 >/dev/null 2>&1
sudo docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic lat_out --partitions 16 --replication-factor 1 >/dev/null 2>&1
# Vajra server — CORRECT args (from k8s manifest): server --ip 0.0.0.0 --port 50051 --mode local-cluster --workers 4
sudo docker run -d --name vajra --network vnet -e RUST_LOG=warn -e VAJRA_KAFKA_STATS=1 \
  $ECR:rt43 server --ip 0.0.0.0 --port 50051 --mode local-cluster --workers 4 >/dev/null 2>&1
# client (pyspark 4.2)
sudo docker run -d --name client --network vnet python:3.12-slim sleep infinity >/dev/null 2>&1
sudo docker exec client pip install -q pyspark-client==4.2.0 grpcio grpcio-status protobuf googleapis-common-protos pandas confluent-kafka >/dev/null 2>&1
sleep 10; echo "containers:"; sudo docker ps --format '{{.Names}} {{.Status}}'
# produce a 20M backlog into lat_in
sudo docker exec client python3 - <<'PY'
import os
from confluent_kafka import Producer
p=Producer({"bootstrap.servers":"kafka:9092","linger.ms":50,"batch.size":1048576,"compression.type":"lz4","queue.buffering.max.messages":2000000})
import json
for i in range(20000000):
    p.produce("lat_in", partition=i%16, value=json.dumps({"k":i%1000,"ts":1700000000000+i//1000,"v":1}))
    if (i & 0x3FFFF)==0: p.poll(0)
p.flush(); print("PRODUCED 20M")
PY
# realtime passthrough (background) + sample memory.stat every 5s
sudo docker exec client python3 - <<'PY' &
from pyspark.sql import SparkSession, functions as F
import time
s=SparkSession.builder.remote("sc://vajra:50051").getOrCreate()
raw=s.readStream.format("kafka").option("kafka.bootstrap.servers","kafka:9092").option("subscribe","lat_in").option("startingOffsets","earliest").load()
q=raw.select(F.col("value")).writeStream.format("kafka").option("kafka.bootstrap.servers","kafka:9092").option("topic","lat_out").option("checkpointLocation","/tmp/ck_pt").trigger(realTime="5 seconds").start()
time.sleep(90)
try: q.stop()
except Exception: pass
print("PASSTHROUGH_DONE")
PY
echo "=== MEMORY.STAT SAMPLES (anon=heap/buffers, file=page cache) + RSS ==="
for i in \$(seq 1 18); do
  sleep 5
  ANON=\$(sudo docker exec vajra sh -c 'cat /sys/fs/cgroup/memory.stat 2>/dev/null | awk "/^anon /{print \\\$2}"')
  FILE=\$(sudo docker exec vajra sh -c 'cat /sys/fs/cgroup/memory.stat 2>/dev/null | awk "/^file /{print \\\$2}"')
  CUR=\$(sudo docker exec vajra sh -c 'cat /sys/fs/cgroup/memory.current 2>/dev/null')
  echo "  t=\${i}0s anon=\$(( \${ANON:-0}/1048576 ))MiB file=\$(( \${FILE:-0}/1048576 ))MiB rss=\$(( \${CUR:-0}/1048576 ))MiB"
done
echo "=== KAFKA_STATS (prefetch) ==="; sudo docker logs vajra 2>&1 | grep -a KAFKA_STATS | tail -3
echo "REMOTE_DONE"
REMOTE
$SSH "bash /tmp/prof.sh" 2>&1 | mask | grep -aiE "containers:|PRODUCED|t=[0-9]+0s anon|KAFKA_STATS|REMOTE_DONE|error" | tail -30
echo "PROFILE_DONE (box terminates on exit)"
