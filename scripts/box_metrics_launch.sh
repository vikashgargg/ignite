#!/usr/bin/env bash
# Confidence validation on ONE mid-size EC2 (32 GiB) instead of an EKS cluster (user rec #3, cheaper).
# Reproduces the REALTIME 100M memory + throughput that an 8 GiB laptop cannot, reusing the SAME kind
# manifests validated at 2M. On-box: build strip=false jemalloc-prof image -> kind -> Kafka+MinIO+Vajra
# -> produce 100M -> Vajra realtime drain (peak RSS + throughput + jeprof heap + correctness) -> Flink
# realtime apples-to-apples -> byte-identical compare. Terminates the box at the end.
# Usage: N=100000000 scripts/box_confidence_100m.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${N:-100000000}"; REGION="${REGION:-ap-south-1}"; ITYPE="${INSTANCE_TYPE:-r7g.2xlarge}"  # 8 vCPU / 64 GiB — Kafka+Vajra headroom for 100M
PROFILE="${PROFILE:-vajra-bench-ec2}"; SG="${SG:-sg-043445d6492980581}"; SUBNET="${SUBNET:-subnet-07d37405bf8df92fa}"
AMI="$(aws ssm get-parameter --region "$REGION" --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64 --query Parameter.Value --output text)"
KEY=/tmp/zelox-metrics-key.pem; KN="zelox-metrics-$$"
mask(){ sed -E 's/[0-9]{12}/<ACCT>/g'; }
KEEP="${KEEP:-0}"   # KEEP=1 leaves the box up for inspection
cleanup(){ set +e;
  if [ "$KEEP" = "0" ]; then
    [ -n "${IID:-}" ] && aws ec2 terminate-instances --region "$REGION" --instance-ids "$IID" >/dev/null 2>&1
    aws ec2 delete-key-pair --region "$REGION" --key-name "$KN" >/dev/null 2>&1; rm -f "$KEY"
    echo "cleanup done (terminated + key removed)"
  else
    # KEEP=1: leave the box AND the key so it stays reachable for inspection/re-run.
    echo "KEEP=1 — box $IID LEFT UP, key at $KEY (ssh -i $KEY ec2-user@${IP:-<ip>}); terminate manually when done"
  fi; }
trap cleanup EXIT

MYIP4="$(curl -4 -s ifconfig.me)"; aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$SG" --protocol tcp --port 22 --cidr "$MYIP4/32" >/dev/null 2>&1 || true
aws ec2 create-key-pair --region "$REGION" --key-name "$KN" --query KeyMaterial --output text > "$KEY"; chmod 600 "$KEY"
IID="$(aws ec2 run-instances --region "$REGION" --image-id "$AMI" --instance-type "$ITYPE" --key-name "$KN" \
  --iam-instance-profile Name="$PROFILE" --security-group-ids "$SG" --subnet-id "$SUBNET" --associate-public-ip-address \
  --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":100,"VolumeType":"gp3"}}]' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=zelox-metrics}]' --query 'Instances[0].InstanceId' --output text)"
echo "launched $IID ($ITYPE, 32 GiB); waiting running..."; aws ec2 wait instance-running --region "$REGION" --instance-ids "$IID"
IP="$(aws ec2 describe-instances --region "$REGION" --instance-ids "$IID" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)"
SSHO="-o StrictHostKeyChecking=no -o ConnectTimeout=5 -i $KEY ec2-user@$IP"
echo "waiting sshd ${IP%.*}.x ..."; for i in $(seq 1 40); do ssh $SSHO true 2>/dev/null && break; sleep 5; done

# deps: docker, kind, kubectl
ssh $SSHO 'sudo dnf install -y docker git >/dev/null 2>&1 && sudo systemctl start docker && sudo usermod -aG docker ec2-user
[ -x /usr/local/bin/kind ] || { sudo curl -sLo /usr/local/bin/kind https://kind.sigs.k8s.io/dl/v0.24.0/kind-linux-arm64 && sudo chmod +x /usr/local/bin/kind; }
[ -x /usr/local/bin/kubectl ] || { sudo curl -sLo /usr/local/bin/kubectl "https://dl.k8s.io/release/v1.30.0/bin/linux/arm64/kubectl" && sudo chmod +x /usr/local/bin/kubectl; }' </dev/null
rsync -az --delete --exclude target --exclude .git --exclude .venvs --exclude node_modules --exclude '*.parquet' \
  -e "ssh -o StrictHostKeyChecking=no -i $KEY" ./ ec2-user@"$IP":~/zelox/

# On-box: build strip=false prof image, run the 100M realtime confidence, report. (see scripts/box_full_metrics.sh)
scp -o StrictHostKeyChecking=no -i "$KEY" scripts/box_full_metrics.sh ec2-user@"$IP":~/zelox/scripts/
ssh $SSHO "cd ~/zelox && sg docker -c 'N=$N bash scripts/box_full_metrics.sh'" 2>&1 | mask
echo "=== confidence run complete (IID=$IID, KEEP=$KEEP) ==="
