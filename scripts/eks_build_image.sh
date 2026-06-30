#!/usr/bin/env bash
# Fast arm64 vajra image build via a throwaway c7g EC2 (native, no Docker-Desktop emulation), pushed to
# ECR. Reuses the prior bench resources (instance profile vajra-bench-ec2 = ECR push, SG vajra-build-sg
# = SSH, public subnet) and a fresh temp key. Builds from the LOCAL working tree (rsync, no GitHub
# auth). Terminates the builder + cleans up at the end. ~8min build on c7g.4xlarge (16 native cores).
#
# Usage: scripts/eks_build_image.sh [TAG]            (default: wmprof)
#        INSTANCE_TYPE=c7g.8xlarge scripts/eks_build_image.sh wmprof   (faster)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
TAG="${1:-wmprof}"; REGION="${REGION:-ap-south-1}"; ITYPE="${INSTANCE_TYPE:-c7g.4xlarge}"
PROFILE="${PROFILE:-vajra-bench-ec2}"; SG="${SG:-sg-043445d6492980581}"; SUBNET="${SUBNET:-subnet-07d37405bf8df92fa}"
AMI="$(aws ssm get-parameter --region "$REGION" --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64 --query Parameter.Value --output text)"
ECR="$(aws ecr describe-repositories --region "$REGION" --repository-name vajra --query 'repositories[0].repositoryUri' --output text)"; REG="${ECR%/vajra}"
KEY=/tmp/vajra-img-key.pem; KN="vajra-img-key-$$"
mask(){ sed -E 's/[0-9]{12}/<ACCT>/g'; }
echo "build -> $(echo "$ECR" | mask):$TAG  on $ITYPE"

cleanup(){ set +e; [ -n "${IID:-}" ] && aws ec2 terminate-instances --region "$REGION" --instance-ids "$IID" >/dev/null 2>&1; aws ec2 delete-key-pair --region "$REGION" --key-name "$KN" >/dev/null 2>&1; rm -f "$KEY"; echo "builder terminated, key removed"; }
trap cleanup EXIT

# fresh key + allow SSH from this host's IPv4
MYIP4="$(curl -4 -s ifconfig.me)"; aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$SG" --protocol tcp --port 22 --cidr "$MYIP4/32" >/dev/null 2>&1 || true
aws ec2 create-key-pair --region "$REGION" --key-name "$KN" --query KeyMaterial --output text > "$KEY"; chmod 600 "$KEY"
IID="$(aws ec2 run-instances --region "$REGION" --image-id "$AMI" --instance-type "$ITYPE" --key-name "$KN" \
  --iam-instance-profile Name="$PROFILE" --security-group-ids "$SG" --subnet-id "$SUBNET" --associate-public-ip-address \
  --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":60,"VolumeType":"gp3"}}]' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=vajra-img-builder}]' --query 'Instances[0].InstanceId' --output text)"
echo "launched $IID; waiting running..."; aws ec2 wait instance-running --region "$REGION" --instance-ids "$IID"
IP="$(aws ec2 describe-instances --region "$REGION" --instance-ids "$IID" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)"
SSHO="-o StrictHostKeyChecking=no -o ConnectTimeout=5 -i $KEY ec2-user@$IP"
echo "waiting sshd on ${IP%.*}.x ..."; for i in $(seq 1 40); do ssh $SSHO true 2>/dev/null && break; sleep 5; done

ssh $SSHO 'sudo dnf install -y docker >/dev/null 2>&1 && sudo systemctl start docker' </dev/null
rsync -az --delete --exclude target --exclude .git --exclude .venvs --exclude node_modules --exclude '*.parquet' \
  -e "ssh -o StrictHostKeyChecking=no -i $KEY" ./ ec2-user@"$IP":~/ignite/
ssh $SSHO "bash -s" <<REMOTE
set -e
aws ecr get-login-password --region $REGION | sudo docker login --username AWS --password-stdin $REG >/dev/null 2>&1
cd ~/ignite && sudo docker build -f docker/Dockerfile -t $REG/vajra:$TAG . && sudo docker push $REG/vajra:$TAG
REMOTE
echo "PUSHED $(echo "$REG" | mask)/vajra:$TAG"
