#!/usr/bin/env bash
# Tear down EVERYTHING created for the Vajra EKS scale benchmark and verify
# nothing cost-incurring is left behind. Idempotent — safe to re-run.
#
# Usage: scripts/aws_eks_teardown.sh [cluster-name] [region] [s3-bucket] [ecr-repo]
set -uo pipefail

CLUSTER="${1:-vajra-scale}"
REGION="${2:-ap-south-1}"
BUCKET="${3:-}"            # optional; pass the data bucket to delete it
ECR_REPO="${4:-vajra}"

echo "================ Vajra EKS teardown (region=$REGION) ================"

# 1. Delete the EKS cluster (removes its CloudFormation stack: VPC, subnets,
#    nodegroup, EC2 nodes, security groups — everything eksctl created).
if eksctl get cluster --name "$CLUSTER" --region "$REGION" >/dev/null 2>&1; then
  echo "[1/5] Deleting EKS cluster '$CLUSTER' (this also removes nodes/VPC)..."
  eksctl delete cluster --name "$CLUSTER" --region "$REGION" --disable-nozzle 2>/dev/null \
    || eksctl delete cluster --name "$CLUSTER" --region "$REGION"
else
  echo "[1/5] No EKS cluster '$CLUSTER' found (already gone)."
fi

# 2. Delete the ECR repository (force = delete images too).
if aws ecr describe-repositories --repository-names "$ECR_REPO" --region "$REGION" >/dev/null 2>&1; then
  echo "[2/5] Deleting ECR repo '$ECR_REPO'..."
  aws ecr delete-repository --repository-name "$ECR_REPO" --region "$REGION" --force >/dev/null
else
  echo "[2/5] No ECR repo '$ECR_REPO'."
fi

# 3. Empty + delete the S3 data bucket (if provided).
if [ -n "$BUCKET" ] && aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "[3/5] Emptying + deleting S3 bucket '$BUCKET'..."
  aws s3 rm "s3://$BUCKET" --recursive >/dev/null 2>&1 || true
  aws s3api delete-bucket --bucket "$BUCKET" --region "$REGION" >/dev/null 2>&1 || true
else
  echo "[3/5] No S3 bucket to delete (pass it as arg 3 if needed)."
fi

# 4. Belt-and-suspenders: orphaned CloudFormation stacks from eksctl.
echo "[4/5] Checking for leftover eksctl CloudFormation stacks..."
aws cloudformation list-stacks --region "$REGION" \
  --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE ROLLBACK_COMPLETE \
  --query "StackSummaries[?contains(StackName, 'eksctl-$CLUSTER')].StackName" --output text 2>/dev/null \
  | tr '\t' '\n' | while read -r s; do
      [ -n "$s" ] && { echo "  deleting stack $s"; aws cloudformation delete-stack --stack-name "$s" --region "$REGION"; }
    done

# 5. VERIFY nothing cost-incurring remains in the region.
echo "[5/5] Verifying no leftover billable resources in $REGION ..."
echo "  EKS clusters    : $(aws eks list-clusters --region "$REGION" --query clusters --output text 2>/dev/null || echo '?')"
echo "  Running EC2     : $(aws ec2 describe-instances --region "$REGION" --filters Name=instance-state-name,Values=running,pending --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null || echo '?')"
echo "  NAT gateways    : $(aws ec2 describe-nat-gateways --region "$REGION" --filter Name=state,Values=available,pending --query 'NatGateways[].NatGatewayId' --output text 2>/dev/null || echo '?')"
echo "  Load balancers  : $(aws elbv2 describe-load-balancers --region "$REGION" --query 'LoadBalancers[].LoadBalancerArn' --output text 2>/dev/null || echo 'none')"
echo "  Unattached EBS  : $(aws ec2 describe-volumes --region "$REGION" --filters Name=status,Values=available --query 'Volumes[].VolumeId' --output text 2>/dev/null || echo 'none')"
echo "  Elastic IPs     : $(aws ec2 describe-addresses --region "$REGION" --query 'Addresses[].AllocationId' --output text 2>/dev/null || echo 'none')"
echo "===================================================================="
echo "If all lines above are empty/none, you are at \$0 ongoing cost."
echo "Also confirm the EKS control plane is gone (no cluster listed)."
