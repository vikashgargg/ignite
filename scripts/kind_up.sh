#!/usr/bin/env bash
# T2 tier bring-up: create the local kind cluster (k8s in Docker) that mirrors the EKS streaming topology,
# then make the vajra image available to it. FREE, local — run BEFORE any EKS spend (docs/design/three-tier-sdlc.md).
# The image is loaded from local Docker; pull it from ECR first if you only have it there:
#   aws ecr get-login-password --region ap-south-1 | docker login --username AWS --password-stdin <ECR>
#   docker pull <ECR>/vajra:TAG && docker tag <ECR>/vajra:TAG vajra:TAG
# Usage: TAG=realtime-fix scripts/kind_up.sh
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
TAG="${TAG:-realtime-fix}"; CLUSTER="${CLUSTER:-vajra-kind}"
kind get clusters 2>/dev/null | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER" --config k8s/kind/kind-cluster.yaml
kubectl cluster-info --context "kind-$CLUSTER" >/dev/null 2>&1 && echo "kind cluster '$CLUSTER' up"
echo "nodes:"; kubectl get nodes -L role --no-headers
# Load the vajra image into the kind nodes (must exist in local Docker; see header for the ECR pull).
if docker image inspect "vajra:$TAG" >/dev/null 2>&1; then
  echo "loading vajra:$TAG into kind..."; kind load docker-image "vajra:$TAG" --name "$CLUSTER"
else
  echo "WARN: local image vajra:$TAG not found — pull+tag it from ECR first (see header) before running the T2 test"
fi
kubectl get ns stream >/dev/null 2>&1 || kubectl create ns stream
echo "T2 kind ready. Next: TAG=$TAG scripts/kind_streaming_test.sh"
