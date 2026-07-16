#!/usr/bin/env bash
# Preflight disk/docker health guard — run at the START of any container/build/kind/EKS task so a full
# disk never again silently breaks the tooling (kubectl hangs, kind OOMKills, docker pull fails). Prunes
# docker + local cargo target when space is low; ABORTS if critically low so we fix disk before "debugging"
# phantom tooling bugs. Usage: bash scripts/preflight_disk.sh  (exit 0 = healthy/pruned, exit 1 = critical)
set -uo pipefail
MIN_GB="${MIN_GB:-15}"        # prune below this
CRIT_GB="${CRIT_GB:-6}"       # abort below this (after pruning)
free_gb() { df -g . 2>/dev/null | awk 'NR==2{print $4}'; }

FG=$(free_gb); echo "disk free: ${FG}Gi (prune<${MIN_GB}, abort<${CRIT_GB})"
if [ "${FG:-0}" -lt "$MIN_GB" ]; then
  echo "→ low disk: pruning docker + cargo target..."
  docker container prune -f >/dev/null 2>&1
  docker image prune -f >/dev/null 2>&1
  docker builder prune -f >/dev/null 2>&1
  # local target is disposable (we build on EC2); reclaim it
  [ -d target ] && du -sg target 2>/dev/null | awk '$1>5{print "  cargo clean ("$1"Gi target)"}' && cargo clean >/dev/null 2>&1
  FG=$(free_gb); echo "  after prune: ${FG}Gi free"
fi
if [ "${FG:-0}" -lt "$CRIT_GB" ]; then
  echo "✗ CRITICAL: ${FG}Gi free after pruning. Free disk (remove big files / docker volumes) BEFORE running container/build tasks — do NOT debug 'flaky' kubectl/kind/pull until fixed." >&2
  exit 1
fi
# quick docker health
docker info >/dev/null 2>&1 && echo "✓ docker daemon healthy" || echo "⚠ docker daemon not responding — restart Docker Desktop"
