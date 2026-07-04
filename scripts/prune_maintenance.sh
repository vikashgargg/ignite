#!/usr/bin/env bash
# Periodic build-cache + docker maintenance to keep disk healthy WITHOUT forcing slow full rebuilds.
# - Always: drop `target/*/incremental` (the biggest, most-churny artifacts — regenerated cheaply) + docker
#   dangling images + build cache.
# - Only when disk is critically low (< THRESHOLD_GB free): full `cargo clean` + `docker system prune`.
# Scheduled via launchd (~/Library/LaunchAgents/com.vajra.prune.plist) or run manually.
# Never touches a build in progress destructively beyond `incremental` (cargo regenerates it).
set -uo pipefail
ROOT="${VAJRA_ROOT:-/Users/vikashgarg/Desktop/ignite}"
THRESHOLD_GB="${THRESHOLD_GB:-25}"
LOG=/tmp/vajra_prune.log
cd "$ROOT" 2>/dev/null || exit 0

# Skip if a cargo/rustc build is actively running (don't disturb an in-flight compile).
if pgrep -x rustc >/dev/null 2>&1 || pgrep -x cargo >/dev/null 2>&1; then
  echo "$(date '+%F %T') skip: build in progress" >> "$LOG"; exit 0
fi

FREE_GB=$(df -g . 2>/dev/null | awk 'NR==2{print $4}')
INCR_BEFORE=$(du -sh target/*/incremental 2>/dev/null | awk '{print $1}' | paste -sd, -)
rm -rf target/*/incremental 2>/dev/null
docker builder prune -f >/dev/null 2>&1 || true
docker image prune -f     >/dev/null 2>&1 || true

if [ "${FREE_GB:-999}" -lt "$THRESHOLD_GB" ]; then
  echo "$(date '+%F %T') LOW disk (${FREE_GB}GB < ${THRESHOLD_GB}GB) -> cargo clean + docker system prune" >> "$LOG"
  cargo clean >/dev/null 2>&1 || true
  docker system prune -f >/dev/null 2>&1 || true
fi

FREE_AFTER=$(df -g . 2>/dev/null | awk 'NR==2{print $4}')
echo "$(date '+%F %T') pruned incremental(${INCR_BEFORE:-none}); free ${FREE_GB}GB -> ${FREE_AFTER}GB" >> "$LOG"
