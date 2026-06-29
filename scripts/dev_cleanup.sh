#!/usr/bin/env bash
# Dev environment cleanup — free memory/disk + recover a wedged Docker after long streaming-test
# sessions (the gates/head-to-head leave servers, Kafka, /tmp data, and can wedge the Docker daemon).
# Usage: bash scripts/dev_cleanup.sh            # kill stale procs + free /tmp + force-restart Docker
#        SKIP_DOCKER=1 bash scripts/dev_cleanup.sh   # procs + /tmp only (don't touch Docker)
set -uo pipefail

echo "== killing stale Vajra/test processes (frees memory) =="
for pat in 'target/debug/vajra' 'target/release/vajra' state_scale_stress inc_ckpt_gate \
           local_headtohead correctness_gate f5_validate f3c_stateful_crash kafka-console-producer \
           'docker (restart|ps|run|exec)'; do
  pkill -9 -f "$pat" 2>/dev/null && echo "  killed: $pat" || true
done

echo "== freeing /tmp + /private/tmp test artifacts =="
rm -rf /tmp/incckpt_* /tmp/h2h /tmp/prof /tmp/f5val.* /tmp/f5cmp.* /tmp/cgate_* /tmp/flink_out.log 2>/dev/null || true
find /private/tmp/claude-* -name '*.output' -size +200k -delete 2>/dev/null || true

if [ "${SKIP_DOCKER:-0}" != "1" ]; then
  echo "== force-restarting Docker Desktop (daemon often wedges under load) =="
  osascript -e 'quit app "Docker"' 2>/dev/null || true; sleep 3
  pkill -9 -f 'com.docker.backend' 2>/dev/null || true; pkill -9 -f 'Docker Desktop' 2>/dev/null || true; sleep 2
  open -a Docker 2>/dev/null && echo "  Docker restarting; wait for it: until docker info >/dev/null 2>&1; do sleep 2; done"
fi

echo "== disk =="; df -h / | tail -1
echo "done. Build artifacts: 'rm -rf target/release' (rebuildable) frees the most if disk is tight."
