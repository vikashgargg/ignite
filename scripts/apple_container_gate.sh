#!/usr/bin/env bash
# ZELOX-K1 — Apple `container` local smoke gate.
#
# Proves the SAME published multi-arch image that runs on EKS also runs on Apple `container`
# (native macOS arm64 runtime, Linux/arm64 VM). One command, exits 0 = green.
#
#   scripts/apple_container_gate.sh [IMAGE]
#   IMAGE defaults to ghcr.io/vikashgargg/ignite:edge (override to test a local build or a tag).
#
# Requires: macOS with the `container` CLI (>=1.0.0) and a Python with pyspark-client installed
# (a Zelox venv, e.g. ~/.local/lib/zelox/venv, or any venv with `pip install pyspark`).
set -euo pipefail

IMAGE="${1:-ghcr.io/vikashgargg/ignite:edge}"
NAME="zelox-gate"
PORT="50051"
PY="${ZELOX_PY:-python3}"

log() { printf '\n\033[1;36m[gate]\033[0m %s\n' "$*"; }
cleanup() { container stop "$NAME" >/dev/null 2>&1 || true; container rm "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

command -v container >/dev/null || { echo "FAIL: Apple 'container' CLI not found"; exit 1; }

log "ensuring builder is up"
container system start >/dev/null 2>&1 || true

log "pulling image: $IMAGE"
container image pull "$IMAGE"

log "starting Zelox server in a container"
cleanup
container run --rm --detach --name "$NAME" -p "${PORT}:${PORT}" "$IMAGE" server --ip 0.0.0.0 --mode local >/dev/null

log "waiting for Spark Connect on :$PORT"
for i in $(seq 1 60); do
  if nc -z localhost "$PORT" 2>/dev/null; then echo "  port open (attempt $i)"; break; fi
  [ "$i" = 60 ] && { echo "FAIL: server did not open :$PORT"; container logs "$NAME" | tail -20; exit 1; }
  sleep 2
done

log "running PySpark smoke query against the container"
SPARK_REMOTE="sc://localhost:${PORT}" "$PY" - <<'PYEOF'
from pyspark.sql import SparkSession, functions as F
import os, sys
s = SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).getOrCreate()
n = 1_000_000
got = s.range(n).select(F.sum("id")).collect()[0][0]
want = n * (n - 1) // 2
assert got == want, f"sum mismatch: got={got} want={want}"
rows = s.range(100).groupBy((F.col("id") % 7).alias("k")).count().count()
assert rows == 7, f"distinct keys: got={rows} want=7"
print(f"OK: sum(range({n}))={got} (bit-exact), groupBy distinct=7")
PYEOF

log "PASS — published image runs on Apple container, results bit-exact vs Spark semantics"
echo "GATE_RESULT apple_container=PASS image=$IMAGE"
