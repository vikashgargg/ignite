#!/usr/bin/env bash
# Distributed mode validation: Docker→kind (k8s) → Apple Container (local + local-cluster)
# Runs all three Ignite execution modes and reports pass/fail for each.
set -euo pipefail

IGNITE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="/tmp/ignite-validation"
mkdir -p "$LOG_DIR"

SCORECARD="$IGNITE_DIR/.venvs/smoke/bin/python $IGNITE_DIR/scripts/spark_compat_score.py"
PYTHONPATH_VAL="$IGNITE_DIR/.venvs/smoke/lib/python3.9/site-packages"
KIND_CLUSTER="ignite-dev"
PASS=0
FAIL=0

log() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG_DIR/master.log"; }
section() { echo; log "═══════════════════════════════════════════════"; log "$*"; log "═══════════════════════════════════════════════"; echo; }

run_scorecard() {
    local label="$1"
    local logfile="$LOG_DIR/scorecard_${label}.log"
    log "Running scorecard: $label → $logfile"
    PYTHONPATH="$PYTHONPATH_VAL" SPARK_REMOTE="sc://localhost:50051" \
        $SCORECARD 2>&1 | tee "$logfile"
    local score
    score=$(grep -oE '[0-9]+/[0-9]+' "$logfile" | tail -1 || echo "0/0")
    local passed
    passed=$(echo "$score" | cut -d/ -f1)
    local total
    total=$(echo "$score" | cut -d/ -f2)
    if [ "$passed" = "$total" ] && [ "$total" -gt 0 ]; then
        log "✓ $label: $score PASSED"
        PASS=$((PASS+1))
    else
        log "✗ $label: $score FAILED"
        FAIL=$((FAIL+1))
    fi
}

# ─── PHASE 1: Wait for Docker build ───────────────────────────────────────────
section "PHASE 1: Waiting for Docker build #3 (PID 68033)"
DOCKER_PID=68033
while ps -p $DOCKER_PID > /dev/null 2>&1; do
    elapsed=$(ps -p $DOCKER_PID -o etime= 2>/dev/null | tr -d ' ')
    last_line=$(tail -1 /tmp/docker-build-restart2.log 2>/dev/null | grep -oE '#[0-9]+ [0-9]+\.[0-9]+ .*' || echo "compiling...")
    log "Docker build running (${elapsed}): ${last_line}"
    sleep 30
done

# Check success
if grep -q "=== Done\." /tmp/docker-build-restart2.log 2>/dev/null; then
    log "✓ Docker build SUCCEEDED"
    docker image inspect ignite:latest > /dev/null 2>&1 && log "✓ ignite:latest image confirmed in Docker"
elif grep -qE "ERROR|make.*Error" /tmp/docker-build-restart2.log 2>/dev/null; then
    log "✗ Docker build FAILED — last 10 lines:"
    tail -10 /tmp/docker-build-restart2.log | tee -a "$LOG_DIR/master.log"
    exit 1
else
    log "Docker build exited — checking image..."
    docker image inspect ignite:latest > /dev/null 2>&1 || { log "✗ ignite:latest not found, build failed"; exit 1; }
    log "✓ ignite:latest found"
fi

# ─── PHASE 2: kind Kubernetes cluster test ────────────────────────────────────
section "PHASE 2: kind cluster (kubernetes-cluster mode)"

# Clean up any stale cluster
kind delete cluster --name "$KIND_CLUSTER" 2>/dev/null || true

log "Creating kind cluster '$KIND_CLUSTER'..."
mkdir -p /tmp/sail /private/tmp/sail
kind create cluster --name "$KIND_CLUSTER" \
    --config "$IGNITE_DIR/k8s/kind-config.yaml" \
    2>&1 | tee -a "$LOG_DIR/kind-setup.log"

log "Loading ignite:latest into kind..."
kind load docker-image ignite:latest --name "$KIND_CLUSTER" \
    2>&1 | tee -a "$LOG_DIR/kind-setup.log"

log "Deploying ignite to kind..."
kubectl apply -f "$IGNITE_DIR/k8s/sail.yaml" 2>&1 | tee -a "$LOG_DIR/kind-setup.log"

log "Waiting for deployment rollout (up to 3 min)..."
kubectl rollout status deployment/ignite-spark-server -n ignite --timeout=180s \
    2>&1 | tee -a "$LOG_DIR/kind-setup.log"

log "Starting port-forward on 50051..."
kubectl port-forward -n ignite svc/ignite-spark-server 50051:50051 \
    > "$LOG_DIR/portforward.log" 2>&1 &
PF_PID=$!
sleep 5

# Verify port is open
if ! nc -z localhost 50051 2>/dev/null; then
    log "Port-forward not ready after 5s, waiting 10 more..."
    sleep 10
fi

if nc -z localhost 50051 2>/dev/null; then
    log "✓ Port 50051 open, running scorecard..."
    run_scorecard "kubernetes_cluster"
else
    log "✗ Port 50051 not accessible — skipping k8s scorecard"
    FAIL=$((FAIL+1))
fi

kill $PF_PID 2>/dev/null || true

log "Tearing down kind cluster..."
kind delete cluster --name "$KIND_CLUSTER" 2>/dev/null || true

# ─── PHASE 3: Apple Container build ──────────────────────────────────────────
section "PHASE 3: Apple Container build (--cpus 4 --memory 6g)"

log "Starting Apple Container builder with recommended specs..."
container builder start --cpus 4 --memory 6g --dns 8.8.8.8 2>&1 | tee -a "$LOG_DIR/container-build.log"
sleep 5

log "Building Apple Container image (LTO=off, jobs=2)..."
cd "$IGNITE_DIR"
make container-build 2>&1 | tee "$LOG_DIR/container-build-full.log"
BUILD_EXIT=$?

if [ $BUILD_EXIT -ne 0 ]; then
    log "✗ Apple Container build FAILED (exit $BUILD_EXIT)"
    tail -20 "$LOG_DIR/container-build-full.log" | tee -a "$LOG_DIR/master.log"
    exit 1
fi

log "✓ Apple Container build SUCCEEDED"

# ─── PHASE 4: Apple Container — local mode (single-node) ─────────────────────
section "PHASE 4: Apple Container local mode (SAIL_MODE=local)"

# Stop any existing container on that port
container stop ignite 2>/dev/null || true
container rm ignite 2>/dev/null || true

log "Starting container in local mode..."
container run --rm --detach --name ignite \
    -p 50051:50051 \
    ignite:latest \
    > "$LOG_DIR/container-local.log" 2>&1
sleep 10

# Wait for health (port open)
for i in $(seq 1 18); do
    if nc -z localhost 50051 2>/dev/null; then
        log "✓ Container healthy after ${i}×5s"
        break
    fi
    sleep 5
done

if nc -z localhost 50051 2>/dev/null; then
    run_scorecard "apple_container_local"
else
    log "✗ Container not ready on port 50051"
    FAIL=$((FAIL+1))
fi

container stop ignite 2>/dev/null || true
sleep 3

# ─── PHASE 5: Apple Container — local-cluster mode ───────────────────────────
section "PHASE 5: Apple Container local-cluster mode (SAIL_MODE=local-cluster)"

container stop ignite-cluster 2>/dev/null || true
container rm ignite-cluster 2>/dev/null || true

log "Starting container in local-cluster mode..."
container run --rm --detach --name ignite-cluster \
    -p 50051:50051 \
    -e SAIL_MODE=local-cluster \
    ignite:latest \
    > "$LOG_DIR/container-cluster.log" 2>&1
sleep 10

for i in $(seq 1 18); do
    if nc -z localhost 50051 2>/dev/null; then
        log "✓ Container healthy after ${i}×5s"
        break
    fi
    sleep 5
done

if nc -z localhost 50051 2>/dev/null; then
    run_scorecard "apple_container_local_cluster"
else
    log "✗ Container not ready"
    FAIL=$((FAIL+1))
fi

container stop ignite-cluster 2>/dev/null || true

# ─── FINAL REPORT ─────────────────────────────────────────────────────────────
section "FINAL REPORT"
TOTAL=$((PASS+FAIL))
log "Modes passed: $PASS / $TOTAL"
log ""
log "Scorecard files:"
for f in "$LOG_DIR"/scorecard_*.log; do
    [ -f "$f" ] || continue
    label=$(basename "$f" .log | sed 's/scorecard_//')
    score=$(grep -oE '[0-9]+/[0-9]+' "$f" | tail -1 || echo "?/?")
    log "  $label: $score"
done
log ""
log "All logs in: $LOG_DIR"

if [ $FAIL -gt 0 ]; then
    log "✗ $FAIL mode(s) FAILED"
    exit 1
fi
log "✓ All $PASS modes PASSED — Ignite distributed validation complete"
