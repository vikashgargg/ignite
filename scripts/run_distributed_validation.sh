#!/usr/bin/env bash
# Distributed mode validation: Docker→kind (k8s) → Apple Container (local + local-cluster)
# Runs all three Zelox execution modes and reports pass/fail for each.
set -euo pipefail

ZELOX_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="/tmp/zelox-validation"
mkdir -p "$LOG_DIR"

SCORECARD="$ZELOX_DIR/.venvs/smoke/bin/python $ZELOX_DIR/scripts/spark_compat_score.py"
# Use Python 3.12 from Homebrew; fall back to system Python 3
PYTHON_BIN="$(command -v python3.12 2>/dev/null || command -v python3)"
PYTHONPATH_VAL="$($PYTHON_BIN -c 'import site; print(site.getsitepackages()[0])' 2>/dev/null || echo "")"
KIND_CLUSTER="zelox-dev"
PASS=0
FAIL=0

log()     { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG_DIR/master.log"; }
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

# ─── PHASE 1: Docker build ────────────────────────────────────────────────────
section "PHASE 1: Docker build (linux/arm64, for kind)"

log "Building Docker image zelox:latest..."
cd "$ZELOX_DIR"
make docker-build 2>&1 | tee "$LOG_DIR/docker-build.log"
BUILD_EXIT=$?

if [ $BUILD_EXIT -ne 0 ]; then
    log "✗ Docker build FAILED (exit $BUILD_EXIT)"
    tail -20 "$LOG_DIR/docker-build.log" | tee -a "$LOG_DIR/master.log"
    exit 1
fi
log "✓ Docker build SUCCEEDED"
docker image inspect zelox:latest > /dev/null 2>&1 && log "✓ zelox:latest image confirmed in Docker"

# ─── PHASE 2: kind Kubernetes cluster test ────────────────────────────────────
section "PHASE 2: kind cluster (kubernetes-cluster mode)"

# Clean up any stale cluster
kind delete cluster --name "$KIND_CLUSTER" 2>/dev/null || true

log "Creating kind cluster '$KIND_CLUSTER'..."
mkdir -p /tmp/zelox /private/tmp/zelox /tmp/zelox /private/tmp/zelox
kind create cluster --name "$KIND_CLUSTER" \
    --config "$ZELOX_DIR/k8s/kind-config.yaml" \
    2>&1 | tee -a "$LOG_DIR/kind-setup.log"

log "Loading zelox:latest into kind..."
kind load docker-image zelox:latest --name "$KIND_CLUSTER" \
    2>&1 | tee -a "$LOG_DIR/kind-setup.log"

log "Deploying Zelox to kind..."
kubectl apply -f "$ZELOX_DIR/k8s/zelox.yaml" 2>&1 | tee -a "$LOG_DIR/kind-setup.log"

log "Waiting for deployment rollout (up to 3 min)..."
kubectl rollout status deployment/zelox-spark-server -n zelox --timeout=180s \
    2>&1 | tee -a "$LOG_DIR/kind-setup.log"

log "Starting port-forward on 50051..."
kubectl port-forward -n zelox svc/zelox-spark-server 50051:50051 \
    > "$LOG_DIR/portforward.log" 2>&1 &
PF_PID=$!
sleep 5

for i in $(seq 1 6); do
    if nc -z localhost 50051 2>/dev/null; then
        log "✓ Port 50051 open"
        break
    fi
    sleep 5
done

if nc -z localhost 50051 2>/dev/null; then
    log "✓ Running kubernetes-cluster scorecard..."
    run_scorecard "kubernetes_cluster"
else
    log "✗ Port 50051 not accessible — skipping k8s scorecard"
    FAIL=$((FAIL+1))
fi

kill $PF_PID 2>/dev/null || true

log "Tearing down kind cluster..."
kind delete cluster --name "$KIND_CLUSTER" 2>/dev/null || true

# ─── PHASE 3: Apple Container build ──────────────────────────────────────────
section "PHASE 3: Apple Container build (--cpus 4 --memory 8g)"

log "Starting Apple Container builder with 8 GB (required to avoid OOM during aws-sdk-glue + zelox-catalog-hms parallel compile)..."
container builder stop 2>/dev/null || true
sleep 3
container builder start --cpus 4 --memory 8g --dns 8.8.8.8 2>&1 | tee -a "$LOG_DIR/container-build.log"
sleep 5

log "Building Apple Container image (LTO=off, jobs=2)..."
cd "$ZELOX_DIR"
make container-build 2>&1 | tee "$LOG_DIR/container-build-full.log"
BUILD_EXIT=$?

if [ $BUILD_EXIT -ne 0 ]; then
    log "✗ Apple Container build FAILED (exit $BUILD_EXIT)"
    tail -20 "$LOG_DIR/container-build-full.log" | tee -a "$LOG_DIR/master.log"
    exit 1
fi
log "✓ Apple Container build SUCCEEDED"

# ─── PHASE 4: Apple Container — local mode (single-node) ─────────────────────
section "PHASE 4: Apple Container local mode (ZELOX_MODE=local)"

container stop zelox 2>/dev/null || true
container rm zelox 2>/dev/null || true
mkdir -p /tmp/zelox

log "Starting container in local mode..."
container run --rm --detach --name zelox \
    -p 50051:50051 \
    -v /tmp/zelox:/tmp/zelox \
    zelox:latest \
    > "$LOG_DIR/container-local.log" 2>&1

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

container stop zelox 2>/dev/null || true
sleep 3

# ─── PHASE 5: Apple Container — local-cluster mode ───────────────────────────
section "PHASE 5: Apple Container local-cluster mode (ZELOX_MODE=local-cluster)"

container stop zelox-cluster 2>/dev/null || true
container rm zelox-cluster 2>/dev/null || true

log "Starting container in local-cluster mode..."
container run --rm --detach --name zelox-cluster \
    -p 50051:50051 \
    -e ZELOX_MODE=local-cluster \
    -v /tmp/zelox:/tmp/zelox \
    zelox:latest \
    > "$LOG_DIR/container-cluster.log" 2>&1

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

container stop zelox-cluster 2>/dev/null || true

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
log "✓ All $PASS modes PASSED — Zelox distributed validation complete"
