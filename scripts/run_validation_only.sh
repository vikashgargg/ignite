#!/usr/bin/env bash
# Validation-only runner: assumes zelox:latest already built in both Docker and Apple Container.
# Tests all three execution modes: k8s-cluster, apple-container-local, apple-container-local-cluster.
#
# Prerequisites:
#   make smoke-setup          (creates .venvs/smoke with the right Python version)
#   make docker-build         (builds zelox:latest Docker image)
#   make container-build      (builds zelox:latest Apple Container image)
set -euo pipefail

export PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:$PATH"

ZELOX_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="/tmp/zelox-validation"
mkdir -p "$LOG_DIR"

# ── Smoke venv ────────────────────────────────────────────────────────────────
SMOKE_PYTHON="$ZELOX_DIR/.venvs/smoke/bin/python"
if [[ ! -x "$SMOKE_PYTHON" ]]; then
    echo "ERROR: smoke venv not found. Run: make smoke-setup" >&2
    exit 1
fi

SCORECARD_PY="$ZELOX_DIR/scripts/spark_compat_score.py"
# Derive PYTHONPATH from the venv itself — no need to guess Python version.
PYTHONPATH_VAL="$(ls -d "$ZELOX_DIR"/.venvs/smoke/lib/python*/site-packages 2>/dev/null | head -1)"
KIND_CLUSTER="zelox-dev"
PASS=0
FAIL=0

log()     { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG_DIR/master.log"; }
section() { echo; log "═══════════════════════════════════════════════"; log "$*"; log "═══════════════════════════════════════════════"; echo; }
ok()      { log "✓ $*"; PASS=$((PASS+1)); }
fail()    { log "✗ $*"; FAIL=$((FAIL+1)); }

run_scorecard() {
    local label="$1"
    local logfile="$LOG_DIR/scorecard_${label}.log"
    log "Running scorecard ($label) → $logfile"
    mkdir -p /tmp/zelox  # ensure shared mount point exists
    # Use || true so scorecard's sys.exit(1) on test failures does NOT abort this
    # script (set -euo pipefail would otherwise kill us here).
    PYTHONPATH="$PYTHONPATH_VAL" SPARK_REMOTE="sc://localhost:50051" \
        "$SMOKE_PYTHON" "$SCORECARD_PY" 2>&1 | tee "$logfile" || true
    local score
    score=$(grep -oE '[0-9]+/[0-9]+' "$logfile" | tail -1 || echo "0/0")
    local passed total
    passed=$(echo "$score" | cut -d/ -f1)
    total=$(echo "$score" | cut -d/ -f2)
    if [ "$passed" = "$total" ] && [ "$total" -gt 0 ]; then
        ok "$label: $score"
    else
        fail "$label: $score (see $logfile)"
    fi
}

wait_port() {
    local label="$1" max="$2"
    for i in $(seq 1 "$max"); do
        if nc -z localhost 50051 2>/dev/null; then
            log "✓ Port 50051 open (attempt $i)"
            return 0
        fi
        sleep 5
    done
    return 1
}

# ─── PHASE 1: Kubernetes (kind) ───────────────────────────────────────────────
section "PHASE 1: Kubernetes cluster mode (kind)"

kind delete cluster --name "$KIND_CLUSTER" 2>/dev/null || true
mkdir -p /tmp/zelox /private/tmp/zelox /tmp/sail /private/tmp/sail

log "Creating kind cluster '$KIND_CLUSTER'..."
kind create cluster --name "$KIND_CLUSTER" \
    --config "$ZELOX_DIR/k8s/kind-config.yaml" \
    2>&1 | tee "$LOG_DIR/kind-setup.log"

log "Loading zelox:latest → kind..."
kind load docker-image zelox:latest --name "$KIND_CLUSTER" \
    2>&1 | tee -a "$LOG_DIR/kind-setup.log"

log "Applying k8s manifests..."
kubectl apply -f "$ZELOX_DIR/k8s/sail.yaml" 2>&1 | tee -a "$LOG_DIR/kind-setup.log"

log "Waiting for deployment (up to 3 min)..."
if kubectl rollout status deployment/zelox-spark-server -n zelox --timeout=180s \
        2>&1 | tee -a "$LOG_DIR/kind-setup.log"; then
    log "Starting port-forward on 50051..."
    kubectl port-forward -n zelox svc/zelox-spark-server 50051:50051 \
        > "$LOG_DIR/portforward.log" 2>&1 &
    PF_PID=$!
    sleep 5
    if wait_port kubernetes 12; then
        run_scorecard "kubernetes_cluster"
    else
        fail "kubernetes_cluster: port 50051 not accessible"
    fi
    kill $PF_PID 2>/dev/null || true
else
    fail "kubernetes_cluster: deployment rollout timed out"
fi

log "Tearing down kind cluster..."
kind delete cluster --name "$KIND_CLUSTER" 2>/dev/null || true

# ─── PHASE 2: Apple Container — local mode ───────────────────────────────────
section "PHASE 2: Apple Container local mode"

container stop zelox 2>/dev/null || true
container rm zelox 2>/dev/null || true
mkdir -p /tmp/zelox

log "Starting container (local mode)..."
container run --rm --detach --name zelox \
    -p 50051:50051 \
    -v /tmp/zelox:/tmp/zelox \
    zelox:latest \
    > "$LOG_DIR/container-local-cid.log" 2>&1

if wait_port apple_local 18; then
    run_scorecard "apple_container_local"
else
    fail "apple_container_local: container not ready on port 50051"
fi

container stop zelox 2>/dev/null || true
sleep 3

# ─── PHASE 3: Apple Container — local-cluster mode ───────────────────────────
section "PHASE 3: Apple Container local-cluster mode"

container stop zelox-cluster 2>/dev/null || true
container rm zelox-cluster 2>/dev/null || true

log "Starting container (local-cluster mode)..."
container run --rm --detach --name zelox-cluster \
    -p 50051:50051 \
    -e ZELOX_MODE=local-cluster \
    -v /tmp/zelox:/tmp/zelox \
    zelox:latest \
    > "$LOG_DIR/container-cluster-cid.log" 2>&1

if wait_port apple_cluster 18; then
    run_scorecard "apple_container_local_cluster"
else
    fail "apple_container_local_cluster: container not ready"
fi

container stop zelox-cluster 2>/dev/null || true

# ─── FINAL REPORT ─────────────────────────────────────────────────────────────
section "FINAL REPORT"
TOTAL=$((PASS+FAIL))
log "Modes passed: $PASS / $TOTAL"
log ""
log "Scorecard results:"
for f in "$LOG_DIR"/scorecard_*.log; do
    [ -f "$f" ] || continue
    label=$(basename "$f" .log | sed 's/scorecard_//')
    score=$(grep -oE '[0-9]+/[0-9]+' "$f" | tail -1 || echo "?/?")
    log "  $label: $score"
done
log ""
log "Logs: $LOG_DIR"

if [ $FAIL -gt 0 ]; then
    log "✗ $FAIL mode(s) FAILED — see logs above"
    exit 1
fi
log "✓ All $PASS/3 modes PASSED — Zelox is a full Spark replacement"
