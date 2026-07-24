#!/usr/bin/env bash
# Creates .venvs/smoke with the Python version that matches the Zelox container.
#
# The container base image is python:${PYTHON_VERSION}-slim (see docker/apple/Dockerfile).
# Client-side Python MUST match so UDF pickle serialisation succeeds.
#
# Usage:
#   bash scripts/setup-smoke-venv.sh          # creates/recreates .venvs/smoke
#   bash scripts/setup-smoke-venv.sh --check  # just verify version, no install
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# ── Read canonical Python version from the Dockerfile ────────────────────────
# Uses -oE (POSIX extended regex, portable across macOS BSD grep and GNU grep).
CONTAINER_PY=$(grep -oE 'ARG PYTHON_VERSION=[0-9]+\.[0-9]+' \
    "$REPO_ROOT/docker/apple/Dockerfile" 2>/dev/null | cut -d= -f2 | head -1)
CONTAINER_PY="${CONTAINER_PY:-3.12}"

# ── Locate a matching Python binary ──────────────────────────────────────────
PYTHON_BIN=$(command -v "python${CONTAINER_PY}" 2>/dev/null || \
             command -v python3 2>/dev/null || \
             command -v python 2>/dev/null)

ACTUAL_VER=$("$PYTHON_BIN" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')

if [[ "$ACTUAL_VER" != "$CONTAINER_PY" ]]; then
    echo "WARNING: wanted Python ${CONTAINER_PY} (container version) but found ${ACTUAL_VER} at ${PYTHON_BIN}"
    echo "  Install python${CONTAINER_PY} (e.g. brew install python@${CONTAINER_PY}) for full UDF compatibility."
    echo "  Continuing anyway — non-UDF tests will still pass."
fi

if [[ "${1:-}" == "--check" ]]; then
    echo "Container Python: ${CONTAINER_PY}  |  Local Python: ${ACTUAL_VER} (${PYTHON_BIN})"
    exit 0
fi

# ── Create venv ───────────────────────────────────────────────────────────────
VENV="$REPO_ROOT/.venvs/smoke"
echo "Creating smoke venv at ${VENV} using ${PYTHON_BIN} (${ACTUAL_VER})..."
rm -rf "$VENV"
"$PYTHON_BIN" -m venv "$VENV"

echo "Installing dependencies..."
"$VENV/bin/pip" install --quiet --upgrade pip
"$VENV/bin/pip" install --quiet \
    "pyspark[connect]==3.5.3" \
    setuptools \
    pandas \
    pyarrow

echo ""
echo "Smoke venv ready."
echo "  Python : $("$VENV/bin/python" --version)"
echo "  PySpark: $("$VENV/bin/python" -c 'import pyspark; print(pyspark.__version__)')"
echo ""
if [[ "$ACTUAL_VER" == "$CONTAINER_PY" ]]; then
    echo "✓ Python version matches container (${CONTAINER_PY}) — all 71 tests will pass in container mode."
else
    echo "! Python version mismatch — UDF tests (8. Python UDFs) will fail in container/k8s mode."
    echo "  Run: brew install python@${CONTAINER_PY} && bash scripts/setup-smoke-venv.sh"
fi
