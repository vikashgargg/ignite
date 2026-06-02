#!/usr/bin/env sh
# Vajra installer — curl https://raw.githubusercontent.com/vikashgargg/ignite/main/install.sh | sh
set -e

REPO="vikashgargg/ignite"
INSTALL_DIR="${VAJRA_INSTALL_DIR:-${IGNITE_INSTALL_DIR:-$HOME/.local/bin}}"
VENV_DIR="${VAJRA_VENV_DIR:-$HOME/.local/lib/vajra/venv}"
BINARY="vajra"

info()  { printf "\033[32m[vajra]\033[0m %s\n" "$*"; }
warn()  { printf "\033[33m[vajra]\033[0m %s\n" "$*"; }
error() { printf "\033[31m[vajra]\033[0m %s\n" "$*" >&2; exit 1; }

# ── 1. Detect OS / arch ────────────────────────────────────────────────────────

OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
  Linux*)
    case "$ARCH" in
      x86_64)  TARGET="x86_64-unknown-linux-gnu" ;;
      aarch64) error "aarch64 Linux: use the container image (docker/Dockerfile) for production, or build from source: https://github.com/$REPO" ;;
      *)       error "Unsupported Linux architecture: $ARCH. Build from source: https://github.com/$REPO" ;;
    esac
    ;;
  Darwin*)
    case "$ARCH" in
      arm64)  TARGET="aarch64-apple-darwin" ;;
      *)      error "Vajra requires Apple Silicon (arm64). Intel Macs are not supported. Build from source: https://github.com/$REPO" ;;
    esac
    ;;
  *)
    error "Unsupported OS: $OS. Build from source: https://github.com/$REPO"
    ;;
esac

# ── 2. Find Python >= 3.10 ─────────────────────────────────────────────────────
# pyspark 4.x requires Python 3.10+. We prefer 3.11/3.12 (well-tested), then
# accept 3.10/3.13/3.14. We do NOT create the venv with system python3 blindly
# because it might be 3.9 (old CLT) or 3.14+ (Homebrew bleeding edge).

find_python310() {
  for py in python3.11 python3.12 python3.10 python3.13 python3.14 python3; do
    if command -v "$py" >/dev/null 2>&1; then
      if "$py" -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)" 2>/dev/null; then
        echo "$py"
        return 0
      fi
    fi
  done
  return 1
}

PYTHON_CMD=$(find_python310 2>/dev/null || true)

# On macOS, auto-install python@3.11 via Homebrew if nothing >= 3.10 found
if [ -z "$PYTHON_CMD" ] && [ "$OS" = "Darwin" ] && command -v brew >/dev/null 2>&1; then
  warn "No Python 3.10+ found — installing python@3.11 via Homebrew..."
  brew install python@3.11 --quiet
  PYTHON_CMD=$(find_python310 2>/dev/null || true)
fi

if [ -z "$PYTHON_CMD" ]; then
  error "Python 3.10 or later is required but was not found.
  macOS:  brew install python@3.11
  Ubuntu: sudo apt-get install python3.11
  Fedora: sudo dnf install python3.11
  Then re-run:  curl https://raw.githubusercontent.com/$REPO/main/install.sh | sh"
fi

PY_VER=$("$PYTHON_CMD" --version 2>&1)
info "Using $PY_VER"

# ── 3. Download binary ─────────────────────────────────────────────────────────

info "Fetching latest release..."
LATEST=$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
  | grep '"tag_name"' | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')

[ -z "$LATEST" ] && error "Could not fetch release info. Check https://github.com/$REPO/releases"

info "Installing Vajra $LATEST for $TARGET"

DOWNLOAD_URL="https://github.com/$REPO/releases/download/$LATEST/vajra-$TARGET"
TMP_DIR="$(mktemp -d)"
TMP_BIN="$TMP_DIR/vajra"

info "Downloading from $DOWNLOAD_URL"
curl -fsSL "$DOWNLOAD_URL" -o "$TMP_BIN" \
  || error "Download failed. Check https://github.com/$REPO/releases for available binaries."

chmod +x "$TMP_BIN"
mkdir -p "$INSTALL_DIR"
# Real binary is vajra-bin; vajra wrapper sets PYTHONPATH + VAJRA_PYTHON
mv "$TMP_BIN" "$INSTALL_DIR/${BINARY}-bin"
rm -rf "$TMP_DIR"

# ── 4. Python venv with pyspark 4.x Spark Connect stack ───────────────────────
# pyspark 4.x: Python 3.10+, no distutils, works on Python 3.10–3.14+
# Spark Connect client requires: pandas>=2.0, pyarrow>=4, grpcio>=1.56

info "Setting up Python environment (pyspark + Spark Connect deps)..."

"$PYTHON_CMD" -m venv "$VENV_DIR"

# Upgrade pip silently to avoid warnings, then install deps
"$VENV_DIR/bin/pip" install --upgrade pip --quiet --disable-pip-version-check 2>/dev/null || true

"$VENV_DIR/bin/pip" install \
  "pyspark>=4.0,<5.0" \
  "pandas>=2.0.0" \
  "pyarrow>=4.0.0" \
  "grpcio>=1.56.0" \
  "grpcio-status>=1.56.0" \
  "googleapis-common-protos>=1.56.4" \
  "zstandard>=0.25.0" \
  --quiet --disable-pip-version-check \
  || error "Dependency install failed.
  Debug with:
    $VENV_DIR/bin/pip install pyspark pandas pyarrow grpcio grpcio-status googleapis-common-protos zstandard"

PYSPARK_SITE="$("$VENV_DIR/bin/python3" -c 'import sysconfig; print(sysconfig.get_path("purelib"))')"
VENV_PYTHON="$VENV_DIR/bin/python3"

# ── 5. Create wrapper script ───────────────────────────────────────────────────
# Sets PYTHONPATH (pyspark importable) and VAJRA_PYTHON (tells vajra-bin to
# spawn pyspark client in venv Python instead of the embedded Python 3.9).
# Variables expand at install time via %s; ${PYTHONPATH} stays literal.

printf '#!/usr/bin/env sh\nexport PYTHONPATH="%s${PYTHONPATH:+:${PYTHONPATH}}"\nexport VAJRA_PYTHON="%s"\nexec "%s" "$@"\n' \
  "$PYSPARK_SITE" "$VENV_PYTHON" "$INSTALL_DIR/${BINARY}-bin" > "$INSTALL_DIR/$BINARY"
chmod +x "$INSTALL_DIR/$BINARY"

# ── 6. Verify ─────────────────────────────────────────────────────────────────

info "Installed to $INSTALL_DIR/$BINARY"

if "$INSTALL_DIR/$BINARY" --version >/dev/null 2>&1; then
  VERSION="$("$INSTALL_DIR/$BINARY" --version 2>/dev/null || echo "$LATEST")"
  info "Vajra $VERSION installed successfully"
fi

# ── 7. PATH hint ──────────────────────────────────────────────────────────────

case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *)
    warn "$INSTALL_DIR is not in your PATH."
    warn "Add this to your shell profile (~/.zshrc or ~/.bashrc):"
    warn "  export PATH=\"$INSTALL_DIR:\$PATH\""
    warn "Then open a new terminal or run:  source ~/.zshrc"
    ;;
esac

info "Done! Quick test:"
info "  vajra --version"
info "  vajra sql \"SELECT 1\""
