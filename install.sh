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

# Detect OS and architecture
OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
  Linux*)
    case "$ARCH" in
      x86_64)  TARGET="x86_64-unknown-linux-musl" ;;
      aarch64) TARGET="aarch64-unknown-linux-musl" ;;
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

# Fetch latest release tag
info "Fetching latest release..."
LATEST=$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
  | grep '"tag_name"' | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')

if [ -z "$LATEST" ]; then
  error "Could not fetch release info. Check https://github.com/$REPO/releases"
fi

info "Installing Vajra $LATEST for $TARGET"

DOWNLOAD_URL="https://github.com/$REPO/releases/download/$LATEST/vajra-$TARGET"
TMP_DIR="$(mktemp -d)"
TMP_BIN="$TMP_DIR/vajra"

info "Downloading from $DOWNLOAD_URL"
if ! curl -fsSL "$DOWNLOAD_URL" -o "$TMP_BIN"; then
  error "Download failed. Check https://github.com/$REPO/releases for available binaries."
fi

chmod +x "$TMP_BIN"

mkdir -p "$INSTALL_DIR"
# Real binary lives at vajra-bin; the vajra wrapper sets PYTHONPATH
mv "$TMP_BIN" "$INSTALL_DIR/${BINARY}-bin"
rm -rf "$TMP_DIR"

# Set up isolated Python venv with pyspark (avoids system pip restrictions)
info "Setting up Python environment with pyspark..."
if ! command -v python3 >/dev/null 2>&1; then
  error "python3 not found. Install Python 3.10+ (brew install python) and re-run."
fi

python3 -m venv "$VENV_DIR"
"$VENV_DIR/bin/pip" install pyspark --quiet --disable-pip-version-check \
  || error "pyspark install failed. Try: $VENV_DIR/bin/pip install pyspark"

PYSPARK_SITE="$("$VENV_DIR/bin/python3" -c 'import sysconfig; print(sysconfig.get_path("purelib"))')"

# Create wrapper that prepends the venv's site-packages to PYTHONPATH
# Uses %s so $PYSPARK_SITE and $INSTALL_DIR expand now; ${PYTHONPATH} is literal in the script
printf '#!/usr/bin/env sh\nexport PYTHONPATH="%s${PYTHONPATH:+:${PYTHONPATH}}"\nexec "%s" "$@"\n' \
  "$PYSPARK_SITE" "$INSTALL_DIR/${BINARY}-bin" > "$INSTALL_DIR/$BINARY"
chmod +x "$INSTALL_DIR/$BINARY"

info "Installed to $INSTALL_DIR/$BINARY"

# Verify
if "$INSTALL_DIR/$BINARY" --version >/dev/null 2>&1; then
  VERSION="$("$INSTALL_DIR/$BINARY" --version 2>/dev/null || echo "$LATEST")"
  info "Vajra $VERSION installed successfully"
fi

# PATH advice
case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *)
    warn "$INSTALL_DIR is not in your PATH."
    warn "Add this to your shell profile (~/.bashrc, ~/.zshrc):"
    warn "  export PATH=\"$INSTALL_DIR:\$PATH\""
    ;;
esac

info "Done! Run:"
info "  vajra --version"
info "  vajra sql \"SELECT 1\""
