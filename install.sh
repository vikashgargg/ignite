#!/usr/bin/env sh
# Vajra installer — curl https://raw.githubusercontent.com/vikashgargg/ignite/main/install.sh | sh
set -e

REPO="vikashgargg/ignite"
INSTALL_DIR="${VAJRA_INSTALL_DIR:-${IGNITE_INSTALL_DIR:-$HOME/.local/bin}}"
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
      *)       error "Unsupported architecture: $ARCH" ;;
    esac
    ;;
  Darwin*)
    TARGET="universal2-apple-darwin"
    ;;
  *)
    error "Unsupported OS: $OS. Please build from source: https://github.com/$REPO"
    ;;
esac

# Fetch latest release tag
info "Fetching latest release..."
LATEST=$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
  | grep '"tag_name"' | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')

if [ -z "$LATEST" ]; then
  error "Could not determine latest release. Check https://github.com/$REPO/releases"
fi

info "Installing Vajra $LATEST for $TARGET"

DOWNLOAD_URL="https://github.com/$REPO/releases/download/$LATEST/vajra-$TARGET"
TMP_DIR="$(mktemp -d)"
TMP_BIN="$TMP_DIR/vajra"

info "Downloading from $DOWNLOAD_URL"
curl -fsSL "$DOWNLOAD_URL" -o "$TMP_BIN" \
  || error "Download failed. Check https://github.com/$REPO/releases for available binaries."

chmod +x "$TMP_BIN"

mkdir -p "$INSTALL_DIR"
mv "$TMP_BIN" "$INSTALL_DIR/$BINARY"
rm -rf "$TMP_DIR"

info "Installed to $INSTALL_DIR/$BINARY"

# Add to PATH advice if needed
case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *)
    warn "$INSTALL_DIR is not in your PATH."
    warn "Add this to your shell profile (~/.bashrc, ~/.zshrc):"
    warn "  export PATH=\"$INSTALL_DIR:\$PATH\""
    ;;
esac

info "Done! Run: vajra --version"
