#!/usr/bin/env bash
# Creates manifests.tar.gz for the container build cache layer.
#
# Includes Cargo.toml files + empty src/lib.rs and src/main.rs stubs so that
# `cargo fetch --locked` can auto-detect crate targets without requiring the full
# source tree. The build layer (crates.tar.gz) overwrites these stubs with real
# source before the actual compile step.
#
# Usage: bash scripts/make-manifests.sh <output.tar.gz>
set -euo pipefail

OUT_DIR="/tmp/zelox-mfst"
DEST="${1:?usage: $0 <output.tar.gz>}"

rm -rf "$OUT_DIR"

find crates -name "Cargo.toml" | while IFS= read -r manifest; do
    dir=$(dirname "$manifest")
    mkdir -p "$OUT_DIR/$dir"
    cp "$manifest" "$OUT_DIR/$manifest"
    for stub in lib.rs main.rs; do
        if [ -f "$dir/src/$stub" ]; then
            mkdir -p "$OUT_DIR/$dir/src"
            touch "$OUT_DIR/$dir/src/$stub"
        fi
    done
done

tar -czf "$DEST" -C "$OUT_DIR" crates/
rm -rf "$OUT_DIR"
echo "Created $DEST (manifests + empty src stubs)"
