#!/usr/bin/env bash
# Fetch the ONNX embedding model used by the vector-search tests.
#
# The model is hosted as a GitHub release asset rather than committed, so it stays out of
# git history (18MB extracted). See models/README.md for why.
#
# Safe to re-run: exits immediately if the model is already present.
set -euo pipefail

ASSET_TAG="test-assets-v1"
ASSET_NAME="bge-micro-v2.tar.gz"
EXPECTED_SHA256="132204045640c19ca555942dcaf82687dbf1cf8b38e1038121d2b6add76ec6e2"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODEL_DIR="$REPO_ROOT/models/bge-micro-v2"

if [[ -f "$MODEL_DIR/model_quantized.onnx" && -f "$MODEL_DIR/tokenizer.json" ]]; then
    echo "Model already present at $MODEL_DIR"
    exit 0
fi

URL="https://github.com/tomlm/LottaDB/releases/download/$ASSET_TAG/$ASSET_NAME"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "Downloading $ASSET_NAME ..."
curl -fsSL --retry 3 --retry-delay 2 -o "$TMP/$ASSET_NAME" "$URL"

# Verify before extracting — a truncated or substituted archive should fail loudly.
ACTUAL_SHA256="$(sha256sum "$TMP/$ASSET_NAME" | awk '{print $1}')"
if [[ "$ACTUAL_SHA256" != "$EXPECTED_SHA256" ]]; then
    echo "ERROR: checksum mismatch for $ASSET_NAME" >&2
    echo "  expected $EXPECTED_SHA256" >&2
    echo "  actual   $ACTUAL_SHA256" >&2
    exit 1
fi

mkdir -p "$REPO_ROOT/models"
tar -xzf "$TMP/$ASSET_NAME" -C "$REPO_ROOT/models"

echo "Model extracted to $MODEL_DIR"
