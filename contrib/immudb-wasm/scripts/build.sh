#!/usr/bin/env bash
# Build the immudb-wasm reactor from the Go export layer. Requires Go 1.25+.
# Produces node/immudb.wasm (the artifact shipped inside the npm package).
set -euo pipefail
cd "$(dirname "$0")/.."

GOOS=wasip1 GOARCH=wasm go build \
  -buildmode=c-shared \
  -ldflags="-s -w" \
  -o node/immudb.wasm \
  ./wasm

echo "Built $(pwd)/node/immudb.wasm ($(du -h node/immudb.wasm | cut -f1))"

# Optionally shrink with binaryen's wasm-opt if available.
if command -v wasm-opt >/dev/null 2>&1; then
  wasm-opt -Oz node/immudb.wasm -o node/immudb.wasm
  echo "Optimized with wasm-opt -Oz -> $(du -h node/immudb.wasm | cut -f1)"
fi
