#!/usr/bin/env bash
# Build the immuledger plugin binary. Run once after installing the plugin
# (and after any update). Requires Go 1.25+.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p bin
go build -o bin/immuledger ./cmd/immuledger
echo "Built $(pwd)/bin/immuledger"
