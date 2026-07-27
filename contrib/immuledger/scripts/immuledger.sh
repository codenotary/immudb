#!/usr/bin/env bash
# Launcher for the immuledger plugin binary.
#
# The plugin manifest and the hooks invoke this script rather than bin/immuledger
# directly: bin/ is a build artifact and is not part of the repository, so after
# a marketplace install the binary does not exist yet. Pointing at it directly
# gave an MCP server that never spawned and a SessionStart hook that failed on
# every session, with nothing saying a build step was missing.
#
# Everything this script prints goes to stderr. stdout belongs to the caller:
# for `serve` it is the MCP JSON-RPC stream, and for `digest` it is text injected
# into the session context.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
bin="$root/bin/immuledger"

if [ ! -x "$bin" ]; then
  if command -v go >/dev/null 2>&1; then
    echo "[immuledger] building $bin (one-time, after install or update)..." >&2
    if ! "$root/scripts/build.sh" >&2; then
      echo "[immuledger] build failed. Build it manually with: cd '$root' && make build" >&2
      exit 1
    fi
  else
    echo "[immuledger] $bin is missing and Go is not on PATH." >&2
    echo "[immuledger] immuledger needs a one-time build: cd '$root' && make build (requires Go 1.25+)." >&2
    exit 1
  fi
fi

exec "$bin" "$@"
