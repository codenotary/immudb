# immudb-wasm — Claude Code plugin

A tamper-evident **key-value + SQL** store for Claude Code, backed by embedded
[immudb](https://github.com/codenotary/immudb) compiled to WebAssembly
([`../`](../)). It runs as a Node MCP server that loads `immudb-wasm` in-process —
**pure JS + one `.wasm`, no native binary, no server, no container**. Installs
identically on macOS/Linux/Windows.

## Setup

```sh
# 1. build the wasm artifact (once), in the immudb-wasm package:
cd .. && make build

# 2. install this plugin's deps as real copies (so the marketplace copy is
#    self-contained — a plain `npm install` would symlink immudb-wasm):
cd plugin && npm install --install-links

# 3. install into Claude Code
/plugin marketplace add /path/to/immudb/contrib/immudb-wasm/plugin
/plugin install immudb-wasm@immudb-wasm
```

Data lives in `~/.immudb-wasm-plugin/<project>` (override with
`IMMUDB_WASM_DATA_DIR`), scoped per project.

## MCP tools

`immudb_set`, `immudb_get`, `immudb_verified_get`, `immudb_scan`,
`immudb_sql_exec`, `immudb_sql_query`, `immudb_status`.

`immudb_verified_get` performs a real in-process cryptographic proof (inclusion +
consistency against the current committed root) and returns `verified` plus the
root hash — no server involved.

## Notes

- **Single writer.** The embedded store is single-process; opening the same data
  directory from a second session is rejected by a lockfile.
- Built on the `immudb-wasm` package in the parent directory; see its README for
  durability and performance trade-offs.

BUSL-1.1 (packages the immudb engine).
