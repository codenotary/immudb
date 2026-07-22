# immudb-wasm — Claude Code plugin

A tamper-evident **key-value + SQL** store for Claude Code, backed by embedded
[immudb](https://github.com/codenotary/immudb) compiled to WebAssembly. Pure JS +
one `.wasm` — **no native binary, no server, no container**. Installs identically
on macOS/Linux/Windows.

The plugin is just a manifest: its MCP server runs via **`npx -y immudb-wasm-mcp`**,
so once that package is published to npm there is **no build step** — installing
from a marketplace is enough.

## Install (once published)

```
/plugin marketplace add codenotary/immudb        # or the marketplace repo
/plugin install immudb-wasm
```

On first use, `npx` fetches and caches `immudb-wasm-mcp` (which bundles the
`.wasm`) and runs it. Requires Node 20+ on the machine running Claude Code.

Data lives in `~/.immudb-wasm-plugin/<project>` (override with
`IMMUDB_WASM_DATA_DIR`), scoped per project.

## MCP tools

`immudb_set`, `immudb_get`, `immudb_verified_get`, `immudb_scan`,
`immudb_sql_exec`, `immudb_sql_query`, `immudb_status`.

`immudb_verified_get` performs a real in-process cryptographic proof (inclusion +
consistency against the current committed root) and returns `verified` plus the
root hash — no server involved.

## Testing locally before publishing

The manifest points `npx` at the published `immudb-wasm-mcp`. To exercise the
plugin before publishing, link the packages so `npx` resolves them locally:

```sh
cd ../node && make -C .. build && npm link           # publishes immudb-wasm to the local link store
cd ../mcp  && npm link immudb-wasm && npm link        # links immudb-wasm-mcp, resolving its dep
# now `npx immudb-wasm-mcp` runs the local server; install the plugin from this dir
```

See [`../PUBLISHING.md`](../PUBLISHING.md) for the release flow.

## Notes

- **Single writer.** The embedded store is single-process; opening the same data
  directory from a second live session is rejected (a lock left by a crashed
  process is reclaimed automatically).

BUSL-1.1 (packages the immudb engine).
