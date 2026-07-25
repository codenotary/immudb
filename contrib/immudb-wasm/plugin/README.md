# immudb-wasm — Claude Code plugin

A tamper-evident **key-value + SQL** store for Claude Code, backed by embedded
[immudb](https://github.com/codenotary/immudb) compiled to WebAssembly. Pure JS +
one `.wasm` — **no native binary, no server, no container**. Installs identically
on macOS/Linux/Windows.

The plugin is just a manifest: its MCP server runs via **`npx -y immudb-wasm-mcp`**,
so once that package is published to npm there is **no build step** — installing
from a marketplace is enough.

## Install

Both packages are published to npm, so nothing is compiled at install time.
Requires **Node 20+**; no Go, no server, no container.

### Fastest — register the MCP server directly (no clone)

```sh
claude mcp add immudb-wasm -- npx -y immudb-wasm-mcp@0.1.0
```

This is the whole plugin. The manifest below contains no code — only a pointer
to that same npm package — so adding the server directly gets you an identical
setup without fetching this repository.

### Or install as a plugin (marketplace discoverability)

```
/plugin marketplace add codenotary/immudb
/plugin install immudb-wasm
```

The plugin transport is git-based, so this clones the repository to deliver a
few KB of manifest. On a monorepo this size, prefer a sparse checkout:

```
/plugin marketplace add codenotary/immudb --sparse .claude-plugin contrib/immudb-wasm/plugin
```

Either way, on first launch `npx` fetches and caches `immudb-wasm-mcp` (which
bundles the `.wasm`) and runs it.

Data lives in `~/.immudb-wasm-plugin/<project>` (override with
`IMMUDB_WASM_DATA_DIR`), scoped per project. MCP servers start with the
session, so start a new session after installing.

## MCP tools

`immudb_set`, `immudb_get`, `immudb_verified_get`, `immudb_scan`,
`immudb_sql_exec`, `immudb_sql_query`, `immudb_status`.

`immudb_verified_get` performs a real in-process cryptographic proof (inclusion +
consistency against the current committed root) and returns `verified` plus the
root hash — no server involved.

## Testing unreleased changes

The manifest pins `npx` to a published `immudb-wasm-mcp` version. To exercise
local edits before releasing them, link the packages so `npx` resolves them
from the working tree instead:

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
