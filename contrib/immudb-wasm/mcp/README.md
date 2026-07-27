# immudb-wasm-mcp

An [MCP](https://modelcontextprotocol.io) (stdio) server exposing a
**tamper-evident key-value + SQL store** backed by embedded
[immudb](https://github.com/codenotary/immudb) compiled to WebAssembly
([`immudb-wasm`](../node)). Pure JS + one `.wasm` — **no native binary, no
server, no container**.

Because it's published to npm, it runs with **zero local build**:

```sh
npx -y immudb-wasm-mcp
```

That is exactly what the Claude Code plugin's manifest does, which is why the
plugin installs from a marketplace with no build step (see [`../plugin`](../plugin)).

## Tools

`immudb_set`, `immudb_get`, `immudb_verified_get`, `immudb_scan`,
`immudb_sql_exec`, `immudb_sql_query`, `immudb_status`.

`immudb_verified_get` performs a real in-process cryptographic proof (inclusion +
consistency against the current committed root) and returns `verified` plus the
root hash.

## Configuration

| Env var | Meaning | Default |
|---|---|---|
| `IMMUDB_WASM_DATA_DIR` | Root directory for stores | `~/.immudb-wasm-plugin` |
| `PROJECT_DIR` | Project that scopes the store | current working dir |

Each project gets its own store under `IMMUDB_WASM_DATA_DIR`, named
`<basename>-<digest of the project path>`. The digest keeps two unrelated
checkouts that happen to share a folder name from landing on the same
single-writer store. Releases before 0.2.0 used the bare basename; a store
already sitting at that older path keeps being used, so upgrading never strands
an existing ledger.

Unset values passed as an unexpanded `${VAR}` placeholder are ignored.

## Use as an MCP server (outside the plugin)

One command, no repository clone and no build:

```sh
claude mcp add immudb-wasm -- npx -y immudb-wasm-mcp@0.2.0
```

Or declare it in an MCP config file directly:

```json
{
  "mcpServers": {
    "immudb-wasm": {
      "command": "npx",
      "args": ["-y", "immudb-wasm-mcp@0.2.0"],
      "env": { "IMMUDB_WASM_DATA_DIR": "/path/to/data" }
    }
  }
}
```

The plugin in [`../plugin`](../plugin) does nothing more than this — its
manifest holds no code, just the same `npx` invocation — so registering the
server directly is equivalent and skips the git-based plugin transport.

## Notes

- **Single writer.** The embedded store is single-process; a second open of the
  same directory by a live process is rejected. A lock left by a crashed process
  is detected and reclaimed.
- Depends on the [`immudb-wasm`](../node) package, which bundles the `.wasm`.

BUSL-1.1 (packages the immudb engine).
