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
| `PROJECT_DIR` | Project whose basename scopes the store | current working dir |

Unset values passed as an unexpanded `${VAR}` placeholder are ignored.

## Use as an MCP server (outside the plugin)

```json
{
  "mcpServers": {
    "immudb-wasm": {
      "command": "npx",
      "args": ["-y", "immudb-wasm-mcp@0.1.0"],
      "env": { "IMMUDB_WASM_DATA_DIR": "/path/to/data" }
    }
  }
}
```

## Notes

- **Single writer.** The embedded store is single-process; a second open of the
  same directory by a live process is rejected. A lock left by a crashed process
  is detected and reclaimed.
- Depends on the [`immudb-wasm`](../node) package, which bundles the `.wasm`.

BUSL-1.1 (packages the immudb engine).
