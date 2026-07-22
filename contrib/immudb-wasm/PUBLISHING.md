# Publishing immudb-wasm

Two npm packages back the Claude Code plugin. Publishing them is what makes the
plugin installable with **zero local build** (the plugin manifest runs
`npx -y immudb-wasm-mcp`).

```
contrib/immudb-wasm/
├── node/    -> npm package "immudb-wasm"       (the library; bundles immudb.wasm)
├── mcp/     -> npm package "immudb-wasm-mcp"    (the MCP server; bin, depends on immudb-wasm)
└── plugin/  -> the Claude Code plugin manifest  (runs `npx -y immudb-wasm-mcp`)
```

## Release flow

1. **Build the wasm artifact** (required — `immudb.wasm` is gitignored but is in
   the library package's `files`, so it must exist on disk before packing):
   ```sh
   make build          # -> node/immudb.wasm
   make test           # optional: 9 Node tests
   ```

2. **Publish the library**, then the server (the server depends on the library):
   ```sh
   cd node && npm publish --access public       # immudb-wasm@<version>
   cd ../mcp && npm publish --access public      # immudb-wasm-mcp@<version>
   ```
   Keep versions in step: `mcp/package.json` depends on `immudb-wasm` and the
   plugin manifest pins `immudb-wasm-mcp@<version>` — bump all three together.

3. **Point the plugin at the published version.** In
   `plugin/.claude-plugin/plugin.json`, `mcpServers.immudb-wasm.args` is
   `["-y", "immudb-wasm-mcp@<version>"]`. Update the pin on each release.

After step 2, `npx -y immudb-wasm-mcp` works anywhere, and so does a plain
`claude plugin install immudb-wasm` from the marketplace.

## Name / scope note

The names `immudb-wasm` and `immudb-wasm-mcp` are unscoped. If they are taken on
npm (or you prefer an org scope), rename to e.g. `@codenotary/immudb-wasm` and
`@codenotary/immudb-wasm-mcp` and update: the library `name`, the server `name`
and its `immudb-wasm` dependency, and the plugin manifest's `npx` argument.

## Verifying a package before publish

```sh
cd node && npm pack --dry-run    # expect: index.mjs host.mjs index.d.ts package.json immudb.wasm
cd mcp  && npm pack --dry-run    # expect: server.mjs package.json
```
