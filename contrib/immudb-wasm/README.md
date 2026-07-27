# immudb-wasm

Embed [immudb](https://github.com/codenotary/immudb) in Node.js — **in-process, via WASM + WASI**. An append-only, cryptographically verifiable **KV + SQL** store with **no server, no container, and no native prebuilds**: a single portable `immudb.wasm` that runs anywhere Node 22+ runs (macOS/Linux/Windows, any arch).

This is what makes immudb viable as a pure-JS dependency (e.g. inside a Claude Code plugin): `npm install` ships one `.wasm`, no postinstall, no compiler.

## Quick start

```js
import { open } from 'immudb-wasm';

const db = open('./data');            // creates the dir if needed

db.set('hello', 'world');
db.get('hello').value.toString();     // 'world'

// tamper-evident, cryptographically verified read (in-process, no server)
const v = db.verifiedGet('hello');
// { found: true, value: <Buffer>, verified: true, txId, rootTxId, rootHash }

// embedded SQL
db.sqlExec('CREATE TABLE items (id INTEGER, name VARCHAR, PRIMARY KEY id)');
db.sqlExec("UPSERT INTO items (id, name) VALUES (1, 'widget')");
db.sqlQuery('SELECT id, name FROM items ORDER BY id');
// { columns: ['id','name'], rows: [[1,'widget']] }

db.close();
```

## Use it from an agent (MCP)

The [`mcp/`](./mcp) package exposes this store as an MCP server, installable
with no clone and no build:

```sh
claude mcp add immudb-wasm -- npx -y immudb-wasm-mcp@0.2.0
```

See [`plugin/`](./plugin) for the equivalent marketplace plugin.

## API

| Method | Description |
|---|---|
| `open(dir)` | Open/create a store at `dir`. Single-writer: throws if already open. |
| `db.set(key, value)` | Write; returns the transaction id. Keys/values are `string \| Uint8Array \| Buffer`. |
| `db.get(key)` | `{ value: Buffer, tx }` or `null`. |
| `db.scan(prefix, limit?)` | Entries whose key starts with `prefix`, ascending. |
| `db.verifiedGet(key)` | Verified read: `{ found, value, verified, txId, rootTxId, rootHash }`. |
| `db.sqlExec(sql)` / `db.sqlQuery(sql)` | Embedded SQL. |
| `db.close()` | Close and release the lock. |

## How verification works

`verifiedGet` performs a real client-side proof, entirely inside the wasm module:

1. read the store's current committed **root** as the trust anchor;
2. prove the value is **included** in its committing transaction (inclusion proof);
3. prove that transaction is **consistent** with the current root (dual proof).

If a record were altered on disk, the inclusion proof fails and `verified` is `false`. These are immudb's native store-level proof primitives — no server, no gRPC, no protobuf.

## Building the wasm artifact

Requires **Go 1.25+** (for `//go:wasmexport`). The npm package ships a prebuilt `node/immudb.wasm`; to rebuild:

```sh
make build        # -> node/immudb.wasm  (or: ./scripts/build.sh)
make test         # build + run the Node test suite
```

The build targets a **wasip1 reactor** (`GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared`). The one engine-side change this needs — a `wasip1` variant of `embedded/appendable/fileutils` — lives in the immudb tree.

## Notes, limits, and trade-offs

- **Single writer.** The embedded store is single-process; `open` takes a lockfile in the data directory and fails fast on a second open. Passing the same directory to two `open()` calls (or two processes) is rejected.
- **Durability.** WASI preview 1 has no directory-fsync primitive, so directory syncs are a no-op (file data still syncs via `fd_sync`). A crash at the wrong instant could lose a just-created file on some host filesystems — the same class of trade-off SQLite documents for some VFSes. Fine for embedded/audit-log use.
- **Performance.** Expect roughly 2–5× slower than native immudb, dominated by fsync cost. Suited to embedded and audit-log workloads, not high-throughput serving.
- **Entry size limits.** Keys are capped at **1023 bytes** (immudb's engine-wide 1 KB key ceiling, less the one-byte key-space prefix); values at **1 MiB**. Exceeding either raises `max key length exceeded` / `max value length exceeded`. These limits are written into the store's metadata when it is created and are authoritative on every later open, so a store created by an earlier version keeps immudb's 4 KB value default.
- **Verified reads are anchored locally.** `verifiedGet` proves the value is included in its transaction *and* that the transaction is consistent with the store's current committed root — always both, including for the most recently written key. The anchor is this store's own root, so it detects tampering with the data directory, not wholesale replacement of it with another internally consistent store.
- **Binary values in SQL.** SQL `BLOB` columns come back as base64 strings in `rows` (JSON transport).
- **Directory sandbox.** The wasm module only ever sees the one preopened directory (`/data`), so it cannot touch anything else on the filesystem.

## License

BUSL-1.1 (this packages the immudb engine). See `LICENSE`.
