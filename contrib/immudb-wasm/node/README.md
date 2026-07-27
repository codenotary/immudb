# immudb-wasm

Embed [immudb](https://github.com/codenotary/immudb) in Node.js — **in-process, via
WASM + WASI**. An append-only, cryptographically verifiable **KV + SQL** store with
**no server, no container, and no native prebuilds**: one portable `immudb.wasm` that
runs anywhere Node 22+ runs (macOS/Linux/Windows, any arch).

```sh
npm install immudb-wasm
```

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

Both proofs always run, including for the most recently written key. These are
immudb's native store-level primitives — no server, no gRPC, no protobuf.

The anchor is this store's own root, so verification detects tampering *within* the
data directory. It does not by itself detect wholesale replacement of the directory
with a different but internally consistent store.

## Notes and limits

- **Node 22+.** Earlier versions abort the process inside `node:wasi` rather than
  raising an error; `open()` fails fast with an explanation instead.
- **Single writer.** `open` takes a lockfile in the data directory and rejects a
  second open, in this or any other process.
- **Entry sizes.** Keys up to **1023 bytes** (immudb's 1 KB engine ceiling less the
  key-space prefix); values up to **1 MiB**. These are fixed when a store is created,
  so a store created before 0.2.0 keeps immudb's 4 KB value default.
- **Durability.** WASI preview 1 has no directory-fsync primitive, so directory syncs
  are a no-op (file data still syncs via `fd_sync`).
- **Performance.** Roughly 2–5× slower than native immudb, dominated by fsync cost.
  Suited to embedded and audit-log workloads, not high-throughput serving.
- **Directory sandbox.** The module only ever sees the one preopened directory.

## Related

- [`immudb-wasm-mcp`](https://www.npmjs.com/package/immudb-wasm-mcp) — this store as
  an MCP server, installable with `npx -y immudb-wasm-mcp`.
- [Full documentation and source](https://github.com/codenotary/immudb/tree/master/contrib/immudb-wasm).

## License

BUSL-1.1 (this packages the immudb engine). See `LICENSE`.
