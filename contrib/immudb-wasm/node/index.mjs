// immudb-wasm — embed immudb in Node.js, in-process, via WASM + WASI.
// No server, no container, no native prebuilds. One portable .wasm.
//
// SPDX-License-Identifier: BUSL-1.1
import { mkdirSync, openSync, closeSync, unlinkSync, existsSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { Host, toB64, fromB64 } from './host.mjs';

const LOCK = '.immudb-wasm.lock';

// open opens (creating if needed) an immudb store at `dir` and returns a Db.
// The store is single-writer; a lockfile guards against a second open of the
// same directory in this or another process.
export function open(dir) {
  const path = resolve(dir);
  mkdirSync(path, { recursive: true });

  const lockPath = join(path, LOCK);
  let lockFd;
  try {
    lockFd = openSync(lockPath, 'wx'); // fails if it already exists
  } catch (e) {
    if (e.code === 'EEXIST') {
      throw new Error(
        `immudb store at ${path} is already open (lock ${LOCK} exists). ` +
          `Close the other handle, or remove the lock file if it is stale.`,
      );
    }
    throw e;
  }

  const host = Host.create(path);
  let handle;
  try {
    handle = host.open('/data'); // the preopened directory
  } catch (e) {
    closeSync(lockFd);
    unlinkSync(lockPath);
    throw e;
  }
  return new Db(host, handle, lockFd, lockPath);
}

class Db {
  #host;
  #handle;
  #lockFd;
  #lockPath;
  #closed = false;

  constructor(host, handle, lockFd, lockPath) {
    this.#host = host;
    this.#handle = handle;
    this.#lockFd = lockFd;
    this.#lockPath = lockPath;
  }

  #check() {
    if (this.#closed) throw new Error('immudb store is closed');
  }

  // set writes key -> value (string | Buffer | Uint8Array). Returns the tx id.
  set(key, value) {
    this.#check();
    const r = this.#host.call('imdb_set', this.#handle, {
      key: toB64(key),
      value: toB64(value),
    });
    return Number(r.tx);
  }

  // get returns { value: Buffer, tx } or null when the key is absent.
  get(key) {
    this.#check();
    const r = this.#host.call('imdb_get', this.#handle, { key: toB64(key) });
    if (!r.found) return null;
    return { value: fromB64(r.value), tx: Number(r.tx) };
  }

  // scan returns entries whose key starts with `prefix` (ascending), each
  // { key: Buffer, value: Buffer, tx }.
  scan(prefix = '', limit = 0) {
    this.#check();
    const r = this.#host.call('imdb_scan', this.#handle, {
      prefix: toB64(prefix),
      limit,
    });
    return (r.entries || []).map((e) => ({
      key: fromB64(e.key),
      value: fromB64(e.value),
      tx: Number(e.tx),
    }));
  }

  // verifiedGet returns { found, value: Buffer|null, verified, txId, rootTxId,
  // rootHash } — a real in-process cryptographic verified read.
  verifiedGet(key) {
    this.#check();
    const r = this.#host.call('imdb_verified_get', this.#handle, { key: toB64(key) });
    return {
      found: !!r.found,
      value: r.found ? fromB64(r.value) : null,
      verified: !!r.verified,
      txId: Number(r.tx_id || 0),
      rootTxId: Number(r.root_tx_id || 0),
      rootHash: r.root_hash || '',
    };
  }

  // sqlExec runs a DDL/DML statement.
  sqlExec(sql) {
    this.#check();
    this.#host.call('imdb_sql_exec', this.#handle, { sql });
  }

  // sqlQuery runs a SELECT and returns { columns: string[], rows: any[][] }.
  sqlQuery(sql) {
    this.#check();
    const r = this.#host.call('imdb_sql_query', this.#handle, { sql });
    return { columns: r.columns || [], rows: r.rows || [] };
  }

  close() {
    if (this.#closed) return;
    this.#closed = true;
    try {
      this.#host.close(this.#handle);
    } finally {
      closeSync(this.#lockFd);
      if (existsSync(this.#lockPath)) unlinkSync(this.#lockPath);
    }
  }
}

export default { open };
