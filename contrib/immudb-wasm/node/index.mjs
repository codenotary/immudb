// immudb-wasm — embed immudb in Node.js, in-process, via WASM + WASI.
// No server, no container, no native prebuilds. One portable .wasm.
//
// SPDX-License-Identifier: BUSL-1.1
import {
  mkdirSync,
  openSync,
  closeSync,
  unlinkSync,
  existsSync,
  writeFileSync,
  readFileSync,
  statSync,
  linkSync,
} from 'node:fs';
import { join, resolve } from 'node:path';
import { Host, toB64, fromB64 } from './host.mjs';

const LOCK = '.immudb-wasm.lock';

// How long to keep trying for the lock before giving up, and how long an
// unreadable lock file must sit untouched before it is assumed to be debris
// rather than a lock in the middle of being published.
const LOCK_TIMEOUT_MS = 5000;
const LOCK_RETRY_MS = 25;
const UNREADABLE_LOCK_GRACE_MS = 10_000;

// Node 20's bundled uvwasi aborts the whole process (SIGABRT, an assertion in
// node_wasi.cc, no catchable JS error) on the poll_oneoff call the Go wasip1
// runtime makes during store open. Fail with an explanation instead.
const MIN_NODE_MAJOR = 22;

function assertSupportedNode() {
  const major = Number.parseInt(process.versions.node.split('.')[0], 10);
  if (Number.isFinite(major) && major < MIN_NODE_MAJOR) {
    throw new Error(
      `immudb-wasm requires Node ${MIN_NODE_MAJOR} or newer (running ${process.versions.node}). ` +
        `Older versions abort the process inside node:wasi instead of raising an error.`,
    );
  }
}

// sleepSync blocks the thread; open() is a synchronous API, so the retry loop
// cannot await.
const sleepBuf = new Int32Array(new SharedArrayBuffer(4));
function sleepSync(ms) {
  Atomics.wait(sleepBuf, 0, 0, ms);
}

// Lock files this process owns, unlinked on exit so a crash/kill (that Node can
// still trap) does not leave a stale lock wedging the directory.
const heldLocks = new Set();
let exitHookInstalled = false;
function installExitHook() {
  if (exitHookInstalled) return;
  exitHookInstalled = true;
  const cleanup = () => {
    for (const p of heldLocks) {
      try {
        unlinkSync(p);
      } catch {
        /* best effort */
      }
    }
  };
  process.on('exit', cleanup);
  for (const sig of ['SIGINT', 'SIGTERM', 'SIGHUP']) {
    process.on(sig, () => {
      cleanup();
      process.exit(process.exitCode ?? 0);
    });
  }
}

function ownerAlive(pid) {
  if (!pid) return false;
  try {
    process.kill(pid, 0); // signal 0 just probes existence
    return true;
  } catch (e) {
    return e.code === 'EPERM'; // exists but not ours
  }
}

// publishLock atomically creates lockPath already containing our pid.
//
// The obvious openSync(lockPath, 'wx') + writeSync(pid) is not atomic: between
// the two calls the lock exists but is empty, and a second process reading it
// there sees no pid, concludes the owner is dead, and unlinks a live lock —
// after which both processes open the same store and corrupt it. Writing the pid
// to a private temp file and hard-linking it into place has no such window:
// link() fails with EEXIST if the lock is held, and when it succeeds the file
// already has its contents.
function publishLock(lockPath) {
  const tmp = `${lockPath}.${process.pid}.${lockSeq++}`;
  try {
    writeFileSync(tmp, String(process.pid), { flag: 'wx' });
    linkSync(tmp, lockPath); // throws EEXIST if another process holds it
  } finally {
    try {
      unlinkSync(tmp);
    } catch {
      /* nothing to clean up */
    }
  }
  return openSync(lockPath, 'r');
}
let lockSeq = 0;

// readLockOwner returns the pid recorded in the lock, or null when the lock
// cannot be read as a pid — which is NOT the same as "the owner is dead".
function readLockOwner(lockPath) {
  try {
    const pid = Number.parseInt(String(readFileSync(lockPath, 'utf8')).trim(), 10);
    return Number.isInteger(pid) && pid > 0 ? pid : null;
  } catch {
    return null;
  }
}

// ageMs is how long the lock file has sat untouched, or 0 if that can't be told.
function ageMs(lockPath) {
  try {
    return Date.now() - statSync(lockPath).mtimeMs;
  } catch {
    return 0;
  }
}

// acquireLock takes the single-writer lock, recording our pid. A lock whose
// recorded owner is gone is broken and reclaimed; a lock held by a live process
// is rejected. A lock that cannot be read is retried rather than broken, and is
// only reclaimed once it has sat untouched well past the moment it takes to
// publish one.
function acquireLock(lockPath) {
  const deadline = Date.now() + LOCK_TIMEOUT_MS;
  for (;;) {
    try {
      const fd = publishLock(lockPath);
      heldLocks.add(lockPath);
      installExitHook();
      return fd;
    } catch (e) {
      if (e.code !== 'EEXIST') throw e;
    }

    const owner = readLockOwner(lockPath);
    if (owner !== null && ownerAlive(owner)) {
      throw new Error(
        `immudb store lock is held by a running process (pid ${owner}). ` +
          `The directory is single-writer; close the other handle first.`,
      );
    }

    const reclaimable = owner !== null || ageMs(lockPath) > UNREADABLE_LOCK_GRACE_MS;
    if (reclaimable) {
      try {
        unlinkSync(lockPath);
        continue; // retry immediately
      } catch {
        /* raced with another breaker, or we may not remove it — fall through */
      }
    }

    if (Date.now() >= deadline) {
      throw new Error(
        owner === null
          ? `could not acquire immudb store lock at ${lockPath}: the lock file exists but ` +
            `records no owner. If no other process is using this directory, remove it.`
          : `could not acquire immudb store lock at ${lockPath} (held by pid ${owner}).`,
      );
    }
    sleepSync(LOCK_RETRY_MS);
  }
}

// open opens (creating if needed) an immudb store at `dir` and returns a Db.
// The store is single-writer; a lockfile guards against a second open of the
// same directory in this or another live process. A lock left behind by a
// crashed process is detected and reclaimed.
//
// The optional `hostFactory` is an internal seam for testing failure paths; the
// public API is just open(dir).
export function open(dir, { hostFactory = Host.create } = {}) {
  assertSupportedNode();
  const path = resolve(dir);
  mkdirSync(path, { recursive: true });

  const lockPath = join(path, LOCK);
  const lockFd = acquireLock(lockPath);

  // Everything after the lock is acquired must release it on failure — including
  // WASI instantiation (Host.create), not just the store open — otherwise a
  // failed setup leaves a stale lock that wedges the directory.
  let host, handle;
  try {
    host = hostFactory(path);
    handle = host.open('/data'); // the preopened directory
  } catch (e) {
    closeSync(lockFd);
    heldLocks.delete(lockPath);
    if (existsSync(lockPath)) unlinkSync(lockPath);
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
      heldLocks.delete(this.#lockPath);
      if (existsSync(this.#lockPath)) unlinkSync(this.#lockPath);
    }
  }
}

export default { open };
