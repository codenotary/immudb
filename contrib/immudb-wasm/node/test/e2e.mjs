import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  mkdtempSync,
  existsSync,
  writeFileSync,
  readdirSync,
  readFileSync,
  utimesSync,
  statSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { open } from '../index.mjs';

function tmpDir() {
  return join(mkdtempSync(join(tmpdir(), 'imdbwasm-')), 'db');
}

test('key-value set/get and missing key', () => {
  const db = open(tmpDir());
  const tx = db.set('hello', 'world');
  assert.ok(tx > 0);
  assert.equal(db.get('hello').value.toString(), 'world');
  assert.equal(db.get('absent'), null);
  db.close();
});

test('binary values round-trip', () => {
  const db = open(tmpDir());
  const bytes = Buffer.from([0, 1, 2, 255, 254]);
  db.set('bin', bytes);
  assert.deepEqual(db.get('bin').value, bytes);
  db.close();
});

test('prefix scan is ordered and bounded', () => {
  const db = open(tmpDir());
  db.set('user:2', 'b');
  db.set('user:1', 'a');
  db.set('other', 'x');
  const rows = db.scan('user:');
  assert.deepEqual(
    rows.map((r) => `${r.key}=${r.value}`),
    ['user:1=a', 'user:2=b'],
  );
  assert.equal(db.scan('user:', 1).length, 1);
  db.close();
});

test('verifiedGet returns a passing cryptographic proof', () => {
  const db = open(tmpDir());
  db.set('audit:1', 'record-A');
  for (let i = 0; i < 3; i++) db.set(`filler:${i}`, 'x'); // advance the root
  const v = db.verifiedGet('audit:1');
  assert.equal(v.found, true);
  assert.equal(v.verified, true);
  assert.equal(v.value.toString(), 'record-A');
  assert.ok(v.txId > 0);
  assert.match(v.rootHash, /^[0-9a-f]{64}$/);
  // missing key: not found, not verified
  const m = db.verifiedGet('missing');
  assert.equal(m.found, false);
  db.close();
});

// The proof must not be skipped when the value's transaction IS the current
// root: that is the ordinary "just wrote it" case, and it used to short-circuit
// to verified:true after only an inclusion check against a field of the very
// transaction being attested.
test('verifiedGet proves the newest key, with no later transactions', () => {
  const db = open(tmpDir());
  const tx = db.set('only', 'record-A');
  const v = db.verifiedGet('only');
  assert.equal(v.found, true);
  assert.equal(v.verified, true);
  assert.equal(v.value.toString(), 'record-A');
  assert.equal(v.txId, tx);
  assert.equal(v.rootTxId, tx, 'the value should be its own store root here');
  db.close();
});

// Damage inside the hash tree must make the dual proof fail rather than be
// reported as a passing proof.
//
// The flipped byte has to sit on the Merkle path of the key being proven — a
// proof only attests the path it covers, so corrupting an unrelated node
// legitimately verifies. Flipping a node in the middle of the tree puts it on
// the path of the earliest key, which is what this checks.
test('damage on a key\'s Merkle path fails verification', () => {
  const dir = tmpDir();
  const db0 = open(dir);
  for (let i = 0; i < 5; i++) db0.set(`k:${i}`, `v${i}`);
  db0.close();

  const treeDir = join(dir, 'aht', 'tree');
  assert.ok(existsSync(treeDir), 'expected an aht/tree directory to corrupt');
  const target = readdirSync(treeDir)
    .map((f) => join(treeDir, f))
    .find((p) => statSync(p).size > 64);
  assert.ok(target, 'expected a non-trivial hash-tree file');

  const bytes = readFileSync(target);
  bytes[Math.floor(bytes.length / 2)] ^= 0xff;
  writeFileSync(target, bytes);

  const db = open(dir);
  assert.equal(db.verifiedGet('k:0').verified, false, 'a damaged Merkle path must not verify');
  db.close();
});

test('embedded SQL exec/query', () => {
  const db = open(tmpDir());
  db.sqlExec('CREATE TABLE items (id INTEGER, name VARCHAR, PRIMARY KEY id)');
  db.sqlExec("UPSERT INTO items (id, name) VALUES (1, 'widget'), (2, 'gadget')");
  const { columns, rows } = db.sqlQuery('SELECT id, name FROM items ORDER BY id');
  assert.deepEqual(columns, ['id', 'name']);
  assert.deepEqual(rows, [
    [1, 'widget'],
    [2, 'gadget'],
  ]);
  db.close();
});

test('data persists across reopen', () => {
  const dir = tmpDir();
  let db = open(dir);
  db.set('persist', 'yes');
  db.close();

  db = open(dir);
  assert.equal(db.get('persist').value.toString(), 'yes');
  db.close();
});

test('single-writer lock rejects a second open', () => {
  const dir = tmpDir();
  const db = open(dir);
  assert.throws(() => open(dir), /held by a running process|single-writer/);
  db.close();
  // after close, reopening works
  const db2 = open(dir);
  db2.close();
});

test('a stale lock from a dead process is reclaimed', () => {
  const dir = tmpDir();
  const db0 = open(dir);
  db0.set('x', '1');
  db0.close();

  // Simulate a lock leaked by a crashed process (a pid that does not exist).
  const lock = join(dir, '.immudb-wasm.lock');
  writeFileSync(lock, '2147483646');

  const db = open(dir); // must break the stale lock and open
  assert.equal(db.get('x').value.toString(), '1');
  db.close();
  assert.equal(existsSync(lock), false);
});

// A lock file with no readable pid is not evidence that the owner is dead — it
// is exactly what a lock looks like for the instant between being created and
// having its pid written. Breaking it there hands two processes the same store.
test('a fresh lock with no readable owner is not stolen', () => {
  const dir = tmpDir();
  const db0 = open(dir);
  db0.close();

  const lock = join(dir, '.immudb-wasm.lock');
  writeFileSync(lock, ''); // freshly created, pid not yet published

  assert.throws(() => open(dir), /records no owner|could not acquire/);
  assert.equal(existsSync(lock), true, 'a live-looking lock must not be removed');
});

test('an unreadable lock is reclaimed once it is clearly abandoned', () => {
  const dir = tmpDir();
  const db0 = open(dir);
  db0.set('x', '1');
  db0.close();

  const lock = join(dir, '.immudb-wasm.lock');
  writeFileSync(lock, '');
  const longAgo = new Date(Date.now() - 60_000);
  utimesSync(lock, longAgo, longAgo);

  const db = open(dir);
  assert.equal(db.get('x').value.toString(), '1');
  db.close();
});

test('values well past immudb\'s 4KB default are accepted', () => {
  const db = open(tmpDir());
  const big = 'x'.repeat(200_000);
  db.set('big', big);
  assert.equal(db.get('big').value.toString(), big);
  db.close();
});

test('failed setup releases the lock (no stale lock left behind)', () => {
  const dir = tmpDir();
  // Simulate WASI/store setup failing after the lock is taken.
  assert.throws(
    () =>
      open(dir, {
        hostFactory: () => {
          throw new Error('simulated WASI init failure');
        },
      }),
    /simulated WASI init failure/,
  );
  // The lock must be gone...
  assert.equal(existsSync(join(dir, '.immudb-wasm.lock')), false);
  // ...and a normal open must succeed rather than hit a stale lock.
  const db = open(dir);
  db.set('ok', '1');
  assert.equal(db.get('ok').value.toString(), '1');
  db.close();
});
