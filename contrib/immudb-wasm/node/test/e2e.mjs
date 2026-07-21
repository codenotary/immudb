import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
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
  assert.throws(() => open(dir), /already open/);
  db.close();
  // after close, reopening works
  const db2 = open(dir);
  db2.close();
});
