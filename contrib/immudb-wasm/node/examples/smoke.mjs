import { open } from '../index.mjs';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const dir = join(mkdtempSync(join(tmpdir(), 'imdbwasm-')), 'db');
const db = open(dir);

console.log('set tx:', db.set('hello', 'world'));
console.log('get:', db.get('hello').value.toString());
console.log('missing:', db.get('nope'));

db.set('k:1', 'a');
db.set('k:2', 'b');
console.log('scan k::', db.scan('k:').map((e) => `${e.key} = ${e.value}`));

const v = db.verifiedGet('hello');
console.log('verifiedGet:', { verified: v.verified, value: v.value.toString(), txId: v.txId, root: v.rootHash.slice(0, 16) + '…' });

db.sqlExec('CREATE TABLE items (id INTEGER, name VARCHAR, PRIMARY KEY id)');
db.sqlExec("UPSERT INTO items (id, name) VALUES (1, 'widget'), (2, 'gadget')");
console.log('sql:', db.sqlQuery('SELECT id, name FROM items ORDER BY id'));

db.close();
console.log('OK');
