// Minimal reactor smoke test: load immudb.wasm as a WASI reactor and call the
// exported imdb_ping / imdb_alloc functions. Validates the toolchain end to end
// before the full ABI is wired up.
import { WASI } from 'node:wasi';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import assert from 'node:assert';

const wasmPath = fileURLToPath(new URL('../immudb.wasm', import.meta.url));

const wasi = new WASI({ version: 'preview1', args: [], env: {}, preopens: {} });
const bytes = await readFile(wasmPath);
const module = await WebAssembly.compile(bytes);
const instance = await WebAssembly.instantiate(module, wasi.getImportObject());

// Reactor: initialize (runs _initialize / Go runtime setup) without starting main.
wasi.initialize(instance);

const { imdb_ping, imdb_alloc, imdb_free, memory } = instance.exports;
assert.equal(typeof imdb_ping, 'function', 'imdb_ping export missing');
assert.equal(typeof memory, 'object', 'memory export missing');

assert.equal(imdb_ping(41), 42, 'imdb_ping(41) should be 42');

const ptr = imdb_alloc(16);
assert.ok(ptr > 0, 'imdb_alloc should return a non-zero pointer');
// Write and read back through the exported linear memory.
const view = new Uint8Array(memory.buffer, ptr, 16);
view.set([1, 2, 3, 4]);
assert.deepEqual([...view.slice(0, 4)], [1, 2, 3, 4], 'linear memory round-trip failed');
imdb_free(ptr);

console.log('ping OK  |  imdb_ping(41)=42  |  alloc/memory round-trip OK  |  wasm bytes=' + bytes.length);
