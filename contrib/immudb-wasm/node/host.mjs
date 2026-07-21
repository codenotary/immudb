// Low-level host bridge for immudb-wasm: loads the reactor, sets up a WASI
// instance whose only preopen is the store directory (mapped to /data), and
// marshals JSON requests/results through linear memory.
//
// SPDX-License-Identifier: BUSL-1.1
import { WASI } from 'node:wasi';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const wasmPath = fileURLToPath(new URL('./immudb.wasm', import.meta.url));
const wasmModule = await WebAssembly.compile(readFileSync(wasmPath));

const encoder = new TextEncoder();
const decoder = new TextDecoder();

// A Host wraps one wasm+WASI instance bound to a single store directory.
export class Host {
  #exports;
  #enc;

  constructor(exports) {
    this.#exports = exports;
  }

  // create instantiates the reactor with `dir` preopened as /data.
  static create(dir) {
    const wasi = new WASI({
      version: 'preview1',
      args: [],
      env: {},
      preopens: { '/data': dir },
    });
    const instance = new WebAssembly.Instance(wasmModule, wasi.getImportObject());
    wasi.initialize(instance); // reactor: run init, do not start main
    return new Host(instance.exports);
  }

  get exports() {
    return this.#exports;
  }

  #memory() {
    return new Uint8Array(this.#exports.memory.buffer);
  }

  #lastError() {
    const cap = 4096;
    const ptr = this.#exports.imdb_alloc(cap);
    const len = this.#exports.imdb_last_error(ptr, cap);
    const n = Math.min(Number(len), cap);
    const msg = decoder.decode(this.#memory().subarray(ptr, ptr + n));
    this.#exports.imdb_free(ptr);
    return msg || 'unknown immudb-wasm error';
  }

  #writeRequest(obj) {
    const bytes = encoder.encode(JSON.stringify(obj));
    const ptr = this.#exports.imdb_alloc(bytes.length || 1);
    this.#memory().set(bytes, ptr);
    return { ptr, len: bytes.length };
  }

  #readResult(packed) {
    // packed is a BigInt: (ptr << 32) | len
    const ptr = Number(packed >> 32n);
    const len = Number(packed & 0xffffffffn);
    const out = this.#memory().slice(ptr, ptr + len);
    this.#exports.imdb_free(ptr);
    return out.length ? JSON.parse(decoder.decode(out)) : {};
  }

  // open returns a positive store handle.
  open(dir) {
    const { ptr, len } = this.#writeRequest({ path: dir });
    const ret = this.#exports.imdb_open(ptr, len);
    this.#exports.imdb_free(ptr);
    if (ret < 0n) throw new Error(`immudb open failed: ${this.#lastError()}`);
    return Number(ret);
  }

  close(handle) {
    const rc = this.#exports.imdb_close(handle);
    if (rc !== 0) throw new Error(`immudb close failed: ${this.#lastError()}`);
  }

  // call invokes a (handle, reqPtr, reqLen) -> i64 export with a JSON request
  // and returns the parsed JSON result.
  call(fn, handle, obj) {
    const { ptr, len } = this.#writeRequest(obj);
    const ret = this.#exports[fn](handle, ptr, len);
    this.#exports.imdb_free(ptr);
    if (ret < 0n) throw new Error(`${fn}: ${this.#lastError()}`);
    return this.#readResult(ret);
  }
}

// toBytes accepts a string, Buffer, or Uint8Array and returns a base64 string
// (immudb-wasm carries binary key/value fields as base64 in JSON).
export function toB64(v) {
  if (v == null) return '';
  const buf = typeof v === 'string' ? Buffer.from(v, 'utf8') : Buffer.from(v);
  return buf.toString('base64');
}

// fromB64 decodes a base64 field to a Buffer.
export function fromB64(s) {
  return s ? Buffer.from(s, 'base64') : Buffer.alloc(0);
}
