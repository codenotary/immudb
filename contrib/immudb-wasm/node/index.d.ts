// Type definitions for immudb-wasm.
// SPDX-License-Identifier: BUSL-1.1

/** A value accepted for keys and values: text is UTF-8 encoded. */
export type Bytes = string | Uint8Array | Buffer;

export interface GetResult {
  /** The stored value. */
  value: Buffer;
  /** Transaction id at which the value was written. */
  tx: number;
}

export interface ScanEntry {
  key: Buffer;
  value: Buffer;
  tx: number;
}

export interface VerifiedGetResult {
  /** Whether the key exists. */
  found: boolean;
  /** The value, or null when not found. */
  value: Buffer | null;
  /** Whether the cryptographic proof (inclusion + consistency) passed. */
  verified: boolean;
  /** Transaction id that committed the value. */
  txId: number;
  /** Transaction id of the current committed root the value was proven against. */
  rootTxId: number;
  /** Hex-encoded current root hash (the tamper-evident anchor). */
  rootHash: string;
}

export interface SqlQueryResult {
  columns: string[];
  rows: unknown[][];
}

/** An open immudb store. Single-writer; call close() to release the lock. */
export interface Db {
  /** Write key -> value; returns the transaction id. */
  set(key: Bytes, value: Bytes): number;
  /** Read the latest value for key, or null if absent. */
  get(key: Bytes): GetResult | null;
  /** Return entries whose key starts with prefix (ascending). limit 0 = all. */
  scan(prefix?: Bytes, limit?: number): ScanEntry[];
  /** Cryptographically verified read (in-process; no server). */
  verifiedGet(key: Bytes): VerifiedGetResult;
  /** Run a DDL/DML SQL statement. */
  sqlExec(sql: string): void;
  /** Run a SELECT and return columns and rows. */
  sqlQuery(sql: string): SqlQueryResult;
  /** Close the store and release the single-writer lock. */
  close(): void;
}

/**
 * Open (creating if needed) an immudb store at `dir`. Throws if the directory
 * is already open (single-writer lock).
 */
export function open(dir: string): Db;

declare const _default: { open: typeof open };
export default _default;
