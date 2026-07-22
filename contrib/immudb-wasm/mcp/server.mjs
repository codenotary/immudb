#!/usr/bin/env node
// immudb-wasm-mcp — an MCP (stdio) server exposing a tamper-evident KV + SQL
// store backed by embedded immudb compiled to WASM. Pure JS + one .wasm: no
// native binary, no server, no container. Published to npm so it runs via
// `npx -y immudb-wasm-mcp` with zero local build.
//
// SPDX-License-Identifier: BUSL-1.1
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';
import { open } from 'immudb-wasm';
import { homedir } from 'node:os';
import { join, basename } from 'node:path';

// env returns an environment value unless it is empty or an unexpanded
// "${VAR}" placeholder (which the plugin runtime passes when the var is unset).
function env(name) {
  const v = process.env[name];
  if (!v || v.includes('${')) return undefined;
  return v;
}

// Project scope: basename of the project dir (or cwd). One data root serves many
// projects.
function projectName() {
  const dir = env('PROJECT_DIR') || process.cwd();
  return basename(dir.replace(/[/\\]+$/, '')) || 'default';
}

const dataRoot = env('IMMUDB_WASM_DATA_DIR') || join(homedir(), '.immudb-wasm-plugin');
const project = projectName();
const dir = join(dataRoot, project);

// Open the embedded store once for the server's lifetime (single-writer).
const db = open(dir);

const ok = (obj) => ({ content: [{ type: 'text', text: JSON.stringify(obj, null, 2) }] });
const asText = (buf) => (buf == null ? null : Buffer.from(buf).toString('utf8'));

const server = new McpServer({ name: 'immudb-wasm', version: '0.1.0' });

server.registerTool(
  'immudb_set',
  {
    description: 'Store a value under a key in the tamper-evident store. Returns the transaction id.',
    inputSchema: { key: z.string(), value: z.string() },
  },
  async ({ key, value }) => ok({ ok: true, tx: db.set(key, value), project }),
);

server.registerTool(
  'immudb_get',
  {
    description: 'Read the latest value for a key (or not found).',
    inputSchema: { key: z.string() },
  },
  async ({ key }) => {
    const r = db.get(key);
    return ok(r ? { ok: true, found: true, value: asText(r.value), tx: r.tx } : { ok: true, found: false });
  },
);

server.registerTool(
  'immudb_verified_get',
  {
    description:
      'Cryptographically verified read (in-process, no server): checks the inclusion and consistency proofs against the current committed root. Returns verified true/false and the root hash.',
    inputSchema: { key: z.string() },
  },
  async ({ key }) => {
    const v = db.verifiedGet(key);
    return ok({
      ok: true,
      found: v.found,
      verified: v.verified,
      value: asText(v.value),
      txId: v.txId,
      rootTxId: v.rootTxId,
      rootHash: v.rootHash,
    });
  },
);

server.registerTool(
  'immudb_scan',
  {
    description: 'List entries whose key starts with a prefix (ascending). limit 0 = all.',
    inputSchema: { prefix: z.string().default(''), limit: z.number().int().default(0) },
  },
  async ({ prefix, limit }) =>
    ok({
      ok: true,
      entries: db.scan(prefix, limit).map((e) => ({
        key: e.key.toString('utf8'),
        value: e.value.toString('utf8'),
        tx: e.tx,
      })),
    }),
);

server.registerTool(
  'immudb_sql_exec',
  {
    description: 'Run a DDL/DML SQL statement (immudb SQL dialect).',
    inputSchema: { sql: z.string() },
  },
  async ({ sql }) => {
    db.sqlExec(sql);
    return ok({ ok: true });
  },
);

server.registerTool(
  'immudb_sql_query',
  {
    description: 'Run a SELECT and return columns and rows.',
    inputSchema: { sql: z.string() },
  },
  async ({ sql }) => ok({ ok: true, ...db.sqlQuery(sql) }),
);

server.registerTool(
  'immudb_status',
  {
    description: 'Report the store location and project scope, plus the current tamper-evident root hash.',
    inputSchema: {},
  },
  async () => {
    const v = db.verifiedGet(' immudb_status_probe'); // root is exposed via any verified read
    return ok({ ok: true, project, dataDir: dir, rootTxId: v.rootTxId, rootHash: v.rootHash });
  },
);

await server.connect(new StdioServerTransport());
