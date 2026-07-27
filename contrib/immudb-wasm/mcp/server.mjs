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
import { join, basename, resolve } from 'node:path';
import { createHash } from 'node:crypto';

// env returns an environment value unless it is empty or an unexpanded
// "${VAR}" placeholder (which the plugin runtime passes when the var is unset).
function env(name) {
  const v = process.env[name];
  if (!v || v.includes('${')) return undefined;
  return v;
}

// Project scope: the project directory's name, plus a short digest of its full
// path. One data root serves many projects, and the digest is what keeps two
// unrelated checkouts that happen to share a folder name (two "app" directories)
// from landing on the same store — and therefore contending for the same
// single-writer lock.
function projectName() {
  const dir = resolve(env('PROJECT_DIR') || process.cwd());
  const base = basename(dir.replace(/[/\\]+$/, '')) || 'default';
  const digest = createHash('sha256').update(dir).digest('hex').slice(0, 8);
  return `${base}-${digest}`;
}

const dataRoot = env('IMMUDB_WASM_DATA_DIR') || join(homedir(), '.immudb-wasm-plugin');
const project = projectName();
const dir = join(dataRoot, project);

// The store is opened on first use, not at import time. Opening here would kill
// the process before server.connect() below ever runs — the client then reports
// only "the server failed to start", with no way to learn that another session
// holds the lock. Opened lazily, the same condition surfaces as a tool error the
// user can act on.
let handle = null;
function db() {
  if (handle === null) handle = open(dir);
  return handle;
}

const ok = (obj) => ({ content: [{ type: 'text', text: JSON.stringify(obj, null, 2) }] });
const asText = (buf) => (buf == null ? null : Buffer.from(buf).toString('utf8'));

// guard turns any store failure into a structured tool error instead of an
// unhandled rejection.
const guard = (fn) => async (args) => {
  try {
    return await fn(args);
  } catch (e) {
    const held = /lock is held|acquire immudb store lock/.test(e.message ?? '');
    return {
      isError: true,
      content: [
        {
          type: 'text',
          text: JSON.stringify(
            {
              ok: false,
              error: e.message ?? String(e),
              hint: held
                ? `The immudb store at ${dir} is single-writer and is currently locked by another ` +
                  `process — usually a second Claude Code session on this project. Close it, or set ` +
                  `IMMUDB_WASM_DATA_DIR to give this session its own data directory.`
                : `The immudb store at ${dir} could not complete the operation. Check the directory ` +
                  `exists and is writable.`,
            },
            null,
            2,
          ),
        },
      ],
    };
  }
};

const server = new McpServer({ name: 'immudb-wasm', version: '0.1.0' });

server.registerTool(
  'immudb_set',
  {
    description: 'Store a value under a key in the tamper-evident store. Returns the transaction id.',
    inputSchema: { key: z.string(), value: z.string() },
  },
  guard(async ({ key, value }) => ok({ ok: true, tx: db().set(key, value), project })),
);

server.registerTool(
  'immudb_get',
  {
    description: 'Read the latest value for a key (or not found).',
    inputSchema: { key: z.string() },
  },
  guard(async ({ key }) => {
    const r = db().get(key);
    return ok(r ? { ok: true, found: true, value: asText(r.value), tx: r.tx } : { ok: true, found: false });
  }),
);

server.registerTool(
  'immudb_verified_get',
  {
    description:
      'Cryptographically verified read (in-process, no server): checks the inclusion and consistency proofs against the current committed root. Returns verified true/false and the root hash.',
    inputSchema: { key: z.string() },
  },
  guard(async ({ key }) => {
    const v = db().verifiedGet(key);
    return ok({
      ok: true,
      found: v.found,
      verified: v.verified,
      value: asText(v.value),
      txId: v.txId,
      rootTxId: v.rootTxId,
      rootHash: v.rootHash,
    });
  }),
);

server.registerTool(
  'immudb_scan',
  {
    description: 'List entries whose key starts with a prefix (ascending). limit 0 = all.',
    inputSchema: { prefix: z.string().default(''), limit: z.number().int().default(0) },
  },
  guard(async ({ prefix, limit }) =>
    ok({
      ok: true,
      entries: db()
        .scan(prefix, limit)
        .map((e) => ({
          key: e.key.toString('utf8'),
          value: e.value.toString('utf8'),
          tx: e.tx,
        })),
    }),
  ),
);

server.registerTool(
  'immudb_sql_exec',
  {
    description: 'Run a DDL/DML SQL statement (immudb SQL dialect).',
    inputSchema: { sql: z.string() },
  },
  guard(async ({ sql }) => {
    db().sqlExec(sql);
    return ok({ ok: true });
  }),
);

server.registerTool(
  'immudb_sql_query',
  {
    description: 'Run a SELECT and return columns and rows.',
    inputSchema: { sql: z.string() },
  },
  guard(async ({ sql }) => ok({ ok: true, ...db().sqlQuery(sql) })),
);

server.registerTool(
  'immudb_status',
  {
    description: 'Report the store location and project scope, plus the current tamper-evident root hash.',
    inputSchema: {},
  },
  guard(async () => {
    const v = db().verifiedGet(' immudb_status_probe'); // root is exposed via any verified read
    return ok({ ok: true, project, dataDir: dir, rootTxId: v.rootTxId, rootHash: v.rootHash });
  }),
);

await server.connect(new StdioServerTransport());
