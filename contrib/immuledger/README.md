# immuledger — a tamper-evident decision & compliance ledger for Claude Code

A Claude Code plugin, backed by **embedded [immudb](https://github.com/codenotary/immudb)**, that gives a project a durable, append-only memory of **what was decided and why** — and lets Claude check its current work against those past decisions so it stays on track even after the original conversation has scrolled out of context.

Unlike a plugin that talks to a *running* immudb over the network, immuledger **embeds immudb in-process** (via `pkg/database`). There is **no immudb server and no container to run**, and because immudb is embedded, the plugin can perform **real client-side cryptographic proofs** itself.

## The problem it solves

Agents lose the thread. Context windows fill up, sessions restart, and the "we already decided *not* to do X" rationale evaporates. immuledger makes those decisions first-class, queryable, and impossible to silently rewrite — because immudb is append-only and tamper-evident at the storage layer.

## What you get

- **`/decision [title]`** — record an ADR with its rationale and the alternatives considered. If it reverses an earlier call, it supersedes it (the old version is retained, not deleted).
- **`/decisions [term]`** — list or search the project's decisions.
- **`/check [what]`** — before non-trivial work, check the plan/diff against recorded decisions and flag conflicts *before* drifting off course.
- **`/verify [id]`** — report ledger status and **cryptographically verify** a decision in-process (inclusion proof + dual proof against the current root).
- **SessionStart digest** — each session starts with a compact list of active decisions injected into context.
- **CLAUDE.md change tracking** — every edit to `CLAUDE.md` (or files under `.claude/`) is recorded as a `claude_md_change` event with a sha256 of the new contents.

Everything is scoped **per project** (the git repo's top-level folder name), so one ledger directory serves many repos.

## Why embedded immudb

A normal database would let you store decisions — but also let anyone quietly edit or delete them. immudb doesn't: it is append-only and tamper-evident. Marking a decision "superseded" is a new version — the original stays in immudb's history. Because immuledger *embeds* immudb rather than talking to it over the PostgreSQL wire protocol, `/verify` validates the Merkle proof **client-side, in the plugin's own process** — the strongest form of the guarantee, with nothing else to run.

## Requirements

- **Go 1.25+** on the machine running Claude Code (to build the plugin binary once).
- No immudb server, no container, no other services.

## Setup

1. Install the plugin. From this directory it is its own single-plugin marketplace:
   ```
   /plugin marketplace add /path/to/immudb/contrib/immuledger
   /plugin install immuledger
   ```
2. **Build the binary** (parallels a `pip install` step):
   ```
   cd /path/to/immudb/contrib/immuledger
   make build            # or: ./scripts/build.sh
   ```
   This produces `bin/immuledger`, which the plugin's MCP server and hooks invoke.
3. (Optional) set `IMMULEDGER_DATA_DIR` to choose where the ledger lives (default `~/.immuledger`). See `.env.example`.
4. Start a session and run `/verify` (or `/decision` once) — the ledger is created automatically on first use.

## MCP tools exposed

`init_ledger`, `record_decision`, `list_decisions`, `get_decision`, `search_decisions`, `check_compliance`, `record_event`, `list_events`, `ledger_status`, `verify_decision`.

## How it stores data

Decisions and events are stored as immudb key-value entries (JSON values) under per-project key prefixes, e.g. `immuledger/<project>/decision/<id>`. Each record is therefore individually verifiable via immudb's `VerifiableGet` path. The ledger is opened **per operation** (open → do work → close), guarded by a file lock, so the long-lived MCP server and the short-lived hook processes never open the single-writer immudb directory at the same time.

## Verification, concretely

`verify_decision` reproduces exactly what immudb's own verified-read client does, but in-process:

1. read the ledger's current committed state (`CurrentState`) as the trust anchor;
2. `VerifiableGet` the decision, obtaining an inclusion proof and a dual proof;
3. `store.VerifyInclusion` — the stored value is part of its transaction's entry-hash tree;
4. `store.VerifyDualProof` — that transaction is consistent with the current root.

If a decision's bytes were tampered with, step 3 fails and `verified` is `false`.

## Development

```
make build    # build bin/immuledger
make test     # run unit tests (ledger + real verification)
make vet
```

## Evals

An agent-level eval case lives under `evals/`. It checks the plugin's headline
flow: the assistant records a decision and then **cryptographically verifies**
it, reporting the result honestly.

```
evals/record-and-verify/
├── prompt.md            # the task given to the agent
└── graders/criteria.md  # LLM-graded rubric
```

Run it (requires `bin/immuledger` built, and grants the plugin's MCP tools):

```
make build
IMMULEDGER_DATA_DIR="$(mktemp -d)" \
  claude plugin eval immuledger@immuledger --case record-and-verify \
    --allow-tools 'mcp__*'
```

Setting `IMMULEDGER_DATA_DIR` to a throwaway directory keeps eval runs out of
your real ledger.

The plugin is a self-contained Go module that builds against the immudb checkout above it (`replace github.com/codenotary/immudb => ../../`).

## Limitations (v0.1)

- Key-value storage with per-project sequence ids; there is no SQL query layer yet.
- Decision ids are read/allocated assuming low write concurrency (a single dev session at a time), which fits the intended use; the file lock serializes concurrent processes.
- A focused starting point meant to be extended (richer compliance heuristics, SQL-backed querying, more event types).

MIT licensed.
