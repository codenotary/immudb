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
- A POSIX shell for the launcher script — on Windows, build `bin/immuledger.exe` and point the plugin at it directly.
- No immudb server, no container, no other services.

## Setup

1. Install the plugin. From this directory it is its own single-plugin marketplace:
   ```
   /plugin marketplace add /path/to/immudb/contrib/immuledger
   /plugin install immuledger
   ```
2. (Optional) set `IMMULEDGER_DATA_DIR` to choose where the ledger lives (default `~/.immuledger`). See `.env.example`.
3. Start a session and run `/verify` (or `/decision` once) — the ledger is created automatically on first use.

The plugin's MCP server and hooks go through `scripts/immuledger.sh`, which builds
`bin/immuledger` the first time it is needed (`bin/` is a build artifact and is not
committed) and execs it directly from then on. To build it up front instead:

```
cd /path/to/immudb/contrib/immuledger
make build            # or: ./scripts/build.sh
```

If Go is not installed the launcher says so on stderr and exits non-zero rather than
failing silently.

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

**Trust anchor.** The anchor in step 1 is the local store's *own* current root, so
verification proves the ledger is internally consistent — a record can't be
altered in place undetected. It does not, by itself, detect wholesale
*replacement* of the data directory with a different but internally consistent
store.

To guard against that, record an anchor out of band — `ledger_status` returns
`root_tx_id` and `root_hash` together — and pass **both** back to
`verify_decision` as `expected_root_tx_id` and `expected_root`. Verification then
also proves, with a dual proof from that transaction to the current one, that the
ledger's current root is a consistent *extension* of your anchor.

Pin both values, not just the hash. Every append advances the root, so an anchor
compared for equality against the current root stops matching the moment the next
decision or CLAUDE.md event lands — which is why passing `expected_root` on its
own only works if nothing has been written since, and reports a mismatch (with an
explanation) otherwise.

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
- A record is one immudb value, so it is bounded by the store's per-value limit. New ledgers are created with a 1 MiB limit, comfortably above the field caps (16 KB rationale, 8 KB alternatives, 16 KB payload). That limit is fixed when a ledger is created and cannot be raised in place, so a ledger created before this was set keeps immudb's 4 KB default — oversized records there fail with an explicit message rather than being silently truncated.
- A record that cannot be parsed back is reported as an error rather than skipped, so a corrupted entry can never quietly disappear from the listings while the rest is presented as a complete history.
- Decision ids are read/allocated assuming low write concurrency (a single dev session at a time), which fits the intended use; the file lock serializes concurrent processes.
- A focused starting point meant to be extended (richer compliance heuristics, SQL-backed querying, more event types).

## License

BUSL-1.1 — immuledger links the immudb engine into its binary, so it carries the
same license as immudb. See `LICENSE`.
