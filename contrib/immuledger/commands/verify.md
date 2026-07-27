---
description: Show the immudb ledger status for this project and cryptographically verify a decision in-process.
argument-hint: [optional decision id to verify]
---

The user wants to confirm the integrity of the decision ledger.

Decision id (may be empty): "$ARGUMENTS"

1. Call the `ledger_status` immuledger MCP tool. Report the counts (active /
   total decisions, events) for this project, plus the ledger's current
   tamper-evident **root** (`root_tx_id` and `root_hash`).
2. Perform a real cryptographic check:
   - If "$ARGUMENTS" is a decision id, call `verify_decision` with that id.
   - Otherwise pick the most recent active decision (via `list_decisions`) and
     call `verify_decision` on it.
   Report whether it returned `verified: true`, and relay the `detail` — this is
   an actual in-process proof (inclusion proof + dual proof against the current
   root), performed by embedded immudb with no server involved.
3. If the user has an anchor recorded out of band (a `root_hash` with the
   `root_tx_id` it was taken at), pass both to `verify_decision` as
   `expected_root` and `expected_root_tx_id`. That additionally proves the
   current root is a consistent extension of their anchor, which detects a
   replaced data directory. Do not pass `expected_root` without its tx id
   unless nothing has been written since it was taken — every append advances
   the root, so a bare hash will not match.
4. Explain briefly: immudb keeps every version of every record in a
   tamper-evident Merkle tree, so a silently altered decision would fail
   `verify_decision`. Unlike a pg-wire connection to a running immudb, this
   plugin embeds immudb and can validate the Merkle proof client-side itself.
5. Optionally, if the user asks to see recent changes, call `list_events` to
   show recorded CLAUDE.md changes and other events.

Keep the summary tight and factual. State plainly whether the proof passed.
