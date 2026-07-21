---
description: Check the current plan or pending changes against past decisions in the immudb ledger for compliance. Use before implementing non-trivial work.
argument-hint: [optional: what you're about to do]
---

The user wants to verify that the work at hand is consistent with decisions
already recorded for this project.

What we're about to do (may be empty): "$ARGUMENTS"

1. Build a short description of the current task. Use "$ARGUMENTS" if given;
   otherwise summarize what is being planned or implemented right now (and, if
   relevant, run `git diff --stat` / `git diff` to see pending changes).
2. Call the `check_compliance` immuledger MCP tool with that description.
3. Carefully compare the proposed work against the returned `active_decisions`
   (and `relevant_decisions`). For each decision that bears on the current work:
   - state whether the work COMPLIES or CONFLICTS, and
   - for any conflict, quote the decision (id + title) and explain the tension.
4. If there are conflicts, stop and surface them to the user before proceeding —
   do not quietly work around a recorded decision. If the user wants to change
   course, offer to record a superseding decision with `/decision`.
5. If everything is consistent, say so briefly and continue.

Be honest and specific; the point is to catch drift from earlier decisions.
