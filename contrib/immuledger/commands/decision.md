---
description: Record an architecture/design decision (ADR) to the tamper-evident immudb ledger for this project. Use when the user makes or confirms a choice worth remembering.
argument-hint: [short decision title]
---

The user wants to record a decision in the project's immudb-backed ledger.

Decision title (may be empty): "$ARGUMENTS"

Do the following:

1. Determine what is being decided. If "$ARGUMENTS" is empty or vague, infer the
   decision from the recent conversation, and briefly confirm the one-line title
   with the user if it is ambiguous.
2. Compose a durable record:
   - title: one crisp line stating the decision.
   - rationale: WHY this was chosen — the part that must survive context loss.
     Be specific and self-contained (someone reading it in 3 months, with no
     memory of this chat, should understand it).
   - alternatives: options considered and why they were rejected, if any.
   - tags: 2–5 comma-separated keywords for later retrieval (e.g. "db,schema").
3. If this decision reverses or replaces an earlier one, call `list_decisions`
   (or `search_decisions`) to find its id and pass it as `supersedes` so the old
   one is marked superseded (its history is retained either way).
4. Call the `record_decision` immuledger MCP tool with those fields.
5. Confirm to the user with the new decision id and title. Keep it to one line.

If the tool returns an error, tell the user the ledger could not be reached and
show the hint, rather than silently dropping the decision.
