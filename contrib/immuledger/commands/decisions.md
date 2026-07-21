---
description: List or search the project's recorded decisions from the immudb ledger.
argument-hint: [optional search term or "all"]
---

Show the user the decisions recorded for this project.

Filter/argument (may be empty): "$ARGUMENTS"

- If "$ARGUMENTS" is empty, call `list_decisions` (status "active") and present
  the results.
- If "$ARGUMENTS" is "all", call `list_decisions` with status "all" (includes
  superseded).
- Otherwise treat "$ARGUMENTS" as a search term and call `search_decisions`.

Present the results as a compact, readable list: id, title, tags, and status.
For any decision the user asks to expand, call `get_decision` to show its full
rationale and alternatives. Do not dump raw JSON — summarize for the user.
