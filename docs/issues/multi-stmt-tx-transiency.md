# Multi-statement SQL transactions trip `ErrCannotUpdateKeyTransiency` on UPDATE-after-INSERT of the same row

> **Resolved** in the follow-up commit that changes `OngoingTx.transientEntries` keyRef allocation to a negative-integer space so it can't collide with `tx.entries[]` slice indices. See the "Root cause — confirmed" and "Fix" sections below.

## Severity

High — blocks any ORM that wraps `INSERT` + `UPDATE` of the same row in a single transaction, which is the norm. Hit in the Gitea compat exercise on `/api/v1/user/repos` where Gitea's `WatchRepo` runs:

```
BEGIN
INSERT INTO repository (...) RETURNING id
INSERT INTO watch (user_id, repo_id, mode, ...) RETURNING id
UPDATE repository SET num_watches = num_watches + 1 WHERE id = ?
COMMIT
```

Surfaced to the client as:

```
pq: cannot change a non-transient key to transient or vice versa
```

## Minimal wire-protocol reproducer

3 lines against a freshly opened Postgres-wire session:

```sql
BEGIN;
INSERT INTO q (id, n) VALUES (1, 0);
UPDATE q SET n = n + 1 WHERE id = 1;   -- fails here
COMMIT;
```

Table schema is minimal — one primary-key column + one data column. No `AUTO_INCREMENT`, no secondary indexes, no defaults, no NOT NULL, no CHECK. Autocommit variant (each statement its own tx) passes, so the bug is specific to multi-statement single-tx sequences.

Driver-independent: reproduced with `lib/pq` and `pgx/v5`; also reproduced in-process when wrapping the three statements with `engine.NewTx` (produces `already closed` instead of the transiency message, but same root tx state corruption).

## Where it fires

`embedded/store/ongoing_tx.go:271-273`:

```go
_, wasTransient := tx.transientEntries[keyRef]
if isKeyUpdate {
    if wasTransient != isTransient {
        return ErrCannotUpdateKeyTransiency
    }
}
```

## Root cause — confirmed

`OngoingTx.entriesByKey[kid] → keyRef` stores keyRefs from two overlapping integer spaces:

- **Non-transient** writes use `len(tx.entries)-1` — a slice index into `tx.entries[]`, starting at 0 and growing monotonically.
- **Transient** writes (both the user-facing `SetTransient` path and the indexer-walk path at `ongoing_tx.go:336-341`) used `len(tx.entriesByKey)` — a counter meant as an opaque map key into `tx.transientEntries[]`, also starting at 0 and growing.

Both counters share the `int` space starting at 0. When the SQL engine's primary-row indexer runs the walk during an INSERT, it stashes a transient entry for the mapped PK key at `keyRef=0`. The outer non-transient main write for the row key then lands at `keyRef=0` too (as `len(tx.entries)-1`).

On a subsequent write to the main row key (the UPDATE), `wasTransient := tx.transientEntries[keyRef]` looks up `transientEntries[0]` and finds the stale indexer-walk entry — concluding (incorrectly) that the main key was previously written transient. `wasTransient != isTransient` and the check at line 271-273 fires.

The `refInterceptor` reader at line 224-225 had the same ambiguity but was harder to trip because in the Gitea repro no one actually reads the mapped key through it before the next write tips over the transience check.

## Fix

Allocate transient keyRefs in a **negative integer space** disjoint from the non-negative `tx.entries[]` slice indices:

```go
tkey := -(len(tx.transientEntries) + 1)
tx.transientEntries[tkey] = e
tx.entriesByKey[kid] = tkey
```

Applied at the two insertion sites (`ongoing_tx.go:339-340` for the indexer-walk path and `353-354` for the outer `SetTransient` path). Existing lookups (`tx.transientEntries[keyRef]` at lines 224 and 269) and the update sites (lines 337 and 347) are unchanged — they correctly read back whatever integer is in `entriesByKey` regardless of sign.

## Why this is safe

- Non-transient keyRefs are always slice indices, i.e. `0 ≤ keyRef < len(tx.entries)` — always non-negative.
- Transient keyRefs after the fix are always negative — disjoint from the non-transient range.
- `tx.transientEntries` is a `map[int]*EntrySpec` — it indexes by int, so negative integer keys are valid map keys with no performance impact.
- The `wasTransient := tx.transientEntries[keyRef]` check now returns `true` **iff** the keyRef points to a transient entry for this specific key — which is what the invariant always meant to enforce.
- The `refInterceptor` at line 224-225 (`entrySpec, transient := tx.transientEntries[keyRef]`) reads the correct entry regardless of sign.
- No change to serialization, on-disk format, or commit semantics — `transientEntries` is a per-tx in-memory map never persisted as indexed keys.
- `embedded/store/...` full test suite green (27s). `embedded/sql/...` full test suite green (36s). No new regressions across `embedded/...` other than the pre-existing `embedded/document/TestGetDocument…` failures that reproduce on HEAD before the fix (`max key length exceeded`, unrelated).

## Historical suspected root cause (kept for reference)

`OngoingTx.set` walks every registered indexer after accepting a write. For the SQL engine's **primary-row indexer** (`SourcePrefix=rowEntryPrefix`, no `SourceEntryMapper`, `TargetEntryMapper` rewrites `RowPrefix+…` into `MappedPrefix+…`), a non-transient `tx.set(rowKey)` triggers the block at `ongoing_tx.go:331-340`:

```go
if !bytes.Equal(key, targetKey) {
    kid := sha256.Sum256(targetKey)
    keyRef, isKeyUpdate := tx.entriesByKey[kid]
    if isKeyUpdate {
        tx.transientEntries[keyRef] = e
    } else {
        tx.transientEntries[len(tx.entriesByKey)] = e
        tx.entriesByKey[kid] = len(tx.entriesByKey)
        ...
    }
}
```

So a user-visible non-transient write on `rowKey` causes an **internal, transient** entry to be stashed at `targetKey = MappedPrefix + table.id + primaryIndex.id + pkEncVals`. On a subsequent write affecting the same row, the flag consistency check on `targetKey` triggers against one of the earlier transient entries.

Interaction graph (to be confirmed):
- INSERT: `tx.set(rowKey, non-transient)` → indexer walk stashes `targetKey` as transient.
- UPDATE: `tx.set(rowKey, non-transient)` → indexer walk revisits `targetKey`. The transience check may see the transient entry from step 1 and compare it against what the current walk wants to write; one of the two paths tracked by `entriesByKey`/`transientEntries` flips and trips the invariant.

`OngoingTx.set` needs closer trace to confirm which of these is the flipped key and whether the invariant is actually being violated or whether the check is being applied to the wrong pair of writes.

## Fix directions (not yet chosen)

1. **Loosen the transience check** to treat keys registered only by the indexer walk as "promotable" — i.e., allow a later non-transient write to the same mapped key to coexist with an earlier walk-induced transient entry.
2. **Make the indexer walk idempotent** for updates — don't re-add transient entries on subsequent writes if a transient entry for `targetKey` already exists.
3. **Split the `entriesByKey` space** so user writes and indexer-walk writes don't share hash buckets.

Option 1 is least invasive but risks masking genuine user bugs. Option 2 is the most targeted. Option 3 is the cleanest architecturally but blast-radius touches every code path that reads from `entriesByKey`.

## What does NOT fix it

- SQL-level rewrites (the grammar accepts the SQL; the failure is at execute time).
- Disabling `IsExplicitCloseRequired` / treating each statement as auto-commit — the whole point of wrapping in a transaction is to not do that.
- Disabling secondary indexes — the bug reproduces with a single-column primary-key-only table.

## Related boundary errors

With the same multi-statement pattern, these errors also surfaced in Gitea's dashboard and were fixed incrementally:

- `issueIDsFromSearch, pq: already closed` — same tx state corruption symptom, fixed in `ddde60eb` by deferring the outer reader close in `jointRowReader.Read`.
- `eventsource/manager_run.go: Unable to get UIDcounts: pq: syntax error: unexpected CASE at position 24` — **still open**. `SUM(CASE WHEN … END)` needs a new engine-level `AggExp` AST node that holds a ValueExp instead of a column, parallel to `AggColSelector`. `embedded/sql/gitea_background_regression_test.go::TestSumCaseWhenAggregate` carries a `t.Skip(...)` marker pointing at this follow-up. The error surface is background (Gitea's notification event poller); all user-visible pages render.

Post-fix remaining background-only errors against Gitea 1.25.5:

- `workflows.go init.0.1: yaml unmarshal errors` — Gitea's own bindata YAML init, unrelated to the DB layer.
- `InsertRun, pq: values are not comparable` — fires through the pgsql wire on `INSERT INTO action_run …` with 22 scalar params. A direct engine-level INSERT with the same schema + bound types passes (`TestInsertActionRunShape`), so the error is in the pgsql wire's type coercion during Bind, not in the SQL engine. Needs pgsql-wire-level bind tracing to root-cause. Safe to defer.

## Acceptance

Fixed when:

- The minimal repro above runs clean through the pgsql wire inside a `BEGIN/COMMIT`.
- Gitea `POST /api/v1/user/repos` returns 201 and the repo is browseable via the UI.
- `go test ./embedded/store/... ./embedded/sql/...` full-suite green.
- No new / expanded test failure elsewhere in the repo.
