# Multi-statement SQL transactions trip `ErrCannotUpdateKeyTransiency` on UPDATE-after-INSERT of the same row

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

## Suspected root cause

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

With the same multi-statement pattern, these errors also surface in Gitea's dashboard and were left unfixed until the transiency bug is rooted out:

- `issueIDsFromSearch, pq: already closed` — same tx state corruption, different symptom.
- `eventsource/manager_run.go: Unable to get UIDcounts: pq: syntax error: unexpected CASE at position 24` — separate grammar gap.

## Acceptance

Fixed when:

- The minimal repro above runs clean through the pgsql wire inside a `BEGIN/COMMIT`.
- Gitea `POST /api/v1/user/repos` returns 201 and the repo is browseable via the UI.
- `go test ./embedded/store/... ./embedded/sql/...` full-suite green.
- No new / expanded test failure elsewhere in the repo.
