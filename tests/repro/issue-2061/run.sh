#!/usr/bin/env bash
# Reproduce codenotary/immudb#2061:
#   "ERROR: indexing failed at 'data/<db>' due to error: tbtree: key not found"
#   after a hot-restore of a SQL database with composite indexes and deleted rows.
#
# Two immudb instances are started side-by-side (src on 3322 + 9497, dst on 3323 + 9498).
# A SQL workload exercising a UNIQUE composite index plus DELETEs and UPDATEs is run on
# the source; the source is then hot-backed-up; a fresh destination performs hot-restore
# and we tail the destination log for the suspect indexer error.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
WORK="/tmp/immudb-2061"
SRC_DIR="$WORK/src"
DST_DIR="$WORK/dst"
BAK_DIR="$WORK/backup"
SRC_LOG_DIR="$WORK"
DST_LOG_DIR="$WORK"
SRC_LOG="$WORK/src.log"
DST_LOG="$WORK/dst.log"
SRC_TOKEN="$WORK/_admin_src"
DST_TOKEN="$WORK/_admin_dst"
SRC_PORT=3322
DST_PORT=3323
SRC_METRICS=9497
DST_METRICS=9498
DB=repro
PASSWORD='Repro@2061!'
# Workload size — target ~4 GB on-disk for the source data dir.
TOTAL=${TOTAL:-2000000}       # row count
BATCH=${BATCH:-60}            # rows per multi-row INSERT; CLI argv must fit kernel per-arg cap (128 KB)
VALUE_BYTES=${VALUE_BYTES:-2000}   # per-row value padding; PAD ~ VALUE_BYTES/2 chars after newline-strip
DELETE_BATCH=${DELETE_BATCH:-50000}
UPDATE_BATCH=${UPDATE_BATCH:-50000}
OBSERVE_SECS=${OBSERVE_SECS:-300}

IMMUDB="$REPO_ROOT/immudb"
IMMUADMIN="$REPO_ROOT/immuadmin"
IMMUCLIENT="$REPO_ROOT/immuclient"

PIDS=()

cleanup() {
    for pid in "${PIDS[@]:-}"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
}
trap cleanup EXIT INT TERM

log() { printf '\n=== %s ===\n' "$*"; }

wait_port() {
    local port=$1
    local timeout=${2:-30}
    local i=0
    until (echo > "/dev/tcp/127.0.0.1/$port") 2>/dev/null; do
        i=$((i + 1))
        if (( i > timeout * 10 )); then
            echo "timeout waiting for port $port — last server stdout follows:" >&2
            cat "$WORK"/*.stdout 2>/dev/null >&2 || true
            exit 3
        fi
        sleep 0.1
    done
}

# 1. Build check
log "1) Build check"
for b in "$IMMUDB" "$IMMUADMIN" "$IMMUCLIENT"; do
    if [[ ! -x "$b" ]]; then
        echo "missing $b — run 'make immudb immuadmin immuclient' first" >&2
        exit 2
    fi
done
echo "binaries OK"

# 2. Clean slate
log "2) Clean slate"
rm -rf "$WORK"
mkdir -p "$SRC_DIR" "$DST_DIR" "$BAK_DIR"

# 3. Start source server
log "3) Start source server (port $SRC_PORT)"
"$IMMUDB" \
    --dir "$SRC_DIR" \
    --port "$SRC_PORT" \
    --metrics-server-port "$SRC_METRICS" \
    --pgsql-server=false \
    --admin-password "$PASSWORD" \
    --force-admin-password \
    --logdir "$SRC_LOG_DIR" \
    --logfile "src.log" \
    > "$WORK/src.stdout" 2>&1 &
PIDS+=($!)
wait_port "$SRC_PORT"
echo "src up (pid ${PIDS[-1]})"

# 4. Source workload via immuadmin + immuclient
log "4) Create source DB and run SQL workload"
"$IMMUADMIN" -p "$SRC_PORT" --tokenfile "$SRC_TOKEN" \
    login immudb < <(printf '%s' "$PASSWORD") >/dev/null
"$IMMUADMIN" -p "$SRC_PORT" --tokenfile "$SRC_TOKEN" \
    database create "$DB" >/dev/null
echo "db created"

# Use immuclient with its own tokenfile + database flag.
client_src() {
    "$IMMUCLIENT" \
        -a 127.0.0.1 -p "$SRC_PORT" \
        --tokenfile "$WORK/_client_src" \
        --username immudb --password "$PASSWORD" \
        --database "$DB" \
        "$@"
}

client_src login immudb < <(printf '%s' "$PASSWORD") >/dev/null
client_src use "$DB" >/dev/null

# Composite UNIQUE index → embedded/store InjectiveMapping=true path.
client_src exec "
    CREATE TABLE t (
        id INTEGER AUTO_INCREMENT,
        k1 VARCHAR[64],
        k2 VARCHAR[64],
        v VARCHAR[$((VALUE_BYTES + 256))],
        PRIMARY KEY id
    );
" >/dev/null
client_src exec "CREATE INDEX ON t(k1);" >/dev/null
client_src exec "CREATE UNIQUE INDEX ON t(k1, k2);" >/dev/null
echo "schema created (target ~$((TOTAL * VALUE_BYTES / 1024 / 1024 / 1024)) GB of values)"

# Pre-build a per-row value padding (~VALUE_BYTES of repeating chars).
PAD=$(yes 'x' | head -c "$VALUE_BYTES" | tr -d '\n')

# Bulk insert TOTAL rows in batches (single statement multi-row insert).
# Each batch is its own immuclient process call — the kernel caps a single argv
# string at MAX_ARG_STRLEN=128 KB, so BATCH * (PAD + framing) must stay under that.
echo "inserting $TOTAL rows in batches of $BATCH..."
batches=$((TOTAL / BATCH))
start_ts=$(date +%s)
for ((b=0; b<batches; b++)); do
    start=$((b * BATCH))
    sql="INSERT INTO t (k1, k2, v) VALUES "
    sep=""
    for ((i=0; i<BATCH; i++)); do
        n=$((start + i))
        sql+="${sep}('grp_$((n % 200))', 'sub_${n}', '${PAD}_${n}')"
        sep=","
    done
    sql+=";"
    err=$(client_src exec "$sql" 2>&1 >/dev/null) || {
        echo "INSERT batch $b failed (sql len ${#sql}): $err" >&2
        exit 4
    }
    if [[ "$err" == *Error* || "$err" == *error* ]]; then
        echo "INSERT batch $b reported: $err" >&2
        exit 4
    fi
    if (( (b + 1) % 500 == 0 )); then
        elapsed=$(( $(date +%s) - start_ts ))
        rate=$(( (b + 1) * BATCH / (elapsed > 0 ? elapsed : 1) ))
        printf '  inserted %d/%d rows (%ds, %d rows/s)\n' \
            "$(( (b + 1) * BATCH ))" "$TOTAL" "$elapsed" "$rate"
    fi
done
echo "  insert complete ($TOTAL rows)"
du -sh "$SRC_DIR" || true

# Delete in large ranges — single SQL statement per range, keeps dest replay heavy
# on composite-index tombstones.
echo "deleting rows in ranges of $DELETE_BATCH (target ~30%)..."
del_target=$((TOTAL * 30 / 100))
del_start=1
while (( del_start <= del_target )); do
    del_end=$((del_start + DELETE_BATCH - 1))
    if (( del_end > del_target )); then del_end=$del_target; fi
    client_src exec "DELETE FROM t WHERE id BETWEEN $del_start AND $del_end;" >/dev/null
    printf '.'
    del_start=$((del_end + 1))
done
echo " done (~$del_target deletes)"

# Update ranges that change the composite-key column k2 — exercises
# InjectiveMapping GetBetween + ReadTxEntry path.
echo "updating rows in ranges of $UPDATE_BATCH (target ~10%)..."
upd_start=$((TOTAL / 2))
upd_end=$((upd_start + TOTAL / 10))
cur=$upd_start
while (( cur <= upd_end )); do
    next=$((cur + UPDATE_BATCH - 1))
    if (( next > upd_end )); then next=$upd_end; fi
    client_src exec "UPDATE t SET k2 = 'sub_' || CAST(id AS VARCHAR) || '_new' WHERE id BETWEEN $cur AND $next;" >/dev/null
    printf '.'
    cur=$((next + 1))
done
echo " done (~$((upd_end - upd_start)) updates)"

# Phase D — multi-statement transactions: INSERT a fresh row + UPDATE a
# previously inserted row, in the same single-call tx (semicolon-separated).
# Pattern: "INSERT new (k1=multi_M, k2=msa_M); UPDATE older row by id"
# — INSERT+UPDATE on the SAME row trips a separate transiency bug
# (cannot change non-transient key to transient or vice versa) on master,
# so we deliberately update a different row to keep this phase additive
# rather than exit-blocking.
MULTI_STMT_COUNT=${MULTI_STMT_COUNT:-5000}
# Anchor updates at the tail of the bulk-insert id range, well past the
# delete and update windows from phases B and C.
upd_anchor=$(( TOTAL - MULTI_STMT_COUNT - 100 ))
if (( upd_anchor < 1 )); then upd_anchor=1; fi
echo "running $MULTI_STMT_COUNT multi-statement INSERT+UPDATE-other iterations (anchor id=$upd_anchor)..."
multi_start_ts=$(date +%s)
multi_failed=0
for ((m=0; m<MULTI_STMT_COUNT; m++)); do
    k1="multi_$((m % 100))"
    k2="msa_${m}"
    target_id=$((upd_anchor + m))
    sql="INSERT INTO t (k1, k2, v) VALUES ('${k1}', '${k2}', 'mv_${m}'); UPDATE t SET v = 'mu_${m}' WHERE id = ${target_id};"
    if ! err=$(client_src exec "$sql" 2>&1 >/dev/null); then
        if (( multi_failed < 3 )); then
            echo "  multi-stmt iter $m failed: $err" >&2
        fi
        multi_failed=$((multi_failed + 1))
    fi
    if (( (m + 1) % 500 == 0 )); then
        elapsed=$(( $(date +%s) - multi_start_ts ))
        printf '  multi-stmt %d/%d (%ds, %d failed so far)\n' \
            "$((m + 1))" "$MULTI_STMT_COUNT" "$elapsed" "$multi_failed"
    fi
done
echo "  multi-stmt phase complete (failures: $multi_failed)"

# Phase E — INSERT → DELETE → INSERT cycles that REUSE the same composite key
# value across separate transactions. Each cycle:
#   tx N:    INSERT (k1='cyc_X', k2='cyc_X')
#   tx N+1:  DELETE the just-inserted row
#   tx N+2:  INSERT a new row with the SAME (k1, k2) — composite-index entry is
#            "freed" by the tombstone in N+1 and re-occupied in N+2.
# This is exactly the pattern most likely to surface a stale GetBetween →
# ReadTxEntry path on hot-restore replay.
CYCLE_COUNT=${CYCLE_COUNT:-3000}
echo "running $CYCLE_COUNT INSERT/DELETE/INSERT cycles reusing composite keys..."
cyc_start_ts=$(date +%s)
cyc_failed=0
for ((c=0; c<CYCLE_COUNT; c++)); do
    k1="cyc_$((c % 50))"
    k2="cyc_${c}"
    # 3 separate immuclient calls = 3 separate transactions
    if ! client_src exec "INSERT INTO t (k1, k2, v) VALUES ('${k1}', '${k2}', 'cv_a_${c}');" >/dev/null 2>&1; then
        cyc_failed=$((cyc_failed + 1)); continue
    fi
    if ! client_src exec "DELETE FROM t WHERE k1 = '${k1}' AND k2 = '${k2}';" >/dev/null 2>&1; then
        cyc_failed=$((cyc_failed + 1)); continue
    fi
    if ! client_src exec "INSERT INTO t (k1, k2, v) VALUES ('${k1}', '${k2}', 'cv_b_${c}');" >/dev/null 2>&1; then
        cyc_failed=$((cyc_failed + 1)); continue
    fi
    if (( (c + 1) % 500 == 0 )); then
        elapsed=$(( $(date +%s) - cyc_start_ts ))
        printf '  cycle %d/%d (%ds, %d failed so far)\n' \
            "$((c + 1))" "$CYCLE_COUNT" "$elapsed" "$cyc_failed"
    fi
done
echo "  cycle phase complete (failures: $cyc_failed)"

"$IMMUADMIN" -p "$SRC_PORT" --tokenfile "$SRC_TOKEN" database flush "$DB" >/dev/null || true
sleep 2

# 5. Hot backup
log "5) Hot backup source -> $BAK_DIR/dump.bin"
"$IMMUADMIN" -p "$SRC_PORT" --tokenfile "$SRC_TOKEN" \
    hot-backup "$DB" -o "$BAK_DIR/dump.bin"
ls -la "$BAK_DIR/dump.bin"

# 6. Start destination server
log "6) Start destination server (port $DST_PORT)"
"$IMMUDB" \
    --dir "$DST_DIR" \
    --port "$DST_PORT" \
    --metrics-server-port "$DST_METRICS" \
    --pgsql-server=false \
    --admin-password "$PASSWORD" \
    --force-admin-password \
    --logdir "$DST_LOG_DIR" \
    --logfile "dst.log" \
    > "$WORK/dst.stdout" 2>&1 &
PIDS+=($!)
wait_port "$DST_PORT"
echo "dst up (pid ${PIDS[-1]})"

"$IMMUADMIN" -p "$DST_PORT" --tokenfile "$DST_TOKEN" \
    login immudb < <(printf '%s' "$PASSWORD") >/dev/null

# 7. Hot restore — DB does not exist, hot-restore auto-creates it as replica.
log "7) Hot restore into fresh dst DB"
"$IMMUADMIN" -p "$DST_PORT" --tokenfile "$DST_TOKEN" \
    hot-restore "$DB" -i "$BAK_DIR/dump.bin" --force-replica
echo "restore command finished"

# 8. Observation window — let the destination indexer chew on the restored stream.
log "8) Watching dst log for 'indexing failed' (${OBSERVE_SECS} s window)"
# Every 30 s, print the last reported indexed ts for the repro DB so we can see progress.
for ((t=0; t<OBSERVE_SECS; t+=30)); do
    sleep 30
    last_ts=$(grep "repro/index_" "$DST_LOG_DIR"/*/dst.log 2>/dev/null \
        | grep -oE 'ts=[0-9]+' | tail -1 || true)
    failed=$(grep -c "indexing failed at" "$DST_LOG_DIR"/*/dst.log 2>/dev/null \
        | awk -F: '{s+=$2} END {print s+0}')
    printf '  +%ds  last_seen=%s  indexing-failed=%d\n' "$((t+30))" "${last_ts:-?}" "$failed"
done

# 8b. Read-side sanity check on dst — a silent ErrKeyNotFound from the indexer
# would surface here as a SQL error or wrong row count even if the log is clean.
log "8b) Read-side sanity check on dst"
client_dst() {
    "$IMMUCLIENT" \
        -a 127.0.0.1 -p "$DST_PORT" \
        --tokenfile "$WORK/_client_dst" \
        --username immudb --password "$PASSWORD" \
        --database "$DB" \
        "$@"
}
client_dst login immudb < <(printf '%s' "$PASSWORD") >/dev/null || true
client_dst use "$DB" >/dev/null || true

# Use the composite UNIQUE index — k1+k2 lookup. If the indexer dropped entries,
# COUNT will be off or the queries will error.
echo "  COUNT(*) on t:"
client_dst query "SELECT COUNT(*) AS c FROM t;" 2>&1 | head -10

echo "  lookup via composite-index path (cycled key from phase E):"
client_dst query "SELECT id, k1, k2 FROM t WHERE k1 = 'cyc_0' AND k2 = 'cyc_0';" 2>&1 | head -10

echo "  lookup via composite-index path (multi-stmt key from phase D):"
client_dst query "SELECT id, k1, k2 FROM t WHERE k1 = 'multi_0' AND k2 = 'msb_0';" 2>&1 | head -10

echo "  scan via simple index k1:"
client_dst query "SELECT COUNT(*) FROM t WHERE k1 = 'grp_42';" 2>&1 | head -10

# 9. Verdict
log "9) Verdict"
# The actual logfile lands at $DST_LOG_DIR/<data-dir-prefix>/dst.log because
# the immudb logger interprets --logdir as a relative path.
ACTUAL_DST_LOG=$(find "$DST_DIR" -name dst.log 2>/dev/null | head -1)
ACTUAL_DST_LOG=${ACTUAL_DST_LOG:-$DST_LOG}
hits=$(grep -c "indexing failed at" "$ACTUAL_DST_LOG" 2>/dev/null); hits=${hits:-0}
tbtree_hits=$(grep -c "tbtree: key not found" "$ACTUAL_DST_LOG" 2>/dev/null); tbtree_hits=${tbtree_hits:-0}
echo "(reading $ACTUAL_DST_LOG)"

echo "indexing failed lines : $hits"
echo "tbtree key-not-found  : $tbtree_hits"

if (( tbtree_hits > 0 )); then
    echo
    echo "FIRST MATCHING LINE:"
    grep -m1 "tbtree: key not found" "$ACTUAL_DST_LOG"
    echo
    echo "REPRO=yes"
    exit 0
elif (( hits > 0 )); then
    echo
    echo "FIRST MATCHING LINE:"
    grep -m1 "indexing failed at" "$ACTUAL_DST_LOG"
    echo
    echo "REPRO=partial (indexing-failed without tbtree:key-not-found — needs investigation)"
    exit 0
else
    echo "REPRO=no"
    exit 1
fi
