# PostgreSQL compatibility — architectural roadmap

Status: proposal / design doc. Not implemented.

## Context & motivation

immudb's PG wire compatibility today is a **regex-triage + canned-response** layer in
`pkg/pgsql/server/`. It works for the narrow set of queries each ORM happens to send,
but it's whack-a-mole: every new client (psql `\d`, pgAdmin, Rails, XORM, Gitea, k3s/kine,
F1DB dumps) has needed its own regex + handler, and the handlers return fabricated
NULL-filled rows rather than the real catalog.

Recent incidents caused by this architecture:
- `c5718b6b` — psql segfaulted on `\d <table>` because the regex column extractor silently
  dropped two anonymous SELECT items, leaving a 13-col RowDescription vs. 15 expected.
- `8c024ae3` — `CREATE VIEW v(col1,col2) AS …` from F1DB dumps broke because column lists
  in CREATE VIEW confused the regex.
- `674b78c6` — k3s/kine DDL broke because of `COLLATE`; regex strip added.
- `c2258ba7` — PG-dump UNIQUE constraints broke DDL parsing; regex strip added.

The root cause is the same each time: **string manipulation of SQL instead of catalog-backed
semantics and AST-driven rewriting.**

This doc plans two long-running pieces of work:

- **Part A** — replace canned pg_catalog responses with real `pg_catalog` *system tables*
  over immudb's catalog, so arbitrary queries against `pg_class`/`pg_attribute`/etc. get
  correct answers from the SQL engine.
- **Part B** — replace the scattered regex rewriters with an AST-based query rewriter
  driven by a real PostgreSQL parser.

Each part is independently useful. Part A is the harder but higher-leverage piece and
should land first.

---

## Part A — `pg_catalog` as real system tables

### Approach

Rather than CREATE VIEW at startup (views need a physical source to read from), register
each `pg_catalog` object as a **system table whose rows come from a Go iterator that
walks immudb's live catalog at query time.** This pattern already exists for `pg_type` at
`embedded/sql/catalog.go:108-149`; generalise it.

Benefit: the SQL engine handles JOINs, WHERE, aggregates, ORDER BY, LIMIT, subqueries
for free. No more per-query handlers. `psql \d` just works because its literal query
executes against real rows.

### Phase A1 — system-table registration mechanism

Goal: replace the one-off `pg_type` hardcode with a pluggable registry. No user-visible
change yet.

**Files:**
- `embedded/sql/catalog.go:108-149` — refactor hardcoded `pg_type` into a registration call.
- `embedded/sql/system_tables.go` (new) — define the `SystemTable` interface:
  ```go
  type SystemTable interface {
      Name() string
      Schema() string           // "pg_catalog" or "information_schema"
      Columns() []ColDescriptor
      Scan(ctx context.Context, cat *Catalog, tx *SQLTx) (RowReader, error)
      IndexOn() []string        // columns that get a pseudo-PK for cheap lookup
  }
  func RegisterSystemTable(t SystemTable)
  ```
- `embedded/sql/stmt.go` (or wherever table resolution happens) — when resolving a
  reference like `pg_catalog.pg_class`, check the registry first; fall back to the
  user catalog.

**Verification:** existing `pg_type` tests pass unchanged; new test registers a dummy
system table and confirms it's queryable via the engine.

### Phase A2 — core relation catalog (`pg_class`, `pg_attribute`, `pg_index`, `pg_namespace`, `pg_am`)

Goal: psql `\d <table>` and `\dt` return real data.

**New system tables** (all under `pkg/pgsql/sys/` — new package to keep the PG-specific
code away from the core engine):

| Table | Row source | Columns (minimum) |
|---|---|---|
| `pg_namespace` | static: `pg_catalog` (oid=11), `public` (oid=2200), `information_schema` (oid=13) | oid, nspname, nspowner, nspacl |
| `pg_am` | static: `btree` (oid=403), `hash` (oid=405) | oid, amname, amhandler, amtype |
| `pg_class` | one row per user table, one per view, one per index | oid (stable hash of name), relname, relnamespace, relkind ('r'/'v'/'i'), relchecks, relhasindex, relhasrules(false), relhastriggers(false), relrowsecurity(false), relforcerowsecurity(false), relispartition(false), reltablespace(0), reloftype(0), relpersistence('p'), relreplident('d'), relam (0 for table, 403 for index), reltoastrelid(0), relhasoids(false), relnatts |
| `pg_attribute` | one row per column per table | attrelid, attname, atttypid (PG OID), attlen, attnum, attnotnull, atthasdef, attidentity(''), attgenerated(''), attisdropped(false), attcollation(0), attndims(0), attbyval |
| `pg_index` | one row per immudb index | indexrelid, indrelid, indnatts, indnkeyatts, indisunique, indisprimary, indkey (int2vector), indcollation(''), indclass(''), indoption(''), indexprs(null), indpred(null) |
| `pg_constraint` | one row per PK + one per unique index | oid, conname, connamespace, contype ('p'/'u'), conrelid, conindid, conkey |
| `pg_description` | empty (or extract from immudb table comments when supported) | objoid, classoid, objsubid, description |

OIDs are synthesized from stable hashes of `(schema, name)` so they're consistent across
server restarts. Reserve the low 16384 range for pre-assigned PG-catalog-object OIDs
(matching real PG, which helps Rails' type map).

**Built-in functions** in `embedded/sql/functions/pg.go` (new):
- `pg_table_is_visible(oid) → boolean` — true for all user objects (single-schema model)
- `pg_get_expr(text, oid) → text` — returns input unchanged (stub, sufficient for psql)
- `pg_get_indexdef(oid) → text` — builds `CREATE [UNIQUE] INDEX <name> ON <table> (<cols>)`
- `pg_get_constraintdef(oid) → text` — builds PK or UNIQUE text
- `pg_get_userbyid(oid) → name` — returns `'immudb'`
- `format_type(oid, int4) → text` — maps PG OID + typmod back to `integer`, `varchar(256)`, etc.
- `current_schema() → name` — returns `'public'`
- `current_schemas(bool) → name[]` — returns `{pg_catalog, public}`
- `current_database() → name` — returns the session's DB
- `regclass(text) → oid` and `regtype(text) → oid` — cast functions keyed on `pg_class.relname` / `pg_type.typname`
- `array_to_string(anyarray, text) → text` — stub
- `quote_ident(text) → text` — wrap identifier in `"…"` if reserved or mixed case
- `has_schema_privilege`, `has_table_privilege`, `has_database_privilege` — stubs returning `true`
- `pg_total_relation_size(oid) → int8`, `pg_relation_size(oid) → int8` — return 0
- Aggregate-lookup casts used by psql: `::pg_catalog.text`, `::pg_catalog.regtype::pg_catalog.text` — handled by Part B rewriter stripping the `pg_catalog.` qualifier and the cast chain.

**Retirements** (handlers deleted once A2 is in, verified by psql smoke tests):
- `handlePgSystemQuery` (pgadmin_compat.go:102)
- `handlePgSystemQueryDataOnly` (pgadmin_compat.go:425)
- `handlePgAttributeForTable` (immudb_functions.go:591)
- `handlePgClass*` / `handlePgAttribute*` regex branches in `stmts_handler.go`.
- `pgSystemQueryRe`, `pgSystemJoinRe`, `pgVirtualTableFromRe`, `pgAttributeForTableRe`,
  `pgAttributeForTableParamRe`, `xormColumnsRe` (stmts_handler.go:25-123) —
  once each backing object exists as a system table.

### Phase A3 — auxiliary catalogs (`pg_database`, `pg_roles`, `pg_settings`, `pg_type` expansion)

- `pg_database` — one row per immudb database (needs `mr`/server interface for enumeration,
  or fall back to single-row for current db).
- `pg_roles` — one row for the session user.
- `pg_settings` — canned rows for `server_version`, `client_encoding`, `TimeZone`, etc.
  Matches what pgAdmin/DBeaver probe for.
- `pg_type` — expand the current 17-type list at `embedded/sql/catalog.go:108-149` to the
  full standard set (~40 types) plus composite/array rows for every user table.
  Important for Rails' `load_additional_types`.

Retire: `handlePgTypeRows` (pgadmin_compat.go:371), `pg_database`/`pg_roles`/`pg_settings`
branches of `handlePgSystemQuery`.

### Phase A4 — `information_schema`

Implement as real SQL views (immudb supports CREATE VIEW including joins/CASE) over the
pg_catalog system tables installed in A2–A3:

```sql
CREATE VIEW information_schema.tables AS
  SELECT current_database() AS table_catalog,
         n.nspname          AS table_schema,
         c.relname          AS table_name,
         CASE c.relkind WHEN 'r' THEN 'BASE TABLE' WHEN 'v' THEN 'VIEW' END AS table_type
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind IN ('r','v');

CREATE VIEW information_schema.columns AS
  SELECT current_database() AS table_catalog, n.nspname AS table_schema,
         c.relname AS table_name, a.attname AS column_name,
         a.attnum AS ordinal_position, /* etc. */
  FROM pg_catalog.pg_attribute a
  JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE a.attnum > 0 AND NOT a.attisdropped;
```

These should be installed on database-create (bootstrap DDL). Store as real views so they
show up in `pg_class` / `pg_views` / `\dv` automatically.

Retire: `handleInfoSchemaColumnsQuery` (pgadmin_compat.go:998), `infoSchemaColumnsRe`.

### Phase A5 — view compatibility: `pg_tables`, `pg_indexes`, `pg_views`

Like A4, these are real views over the system tables:

```sql
CREATE VIEW pg_catalog.pg_tables AS
  SELECT n.nspname AS schemaname, c.relname AS tablename,
         pg_get_userbyid(c.relowner) AS tableowner,
         NULL AS tablespace, c.relhasindex AS hasindexes,
         c.relhasrules AS hasrules, c.relhastriggers AS hastriggers,
         c.relrowsecurity AS rowsecurity
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind IN ('r','p');
```

Same for `pg_indexes`, `pg_views`, `pg_sequences`.

Retire: `handlePgTablesQuery`, `handlePgIndexesQuery`, `pgTablesRe`, `pgIndexesRe`.

### Part A exit criteria

- `psql \d <table>`, `\dt`, `\di`, `\dv`, `\l`, `\du`, `\df` all return correct data.
- `SELECT … FROM pg_catalog.pg_class JOIN pg_attribute …` arbitrary queries work.
- Rails migration connect sequence completes without the "TABLE DOESN'T EXIST" storm.
- All existing `pkg/pgsql/server/*_test.go` tests pass with zero canned-handler code.
- pgadmin_compat.go shrinks from ~1100 LOC to ~100 LOC (just the advisory-lock stub).

---

## Part B — AST-based SQL rewriter

### Motivation

`pkg/pgsql/server/query_machine.go:484-625` has **50+ regex replacements** applied before
every query hits immudb's parser. They're order-sensitive, mutually interfering, and miss
edge cases (quoted identifiers, dollar-quoted strings, comments, nested casts). `unique_ddl.go`
and pieces of `stmts_handler.go` layer on more.

We want: parse PG dialect → walk AST → rewrite targeted constructs → deparse → feed to
immudb's own parser.

### Parser choice

Two candidates:

**Option B-cgo — `github.com/pganalyze/pg_query_go/v6`** (recommended)
- Wraps the real PostgreSQL parser (libpg_query). Parses every PG syntax construct
  correctly including dollar-quoted strings, comments, `COLLATE`, arrays, jsonb path,
  `CASE`, every DDL form.
- Used by CockroachDB, YugabyteDB, pganalyze, Supabase.
- **Downside: cgo.** immudb currently has zero cgo (verified: no `import "C"` anywhere, no
  `// #cgo` directives, no build-tag-gated cgo files). Adding cgo affects cross-compile
  matrix (Windows/ARM builds need a C toolchain), Docker multi-arch manifests, and
  release build times.

**Option B-gopure — `github.com/auxten/postgresql-parser`** (fallback)
- CockroachDB parser fork; pure Go. Weaker than pg_query for bleeding-edge PG syntax but
  covers everything we care about (DDL, DML, casts, comments).
- No cgo impact. Larger dependency footprint (~100k LOC vendored).

Recommendation: **start with B-gopure** to avoid the cgo transition. If we hit coverage
gaps (likely around pg-specific operators / extensions), switch to B-cgo and deal with
the build-system change as a separate PR.

### Rewriter design

**Entry point**: `pkg/pgsql/server/rewrite/rewriter.go` (new).

```go
type Rewriter struct { /* config / rule set */ }
func (r *Rewriter) Rewrite(sql string) (string, error)
```

Called from `query_machine.go` at the point where `pgTypeReplacements` currently runs,
replacing the whole translator block.

**Rules** (each a small AST-walker, independently testable):

1. **StripSchemaQualifier** — drop `pg_catalog.` and `public.` from identifiers when
   referring to system or default-schema objects. (A2 system tables live in the logical
   `pg_catalog` schema but we accept unqualified references too.)
2. **StripPGCasts** — `x::regclass` → `x`, `x::name` → `x`, `x::oid` → `x`, `x::regtype` → `x`,
   `x::regproc` → `x`, `x::pg_catalog.text` → `x` (these casts were only needed for type
   coercion; our system-table scanners already return the right types).
3. **StripCollate** — remove `COLLATE "…"` from expressions and DDL.
4. **StripCreateViewColList** — `CREATE VIEW v(a,b) AS SELECT …` → `CREATE VIEW v AS SELECT …`.
5. **RewriteTypes** — map PG-only type names in DDL:
   `CHARACTER VARYING[(n)]` → `VARCHAR[n|256]`, `TEXT` → `VARCHAR[1048576]`,
   `BIGSERIAL` → `BIGINT AUTO_INCREMENT`, `SERIAL` → `INTEGER AUTO_INCREMENT`,
   `BYTEA` → `BLOB`, `TIMESTAMP(n)` → `TIMESTAMP`, `DOUBLE PRECISION` → `FLOAT`,
   `INTEGER[]`/`TEXT[]` → `VARCHAR[1048576]`, `NUMERIC[(p,s)]` → `FLOAT`.
6. **ExtractInlineUnique** — `col INTEGER UNIQUE` → `col INTEGER`, plus emit sibling
   `CREATE UNIQUE INDEX` statement against the same table.
7. **DropUnsupportedDDL** — drop `SET`, `CREATE TYPE`, `CREATE FUNCTION`, `CREATE TRIGGER`,
   `ALTER TABLE … OWNER TO`, `GRANT`, `REVOKE`, `COMMENT ON`, standalone `FOREIGN KEY`.
   Return empty statement list for these (harmless).
8. **NormalizeReservedIdent** — `"order"` → `_order`, digit-prefix `"1col"` → `_1col` etc.,
   when safe (only in DDL column lists and SELECT targets, not string literals).
9. **StripComments** — drop `-- …` and `/* … */` before feeding to immudb's parser.
   (This already works after commit 82000e0c but moves into the rewriter for uniformity.)
10. **AdvisoryLockStub** — `SELECT pg_try_advisory_lock($1)` → `SELECT true` (keeps Rails'
    migration serialization happy).

**Testing**: each rule has its own table-driven test in
`pkg/pgsql/server/rewrite/<rule>_test.go`, asserting (input SQL, expected output SQL)
pairs. Plus an end-to-end test that feeds real psql/pgAdmin/Rails transcripts through
the rewriter and asserts the output parses cleanly with immudb's engine.

### Part B retirements

When the rewriter lands:
- `pkg/pgsql/server/query_machine.go:484-625` (`pgTypeReplacements` + helpers) — deleted.
- `pkg/pgsql/server/unique_ddl.go` — deleted, logic moves into `ExtractInlineUnique`.
- `pkg/pgsql/server/stmts_handler.go:132-180` (DDL blacklist) — deleted, moves into
  `DropUnsupportedDDL`.
- The `removePGCatalogReferences` in query_machine.go (≈line 958) — deleted.

Net LOC: `pkg/pgsql/server/` should shrink by ~1500 lines.

---

## Phasing & ordering

| Phase | Depends on | Time estimate | Deliverable |
|---|---|---|---|
| A1: system-table registry | — | ~1 week | Internal refactor, pg_type still hardcoded behaviour |
| A2: pg_class/pg_attribute/pg_index/pg_namespace/pg_am + functions | A1 | ~2 weeks | `\d` works |
| A3: pg_database/pg_roles/pg_settings + pg_type expansion | A2 | ~1 week | pgAdmin connect works |
| A4: information_schema views | A2 | ~3 days | k3s/kine + Hibernate work |
| A5: pg_tables/pg_indexes/pg_views compat views | A2 | ~2 days | XORM/GORM work without handlers |
| B1: parser selection + scaffold | — (independent) | ~3 days | Rewriter skeleton; no rules yet |
| B2: per-rule implementation + retirement | B1, A-phases done | ~2 weeks | Regex layers deleted |

Phases A1–A5 are a hard prerequisite for dropping canned handlers; B1–B2 can run in
parallel with late A phases. Ship each phase behind a feature flag (`--pg-catalog-v2`)
so we can A/B against the old canned-handler path in production for one release cycle.

---

## Out of scope

- **Schemas / namespaces.** immudb remains single-schema. `pg_namespace` has a fixed
  set of three rows.
- **SEQUENCE, TRIGGER, EVENT, EXTENSION, RULE.** No SQL engine support; DDL rewriter
  drops them.
- **Windows functions in pg_catalog views.** Not needed by current clients.
- **Replicating PostgreSQL parse/plan stages.** We're not a PG clone; the goal is "enough
  compatibility that psql/ORMs work," not ANSI conformance.
- **Multi-database `pg_database` enumeration from SQL.** Would need the server-level
  database manager exposed into the SQL tx context; deferred unless a concrete client
  demands it.

---

## Risks

- **Catalog OID stability.** Hashing relname → oid is stable across restarts but not
  across renames. Clients that cache oids (Rails type map) will see stale oids after a
  rename. Mitigation: document this limitation; most clients re-read the oid map on
  connect.
- **Performance of system-table scans for large catalogs.** Walking every column of every
  table on every `\d` is O(N·M). Cache the catalog snapshot per-transaction.
- **Parser selection lock-in.** Choosing gopure first means we may later redo the
  migration to pg_query_go if coverage is insufficient. The rule-based rewriter API
  insulates callers, so the switch is contained to `pkg/pgsql/server/rewrite/`.
- **Regression surface.** Part A deletes ~1100 lines of canned handlers. Each phase
  must ship with a recorded psql / pgAdmin / Rails / XORM / Gitea transcript test to
  guard every historically-broken query.
