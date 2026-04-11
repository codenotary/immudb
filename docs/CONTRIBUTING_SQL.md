# Contributing to immudb's SQL Engine

This guide covers how to add new SQL features, functions, and PostgreSQL compatibility improvements to immudb.

## Architecture Overview

```
Client (psql, pgx, JDBC, ORM)
    |
    v
pkg/pgsql/server/         -- PostgreSQL v3 wire protocol
    |
    v
embedded/sql/engine.go    -- SQL engine (parse, plan, execute)
embedded/sql/stmt.go      -- AST nodes and statement execution
embedded/sql/sql_grammar.y -- Yacc grammar (generates sql_parser.go)
embedded/sql/parser.go    -- Lexer and keyword mapping
embedded/sql/catalog.go   -- Schema metadata (tables, columns, indexes)
embedded/sql/functions.go -- Built-in SQL functions
    |
    v
embedded/store/           -- Immutable key-value store with Merkle trees
```

### Key Interfaces

```go
// SQLStmt — any executable SQL statement
type SQLStmt interface {
    readOnly() bool
    requiredPrivileges() []SQLPrivilege
    execAt(ctx, tx, params) (*SQLTx, error)
    inferParameters(ctx, tx, params)
}

// DataSource — a statement that returns rows (SELECT, UNION, CTE, RETURNING)
type DataSource interface {
    SQLStmt
    Resolve(ctx, tx, params, scanSpecs) (RowReader, error)
    Alias() string
}

// Function — a built-in SQL function
type Function interface {
    InferType(cols, params, implicitTable) (SQLValueType, error)
    RequiresType(t, cols, params, implicitTable) error
    Apply(tx *SQLTx, params []TypedValue) (TypedValue, error)
}

// TableResolver — virtual table (pg_catalog, views, CTEs)
type TableResolver interface {
    Table() string
    Resolve(ctx, tx, alias) (RowReader, error)
}
```

## Adding a New SQL Function

### Step 1: Define the function name constant

In `embedded/sql/functions.go`:

```go
const (
    // ... existing constants ...
    MyNewFnCall string = "MY_NEW_FN"
)
```

### Step 2: Implement the Function interface

```go
type myNewFn struct{}

func (f *myNewFn) InferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
    return VarcharType, nil // return type
}

func (f *myNewFn) RequiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
    if t != VarcharType {
        return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
    }
    return nil
}

func (f *myNewFn) Apply(tx *SQLTx, params []TypedValue) (TypedValue, error) {
    if len(params) != 1 {
        return nil, fmt.Errorf("%w: '%s' expects 1 argument", ErrIllegalArguments, MyNewFnCall)
    }
    if params[0].IsNull() {
        return NewNull(VarcharType), nil
    }
    // ... implement logic ...
    return NewVarchar(result), nil
}
```

### Step 3: Register in the builtinFunctions map

```go
var builtinFunctions = map[string]Function{
    // ... existing entries ...
    MyNewFnCall: &myNewFn{},
}
```

### Step 4: Add tests

See [Testing Requirements](#testing-requirements) below.

### Patterns to Follow

- **Simple stub** (returns constant): see `pgTableIsVisible`, `pgCurrentSchema`
- **Variable args** (accepts any count): see `CoalesceFn`, `ConcatFn`
- **Accessing tx context**: see `NowFn` (timestamp), `pgGetUserByIDFunc` (users)
- **Math functions**: see `mathFn` (generic unary/binary math)
- **Null handling**: always check `params[0].IsNull()` and return `NewNull(type)`

## Adding New SQL Syntax

### Step 1: Add keyword to the lexer

In `embedded/sql/parser.go`, add to the `keywords` map:

```go
var keywords = map[string]int{
    // ... existing keywords ...
    "MYNEWKW": MYNEWKW,
}
```

### Step 2: Declare the token in the grammar

In `embedded/sql/sql_grammar.y`:

```yacc
%token <keyword> ... MYNEWKW
```

### Step 3: Add grammar rules

```yacc
my_new_stmt:
    MYNEWKW IDENTIFIER
    {
        $$ = &MyNewStmt{name: $2}
    }
```

Add the rule to the appropriate parent rule (`ddlstmt`, `dmlstmt`, or `dqlstmt`).

### Step 4: Add the statement type

In `embedded/sql/stmt.go`:

```go
type MyNewStmt struct {
    name string
}

func (stmt *MyNewStmt) readOnly() bool                     { return true }
func (stmt *MyNewStmt) requiredPrivileges() []SQLPrivilege { return nil }

func (stmt *MyNewStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
    return nil
}

func (stmt *MyNewStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
    // ... implement execution ...
    return tx, nil
}
```

### Step 5: Regenerate the parser

```bash
go run golang.org/x/tools/cmd/goyacc -l -o embedded/sql/sql_parser.go embedded/sql/sql_grammar.y
```

One shift/reduce conflict is expected (from CROSS JOIN optional ON clause). Additional conflicts should be investigated.

### Step 6: Add tests

See [Testing Requirements](#testing-requirements) below.

## Adding a pg_catalog or information_schema Resolver

### Step 1: Define columns

In `pkg/pgsql/pgschema/table_resolvers.go`:

```go
var myTableCols = []sql.ColDescriptor{
    {Column: "col1", Type: sql.IntegerType},
    {Column: "col2", Type: sql.VarcharType},
}
```

### Step 2: Implement the resolver

```go
type myTableResolver struct{}

func (r *myTableResolver) Table() string { return "my_table" }

func (r *myTableResolver) Resolve(ctx context.Context, tx *sql.SQLTx, alias string) (sql.RowReader, error) {
    catalog := tx.Catalog()
    tables := catalog.GetTables()

    var rows [][]sql.ValueExp
    for _, t := range tables {
        rows = append(rows, []sql.ValueExp{
            sql.NewInteger(int64(t.ID())),
            sql.NewVarchar(t.Name()),
        })
    }

    return sql.NewValuesRowReader(tx, nil, myTableCols, true, alias, rows)
}
```

### Step 3: Register the resolver

Add to the `tableResolvers` slice at the bottom of the file:

```go
var tableResolvers = []sql.TableResolver{
    // ... existing resolvers ...
    &myTableResolver{},
}
```

### Step 4: Add tests

Add a test in `pkg/pgsql/pgschema/resolvers_test.go` following the existing pattern.

## Adding PG Wire Protocol Emulation

### SHOW statements

In `pkg/pgsql/server/immudb_functions.go`, add to the `showSettings` map:

```go
var showSettings = map[string]string{
    // ... existing settings ...
    "my_setting": "my_value",
}
```

### Custom SQL functions (non-standard)

In `pkg/pgsql/server/stmts_handler.go`, add a regex pattern and handler:

```go
var myFuncRe = regexp.MustCompile(`(?i)select\s+my_func\(\s*\)`)
```

Add to `isEmulableInternally()` and `tryToHandleInternally()`.

## Testing Requirements

Every new feature must have tests at multiple levels:

### 1. Engine Unit Test (required)

In `embedded/sql/engine_test.go`:

```go
func TestMyNewFeature(t *testing.T) {
    engine := setupCommonTest(t)
    
    _, _, err := engine.Exec(context.Background(), nil,
        `CREATE TABLE test (id INTEGER, PRIMARY KEY id)`, nil)
    require.NoError(t, err)
    
    r, err := engine.Query(context.Background(), nil,
        `SELECT MY_NEW_FN('input')`, nil)
    require.NoError(t, err)
    row, err := r.Read(context.Background())
    require.NoError(t, err)
    require.Equal(t, expectedValue, row.ValuesByPosition[0].RawValue())
    r.Close()
}
```

### 2. Edge Case Test (required)

In `embedded/sql/new_features_edge_test.go`:

- NULL inputs
- Empty strings/tables
- Boundary values
- Error conditions
- NOT variants (NOT LIKE, NOT IN, NOT EXISTS)

### 3. PG Wire Protocol Integration Test (required)

In `pkg/pgsql/server/pgsql_compat_integration_test.go`:

```go
func TestPgsqlCompat_MyFeature(t *testing.T) {
    _, port := setupTestServer(t)

    conn, err := pgx.Connect(context.Background(),
        fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
    require.NoError(t, err)
    defer conn.Close(context.Background())

    // Test through actual PG wire protocol
    rows, err := conn.Query(context.Background(), "SELECT MY_NEW_FN('test')")
    require.NoError(t, err)
    require.True(t, rows.Next())
    rows.Close()
}
```

### 4. Stress Test (recommended)

In `embedded/sql/new_features_stress_test.go`:

- Test with larger datasets (50+ rows)
- Verify correctness with computed expected values
- Test performance-sensitive operations (window functions, CTEs, set operations)

### Test Patterns

- Use `setupCommonTest(t)` for engine tests
- Use `setupTestServer(t)` for PG wire tests — returns `(server, port)`
- pgx uses extended query protocol, lib/pq uses simple query protocol — test both when behavior differs
- Always `r.Close()` or `rows.Close()` to avoid snapshot leaks
- Use `require.ErrorIs(t, err, ErrNoMoreRows)` to check for end of results
- Avoid `time.Sleep` in tests — immudb's async index makes timing-dependent tests flaky

## Important Design Constraints

### Immutability

immudb is append-only. All mutations create new versions, never modify or delete physical data:
- `DELETE` marks rows as deleted (soft delete)
- `UPDATE` creates a new version of the row
- Never bypass `tx.doUpsert` or `tx.deleteIndexEntries`

### LIKE Uses Regex Syntax

immudb's `LIKE` and `ILIKE` operators use Go regex syntax, not SQL `%`/`_` wildcards:
- Use `'hello.*'` not `'hello%'`
- Use `'.'` not `'_'`
- This is existing behavior and should not be changed without careful migration planning

### Catalog Persistence

New metadata that must survive restarts needs catalog persistence:
- Key prefix in `stmt.go` (e.g., `catalogSequencePrefix = "CTL.SEQUENCE."`)
- `persist*` function to write to KV store
- Loading logic in `catalog.go` or `engine.go` (`NewTx` path)
- Column metadata format: `{flags:1byte}{maxLen:4bytes}{colName}`

Features that are session-scoped (lost on restart):
- Views (require AST-to-SQL serialization for persistence)
- ALTER COLUMN changes
- DEFAULT values

### PG Wire Protocol

The PG wire handler dispatches by type:
- `sql.DataSource` implementations → `query()` path (returns rows)
- Everything else → `exec()` path (no rows returned)
- `RETURNING` makes DML statements implement `DataSource`

### Grammar Conflicts

The grammar has 1 expected shift/reduce conflict from the CROSS JOIN optional ON clause. Yacc resolves it correctly (shift prefers ON when present). Additional conflicts should be investigated.

## Running Tests

```bash
# SQL engine tests
go test ./embedded/sql/ -count=1 -short -timeout 300s

# PG wire protocol tests
go test ./pkg/pgsql/... -count=1 -short -timeout 120s

# pg_catalog/information_schema resolver tests
go test ./pkg/pgsql/pgschema/ -count=1 -v

# Full test suite (all packages)
go test ./embedded/... ./pkg/... -count=1 -short -timeout 600s

# Specific feature tests
go test ./embedded/sql/ -run "TestWindowFunctions" -count=1 -v

# PG wire integration tests only
go test ./pkg/pgsql/server/ -run "TestPgsqlCompat_" -count=1 -v
```

## File Reference

| File | Purpose |
|------|---------|
| `embedded/sql/sql_grammar.y` | Yacc grammar — all SQL syntax rules |
| `embedded/sql/sql_parser.go` | Generated parser (do not edit manually) |
| `embedded/sql/parser.go` | Lexer, keyword map, token handling |
| `embedded/sql/stmt.go` | AST nodes, statement types, execution logic |
| `embedded/sql/engine.go` | SQL engine, transaction management, sequences |
| `embedded/sql/catalog.go` | Schema metadata (tables, columns, indexes) |
| `embedded/sql/functions.go` | All built-in SQL functions |
| `embedded/sql/row_reader.go` | Base row reader, raw KV scan |
| `embedded/sql/cond_row_reader.go` | WHERE clause filtering |
| `embedded/sql/joint_row_reader.go` | JOIN execution (INNER, LEFT, CROSS) |
| `embedded/sql/full_outer_join_reader.go` | FULL OUTER JOIN (materializing) |
| `embedded/sql/sort_reader.go` | ORDER BY sorting |
| `embedded/sql/grouped_row_reader.go` | GROUP BY aggregation |
| `embedded/sql/distinct_row_reader.go` | DISTINCT filtering |
| `embedded/sql/union_row_reader.go` | UNION execution |
| `embedded/sql/set_op_row_reader.go` | EXCEPT/INTERSECT execution |
| `embedded/sql/window_row_reader.go` | Window function execution |
| `embedded/sql/proj_row_reader.go` | Column projection and aliases |
| `embedded/sql/limit_row_reader.go` | LIMIT clause |
| `embedded/sql/offset_row_reader.go` | OFFSET clause |
| `embedded/sql/values_row_reader.go` | Literal VALUES and virtual tables |
| `pkg/pgsql/server/query_machine.go` | PG wire protocol query dispatch |
| `pkg/pgsql/server/stmts_handler.go` | Statement classification and emulation |
| `pkg/pgsql/server/immudb_functions.go` | immudb verification SQL functions |
| `pkg/pgsql/server/session.go` | PG wire session management |
| `pkg/pgsql/server/bmessages/` | PG wire backend messages (responses) |
| `pkg/pgsql/server/fmessages/` | PG wire frontend messages (requests) |
| `pkg/pgsql/server/pgmeta/pg_type.go` | PG type OID mapping |
| `pkg/pgsql/pgschema/table_resolvers.go` | pg_catalog and information_schema resolvers |
