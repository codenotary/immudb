# Embedding immudb in-process (as a Go library)

immudb can run **entirely inside your Go process** — no server binary, no
container, no network. You import a package, open a local data directory, and
call methods directly. This is ideal for embedding immudb into another
application (a CLI, a desktop app, a plugin, …).

The `immudb` server is just one consumer of these same packages. Everything the
server does with your data, it does through `pkg/database`, which in turn builds
on the `embedded/*` engines.

## Choosing a layer

There are two practical ways to embed, depending on how much you want and how
lean a dependency graph you need.

| Layer | Import | Gives you | gRPC pulled in? |
|---|---|---|---|
| **A. `pkg/database`** (recommended) | `github.com/codenotary/immudb/pkg/database` | High-level `DB`: verifiable Set/Get, SQL, documents, transactions, proofs — the exact code path the server uses | Links the gRPC library (for typed `codes`/`status` errors and the `schema` types), but **never starts a server** or opens a socket |
| **B. `embedded/store` (+ `embedded/sql`)** (leanest) | `github.com/codenotary/immudb/embedded/store`, `.../embedded/sql` | Append-only key-value store and SQL engine; you build your own API and call the stateless `store.Verify*` proof helpers | **No gRPC at all**, no protobuf — just `google/uuid` and Prometheus |

**Recommendation:** start with **Layer A (`pkg/database`)**. It gives you
immudb's core value — tamper-evident, cryptographically verifiable KV + SQL —
with the least code. Drop to **Layer B** only if you want the absolute minimum
dependency footprint and are happy to define your own API surface.

> A third option exists for tests — `pkg/server/servertest.NewBufconnServer`
> runs the full server (auth, sessions, multi-DB) over an in-memory buffer and
> talks to it with the standard `pkg/client`. It links the entire server stack,
> so it is **not** recommended for embedding; it is listed here only for
> completeness.

## Layer A — `pkg/database` (recommended)

```go
log := logger.NewSimpleLoggerWithLevel("embedded", os.Stdout, logger.LogError)
opts := database.DefaultOptions().WithDBRootPath("./data")

// NewDB creates the directory + files; OpenDB opens an existing one.
// Pass a nil sql.MultiDBHandler for a single, self-contained database.
db, err := database.NewDB("mydb", nil, opts, log)
// ...
defer db.Close()

ctx := context.Background()

// verifiable write + read
db.VerifiableSet(ctx, &schema.VerifiableSetRequest{
    SetRequest: &schema.SetRequest{
        KVs: []*schema.KeyValue{{Key: []byte("k"), Value: []byte("v")}},
    },
})
entry, _ := db.VerifiableGet(ctx, &schema.VerifiableGetRequest{
    KeyRequest: &schema.KeyRequest{Key: []byte("k")},
})

// embedded SQL
db.SQLExec(ctx, nil, &schema.SQLExecRequest{Sql: "CREATE TABLE t (id INTEGER, PRIMARY KEY id)"})
reader, _ := db.SQLQuery(ctx, nil, &schema.SQLQueryRequest{Sql: "SELECT id FROM t"})
defer reader.Close()
```

`DB` methods take `schema.*` request structs — the same messages the gRPC API
uses — which makes them verbose but complete. Full runnable program:
[`examples/embedded/main.go`](../examples/embedded/main.go).

Key constructors: `database.NewDB` / `database.OpenDB`
(`pkg/database/database.go`), options via `database.DefaultOptions()`
(`pkg/database/dboptions.go`).

## Layer B — `embedded/store` + `embedded/sql` (leanest)

```go
st, _ := store.Open("./data", store.DefaultOptions())
defer st.Close()

tx, _ := st.NewWriteOnlyTx(ctx)
tx.Set([]byte("k"), nil, []byte("v"))
tx.Commit(ctx)

valRef, _ := st.Get(ctx, []byte("k"))
val, _ := valRef.Resolve()
```

Verifiable, runnable examples live next to the packages as Go example tests:

- Key-value + dual-proof verification: [`embedded/store/example_test.go`](../embedded/store/example_test.go)
- SQL engine: [`embedded/sql/example_test.go`](../embedded/sql/example_test.go)

### SQL requires multi-indexing

`sql.NewEngine` needs the store opened with multi-indexing enabled, otherwise it
returns `ErrMultiIndexingNotEnabled`:

```go
st, _ := store.Open("./data", store.DefaultOptions().WithMultiIndexing(true))
engine, _ := sql.NewEngine(st, sql.DefaultOptions().WithPrefix([]byte("sql")))
```

(Layer A's `NewDB`/`OpenDB` already sets this for you.)

## Important: single-writer, one process per data directory

A data directory is locked by the process that opens it. **Only one process may
open a given directory at a time.** Within that process the store is safe for
concurrent use by multiple goroutines.

This is exactly what you want for true in-process embedding. But if your host
can launch **several processes** that would each try to open the *same*
directory, they will conflict — give each its own directory, or funnel all
access through a single long-lived process.

## Data directory layout

Point the store at any path via `WithDBRootPath` (Layer A) or the first argument
to `store.Open` (Layer B). immudb creates its append-only logs, index, and
hash-tree files under that directory. Back it up by copying the directory while
the process is stopped (or use immudb's export/replication APIs for online
copies).

## Verifying it links no server

To prove an embed does not pull in the gRPC server:

```sh
go list -deps ./yourpkg | grep immudb/pkg/server   # should print nothing
```
