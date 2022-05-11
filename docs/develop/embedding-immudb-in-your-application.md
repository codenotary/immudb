# Embedding the SQL engine in your application

Using the Go client SDK means you are connecting to a immudb database server. There are cases where you don't want a separate server but embed immudb directly in the same application process, as a library.

immudb provides you a immutable embedded SQL engine which keeps all history, is tamper-proof and can travel in time.

immudb already provides an embeddable key-value store in the [embedded (opens new window)](https://github.com/codenotary/immudb/tree/master/embedded) package.

The SQL engine is mounted on top of the embedded key value store, and requires two stores to be initialized, one for the data, and one for the catalog (schema).

Make sure to import:

Create stores for the data and catalog:

And now you can create the SQL engine, passing both stores and a key prefix:

Create and use a database:

The engine has an API to execute statements and queries. To execute an statement:

Queries can be executed using `QueryStmt` and you can pass a map of parameters to substitute, and whether the engine should wait for indexing:

To iterate over a result set `r`, just fetch rows until there are no more entries. Every row has a `Values` member you can index to access the column:

And that is all you need. If you need to change options like where things get stored by default, you can do that in the underlying store objects that the SQL engine is using.
