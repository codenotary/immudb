# PGSQL protocol compatibility

immudb can talk the [pgsql wire protocol (opens new window)](https://www.postgresql.org/docs/9.3/protocol.html) which makes it compatible with a widely available set of clients and drivers.

> Note: immudb supports the pgsql wire protocol. It is _not_ compatible with the SQL dialect. Check the immudb SQL reference to see what queries and operations are supported.
>
> Some pgsql clients and browser application execute incompatible statements in the background or directly query the pgsql catalog. Those may not work with immudb.

immudb needs to be started with the `pgsql-server` option enabled (`IMMUDB_PGSQL_SERVER=true`).

SSL is supported, if you configured immudb with a certificate.

To connect to the database:

Execute statements:

Query and iterate over results:
