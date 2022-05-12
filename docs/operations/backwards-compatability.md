# Backwards compatibility

### immudb v1.1 proof compatibility

Starting in immudb v1.2 a new KV metadata feature was inroduced to support new features such as logical deletion and data expiration.
This change required updates to the way a transaction hash is calculated.
The downside of such change is that immudb clients using immudb v1.2+ required an updated method of proof calculation in order to verify newly added data.

In some cases it is very hard or impossible to update the verification code on the client side.
If this is the case, immudb offers a way to disable metadata to maintain compatibility with older clients.

#### Enabling the v1.1 proof compatibility mode

*Note: backwards compatibility mode is currently not available for the `detaultdb` database.*

When creating new database, the mode can be specified with:

```sh
immuadmin database create <db-name> --write-tx-header-version 0
```

Enabling compatibility mode for existing databases can be done by:

```sh
immuadmin database update <db-name> --write-tx-header-version 0
```

*Note: immudb restart is needed to make this change effective.*

In order to re-enable metadata-enhanced proofs,
update database settings with `--write-tx-header-version 1` option.

#### Limitations of v1.1 compatibility mode

Switching to v1.1-compatible proof mode will disable metadata support and thus will make the following operations unavailable:

* For KV operations:
  * Logical deletion
  * Data expiration
  * Non-indexable entries
* For SQL operations:
  * Logical deletion
  * Updates to indexed columns

Make sure to test your application before enabling v1.1 compatibility mode.

#### Working with a database that already contains metadata-enhanced entries

Even though old clients can not validate proofs for metadata-enhanced records,
those can still read the data without proofs as long as those don't use metadata.
Operations such as `Get`, `Scan` or `History` will not cause errors in such workloads.

If proofs are needed, KV entries that were previously added with metadata should
be re-added to the database after enabling immudb v1.1 compatibility mode.