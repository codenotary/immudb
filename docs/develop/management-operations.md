# Management operations

### User management <a href="#user-management" id="user-management"></a>

User management is exposed with following methods:

* CreateUser
* ChangePermission
* ChangePassword

Password must have between 8 and 32 letters, digits and special characters of which at least 1 uppercase letter, 1 digit and 1 special character.

Admin permissions are:

* PermissionSysAdmin = 255
* PermissionAdmin = 254

Non-admin permissions are:

* PermissionNone = 0
* PermissionR = 1
* PermissionRW = 2

### Multiple databases <a href="#multiple-databases" id="multiple-databases"></a>

Starting with version 0.7.0 of immudb, we introduced multi-database support. By default, the first database is either called `defaultdb` or based on the environment variable `IMMUDB_DBNAME`. Handling users and databases requires the appropriate privileges. Users with `PermissionAdmin` can control everything. Non-admin users have restricted permissions and can read or write only their databases, assuming sufficient privileges.

> Each database has default MaxValueLen and MaxKeyLen values. These are fixed respectively to 1MB and 1KB. These values at the moment are not exposed to client SDK and can be modified using internal store options.

### Index Cleaning <a href="#index-cleaning" id="index-cleaning"></a>

It's important to keep disk usage under control. `CleanIndex` it's a temporary solution to launch an internal clean routine that could free disk space.

> immudb uses a btree to index key-value entries. While the key is the same submitted by the client, the value stored in the btree is an offset to the file where the actual value as stored, its size and hash value. The btree is keep in memory as new data is inserted, getting a key or even the historical values of a key can directly be made by using a mutex lock on the btree but scanning by prefix requires the tree to be stored into disk, this is referred as a snapshot. The persistence is implemented in append-only mode, thus whenever a snapshot is created (btree flushed to disk), updated and new nodes are appended to the file, while new or updated nodes may be linked to unmodified nodes (already written into disk) and those unmodified nodes are not rewritten. The snapshot creation does not necessarily take place upon each scan by prefix, it's possible to reuse an already created one, client can provide his requirements on how new the snapshot should be by providing a transaction ID which at least must be indexed (sinceTx). After some time, several snapshots may be created (specified by flushAfter properties of the btree and the scan requests), the file backing the btree will hold several old snapshots. Thus the clean index process will dump to a different location only the latest snapshot but in this case also writing the unmodified nodes. Once that dump is done, the index folder is replaced by the new one. While the clean process is made, no data is indexed and there will be an extra disk space requirement due to the new dump. Once completed, a considerable disk space will be reduced by removing the previously indexed data (older snapshots). The btree and clean up process is something specific to indexing. And will not lock transaction processing as indexing is asynchronously generated.

### HealthCheck <a href="#healthcheck" id="healthcheck"></a>

HealthCheck return an error if `immudb` status is not ok.
