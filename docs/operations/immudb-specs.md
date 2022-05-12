# immudb specs

immudb is an append-only, tamperproof database with key/value and SQL (Structured Query Language) application programming interfaces.

The immudb core persistence layer consists of a cryptographically-immutable log. Transactions are sequentially stored and uniquely identified by unsigned 64-bit integer values, thus setting a theoretical limit of 18446744073709551615 transactions (1^64 - 1).

In order to provide manageable limits, immudb is designed to set an upper bound to the number of key-value pairs included in a single transaction. The default value being 1024, so using default settings, the theoretical number of key-value pairs that can be stored in immudb is: 1024 \* (1^64 - 1).

We designed immudb to require stable resources but not imposing strong limitations as most of the settings can be adjusted based on the use-case requirements.

While key-value pairs consist of arbitrary byte arrays, a maximum length may be set at database creation time. Both parameters can be increased as needed, considering:

* long keys may degrade performance (when querying data through the index)
* longer values require more memory at runtime

Note: The cryptographic linking does not impose a practical restriction.

immudb relies on a file abstraction layer and does not directly interact with the underlying file-system, if any. The default storage layer implementation writes data into fixed-size files, with the default size being 512MB. The current theoretical maximum number of files is in the 100 millions range.

### Summary <a href="#summary" id="summary"></a>

Theoretical limits may be determined by a couple of elements:

* max number transactions: 2^64-1 (uint64)
* max number of files: 2^63-1 (max file size 2^56-1)
* max value length: 32 MB (max size: 2^56-1 bytes)
* max key length: 1024 Bytes (max length: 2^31-1 bytes)

### Supported platforms <a href="#supported-platforms" id="supported-platforms"></a>

immudb server runs on most operating systems including BSD, Linux, OS X, Solaris, Windows and IBM z/OS

### Key/Value API <a href="#key-value-api" id="key-value-api"></a>

immudb includes a basic yet complete set of operations you would expect from a key-value store:

```java
Set (SetRequest) returns (TxMetadata)
Get (KeyRequest) returns (Entry)
SetReference (ReferenceRequest) returns (TxMetadata)

Scan(ScanRequest) returns (Entries)
History(HistoryRequest) returns (Entries)

// grouping and sorting keys based on assgined scores
ZAdd (ZAddRequest) returns (TxMetadata)
ZScan (ZScanRequest) returns (ZEntries)
```

Additionally, immudb exposes direct access to transactions based on their unique identifier:

```java
TxById(TxRequest) returns (Tx)
TxScan(TxScanRequest) returns (TxList)
```

But more importantly, immudb is able to generate cryptographic-proofs whenever is needed:

```java
VerifiableSet (VerifiableSetRequest) returns (VerifiableTx)
VerifiableGet (VerifiableGetRequest) returns (VerifiableEntry)

VerifiableTxById(VerifiableTxRequest) returns (VerifiableTx)

VerifiableSetReference (VerifiableReferenceRequest) returns (VerifiableTx)

VerifiableZAdd (VerifiableZAddRequest) returns (VerifiableTx)
```

Check the official SDKs documentation in the [Develop with immudb](../develop/connection-and-authentication.md) section.

### SQL API <a href="#sql-api" id="sql-api"></a>

Based on the key-value storage layer, immudb includes a simplified SQL engine which is able to interprete most common features you would expect from a relational database but with the possibility to verify that rows have not been tampered with.

```java
SQLExec(SQLExecRequest) returns (SQLExecResult)
SQLQuery(SQLQueryRequest) returns (SQLQueryResult)
​
VerifiableSQLGet (VerifiableSQLGetRequest) returns (VerifiableSQLEntry)
```

Check the [SQL reference](../reference/sql-reference.md) for a comprehensive description of SQL statements.

### S3 Storage Backend <a href="#s3-storage-backend" id="s3-storage-backend"></a>

immudb can store its data on the Amazon S3 service (or a compatible alternative). The following example shows how to run immudb with S3 storage enabled:


```bash
export IMMUDB_S3_STORAGE=true
export IMMUDB_S3_ACCESS_KEY_ID=<S3 ACCESS KEY ID>
export IMMUDB_S3_SECRET_KEY=<SECRET KEY>
export IMMUDB_S3_BUCKET_NAME=<BUCKET NAME>
export IMMUDB_S3_LOCATION=<AWS S3 REGION>
export IMMUDB_S3_PATH_PREFIX=testing-001
export IMMUDB_S3_ENDPOINT="https://${IMMUDB_S3_BUCKET_NAME}.s3.amazonaws.com"

./immudb
```
