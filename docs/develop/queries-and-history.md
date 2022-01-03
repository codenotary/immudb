# Queries and history

### Queries and history <a href="#queries-and-history" id="queries-and-history"></a>

The fundamental property of immudb is that it's an append-only database. This means that an update is a new insert of the same key with a new value. It's possible to retrieve all the values for a particular key with the history command.

`History` accepts the following parameters:

* `Key`: a key of an item
* `Offset`: the starting index (excluded from the search). Optional
* `Limit`: maximum returned items. Optional
* `Desc`: items are returned in reverse order. Optional
* `SinceTx`: immudb will wait that the transaction specified by SinceTx is processed. Optional

### Counting <a href="#counting" id="counting"></a>

Counting entries is not supported at the moment.

### Scan <a href="#scan" id="scan"></a>

The `scan` command is used to iterate over the collection of elements present in the currently selected database. `Scan` accepts the following parameters:

* `Prefix`: prefix. If not provided all keys will be involved. Optional
* `SeekKey`: initial key for the first entry in the iteration. Optional
* `Desc`: DESC or ASC sorting order. Optional
* `Limit`: maximum returned items. Optional
* `SinceTx`: immudb will wait that the transaction provided by SinceTx be processed. Optional
* `NoWait`: Default false. When true scan doesn't wait for the index to be fully generated and returns the last indexed value. Optional

> To gain speed it's possible to specify `noWait=true`. The control will be returned to the caller immediately, without waiting for the indexing to complete. When `noWait` is used, keep in mind that the returned data may not be yet up to date with the inserted data, as the indexing might not have completed.

### References <a href="#references" id="references"></a>

`SetReference` is like a "tag" operation. It appends a reference on a key/value element. As a consequence, when we retrieve that reference with a `Get` or `VerifiedGet` the value retrieved will be the original value associated with the original key. Its `VerifiedReference` counterpart is the same except that it also produces the inclusion and consistency proofs.

#### SetReference and VerifiedSetReference <a href="#setreference-and-verifiedsetreference" id="setreference-and-verifiedsetreference"></a>

#### GetReference and VerifiedGetReference <a href="#getreference-and-verifiedgetreference" id="getreference-and-verifiedgetreference"></a>

When reference is resolved with get or verifiedGet in case of multiples equals references the last reference is returned.

#### Resolving reference with transaction id <a href="#resolving-reference-with-transaction-id" id="resolving-reference-with-transaction-id"></a>

It's possible to bind a reference to a key on a specific transaction using `SetReferenceAt` and `VerifiedSetReferenceAt`
