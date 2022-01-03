# KV secondary indexes

On top of the key value store immudb provides secondary indexes to help developers to handle complex queries.

### Sorted sets <a href="#sorted-sets" id="sorted-sets"></a>

The `sorted set` data type provides a simple secondary index that can be created with immudb. This data structure contains a set of references to other key-value entries. Elements of this set are ordered using a floating-point `score` specified for each element upon insertion. Entries having equal score will have the order in which they were inserted into the set.

> Note: The score type is a 64-bit floating point number to support a large number of uses cases. 64-bit floating point gives a lot of flexibility and dynamic range, at the expense of having only 53-bits of integer. When a 64-bit integer is cast to a float value there _could_ be a loss of precision, in which case the order of entries having same float64 score value will be determined by the insertion order.

The KV entry referenced in the set can be bound to a specific transaction id - such entry is called a `bound` reference. A `bound` reference will always get the value for the key at a specific transaction instead of the most recent value, including a case where one set contains multiple values for the same key but for different transactions. That way, sets allow optimal access to historical data using a single immudb read operation.

> Note: If a compound operation is executed with the `ExecAll` call, a bound entry added to the set can reference a key created/updated in the same `ExecAll` call. To make such an operation, set the `BoundRef` value to `true` and the `AtTx` value to `0`.

Inserting entries into sets can be done using the following operations: `ZAdd`, `VerifiedZAdd`, `ZAddAt`, `VerifiedZAddAt`, `ExecAll`. Those operations accept the following parameters:

* `Set`: the name of the collection
* `Score`: entry score used to order items within the set
* `Key`: the key of entry to be added to the set
* `AtTx`: for bound references, a transaction id at which the value will be read, if set to `0` for `ExecAll` operation, current transaction id will be used. Optional
* `BoundRef`: if set to true, this will be a reference bound to a specific transaction. Optional
* `NoWait`: if set to true, don't wait for indexing to be finished after adding this entry

Reading data from the set can be done using the following operations: `ZScan`, `StreamZScan`. Those operations accept the following parameters:

* `Set`: the name of the collection
* `SeekKey`: initial key for the first entry in the iteration. Optional
* `SeekScore`: the min or max score for the first entry in the iteration, depending on Desc value. Optional
* `SeekAtTx`: the tx id for the first entry in the iteration. Optional
* `InclusiveSeek`: the element resulting from the combination of the `SeekKey` `SeekScore` and `SeekAtTx` is returned with the result. Optional
* `Desc`: If set to true, entries will be returned in an descending (reversed) order. Optional
* `SinceTx`: immudb will wait that the transaction provided by SinceTx be processed. Optional
* `NoWait`: when true scan doesn't wait that txSinceTx is processed. Optional
* `MinScore`: minimum score filter. Optional
* `MaxScore`: maximum score filter. Optional
* `Limit`: maximum number of returned items. Optional

> Note: issuing a `ZScan` or `StreamZScan` operation will by default wait for the index to be up-to-date. To avoid waiting for the index (and thus to allow reading the data from some older state), set the `SinceTx` to a very high value exceeding the most recent transaction id (e.g. maximum int value) and set `NoWait` to `true`.
