# Transactions

Immudb supports transactions both on key-value and SQL level, but interactive transactions are supported only on SQL with the exception of `execAll` method, that provides some additional properties.

### SQL interactive transactions <a href="#sql-interactive-transactions" id="sql-interactive-transactions"></a>

Interactive transactions are a way to execute multiple SQL statements in a single transaction. This makes possible to delegate application logic to SQL statements - a very common use case is for example checking if the balance > 0 before making a purchase. In order to create a transaction, you must call the `NewTx()` method on the client instance. The resulting object is a transaction object that can be used to execute multiple SQL statements, queries, commit or rollback. Following there are methods exposed by the transaction object:

It's possible to rollback a transaction by calling the `Rollback()` method. In this case, the transaction object is no longer valid and should not be used anymore. To commit a transaction, you must call the `Commit()` method.

> **Note**: At the moment immudb support only 1 read-write transaction at a time, so it's up the application to ensure that only one read-write transaction is open at a time, or to handle read conflict error. In such case the error code returned by sdk will be `25P02` **CodInFailedSqlTransaction**.

### Key Value transactions <a href="#key-value-transactions" id="key-value-transactions"></a>

`GetAll`, `SetAll` and `ExecAll` are the foundation of transactions at key value level in immudb. They allow the execution of a group of commands in a single step, with two important guarantees:

* All the commands in a transaction are serialized and executed sequentially. No request issued by another client can ever interrupt the execution of a transaction. This guarantees that the commands are executed as a single isolated operation.
* Either all of the commands are processed, or none are, so the transaction is also atomic.

#### GetAll <a href="#getall" id="getall"></a>

#### SetAll <a href="#setall" id="setall"></a>

A more versatile atomic multi set operation

SetBatch and GetBatch example

#### ExecAll <a href="#execall" id="execall"></a>

`ExecAll` permits many insertions at once. The difference is that is possible to specify a list of a mix of key value set, reference and zAdd insertions. The argument of a ExecAll is an array of the following types:

* `Op_Kv`: ordinary key value item
* `Op_ZAdd`: [ZAdd](broken-reference) option element
* `Op_Ref`: [Reference](broken-reference) option element

It's possible to persist and reference items that are already persisted on disk. In that case is mandatory to provide the index of the referenced item. This has to be done for:

* `Op_ZAdd`
* `Op_Ref` If `zAdd` or `reference` is not yet persisted on disk it's possible to add it as a regular key value and the reference is done onFly. In that case if `BoundRef` is true the reference is bounded to the current transaction values.

#### Tx Scan <a href="#tx-scan" id="tx-scan"></a>

`TxScan` permits iterating over transactions.

The argument of a `TxScan` is an array of the following types:

* `InitialTx`: initial transaction id
* `Limit`: number of transactions returned
* `Desc`: order of returned transacations
