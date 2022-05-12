# Reading and inserting data

The format for writing and reading data is the same both in `Set` and `VerifiedSet`, just as it is for reading data both in both `Get` and `VerifiedGet`.

The only difference is that `VerifiedSet` returns the proofs needed to mathematically verify that the data was not tampered with. Note that generating that proof has a slight performance impact, so primitives are allowed without the proof. It is still possible get the proofs for a specific item at any time, so the decision about when or how frequently to do checks (with the `Verified` version of a method) is completely up to the developer. It's also possible to use dedicated [auditors](./running-an-auditor-with-immuclient.md) to ensure database consistency, but the pattern in which every client is also an auditor is the more interesting one.

[//]: # "review for codeblocks"

#### Get and set <a href="#get-and-set" id="get-and-set"></a>

:::: tabs

::: tab Go

```go
tx, err = client.Set(ctx, []byte(`hello`), []byte(`immutable world`))
if  err != nil {
    log.Fatal(err)
}

fmt.Printf("Successfully committed tx %d\n", tx.Id)

entry, err := client.Get(ctx, []byte(`hello`))
if  err != nil {
    log.Fatal(err)
}

fmt.Printf("Successfully retrieved entry: %v\n", entry)
```

:::

::: tab Java

```java
String key = "key1";
byte[] value = new byte[]{1, 2, 3};

try {
    immuClient.set(key, value);
} catch (CorruptedDataException e) {
    // ...
}

try {
    value = immuClient.get(key);
} catch (Exception e) {
    // ...
}
```

Note that `value` is a primitive byte array. You can set the value of a String using:<br/>
`"some string".getBytes(StandardCharsets.UTF_8)`

Also, `set` method is overloaded to allow receiving the `key` parameter as a `byte[]` data type.

:::

::: tab Python
```python
from immudb import ImmudbClient
import json

URL = "localhost:3322"  # immudb running on your machine
LOGIN = "immudb"        # Default username
PASSWORD = "immudb"     # Default password
DB = b"defaultdb"       # Default database name (must be in bytes)

def encode(what: str):
    return what.encode("utf-8")

def decode(what: bytes):
    return what.decode("utf-8")

def main():
    client = ImmudbClient(URL)
    client.login(LOGIN, PASSWORD, database = DB)
    
    # You have to operate on bytes
    setResult = client.set(b'x', b'y')
    print(setResult)            # immudb.datatypes.SetResponse
    print(setResult.id)         # id of transaction
    print(setResult.verified)   # in this case verified = False
								# see Tamperproof reading and writing

    # Also you get response in bytes
    retrieved = client.get(b'x')
    print(retrieved)        # immudb.datatypes.GetResponse
    print(retrieved.key)    # Value is b'x'
    print(retrieved.value)  # Value is b'y'
    print(retrieved.tx)     # Transaction number

    print(type(retrieved.key))      # <class 'bytes'>
    print(type(retrieved.value))    # <class 'bytes'>

    # Operating with strings
    encodedHello = encode("Hello")
    encodedImmutable = encode("Immutable")
    client.set(encodedHello, encodedImmutable)
    retrieved = client.get(encodedHello)

    print(decode(retrieved.value) == "Immutable")   # Value is True

    notExisting = client.get(b'asdasd')
    print(notExisting)                              # Value is None


    # JSON example
    toSet = {"hello": "immutable"}
    encodedToSet = encode(json.dumps(toSet))
    client.set(encodedHello, encodedToSet)

    retrieved = json.loads(decode(client.get(encodedHello).value))
    print(retrieved)    # Value is {"hello": "immutable"}

    # setAll example - sets all keys to value from dictionary
    toSet = {
        b'1': b'test1',
        b'2': b'test2',
        b'3': b'test3'
    }

    client.setAll(toSet)
    retrieved = client.getAll(list(toSet.keys()))
    print(retrieved) 
    # Value is {b'1': b'test1', b'2': b'test2', b'3': b'test3'}


if __name__ == "__main__":
    main()
```

:::

::: tab Node.js

```ts
import ImmudbClient from 'immudb-node'
import Parameters from 'immudb-node/types/parameters'

const IMMUDB_HOST = '127.0.0.1'
const IMMUDB_PORT = '3322'
const IMMUDB_USER = 'immudb'
const IMMUDB_PWD = 'immudb'

const cl = new ImmudbClient({ host: IMMUDB_HOST, port: IMMUDB_PORT });

(async () => {
    await cl.login({ user: IMMUDB_USER, password: IMMUDB_PWD })

    const setReq: Parameters.Set = { key: 'hello', value: 'world' }
    const setRes = await cl.set(setReq)
    console.log('success: set', setRes)

    const getReq: Parameters.Get = { key: 'hello' }
    const getRes = await cl.get(getReq)
    console.log('success: get', getRes)
})()
```

:::

::: tab .Net
This feature is not yet supported or not documented.
Do you want to make a feature request or help out? Open an issue on [.Net sdk github project](https://github.com/codenotary/immudb4dotnet/issues/new)
:::

::: tab Others
If you're using another development language, please refer to the [immugw](/master/immugw/) option.
:::

::::

#### Get at and since a transaction <a href="#get-at-and-since-a-transaction" id="get-at-and-since-a-transaction"></a>

You can retrieve a key on a specific transaction with `VerifiedGetAt` and since a specific transaction with `VerifiedGetSince`.

:::: tabs

::: tab Go

```go
ventry, err = client.VerifiedGetAt(ctx, []byte(`key`), meta.Id)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Successfully retrieved entry at %v with value %s\n", ventry.Tx, ventry.Value)

ventry, err = client.VerifiedGetSince(ctx, []byte(`key`), 4)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Successfully retrieved entry at %v with value %s\n", ventry.Tx, ventry.Value)
```

:::

::: tab Java

```java
byte[] key = "key1".getBytes(StandardCharsets.UTF_8);
byte[] val = new byte[]{1, 2, 3, 4, 5};
TxMetadata txMd = null;

try {
    txMd = immuClient.set(key, val);
} catch (CorruptedDataException e) {
    // ...
}

// The standard (traditional) get options:

KV kv = immuClient.getAt(key, txMd.id);

kv = immuClient.getSince(key, txMd.id);

// The verified get flavours:

Entry vEntry = null;
try {
    vEntry = immuClient.verifiedGetAt(key, vEntry.txId);
} catch (VerificationException e) {
    // ...
}

try {
    vEntry = immuClient.verifiedGetSince(key, vEntry.txId);
} catch (VerificationException e) {
    // ...
}
```

:::

::: tab Python

```python
from grpc import RpcError
from immudb import ImmudbClient

URL = "localhost:3322"  # immudb running on your machine
LOGIN = "immudb"        # Default username
PASSWORD = "immudb"     # Default password
DB = b"defaultdb"       # Default database name (must be in bytes)

def main():
    client = ImmudbClient(URL)
    client.login(LOGIN, PASSWORD, database = DB)
    first = client.set(b'justfirsttransaction', b'justfirsttransaction')

    key = b'123123'

    first = client.set(key, b'111')
    firstTransaction = first.id

    second = client.set(key, b'222')
    secondTransaction = second.id

    third = client.set(key, b'333')
    thirdTransaction = third.id

    print(client.verifiedGetSince(key, firstTransaction))   # b"111"
    print(client.verifiedGetSince(key, firstTransaction + 1))   # b"222"

    try:
        # This key wasn't set on this transaction
        print(client.verifiedGetAt(key, firstTransaction - 1))
    except RpcError as exception:
        print(exception.debug_error_string())
        print(exception.details())

    verifiedFirst = client.verifiedGetAt(key, firstTransaction) 
                                    # immudb.datatypes.SafeGetResponse
    print(verifiedFirst.id)         # id of transaction
    print(verifiedFirst.key)        # Key that was modified
    print(verifiedFirst.value)      # Value after this transaction
    print(verifiedFirst.refkey)     # Reference key
									# (Queries And History -> setReference)
    print(verifiedFirst.verified)   # Response is verified or not
    print(verifiedFirst.timestamp)  # Time of this transaction

    print(client.verifiedGetAt(key, secondTransaction))
    print(client.verifiedGetAt(key, thirdTransaction))

    try:
        # Transaction doesn't exists yet
        print(client.verifiedGetAt(key, thirdTransaction + 1))
    except RpcError as exception:
        print(exception.debug_error_string())
        print(exception.details())

if __name__ == "__main__":
    main()

```
:::

::: tab Node.js

```ts
import ImmudbClient from 'immudb-node'
import Parameters from 'immudb-node/types/parameters'

const IMMUDB_HOST = '127.0.0.1'
const IMMUDB_PORT = '3322'
const IMMUDB_USER = 'immudb'
const IMMUDB_PWD = 'immudb'

const cl = new ImmudbClient({ host: IMMUDB_HOST, port: IMMUDB_PORT });

(async () => {
    await cl.login({ user: IMMUDB_USER, password: IMMUDB_PWD })
    const { id } = await cl.set({ key: 'key', value: 'value' })

    const verifiedGetAtReq: Parameters.VerifiedGetAt = {
        key: 'key',
        attx: id
    }
    const verifiedGetAtRes = await cl.verifiedGetAt(verifiedGetAtReq)
    console.log('success: verifiedGetAt', verifiedGetAtRes)

    for (let i = 0; i < 4; i++) {
        await cl.set({ key: 'key', value: `value-${i}` })
    }

    const verifiedGetSinceReq: Parameters.VerifiedGetSince = {
        key: 'key',
        sincetx: 2
    }
    const verifiedGetSinceRes = await cl.verifiedGetSince(verifiedGetSinceReq)
    console.log('success: verifiedGetSince', verifiedGetSinceRes)
})()
```

:::

::: tab .Net
This feature is not yet supported or not documented.
Do you want to make a feature request or help out? Open an issue on [.Net sdk github project](https://github.com/codenotary/immudb4dotnet/issues/new)
:::

::: tab Others
If you're using another development language, please refer to the [immugw](/master/immugw/) option.
:::

::::

### Retrieving transactions by ID <a href="#retrieving-transactions-by-ID" id="retrieving-transactions-by-ID"></a>


It's possible to retrieve all the keys inside of a specific transaction.

:::: tabs

::: tab Go

```go
setRequest := &schema.SetRequest{KVs: []*schema.KeyValue{
    {Key: []byte("key1"), Value: []byte("val1")},
    {Key: []byte("key2"), Value: []byte("val2")},
}}

meta, err := client.SetAll(ctx, setRequest)
if err != nil {
    log.Fatal(err)
}

tx , err := client.TxByID(ctx, meta.Id)
if err != nil {
    log.Fatal(err)
}

for _, entry := range tx.Entries {
    item, err := client.VerifiedGetAt(ctx, entry.Key, meta.Id)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("retrieved key %s and val %s\n", item.Key, item.Value)
}
```

:::

::: tab Java

```java
TxMetadata txMd = null;
try {
    txMd = immuClient.verifiedSet(key, val);
} catch (VerificationException e) {
    // ...
}
try {
    Tx tx = immuClient.txById(txMd.id);
} catch (MaxWidthExceededException | NoSuchAlgorithmException e) {
    // ...
}
```

:::

::: tab Python
```python
from immudb import ImmudbClient

URL = "localhost:3322"  # immudb running on your machine
LOGIN = "immudb"        # Default username
PASSWORD = "immudb"     # Default password
DB = b"defaultdb"       # Default database name (must be in bytes)

def main():
    client = ImmudbClient(URL)
    client.login(LOGIN, PASSWORD, database = DB)

    keyFirst = b'333'
    keySecond = b'555'

    first = client.set(keyFirst, b'111')
    firstTransaction = first.id

    second = client.set(keySecond, b'222')
    secondTransaction = second.id

    toSet = {
        b'1': b'test1',
        b'2': b'test2',
        b'3': b'test3'
    }

    third = client.setAll(toSet)
    thirdTransaction = third.id

    keysAtFirst = client.txById(firstTransaction)
    keysAtSecond = client.txById(secondTransaction)
    keysAtThird = client.txById(thirdTransaction)

    print(keysAtFirst)  # [b'333']
    print(keysAtSecond) # [b'555']
    print(keysAtThird)  # [b'1', b'2', b'3']


if __name__ == "__main__":
    main()
```
:::

::: tab Node.js

```ts
import ImmudbClient from 'immudb-node'
import Parameters from 'immudb-node/types/parameters'

const IMMUDB_HOST = '127.0.0.1'
const IMMUDB_PORT = '3322'
const IMMUDB_USER = 'immudb'
const IMMUDB_PWD = 'immudb'

const cl = new ImmudbClient({ host: IMMUDB_HOST, port: IMMUDB_PORT });

(async () => {
    await cl.login({ user: IMMUDB_USER, password: IMMUDB_PWD })
    const { id } = await cl.set({ key: 'key', value: 'value' })

    const txByIdReq: Parameters.TxById = { tx: id }
    const txByIdRes = await cl.txById(txByIdReq)
    console.log('success: txById', txByIdRes)
})()
```

:::

::: tab .Net
This feature is not yet supported or not documented.
Do you want to make a feature request or help out? Open an issue on [.Net sdk github project](https://github.com/codenotary/immudb4dotnet/issues/new)
:::

::: tab Others
If you're using another development language, please refer to the [immugw](/master/immugw/) option.
:::

::::

### Retrieving verified transactions by ID <a href="#retrieving-verified-transactions-by-id" id="retrieving-verified-transactions-by-id"></a>

It's possible to retrieve all of the keys inside a specific verified transaction.

:::: tabs

::: tab Go

```go
setRequest := &schema.SetRequest{KVs: []*schema.KeyValue{
    {Key: []byte("key1"), Value: []byte("val1")},
    {Key: []byte("key2"), Value: []byte("val2")},
}}

meta, err := client.SetAll(ctx, setRequest)
if err != nil {
    log.Fatal(err)
}

tx , err := client.VerifiedTxByID(ctx, meta.Id)
if err != nil {
    log.Fatal(err)
}

for _, entry := range tx.Entries {
    item, err := client.VerifiedGetAt(ctx, entry.Key, meta.Id)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("retrieved key %s and val %s\n", item.Key, item.Value)
}
```

:::

::: tab Java

```java
TxMetadata txMd = null;
try {
    txMd = immuClient.verifiedSet(key, val);
} catch (VerificationException e) {
    // ...
}
try {
    Tx tx = immuClient.verifiedTxById(txMd.id);
} catch (VerificationException e) {
    // ...
}
```

:::

::: tab Python
```python
from immudb import ImmudbClient

URL = "localhost:3322"  # immudb running on your machine
LOGIN = "immudb"        # Default username
PASSWORD = "immudb"     # Default password
DB = b"defaultdb"       # Default database name (must be in bytes)

def main():
    client = ImmudbClient(URL)
    client.login(LOGIN, PASSWORD, database = DB)
    
    keyFirst = b'333'
    keySecond = b'555'

    first = client.set(keyFirst, b'111')
    firstTransaction = first.id

    second = client.set(keySecond, b'222')
    secondTransaction = second.id

    toSet = {
        b'1': b'test1',
        b'2': b'test2',
        b'3': b'test3'
    }

    third = client.setAll(toSet)
    thirdTransaction = third.id

    keysAtFirst = client.verifiedTxById(firstTransaction)
    keysAtSecond = client.verifiedTxById(secondTransaction)
    keysAtThird = client.verifiedTxById(thirdTransaction)

    print(keysAtFirst)  # [b'333']
    print(keysAtSecond) # [b'555']
    print(keysAtThird)  # [b'1', b'2', b'3']


if __name__ == "__main__":
    main()
```
:::

::: tab Node.js

```ts
import ImmudbClient from 'immudb-node'
import Parameters from 'immudb-node/types/parameters'

const IMMUDB_HOST = '127.0.0.1'
const IMMUDB_PORT = '3322'
const IMMUDB_USER = 'immudb'
const IMMUDB_PWD = 'immudb'

const cl = new ImmudbClient({ host: IMMUDB_HOST, port: IMMUDB_PORT });

(async () => {
    await cl.login({ user: IMMUDB_USER, password: IMMUDB_PWD })
    const { id } = await cl.set({ key: 'key', value: 'value' })
    
    const verifiedTxByIdReq: Parameters.VerifiedTxById = { tx: id }
    const verifiedTxByIdRes = await cl.verifiedTxByID(verifiedTxByIdReq)
    console.log('success: verifiedTxByID', verifiedTxByIdRes)
})()
```

:::

::: tab .Net
This feature is not yet supported or not documented.
Do you want to make a feature request or help out? Open an issue on [.Net sdk github project](https://github.com/codenotary/immudb4dotnet/issues/new)
:::

::: tab Others
If you're using another development language, please refer to the [immugw](/master/immugw/) option.
:::

::::

### Retrieving transactions by ID with filters

Transaction entries are generated by writing key-value pairs, referencing keys, associating scores to key-value pairs (with `ZAdd` operation), and by mapping SQL data model into key-value model.

Using the `TxByIDWithSpec` operation it's possible to retrieve entries of certain types, either retrieving the digest of the value assigned to the key (`EntryTypeAction_ONLY_DIGEST`), the raw value (`EntryTypeAction_RAW_VALUE`) or the structured value (`EntryTypeAction_RESOLVE`).

:::: tabs

::: tab Go

```go
// fetch kv and sorted-set entries as structured values while skipping sql-related entries
tx , err := client.TxByIDWithSpec(ctx, &schema.TxRequest{
                Tx: hdr.Id,
                EntriesSpec: &schema.EntriesSpec{
                    KvEntriesSpec: &schema.EntryTypeSpec{
                        Action: schema.EntryTypeAction_RESOLVE,
                    },
                    ZEntriesSpec: &schema.EntryTypeSpec{
                        Action: schema.EntryTypeAction_RESOLVE,
                    },
                    // explicit exclusion is optional
                    SqlEntriesSpec: &schema.EntryTypeSpec{
                        // resolution of sql entries is not supported
                        Action: schema.EntryTypeAction_EXCLUDE,
                    },
                },
            }))
if err != nil {
    log.Fatal(err)
}

for _, entry := range tx.KvEntries {
   fmt.Printf("retrieved key %s and val %s\n", entry.Key, entry.Value)
}

for _, entry := range tx.ZEntries {
   fmt.Printf("retrieved set %s key %s and score %v\n", entry.Set, entry.Key, entry.Score)
}

// scan over unresolved entries
// either EntryTypeAction_ONLY_DIGEST or EntryTypeAction_RAW_VALUE options
for _, entry := range tx.Entries {
   fmt.Printf("retrieved key %s and digest %v\n", entry.Key, entry.HValue)
}
```

:::

::: tab Java
This feature is not yet supported or not documented.
Do you want to make a feature request or help out? Open an issue on [Java sdk github project](https://github.com/codenotary/immudb4j/issues/new)
:::

::: tab Python
This feature is not yet supported or not documented.
Do you want to make a feature request or help out? Open an issue on [Python sdk github project](https://github.com/codenotary/immudb-py/issues/new)
:::

::: tab Node.js
This feature is not yet supported or not documented.
Do you want to make a feature request or help out? Open an issue on [Node.js sdk github project](https://github.com/codenotary/immudb-node/issues/new)
:::

::: tab .Net
This feature is not yet supported or not documented.
Do you want to make a feature request or help out? Open an issue on [.Net sdk github project](https://github.com/codenotary/immudb4dotnet/issues/new)
:::

::: tab Others
If you're using another development language, please refer to the [immugw](/master/immugw/) option.
:::

::::

### Conditional writes

Immudb can check additional preconditions before the write operation is made.
Precondition is checked atomically with the write operation.
It can be then used to ensure consistent state of data inside the database.

Following preconditions are supported:

* MustExist - precondition that checks if given key exists in the database,
  this precondition takes into consideration logical deletion and data expiration,
  if the entry was logically deleted or has expired, MustExist precondition for
  such entry will fail
* MustNotExist - precondition that checks if given key does not exist in the database,
  this precondition also takes into consideration logical deletion and data expiration,
  if the entry was logically deleted or has expired, the MustNotExist precondition for
  such an entry will succeed
* NotModifiedAfterTX - precondition that checks if a given key was not modified after a given transaction id,
  local deletion and setting an entry with expiration data is also considered modification of the
  entry

In many cases, keys used for constraints will be the same as keys for written entries.
A good example here is a situation when a value is set only if that key does not exist.
This is not strictly required - keys used in constraints do not have to be the same
or even overlap with keys for modified entries. An example would be if only one of
two keys should exist in the database. In such case, the first key will be modified
and the second key will be used for MustNotExist constraint.

A write operation using precondition can not be done in an asynchronous way.
Preconditions are checked twice when processing such requests - first check is done
against the current state of internal index, the second check is done just before
persisting the write and requires up-to-date index.

Preconditions are available on `SetAll`, `Reference` and `ExecAll` operations.

:::: tabs

::: tab Go

In go sdk, the `schema` package contains convenient wrappers for creating constraint objects,
such as `schema.PreconditionKeyMustNotExist`.

#### Example - ensure modification is done atomically when there are concurrent writers

```go
entry, err := c.Get(ctx, []byte("key"))
if err != nil {
    log.Fatal(err)
}

newValue := modifyValue(entry.Value)

_, err = c.SetAll(ctx, &schema.SetRequest{
    KVs: []*schema.KeyValue{{
        Key:   []byte("key"),
        Value: newValue,
    }},
    Preconditions: []*schema.Precondition{
        schema.PreconditionKeyNotModifiedAfterTX(
            []byte("key"),
            entry.Tx,
        ),
    },
})
if err != nil {
    log.Fatal(err)
}
```

#### Example - allow setting the key only once

```go
tx, err := client.SetAll(ctx, &schema.SetRequest{
    KVs: []*schema.KeyValue{
        {Key: []byte("key"), Value: []byte("val")},
    },
    Preconditions: []*schema.Precondition{
        schema.PreconditionKeyMustNotExist([]byte("key")),
    },
})
if err != nil {
    log.Fatal(err)
}
```

#### Example - set only one key in a group of keys

```go
tx, err := client.SetAll(ctx, &schema.SetRequest{
    KVs: []*schema.KeyValue{
        {Key: []byte("key1"), Value: []byte("val1")},
    },
    Preconditions: []*schema.Precondition{
        schema.PreconditionKeyMustNotExist([]byte("key2")),
        schema.PreconditionKeyMustNotExist([]byte("key3")),
        schema.PreconditionKeyMustNotExist([]byte("key4")),
    },
})
if err != nil {
    log.Fatal(err)
}
```

#### Example - check if returned error indicates precondition failure

```go
import (
    immuerrors "github.com/codenotary/immudb/pkg/client/errors"
)

...

tx, err := client.SetAll(ctx, &schema.SetRequest{
    KVs: []*schema.KeyValue{
        {Key: []byte("key"), Value: []byte("val")},
    },
    Preconditions: []*schema.Precondition{
        schema.PreconditionKeyMustExist([]byte("key")),
    },
})
immuErr := immuerrors.FromError(err)
if immuErr != nil && immuErr.Code() == immuerrors.CodIntegrityConstraintViolation {
    log.Println("Constraint validation failed")
}
```

:::

::: tab Java
This feature is not yet supported or not documented.
Do you want to make a feature request or help out? Open an issue on [Java sdk github project](https://github.com/codenotary/immudb4j)
:::

::: tab Python
This feature is not yet supported or not documented.
Do you want to make a feature request or help out? Open an issue on [Python sdk github project](https://github.com/codenotary/immudb-py/issues/new)
:::

::: tab Node.js
This feature is not yet supported or not documented.
Do you want to make a feature request or help out? Open an issue on [Node.js sdk github project](https://github.com/codenotary/immudb-node)
:::

::: tab .Net
This feature is not yet supported or not documented.
Do you want to make a feature request or help out? Open an issue on [.Net sdk github project](https://github.com/codenotary/immudb4dotnet/issues/new)
:::

::: tab Others
If you're using another development language, please refer to the [immugw](/master/immugw/) option.
:::

::::