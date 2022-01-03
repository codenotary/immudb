# User quickstart with immudb and immuclient

{% hint style="info" %}
To learn interactively and to get started with immudb from the command line and with programming languages, visit the immudb Playground at [https://play.codenotary.com](https://play.codenotary.com)
{% endhint %}

### Getting immudb running <a href="#getting-immudb-running" id="getting-immudb-running"></a>

You may download the immudb binary from [the latest releases on Github (opens new window)](https://github.com/codenotary/immudb/releases/latest). Once you have downloaded immudb, rename it to **`immudb`**, make sure to mark it as executable, then run it. The following example shows how to obtain v1.0 for linux amd64:

```bash
wget https://github.com/vchain-us/immudb/releases/download/v1.0.0/immudb-v1.0.0-linux-amd64
mv immudb-v1.0.0-linux-amd64 immudb
chmod +x immudb

# run immudb in the foreground to see all output
./immudb

# or run immudb in the background
./immudb -d
```

Alternatively, you may use Docker to run immudb in a ready-to-use container:

```bash
docker run -d --net host -it --rm --name immudb codenotary/immudb:latest
```

### Connecting with immuclient

You may download the immuclient binary from [the latest releases on Github (opens new window)](https://github.com/codenotary/immudb/releases/latest). Once you have downloaded immuclient, rename it to **`immuclient`**, make sure to mark it as executable, then run it. The following example shows how to obtain v1.0.0 for linux amd64:

```
wget https://github.com/vchain-us/immudb/releases/download/v1.0.0/immuclient-v1.0.0-linux-amd64
mv immuclient-v1.0.0-linux-amd64 immuclient
chmod +x immuclient

# start the interactive shell

./immuclient
```

Alternatively, you may use Docker to run immuclient in a ready-to-use container:

```bash
docker run -it --rm --net host --name immuclient codenotary/immuclient:latest
```

### Basic operations with immuclient <a href="#basic-operations-with-immuclient" id="basic-operations-with-immuclient"></a>

Before any operations can be run by immuclient, it is necessary to authenticate against the running immudb server.

When immudb is first run, it is ready to use immediately with the default database and credentials:

* Database name: defaultdb
* User: immudb
* Password: immudb
* Port: 3322

Running **`login immudb`** from within immuclient will use the default database name and port. All you need to supply is the user and password:

```bash
immuclient> login immudb
Password: immudb
```

While immudb supports **`set`** and **`get`** for key-value storing and retrieving, its immutability means that we can verify the integrity of the underlying Merkle tree. To do this, we use the **`safeset`** and **`safeget`** commands. Let's try setting a value of **`100`** for the key **`balance`**:

```bash
immuclient> safeset balance 100
```

Then, we can immediately overwrite the key **`balance`** with a value of **`9001`** instead:

```bash
immuclient> safeset balance 9001
```

If we try to retrieve the current value of key **`balance`**, we should get **`9001`**:

```bash
immuclient> safeget balance
```

Note that at each step so far, the **`verified`** flag is set to true. This ensures that the Merkle tree remains consistent for each transaction.

We can show the history of transactions for key **`balance`** using the **`history`** command:

```bash
immuclient> history balance
```

### SQL operations with immuclient <a href="#sql-operations-with-immuclient" id="sql-operations-with-immuclient"></a>

In addition to a key-value store, immudb supports the relational model (SQL). For example, to a table:

```bash
$ immuclient exec "CREATE TABLE people(id INTEGER, name VARCHAR, salary INTEGER, PRIMARY KEY id);"
sql ok, Ctxs: 1 Dtxs: 0
```

To insert data, use **`UPSERT`** (insert or update), which will add an entry, or overwrite it if already exists (based on the primary key):

```bash
$ immuclient exec "UPSERT INTO people(id, name, salary) VALUES (1, 'Joe', 10000);"sql ok, Ctxs: 0 Dtxs: 1immuclient exec "UPSERT INTO people(id, name, salary) VALUES (2, 'Bob', 30000);"
sql ok, Ctxs: 0 Dtxs: 1
```

To query the data you can use the traditional **`SELECT`**:

```bash
$ immuclient query "SELECT id, name, salary FROM people;"
+-----------------------+-------------------------+---------------------------+
| (DEFAULTDB PEOPLE ID) | (DEFAULTDB PEOPLE NAME) | (DEFAULTDB PEOPLE SALARY) |
+-----------------------+-------------------------+---------------------------+
|                     1 | Joe                     |                     10000 |
|                     2 | Bob                     |                     30000 |
+-----------------------+-------------------------+---------------------------+
```

If we upsert again on the primary key "1", the value for "Joe" will be overwriten:

```bash
immuclient exec "UPSERT INTO people(id, name, salary) VALUES (1, 'Joe', 20000);"
sql ok, Ctxs: 0 Dtxs: 1

immuclient query "SELECT id, name, salary FROM people;"
+-----------------------+-------------------------+---------------------------+
| (DEFAULTDB PEOPLE ID) | (DEFAULTDB PEOPLE NAME) | (DEFAULTDB PEOPLE SALARY) |
+-----------------------+-------------------------+---------------------------+
|                     1 | Joe                     |                     20000 |
|                     2 | Bob                     |                     30000 |
+-----------------------+-------------------------+---------------------------+
```

### Time travel <a href="#time-travel" id="time-travel"></a>

immudb is a immutable database. History is always preserved. With immudb you can travel in time!

```bash
immuclient query "SELECT id, name, salary FROM people WHERE name='Joe';"
+-----------------------+-------------------------+---------------------------+
| (DEFAULTDB PEOPLE ID) | (DEFAULTDB PEOPLE NAME) | (DEFAULTDB PEOPLE SALARY) |
+-----------------------+-------------------------+---------------------------+
|                     1 | Joe                     |                     20000 |
+-----------------------+-------------------------+---------------------------+
```

Eg. before the update:

```bash
immuclient query "SELECT id, name, salary FROM (people BEFORE TX 3) WHERE name='Joe';"
+-----------------------+-------------------------+---------------------------+
| (DEFAULTDB PEOPLE ID) | (DEFAULTDB PEOPLE NAME) | (DEFAULTDB PEOPLE SALARY) |
+-----------------------+-------------------------+---------------------------+
|                     1 | Joe                     |                     10000 |
+-----------------------+-------------------------+---------------------------+
```

or even before the first time insert (guess what, it is empty!):

```bash
immuclient query "SELECT id, name, salary FROM (people BEFORE TX 1) WHERE name='Joe';"
+-----------------------+-------------------------+---------------------------+
| (DEFAULTDB PEOPLE ID) | (DEFAULTDB PEOPLE NAME) | (DEFAULTDB PEOPLE SALARY) |
+-----------------------+-------------------------+---------------------------+
You can even TABLE a table with itself in the past. Imagine you want to see how people salary changed between two points in time:
```

You can even **`TABLE`** a table with itself in the past. Imagine you want to see how people salary changed between two points in time:

```bash
immuclient query "SELECT peoplenow.id, peoplenow.name, peoplethen.salary, peoplenow.salary FROM (people BEFORE TX 3 AS peoplethen) INNER JOIN (people AS peoplenow) ON peoplenow.id=peoplethen.id;"
+--------------------------+----------------------------+-------------------------------+------------------------------+
| (DEFAULTDB PEOPLENOW ID) | (DEFAULTDB PEOPLENOW NAME) | (DEFAULTDB PEOPLETHEN SALARY) | (DEFAULTDB PEOPLENOW SALARY) |
+--------------------------+----------------------------+-------------------------------+------------------------------+
|                        1 | Joe                        |                         10000 |                        20000 |
|                        2 | Bob                        |                         30000 |                        30000 |
+--------------------------+----------------------------+-------------------------------+------------------------------+
```
