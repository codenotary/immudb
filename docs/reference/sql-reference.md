# SQL reference

### Data types <a href="#data-types" id="data-types"></a>

| Name      | Description                                                                      | Length constraints                                                        |
| --------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| INTEGER   | Signed 64-bit integer value. Usually referred to as `BIGINT` in other databases. | -                                                                         |
| BOOLEAN   | A boolean value, either `TRUE` or `FALSE`                                        | -                                                                         |
| VARCHAR   | UTF8-encoded text                                                                | Maximum number of bytes in the UTF-8 encoded representation of the string |
| BLOB      | sequence of bytes                                                                | Maximum number of bytes in the sequence                                   |
| TIMESTAMP | datetime value with microsecond precision                                        | -                                                                         |

### Size constraints <a href="#size-constraints" id="size-constraints"></a>

Size constraint is specified with a `[MAX_SIZE]` suffix on the type, e.g. `BLOB[16]` represents a sequence of up to 16 bytes.

### NULL values <a href="#null-values" id="null-values"></a>

`NULL` values in immudb are not unique - two `NULL` values are considered equal on comparisons.

### Timestamp values <a href="#timestamp-values" id="timestamp-values"></a>

Timestamp values are internally stored as a 64-bit signed integer being a number of microseconds since the epoch time. Those values are not associated with any timezone, whenever a conversion is needed, it is considered to be in UTC.

### Creating tables <a href="#creating-tables" id="creating-tables"></a>

Common examples of `CREATE TABLE` statements are presented below.

#### `IF NOT EXISTS` <a href="#if-not-exists" id="if-not-exists"></a>

With this clause the `CREATE TABLE` statement will not fail if a table with same name already exists.

Note: If the table already exists, it is not compared against the provided table definition neither it is updated to match it.

#### `NOT NULL` <a href="#not-null" id="not-null"></a>

Columns marked as not null can not have a null value assigned.

#### `PRIMARY KEY` <a href="#primary-key" id="primary-key"></a>

Every table in immudb must have a primary key. Primary key can use at least 1 and up to 8 columns.

Columns used in a primary key can not have `NULL` values assigned, even if those columns are not explicitly marked as `NOT NULL`.

Primary key creates an implicit unique index on all contained columns.

#### `AUTO_INCREMENT` <a href="#auto-increment" id="auto-increment"></a>

A single-column `PRIMARY KEY` can be marked as `AUTO_INCREMENT`. immudb will automatically set a unique value of this column for new rows.

When inserting data into a table with an `INSERT` statement, the value for such primary key must be omitted. When updating data in such table with `UPSERT` statement, the value for such primary key is obligatory and the `UPSERT` statement can only update existing rows.

The type of an `AUTO_INCREMENT` column must be `INTEGER`. Internally immudb will assign sequentially increasing values for new rows ensuring this value is unique within a single table.

#### Foreign keys <a href="#foreign-keys" id="foreign-keys"></a>

Explicit support for relations to foreign tables is not currently supported in immudb. It is possible however to create ordinary columns containing foreign key values that can be used in `JOIN` statements. Application logic is responsible for ensuring data consistency and foreign key constraints.

### Indexes <a href="#indexes" id="indexes"></a>

immudb indexes can be used for a quick search of rows with columns having specific values.

Certain operations such as ordering values with `ORDER BY` clause require columns to be indexed.

Index can only be added to an empty table.

Index do not have explicit name and is referenced by the ordered list of indexed columns.

#### Column value constraints <a href="#column-value-constraints" id="column-value-constraints"></a>

Columns of `BLOB` or `VARCHAR` type must have a size limit set on them. The maximum allowed value size for one indexed column is 256 bytes.

#### Unique indexes <a href="#unique-indexes" id="unique-indexes"></a>

Index can be marked as unique with extra `UNIQUE` keyword. Unique index will prevent insertion of new data into the table that would violate uniqueness of indexed columns within the table.

#### Multi-column indexes <a href="#multi-column-indexes" id="multi-column-indexes"></a>

Index can be set on up to 8 columns. The order of columns is important when doing range scans, iterating over such index will first sort by the value of the first column, then by the second and so on.

Note: Large indexes will increase the storage requirement and will reduce the performance of data insertion. Iterating using small indexes will also be faster than with the large ones.

#### `IF NOT EXISTS` <a href="#if-not-exists-2" id="if-not-exists-2"></a>

With this clause the `CREATE INDEX` statement will not fail if an index with same type and list of columns already exists. This includes a use case where the table is not empty which can be used to simplify database schema initialization.

Note: If the index already exists, it is not compared against the provided index definition neither it is updated to match it.

### Inserting or updating data <a href="#inserting-or-updating-data" id="inserting-or-updating-data"></a>

#### `INSERT` <a href="#insert" id="insert"></a>

immudb supports standard `INSERT` sql statement. It can be used to add one or multiple values within the same transaction.

#### `UPSERT` <a href="#upsert" id="upsert"></a>

`UPSERT` is an operation with a syntax similar to `INSERT`, the difference between those two is that `UPSERT` either creates a new or replaces an existing row. A new row is created if an entry with the same primary key does not yet exist in the table, otherwise the current row is replaced with the new one.

If a table contains an `AUTO_INCREMENT` primary key, the value for that key must be provided and the `UPSERT` operation will only update the existing row.

#### Timestamp, NOW() and CAST() built-in function <a href="#timestamp-now-and-cast-built-in-function" id="timestamp-now-and-cast-built-in-function"></a>

The built-in `NOW()` function returns the current timestamp value as seen on the server.

The `CAST` function can be used to convert a string or an integer to a timestamp value.

The integer value is interpreted as a Unix timestamp (number of seconds since the epoch time).

The string value passed to the `CAST` function must be in one of the following formats: `2021-12-08`, `2021-12-08 17:21`, `2021-12-08 17:21:59`, `2021-12-08 17:21:59.342516`. Time components not specified in the string are set to 0.

### Querying <a href="#querying" id="querying"></a>

#### Selecting all columns <a href="#selecting-all-columns" id="selecting-all-columns"></a>

All columns from all joined tables can be queried with `SELECT *` statement.

#### Selecting specific columns <a href="#selecting-specific-columns" id="selecting-specific-columns"></a>

#### Filtering entries <a href="#filtering-entries" id="filtering-entries"></a>

#### Ordering by column value <a href="#ordering-by-column-value" id="ordering-by-column-value"></a>

Currently only one column can be used in the `ORDER BY` clause.

The order may be either ascending (`ASC` suffix, default) or descending (`DESC` suffix).

Ordering rows by a value of a column requires a matching index on that column.

#### `INNER JOIN` <a href="#inner-join" id="inner-join"></a>

immudb supports standard SQL `INNER JOIN` syntax. The `INNER` join type is optional.

#### `LIKE` operator <a href="#like-operator" id="like-operator"></a>

immudb supports the `LIKE` operator. Unlike in other SQL engines though, the pattern use a regexp syntax supported by the [regexp library in the go language (opens new window)](https://pkg.go.dev/regexp).

A `NOT` prefix negates the value of the `LIKE` operator.

#### `IN` operator <a href="#in-operator" id="in-operator"></a>

immudb has a basic supports for the `IN` operator.

A `NOT` prefix negates the value of the `IN` operator.

Note: Currently the list for the `IN` operator can not be calculated using a sub-query.

#### Column and table aliasing <a href="#column-and-table-aliasing" id="column-and-table-aliasing"></a>

Table name aliasing is necessary when using more than one join with the same table.

#### Aggregations <a href="#aggregations" id="aggregations"></a>

Available aggregation functions:

* COUNT
* SUM
* MAX
* MIN
* AVG

#### Grouping results with `GROUP BY` <a href="#grouping-results-with-group-by" id="grouping-results-with-group-by"></a>

Results can be grouped by a value of a single column. That column must also be used in a matching `ORDER BY` clause.

#### Filtering grouped results with `HAVING` <a href="#filtering-grouped-results-with-having" id="filtering-grouped-results-with-having"></a>

#### Sub-queries <a href="#sub-queries" id="sub-queries"></a>

The table in the `SELECT` or `JOIN` clauses can be replaced with a sub-query.

Note: the context of a sub-query does not propagate outside, e.g. it is not possible to reference a table from a sub-query in the `WHERE` clause outside of the sub-query.

#### Transactions <a href="#transactions" id="transactions"></a>

Multiple insert and upsert statements can be issued within a single transaction.

The easiest way to tested it is with the `./immuclient exec "..."` shell command (make sure to use an escaped `\$` value to avoid cutting out part of the price).

#### Time travel <a href="#time-travel" id="time-travel"></a>

Time travel allows reading data from SQL as if it was in some previous state. The state is indicated by transaction id.

A historical version of a table can be used in `SELECT` statements using the `BEFORE TX` clause:
