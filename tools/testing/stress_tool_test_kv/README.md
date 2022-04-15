# Stress tool for KV testing

This tool enables parallel stress test of immudb KV using randomized key / value entries.
Randomized keys are a very heavy test for btree where reads / writes are scattered across
whole btree increasing internal btree cache pressure.

By default the test will connect to an immudb running on localhost.
It will run parallel workers, each worker first inserts the data, then it reads keys
checking if the read value is correct.

## Mixed read-write mode

In order to test how database performs when parallel reads and writes are performed,
use the `-mix-read-writes` flag. By doing so, the test starts with first half of workers.
Once those workers finish their writes, they start the reading test and the second
half of workers is spawned in parallel.

## Sample invocation

```sh
# Run full test
go run .
```

```sh
# Run quick test with reduced amount of entries
go run . -total-entries-written 200000 -total-entries-read 20000
```

```sh
# Run quick test with mixed reads and writes
go run . -total-entries-written 200000 -total-entries-read 20000 -mix-read-writes
```
