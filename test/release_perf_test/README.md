# Performance test

This utility test performs a set of performance test on immudb and outputs a CSV table that
can be easily imported in a spreadsheet.
Both immudb and the testing tools are built in a docker container. Docker compose is then used
to run immudb and the test.

# Usage

## Quickstart

Just launch `./runme.sh` and come back tomorrow.

## Script usage

**Note**: We recommend using `screen` or `tmux` to detach the session from the console. The test
will run for a very long time and you risk a disconnection will ruin the result.

The `runme.sh` scripts accept some options. Please refer to the test description below to better
understand what the meaning of each of them is.

```sh
./runme.sh [ -d duration ] [ -B ] [ -D ] [ -Q ] [ -K ]
        -d duration: duration of each test in seconds
        -B (nobuild): skip building of containers
        -D (nodocument): skip document test
        -Q (nosql): skip sql test
        -K (nokv): skip KV test
```

By default, the duration of each test is 600 seconds (10 minutes). You can change that with the `-d`
parameter. You can also skip a test section with flags `-D`, `-Q`, `-K` or avoid building the
containers (useful if they are already built) with `-B`.

## Result / Output

At the end of the tests a table will be printed, in CSV format, like this:
```csv
test;client,batchsize,replication,Write TX/s,Write KV/s
kv;1;1;no;31;31
kv;10;1;no;261;261
kv;100;1;no;2218;2218
kv;1;100;no;2717;271700
kv;10;100;no;21033;2103300
[...]
doc;1;100;async;14.9457;1494.568
doc;10;100;async;16.7671;1676.706
doc;100;100;async;18.2019;1820.194
doc;1;1000;async;11.4383;11438.330
doc;10;1000;async;12.8969;12896.903
doc;100;1000;async;13.6866;13686.639
```

This table is also saved in file `result.csv`. It shows all tests performed, with the indication of
the subsystem (kv, sql, doc), the number of clients, the batchsize, the type of replication setup and
(finally) the number of transaction per second and the number of document (KV pair, SQL lines or JSON
document) inserted per second.

## Test matrix

Three set of tests are executed: key/value inserts (KV), SQL inserts (SQL) and json document
insertion, using HTTP APIs. For each set, we execute multiple runs, with different number of concurrent
client, and with different batch size.
We use runs with 1, 10, 100 clients and batch size 1, 100, 1000. So each single test is executed 9 times.
Each run has a fixed duration, which by default is 10 minutes.

### Replication
On top of that, each test matrix is run three times:
- on a single immudb instance,
- on an asynchronous replicated couple of instances,
- and on a synchronous replicated couple of instance.

So every subsystem (KV, SQL, DOC) performs 27 tests, each lasting by default 10 minutes, for a total of
81 tests, or 810 minutes. Plus startup/teardown time. So the whole test procedure can easily last 14 hours.

## Immudb Version tested

By default, the current immudb code is compiled and tested. It is possible to have a different
immudb version, by using a specifically crafted Dockerfile to create a custom immudb docker.
As a reference, we have the `Dockerfile-141` file that builds immudb 1.4.1

To use the custom dockerfile, simply set the `DOCKERFILE` variable (i.e. `export DOCKERFILE=Dockerfile-1.4.1`)
before running the script.

## Docker architecture

We use docker-compose to build and run the tests

### Containers
The server is built using the `Dockerfile` in the present directory. It compiles immudb from the
very same branch that is checked out, adding a startup scripts that takes care of tuning the server
startup parameters, creating a testing database with correct settings and, if required, needed replication
options.

The client docker checks out the testing tools from the `immudb-tools` repository and builds a client
that contains all the binaries and scripts needed for testing.

### Running the tests

Docker compose "recycles" the same server container for all different replication scenario, just
providing the startup script with the correct option.
The test is then run by providing the client container the correct entrypoint for the test that is
being performed.

