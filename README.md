<!--
---

title: "immudb"

custom_edit_url: https://github.com/codenotary/immudb/edit/master/README.md
---

-->

# immudb [![License](https://img.shields.io/github/license/codenotary/immudb)](LICENSE) <img align="right" src="img/Black%20logo%20-%20no%20background.png" height="47px" />

[![Build Status](https://travis-ci.com/codenotary/immudb.svg?branch=master)](https://travis-ci.com/codenotary/immudb)
[![Go Report Card](https://goreportcard.com/badge/github.com/codenotary/immudb)](https://goreportcard.com/report/github.com/codenotary/immudb)
[![Coverage](https://coveralls.io/repos/github/codenotary/immudb/badge.svg?branch=master)](https://coveralls.io/github/codenotary/immudb?branch=master)
[![Homebrew](https://img.shields.io/homebrew/v/immudb)](https://formulae.brew.sh/formula/immudb)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)

Note: The `master` branch is the joint point for all ongoing development efforts. Thus it may be in an `unstable` state and should not be used in production deployments. Please use the latest release in order to get stable binaries.

[![Slack](https://img.shields.io/badge/join%20slack-%23immutability-brightgreen.svg)](https://slack.vchain.us/)
[![Discuss at immudb@googlegroups.com](https://img.shields.io/badge/discuss-immudb%40googlegroups.com-blue.svg)](https://groups.google.com/group/immudb)
[![Immudb Careers](https://img.shields.io/badge/careers-We%20are%20hiring!-blue?style=flat)](https://immudb.io/careers/)
[![Tweet about
immudb!](https://img.shields.io/twitter/url/http/shields.io.svg?style=social&label=Tweet%20about%20immudb)](https://twitter.com/intent/tweet?text=immudb:%20lightweight,%20high-speed%20immutable%20database!&url=https://github.com/codenotary/immudb)
[![Interactive Playground](https://img.shields.io/badge/Playground-blue?style=flat)](https://play.codenotary.com/)

```
Don't forget to ⭐ this repo if you like immudb!
```

[:tada: 2M+ pulls from docker hub!](https://hub.docker.com/r/codenotary/immudb/tags)

---

<img align="right" src="img/immudb-mascot-small.png" width="256px"/>

immudb is a key-value database with built-in cryptographic proof and verification. It can track changes in sensitive data and the integrity of the history will be protected by the clients, without the need to trust the server.

Traditional database transactions and logs are hard to scale and are mutable, so there is no way to know for sure if your data has been compromised. immudb is immutable. You can add new versions of existing records, but never change or delete records. This lets you store critical data without fear of it being changed silently.

Data stored in immudb is cryptographically coherent and verifiable, just like blockchains, but without all the complexity. Unlike blockchains, immudb can handle millions of transactions per second, and can be used both as a lightweight service or embedded in your application as a library.

Companies use immudb to protect credit card transactions and to secure processes by storing digital certificates and checksums.



### Tech specs

| Topic                   | Description                                                   |
| ----------------------- | ------------------------------------------------------------- |
| DB Model                | Key-Value store with 3D access (key-value-index)              |
| Data scheme             | schema-free                                                   |
| Implementation design   | LSM tree with value log and parallel Merkle Tree              |
| Implementation language | Go                                                            |
| Server OS(s)            | BSD, Linux, OS X, Solaris, Windows                            |
| Embeddable              | Yes, optionally                                               |
| Server APIs             | gRPC (using protocol buffers); immudb RESTful;                |
| Partition methods       | Sharding                                                      |
| Consistency concepts    | Eventual Consistency Immediate Consistency                    |
| Transaction concepts    | ACID with Snapshot Isolation (SSI)                            |
| Durability              | Yes                                                           |
| Snapshots               | Yes                                                           |
| High Read throughput    | Yes                                                           |
| High Write throughput   | Yes                                                           |
| Optimized for SSD       | Yes                                                           |

Benchmarks (v0.8.x):

| Specifications | Sequential write | Batch write |
| --- | --- | --- |
| 4 CPU cores | Concurrency: 128 | Concurrency: 16 |
| Intel(R) Xeon(R) CPU E3-1275 v6 @ 3.80GHz | Iterations: 1000000 | Iterations: 1000000 |
| 64 GB memory | Elapsted t.: 3.06 sec | Elapsed t.: 0.36 sec |
| SSD | Throughput: 326626 tx/sec | Throughput: 2772181 tx/sec |

## Quickstart

>To learn interactively and to get started with immudb from the command line and programming languages, visit the [immudb Playground](https://play.codenotary.com).

Getting immudb up and running is quite simple. For a super quick start, please follow step by step guides for each SDK or pick a basic running sample from [immudb-client-examples](https://github.com/codenotary/immudb-client-examples). Otherwise, you can use the immudb CLI tools described below.

- **immuclient** is the CLI client for immudb. You can read, write data into immudb from the commandline using direct or interactive mode.
- **immuadmin** is the admin CLI for immudb. You can manage immudb and get statistics as well as runtime information.

### Binary

Download latest release binaries from [here](https://github.com/codenotary/immudb/releases). Then just run immudb as follows:

```bash
./immudb
```

You can connect to the default database with immuclient by first running the interactive shell, then logging into the `immudb` database with the password `immudb`:

```bash
./immuclient

immuclient> login immudb
Password: immudb
immuclient> 
```

For more information, run `immuclient --help` to see all options.

### Docker

```bash
docker run -it -d -p 3322:3322 -p 9497:9497 --name immudb codenotary/immudb:latest
```

immudb should be up and running, you can check the container logs:

```bash
docker logs immudb
```

To perform actions, simply run ```./immuclient``` on the same machine or ```./immuclient -a <immudb-host>```. You can also find immuclient in the immudb container at `/usr/local/bin/immuadmin` or run a Docker container to connect to the local or remote immudb with `docker run -it --rm --name immuclient codenotary/immuclient:latest -a <immudb-host>`

To manage the database, simply run ```./immuadmin``` on the same machine. You can also find immuadmin in the immudb container at `/usr/local/bin/immuadmin` or run a Docker container to connect to the local immudb with `docker run -it --rm --name immuadmin codenotary/immuadmin:latest status`. For security reasons we recommend using immuadmin only on the same system as immudb. User management is restricted to localhost usage.

## Using immudb

Lot of useful documentation and step by step guides can be found at https://docs.immudb.io/

### Real world examples

We already learned about the following use cases from users:

- use immudb to immutably store every update to sensitive database fields (credit card or bank account data) of an existing application database
- store CI/CD recipes in immudb to protect build and deployment pipelines
- store public certificates in immudb
- use immudb as an additional hash storage for digital objects checksums
- store log streams (i. e. audit logs) tamperproof

### How to integrate immudb in your application

Integrate immudb into your application using official SDKs already available for the following programming languages:

1. Java [immudb4j](https://github.com/codenotary/immudb4j)
2. Golang [immudb-go](https://docs.immudb.io/immudb/golang.html)
3. .net [immudb4dotnet](https://github.com/codenotary/immudb4dotnet)
4. Python [immudb-py](https://github.com/codenotary/immudb-py)
5. Node.js [immudb-node](https://github.com/codenotary/immudb-node)

We've developed a "language-agnostic SDK" which exposes a REST API for easy consumption by any application.
[immugw](https://github.com/codenotary/immugw) may be convenient tool when SDKs are not available for the
programming language you're using, for experimentation, or just because you prefer your app only uses REST endpoints.

## Contributing

We welcome [contributions](CONTRIBUTING.md). Feel free to join the team!

To report bugs or get help, use [GitHub's issues](https://github.com/codenotary/immudb/issues).

immudb is [Apache v2.0 License](LICENSE).

immudb re-distributes other open-source tools and libraries - [Acknowledgements](ACKNOWLEDGEMENTS.md).
