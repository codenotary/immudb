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

[![Discord](https://img.shields.io/discord/831257098368319569)](https://discord.gg/pZnR7QC6)
[![Immudb Careers](https://img.shields.io/badge/careers-We%20are%20hiring!-blue?style=flat)](https://immudb.io/careers/)
[![Tweet about
immudb!](https://img.shields.io/twitter/url/http/shields.io.svg?style=social&label=Tweet%20about%20immudb)](https://twitter.com/intent/tweet?text=immudb:%20lightweight,%20high-speed%20immutable%20database!&url=https://github.com/codenotary/immudb)

```
Don't forget to ⭐ this repo if you like immudb!
```

[:tada: 3M+ pulls from docker hub!](https://hub.docker.com/r/codenotary/immudb/tags)

---

<img align="right" src="img/immudb-mascot-small.png" width="256px"/>

immudb is a key-value database with built-in cryptographic proof and verification. It can track changes in sensitive data and the integrity of the history will be protected by the clients, without the need to trust the server.

Traditional database transactions and logs are hard to scale and are mutable, so there is no way to know for sure if your data has been compromised. immudb is immutable. You can add new versions of existing records, but never change or delete records. This lets you store critical data without fear of it being changed silently.

Data stored in immudb is cryptographically coherent and verifiable, just like blockchains, but without all the complexity. Unlike blockchains, immudb can handle millions of transactions per second, and can be used both as a lightweight service or embedded in your application as a library.

Companies use immudb to protect credit card transactions and to secure processes by storing digital certificates and checksums.

<div align="center">
  <a href="https://play.codenotary.com">
    <img alt="immudb playground to start using immudb in an online demo environment" src="img/playground2.png"/>
  </a>
</div>


### Tech specs

| Topic                   | Description                                      |
| ----------------------- | ------------------------------------------------ |
| DB Model                | Key-Value store with 3D access (key-value-index) |
| Data scheme             | schema-free                                      |
| Implementation design   | LSM tree with value log and parallel Merkle Tree |
| Implementation language | Go                                               |
| Server OS(s)            | BSD, Linux, OS X, Solaris, Windows and more      |
| Embeddable              | Yes, optionally                                  |
| Server APIs             | gRPC                                             |
| Partition methods       | Sharding                                         |
| Consistency concepts    | Eventual Consistency Immediate Consistency       |
| Transaction concepts    | ACID with Snapshot Isolation (SSI)               |
| Durability              | Yes                                              |
| Snapshots               | Yes                                              |
| High Read throughput    | Yes                                              |
| High Write throughput   | Yes                                              |
| Optimized for SSD       | Yes                                              |


## Quickstart


### Getting immudb running

You may download the immudb binary from [the latest releases on Github](https://github.com/codenotary/immudb/releases/latest). Once you have downloaded immudb, rename it to `immudb`, make sure to mark it as executable, then run it. The following example shows how to obtain v0.9.2 for linux amd64:

```bash
wget https://github.com/vchain-us/immudb/releases/download/v0.9.2/immudb-v0.9.2-linux-amd64
mv immudb-v0.9.2-linux-amd64 immudb
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

If you are running the Docker image without host networking, make sure to expose ports 3322 and 9497.

### Connecting with immuclient

You may download the immuclient binary from [the latest releases on Github](https://github.com/codenotary/immudb/releases/latest). Once you have downloaded immuclient, rename it to `immuclient`, make sure to mark it as executable, then run it. The following example shows how to obtain v0.9.2 for linux amd64:

```bash
wget https://github.com/vchain-us/immudb/releases/download/v0.9.2/immuclient-v0.9.2-linux-amd64
mv immuclient-v0.9.2-linux-amd64 immuclient
chmod +x immuclient

# start the interactive shell
./immuclient

# or use commands directly
./immuclient help
```

Alternatively, you may use Docker to run immuclient in a ready-to-use container:

```bash
docker run -it --rm --net host --name immuclient codenotary/immuclient:latest
```



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

For a super quick start, please follow step by step guides for each SDK or pick a basic running sample from [immudb-client-examples](https://github.com/codenotary/immudb-client-examples).

We've developed a "language-agnostic SDK" which exposes a REST API for easy consumption by any application.
[immugw](https://github.com/codenotary/immugw) may be a convenient tool when SDKs are not available for the
programming language you're using, for experimentation, or just because you prefer your app only uses REST endpoints.

# Performance figures

immudb can handle millions of writes per second. The following table shows performance of the embedded store inserting 1M entries on a 4-core E3-1275v6 CPU and SSD disk with 20-100 parallel workers:

| Entries | Workers | Batch | Batches | time (s) | Entries/s |
| ------ | ------ | ------ | ------ | ------ | ------ |
| 1M | 20 | 1000 | 50 | 1.061 | 	1.2M /s |
| 1M	| 50	| 1000 |	20 | 0.543	| 1.8M /s |
| 1M |	100 |	1000 |	10 | 0.615 |	1.6M /s |

You can generate your own benchmarks using the `stress_tool` under `embedded/tools`.

## Contributing

We welcome [contributions](CONTRIBUTING.md). Feel free to join the team!

Learn how to [build](BUILD.md) immudb components in both binary and Docker image form.

To report bugs or get help, use [GitHub's issues](https://github.com/codenotary/immudb/issues).

immudb is [Apache v2.0 License](LICENSE).

immudb re-distributes other open-source tools and libraries - [Acknowledgements](ACKNOWLEDGEMENTS.md).
