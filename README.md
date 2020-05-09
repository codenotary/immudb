<!--
---

title: "immudb"

custom_edit_url: https://github.com/codenotary/immudb/edit/master/README.md
---

-->

# immudb [![Build Status](https://travis-ci.com/codenotary/immudb.svg?branch=master)](https://travis-ci.com/codenotary/immudb) [![License](https://img.shields.io/github/license/codenotary/immudb)](LICENSE) <img align="right" src="img/Black%20logo%20-%20no%20background.png" width="160px"/>

[![Go Report Card](https://goreportcard.com/badge/github.com/codenotary/immudb)](https://goreportcard.com/report/github.com/codenotary/immudb)

immudb is **lightweight, high-speed immutable database** for systems and applications. With immmudb you can
track changes in sensitive data in your transactional databases and then record those changes indelibly in a the
tamperproof immudb database. This allows you to keep an indelible history of, say, your debit/credit transactions.
<img align="right" src="img/immudb-mascot-small.png" width="256px"/>

Traditional transaction logs are hard to scale, and are not immutable. So there is no way to know for sure if your data has been compromised.

As such immudb provides **unparalleled insights** **retro-actively**, of what happened to your sensitive data, even
if your perimiter was compromised. immudb provides the guarantatee of immutability by using internally a **Merkle tree structure**.

immudb gives you the same **cyrptographic verification** of the integrity of data written with **SHA-256** like classic blockhain without the cost and complexity associated with blockchains today.

immudb has 4 main benefits:

1. **immudb is immutable**. You can only add records, but **never change or delete records**.
2. data stored in immudb is **cryptographically coherent and verifiable**, like blockchains, just without all the complexity and at high speed.
3. Anyone can get **started with immudb in minutes**. Wether in node.js, Java, Python, Golang, .Net, or any other language. It's very easy to use and you can have your immutable database running in just a few minutes.
4. Finally, immudb is  **Open Source**. You can run it **on premise**, or in the **cloud** and it's completely free. immudb is governed by the Apache 2.0 License.

immudb is currently runs on **Linux**, **FreeBSD**, **Windows**, and **MacOS**, along with
other systems derived from them, such as **Kubernetes** and **Docker**.

[![Tweet about
immudb!](https://img.shields.io/twitter/url/http/shields.io.svg?style=social&label=Tweet%20about%20immudb)](https://twitter.com/intent/tweet?text=immudb:%20lightweight,%20high-speed%20immutable%20database!&url=https://github.com/codenotary/immudb)


## Contents

1.  [What does it look like?](#what-does-it-look-like) - Take a quick tour through the project
2.  [Our userbase](#user-base) - Our userbase
3.  [Quickstart](#quickstart) - How to try it now on your systems, get a Docker container running in seconds
4.  [Why immudb](#why-immudb) - Why people love immudb and how it compares with other solutions
5.  [News](#news) - The latest news about immudb
6.  [How immudb works](#how-immudb-works) - A high-level diagram of how immudb works
7.  [Features](#features) - How you'll use immudb on your systems
8.  [Read world examples](#examples) - Read about how others use immudb
9.  [Documentation](#documentation) - Read the documentation
10.  [FAQ](#faq) - Frequently asked questions
11.  [Community](#community) - Discuss immudb with others and get support
12.  [License](#license) - Check immudb's licencing
13.  [Is it awesome?](#is-it-awesome) - Yes.

## What does it look like?

![immudb Highlevel](img/highlevel.png "immudb highlevel overview")

### High level features

#### Simplified API for safe SET/GET

single API call that performs all steps and returns the proofs directly.

#### REST gateway (for legacy systems)

A gRPC REST gateway is a reverse proxy that sits in the middle between the gRPC API and the application.

Other than simply converting the gRPC API to a REST interface, this component will have a built-in verification on query results and will return the verification result directly.

This solution is completely transparent: the client application can use just one endpoint (the REST interface) to perform all operations.
The REST gateway can be also embedded into the immudb binary directly.

#### Driver for common languages

Driver available for:

1. Java
2. .net
3. Golang
4. Python
5. Node.js

#### Structured value

The Any message type of protobuffer allows to lets you use messages as embedded types without having their .proto definition. Thus it’s possible to decouple and extend (in future) the value structure.
Value can be augmented with some client provided metadata. That also permits to use an on-demand serialization/deserialization strategy

#### Item References

enables the insertion of a special entry which references to another item

#### Value timestamp

The server should not set the timestamp, to avoid relying on a not verifiable “single source of truth”.
Thus the clients have to provide it. The client driver implementation can automatically do that for the user.

#### Primary Index

Index enables queries and search based on the data key

#### Secondary Index

Index enables queries and search based on the data value

#### Cryptographic signatures

A signature (PKI) provided by the client can be became part of the insertion process

#### Authentication (transport)

integrated mTLS offers the best approach for machine-to-machine authentication, also providing communications security (entryption) over the transport channel

### Benchmark

* 4 CPU cores
* Intel(R) Xeon(R) CPU E3-1275 v6 @ 3.80GHz
* 64 GB memory
* SSD

![immudb throughput read Benchmark](img/throughput_read.png "Throughput read (higher is better)")

![immudb Throughput write Benchmark](img/throughput_write.png "Throughput write (higher is better)")

![immudb Query Benchmark](img/query_bm.png "100 records read execution time (lower is better)")

![immudb Execution Benchmark](img/exectime.png "100 records write execution time (lower is better)")

### Tech specs

| Topic                     | Description                                      |
| ------------------------- | ------------------------------------------------ |
| DB Model                  | Key-Value store with 3D access (key-value-index) |
| Data scheme               | schema-free                                      |
| Implementation design     | LSM tree with value log and parallel Merkle Tree |
| Implemementation language | Golang                                           |
| Server OS(s)              | BSD, Linux, OS X, Solaris, Windows               |
| Embeddable                | Yes, optionally                                  |
| Server APIs               | gRPC (using protocol buffers); immugw RESTful    |
| Partition methods         | Sharding                                         |
| Consistency concepts      | Eventual Consistency Immediate Consistency       |
| Transaction concepts      | ACID with Snapshot Isolation (SSI)               |
| Durability                | Yes                                              |
| Snapshots                 | Yes                                              |
| High Read throughput      | Yes                                              |
| High Write throughput     | Yes                                              |
| Optimized for SSD         | Yes                                              |

## Our Userbase

### Docker pulls

We provide Docker images for the most common architectures. These are statistics reported by Docker Hub:

The immudb container images can be found here:

| Component | Container image                               | Pull stats                                                   |
| --------- | --------------------------------------------- | ------------------------------------------------------------ |
| immudb    | https://hub.docker.com/r/codenotary/immudb    | [![codenotary/immudb<br/>(official)](https://img.shields.io/docker/pulls/codenotary/immudb.svg?label=codenotary/immudb+%28official%29)](https://hub.docker.com/r/codenotary/immudb/) |
| immugw    | https://hub.docker.com/r/codenotary/immugw    | [![codenotary/immugw<br/>(official)](https://img.shields.io/docker/pulls/codenotary/immudb.svg?label=codenotary/immugw+%28official%29)](https://hub.docker.com/r/codenotary/immugw/) |
| immuadmin | https://hub.docker.com/r/codenotary/immuadmin | [![codenotary/immuadmin<br/>(official)](https://img.shields.io/docker/pulls/codenotary/immudb.svg?label=codenotary/immuadmin+%28official%29)](https://hub.docker.com/r/codenotary/immuadmin/) |



## Quickstart

### Binaries

Check out our releases and download the required binaries depending on your needs.

- **immudb** is the server binary that listens on port 3322 on localhost and provides a gRPC interface
- **immugw** is the intelligent REST proxy that connects to immudb and provides a RESTful interface for applications. We recommend to run immudb and immugw on separate machines to enhance security
- **immuadmin** is the admin CLI for immudb and immugw. You can install and manage the service installation for both components and get statistics as well as runtime information.

The latest release binaries can be found [here](https://github.com/codenotary/immudb/releases )

#### Build the binaries yourself

To build the binaries yourself, simply clone this repo and run

##### Linux

```bash
GOOS=linux GOARCH=amd64 make immuadmin-static immudb-static immugw-static
```

##### Windows

```bash
GOOS=windows GOARCH=amd64 make immuadmin-static immudb-static immugw-static
```



#### immudb

Simply run ```./immudb -d``` to start immudb locally in the background

```
immudb - the lightweight, high-speed immutable database for systems and applications.

Environment variables:
  IMMUDB_DIR=.
  IMMUDB_NETWORK=tcp
  IMMUDB_ADDRESS=127.0.0.1
  IMMUDB_PORT=3322
  IMMUDB_DBNAME=immudb
  IMMUDB_PIDFILE=
  IMMUDB_LOGFILE=
  IMMUDB_MTLS=false
  IMMUDB_AUTH=false
  IMMUDB_DETACHED=false
  IMMUDB_PKEY=./tools/mtls/3_application/private/localhost.key.pem
  IMMUDB_CERTIFICATE=./tools/mtls/3_application/certs/localhost.cert.pem
  IMMUDB_CLIENTCAS=./tools/mtls/2_intermediate/certs/ca-chain.cert.pem

Usage:
  immudb [flags]
  immudb [command]

Available Commands:
  help        Help about any command
  version     Show the immudb version

Flags:
  -a, --address string       bind address (default "127.0.0.1")
  -s, --auth                 enable auth
      --certificate string   server certificate file path (default "./tools/mtls/3_application/certs/localhost.cert.pem")
      --clientcas string     clients certificates list. Aka certificate authority (default "./tools/mtls/2_intermediate/certs/ca-chain.cert.pem")
      --config string        config file (default path are configs or $HOME. Default filename is immudb.ini)
  -n, --dbname string        db name (default "immudb")
  -d, --detached             run immudb in background
      --dir string           data folder (default "./db")
  -h, --help                 help for immudb
      --logfile string       log path with filename. E.g. /tmp/immudb/immudb.log
  -m, --mtls                 enable mutual tls
      --no-histograms        disable collection of histogram metrics like query durations
      --pidfile string       pid path with filename. E.g. /var/run/immudb.pid
      --pkey string          server private key path (default "./tools/mtls/3_application/private/localhost.key.pem")
  -p, --port int             port number (default 3322)

Use "immudb [command] --help" for more information about a command.

```

#### immugw

Simply run ```./immugw``` to start immugw on the same machine as immudb (test or dev environment) or pointing to the remote immudb system ```./immugw --immudbaddress "immudb-server"```.

```
immu gateway: a smart REST proxy for immudb - the lightweight, high-speed immutable database for systems and applications.
It exposes all gRPC methods with a REST interface while wrapping all SAFE endpoints with a verification service.

Environment variables:
  IMMUGW_ADDRESS=127.0.0.1
  IMMUGW_PORT=3323
  IMMUGW_IMMUDBADDRESS=127.0.0.1
  IMMUGW_IMMUDBPORT=3322
  IMMUGW_PIDFILE=
  IMMUGW_LOGFILE=
  IMMUGW_DETACHED=false
  IMMUGW_MTLS=false
  IMMUGW_SERVERNAME=localhost
  IMMUGW_CERTIFICATE=./tools/mtls/4_client/certs/localhost.cert.pem
  IMMUGW_PKEY=./tools/mtls/4_client/private/localhost.key.pem
  IMMUGW_CLIENTCAS=./tools/mtls/2_intermediate/certs/ca-chain.cert.pem

Usage:
  immugw [flags]
  immugw [command]

Available Commands:
  help        Help about any command
  version     Show the immugw version

Flags:
  -a, --address string         immugw host address (default "127.0.0.1")
      --certificate string     server certificate file path (default "./tools/mtls/4_client/certs/localhost.cert.pem")
      --clientcas string       clients certificates list. Aka certificate authority (default "./tools/mtls/2_intermediate/certs/ca-chain.cert.pem")
      --config string          config file (default path are configs or $HOME. Default filename is immugw.ini)
  -d, --detached               run immudb in background
  -h, --help                   help for immugw
  -k, --immudbaddress string   immudb host address (default "127.0.0.1")
  -j, --immudbport int         immudb port number (default 3322)
      --logfile string         log path with filename. E.g. /tmp/immugw/immugw.log
  -m, --mtls                   enable mutual tls
      --pidfile string         pid path with filename. E.g. /var/run/immugw.pid
      --pkey string            server private key path (default "./tools/mtls/4_client/private/localhost.key.pem")
  -p, --port int               immugw port number (default 3323)
      --servername string      used to verify the hostname on the returned certificates (default "localhost")

Use "immugw [command] --help" for more information about a command.
```



#### immuadmin

For security reasons we recommend using immuadmin only on the same system as immudb. Simply run ```./immuadmin``` on the same machine.

```
CLI admin client for immudb - the lightweight, high-speed immutable database for systems and applications.

Environment variables:
  IMMUADMIN_ADDRESS=127.0.0.1
  IMMUADMIN_PORT=3322
  IMMUADMIN_MTLS=true

Usage:
  immuadmin [command]

Available Commands:
  dump        Dump database content to a file
  help        Help about any command
  login       Login using the specified username and password (admin username is immu)
  logout
  service     Manage immu services
  stats       Show statistics as text or visually with the '-v' option. Run 'immuadmin stats -h' for details.
  status      Show heartbeat status
  user        Perform various user operations: create, delete, change password
  version     Show the immuadmin version

Flags:
  -a, --address string       immudb host address (default "127.0.0.1")
      --certificate string   server certificate file path (default "./tools/mtls/4_client/certs/localhost.cert.pem")
      --clientcas string     clients certificates list. Aka certificate authority (default "./tools/mtls/2_intermediate/certs/ca-chain.cert.pem")
      --config string        config file (default path are configs or $HOME. Default filename is immuadmin.ini)
  -h, --help                 help for immuadmin
  -m, --mtls                 enable mutual tls
      --pkey string          server private key path (default "./tools/mtls/4_client/private/localhost.key.pem")
  -p, --port int             immudb port number (default 3322)
      --servername string    used to verify the hostname on the returned certificates (default "localhost")

Use "immuadmin [command] --help" for more information about a command.

```



### Docker

All services and cli components are also available as docker images on dockerhub.com. 

| Component | Container image                               |
| --------- | --------------------------------------------- |
| immudb    | https://hub.docker.com/r/codenotary/immudb    |
| immugw    | https://hub.docker.com/r/codenotary/immugw    |
| immuadmin | https://hub.docker.com/r/codenotary/immuadmin |

#### Run immudb

```bash
docker run -it -d -p 3322:3322 -p 9497:9497 --name immudb codenotary/immudb:latest
```

#### Run immugw

```
docker run -it -d -p 3323:3323 --name immugw --env IMMUGW_IMMUDBADDRESS=immudb codenotary/immugw:latest
```

#### Run immuadmin

You can either find immuadmin in the immudb container (/usr/local/bin/immuadmin) or run the docker container to connect to the local immudb.

```
docker run -it --rm --name immuadmin codenotary/immuadmin:latest status
```

#### Build the container images yourself

If you want to build the container images yourself, simply clone this repo and run

```
docker build -t myown/immudb:latest -f Dockerfile .
docker build -t myown/immugw:latest -f Dockerfile.immugw .
docker build -t myown/immuadmin:latest -f Dockerfile.immuadmin .
```



### Performance monitoring (Prometheus)

immudb has a built-in prometheus exporter that publishes all metrics at port 9497 (:9497/metrics) by default. When running a Prometheus instance, you can configure the target like in this example:

```
  - job_name: 'immudbmetrics'
    scrape_interval: 60s
    static_configs:
         - targets: ['my-immudb-server:9497']

```

#### Grafana

There is a Grafana dashboard available as well: https://grafana.com/grafana/dashboards/12026

![immudb Grafana dashboard](img/grafana-dashboard.png "immudb Performance dashboard")

## FAQ

| Question                                                     | Answer                                                       | Release date    |
| ------------------------------------------------------------ | ------------------------------------------------------------ | --------------- |
| Where is the Immudb data stored?                             | Files in the working directory                               | initial release |
| How is the data structured?                                  | Data is stored in a Key-Value fashion. Data is always appended and never overwritten, so multiple versions of the same Key-Value can exist and can be inspected (by using the History API). | initial release |
| What kind of data can be stored?                             | Any kind of Key-Value of data, values can be json data, configuration data, etc... Clients can choose how to structure data. | initial release |
| What happens to my data if someone tamperes with it?         | immudb is a tamper-evident history system. When data (or the data history) is being tampered, the DB will not able to produce a valid consistency proof, so each client connect to the db will be able to notice the tampering and notify the user. | initial release |
| How can data be backed up?                                   | immudb provides APIs to perform online backups and restores. Data will be dumped to files. | initial release |
| How can data be restored?                                    | Backups files can easily restored using the built-in client. | initial release |
| Is there a way to incremently backup data?                   | immudb provides stream APIs and data can be streamed in insertion order, that can be easily used to perform incremental backups and incremental restores. | Q3/2020         |
| Is there a way to incremently restore data?                  | (see above)                                                  | Q3/2020         |
| How can the data be replicated to other systems?             | Our goal is to provide a scalable and redundant solution for enterprises. The investigation for the best approach is ongoing and not finalized yet. Our goal is to have it ready shortly after the official enterprise version release | Q3/2020         |
| Would replication stop, when unverifiable data is detected?  | Customers will able to configure the wanted behavior when a unverifiable state is detected across replicas. By default, all valid replicas will able to continue working and replicas with invalid states will be skipped by all clients. | Q3/2020         |
| Somebody changes one value in the database - how can it be detected and reverted? | With replication, it's possible to detect which replica nodes are valid and which are not. If at least a replica node was not tampered data can be easily restored. | Q3/2020         |
| Somebody changes the merkle root entry - how can I recover?  | Each client locally stores the last valid Merkle Tree Root (just 32 bytes of data). When the root of a DB instance is tampered then client will be able to mathematically proof that the provided root is not consistent with the last valid one. If an authenticated backup or a not tampered replica node is available, not-tampered data can be used to recover the Merkle Tree Root to a valid state. | Q3/2020         |
| How is the database protected? outside probes?               | Each client helps in protecting the DB. Special clients (called "agents") can be installed on different systems and continuosly monitor the DB. | Q3/2020         |
| How can I monitor database performance?                      | immudb provides realtime metrics that can be collected using Prometheus | initial release |
| How can I monitor database health?                           | immudb provides realtime healthcheck endpoints via API and immu client | initial release |
| How can I monitor database integrity?                        | immudb provides proof APIs and clients and agents can ask for proof in realtime. | initial release |
| How can I monitor database integrity for single objects or specific entries? | immu client has a functionality to authenticate a specific entry at a given point in time. So both last version and the whole history of an item can be verified. | initial release |
| Can I build and distribute an immudb that skips the verification? If yes, how to avoid that? | CodeNotary team notarizes sources and releases of all immudb components. Only one authentic client notarization by the CodeNotary team required to detect fake installations or any kind of tampering. | initial release |
| How many databases can I run on a single immudb server?      | We currently support one database, but in future releases there will be support for many databases. | Q3/2020         |

## Community

We welcome [contributions](CONTRIBUTING.md). Feel free to join the team!

To report bugs or get help, use [GitHub's issues](https://github.com/codenotary/immudb/issues).


## License

immudb is [Apache v2.0 License](LICENSE).

immudb re-distributes other open-source tools and libraries. (++add repo list++)


## Is it awesome?

Yes.


