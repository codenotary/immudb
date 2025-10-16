<!--
---

title: "immudb"

custom_edit_url: https://github.com/codenotary/immudb/edit/master/README.md
---

-->

# immudb <img align="right" src="img/Black%20logo%20-%20no%20background.png" height="47px" />


[![Documentation](https://img.shields.io/website?label=Docs&url=https%3A%2F%2Fdocs.immudb.io%2F)](https://docs.immudb.io/)
[![Build Status](https://github.com/codenotary/immudb/actions/workflows/push.yml/badge.svg)](https://github.com/codenotary/immudb/actions/workflows/push.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/codenotary/immudb)](https://goreportcard.com/report/github.com/codenotary/immudb)
[![View SBOM](https://img.shields.io/badge/sbom.sh-viewSBOM-blue?link=https%3A%2F%2Fsbom.sh%2F37cbffcf-1bd3-4daf-86b7-77deb71575b7)](https://sbom.sh/37cbffcf-1bd3-4daf-86b7-77deb71575b7)
[![Homebrew](https://img.shields.io/homebrew/v/immudb)](https://formulae.brew.sh/formula/immudb)

[![Discord](https://img.shields.io/discord/831257098368319569)](https://discord.gg/EWeCbkjZVu)
[![Immudb Careers](https://img.shields.io/badge/careers-We%20are%20hiring!-blue?style=flat)](https://www.codenotary.com/job)
[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/codenotary)](https://artifacthub.io/packages/search?repo=codenotary)

Don't forget to ⭐ this repo if you like immudb!

[:tada: 27M pulls from docker hub!](https://hub.docker.com/r/codenotary)

---

Detailed documentation can be found at https://docs.immudb.io/

---

<img align="right" src="img/immudb-mascot-small.png" width="256px"/>

immudb is a database with built-in cryptographic proof and verification. It tracks changes in sensitive data and the integrity of the history will be protected by the clients, without the need to trust the database. It can operate as a key-value store, as a document model database, and/or as relational database (SQL).

Traditional database transactions and logs are mutable, and therefore there is no way to know for sure if your data has been compromised. immudb is immutable. You can add new versions of existing records, but never change or delete records. This lets you store critical data without fear of it being tampered.

Data stored in immudb is cryptographically coherent and verifiable. Unlike blockchains, immudb can handle millions of transactions per second, and can be used both as a lightweight service or embedded in your application as a library. immudb runs everywhere, on an IoT device, your notebook, a server, on-premise or in the cloud.


When used as a relational data database, it supports both transactions and blobs, so there are no limits to the use cases. Developers and organizations use immudb to secure and tamper-evident log data, sensor data, sensitive data, transactions, software build recipes, rule-base data, artifacts and even video streams. [Examples of organizations using immudb today.](https://www.immudb.io)

## Contents

- [immudb](#immudb)
  - [Contents](#contents)
  - [Quickstart](#quickstart)
    - [Getting immudb running: executable](#getting-immudb-running-executable)
    - [Getting immudb running: docker](#getting-immudb-running-docker)
    - [Getting immudb running: kubernetes](#getting-immudb-running-kubernetes)
    - [Using subfolders](#using-subfolders)
    - [Enabling Amazon S3 storage](#enabling-amazon-s3-storage)
    - [Connecting with immuclient](#connecting-with-immuclient)
    - [Using immudb](#using-immudb)
      - [Real world examples](#real-world-examples)
      - [How to integrate immudb in your application](#how-to-integrate-immudb-in-your-application)
      - [Online demo environment](#online-demo-environment)
  - [Tech specs](#tech-specs)
  - [Performance figures](#performance-figures)
  - [Roadmap](#roadmap)
  - [Projects using immudb](#projects-using-immudb)
  - [Contributing](#contributing)

## Quickstart


### Getting immudb running: executable

You may download the immudb binary from [the latest releases on Github](https://github.com/codenotary/immudb/releases/latest). Once you have downloaded immudb, rename it to `immudb`, make sure to mark it as executable, then run it. The following example shows how to obtain v1.9.5 for linux amd64:

```bash
wget https://github.com/codenotary/immudb/releases/download/v1.9.5/immudb-v1.9.5-linux-amd64
mv immudb-v1.9.5-linux-amd64 immudb
chmod +x immudb

# run immudb in the foreground to see all output
./immudb

# or run immudb in the background
./immudb -d
```

### Getting immudb running: Docker

Use Docker to run immudb in a ready-to-use container:

```bash
docker run -d --net host -it --rm --name immudb codenotary/immudb:latest
```

If you are running the Docker image without host networking, make sure to expose ports 3322 and 9497.

### Getting immudb running: Kubernetes

In kubernetes, use helm for an easy deployment: just add our repository and install immudb with these simple commands:

```bash
helm repo add immudb https://packages.codenotary.org/helm
helm repo update
helm install immudb/immudb --generate-name
```

### Using subfolders

Immudb helm chart creates a persistent volume for storing immudb database.
Those database are now by default placed in a subdirectory.

That's for compatibility with ext4 volumes that have a `/lost+found` directory that can confuse immudb. Some volume providers,
like EBS or DigitalOcean, are using this kind of volumes. If we placed database directory on the root of the volume,
that `/lost+found` would be treated as a database. So we now create a subpath (usually `immudb`) subpath for storing that.

This is different from what we did on older (<=1.3.1) helm charts, so if you have already some volumes with data you can set
value volumeSubPath to false (i.e.: `--set volumeSubPath.enabled=false`) when upgrading so that the old way is used.

You can alternatively migrate the data in a `/immudb` directory. You can use this pod as a reference for the job:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: migrator
spec:
  volumes:
    - name: "vol0"
      persistentVolumeClaim:
        claimName: <your-claim-name-here>
  containers:
    - name: migrator
      image: busybox
      volumeMounts:
        - mountPath: "/data"
          name: "vol0"
      command:
        - sh
        - -c
        - |
          mkdir -p /data/immudb
          ls /data | grep -v -E 'immudb|lost\+found'|while read i; do mv /data/$i /data/immudb/$i; done
```

As said before, you can totally disable the use of subPath by setting `volumeSubPath.enabled=false`.
You can also tune the subfolder path using `volumeSubPath.path` value, if you prefer your data on a
different directory than the default `immudb`.

### Enabling Amazon S3 storage

immudb can store its data in the Amazon S3 service (or a compatible alternative).
The following example shows how to run immudb with the S3 storage enabled:

```bash
export IMMUDB_S3_STORAGE=true
export IMMUDB_S3_ACCESS_KEY_ID=<S3 ACCESS KEY ID>
export IMMUDB_S3_SECRET_KEY=<SECRET KEY>
export IMMUDB_S3_BUCKET_NAME=<BUCKET NAME>
export IMMUDB_S3_LOCATION=<AWS S3 REGION>
export IMMUDB_S3_PATH_PREFIX=testing-001
export IMMUDB_S3_ENDPOINT="https://${IMMUDB_S3_BUCKET_NAME}.s3.${IMMUDB_S3_LOCATION}.amazonaws.com"

./immudb
```

When working with the external storage, you can enable the option for the remote storage
to be the primary source of identifier. This way, if immudb is run using ephemeral disks,
such as with AWS ECS Fargate, the identifier can be taken from S3. To enable that, use:

```bash
export IMMUDB_S3_EXTERNAL_IDENTIFIER=true
```

You can also use the role-based credentials for more flexible and secure configuration.
This allows the service to be used with instance role configuration without a user entity.
The following example shows how to run immudb with the S3 storage enabled using AWS Roles:

```bash
export IMMUDB_S3_STORAGE=true
export IMMUDB_S3_ROLE_ENABLED=true
export IMMUDB_S3_BUCKET_NAME=<BUCKET NAME>
export IMMUDB_S3_LOCATION=<AWS S3 REGION>
export IMMUDB_S3_PATH_PREFIX=testing-001
export IMMUDB_S3_ENDPOINT="https://${IMMUDB_S3_BUCKET_NAME}.s3.${IMMUDB_S3_LOCATION}.amazonaws.com"

./immudb
```

If using Fargate, the credentials URL can be sourced automatically:

```bash
export IMMUDB_S3_USE_FARGATE_CREDENTIALS=true
```

Optionally, you can specify the exact role immudb should be using with:

```bash
export IMMUDB_S3_ROLE=<AWS S3 ACCESS ROLE NAME>
```

Remember, the `IMMUDB_S3_ROLE_ENABLED` parameter still should be on.

You can also easily use immudb with compatible s3 alternatives
such as the [minio](https://github.com/minio/minio) server:

```bash
export IMMUDB_S3_ACCESS_KEY_ID=minioadmin
export IMMUDB_S3_SECRET_KEY=minioadmin
export IMMUDB_S3_STORAGE=true
export IMMUDB_S3_BUCKET_NAME=immudb-bucket
export IMMUDB_S3_PATH_PREFIX=testing-001
export IMMUDB_S3_ENDPOINT="http://localhost:9000"

# Note: This spawns a temporary minio server without data persistence
docker run -d -p 9000:9000 minio/minio server /data

# Create the bucket - this can also be done through web console at http://localhost:9000
docker run --net=host -it --entrypoint /bin/sh minio/mc -c "
  mc alias set local http://localhost:9000 minioadmin minioadmin &&
  mc mb local/${IMMUDB_S3_BUCKET_NAME}
"

# Run immudb instance
./immudb
```

### Connecting with immuclient

You may download the immuclient binary from [the latest releases on Github](https://github.com/codenotary/immudb/releases/latest). Once you have downloaded immuclient, rename it to `immuclient`, make sure to mark it as executable, then run it. The following example shows how to obtain v1.5.0 for linux amd64:

```bash
wget https://github.com/codenotary/immudb/releases/download/v1.5.0/immuclient-v1.5.0-linux-amd64
mv immuclient-v1.5.0-linux-amd64 immuclient
chmod +x immuclient

# start the interactive shell
./immuclient

# or use commands directly
./immuclient help
```

Or just use Docker to run immuclient in a ready-to-use container. Nice and simple.

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
- store the last known positions of submarines
- record the location where fish was found aboard fishing trawlers

### How to integrate immudb in your application

We have SDKs available for the following programming languages:

1. Java [immudb4j](https://github.com/codenotary/immudb4j)
2. Golang ([golang sdk](https://pkg.go.dev/github.com/codenotary/immudb/pkg/client), [Gorm adapter](https://github.com/codenotary/immugorm))
3. .net [immudb4net](https://github.com/codenotary/immudb4net)
4. Python [immudb-py](https://github.com/codenotary/immudb-py)
5. Node.js [immudb-node](https://github.com/codenotary/immudb-node)

To get started with development, there is a [quickstart in our documentation](https://docs.immudb.io/master/immudb.html): or pick a basic running sample from [immudb-client-examples](https://github.com/codenotary/immudb-client-examples).

Our [immudb Playground](https://play.codenotary.com) provides a guided environment to learn the Python SDK.

<div align="center">
  <a href="https://play.codenotary.com">
    <img alt="immudb playground to start using immudb in an online demo environment" src="img/playground2.png"/>
  </a>
</div>

We've developed a "language-agnostic SDK" which exposes a REST API for easy consumption by any application.
[immugw](https://github.com/codenotary/immugw) may be a convenient tool when SDKs are not available for the
programming language you're using, for experimentation, or just because you prefer your app only uses REST endpoints.

### Online demo environment

Click here to try out the immudb web console access in an [online demo environment](https://demo.immudb.io) (username: immudb; password: immudb)

<div align="center">
  <a href="https://demo.immudb.io">
    <img alt="Your own temporary immudb web console access to start using immudb in an online demo environment" src="img/demoimmudb.png"/>
  </a>
</div>

## Tech specs

| Topic                   | Description                                         |
| ----------------------- | --------------------------------------------------- |
| DB Model                | Key-Value with 3D access, Document Model, SQL       |
| Data scheme             | schema-free                                         |
| Implementation design   | Cryptographic commit log with parallel Merkle Tree, |
|                         | (sync/async) indexing with extended B-tree          |
| Implementation language | Go                                                  |
| Server OS(s)            | BSD, Linux, OS X, Solaris, Windows, IBM z/OS        |
| Embeddable              | Yes, optionally                                     |
| Server APIs             | gRPC                                                |
| Partition methods       | Sharding                                            |
| Consistency concepts    | Immediate Consistency                               |
| Transaction concepts    | ACID with Snapshot Isolation (SSI)                  |
| Durability              | Yes                                                 |
| Snapshots               | Yes                                                 |
| High Read throughput    | Yes                                                 |
| High Write throughput   | Yes                                                 |
| Optimized for SSD       | Yes                                                 |

## Performance figures

immudb can handle millions of writes per second. The following table shows performance of the embedded store inserting 1M entries on a machine with 4-core E3-1275v6 CPU and SSD disk:

| Entries | Workers | Batch | Batches | time (s) | Entries/s |
| ------- | ------- | ----- | ------- | -------- | --------- |
| 1M      | 20      | 1000  | 50      | 1.061    | 1.2M /s   |
| 1M      | 50      | 1000  | 20      | 0.543    | 1.8M /s   |
| 1M      | 100     | 1000  | 10      | 0.615    | 1.6M /s   |

You can generate your own benchmarks using the `stress_tool` under `embedded/tools`.

## Roadmap

The following topics are important to us and are planned or already being worked on:

* Data pruning
* Compression
* compatibility with other database storage files
* Easier API for developers
* API compatibility with other, well-known embedded databases

## Projects using immudb

Below is a list of known projects that use immudb:

- [alma-sbom](https://github.com/AlmaLinux/alma-sbom) - AlmaLinux OS SBOM data management utility.

- [immudb-log-audit](https://github.com/codenotary/immudb-log-audit) - A service and cli tool to store json formatted log input
  and audit it later in immudb Vault.

- [immudb-operator](https://github.com/unagex/immudb-operator) - Unagex Kubernetes Operator for immudb.

- [immufluent](https://github.com/codenotary/immufluent) - Send fluentbit collected logs to immudb.

- [immuvoting](https://github.com/padurean/immuvoting) - Publicly cryptographically verifiable electronic voting system powered by immudb.

Are you using immudb in your project? Open a pull request to add it to the list.

## Contributing

We welcome [contributors](CONTRIBUTING.md). Feel free to join the team!

<a href="https://github.com/codenotary/immudb/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=codenotary/immudb" />
</a>

Learn how to [build](BUILD.md) immudb components in both binary and Docker image form.

To report bugs or get help, use [GitHub's issues](https://github.com/codenotary/immudb/issues).

immudb is licensed under the [Business Source License 1.1](LICENSE).

immudb re-distributes other open-source tools and libraries - [Acknowledgements](ACKNOWLEDGEMENTS.md).
