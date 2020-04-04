<!--
---
title: "immudb"
custom_edit_url: https://github.com/codenotary/immudb/edit/master/README.md
---
-->

# immudb [![Build Status](https://travis-ci.com/codenotary/immudb.svg?branch=master)](https://travis-ci.com/codenotary/immudb) [![License](https://img.shields.io/github/license/codenotary/immudb)](LICENSE)

[![Go Report Card](https://goreportcard.com/badge/github.com/codenotary/immudb)](https://goreportcard.com/report/github.com/codenotary/immudb)


immudb is **lightweight, high-speed immutable database** for systems and applications. With immmudb you can
track changes in sensitive data in your transactional databases and then record those changes indelibly in a the 
tamperproof immudb database. This allows you to keep an indelible history of, say, your debit/credit transactions. 

Traditional transaction logs are hard to scale, and are not immutable. So there is no way to know for sure if your data has been compromised. 

As such immudb provides **unparalleled insights** **retro-actively**, of what happened to your sensitive data, even
if your perimiter was compromised. immudb provides the guarantatee of immutability by using internally a **Merkle tree structure**. 

immudb gives you the same **cyrptographic verification** of the integrity of data written with **SHA-256** like classic blockhain without the cost and complexity associated with blockhains today. 

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
3.  [Quickstart](#quickstart) - How to try it now on your systems, get a Docket container running in seconds
4.  [Why immudb](#why-immudb) - Why people love immudb and how it compares with other solutions
5.  [News](#news) - The latest news about immudb
6.  [How immudb works](#how-immudb-works) - A high-level diagram of how immudb works
7.  [Features](#features) - How you'll use immudb on your systems
8.  [Read world examples](#examples) - Read about how others use immudb
9.  [Documentation](#documentation) - Read the documentation
10.  [Community](#community) - Discuss immudb with others and get support
11. [License](#license) - Check immudb's licencing
12. [Is it awesome?](#is-it-awesome) - Yes.

## What does it look like?

## User base

### Docker pulls

We provide Docker images for the most common architectures. These are statistics reported by Docker Hub:

[![codenotary/immudb
(official)](https://img.shields.io/docker/pulls/codenotary/immudb.svg?label=codenotary/immudb+%28official%29)](https://hub.docker.com/r/codenotary/immudb/)

## Community

We welcome [contributions](CONTRIBUTING.md). Feel free to join the team!

To report bugs or get help, use [GitHub's issues](https://github.com/codenotary/immudb/issues).


## License

immudb is [Apache v2.0 License](LICENSE).

immudb re-distributes other open-source tools and libraries. (++add repo list++)


## Is it awesome?

Yes.
