# Getting started with immudb development

### Getting started with immudb Development <a href="#getting-started-with-immudb-development" id="getting-started-with-immudb-development"></a>

This guide provides developers with the first steps of using immudb from their application and from their favourite programming language:

* Connect to the database
* Insert and retrieve data

### Clients <a href="#clients" id="clients"></a>

In the most common scenario, you would perform write and read operations on the database talking to the server. In this case your application will be a client to immudb.

### SDKs <a href="#sdks" id="sdks"></a>

The immudb server manages the requests from the outside world to the store. In order to insert or retrieve data, you need to talk with the server.

SDKs make it comfortable to talk to the server from your favourite language, without having to deal with details about how to talk to it.

The most well-known immudb SDK is written in [Golang (opens new window)](https://golang.org), but there are SDKs available for Python, NodeJS, Java and others.

For other unsupported programming languages, immugw provides a REST gateway that can be used to talk to the server via generic HTTP.

### Getting immudb running <a href="#getting-immudb-running" id="getting-immudb-running"></a>

You may download the immudb binary from [the latest releases on Github (opens new window)](https://github.com/codenotary/immudb/releases/latest). Once you have downloaded immudb, rename it to `immudb`, make sure to mark it as executable, then run it. The following example shows how to obtain v1.0.0 for linux amd64:

Alternatively, you may use Docker to run immudb in a ready-to-use container. In a terminal type:

(you can add the `-d --rm --name immudb` options to send it to the background).

### Connecting from your programming language <a href="#connecting-from-your-programming-language" id="connecting-from-your-programming-language"></a>

#### Importing the SDK <a href="#importing-the-sdk" id="importing-the-sdk"></a>

In order to use the SDK, you need to download and import the libraries:

#### Connection and authentication <a href="#connection-and-authentication" id="connection-and-authentication"></a>

The first step is to connect to the database, which listens by default in port 3322 and authenticate using the default user and password (`immudb / immudb`):

> Note: You can change the server default options using environment variables, flags or the `immudb.toml` configuration file.

#### Tamperproof read and write <a href="#tamperproof-read-and-write" id="tamperproof-read-and-write"></a>

You can write with built-in cryptographic verification. The client implements the mathematical validations, while your application uses a traditional read or write function.

### SQL Operations with the Go SDK <a href="#sql-operations-with-the-go-sdk" id="sql-operations-with-the-go-sdk"></a>

In order to use SQL from the Go SDK, you create a immudb client and login to the server like usual. First make sure you import:

Then you can create the client and open a new session to the database:

To perform SQL statements, use the `SQLExec` function, which takes a `SQLExecRequest` with a SQL operation:

This is also how you perform inserts:

Once you have data in the database, you can use the `SQLQuery` method of the client to query.

Both `SQLQuery` and `SQLExec` allows named parameters. Just encode them as `@param` and pass `map[string]{}interface` as values:

`res` is of the type `*schema.SQLQueryResult`. In order to iterate over the results, you iterate over `res.Rows`. On each iteration, the row `r` will have a member `Values`, which you can iterate to get each column.

#### Additional resources <a href="#additional-resources" id="additional-resources"></a>

* Get the [immudb-client-example code (opens new window)](https://github.com/codenotary/immudb-client-examples)
