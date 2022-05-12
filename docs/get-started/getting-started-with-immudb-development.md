# Getting started with immudb development

### Getting started with immudb Development <a href="#getting-started-with-immudb-development" id="getting-started-with-immudb-development"></a>

This guide provides developers with the first steps for using immudb from your applications and with your favorite programming language:

* Connect to the database
* Insert and retrieve data

{% hint style="info" %}
To learn how to develop for immudb with Python in a guided online environment, visit the immudb Playground at <https://play.codenotary.com>
{% endhint %}

## SDKs

In the most common scenario, you would perform write and read operations on the database talking to the server. In this case your application will be a client to immudb.

SDKs make it comfortable to talk to the server from your favourite language, without having to deal with details about how to talk to it.

The most well-known and recommended immudb SDK is written in [Golang](https://golang.org/), but there are other SDKs available, both maintainer by the internal team and by the community.


| Language         | Maintainer | Immdb version | link | Notes                                                                              |
|-------------------|---------|------------------|-------------|-----------------------------------------------------------|
| `go`               | immudb team  | 1.2.1       |     [link](https://pkg.go.dev/github.com/codenotary/immudb/pkg/client)  |                                   |
| `python`               | immudb team  | 1.2.1       |  [link](https://github.com/codenotary/immudb-py) | Verification is not working                                    |
| `JAVA`               | immudb team  | 1.2.1       |   [link](https://github.com/codenotary/immudb4j)  | Verification is not working                                      |
| `.NET`               | immudb team  | 1.2.1       |   [link](https://github.com/codenotary/immudb4dotnet)  | Verification is not working                                      |
| `NODE`               | immudb team | 1.2.1       |   [link](https://github.com/codenotary/immudb-node) | Verification is not working                 |
| `JS`               | immudb team | 1.2.1       |   [link](https://github.com/codenotary/immu-js) | Verification is not working                 |
| `ruby`               | Community ([Ankane](https://github.com/ankane))  | 1.2.1       |   [link](https://github.com/ankane/immudb-ruby) |Verification is not working                 |


For other unsupported programming languages, [immugw](/master/immugw/) provides a REST gateway that can be used to talk to the server via generic HTTP.
 

The immudb server manages the requests from the outside world to the store. In order to insert or retrieve data, you need to talk with the server.

<div class="wrapped-picture">

![SDK Architecture](../.gitbook/assets/immudb-server.svg)

</div>

</WrappedSection>


## Connecting from your programming language

### Importing the SDK

In order to use the SDK, you need to download and import the libraries:

:::: tabs

::: tab Go

```shell script
# Make sure your project is using Go Modules
go mod init example.com/hello
go get github.com/codenotary/immudb/pkg/client
```

```go
// Then import the package
import (
    immudb "github.com/codenotary/immudb/pkg/client"
)
 ```

:::

::: tab Java
Just include immudb4j as a dependency in your project:

if using `Maven`:

```xml
    <dependency>
        <groupId>io.codenotary</groupId>
        <artifactId>immudb4j</artifactId>
        <version>0.9.0.6</version>
    </dependency>
```

if using `Gradle`:

```groovy
    compile 'io.codenotary:immudb4j:0.9.0.6'
```

[Java SDK repository](https://github.com/codenotary/immudb4j)

immudb4j is currently hosted on both [Maven Central] and [Github Packages].

[Github Packages]: https://github.com/orgs/codenotary/packages?repo_name=immudb4j
[Maven Central]: https://search.maven.org/artifact/io.codenotary/immudb4j
:::

::: tab Python
Install the package using pip:

```shell
    pip3 install immudb-py
```

 Then import the client as follows:

```python
    from immudb import ImmudbClient
```

*Note*: immudb-py need `grpcio` module from google. On Alpine linux, you need
 these packages in order to correctly build (and install) grpcio:

* `linux-headers`
* `python3-dev`
* `g++`

[Python SDK repository](https://github.com/codenotary/immudb-py)

:::

::: tab Node.js

Install the package using npm:

```shell
    npm install immudb-node
```
Codenotary
Include the immudb-node as a dependency in your project.

```javascript
 const immudbClient = require('immudb-node')
```
Codenotary
[Node.js SDK repository](https://github.com/codenotary/immudb-node)

:::

::: tab .Net

Use Microsoft's [NuGet](https://www.nuget.org/packages/Immudb4DotNet/) package manager to get immudb4DotNet.

Creating a Client.

* Using the default configuration.

 ```csharp
   var client = new CodeNotary.ImmuDb.ImmuClient("localhost"))
 ```

* The immudb implements IDisposable, so you can wrap it with "using".

 ```csharp
 using (var client = new CodeNotary.ImmuDb.ImmuClient("localhost", 3322)){}
 ```

 [.Net SDK repository](https://github.com/codenotary/immudb4dotnet)
:::

::: tab Others
If you're using another language, then read up on our [immugw](/master/immugw/) option.
:::

::::

### Connection and authentication

The first step is to connect to the database, which listens by default in port 3322 and authenticate using the default user and password (`immudb / immudb`):

>Note: You can [change the server default options](reference/configuration.md) using environment variables, flags or the `immudb.toml` configuration file.

:::: tabs

::: tab Go

```go
import (
 "log"
 "context"
 immudb "github.com/codenotary/immudb/pkg/client"
)

opts := immudb.DefaultOptions().
            WithAddress("localhost").
            WithPort(3322)

client := immudb.NewClient().WithOptions(opts)
err := client.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
if err != nil {
    log.Fatal(err)
}

defer client.CloseSession(context.TODO())

// do amazing stuff
```

:::

::: tab Java

```java
client = ImmuClient.newBuilder()
    .withServerUrl("localhost")
    .withServerPort(3322)
    .build();
client.login("immudb", "immudb");
```

:::

::: tab Python

```python
from immudb.client import ImmudbClient
ic=ImmudbClient()
ic.login("immudb","immudb")
```

:::

::: tab Node.js

```javascript
const ImmudbClient = require('immudb-node');

const cl = new ImmudbClient();

(async () => {
  try {
    const loginReq = { user: 'immudb', password: 'immudb' }
    const loginRes = await cl.login(loginReq)
// ...
} catch (err) {
    console.log(err)
  }
})()
```

You can also use exported types in your TypeScript projects:

```typescript
import ImmudbClient from 'immudb-node'
import Parameters from 'immudb-node/types/parameters'

const cl = new ImmudbClient();

(async () => {
  try {
    const loginReq: Parameters.Login = { user: 'immudb', password: 'immudb' }
    const loginRes = await cl.login(loginReq)
// ...
} catch (err) {
    console.log(err)
  }
})()
```

:::

::: tab .Net
This feature is not yet supported or not documented.
Do you want to make a feature request or help out? Open an issue on [.Net sdk github project](https://github.com/codenotary/immudb4dotnet/issues/new)
:::

::: tab Others
If you're using another development language, please refer to the [immugw](/master/immugw/) option.
:::

::::

### Tamperproof read and write

:::: tabs

You can write with built-in cryptographic verification. The client implements the mathematical validations, while your application uses a traditional read or write function.

::: tab Go

```go
vtx, err := client.VerifiedSet(ctx, []byte(`hello`), []byte(`immutable world`))
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Set and verified key '%s' with value '%s' at tx %d\n", []byte(`hello`), []byte(`immutable world`), vtx.Id)

ventry, err := client.VerifiedGet(ctx, []byte(`hello`))
if err != nil {
    log.Fatal(err)
}
 
fmt.Printf("Sucessfully verified key '%s' with value '%s' at tx %d\n", ventry.Key, ventry.Value, ventry.Tx)
```

:::

::: tab Java
This feature is not yet supported or not documented.
Do you want to make a feature request or help out? Open an issue on [Java sdk github project](https://github.com/codenotary/immudb4j/issues/new)
:::

::: tab Python
This feature is not yet supported or not documented.
Do you want to make a feature request or help out? Open an issue on [Python sdk github project](https://github.com/codenotary/immudb-py/issues/new)
:::

::: tab Node.js
This feature is not yet supported or not documented.
Do you want to make a feature request or help out? Open an issue on [Node.js sdk github project](https://github.com/codenotary/immudb-node/issues/new)
:::

::: tab .Net
This feature is not yet supported or not documented.
Do you want to make a feature request or help out? Open an issue on [.Net sdk github project](https://github.com/codenotary/immudb4dotnet/issues/new)
:::

::: tab Others
If you're using another development language, please refer to the [immugw](/master/immugw/) option.
:::

::::

<WrappedSection>

## SQL Operations with the Go SDK

In order to use SQL from the Go SDK, you create a immudb client and login to the server like usual. First make sure you import:

```
"github.com/codenotary/immudb/pkg/api/schema"
"github.com/codenotary/immudb/pkg/client"
```

Then you can create the client and open a new session to the database:

```go
import (
"log"
"context"
immudb "github.com/codenotary/immudb/pkg/client"
)

c := immudb.NewClient()
err := c.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
if err != nil {
    log.Fatal(err)
}

defer c.CloseSession(context.TODO())

// do amazing stuff
```

To perform SQL statements, use the `SQLExec` function, which takes a `SQLExecRequest` with a SQL operation:

```go
 _, err = c.SQLExec(ctx, `
  BEGIN TRANSACTION
          CREATE TABLE people(id INTEGER, name VARCHAR, salary INTEGER, PRIMARY KEY id);
          CREATE INDEX ON people(name)
  COMMIT
 `, map[string]interface{}{})
  if err != nil {
  log.Fatal(err)
 }
```

This is also how you perform inserts:

```go
_, err = c.SQLExec(ctx, "UPSERT INTO people(id, name, salary) VALUES (@id, @name, @salary);", map[string]interface{}{"id": 1, "name": "Joe", "salary": 1000})
if err != nil {
    log.Fatal(err)
}
```

Once you have data in the database, you can use the `SQLQuery` method of the client to query.

Both `SQLQuery` and `SQLExec` allows named parameters. Just encode them as `@param` and pass `map[string]{}interface` as values:

```go
res, err := c.SQLQuery(ctx, "SELECT t.id as d,t.name FROM (people AS t) WHERE id <= 3 AND name = @name", map[string]interface{}{"name": "Joe"}, true)
if err != nil {
    log.Fatal(err)
}
```

`res` is of the type `*schema.SQLQueryResult`. In order to iterate over the results, you iterate over `res.Rows`. On each iteration, the row `r` will have a member `Values`, which you can iterate to get each column.

```go
for _, r := range res.Rows {
    for _, v := range r.Values {
        log.Printf("%s\n", schema.RenderValue(v.Value))
    }
}
```

### Additional resources

* Get the [immudb-client-example code](https://github.com/codenotary/immudb-client-examples)

</WrappedSection>

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
