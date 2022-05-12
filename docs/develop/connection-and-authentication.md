# Connection and authentication

immudb runs on port 3323 as the default. The code samples below illustrate how to connect your client to the server and authenticate using default options and the default username and password. You can modify defaults on the immudb server in `immudb.toml` in the config folder.

[//]: # "need to be fixed for code snippets/formatting"

:::: tabs

::: tab Go

```go
import (
  "context"
  immudb "github.com/codenotary/immudb/pkg/client"
  "log"
)
```

```go
client:= immudb.NewClient()
err := client.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
if err != nil {
    log.Fatal(err)
}

defer client.CloseSession(context.TODO())

// do amazing stuff
```

The server address and port can be set in client options as follows:
```go
opts := immudb.DefaultOptions().WithAddress("localhost").WithPort(3322)
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

Under the hood, during `login`, a token is being retrieved from the server,
stored in memory and reused for subsequent operations.

The state is internally used for doing _verified_ operations (such as verifiedSet or verifiedGet).

```java
// Setting the "store" where the internal states are being persisted.
FileImmuStateHolder stateHolder = FileImmuStateHolder.newBuilder()
            .withStatesFolder("immu_states")
            .build();

// Creating an new ImmuClient instance.
ImmuClient immuClient = ImmuClient.newBuilder()
            .withStateHolder(stateHolder)
            .withServerUrl("localhost")
            .withServerPort(3322)
            .build();

// Login with default credentials.
immuClient.login("immudb", "immudb");
```

:::

::: tab Python

```python
from grpc import RpcError
from immudb import ImmudbClient

URL = "localhost:3322"  # immudb running on your machine
LOGIN = "immudb"        # Default username
PASSWORD = "immudb"     # Default password
DB = b"defaultdb"       # Default database name (must be in bytes)


def main():
    client = ImmudbClient(URL)
    # database parameter is optional
    client.login(LOGIN, PASSWORD, database=DB)
    client.logout()

    # Bad login
    try:
        client.login("verybadlogin", "verybadpassword")
    except RpcError as exception:
        print(exception.debug_error_string())
        print(exception.details())


if __name__ == "__main__":
    main()

```
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

#### Mutual TLS <a href="#mutual-tls" id="mutual-tls"></a>

To enable mutual authentication, a certificate chain must be provided to both the server and client. This will allow each to authenticate with the other simultaneously. In order to generate certs, use the openssl tool: [generate.sh (opens in new window)](https://github.com/codenotary/immudb/tree/master/tools/mtls).

```bash
./generate.sh localhost mysecretpassword
```

This generates a list of folders containing certificates and private keys to set up a mTLS connection.

:::: tabs

::: tab Go

```go
opts := immudb.DefaultOptions().
		WithMTLs(true).
		WithMTLsOptions(
			immudb.MTLsOptions{}.
                WithCertificate("{path-to-immudb-src-folder}/tools/mtls/4_client/certs/localhost.cert.pem").
				WithPkey("{path-to-immudb-src-folder}/tools/mtls/4_client/private/localhost.key.pem").
				WithClientCAs("{path-to-immudb-src-folder}/tools/mtls/2_intermediate/certs/ca-chain.cert.pem").
				WithServername("localhost"),
		)

client := immudb.NewClient().WithOptions(opts)

// do amazing stuff
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

```ts
import ImmudbClient from 'immudb-node'
import Parameters from 'immudb-node/types/parameters'

const IMMUDB_HOST = '127.0.0.1'
const IMMUDB_PORT = '3322'
const IMMUDB_USER = 'immudb'
const IMMUDB_PWD = 'immudb'

const cl = new ImmudbClient({ host: IMMUDB_HOST, port: IMMUDB_PORT });

(async () => {
 const loginReq: Parameters.Login = { user: IMMUDB_USER, password: IMMUDB_PWD }
 const loginRes = await cl.login(loginReq)
 console.log('success: login:', loginRes)
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

#### Disable authentication. Deprecated <a href="#disable-authentication-deprecated" id="disable-authentication-deprecated"></a>

### Disable authentication. Deprecated

You also have the option to run immudb with authentication disabled. However, without authentication enabled, it's not possible to connect to a server already configured with databases and user permissions. If a valid token is present, authentication is enabled by default.

:::: tabs

::: tab Go

```go
    client, err := c.NewImmuClient(
  c.DefaultOptions().WithAuth(false),
 )
 if err != nil {
  log.Fatal(err)
 }
 vi, err := client.VerifiedSet(ctx, []byte(`immudb`), []byte(`hello world`))
 if  err != nil {
  log.Fatal(err)
 }
```

:::

::: tab Java

```java
FileImmuStateHolder stateHolder = FileImmuStateHolder.newBuilder()
            .withStatesFolder("immu_states")
            .build();

ImmuClient immuClient = ImmuClient.newBuilder()
            .withStateHolder(stateHolder)
            .withServerUrl("localhost")
            .withServerPort(3322)
            .withAuth(false) // No authentication is needed.
            .build();
try {
    immuClient.set(key, val);
} catch (CorruptedDataException e) {
    // ...
}
```

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