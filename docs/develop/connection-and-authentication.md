# Connection and authentication

immudb runs on port 3323 as the default. The code samples below illustrate how to connect your client to the server and authenticate using default options and the default username and password. You can modify defaults on the immudb server in `immudb.toml` in the config folder.

#### Mutual TLS <a href="#mutual-tls" id="mutual-tls"></a>

To enable mutual authentication, a certificate chain must be provided to both the server and client. That will cause each to authenticate with the other simultaneously. In order to generate certs, use the openssl tool: [generate.sh (opens new window)](https://github.com/codenotary/immudb/tree/master/tools/mtls).

This generates a list of folders containing certificates and private keys to set up a mTLS connection.

#### Disable authentication. Deprecated <a href="#disable-authentication-deprecated" id="disable-authentication-deprecated"></a>

You also have the option to run immudb with authentication disabled. However, without authentication enabled, it's not possible to connect to a server already configured with databases and user permissions. If a valid token is present, authentication is enabled by default.
