# Concepts

Download the [immudb short research paper (opens in new window)](https://codenotary.com/technologies/immudb/) to learn about the technical foundations of immudb.

[//]: # "update this to include missing content"

### Adding data <a href="#adding-data" id="adding-data"></a>

This section is not yet ready.

### Checking data consistency <a href="#checking-data-consistency" id="checking-data-consistency"></a>

This section is not yet ready.

### State signature <a href="#state-signature" id="state-signature"></a>

Providing `immudb` with a signing key enables the cryptographic state signature. That means that an auditor or a third party client, for instance, could verify the authenticity of the returned current state after calling the `currentState` gRPC method.

Here are the gRPC message definitions:

```go
message ImmutableState {
	uint64 txId = 3;
	bytes txHash = 4;
	Signature signature = 5;
}

message Signature {
	bytes signature = 1;
	bytes publicKey = 2;
}
```

Check [state signature](https://docs.immudb.io/master/old/immudb/#state-signature) and [verify state signature](https://docs.immudb.io/master/sdks-api.html#verify-state-signature) paragraphs for additional details.

Immuclient and [immugw (opens new window)](https://github.com/codenotary/immugw) are shipped with auditor capabilities. To get the signed state in combination with the auditor, launch...

* ...immuclient with auditor capabilities:

```bash
immuclient audit-mode --audit-username {immudb-username} --audit-password {immudb-pw} --audit-signature validate
```

* ...with [immugw (opens new window)](https://github.com/codenotary/immugw) with auditor capabilities:

```bash
./immugw --audit --audit-username {immudb-username} --audit-password {immudb-pw} --audit-signature validate
```

Enables the insertion of a special entry which references to another item.

### Primary Index <a href="#primary-index" id="primary-index"></a>

Index enables queries and search based on the data key.

### Secondary Index <a href="#secondary-index" id="secondary-index"></a>

Index enables queries and search based on the data value.

### Streams <a href="#streams" id="streams"></a>

Allows client server communication with streams of “delimited” \[]byte messages.

### Cryptographic signatures <a href="#cryptographic-signatures" id="cryptographic-signatures"></a>

A signature (PKI) provided by the client can be became part of the insertion process.

### Authentication (transport) <a href="#authentication-transport" id="authentication-transport"></a>

Integrated mTLS offers the best approach for machine-to-machine authentication, also providing communications security (entryption) over the transport channel.

### immugw communication <a href="#immugw-communication" id="immugw-communication"></a>

immugw can be found in its [own repository (opens new window)](https://github.com/codenotary/immugw)

immugw proxies REST client communication and gRPC server interface. For security reasons, immugw should not run on the same server as immudb. The following diagram shows how the communication works:

![](../.gitbook/assets/diagram-immugw.svg)
