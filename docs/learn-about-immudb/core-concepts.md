# Concepts

Download the [immudb short research paper](https://codenotary.com/technologies/immudb/) to learn about the technical foundations of immudb.

## Adding data

[//]: # "update these sections to reflect accurate information"

This section is not yet ready.

## Checking data consistency
This section is not yet.

## State signature

Providing `immudb` with a signing key enables cryptographic state signatures.
This means that an auditor or a third party client, for instance, could verify the authenticity of the returned current state after calling the `currentState` gRPC method.

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

[//]: # "these link are broken and lead to pages in the /old hierarchy"

Check [state signature](old/immudb/#state-signature) and [verify state signature](sdks-api.html#verify-state-signature) paragraphs for additional details.

Immuclient and [immugw](https://github.com/codenotary/immugw) are shipped with auditor capabilities.
To get the signed state in combination with the auditor, launch...
* ...immuclient with auditor capabilities:
```bash
immuclient audit-mode --audit-username {immudb-username} --audit-password {immudb-pw} --audit-signature validate
```
* ...with [immugw](https://github.com/codenotary/immugw) with auditor capabilities:
```bash
./immugw --audit --audit-username {immudb-username} --audit-password {immudb-pw} --audit-signature validate
```

## Item References

Enables the insertion of a special entry that references another item.

## Primary Index

The primary index enables queries and search based on the **data key**.

## Secondary Index

The secondary index enables queries and search based on the **data value**.

## Streams
Allows client server communication with streams of “delimited” []byte messages.

## Cryptographic signatures

A signature (PKI) provided by the client can become part of the insertion process.

## Authentication (transport)

Integrated mTLS offers the best approach for machine-to-machine authentication, also providing communications security (entryption) over the transport channel.

## immugw communication
immugw can be found in its [own repository](https://github.com/codenotary/immugw).

immugw serves as a proxy to relay REST client communication from your application to immudb's gRPC server interface. For security reasons, immugw should not run on the same server as immudb. The following diagram shows how the communication works:

[//]: # "Image not rendering for some reason"

<img src="gitbook/assets/diagram-immugw.svg" />