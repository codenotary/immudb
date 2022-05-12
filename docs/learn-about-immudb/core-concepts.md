# Concepts

Download the [immudb short research paper](https://codenotary.com/technologies/immudb/) to learn about the technical foundations of immudb.

<WrappedSection>

## Adding data

This section is not yet ready for immudb 0.9. We are working on it in order to improve it and we are close to deliver. Stay tuned!

</WrappedSection>

<WrappedSection>

## Checking data consistency
This section is not yet ready for immudb 0.9. We are working on it in order to improve it and we are close to deliver. Stay tuned!

</WrappedSection>

<WrappedSection>

## State signature

Providing `immudb` with a signing key enables the cryptographic state signature.
That means that an auditor or a third party client, for instance, could verify the authenticity of the returned current state after calling the `currentState` gRPC method.

Here are the gRPC message definitions:
```
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

</WrappedSection>

<WrappedSection>

## Item References

Enables the insertion of a special entry that references another item.

</WrappedSection>

<WrappedSection>

## Primary Index

The primary index enables queries and search based on the **data key**.

</WrappedSection>

<WrappedSection>

## Secondary Index

The secondary index enables queries and search based on the **data value**.

</WrappedSection>

<WrappedSection>

## Streams
Allows client server communication with streams of “delimited” []byte messages.

</WrappedSection>

<WrappedSection>

## Cryptographic signatures

A signature (PKI) provided by the client can become part of the insertion process.

</WrappedSection>

<WrappedSection>

## Authentication (transport)

Integrated mTLS offers the best approach for machine-to-machine authentication, also providing communications security (entryption) over the transport channel.

</WrappedSection>

<WrappedSection>

## immugw communication
immugw can be found in its [own repository](https://github.com/codenotary/immugw)

immugw proxies REST client communication and gRPC server interface. For security reasons, immugw should not run on the same server as immudb. The following diagram shows how the communication works:

![immugw communication explained](/diagram-immugw.svg)

</WrappedSection>
