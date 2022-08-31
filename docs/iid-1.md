---
IIP: 1
title: FIPS 140-2 in Immudb
author: Farhan Khan <farhan@codenotary.com>
status: Draft
category: Compliance
created: 2022-08-30
---

### Abstract

This IIP (Immudb Improvement Proposal) introduces FIPS-140 compliance requirements for implementation in immudb

### Motivation

The Federal Information Processing Standard (FIPS) 140-2 publication describes United States government approved security requirements for cryptographic modules. FIPS-140 series is a collection of computer security standards set by the National Institute of Standards and Technology (NIST) for the United States government. FIPS 140–2 defines the critical security parameters vendors must use for encryption before selling into the U.S government.

### Requirements

To be FIPS compliant, all components of immudb must be compliant, along with the communication between those components, and any storage used by them. For a fully FIPS compliant deployment of immudb a few things are required:

- immudb must be compiled with a FIPS validated cryptographic module
- immudb must be configured to use FIPS approved cryptographic algorithms

### FIPS compliance in Go

The native go crypto standard library is not FIPS compliant. There are two options available:
- Use BoringCrypto based Go image.
- Use RedHat go compiler toolchain.


##### **Redhat toolset**

Red Hat Enterprise Linux (RHEL) ships with several Federal Information Processing Standards (FIPS)-validated cryptography libraries, including OpenSSL. This allows applications that use these libraries to operate in FIPS mode, which means that the cryptographic techniques they use can are in compliance with the FIPS-140-2 standard. **This seems to be a paid subscription.**

By default, applications written in Go use cryptographic functions from the Go standard library, which is not FIPS-validated. However, the version of Go shipped in RHEL is based on upstream Go's dev.boringcrypto branch, which is modified to use BoringSSL for crypto primitives. Modifications made in the RHEL version replace BoringSSL with OpenSSL. These modifications allow applications written with RHEL's Go to use crypto functions from a FIPS-validated version of OpenSSL.

##### **BoringCrypto**
BoringCrypto (BoringSSL based crypto) maintained by Google is an open-source, general-purpose cryptographic library that provides FIPS 140–2 approved cryptographic algorithms to serve BoringSSL and other user-space applications BoringSSL is Google’s forked version of OpenSSL cryptographic library.

##### Limitations of BoringCrypto

- The build must be GOOS=linux, GOARCH=amd64.
- The build must have cgo enabled.
- The android build tag must not be specified.
- The cmd_go_bootstrap build tag must not be specified.
- The version string reported by runtime.Version does not indicate that BoringCrypto was actually used for the build. For example, linux/386 and non-cgo linux/amd64 binaries will report a version of go1.8.3b2 but not be using BoringCrypto.

Please note, currently there is no support for arm64 builds for BoringCrypto. Refer [here](https://github.com/golang/go/issues/39760) for the same.

### What does BoringCrypto actually do?

Golang has a crypto standard library, Go `crypto` package which supports various crypto requirements. (TLS implementation HTTPS servers/clients, libraries for signatures to verify hashes, encrypt/decrypt messages)

TLS protocol supports the use of numerous cryptographic protocols. The knowledge of which protocol (or cipher suites) to use is important. See [cloudflare](https://blog.cloudflare.com/exposing-go-on-the-internet/) post on this. [Mozilla](https://wiki.mozilla.org/Security/Server_Side_TLS) provides a document is to help operational teams with the configuration of TLS, which is widely used.

Since the native go crypto is not FIPS compliant because it allows the usage of multiple algorithms, protocols, ciper suites and versions (which are vulnerable), BoringCrypto restrics this and provides FIPS 140–2 approved cryptographic algorithms to serve BoringSSL and other user-space applications.

BoringCrypto internally uses cgo to execute calls to BoringSSL to implement various crypto primitives: https://github.com/golang/go/blob/dev.boringcrypto/README.boringcrypto.md.

The BoringCrypto library provides a C-language API for use by other processes that require cryptographic functionality. All operations of the module occur via calls from host applications and their respective internal daemons/processes. The cryptographic boundary of the BoringCrypto module is a single object file named bcm.o which is statically linked to BoringSSL. The library performs no communications other than with the calling application (the process that invokes the library services) and the host operating system.


### Implementation

Implementation proposal for **immudb** FIPS binary is by creating a new `Dockerfile.fips` which builds from the boringcrypto golang image. This helps in the following ways:

- No changes required in current build
- FIP compliant images are available only for amd64, and **not for arm64 builds**. A separate Dockerfile will help in creating a separate workflow stage during CI build to export the FIPS compliant binaries.
- Separate stage in CI can be added to run tests from the FIPS compliant build

Google [maintains](https://github.com/golang/go/blob/dev.boringcrypto.go1.18/misc/boring/README.md) docker images with patched go toolchain, similar to golang:x.y.z.
```
A Dockerfile that starts with FROM golang:1.17.2 can switch to FROM us-docker.pkg.dev/google.com/api-project-999119582588/go-boringcrypto/golang:1.17.2b7 and should need no other modifications.
```

Upstream golang/go repository maintains a separate dev branch dev.boringcrypto.go<go-version>. This branch holds all the patches to make Go using BoringCrypto and the community has specified that there is no intention to merge this branch to main. The [patches](https://github.com/golang/go/branches/all?query=dev.boringcrypto) are kept up-to-date against minor version releases and master.


##### Building Immudb
This section describes how the immudb container image can be compiled and linked to BoringCrypto for FIPS compliance.

- The Dockerfile.fips can be used for building immudb from the goboring image for Go 1.18 (us-docker.pkg.dev/google.com/api-project-999119582588/go-boringcrypto/golang:1.18.5b7).

- Enable cgo: `CGO_ENABLED=1` ensures `cgo` is enabled in the immudb compilation process

- To ensure the fips compliant `immudb` binary is linked with the BoringCrypto C library, we can run the `go tool nm` command which lists the symbols defined or used by an object file, archive, or executable. The presence of boring crypto can be analysed by checking if the `_Cfunc__goboringcrypto_` string is present. To ensure FIPS-only schemes are used, a check on the symbols is peformed for `crypto/internal/boring/sig.FIPSOnly` string. This is part of a script `check-fips.sh` which can be run on FIPS docker build for verification.

- With boringcrypto image, a new function is added to the crypto/tls pkg `crypto/tls/fipsonly` which can be used to ensure fips mode has been enabled.

### Backwards Compatibility

The data directory to which immudb writes (in FIPS mode) is backward compatible with non-FIPS binaries. To test this, the FIPS binary was executed with a bunch of transactions. Then a non-FIPS binary was executed using the same directory to which FIPS binary had written. On querying the database, all the transactions were readable.

### Performance impact
There is an overhead in calling into BoringCrypto via cgo for the crypto library functions, which incur a performance penalty. The library performs slower than the built-in crypto library.

Quoting Russ Cox [performance analysis](https://github.com/golang/go/issues/21525) for boringcrypto done back in 2017

```
In general there is about a 200ns overhead to calling into BoringCrypto via cgo for a particular call. So for example aes.BenchmarkEncrypt (testing encryption of a single 16-byte block) went from 13ns to 209ns, or +1500%. That we can't do much about except hope that bulk operations call into cgo once instead of once per 16 bytes.
```

There is no exact benchmark on the latest improvements with the newer Go versions, but a test has been performed using the `immudb` binaries to judge real time performance.

The following tests were performed on the Hetzner box. Both (FIPS and non-FIPS) build were run as docker containers. Benchmarks have been done using the [stresser2](https://github.com/codenotary/immudb-tools) tool.

Configuration: | Writers  100 | Sync Frequency  5ms


##### With FIPS build

| Key | Value | KV/Tx | Tx/Writer | Total Txn | Total time (sec) | Txn/sec | KV/sec | Battery  | % Difference |
|:---:|:-----:|:-----:|:---------:|:---------:|:----------------:|:-------:|:------:|:--------:|--------------|
|   8 |   256 |     1 |     10000 |   1000000 |            85.36 |   11715 |  11715 | ON       |       -9.84% |
|   8 |  1024 |     1 |     10000 |   1000000 |            88.89 |   11250 |  11250 | ON       |       -8.88% |
|   8 |   256 |   100 |     10000 |   1000000 |           223.45 |    4475 | 447527 | ON       |      -24.86% |
|   8 |  1024 |   100 |     10000 |   1000000 |           362.74 |    2757 | 275680 | ON       |      -17.85% |
|  32 |  4096 |     1 |     10000 |   1000000 |            94.60 |   10571 |  10571 | ON       |       -9.05% |

##### With Non-FIPS build

| Key | Value | KV/Tx | Tx/Writer | Total Txn | Total time (sec) | Txn/sec | KV/sec | Battery  |
|:---:|:-----:|:-----:|:---------:|:---------:|:----------------:|:-------:|:------:|:--------:|
|   8 |   256 |     1 |     10000 |   1000000 |            76.96 |   12994 |  12994 | ON       |
|   8 |  1024 |     1 |     10000 |   1000000 |               81 |   12346 |  12346 | ON       |
|   8 |   256 |   100 |     10000 |   1000000 |           167.91 |    5956 | 595557 | ON       |
|   8 |  1024 |   100 |     10000 |   1000000 |              298 |    3356 | 335570 | ON       |
|  32 |  4096 |     1 |     10000 |   1000000 |            86.04 |   11623 |  11623 | ON       |


As seen from the tests, there is ~9-10% drop in performance when batch size is 1, but the drop is more when the batch size is 100.


### Ciphersuites (Go vs BoringCryto)

When FIPS mode is enabled, the following TLS version and ciphersuites are only enabled. To enable FIPS mode, the `_ "crypto/tls/fipsonly"` pkg needs to be imported in the main.go file

```
// The list is taken from https://github.com/golang/go/blob/dev.boringcrypto.go1.18/src/crypto/tls/boring.go#L54

// FIPS compliant minimum TLS version
"TLSv1.2"

// FIPS compliant ciphers
var defaultCipherSuitesFIPS = []uint16{
	TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	TLS_RSA_WITH_AES_128_GCM_SHA256,
	TLS_RSA_WITH_AES_256_GCM_SHA384,
}
```

This can be verified by creating a simple HTTP server program, and running [testssl.sh](https://github.com/drwetter/testssl.sh) against it. Following is the result with FIPS mode enabled.

```
 Testing protocols via sockets except NPN+ALPN

 SSLv2      not offered (OK)
 SSLv3      not offered (OK)
 TLS 1      not offered
 TLS 1.1    not offered
 TLS 1.2    offered (OK)
 TLS 1.3    not offered and downgraded to a weaker protocol
 NPN/SPDY   not offered
 ALPN/HTTP2 h2, http/1.1 (offered)

 Testing cipher categories

 NULL ciphers (no encryption)                      not offered (OK)
 Anonymous NULL Ciphers (no authentication)        not offered (OK)
 Export ciphers (w/o ADH+NULL)                     not offered (OK)
 LOW: 64 Bit + DES, RC[2,4], MD5 (w/o export)      not offered (OK)
 Triple DES Ciphers / IDEA                         not offered
 Obsoleted CBC ciphers (AES, ARIA etc.)            not offered
 Strong encryption (AEAD ciphers) with no FS       offered (OK)
 Forward Secrecy strong encryption (AEAD ciphers)  offered (OK)


 Testing server's cipher preferences

Hexcode  Cipher Suite Name (OpenSSL)       KeyExch.   Encryption  Bits     Cipher Suite Name (IANA/RFC)
-----------------------------------------------------------------------------------------------------------------------------
TLSv1.2 (server order)
 xc02f   ECDHE-RSA-AES128-GCM-SHA256       ECDH 521   AESGCM      128      TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
 xc030   ECDHE-RSA-AES256-GCM-SHA384       ECDH 521   AESGCM      256      TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
 x9c     AES128-GCM-SHA256                 RSA        AESGCM      128      TLS_RSA_WITH_AES_128_GCM_SHA256
 x9d     AES256-GCM-SHA384                 RSA        AESGCM      256      TLS_RSA_WITH_AES_256_GCM_SHA384
TLSv1.3
 -

 Has server cipher order?     yes (OK)
 Negotiated protocol          TLSv1.2
 Negotiated cipher            ECDHE-RSA-AES128-GCM-SHA256, 521 bit ECDH (P-521)

 Testing robust forward secrecy (FS) -- omitting Null Authentication/Encryption, 3DES, RC4

 FS is offered (OK)           ECDHE-RSA-AES256-GCM-SHA384 ECDHE-RSA-AES128-GCM-SHA256
 Elliptic curves offered:     prime256v1 secp384r1 secp521r1

```

Without FIPS mode disabled, the following are the defaults

```
 Testing protocols via sockets except NPN+ALPN

 SSLv2      not offered (OK)
 SSLv3      not offered (OK)
 TLS 1      offered (deprecated)
 TLS 1.1    offered (deprecated)
 TLS 1.2    offered (OK)
 TLS 1.3    offered (OK): final
 NPN/SPDY   not offered
 ALPN/HTTP2 h2, http/1.1 (offered)

 Testing cipher categories

 NULL ciphers (no encryption)                      not offered (OK)
 Anonymous NULL Ciphers (no authentication)        not offered (OK)
 Export ciphers (w/o ADH+NULL)                     not offered (OK)
 LOW: 64 Bit + DES, RC[2,4], MD5 (w/o export)      not offered (OK)
 Triple DES Ciphers / IDEA                         offered
 Obsoleted CBC ciphers (AES, ARIA etc.)            offered
 Strong encryption (AEAD ciphers) with no FS       offered (OK)
 Forward Secrecy strong encryption (AEAD ciphers)  offered (OK)


 Testing server's cipher preferences

Hexcode  Cipher Suite Name (OpenSSL)       KeyExch.   Encryption  Bits     Cipher Suite Name (IANA/RFC)
-----------------------------------------------------------------------------------------------------------------------------
TLSv1 (server order)
 xc013   ECDHE-RSA-AES128-SHA              ECDH 521   AES         128      TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA
 xc014   ECDHE-RSA-AES256-SHA              ECDH 521   AES         256      TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA
 x2f     AES128-SHA                        RSA        AES         128      TLS_RSA_WITH_AES_128_CBC_SHA
 x35     AES256-SHA                        RSA        AES         256      TLS_RSA_WITH_AES_256_CBC_SHA
 xc012   ECDHE-RSA-DES-CBC3-SHA            ECDH 521   3DES        168      TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA
 x0a     DES-CBC3-SHA                      RSA        3DES        168      TLS_RSA_WITH_3DES_EDE_CBC_SHA
TLSv1.1 (server order)
 xc013   ECDHE-RSA-AES128-SHA              ECDH 521   AES         128      TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA
 xc014   ECDHE-RSA-AES256-SHA              ECDH 521   AES         256      TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA
 x2f     AES128-SHA                        RSA        AES         128      TLS_RSA_WITH_AES_128_CBC_SHA
 x35     AES256-SHA                        RSA        AES         256      TLS_RSA_WITH_AES_256_CBC_SHA
 xc012   ECDHE-RSA-DES-CBC3-SHA            ECDH 521   3DES        168      TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA
 x0a     DES-CBC3-SHA                      RSA        3DES        168      TLS_RSA_WITH_3DES_EDE_CBC_SHA
TLSv1.2 (server order -- server prioritizes ChaCha ciphers when preferred by clients)
 xc02f   ECDHE-RSA-AES128-GCM-SHA256       ECDH 521   AESGCM      128      TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
 xc030   ECDHE-RSA-AES256-GCM-SHA384       ECDH 521   AESGCM      256      TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
 xcca8   ECDHE-RSA-CHACHA20-POLY1305       ECDH 521   ChaCha20    256      TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256
 xc013   ECDHE-RSA-AES128-SHA              ECDH 521   AES         128      TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA
 xc014   ECDHE-RSA-AES256-SHA              ECDH 521   AES         256      TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA
 x9c     AES128-GCM-SHA256                 RSA        AESGCM      128      TLS_RSA_WITH_AES_128_GCM_SHA256
 x9d     AES256-GCM-SHA384                 RSA        AESGCM      256      TLS_RSA_WITH_AES_256_GCM_SHA384
 x2f     AES128-SHA                        RSA        AES         128      TLS_RSA_WITH_AES_128_CBC_SHA
 x35     AES256-SHA                        RSA        AES         256      TLS_RSA_WITH_AES_256_CBC_SHA
 xc012   ECDHE-RSA-DES-CBC3-SHA            ECDH 521   3DES        168      TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA
 x0a     DES-CBC3-SHA                      RSA        3DES        168      TLS_RSA_WITH_3DES_EDE_CBC_SHA
TLSv1.3 (no server order, thus listed by strength)
 x1302   TLS_AES_256_GCM_SHA384            ECDH 253   AESGCM      256      TLS_AES_256_GCM_SHA384
 x1303   TLS_CHACHA20_POLY1305_SHA256      ECDH 253   ChaCha20    256      TLS_CHACHA20_POLY1305_SHA256
 x1301   TLS_AES_128_GCM_SHA256            ECDH 253   AESGCM      128      TLS_AES_128_GCM_SHA256

 Has server cipher order?     yes (OK) -- only for < TLS 1.3
 Negotiated protocol          TLSv1.3
 Negotiated cipher            TLS_AES_128_GCM_SHA256, 253 bit ECDH (X25519)

 Testing robust forward secrecy (FS) -- omitting Null Authentication/Encryption, 3DES, RC4

 FS is offered (OK)           TLS_AES_256_GCM_SHA384 TLS_CHACHA20_POLY1305_SHA256
                              ECDHE-RSA-AES256-GCM-SHA384 ECDHE-RSA-AES256-SHA
                              ECDHE-RSA-CHACHA20-POLY1305 TLS_AES_128_GCM_SHA256
                              ECDHE-RSA-AES128-GCM-SHA256 ECDHE-RSA-AES128-SHA
 Elliptic curves offered:     prime256v1 secp384r1 secp521r1 X25519
```

##### Key Differences

- BoringCrypto does not allow unsupported TLS v1 and v1.1 and also TLS v1.2 ciphers such as ECDHE-RSA-DES-CBC3-SHA and DES-CBC3-SHA based on Triple DES which are not FIPS compliant. Go crypto also allows multiple non-compliant CBC ciphers with TLS v1.2
- Triple DES is dis-allowed/not offered
- Obsoleted CBC ciphers are dis-allowed/not offered

More on FIPS 140–2 compliant ciphers: [cipher-specifications](https://www.ibm.com/docs/en/ibm-http-server/9.0.5?topic=options-ssl-cipher-specifications)

### Action items

- Build FIPS image using (us-docker.pkg.dev/google.com/api-project-999119582588/go-boringcrypto/golang:1.18.5b7) to compile a binary that is FIPS compliant.

- Add checks for binary being FIPS compliant

- Use the crypto/tls pkg `crypto/tls/fipsonly` to ensure fips mode has been enabled.

- Add CI settings to release FIPS amd64 build

- Add CI settings to test release on FIPS amd64 build