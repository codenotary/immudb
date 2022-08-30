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

The Federal Information Processing Standard (FIPS) 140-2 publication describes United States government approved security requirements for cryptographic modules. FIPS-140 series is a collection of computer security standards set by the National Institute of Standards & Technology (NIST) for the United States government. FIPS 140–2 defines the critical security parameters vendors must use for encryption before selling into the U.S government.

### Requirements

To be compliant, all components of immudb must be compliant, along with the communication between those components, and any storage used by them. For a fully FIPS compliant deployment of immudb a few things are required:

- immudb must be compiled with a FIPS validated cryptographic module
- immudb must be configured to use FIPS approved cryptographic algorithms

### FIPS compliance in Go

The native go crypto standard library is not FIPS compliant. There are two options available:
- Use BoringCrypto based Go image.
- Use RedHat go compiler toolchain.


##### Redhat toolset

Red Hat Enterprise Linux (RHEL) ships with several Federal Information Processing Standards (FIPS)-validated cryptography libraries, including OpenSSL. This allows applications that use these libraries to operate in FIPS mode, which means that the cryptographic techniques they use can are in compliance with the FIPS-140-2 standard. **This seems to be a paid subscription.**

By default, applications written in Go use cryptographic functions from the Go standard library, which is not FIPS-validated. However, the version of Go shipped in RHEL is based on upstream Go's dev.boringcrypto branch, which is modified to use BoringSSL for crypto primitives. Modifications made in the RHEL version replace BoringSSL with OpenSSL. These modifications allow applications written with RHEL's Go to use crypto functions from a FIPS-validated version of OpenSSL.

##### BoringCrypto
BoringCrypto (BoringSSL based crypto) maintained by Google is an open-source, general-purpose cryptographic library that provides FIPS 140–2 approved cryptographic algorithms to serve BoringSSL and other user-space applications.

[BoringSSL](https://www.imperialviolet.org/2015/10/17/boringssl.html) (Google's fork of OpenSSL) as a whole is not FIPS validated. However, there is a core library (called BoringCrypto) that has been FIPS validated. For more detailed information about BoringCrypto see [this document](https://boringssl.googlesource.com/boringssl/+/master/crypto/fipsmodule/FIPS.md).


##### Limitations of BoringCrypto

- The build must be GOOS=linux, GOARCH=amd64.
- The build must have cgo enabled.
- The android build tag must not be specified.
- The cmd_go_bootstrap build tag must not be specified.
- The version string reported by runtime.Version does not indicate that BoringCrypto was actually used for the build. For example, linux/386 and non-cgo linux/amd64 binaries will report a version of go1.8.3b2 but not be using BoringCrypto.

Please note, currently there is no support for arm64 builds for BoringCrypto. Refer [here](https://github.com/golang/go/issues/39760) for the same.



### Implementation

Implementation proposal for **immudb** FIPS binary is by creating a new `Dockerfile.fips` which builds from the boringcrypto golang image. This helps in the following ways:

- No changes required in current build
- FIP compliant images are available only for amd64, and not for arm64 builds. A separate Dockerfile will help in creating a separate workflow stage during CI build to export the FIPS compliant binaries.
- Separate stage in CI can be added to run tests from the FIPS compliant build

Google [maintains](https://github.com/golang/go/blob/dev.boringcrypto.go1.18/misc/boring/README.md) docker images with patched go toolchain, similar to golang:x.y.z.
```
A Dockerfile that starts with FROM golang:1.17.2 can switch to FROM us-docker.pkg.dev/google.com/api-project-999119582588/go-boringcrypto/golang:1.17.2b7 and should need no other modifications.
```

Upstream golang/go repository maintains a separate dev branch dev.boringcrypto.go<go-version>. This branch holds all the patches to make Go using BoringCrypto and the community has specified that there is no intention to merge this branch to main. The [patches](https://github.com/golang/go/branches/all?query=dev.boringcrypto) are kept up-to-date against minor version releases and master.



##### Building Immudb
This section describes how the immudb container image can be compiled and linked to BoringCrypto for FIPS compliance.

The immudb FIPS [Dockerfile] uses a multistage build that performs compilation in an image that contains the necessary build tools and dependencies and then exports compiled artifacts to a final image.

In addition, to ensure we can statically compile the `immudb` binary when it is linked with the BoringCrypto C library, we must pass some additional arguments to the `make immudb-static` target.

```bash
make immudb-static BUILD_CGO_ENABLED=1 BUILD_BASE_IMAGE=us-docker.pkg.dev/google.com/api-project-999119582588/go-boringcrypto/golang:1.18.5b7 BUILD_EXTRA_GO_LDFLAGS="-linkmode=external -extldflags=-static"
```

The command above can be broken down as follows:
- `make immudb-static` invokes the container image build target
- `BUILD_CGO_ENABLED=1` ensures `cgo` is enabled in the immudb compilation process
- `BUILD_BASE_IMAGE=us-docker.pkg.dev/google.com/api-project-999119582588/go-boringcrypto/golang:1.18.5b7` ensures we use the BoringCrypto flavor of Go
- `BUILD_EXTRA_GO_LDFLAGS` contains the additional linker flags we need to perform a static build
  - `-linkmode=external` tells the Go linker to use an external linker
  - `-extldflags=-static"` passes the `-static` flag to the external link to ensure a statically linked executable is produced

The container image build process should fail before export of the `immudb` binary to the final image if the compiled binary is not statically linked.

To be fully sure the produced `immudb` binary has been compiled with BoringCrypto you must remove the `-s` flag from the base immudb `Makefile` to stop stripping symbols and run through the build process above.

Then you will be able to inspect the `immudb` binary with `go tool nm` to check for symbols containing the string `_Cfunc__goboringcrypto_`.


### Backwards Compatibility
TODO

### Performance impact
TODO
