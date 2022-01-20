# Relasing a new `immudb` version

This document provides all steps required by a maintainer to release a new `immudb` version.

We assume all commands are entered from the root of the `immudb` working directory.
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html), in this document we will use vX.Y.V as placeholder for the version we are going to relase.

Before relasing, ensure that all modifications have been tested and pushed. When the release introduce a user-facing change, please ensure the documentation is updated accordingly.

### About the branching model

Although `immudb` aims to have a "[OneFlow](https://www.endoflineblog.com/oneflow-a-git-branching-model-and-workflow)" git branching model, [release branches](https://www.endoflineblog.com/oneflow-a-git-branching-model-and-workflow#release-branches) have never been used.
Thus, the instructions on the current document assume that just the `master` branch is used for the release process (with all modifications previously merged in). However the whole process can be easily adapter to a release branch if needed.

## 1. Ensure the matching release of the webconsole is ready

When building final binaries, a matching release of the [webconsole] will be used.
Make sure that the appropriate version is released there.

Also make sure that the webconsole/dist folder does not exist,
any existing content will be used instead of the released webconsole version:

```sh
make clean
```

[webconsole]: https://github.com/codenotary/immudb-webconsole/releases/latest

## 2. Bump version (vX.Y.Z)

Edit `Makefile` and modify the `VERSION` and `DEFAULT_WEBCONSOLE_VERSION` variables:

```
VERSION=X.Y.Z
DEFAULT_WEBCONSOLE_VERSION=A.B.C
```
> N.B. Omit the `v` prefix.

Then run:

```sh
make CHANGELOG.md.next-tag
```

Be sure to also bump version of the helm chart, in `helm/Chart.yaml`
```
[...]
version: a.b.c
appVersion: "X.Y.Z"
```
The first line (`version`) is the version of the helm chart, the second the version of immudb.
We may want to keep them aligned.

## 3. Commit and tag (locally)

Add the files modified above to the git index:

```sh
git add Makefile
git add CHANGELOG.md
```

Then:

```sh
git commit -m "release: vX.Y.Z"
git tag vX.Y.Z
```

> Do not push now.

## 4. Make dist files

In order to sign the Windows binaries with a digital certificate, you will need an `.spc` and `.pvk` files (and the password to unlook the `.pvk`).
Make sure the path of those files is accessible.

Build all dist files:

```sh
WEBCONSOLE=default  make dist
```

> Distribution files will be created into the `dist` directory.
>
## 5. Create a pre-release in GitHub
On GitHub, [draft a new release](https://github.com/vchain-us/immudb/releases) and upload the `dist` files using the follwing template

> Assets will not be available until the release is published so postpone links generation.
```md
# Changelog

<!-- copy and past here the latest part from CHANGELOG.md -->

# Downloads

**Docker image**
https://hub.docker.com/r/codenotary/immudb

**Immudb Binaries**

File | SHA256
------------- | -------------
<!-- use `make dist/binary.md` to generate the downloads links and checksums -->


**Immuclient Binaries**

File | SHA256
------------- | -------------
<!-- use `make dist/binary.md` to generate the downloads links and checksums -->

**Immuadmin Binaries**

File | SHA256
------------- | -------------
<!-- use `make dist/binary.md` to generate the downloads links and checksums -->
```

## 6. Validate dist files

Each file generated in the `dist` directory should be quickly checked in all architectures.
For every platform do the following:
 * Run immudb server, make sure it works as expected
 * Check the webconsole - make sure it shows correct versions on the footer after login
 * connect to the immudb server with immuclient and perform few get/set operations
 * connect to the immudb server with immuadmin and perform few operations such as creating and listing databases

## 7. Notarize git repository and binaries
After completing tests notarize git repository and dist files using the immudb@codenotary.com account:

```sh
export CAS_NOTARIZATION_PASSWORD
read -s CAS_NOTARIZATION_PASSWORD

cas login
cas n -p git://.

make dist/sign
```

## 8. Push and edit the release on github
Now it's possible to generate link to the binaries in github pre-release page.

Push your commits and tag:

```sh
git push
git push --tags
```
Then it's needed to choose the appropriate tag on the newly created release.

> From now on, your relase will be publicy visible, and github actions should start building docker images for `immudb`.

Mark this tag as a `pre-release` for now.

Do the final check of uploaded binaries by doing manual smoke tests,
once everything works correctly, uncheck the `pre-release` mark.

## 9. Create documentation for the version

Documentation is kept inside the [immudb.io repo](https://github.com/codenotary/immudb.io).

Make sure that the documentation in `src/master` is up-to-date and then copy it to `src/<version>` folder.

Once done, add new version to the list in the [version constant in src/.vuepress/theme/util/index.js file][index.js]
and adjust the right-side menu list in the [getSidebar function in src/.vuepress/enhanceApp.js file][enhanceApp.js].

Once those changes end up in master, the documentation will be compiled and deployed automatically.

[index.js]: https://github.com/codenotary/immudb.io/blob/master/src/.vuepress/theme/util/index.js#L242
[enhanceApp.js]: https://github.com/codenotary/immudb.io/blob/master/src/.vuepress/enhanceApp.js#L27

## 10. Update immudb readme on docker hub

Once the release is done, make sure that the readme in docker hub are up-to-date.
For immudb please edit  the Readme in https://hub.docker.com/repository/docker/codenotary/immudb
and synchronzie it with README.md from this repository.
