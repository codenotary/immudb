# Relasing a new `immudb` version

This document provides all steps required by a maintainer to release a new `immudb` version.

We assume all commands are entered from the root of the `immudb` working directory.
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html), in this document we will use vX.Y.V as placeholder for the version we are going to relase.

Before relasing, ensure that all modifications have been tested and pushed. When the release introduce a user-facing change, please ensure the documentation is updated accordingly.

### About the branching model

Although `immudb` aims to have a "[OneFlow](https://www.endoflineblog.com/oneflow-a-git-branching-model-and-workflow)" git branching model, [release branches](https://www.endoflineblog.com/oneflow-a-git-branching-model-and-workflow#release-branches) have never been used.
Thus, the instructions on the current document assume that just the `master` branch is used for the release process (with all modifications previously merged in). However the whole process can be easily adapter to a release branch if needed.

## 1. Ensure the latest release of the webconsole is up-to-date

When building final binaries, the latest release of the [webconsole] will be used.
Make sure that the appropriate version is released there.

Also make sure that the webconsole/dist folder does not exist,
any existing content will be used instead of the released webconsole version:

```sh
rm -rf webconsole/dist
```

[webconsole]: github.com/codenotary/immudb-webconsole/releases/latest

## 2. Bump version (vX.Y.Z)

Edit `Makefile` and modify the `VERSION` and `DEFAULT_WEBCONSOLE_VERSION` variables:

```
VERSION=X.Y.Z
DEFAULT_WEBCONSOLE_VERSION=A.B.C
```
> N.B. Omit the `v` prefix.

Then run:

```
make CHANGELOG.md.next-tag
```

## 3. Commit and tag (locally)

Add the files modified above to the git index:

```
git add Makefile
git add CHANGELOG.md
```

Then:
```
git commit -m "release: vX.Y.Z"
git tag vX.Y.Z
```
> Do not push now.

Finally, sign the commit using `vcn` :
```
vcn n -p git://.
```

## 4. Make dist files

In order to sign the Windows binaries with a digital certificate, you will need an `.spc` and `.pvk` files (and the password to unlook the `.pvk`).
Make sure the path of those files is accessible.

Build all dist files. It's possible launch script on all services. Modify SERVICE_NAME according your needs:

```
WEBCONSOLE=default SIGNCODE_PVK_PASSWORD=<pvk password> SIGNCODE_PVK=<path to vchain.pvk> SIGNCODE_SPC=<path to vchain.spc> make dist
```
> Distribution files will be created into the `dist` directory.


Check that everthing worked as expected and finally sign all files using `vcn`:
```
make dist/sign
```

## 5. Push and edit the release on github

Push your commits and tag:
```
git push
git push --tags
```
> From now on, your relase will be publicy visible, and [dockerhub](https://hub.docker.com/repository/docker/codenotary/immudb/builds) should start building docker images for `immudb`.

Now you can edit the vX.Y.Z newly created [release on GitHub](https://github.com/vchain-us/immudb/releases), using the follwing template

```
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

Mark this tag as a `pre-release` for now.

Finally, uploads all files from `dist`.

Do the final check of uploaded binaries by doing manual smoke tests,
once everything works correctly, uncheck the `pre-release` mark.

## 6. Sign docker images

Once [dockerhub](https://hub.docker.com/repository/docker/codenotary/immudb/builds) has finished to build the images, then pull, check and finally sign them.

Pull:
```
docker pull codenotary/immudb:X.Y.Z
```

Check:
```
docker run --rm -it codenotary/immudb:X.Y.Z version
```

Sign:
```
vcn n -p docker://codenotary/immudb:X.Y.Z
```

## 7. Create documentation for the version

Documentation is kept inside the [immudb.io repo](https://github.com/codenotary/immudb.io).

Make sure that the documentation in `src/master` is up-to-date and then copy it to `src/<version>` folder.
Once done, add new version to the list in [version constant in vuepress index.js file](https://github.com/codenotary/immudb.io/blob/master/src/.vuepress/theme/util/index.js#L242).

Once those changes end up in master, the documentation will be compiled and deployed automatically.
