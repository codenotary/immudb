# Releasing a new `immudb` version

This document provides all steps required by a maintainer to release a new `immudb` version.

We assume all commands are entered from the root of the `immudb` working directory.
This project adheres to [Semantic Versioning][semever],
in this document we will use vX.Y.V as placeholder for the version we are going to release.

Before releasing, ensure that all modifications have been tested and pushed.
When the release introduce a user-facing change, please ensure the documentation is updated accordingly.

[semver]: https://semver.org/spec/v2.0.0.html

## About the branching model

Although `immudb` aims to have a "[OneFlow][oneflow]" git branching model,
[release branches][relbranches] have never been used.
Thus, the instructions on the current document assume that just the `master` branch is used for the release process
(with all modifications previously merged in). However the whole process can be easily adapter to a release branch if needed.

During the release process, an additional `release/vX.Y.Z` is created to trigger github actions needed to prepare binaries.
That branch is removed after the release is finished.

[oneflow]: https://www.endoflineblog.com/oneflow-a-git-branching-model-and-workflow
[relbranches]: https://www.endoflineblog.com/oneflow-a-git-branching-model-and-workflow#release-branches

## 1. Ensure the matching release of the webconsole is ready

When building final binaries, a matching release of the [webconsole] will be used.
Make sure that the appropriate version is released there.

Also make sure that the `webconsole/dist` folder does not exist,
any existing content will be used instead of the released webconsole version:

```sh
make clean
```

[webconsole]: https://github.com/codenotary/immudb-webconsole/releases/latest

## 2. Create release branch and bump version (vX.Y.Z)

Switch to a new branch from `master` called `release/vX.Y.Z`.
Do not push yet as it triggers build process.

Edit `Makefile` and modify the `VERSION` and `DEFAULT_WEBCONSOLE_VERSION` variables:

```Makefile
VERSION=X.Y.Z
DEFAULT_WEBCONSOLE_VERSION=A.B.C
```

> N.B. Omit the `v` prefix.

Then run:

```sh
make CHANGELOG.md.next-tag
```

For non-RC versions: bump the version of the helm chart, in `helm/Chart.yaml`

```yaml
[...]
version: a.b.c
appVersion: "X.Y.Z"
```

The first line (`version`) is the version of the helm chart, the second the version of immudb.
We may want to keep them aligned.

For non-RC versions: bump the version in the README.md file in examples on how to download immudb binaries.

## 3. Commit and push the release branch

Add the files modified above to the git index:

```sh
git add Makefile
git add CHANGELOG.md
git commit -m "release: vX.Y.Z"
```

Then push the `release/vX.Y.Z` branch to github.

## 4. Tag the release locally

```sh
git tag vX.Y.Z
```

> Do not push this tag now.

## 5. Wait for github to build release files and docker images

Binaries and docker images are built automatically with github actions.

## 6. Create a draft pre-release in GitHub

On GitHub, [draft a new release](https://github.com/vchain-us/immudb/releases),
attach all binaries built on github.
Binaries will be available as a single compressed artifact from the `pushCI` action.
Download it, decompress locally and upload as separate binary files.

Do not assign any specific tag to this release yet. Save it as a draft.

> Assets will not be available until the release is published so postpone links generation.

Use the following template for release notes:

```md
# Changelog

<!-- copy and past here the latest part from CHANGELOG.md -->

# Downloads

**Docker image**
https://hub.docker.com/r/codenotary/immudb

**Immudb Binaries**

File | SHA256
------------- | -------------
<!--
    Paste checksums from github step: pushCI / Build binaries and notarize sources/Calculate checksums
    Ensure to paste as a plain text
-->


**Immuclient Binaries**

File | SHA256
------------- | -------------
<!--
    Paste checksums from github step: pushCI / Build binaries and notarize sources/Calculate checksums
    Ensure to paste as a plain text
-->

**Immuadmin Binaries**

File | SHA256
------------- | -------------
<!--
    Paste checksums from github step: pushCI / Build binaries and notarize sources/Calculate checksums
    Ensure to paste as a plain text
-->
```

In the changelog section of non-RC releases, also include changes for all prior RC versions.

## 7. Validate dist files

Following binaries are validated automatically in github actions:

* linux-amd64
* linux-amd64-static
* linux-arm64
* windows-amd64
* darwin-amd64

The linux-s390x build is tested on github but it can occasionally fail due to unknown
issue with zip checksum mismatch. For that build, repeat the github action job.

The following builds have to be manually tested:

* darwin-arm64
* freebsd-amd64

Those are not currently tested due to lack of github runners for them.

The following manual tests should be performed:

* Run immudb server, make sure it works as expected
* Check the webconsole - make sure it shows correct versions on the footer after login
* connect to the immudb server with immuclient and perform few get/set operations
* connect to the immudb server with immuadmin and perform few operations such as creating and listing databases

## 8. Push and edit the release on github

Create the master branch from the release branch and push the new master:

```sh
# Push new master
git checkout -B master release/vX.Y.Z
git push origin master

# Push the version tag
git push origin vX.Y.Z
```

Then it's needed to choose the appropriate git tag on the newly created github release page.
Mark this tag as a `pre-release` for now and publish the draft.

> From now on, your release will be publicly visible, and github actions should start building docker images for `immudb`.

Once tags are pushed, corresponding docker images will be automatically built and notarized in CI pipelines.

Non-RC versions: Once everything works correctly, uncheck the `pre-release` mark.

Finally remove the temporary release branch:

```sh
git branch -d release/vX.Y.Z
git push origin :release/vX.Y.Z
```

## 9. Non-RC versions: Create documentation for the version

Documentation is kept inside the [immudb.io repo](https://github.com/codenotary/immudb.io).

Make sure that the documentation in `src/master` is up-to-date and then copy it to `src/<version>` folder.
This includes changing the version in examples in how to download and run immudb binaries (get started / quickstart section).
Also make sure to update the SDK compatibility matrix (get started / sdks).

Once done, add new version to the list in the [version constant in src/.vuepress/theme/util/index.js file][index.js]
and adjust the right-side menu list in the [getSidebar function in src/.vuepress/enhanceApp.js file][enhanceApp.js].

Once those changes end up in master, the documentation will be compiled and deployed automatically.

[index.js]: https://github.com/codenotary/immudb.io/blob/master/src/.vuepress/theme/util/index.js#L242
[enhanceApp.js]: https://github.com/codenotary/immudb.io/blob/master/src/.vuepress/enhanceApp.js#L27

## 10. Non-RC versions: Update immudb readme on docker hub

Once the release is done, make sure that the readme in docker hub are up-to-date.
For immudb please edit the Readme in <https://hub.docker.com/repository/docker/codenotary/immudb>
and synchronize it with README.md from this repository.

## 11. Non-RC versions: Post-release actions

Once the release is done, following post-release actions are needed

* Ensure the new brew version is ready - sometimes it may need some manual fixes in [the formula file](https://github.com/Homebrew/homebrew-core/blob/master/Formula/immudb.rb)
* Ensure helm chart on artifactory is updated
* Start release of new AWS image
* Create PR with updates to this file if there were any undocumented / unclear steps
