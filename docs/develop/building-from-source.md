# Building from source

### Build the binaries <a href="#build-the-binaries" id="build-the-binaries"></a>

Building binaries requires a Linux operating system.

To build the binaries yourself, simply clone this repo and run:

Below are instructions on how to build binaries for other architectures and systems.

#### Linux (by component) <a href="#linux-by-component" id="linux-by-component"></a>

```
GOOS=linux GOARCH=amd64 make immuclient-static immuadmin-static immudb-static
```

#### MacOS (by component) <a href="#macos-by-component" id="macos-by-component"></a>

```
GOOS=darwin GOARCH=amd64 make immuclient-static immuadmin-static immudb-static
```

#### Windows (by component) <a href="#windows-by-component" id="windows-by-component"></a>

```
GOOS=windows GOARCH=amd64 make immuclient-static immuadmin-static immudb-static
```

### Build the Docker images <a href="#build-the-docker-images" id="build-the-docker-images"></a>

If you want to build the container images yourself, simply clone this repo and run:

```
docker build -t myown/immudb:latest -f Dockerfile .
docker build -t myown/immuadmin:latest -f Dockerfile.immuadmin .
docker build -t myown/immuclient:latest -f Dockerfile.immuclient .
```
