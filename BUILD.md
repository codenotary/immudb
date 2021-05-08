# Build the binaries yourself

To build the binaries yourself, simply clone this repo and run

```
make all
```

To embed the webconsole, place the front-end code in `webconsole/dist`, and build with

```
make WEBCONSOLE=1
```

This will add the Go build tag `webconsole` which will use the *statik* library to embed the
front-end code. The front-end will be then served in the web API root "/".

## Linux (by component)

```bash
GOOS=linux GOARCH=amd64 make immuclient-static immuadmin-static immudb-static
```

## MacOS (by component)

```bash
GOOS=darwin GOARCH=amd64 make immuclient-static immuadmin-static immudb-static
```

## Windows (by component)

```bash
GOOS=windows GOARCH=amd64 make immuclient-static immuadmin-static immudb-static
```

## Freebsd (by component)

```bash
GOOS=freebsd GOARCH=amd64 make immuclient-static immuadmin-static immudb-static
```
# Build the Docker images yourself

If you want to build the container images yourself, simply clone this repo and run

```
docker build -t myown/immudb:latest -f Dockerfile .
docker build -t myown/immuadmin:latest -f Dockerfile.immuadmin .
docker build -t myown/immuclient:latest -f Dockerfile.immuclient .
```
And then run immudb as described when pulling official immudb Docker image.
