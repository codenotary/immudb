# Build the binaries yourself

To build the binaries yourself, simply clone this repo and run

```
make all
```

To embed the webconsole, build with

```
rm -rf webconsole/dist  # force download of the correct webconsole version
make WEBCONSOLE=default
```

This will download the appropriate webconsole release and add the Go build tag `webconsole`
which will use the go:embed to embed the front-end code.
The front-end will be then served in the web API root "/".

To regenerate the default page, change the files in webconsole/default and run `make webconsole/default`

## Linux (by component)

```bash
GOOS=linux GOARCH=amd64 make immuclient-static immuadmin-static immudb-static
```

## MacOS (by component)
For Apple Silicon (M1) use `GOARCH=arm64` instead of `GOARCH=amd64`

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
