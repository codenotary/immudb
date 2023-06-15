FROM golang:1.18 as builder
WORKDIR /src/immudb
COPY go.mod go.sum /src/immudb
RUN go mod download -x
COPY . .
RUN rm -rf /src/webconsole/dist &&   mkdir /var/lib/immudb
RUN GOOS=linux GOARCH=amd64 make immuadmin-static immudb-static immuclient-static

FROM debian:stable-slim
COPY --from=builder /src/immudb/immuadmin /src/immudb/immudb /src/immudb/immuclient \
	/src/immudb/test/release_perf_test/startup.sh /usr/local/bin
RUN apt-get update && apt-get -y install netcat-traditional && apt-get clean
ENTRYPOINT ["/usr/local/bin/startup.sh"]
CMD ["standalone"]
