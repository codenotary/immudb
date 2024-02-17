FROM golang:1.18 as build
ARG BUILD_ARCH=amd64
WORKDIR /src
COPY go.mod go.sum /src/
RUN go mod download
COPY . .
RUN rm -rf /src/webconsole/dist
RUN GOOS=linux GOARCH=arm64 WEBCONSOLE=default make immuadmin-static immudb-static
RUN mkdir /empty

FROM gcr.io/distroless/base:nonroot AS distroless
LABEL org.opencontainers.image.authors="Codenotary Inc. <info@codenotary.com>"

ARG IMMUDB_HOME="/usr/share/immudb"

WORKDIR /usr/bin
COPY --from=build /src/immudb .
COPY --from=build /src/immuadmin .


ENV IMMUDB_HOME="${IMMUDB_HOME}" \
    IMMUDB_DIR="/var/lib/immudb" \
    IMMUDB_ADDRESS="0.0.0.0" \
    IMMUDB_PORT="3322" \
    IMMUDB_PIDFILE="" \
    IMMUDB_LOGFILE="" \
    IMMUDB_MTLS="false" \
    IMMUDB_AUTH="true" \
    IMMUDB_DETACHED="false" \
    IMMUDB_DEVMODE="true" \
    IMMUDB_MAINTENANCE="false" \
    IMMUDB_ADMIN_PASSWORD="immudb" \
    IMMUDB_PGSQL_SERVER="true" \
    IMMUADMIN_TOKENFILE="/var/lib/immudb/admin_token" \
    USER=nonroot \
    HOME="${IMMUDB_HOME}"

COPY --from=build --chown=nonroot:nonroot /empty "$IMMUDB_HOME"
COPY --from=build --chown=nonroot:nonroot /empty "$IMMUDB_DIR"
COPY --from=build --chown=nonroot:nonroot /empty /tmp

EXPOSE 3322
EXPOSE 9497
EXPOSE 8080
EXPOSE 5432

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 CMD [ "/usr/bin/immuadmin", "status" ]
USER nonroot
ENTRYPOINT ["/usr/bin/immudb"]
