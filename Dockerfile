FROM golang:1.13-stretch as build
WORKDIR /src
COPY . .
RUN GOOS=linux GOARCH=amd64 make immuadmin-static immudb-static
FROM ubuntu:18.04
MAINTAINER vChain, Inc.  <info@vchain.us>

COPY --from=build /src/immudb /usr/sbin/immudb
COPY --from=build /src/immuadmin /usr/local/bin/immuadmin

ARG IMMU_UID="3322"
ARG IMMU_GID="3322"

ENV IMMUDB_HOME="/usr/share/immudb" \
    IMMUDB_DIR="/var/lib/immudb" \
    IMMUDB_DBNAME="immudb" \
    IMMUDB_ADDRESS="0.0.0.0" \
    IMMUDB_PORT="3322" \
    IMMUDB_PIDFILE="" \
    IMMUDB_LOGFILE="" \
    IMMUDB_MTLS="false" \
    IMMUDB_AUTH="true" \
    IMMUDB_DETACHED="false" \
    IMMUDB_PKEY="/usr/share/immudb/mtls/3_application/private/key.pem" \
    IMMUDB_CERTIFICATE="/usr/share/immudb/mtls/3_application/certs/server.pem" \
    IMMUDB_CLIENTCAS="/usr/share/immudb/mtls/2_intermediate/certs/ca-chain.pem" \
    IMMUDB_DEVMODE="true" \
    IMMUDB_MAINTENANCE="false" \
    IMMUDB_ADMIN_PASSWORD="immudb" \
    IMMUADMIN_TOKENFILE="/var/lib/immudb/admin_token"

RUN addgroup --system --gid $IMMU_GID immu && \
    adduser --system --uid $IMMU_UID --no-create-home --ingroup immu immu && \
    mkdir -p "$IMMUDB_HOME" && \
    mkdir -p "$IMMUDB_DIR" && \
    chown -R immu:immu "$IMMUDB_HOME" "$IMMUDB_DIR" && \
    chmod -R 777 "$IMMUDB_HOME" "$IMMUDB_DIR" && \
    chmod +x /usr/sbin/immudb /usr/local/bin/immuadmin

EXPOSE 3322
EXPOSE 9497

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 CMD [ "/usr/local/bin/immuadmin", "status" ]
USER immu
ENTRYPOINT ["/usr/sbin/immudb"]
