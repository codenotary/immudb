FROM golang:1.17 as build
WORKDIR /src
COPY go.mod go.sum /src/
RUN go mod download -x
COPY . .
RUN rm -rf /src/webconsole/dist
RUN GOOS=linux GOARCH=amd64 WEBCONSOLE=default make immuadmin-static immudb-static
RUN mkdir /empty

FROM debian:bullseye-slim as bullseye-slim
LABEL org.opencontainers.image.authors="CodeNotary, Inc. <info@codenotary.com>"

COPY --from=build /src/immudb /usr/sbin/immudb
COPY --from=build /src/immuadmin /usr/local/bin/immuadmin
COPY --from=build "/etc/ssl/certs/ca-certificates.crt" "/etc/ssl/certs/ca-certificates.crt"

ARG IMMU_UID="3322"
ARG IMMU_GID="3322"

ENV IMMUDB_HOME="/usr/share/immudb" \
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
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 CMD [ "/usr/local/bin/immuadmin", "status" ]
USER immu
ENTRYPOINT ["/usr/sbin/immudb"]


FROM scratch as scratch
LABEL org.opencontainers.image.authors="CodeNotary, Inc. <info@codenotary.com>"

ARG IMMU_UID="3322"
ARG IMMU_GID="3322"

ENV IMMUDB_HOME="/usr/share/immudb" \
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
    IMMUADMIN_TOKENFILE="/var/lib/immudb/admin_token" \
    USER=immu \
    HOME="${IMMUDB_HOME}"

COPY --from=build /src/immudb /usr/sbin/immudb
COPY --from=build /src/immuadmin /usr/local/bin/immuadmin
COPY --from=build --chown="$IMMU_UID:$IMMU_GID" /empty "$IMMUDB_HOME"
COPY --from=build --chown="$IMMU_UID:$IMMU_GID" /empty "$IMMUDB_DIR"
COPY --from=build "/etc/ssl/certs/ca-certificates.crt" "/etc/ssl/certs/ca-certificates.crt"

EXPOSE 3322
EXPOSE 9497
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 CMD [ "/usr/local/bin/immuadmin", "status" ]
USER "${IMMU_UID}:${IMMU_GID}"
ENTRYPOINT ["/usr/sbin/immudb"]
