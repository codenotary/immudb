FROM golang:1.13-stretch as build
WORKDIR /src
COPY . .
RUN GOOS=linux GOARCH=amd64 make immugw-static
FROM ubuntu:18.04
MAINTAINER vChain, Inc.  <info@vchain.us>

COPY --from=build /src/immugw /usr/sbin/immugw

ARG IMMU_UID="3323"
ARG IMMU_GID="3323"

ENV IMMUGW_DIR="/var/lib/immudb" \
    IMMUGW_ADDRESS="0.0.0.0" \
    IMMUGW_PORT="3323" \
    IMMUGW_IMMUDB_ADDRESS="127.0.0.1" \
    IMMUGW_IMMUDB_PORT="3322" \
    IMMUGW_MTLS="false" \
    IMMUGW_DETACHED="false" \
    IMMUGW_PKEY="/var/lib/immudb/mtls/3_application/private/key.pem" \
    IMMUGW_CERTIFICATE="/var/lib/immudb/mtls/3_application/certs/server.pem" \
    IMMUGW_CLIENTCAS="/var/lib/immudb/mtls/2_intermediate/certs/ca-chain.pem" \
    IMMUGW_AUDIT="false" \
    IMMUGW_AUDIT_USERNAME="" \
    IMMUGW_AUDIT_PASSWORD=""

RUN addgroup --system --gid $IMMU_GID immu && \
    adduser --system --uid $IMMU_UID --no-create-home --ingroup immu immu && \
    mkdir -p "$IMMUGW_DIR" && \
    chown -R immu:immu "$IMMUGW_DIR" && \
    chmod -R 777 "$IMMUGW_DIR" && \
    chmod +x /usr/sbin/immugw

EXPOSE 3323
EXPOSE 9476

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 CMD [ "/usr/sbin/immugw", "version" ]
USER immu
ENTRYPOINT ["/usr/sbin/immugw"]
