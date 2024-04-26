# Copyright 2022 Codenotary Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM golang:1.18 as build
ARG BUILD_ARCH=amd64
WORKDIR /src
COPY go.mod go.sum /src/
RUN go mod download -x
COPY . .
RUN make clean
RUN make prerequisites
RUN make swagger
RUN make swagger/dist
RUN GOOS=linux GOARCH=${BUILD_ARCH} WEBCONSOLE=default SWAGGER=true make immudb-static immuadmin-static
RUN mkdir /empty

FROM scratch
LABEL org.opencontainers.image.authors="Codenotary Inc. <info@codenotary.com>"

ARG IMMU_UID="3322"
ARG IMMU_GID="3322"
ARG IMMUDB_HOME="/usr/share/immudb"

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
    USER=immu \
    HOME="${IMMUDB_HOME}"

COPY --from=build /src/immudb /usr/sbin/immudb
COPY --from=build /src/immuadmin /usr/local/bin/immuadmin
COPY --from=build --chown="$IMMU_UID:$IMMU_GID" /empty "$IMMUDB_HOME"
COPY --from=build --chown="$IMMU_UID:$IMMU_GID" /empty "$IMMUDB_DIR"
COPY --from=build --chown="$IMMU_UID:$IMMU_GID" /empty /tmp
COPY --from=build "/etc/ssl/certs/ca-certificates.crt" "/etc/ssl/certs/ca-certificates.crt"

EXPOSE 3322
EXPOSE 9497
EXPOSE 8080
EXPOSE 5432

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 CMD [ "/usr/local/bin/immuadmin", "status" ]
USER "${IMMU_UID}:${IMMU_GID}"
ENTRYPOINT ["/usr/sbin/immudb"]
