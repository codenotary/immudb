FROM golang:1.13-stretch as build
WORKDIR /src
COPY . .
RUN GOOS=linux GOARCH=amd64 make immuclient-static immudb-static immugw-static
FROM scratch
COPY --from=build /src/immudb /bin/immudb
COPY --from=build /src/immuclient /bin/immuclient
COPY --from=build /src/immugw /bin/immugw

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 CMD [ "/bin/immuclient", "ping" ]
ENTRYPOINT ["/bin/immudb"]
