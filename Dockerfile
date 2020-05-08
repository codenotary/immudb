FROM golang:1.13-stretch as build
WORKDIR /src
COPY . .
RUN GOOS=linux GOARCH=amd64 make immuadmin-static immudb-static immugw-static
FROM scratch
COPY --from=build /src/immudb /bin/immudb
COPY --from=build /src/immuadmin /bin/immuadmin
COPY --from=build /src/immugw /bin/immugw

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 CMD [ "/bin/immuadmin", "status" ]
ENTRYPOINT ["/bin/immudb"]
