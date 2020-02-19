FROM golang:1.13-stretch as build
WORKDIR /src
COPY . .
RUN GOOS=linux GOARCH=amd64 make immu-static immud-static immugw-static
FROM scratch
COPY --from=build /src/immud /bin/immud
COPY --from=build /src/immu /bin/immu
COPY --from=build /src/immugw /bin/immugw

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 CMD [ "/bin/immu", "ping" ]
ENTRYPOINT ["/bin/immud"]
