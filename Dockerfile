FROM golang:1.13-stretch as build
WORKDIR /src
COPY . .
RUN GOOS=linux GOARCH=amd64 make immu-static immud-static
FROM alpine:3.9 as ca
RUN apk add --no-cache ca-certificates
FROM scratch
COPY --from=build /src/immud /bin/immud
COPY --from=build /src/immu /bin/immu
ENTRYPOINT ["/bin/immud"]
