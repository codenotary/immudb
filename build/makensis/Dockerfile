FROM debian:10

ENV NSIS 3.04-1

RUN apt-get update && \
    apt-get install -y nsis=${NSIS} && \
    apt-get clean

WORKDIR /build

ENTRYPOINT [ "/usr/bin/makensis" ]
