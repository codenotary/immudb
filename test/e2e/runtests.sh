#!/bin/sh
set -e

TAG=bla

docker build -f Dockerfile ../.. -t $TAG
docker run -it --rm --entrypoint /src/immudb/test/e2e/replication/run.sh $TAG
docker run -it --rm --entrypoint /src/immudb/test/e2e/truncation/run.sh $TAG

