#!/bin/sh
# docker-compose build immudb-standalone immudb-tools

for BATCHSIZE in 1 10 100
do

for WORKERS in 1 10 100
do
docker-compose up -d immudb-standalone
docker-compose run immudb-tools-kv -addr immudb-standalone -read-workers 0 -read-batchsize 0 -write-speed 0 -write-workers $WORKERS -write-batchsize $BATCHSIZE -write-batchnum 500 | tee /tmp/run-w${WORKERS}-b${BATCHSIZE}.log
docker-compose down
done

done
