#!/bin/sh
DURATION=300
WORKERS=100
BATCHSIZE=10

#docker-compose build immudb-perftest immudb-tools
docker-compose up -d immudb-standalone
docker-compose run immudb-tools-kv \
	-addr  immudb-standalone -db perf -duration $DURATION \
	-read-workers 0 -read-batchsize 0 -write-speed 0 \
	-write-workers $WORKERS -write-batchsize $BATCHSIZE \
	-silent -summary

