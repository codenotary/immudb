#!/bin/bash
# docker-compose build immudb-perftest immudb-tools

BATCHNUM=10000

STATS=()
for BATCHSIZE in 1 10 100
do
	for WORKERS in 1 10 100
	do
	echo "BATCHSIZE $BATCHSIZE WORKERS $WORKERS" >> /tmp/runme.log
	docker-compose up -d immudb-standalone
	sleep 5
	docker-compose run immudb-tools-kv -addr immudb-standalone -db perf -read-workers 0 -read-batchsize 0 -write-speed 0 -write-workers $WORKERS -write-batchsize $BATCHSIZE -write-batchnum $BATCHNUM -silent -summary | tee -a /tmp/runme.log
	TXS=$(tail -n1 /tmp/runme.log|grep -F "TOTAL WRITE"|grep -Eo '[0-9]+ Txs/s'|cut -d ' ' -f 1)
	STATS+=( $TXS )
	docker-compose down
	done
done
# print resulting table
IDX=0
echo "#---"
echo "batchsize\\workers	1	10	100"
for BATCHSIZE in 1 10 100
do
	echo -n "$BATCHSIZE			"
	for WORKERS in 1 10 100
	do
	echo -n "${STATS[IDX]}	"
	IDX=$((IDX+1))
	done
echo ""
done
