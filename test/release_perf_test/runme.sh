#!/bin/bash
# docker-compose build immudb-perftest immudb-tools

BASE_BATCHNUM=10000

CSV_LINES=( 'client,batchsize,replication,Write TX/s,Write KV/s' )

## NO REPLICATION
if false; then # FIXME remove

STATS=()
> /tmp/runme.log
for BATCHSIZE in 1 10 100
do
	for WORKERS in 1 10 100
	do
	echo "BATCHSIZE $BATCHSIZE WORKERS $WORKERS" >> /tmp/runme.log
	docker-compose up -d immudb-standalone
	sleep 5
	WLEN=$(expr length $WORKERS)
	BATCHNUM=$((BASE_BATCHNUM/WLEN))
	docker-compose run immudb-tools-kv -addr immudb-standalone -db perf -read-workers 0 -read-batchsize 0 -write-speed 0 -write-workers $WORKERS -write-batchsize $BATCHSIZE -write-batchnum $BATCHNUM -silent -summary 2>&1 | tee -a /tmp/runme.log
	TXS=$(tail -n1 /tmp/runme.log|grep -F "TOTAL WRITE"|grep -Eo '[0-9]+ Txs/s'|cut -d ' ' -f 1)
	STATS+=( $TXS )
	echo "TXS: $TXS, STATS: ${STATS[*]}"
	docker-compose down
	done
done
# print resulting table
IDX=0
echo "#---"
echo "clients	batchsize	repl.	Write TX/s	Write KV/s"
for BATCHSIZE in 1 10 100
do
	for WORKERS in 1 10 100
	do
	TXS=${STATS[IDX]}
	echo "$WORKERS	$BATCHSIZE		no	$TXS	$((TXS*BATCHSIZE))"
	CSVLINE="$WORKERS,$BATCHSIZE,no,$TXS,$((TXS*BATCHSIZE))"
	CSV_LINES+=( $CSVLINE )
	IDX=$((IDX+1))
	done
done

fi # FIXME remove

## ASYNC REPLICATION

docker-compose up -d immudb-async-main immudb-async-replica
sleep 5
