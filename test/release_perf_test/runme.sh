#!/bin/bash
docker-compose build immudb-perftest immudb-tools

BASE_BATCHNUM=10000

CSV_LINES=( 'client,batchsize,replication,Write TX/s,Write KV/s' )

function print_result() {
	local REPL=$1
	shift
	local STATS=("$@")
	# print resulting table
	IDX=0
	echo "#---"
	echo "clients	batchsize	repl.	Write TX/s	Write KV/s"
	for BATCHSIZE in 1 10 100
	do
		for WORKERS in 1 10 100
		do
		TXS=${STATS[IDX]}
		echo "$WORKERS	$BATCHSIZE		$REPL	$TXS	$((TXS*BATCHSIZE))"
		IDX=$((IDX+1))
		done
done
}

## NO REPLICATION
function test_matrix() {
	SRV=$1
	ADDR=$2
	REPL=$3

	STATS=()
	> /tmp/runme.log
	for BATCHSIZE in 1 10 100
	do
		for WORKERS in 1 10 100
		do
		echo "BATCHSIZE $BATCHSIZE WORKERS $WORKERS" >> /tmp/runme.log
		docker-compose up -d $SRV
		sleep 5
		WLEN=$(expr length $WORKERS)
		BATCHNUM=$((BASE_BATCHNUM/WLEN))
		docker-compose run immudb-tools-kv -addr $ADDR -db perf -read-workers 0 -read-batchsize 0 -write-speed 0 -write-workers $WORKERS -write-batchsize $BATCHSIZE -write-batchnum $BATCHNUM -silent -summary 2>&1 | tee -a /tmp/runme.log
		TXS=$(tail -n1 /tmp/runme.log|grep -F "TOTAL WRITE"|grep -Eo '[0-9]+ Txs/s'|cut -d ' ' -f 1)
		STATS+=( $TXS )
		CSVLINE="$WORKERS,$BATCHSIZE,$REPL,$TXS,$((TXS*BATCHSIZE))"
		CSV_LINES+=( $CSVLINE )
		echo "TXS: $TXS, STATS: ${STATS[*]}"
		docker-compose down
		done
	done
	print_result "$REPL" "${STATS[@]}"
}

test_matrix "immudb-standalone" "immudb-standalone" "no"
test_matrix "immudb-async-main immudb-async-replica" "immudb-async-main" "async"
test_matrix "immudb-sync-main immudb-sync-replica" "immudb-sync-main" "sync"

printf '%s\n' "${CSV_LINES[@]}"


