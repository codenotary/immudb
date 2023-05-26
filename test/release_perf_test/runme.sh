#!/bin/bash

DURATION=600
BUILD=1
DOCTEST=1
SQLTEST=1
KVTEST=1

while getopts "hd:DQKB" arg; do
  case $arg in
    d)
      DURATION=$OPTARG
      ;;
    B)
      unset BUILD
      ;;
    D)
      unset DOCKTEST
      ;;
    Q)
      unset SQLTEST
      ;;
    K)
      unset KVTEST
      ;;
    h | *)
      echo "Usage:"
      echo "$0 [ -d duration ] [ -B ] [ -D ] [ -Q ] [ -K ]"
      echo "        -d duration: duration of each test in seconds"
      echo "        -B (nobuild): skip building of containers"
      echo "        -D (nodocument): skip document test"
      echo "        -Q (nosql): skip sql test"
      echo "        -K (nokv): skip KV test"
      exit 1
      ;;
  esac
done

if [ $BUILD ]
then
docker-compose build immudb-perftest immudb-tools
fi
CSV_LINES=( 'test,client,batchsize,replication,Write TX/s,Write KV/s' )


function print_result() {
	local REPL=$1
	shift
	local STATS=("$@")
	# print resulting table
	IDX=0
	echo "#---"
	echo "clients	batchsize	repl.	Write TX/s	Write KV/s"
	for BATCHSIZE in 1 100 1000
	do
		for WORKERS in 1 10 100
		do
		TXS=${STATS[IDX]}
		echo "$WORKERS	$BATCHSIZE		$REPL	$TXS	$((TXS*BATCHSIZE))"
		IDX=$((IDX+1))
		done
done
}

function test_matrix_kv() {
	SRV=$1
	ADDR=$2
	REPL=$3
	STATS=()
	> /tmp/runme.log
	for BATCHSIZE in 1 100 1000
	do
		for WORKERS in 1 10 100
		do
		echo "BATCHSIZE $BATCHSIZE WORKERS $WORKERS REPL $REPL" | tee -a /tmp/runme.log
		docker-compose up -d $SRV
		sleep 5
		docker-compose run immudb-tools-kv \
			-addr $ADDR -db perf -duration $DURATION \
			-read-workers 0 -read-batchsize 0 -write-speed 0 \
			-write-workers $WORKERS -write-batchsize $BATCHSIZE \
			-silent -summary 2>&1 | tee -a /tmp/runme.log
		TXS=$(tail -n1 /tmp/runme.log|grep -F "TOTAL WRITE"|grep -Eo '[0-9]+ Txs/s'|cut -d ' ' -f 1)
		STATS+=( $TXS )
		CSVLINE="kv;$WORKERS;$BATCHSIZE;$REPL;$TXS;$((TXS*BATCHSIZE))"
		CSV_LINES+=( $CSVLINE )
		echo "TXS: $TXS, STATS: ${STATS[*]}"
		docker-compose down --timeout 2
		done
	done
	print_result "$REPL" "${STATS[@]}"
}

function test_matrix_sql() {
	SRV=$1
	ADDR=$2
	REPL=$3
	STATS=()
	> /tmp/runme.log
	for BATCHSIZE in 1 100 1000
	do
		for WORKERS in 1 10 100
		do
		echo "BATCHSIZE $BATCHSIZE WORKERS $WORKERS REPL $REPL" | tee -a /tmp/runme.log
		docker-compose up -d $SRV
		sleep 5
		docker-compose run immudb-tools-sql \
			-addr $ADDR -db perf -duration $DURATION \
			-workers $WORKERS -txsize 1 -insert-size $BATCHSIZE \
			2>&1 | tee -a /tmp/runme.log
		WRS=$(tail -n1 /tmp/runme.log|grep -F "Total Writes"|grep -Eo '[0-9.]+ writes/s'|cut -d ' ' -f 1)
		TXS=$(tail -n1 /tmp/runme.log|awk "/Total Writes/{print \$9/$BATCHSIZE}")
		TRUNC_TRS=$(echo $WRS|grep -Eo '^[0-9]+')
		STATS+=( $TRUNC_TRS )
		CSVLINE="sql;$WORKERS;$BATCHSIZE;$REPL;$TXS;$WRS"
		CSV_LINES+=( $CSVLINE )
		echo "TXS: $TXS, WRS: $WRS, STATS: ${STATS[*]}"
		docker-compose down --timeout 2
		done
	done
	print_result "$REPL" "${STATS[@]}"
}

function test_matrix_doc() {
	SRV=$1
	ADDR=$2
	REPL=$3
	STATS=()
	> /tmp/runme.log
	for BATCHSIZE in 1 100 1000
	do
		for WORKERS in 1 10 100
		do
		echo "BATCHSIZE $BATCHSIZE WORKERS $WORKERS REPL $REPL" | tee -a /tmp/runme.log
		docker-compose up -d $SRV
		sleep 5
		docker-compose run immudb-tools-web-api \
			-b http://$ADDR:8080 --duration $DURATION -db perf \
			-w $WORKERS -s $BATCHSIZE \
			2>&1 | tee -a /tmp/runme.log
		TX=$(awk '/TX:/{print $7}' /tmp/runme.log | tail -n 1)
		TXS=$((TX/DURATION))
		WRS=$((TX*BATCHSIZE/DURATION))
		STATS+=( $TXS )
		CSVLINE="doc;$WORKERS;$BATCHSIZE;$REPL;$TXS;$WRS"
		CSV_LINES+=( $CSVLINE )
		echo "TXS: $TXS, WRS: $WRS, STATS: ${STATS[*]}"
		docker-compose down --timeout 2
		done
	done
	print_result "$REPL" "${STATS[@]}"
}
if [ $KVTEST ]
then
test_matrix_kv "immudb-standalone" "immudb-standalone" "no"
test_matrix_kv "immudb-async-main immudb-async-replica" "immudb-async-main" "async"
test_matrix_kv "immudb-sync-main immudb-sync-replica" "immudb-sync-main" "sync"
fi

if [ $SQLTEST ]
then
test_matrix_sql "immudb-standalone" "immudb-standalone" "no"
test_matrix_sql "immudb-async-main immudb-async-replica" "immudb-async-main" "async"
# FIXME sql + sync is currently broken
#test_matrix_sql "immudb-sync-main immudb-sync-replica" "immudb-sync-main" "sync"
fi

if [ $DOCTEST ]
then
test_matrix_doc "immudb-standalone" "immudb-standalone" "no"
test_matrix_doc "immudb-async-main immudb-async-replica" "immudb-async-main" "async"
test_matrix_doc "immudb-sync-main immudb-sync-replica" "immudb-sync-main" "sync"
fi

printf '%s\n' "${CSV_LINES[@]}"
printf '%s\n' "${CSV_LINES[@]}" > result.csv


