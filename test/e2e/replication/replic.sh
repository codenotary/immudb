#!/bin/sh

PRIMARY_ADDR=127.71.17.10
REPLICA_ADDR=127.71.17.11
STRESS_APPLICATION=../immudb-tools/stresser2/stresser2
DB=repl
DATADIR=/tmp/immudb
IMMUDB=./immudb
IMMUADMIN=./immuadmin
IMMUCLIENT=./immuclient
SIZE=500
SYNC_OPTION_PRIMARY=()
SYNC_OPTION_REPLICA=()


usage () {
cat <<EOF
Usage: $0 [ options ]
Options:
	-s size [default $SIZE]
	-c immuclient_binary [default $IMMUCLIENT]
	-a immuadmin_binary [default $IMMUADMIN]
	-i immudb_binary [default $IMMUDB]
	-d data directory path [default $DATADIR]
	-x stress_application [default $STRESS_APPLICATION]
	-P primary_address [default $PRIMARY_ADDR]
	-R replica_address [default $REPLICA_ADDR]
	-D database_name [default $DB]
	-S synchronous
EOF
exit 1
}
while getopts "s:c:a:i:d:x:P:R:D:Sh" opt; do
	case "${opt}" in
	s)
		SIZE=${OPTARG}
		;;
	c)
		IMMUCLIENT=${OPTARG}
		;;
	a)
		IMMUADMIN=${OPTARG}
		;;
	i)
		IMMUDB=${OPTARG}
		;;
	d)
		DATADIR=${OPTARG}
		;;
	x)
		STRESS_APPLICATION=${OPTARG}
		;;
	P)
		PRIMARY_ADDR=${OPTARG}
		;;
	R)
		REPLICA_ADDR=${OPTARG}
		;;
	D)
		DB=${OPTARG}
		;;
	S)
		SYNC_OPTION_PRIMARY=("--replication-sync-acks" "1" "--replication-sync-enabled")
		SYNC_OPTION_REPLICA=("--replication-sync-enabled")
		;;
	*) usage
		;;
	esac
done

mkdir -p $DATADIR
rm -rf $DATADIR/*

$IMMUDB --dir $DATADIR/primary_data -a $PRIMARY_ADDR 2>/dev/null &
PRIMARY_PID=$!

while nc -z $PRIMARY_ADDR 3322
do
  echo "Waiting primary"
  sleep 1
done

$IMMUDB --dir $DATADIR/replica_data -a $REPLICA_ADDR 2>/dev/null &
REPLICA_PID=$!

while nc -z $PRIMARY_ADDR 3322
do
  echo "Waiting replica"
  sleep 1
done

echo -n "immudb" | $IMMUADMIN login -a $PRIMARY_ADDR immudb
$IMMUADMIN -a $PRIMARY_ADDR database create $DB ${SYNC_OPTION_PRIMARY[@]}

echo -n "immudb" | $IMMUADMIN login -a $REPLICA_ADDR immudb
$IMMUADMIN -a $REPLICA_ADDR database create $DB \
  --replication-is-replica \
  --replication-primary-database $DB \
  --replication-primary-host $PRIMARY_ADDR \
  --replication-primary-password immudb \
  --replication-primary-port 3322 \
  --replication-primary-username immudb \
  ${SYNC_OPTION_REPLICA[@]}

echo "Launching $STRESS_APPLICATION"

T0=`date +%s`
$STRESS_APPLICATION -addr $PRIMARY_ADDR -write-speed 0 -read-workers 0 -write-batchnum $SIZE -write-workers 10 -db $DB -batchsize 100
T1=`date +%s`

txid() {
ADDR=$1
$IMMUCLIENT login -a $ADDR --username immudb --password immudb > /dev/null 2>/dev/null
$IMMUCLIENT status -a $ADDR --username immudb --password immudb --database repl | awk '/^txID/{print $2}'
}

TX1=$(txid $PRIMARY_ADDR)
TX2=$(txid $REPLICA_ADDR)
echo "Replication in progress ($TX1 / $TX2)"
while [ "$TX1" != "$TX2" ]
do
echo "waiting replica ($TX1 / $TX2)"
sleep 0.5
TX2=$(txid $REPLICA_ADDR)
done

T2=`date +%s`

kill $PRIMARY_PID
kill $REPLICA_PID

echo "Elapsed: $((T2-T0)) seconds, $((T1-T0)) for inserting, $((T2-T1)) for sync"


