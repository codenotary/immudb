#!/bin/bash
ADDR=127.71.17.10
CLIENT_APPLICATION=/src/immudb/test/e2e/truncation/truncation
DB=expire
DATADIR=/tmp/immudb
IMMUDB=/src/immudb/immudb
IMMUADMIN=/src/immudb/immuadmin
IMMUCLIENT=/src/immudb/immuclient
SIZE=500
RETENTION=90s
INTERVAL=5s
FREQUENCY=30s
DURATION=300s

usage () {
cat <<EOF
Usage: $0 [ options ]
Options:
	-s size [default $SIZE]
	-c immuclient_binary [default $IMMUCLIENT]
	-a immuadmin_binary [default $IMMUADMIN]
	-i immudb_binary [default $IMMUDB]
	-d data directory path [default $DATADIR]
	-x client_application [default $CLIENT_APPLICATION
	-A address [default $ADDR]
	-D database_name [default $DB]
	-R retention [default $RETENTION]
	-I interval [default $INTERVAL]
	-F frequency [default $FREQUENCY]
	-T test duration [default $DURATION]
EOF
exit 1
}

while getopts "s:c:a:i:d:x:A:D:R:I:F:T:h" opt; do
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
		CLIENT_APPLICATION=${OPTARG}
		;;
	A)
		ADDR=${OPTARG}
		;;
	D)
		DB=${OPTARG}
		;;
	R)
		RETENTION=${OPTARG}
		;;
	I)
		INTERVAL=${OPTARG}
		;;
	F)
		FREQUENCY=${OPTARG}
		;;
	T)
		DURATION=${OPTARG}
		;;
	*) usage
		;;
	esac
done

function secs() {
	T=$1
	case ${T:0-1} in
	[0123456789]) k=1; Tx=$T ;;
	s) k=1 ;;
	m) k=60 ;;
	h) k=3600 ;;
	d) k=86400 ;;
	*) echo "ERROR" ; exit 1
	esac
	if [ -z "$Tx" ]
	then
	Tx=${T:0:-1}
	fi
	echo $((Tx * k))
}

sDURATION=$(secs $DURATION)
sRETENTION=$(secs $RETENTION)
sFREQUENCY=$(secs $FREQUENCY)
mkdir -p $DATADIR
rm -rf $DATADIR/*

$IMMUDB --dir $DATADIR/data -a $ADDR 2>/dev/null &
PRIMARY_PID=$!
sleep 5
while ! nc -z $ADDR 3322
do
  echo "Waiting immudb"
  sleep 1
done

echo "Launching $CLIENT_APPLICATION"

T0=`date +%s`
$CLIENT_APPLICATION -addr $ADDR -retention $RETENTION -interval $INTERVAL -frequency $FREQUENCY  -db $DB&
CLIENT_PID=$!
T1=`date +%s`

while [ $((T1-T0)) -lt $sDURATION ]
do
sleep 60
echo "Waiting for duration"
T1=`date +%s`
done

kill $CLIENT_PID
kill $PRIMARY_PID

LAST=$(ls -t $DATADIR/data/$DB/val_0|tail -n 1)
LAST_TIME=$(stat -c%Z $DATADIR/data/$DB/val_0/$LAST)

ls -l $DATADIR/data/$DB/val_0
date

if [ $((T1-LAST_TIME)) -gt $((sRETENTION+sFREQUENCY)) ] #
then
echo "RESULT: FAILED: $LAST is older ( $((T1-LAST_TIME)) ) than $RETENTION+$FREQUENCY"
exit 99
else
echo "RESULT: OK: ( $LAST $((T1-LAST_TIME)) ) less than $RETENTION+$FREQUENCY"
exit 0
fi

