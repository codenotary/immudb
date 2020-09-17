#!/bin/bash
NUM_DATA=5
NUM_DB=2

IMMUCLIENT=../../immuclient
IMMUADMIN=../../immuadmin
IMMUDB=../../immudb

# just in case it was previously set
unset IMMUDB_ADMIN_PASSWORD

# purge existing data
rm -rf data
# starts the database

$IMMUDB &
PID=$!

PWD="daFosdo0."

# login using expect script
expect adminlogin.expect $PWD
expect login.expect $PWD

# create $NUM_DB databases and load $NUM_DATA in each of them
for j in `seq 0 $NUM_DB`
do
	# create db
	IDX=($(echo "$j"|md5sum))
	DBNAME=database${IDX:0:8}
	echo "DATABASE $j: $DBNAME"
	$IMMUADMIN database create $DBNAME
	$IMMUCLIENT use $DBNAME
	# load data
	for i in `seq 0 $NUM_DATA`
	do
	key=($(echo "key:$i"|md5sum))
	value=($(echo "value:$i"|md5sum))
	$IMMUCLIENT safeset $key $value
	done
done

echo "Done loading, now checing"

RET=0

# and now, we check the data:
for j in `seq 0 $NUM_DB`
do
	IDX=($(echo "$j"|md5sum))
	DBNAME=database${IDX:0:8}
	echo "DATABASE $j: $DBNAME"
	$IMMUCLIENT use $DBNAME
	for i in `seq 0 $NUM_DATA`
	do
		echo "Reading $i"
		key=($(echo "key:$i"|md5sum))
		value=($(echo "value:$i"|md5sum))
		RESP=`$IMMUCLIENT getByIndex $i`
		rkey=`echo $RESP|egrep -o "key: [^ ]+"|cut -d ' ' -f 2`
		rvalue=`echo $RESP|egrep -o 'payload:".*"'|cut -d '"' -f 2`
		if [ "$key" != "$rkey" ]
		then
			echo "ERROR WRONG KEY '$key' != '$rkey'"
			RET=-1
			break
		fi
		if [ "$value" != "$rvalue" ]
		then
			echo "ERROR WRONG VALUE '$value' != '$rvalue'"
			RET=-2
			break
		fi

		RESP=`$IMMUCLIENT safeget $key`
		rkey=`echo $RESP|egrep -o "key: [^ ]+"|cut -d ' ' -f 2`
		rvalue=`echo $RESP|egrep -o 'value: [^ ]+'|cut -d ' ' -f 2`
		rverified=`echo $RESP|egrep -o 'verified: [^ ]+'|cut -d ' ' -f 2`
		if [ "$key" != "$rkey" ]
		then
			echo "ERROR WRONG KEY '$key' != '$rkey'"
			RET=-3
			break
		fi
		if [ "$value" != "$rvalue" ]
		then
			echo "ERROR WRONG VALUE '$value' != '$rvalue'"
			RET=-4
			break
		fi
		if [ "$rverified" != "true" ]
		then
			echo "ERROR NOT VERIFIED"
			RET=-5
			break
		fi
	done
done

kill $PID
wait $PID

# purge existing data
rm -rf data

if [ $RET -eq 0 ]
then printf "\n\n\033[0;32mSUCCESS!\033[0m\n\n"
else printf "\n\n\033[0;31mFAIL\033[0m\n\n"
fi

exit $RET
