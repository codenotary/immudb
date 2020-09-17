#!/bin/bash

IMMUCLIENT=../../immuclient
IMMUADMIN=../../immuadmin
IMMUDB=../../immudb

export IMMUDB_ADMIN_PASSWORD="IfHDozxly.2ERYfKg"

# purge existing data
rm -rf data
# starts the database
$IMMUDB &
PID=$!
sleep 1
expect adminlogin1.expect ${IMMUDB_ADMIN_PASSWORD}

if [ $? -eq 0 ]; then
	printf "\n\n\033[0;32mSUCCESS!\033[0m\n\n"
	RET=0
else
	printf "\n\n\033[0;31mFailed login using env variable IMMUDB_ADMIN_PASSWORD\033[0m\n\n"
	RET=1
fi

kill $PID
wait $PID
rm -rf data # clear data

exit $RET
