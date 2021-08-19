#!/bin/sh

if [ -z "$IMMUDB_ADMIN_PASSWORD" ]
then
   export IMMUDB_ADMIN_PASSWORD=`tr -cd '[:alnum:].,:;/@_=' < /dev/urandom|head -c 16`
   echo "Generated immudb password: $IMMUDB_ADMIN_PASSWORD"
fi

exec $@
