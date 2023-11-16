#!/bin/sh
if [ -f /etc/sysconfig/immudb ]
then
    source /etc/sysconfig/immudb
fi
if [ -z "$IMMUDB_ADMIN_PASSWORD" ]
then
   IMMUDB_ADMIN_PASSWORD=`tr -cd '[:alnum:].,:;/@_=' < /dev/urandom|head -c 16`
   echo "Generated immudb password: $IMMUDB_ADMIN_PASSWORD"
fi
export IMMUDB_ADMIN_PASSWORD
exec $@
