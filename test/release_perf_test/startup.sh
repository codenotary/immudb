#!/bin/sh
IMMUDB=/usr/local/bin/immudb

if [ -z "$1" ]
then
MODE="standalone"
else
MODE=$1
fi

echo "Startup mode '$MODE'"

case $MODE in
  standalone)
  $IMMUDB --dir /usr/lib/immudb
  ;;
  syncmain)
  $IMMUDB --dir /usr/lib/immudb
  ;;
  asyncmain)
  $IMMUDB --dir /usr/lib/immudb
  ;;
  syncreplica)
  $IMMUDB --dir /usr/lib/immudb
  ;;
  asyncreplica)
  $IMMUDB --dir /usr/lib/immudb
  ;;
  *)
  echo "Wrong startup mode ($MODE)"
  exit 1
  ;;
esac

