#!/bin/sh
IMMUDB=/usr/local/bin/immudb
IMMUADMIN=/usr/local/bin/immuadmin

if [ -z "$1" ]
then
MODE="standalone"
else
MODE=$1
fi

echo "Startup mode '$MODE'"

case $MODE in
  standalone)
  (
  sleep 3
  echo -n immudb | $IMMUADMIN login immudb
  $IMMUADMIN database create perf --max-commit-concurrency 120
  ) &
  $IMMUDB --dir /usr/lib/immudb
  ;;

  asyncmain)
  (
  sleep 3
  echo -n immudb | $IMMUADMIN login immudb
  $IMMUADMIN database create perf --max-commit-concurrency 120
  ) &
  $IMMUDB --dir /usr/lib/immudb
  ;;
  asyncreplica)
  (
  sleep 3
  echo -n immudb | $IMMUADMIN login immudb
  $IMMUADMIN database create perf \
    --replication-is-replica \
    --replication-primary-database perf \
    --replication-primary-host immudb-async-main \
    --replication-primary-password immudb \
    --replication-primary-port 3322 \
    --replication-primary-username immudb
  ) &
  $IMMUDB --dir /usr/lib/immudb
  ;;

  syncmain)
  $IMMUDB --dir /usr/lib/immudb
  ;;
  syncreplica)
  $IMMUDB --dir /usr/lib/immudb
  ;;
  *)
  echo "Wrong startup mode ($MODE)"
  exit 1
  ;;
esac

