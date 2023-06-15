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
  while ! nc -z 127.0.0.1 3322 ; do echo "waiting"; sleep 1; done
  echo -n immudb | $IMMUADMIN login immudb
  $IMMUADMIN database create perf --max-commit-concurrency 120 # --embedded-values=true --prealloc-files=true
  ) &
  $IMMUDB --dir /var/lib/immudb --web-server
  ;;

  asyncmain)
  (
  while ! nc -z 127.0.0.1 3322 ; do echo "waiting"; sleep 1; done
  echo -n immudb | $IMMUADMIN login immudb
  $IMMUADMIN database create perf --max-commit-concurrency 120
  ) &
  $IMMUDB --dir /var/lib/immudb --max-sessions 120 --web-server
  ;;
  asyncreplica)
  (
  while ! nc -z 127.0.0.1 3322 ; do echo "waiting"; sleep 1; done
  echo -n immudb | $IMMUADMIN login immudb
  $IMMUADMIN database create perf \
    --max-commit-concurrency 120 \
    --replication-is-replica \
    --replication-primary-database perf \
    --replication-primary-host immudb-async-main \
    --replication-primary-password immudb \
    --replication-primary-port 3322 \
    --replication-primary-username immudb
  ) &
  $IMMUDB --dir /var/lib/immudb --web-server
  ;;

  syncmain)
  (
  while ! nc -z 127.0.0.1 3322 ; do echo "waiting"; sleep 1; done
  echo -n immudb | $IMMUADMIN login immudb
  $IMMUADMIN database create perf --max-commit-concurrency 120 \
    --replication-sync-enabled \
    --replication-sync-acks 1
  ) &
  $IMMUDB --dir /var/lib/immudb --max-sessions 120
  ;;
  syncreplica)
  (
  while ! nc -z 127.0.0.1 3322 ; do echo "waiting"; sleep 1; done
  echo -n immudb | $IMMUADMIN login immudb
  $IMMUADMIN database create perf \
    --max-commit-concurrency 120 \
    --replication-sync-enabled \
    --replication-is-replica \
    --replication-primary-database perf \
    --replication-primary-host immudb-sync-main \
    --replication-primary-password immudb \
    --replication-primary-port 3322 \
    --replication-primary-username immudb
  ) &
  $IMMUDB --dir /var/lib/immudb
  ;;
  *)
  echo "Wrong startup mode ($MODE)"
  exit 1
  ;;
esac

