#!/bin/sh
echo -n "Starting scylladb..."
python3 -s /docker-entrypoint.py --listen-address 127.0.0.1 > /dev/null 2>&1 &
while ! echo | cqlsh 127.0.0.1 > /dev/null 2>&1; do echo -n "."; sleep 1; done
echo
cqlsh 127.0.0.1 < schema
python -u bench.py
