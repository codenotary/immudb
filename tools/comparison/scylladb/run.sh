#!/bin/sh
python3 -s /docker-entrypoint.py --listen-address 127.0.0.1 &
sleep 30 # very slow startup before client requests go through
cqlsh 127.0.0.1 < schema
time python bench.py
