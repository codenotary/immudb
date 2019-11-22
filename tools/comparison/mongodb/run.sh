#!/bin/bash
set -Eeuo pipefail

docker-entrypoint.sh mongod &

sleep 5 

mongo load.js