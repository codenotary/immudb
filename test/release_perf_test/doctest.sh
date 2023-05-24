#!/bin/sh
docker-compose build immudb-perftest immudb-tools
docker-compose up -d immudb-standalone
docker-compose run immudb-tools-web-api -b http://immudb-standalone:8080 --duration 10 -w 1 -s 1
