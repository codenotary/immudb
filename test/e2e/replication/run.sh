#!/bin/sh
cd "$(dirname "$0")"
BASE=/src/immudb
./replic.sh -x /src/tools/stresser2/stresser2 -a $BASE/immuadmin -i $BASE/immudb -c $BASE/immuclient
