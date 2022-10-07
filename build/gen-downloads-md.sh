#!/bin/sh

set -eu

cd "$(dirname "$0")/../dist/"

VERSION="$1"

generate_checksums_for()
{
    cat <<EOF

**$1 Binaries**

File | SHA256
------------- | -------------
$(
    for f in $1*; do
        ff="$(basename $f)"
		shm_id="$(sha256sum "$f" | awk '{print $1}')"
		printf "[$ff](https://github.com/codenotary/immudb/releases/download/v${VERSION}/$ff) | $shm_id \n"
    done
)
EOF
}

cat <<EOF
# Downloads

**Docker image**
https://hub.docker.com/r/codenotary/immudb


EOF

generate_checksums_for immudb
generate_checksums_for immuclient
generate_checksums_for immuadmin
