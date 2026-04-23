# Pass the path to the executable to check for FIPS compliance
exe=$1

if [ "$(go tool nm "${exe}" | grep -c 'crypto/internal/fips140')" -eq 0 ]; then
    # Asserts that executable is using Go 1.24+ native FIPS 140-3 module
    echo "${exe}: missing FIPS 140 symbols" >&2
    exit 1
fi

echo "${exe} is FIPS-compliant"