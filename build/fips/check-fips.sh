# Pass the path to the executable to check for FIPS compliance
exe=$1

if [ "$(go tool nm "${exe}" | grep -c '_Cfunc__goboringcrypto_')" -eq 0 ]; then
    # Asserts that executable is using FIPS-compliant boringcrypto
    echo "${exe}: missing goboring symbols" >&2
    exit 1
fi

echo "${exe} is FIPS-compliant"