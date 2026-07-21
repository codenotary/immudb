.PHONY: build test test-node vet clean

# Build the wasm artifact into node/immudb.wasm.
build:
	./scripts/build.sh

# Vet the Go export layer under the wasm target.
vet:
	GOOS=wasip1 GOARCH=wasm go vet ./wasm

# Run the Node test suite (requires build first).
test-node:
	cd node && node --test 'test/*.mjs'

# Build then run the Node suite.
test: build test-node

clean:
	rm -f node/immudb.wasm
