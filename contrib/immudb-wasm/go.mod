module github.com/codenotary/immudb/contrib/immudb-wasm

go 1.25.0

require github.com/codenotary/immudb v1.11.1

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/prometheus/client_golang v1.12.2 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.32.1 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)

// Build the WASM export layer against this checkout of immudb (embedded/store,
// embedded/sql and the wasip1 fileutils shim) rather than a published release.
replace github.com/codenotary/immudb => ../../
