export GO111MODULE=on

SHELL=/bin/bash -o pipefail
PWD = $(shell pwd)
GO ?= go
DOCKER ?= docker
PROTOC ?= protoc
STRIP = strip

.PHONY: all
all: immu immud immugw

.PHONY: immu
immu:
	$(GO) build ./cmd/immu

.PHONY: immud
immud:
	$(GO) build ./cmd/immud

.PHONY: immugw
immugw:
	$(GO) build ./cmd/immugw

.PHONY: immu-static
immu-static:
	$(GO) build -a -tags netgo -ldflags '${LDFLAGS} -extldflags "-static"' ./cmd/immu

.PHONY: immud-static
immud-static:
	$(GO) build -a -tags netgo -ldflags '${LDFLAGS} -extldflags "-static"' ./cmd/immud

.PHONY: immugw-static
immugw-static:
	$(GO) build -a -tags netgo -ldflags '${LDFLAGS} -extldflags "-static"' ./cmd/immugw

.PHONY: vendor
vendor:
	$(GO) mod vendor

.PHONY: test
test:
	$(GO) vet ./...
	$(GO) test --race ${TEST_FLAGS} ./...

.PHONY: build/codegen
build/codegen:
	$(PROTOC) -I pkg/api/schema/ pkg/api/schema/schema.proto \
	-I${GOPATH}/pkg/mod \
	-I${GOPATH}/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.12.2/third_party/googleapis \
	-I${GOPATH}/pkg/mod/github.com/dgraph-io/badger/v2@v2.0.1 \
	--go_out=plugins=grpc,paths=source_relative:pkg/api/schema

	$(PROTOC) -I pkg/api/schema/ pkg/api/schema/schema.proto \
	-I${GOPATH}/pkg/mod \
	-I${GOPATH}/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.12.2/third_party/googleapis \
	-I${GOPATH}/pkg/mod/github.com/dgraph-io/badger/v2@v2.0.1 \
  	--grpc-gateway_out=logtostderr=true,paths=source_relative:pkg/api/schema \

	$(PROTOC) -I pkg/api/schema/ pkg/api/schema/schema.proto \
	-I${GOPATH}/pkg/mod \
	-I${GOPATH}/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.12.2/third_party/googleapis \
	-I${GOPATH}/pkg/mod/github.com/dgraph-io/badger/v2@v2.0.1 \
  	--swagger_out=logtostderr=true:pkg/api/schema \

.PHONY: clean
clean:
	rm -f immu immud bm

.PHONY: nimmu
nimmu:
	$(GO) build -o nimmu ./tools/nimmu

.PHONY: bm
bm:
	$(GO) build -ldflags '-s -w' ./tools/bm
	$(STRIP) bm

.PHONY: bm/function
bm/function: bm
	./bm function

.PHONY: bm/rpc
bm/rpc: bm
	./bm rpc

.PHONY: bench
bench:
	$(DOCKER) build -t immu_bench -f ./Dockerfile.bench .
	$(DOCKER) run --rm -it immu_bench

.PHONY: tools/comparison/mongodb
tools/comparison/mongodb:
	$(DOCKER) build -t immu_mongodb ./tools/comparison/mongodb
	$(DOCKER) run --rm -it immu_mongodb

.PHONY: tools/comparison/scylladb
tools/comparison/scylladb:
	$(DOCKER) build -t immu_scylladb ./tools/comparison/scylladb
	$(DOCKER) run --rm -it immu_scylladb
