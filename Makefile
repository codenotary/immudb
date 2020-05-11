export GO111MODULE=on

SHELL=/bin/bash -o pipefail
PWD = $(shell pwd)
GO ?= go
DOCKER ?= docker
PROTOC ?= protoc
STRIP = strip
#~~~> Binaries versions
V_COMMIT := $(shell git rev-parse HEAD)
V_BUILT_BY := $(shell git config user.name)
V_BUILT_AT := $(shell date)
V_LDFLAGS_COMMON := -X "github.com/codenotary/immudb/cmd/version.Commit=$(V_COMMIT)" -X "github.com/codenotary/immudb/cmd/version.BuiltBy=$(V_BUILT_BY)" -X "github.com/codenotary/immudb/cmd/version.BuiltAt=$(V_BUILT_AT)"

V_COMMON := v.0.0.1
V_IMMUCLIENT := $(V_COMMON)
V_IMMUADMIN := $(V_COMMON)
V_IMMUDB := $(V_COMMON)
V_IMMUGW := $(V_COMMON)
V_IMMUTEST := $(V_COMMON)

V_IMMUCLIENT_LDFLAGS := -X "github.com/codenotary/immudb/cmd/version.Version=$(V_IMMUCLIENT)" $(V_LDFLAGS_COMMON)
V_IMMUADMIN_LDFLAGS := -X "github.com/codenotary/immudb/cmd/version.Version=$(V_IMMUADMIN)" $(V_LDFLAGS_COMMON)
V_IMMUDB_LDFLAGS := -X "github.com/codenotary/immudb/cmd/version.Version=$(V_IMMUDB)" $(V_LDFLAGS_COMMON)
V_IMMUGW_LDFLAGS := -X "github.com/codenotary/immudb/cmd/version.Version=$(V_IMMUGW)" $(V_LDFLAGS_COMMON)
V_IMMUTEST_LDFLAGS := -X "github.com/codenotary/immudb/cmd/version.Version=$(V_IMMUTEST)" $(V_LDFLAGS_COMMON)
#<~~~
.PHONY: all
all: immudb immuclient immugw immuadmin immutest bm
	@echo 'Build successful, now you can make the manuals or check the status of the database with immuadmin.'

.PHONY: rebuild
rebuild: clean build/codegen all

.PHONY: immuclient
immuclient:
	$(GO) build -v -ldflags '$(V_IMMUCLIENT_LDFLAGS)' ./cmd/immuclient

.PHONY: immuadmin
immuadmin:
	$(GO) build -v -ldflags '$(V_IMMUADMIN_LDFLAGS)' ./cmd/immuadmin

.PHONY: immudb
immudb:
	$(GO) build -v -ldflags '$(V_IMMUDB_LDFLAGS)' ./cmd/immudb

.PHONY: immugw
immugw:
	$(GO) build -v -ldflags '$(V_IMMUGW_LDFLAGS)' ./cmd/immugw

.PHONY: immutest
immutest:
	$(GO) build -v -ldflags '$(V_IMMUTEST_LDFLAGS)' ./cmd/immutest

.PHONY: immuclient-static
immuclient-static:
	CGO_ENABLED=0 $(GO) build -a -tags netgo -ldflags '${LDFLAGS} $(V_IMMUCLIENT_LDFLAGS) -extldflags  "-static"' ./cmd/immuclient

.PHONY: immuadmin-static
immuadmin-static:
	CGO_ENABLED=0 $(GO) build -a -tags netgo -ldflags '${LDFLAGS} $(V_IMMUADMIN_LDFLAGS) -extldflags "-static"' ./cmd/immuadmin

.PHONY: immudb-static
immudb-static:
	CGO_ENABLED=0 $(GO) build -a -tags netgo -ldflags '${LDFLAGS} $(V_IMMUDB_LDFLAGS) -extldflags "-static"' ./cmd/immudb

.PHONY: immugw-static
immugw-static:
	CGO_ENABLED=0 $(GO) build -a -tags netgo -ldflags '${LDFLAGS} $(V_IMMUGW_LDFLAGS) -extldflags "-static"' ./cmd/immugw

.PHONY: immutest-static
immutest-static:
	CGO_ENABLED=0 $(GO) build -a -tags netgo -ldflags '${LDFLAGS} $(V_IMMUTEST_LDFLAGS) -extldflags "-static"' ./cmd/immutest

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
	-I${GOPATH}/pkg/mod/github.com/dgraph-io/badger/v2@v2.0.0-20200408100755-2e708d968e94 \
	-I${GOPATH}/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.12.2 \
	--go_out=plugins=grpc,paths=source_relative:pkg/api/schema

	$(PROTOC) -I pkg/api/schema/ pkg/api/schema/schema.proto \
	-I${GOPATH}/pkg/mod \
	-I${GOPATH}/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.12.2/third_party/googleapis \
	-I${GOPATH}/pkg/mod/github.com/dgraph-io/badger/v2@v2.0.0-20200408100755-2e708d968e94 \
	-I${GOPATH}/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.12.2 \
  	--grpc-gateway_out=logtostderr=true,paths=source_relative:pkg/api/schema \

	$(PROTOC) -I pkg/api/schema/ pkg/api/schema/schema.proto \
	-I${GOPATH}/pkg/mod \
	-I${GOPATH}/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.12.2/third_party/googleapis \
	-I${GOPATH}/pkg/mod/github.com/dgraph-io/badger/v2@v2.0.0-20200408100755-2e708d968e94 \
	-I${GOPATH}/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.12.2 \
  	--swagger_out=logtostderr=true:pkg/api/schema \

.PHONY: clean
clean:
	rm -f immudb immuclient immugw immuadmin immutest bm

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

.PHONY: man
man:
	$(GO) run ./cmd/immuclient mangen ./cmd/docs/man/immuclient
	$(GO) run ./cmd/immuadmin mangen ./cmd/docs/man/immuadmin
	$(GO) run ./cmd/immudb mangen ./cmd/docs/man/immudb
	$(GO) run ./cmd/immugw mangen ./cmd/docs/man/immugw
	$(GO) run ./cmd/immutest mangen ./cmd/docs/man/immutest

.PHONY: prerequisites
prerequisites:
	wget https://github.com/protocolbuffers/protobuf/releases/download/v3.11.4/protoc-3.11.4-linux-x86_64.zip -O /tmp/protoc.zip
	unzip -o /tmp/protoc.zip -d $(GOPATH)/bin
	rm -rf $(GOPATH)/pkg/mod/google
	mv $(GOPATH)/bin/include/google $(GOPATH)/pkg/mod
	rmdir $(GOPATH)/bin/include
	rm /tmp/protoc.zip
	go get -u github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway
	go get -u github.com/grpc-ecosystem/grpc-gateway/protoc-gen-swagger
	go get -u google.golang.org/grpc
	go get -u github.com/golang/protobuf/
	go get -u github.com/golang/protobuf/proto
	go get -u github.com/golang/protobuf/protoc-gen-go
