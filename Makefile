# Copyright 2019-2020 vChain, Inc. 											\
																			\
Licensed under the Apache License, Version 2.0 (the "License"); 			\
you may not use this file except in compliance with the License. 			\
You may obtain a copy of the License at 									\
																			\
	http://www.apache.org/licenses/LICENSE-2.0 								\
																			\
Unless required by applicable law or agreed to in writing, software 		\
distributed under the License is distributed on an "AS IS" BASIS, 			\
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.	\
See the License for the specific language governing permissions and 		\
limitations under the License.

export GO111MODULE=on

SHELL=/bin/bash -o pipefail

VERSION=0.6.0
TARGETS=linux/amd64 windows/amd64 darwin/amd64 linux/s390x linux/ppc64le linux/arm64
#TARGETS=linux/arm-7 is not supported due to int -> maxInt64 assignment
IMMUDBEXE=immudb-v${VERSION}-windows-amd64.exe
SETUPEXE=codenotary_immudb_v${VERSION}_setup.exe


PWD = $(shell pwd)
GO ?= go
DOCKER ?= docker
PROTOC ?= protoc
STRIP = strip
#~~~> Binaries versions
V_COMMIT := $(shell git rev-parse HEAD)
username := $(shell git config user.name)

V_BUILT_BY := "${username,,}"
V_BUILT_AT := $(shell date +%s)
V_LDFLAGS_COMMON := -X "github.com/codenotary/immudb/cmd/version.Commit=$(V_COMMIT)" -X "github.com/codenotary/immudb/cmd/version.BuiltBy=$(V_BUILT_BY)" -X "github.com/codenotary/immudb/cmd/version.BuiltAt=$(V_BUILT_AT)"

V_COMMON := $(shell git tag --sort=-version:refname | head -n 1)
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
all: immudb immuclient immugw immuadmin immutest
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
	rm -f immudb immuclient immugw immuadmin immutest

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

.PHONY: CHANGELOG.md
CHANGELOG.md:
	git-chglog -o CHANGELOG.md

.PHONY: CHANGELOG.md.next-tag
CHANGELOG.md.next-tag:
	git-chglog -o CHANGELOG.md --next-tag v${VERSION}

.PHONY: build/xgo
build/xgo:
	$(DOCKER) build \
			-f ./build/xgo/Dockerfile \
			-t immudb-xgo \
			--pull=true \
			./build/xgo

.PHONY: build/makensis
build/makensis:
	$(DOCKER) build \
		-f ./build/makensis/Dockerfile \
		-t immudb-makensis \
		./build/makensis

.PHONY: clean/dist
clean/dist:
	rm -Rf ./dist

.PHONY: dist
dist: clean/dist build/xgo
	mkdir -p dist
	$(GO) build -a -tags netgo -ldflags '${LDFLAGS_STATIC}' \
			-o ./dist/immudb-v${VERSION}-linux-amd64-static \
     		./cmd/immudb
	$(DOCKER) run --rm \
			-v "${PWD}/dist:/dist" \
			-v "${PWD}:/source:ro" \
			-e GO111MODULE=on \
			-e FLAG_LDFLAGS="-s ${V_LDFLAGS_COMMON}" \
			-e TARGETS="${TARGETS}" \
			-e PACK=cmd/immudb \
			-e OUT=immudb-v${VERSION} \
			immudb-xgo .
	#mv ./dist/immudb-v${VERSION}-linux-arm-7 ./dist/immudb-v${VERSION}-linux-arm
	mv ./dist/immudb-v${VERSION}-windows-4.0-amd64.exe ./dist/${IMMUDBEXE}
	mv ./dist/immudb-v${VERSION}-darwin-10.6-amd64 ./dist/immudb-v${VERSION}-darwin-amd64

.PHONY: dist/${IMMUDBEXE} dist/${SETUPEXE}
dist/${IMMUDBEXE} dist/${SETUPEXE}:
	echo ${SIGNCODE_PVK_PASSWORD} | $(DOCKER) run --rm -i \
		-v ${PWD}/dist:/dist \
		-v ${SIGNCODE_SPC}:/certs/f.spc:ro \
		-v ${SIGNCODE_PVK}:/certs/f.pvk:ro \
		mono:6.8.0 signcode \
		-spc /certs/f.spc -v /certs/f.pvk \
		-a sha1 -$ commercial \
		-n "CodeNotary immudb" \
		-i https://codenotary.io/ \
		-t http://timestamp.comodoca.com -tr 10 \
		$@
	rm -Rf $@.bak

.PHONY: dist/NSIS
dist/NSIS: build/makensis
	mkdir -p dist/NSIS
	cp -f ./dist/${IMMUDBEXE} ./dist/NSIS/immudb.exe
	cp -f ./build/NSIS/* ./dist/NSIS/
	sed -e "s/{IMMUDB_VERSION}/v${VERSION}/g" ./build/NSIS/setup.nsi > ./dist/NSIS/setup.nsi
	$(DOCKER) run --rm \
			-v ${PWD}/dist/NSIS/:/app \
			immudb-makensis /app/setup.nsi
	cp ./dist/NSIS/*_setup.exe ./dist/
	rm -Rf ./dist/NSIS

.PHONY: dist/sign
dist/sign: vendor immudb
	for f in ./dist/*; do vcn sign -p $$f; printf "\n\n"; done

.PHONY: dist/all
dist/all: dist dist/${IMMUDBEXE} dist/NSIS dist/${SETUPEXE}

.PHONY: dist/binary.md
dist/binary.md:
	@for f in ./dist/*; do \
		ff=$$(basename $$f); \
		shm_id=$$(sha256sum $$f | awk '{print $$1}'); \
		printf "[$$ff](https://github.com/vchain-us/immudb/releases/download/v${VERSION}/$$ff) | $$shm_id \n" ; \
	done
