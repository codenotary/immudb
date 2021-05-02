# Copyright 2021 CodeNotary, Inc. All rights reserved. 											\
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

VERSION=0.9.2
TARGETS=linux/amd64 windows/amd64 darwin/amd64 linux/s390x
SERVICE_EXE=${SERVICE_NAME}-v${VERSION}-windows-amd64.exe

PWD = $(shell pwd)
GO ?= go
GOPATH ?= $(shell go env GOPATH)
DOCKER ?= docker
PROTOC ?= protoc
STRIP = strip

V_COMMIT := $(shell git rev-parse HEAD)
#V_BUILT_BY := "$(shell echo "`git config user.name`<`git config user.email`>")"
V_BUILT_BY := $(shell git config user.email)
V_BUILT_AT := $(shell date +%s)
V_LDFLAGS_COMMON := -s -X "github.com/codenotary/immudb/cmd/version.Version=$(VERSION)" \
					-X "github.com/codenotary/immudb/cmd/version.Commit=$(V_COMMIT)" \
					-X "github.com/codenotary/immudb/cmd/version.BuiltBy=$(V_BUILT_BY)"\
					-X "github.com/codenotary/immudb/cmd/version.BuiltAt=$(V_BUILT_AT)"
V_LDFLAGS_STATIC := ${V_LDFLAGS_COMMON} \
				  -X github.com/codenotary/immudb/cmd/version.Static=static \
				  -extldflags "-static"
ifdef WEBCONSOLE
  IMMUDB_BUILD_TAGS=-tags webconsole
endif

.PHONY: all
all: immudb immuclient immuadmin immutest
	@echo 'Build successful, now you can make the manuals or check the status of the database with immuadmin.'

.PHONY: rebuild
rebuild: clean build/codegen all

.PHONY: webconsole
webconsole:
	$(GO) generate $(IMMUDB_BUILD_TAGS) ./webconsole

.PHONY: immuclient
immuclient:
	$(GO) build -v -ldflags '$(V_LDFLAGS_COMMON)' ./cmd/immuclient

.PHONY: immuadmin
immuadmin:
	$(GO) build -v -ldflags '$(V_LDFLAGS_COMMON)' ./cmd/immuadmin

.PHONY: immudb
immudb: webconsole
	$(GO) build $(IMMUDB_BUILD_TAGS) -v -ldflags '$(V_LDFLAGS_COMMON)' ./cmd/immudb

.PHONY: immutest
immutest:
	$(GO) build -v -ldflags '$(V_LDFLAGS_COMMON)' ./cmd/immutest

.PHONY: immuclient-static
immuclient-static:
	CGO_ENABLED=0 $(GO) build -a -ldflags '$(V_LDFLAGS_STATIC) -extldflags  "-static"' ./cmd/immuclient

.PHONY: immuadmin-static
immuadmin-static:
	CGO_ENABLED=0 $(GO) build -a -ldflags '$(V_LDFLAGS_STATIC) -extldflags "-static"' ./cmd/immuadmin

.PHONY: immudb-static
immudb-static: webconsole
	CGO_ENABLED=0 $(GO) build $(IMMUDB_BUILD_TAGS) -a -ldflags '$(V_LDFLAGS_STATIC) -extldflags "-static"' ./cmd/immudb

.PHONY: immutest-static
immutest-static:
	CGO_ENABLED=0 $(GO) build -a -ldflags '$(V_LDFLAGS_STATIC) -extldflags "-static"' ./cmd/immutest

.PHONY: vendor
vendor:
	$(GO) mod vendor

# To view coverage as HTML run: go tool cover -html=coverage.txt
.PHONY: test
test:
	$(GO) vet ./...
	$(GO) test -failfast $(go list ./... | grep -v test | grep -v immuclient | grep -v immuadmin ) --race -coverprofile=coverage.txt -covermode=atomic ./...

.PHONY: coverage
coverage:
	go-acc ./... --covermode=atomic --ignore=test,immuclient,immuadmin,helper,cmdtest,sservice,version
	cat coverage.txt | grep -v "schema.pb" | grep -v "immuclient" | grep -v "immuadmin" | grep -v "helper" | grep -v "cmdtest" | grep -v "sservice" | grep -v "version" > coverage.out
	$(GO) tool cover -func coverage.out


.PHONY: build/codegen
build/codegen:
	$(PROTOC) -I pkg/api/schema/ pkg/api/schema/schema.proto \
	-I$(GOPATH)/pkg/mod \
	-I$(GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.14.4/third_party/googleapis \
	-I$(GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.14.4 \
	--go_out=plugins=grpc,paths=source_relative:pkg/api/schema

	$(PROTOC) -I pkg/api/schema/ pkg/api/schema/schema.proto \
	-I$(GOPATH)/pkg/mod \
	-I$(GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.14.4/third_party/googleapis \
	-I$(GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.14.4 \
  	--grpc-gateway_out=logtostderr=true,paths=source_relative:pkg/api/schema \

	$(PROTOC) -I pkg/api/schema/ pkg/api/schema/schema.proto \
	-I$(GOPATH)/pkg/mod \
	-I$(GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.14.4/third_party/googleapis \
	-I$(GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.14.4 \
  	--swagger_out=logtostderr=true:pkg/api/schema

	$(PROTOC) -I pkg/api/schema/ pkg/api/schema/schema.proto \
	-I$(GOPATH)/pkg/mod \
	-I$(GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.14.4/third_party/googleapis \
	-I$(GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.14.4 \
	--doc_out=pkg/api/schema --doc_opt=markdown,docs.md \

.PHONY: clean
clean:
	rm -f immudb immuclient immuadmin immutest

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
	$(GO) run ./cmd/immutest mangen ./cmd/docs/man/immutest

.PHONY: prerequisites
prerequisites:
	$(GO) mod tidy
	cat tools.go | grep _ | awk -F'"' '{print $$2}' | xargs -tI % go install %

########################## releases scripts ############################################################################
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
			-t ${SERVICE_NAME}-xgo \
			--pull=true \
			./build/xgo

.PHONY: build/makensis
build/makensis:
	$(DOCKER) build \
		-f ./build/makensis/Dockerfile \
		-t ${SERVICE_NAME}-makensis \
		./build/makensis

.PHONY: clean/dist
clean/dist:
	rm -Rf ./dist

.PHONY: dist
dist: clean/dist build/xgo
	mkdir -p dist
	CGO_ENABLED=0 $(GO) build -a -ldflags '${V_LDFLAGS_STATIC}' \
			-o ./dist/${SERVICE_NAME}-v${VERSION}-linux-amd64-static \
     		./cmd/${SERVICE_NAME}
	$(DOCKER) run --rm \
			-v "${PWD}/dist:/dist" \
			-v "${PWD}:/source:ro" \
			-e GO111MODULE=on \
			-e FLAG_LDFLAGS="-s ${V_LDFLAGS_COMMON}" \
			-e TARGETS="${TARGETS}" \
			-e PACK=cmd/${SERVICE_NAME} \
			-e OUT=${SERVICE_NAME}-v${VERSION} \
			${SERVICE_NAME}-xgo .
	mv ./dist/${SERVICE_NAME}-v${VERSION}-windows-4.0-amd64.exe ./dist/${SERVICE_EXE}
	mv ./dist/${SERVICE_NAME}-v${VERSION}-darwin-10.6-amd64 ./dist/${SERVICE_NAME}-v${VERSION}-darwin-amd64

.PHONY: dist/${SERVICE_EXE}
dist/${SERVICE_EXE}:
	echo ${SIGNCODE_PVK_PASSWORD} | $(DOCKER) run --rm -i \
		-v ${PWD}/dist:/dist \
		-v ${SIGNCODE_SPC}:/certs/f.spc:ro \
		-v ${SIGNCODE_PVK}:/certs/f.pvk:ro \
		mono:6.8.0 signcode \
		-spc /certs/f.spc -v /certs/f.pvk \
		-a sha1 -$ commercial \
		-n "CodeNotary ${SERVICE_NAME}" \
		-i https://codenotary.io/ \
		-t http://timestamp.comodoca.com -tr 10 \
		$@
	rm -Rf $@.bak

.PHONY: dist/sign
dist/sign: vendor ${SERVICE_NAME}
	for f in ./dist/*; do vcn sign -p $$f; printf "\n\n"; done

# SERVICE_NAME=immudb|immuclient|immuadmin SIGNCODE_PVK_PASSWORD=<pvk password> SIGNCODE_PVK=<path to vchain.pvk> SIGNCODE_SPC=<path to vchain.spc> make dist/all
.PHONY: dist/all
dist/all: dist dist/${SERVICE_EXE}

.PHONY: dist/binary.md
dist/binary.md:
	@for f in ./dist/*; do \
		ff=$$(basename $$f); \
		shm_id=$$(sha256sum $$f | awk '{print $$1}'); \
		printf "[$$ff](https://github.com/vchain-us/immudb/releases/download/v${VERSION}/$$ff) | $$shm_id \n" ; \
	done
########################## releases scripts end ########################################################################
