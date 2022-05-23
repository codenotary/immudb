# Copyright 2022 CodeNotary, Inc. All rights reserved. 											\
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

VERSION=1.3.0
DEFAULT_WEBCONSOLE_VERSION=1.0.15
SERVICES=immudb immuadmin immuclient
TARGETS=linux/amd64 windows/amd64 darwin/amd64 linux/s390x linux/arm64 freebsd/amd64 darwin/arm64

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
V_LDFLAGS_COMMON := -s -X "github.com/codenotary/immudb/cmd/version.Version=${VERSION}" \
					-X "github.com/codenotary/immudb/cmd/version.Commit=${V_COMMIT}" \
					-X "github.com/codenotary/immudb/cmd/version.BuiltBy=${V_BUILT_BY}"\
					-X "github.com/codenotary/immudb/cmd/version.BuiltAt=${V_BUILT_AT}"
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
ifdef WEBCONSOLE
webconsole: ./webconsole/dist
	env -u GOOS -u GOARCH $(GO) generate $(IMMUDB_BUILD_TAGS) ./webconsole
else
webconsole:
	env -u GOOS -u GOARCH $(GO) generate $(IMMUDB_BUILD_TAGS) ./webconsole
endif

# To be called manually to update the default webconsole
.PHONY: webconsole/default
webconsole/default:
	$(GO) generate ./webconsole

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

.PHONY: test
test:
	$(GO) vet ./...
	$(GO) test -failfast ./...

.PHONY: test-client
test-client:
	$(GO) test -failfast ./pkg/client

# To view coverage as HTML run: go tool cover -html=coverage.txt
.PHONY: coverage
coverage:
	go-acc ./... --covermode=atomic --ignore=test,immuclient,immuadmin,helper,cmdtest,sservice,version
	cat coverage.txt | grep -v "schema.pb" | grep -v "immuclient" | grep -v "immuadmin" | grep -v "helper" | grep -v "cmdtest" | grep -v "sservice" | grep -v "version" > coverage.out
	$(GO) tool cover -func coverage.out


.PHONY: build/codegen
build/codegen:
	$(PROTOC) -I pkg/api/schema/ pkg/api/schema/schema.proto \
	-I$(GOPATH)/pkg/mod \
	-I$(GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.16.0/third_party/googleapis \
	-I$(GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.16.0 \
	--go_out=plugins=grpc,paths=source_relative:pkg/api/schema

	$(PROTOC) -I pkg/api/schema/ pkg/api/schema/schema.proto \
	-I$(GOPATH)/pkg/mod \
	-I$(GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.16.0/third_party/googleapis \
	-I$(GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.16.0 \
  	--grpc-gateway_out=logtostderr=true,paths=source_relative:pkg/api/schema \

	$(PROTOC) -I pkg/api/schema/ pkg/api/schema/schema.proto \
	-I$(GOPATH)/pkg/mod \
	-I$(GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.16.0/third_party/googleapis \
	-I$(GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.16.0 \
  	--swagger_out=logtostderr=true:pkg/api/schema

	$(PROTOC) -I pkg/api/schema/ pkg/api/schema/schema.proto \
	-I$(GOPATH)/pkg/mod \
	-I$(GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.16.0/third_party/googleapis \
	-I$(GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.16.0 \
	--doc_out=pkg/api/schema --doc_opt=markdown,docs.md \

.PHONY: clean
clean:
	rm -rf immudb immuclient immuadmin immutest ./webconsole/dist

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

.PHONY: clean/dist
clean/dist:
	rm -Rf ./dist

# WEBCONSOLE=default make dist
# it enables by default webconsole
.PHONY: dist
dist: webconsole dist/binaries
	@echo 'Binaries generation complete. Now vcn signature is needed.'

.PHONY: dist/binaries
dist/binaries:
		mkdir -p dist; \
		for service in ${SERVICES}; do \
    		for os_arch in ${TARGETS}; do \
    			goos=`echo $$os_arch|sed 's|/.*||'`; \
    			goarch=`echo $$os_arch|sed 's|^.*/||'`; \
    		    GOOS=$$goos GOARCH=$$goarch $(GO) build -tags webconsole -v -ldflags '${V_LDFLAGS_COMMON}' -o ./dist/$$service-v${VERSION}-$$goos-$$goarch ./cmd/$$service/$$service.go ; \
    		done; \
    		CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -tags webconsole -a -ldflags '${V_LDFLAGS_STATIC} -extldflags "-static"' -o ./dist/$$service-v${VERSION}-linux-amd64-static ./cmd/$$service/$$service.go ; \
    		mv ./dist/$$service-v${VERSION}-windows-amd64 ./dist/$$service-v${VERSION}-windows-amd64.exe; \
    	done


.PHONY: dist/winsign
dist/winsign:
	for service in ${SERVICES}; do \
		echo ${SIGNCODE_PVK_PASSWORD} | $(DOCKER) run --rm -i \
			-v ${PWD}/dist:/dist \
			-v ${SIGNCODE_SPC}:/certs/f.spc:ro \
			-v ${SIGNCODE_PVK}:/certs/f.pvk:ro \
			mono:6.8.0 signcode \
			-spc /certs/f.spc -v /certs/f.pvk \
			-a sha1 -$ commercial \
			-n "CodeNotary $$service" \
			-i https://codenotary.io/ \
			-t http://timestamp.comodoca.com -tr 10 \
			dist/$$service-v${VERSION}-windows-amd64.exe; \
		rm ./dist/$$service-v${VERSION}-windows-amd64.exe.bak -f; \
	done

.PHONY: dist/sign
dist/sign:
	for f in ./dist/*; do cas n $$f; printf "\n\n"; done


.PHONY: dist/binary.md
dist/binary.md:
	@for f in ./dist/*; do \
		ff=$$(basename $$f); \
		shm_id=$$(sha256sum $$f | awk '{print $$1}'); \
		printf "[$$ff](https://github.com/vchain-us/immudb/releases/download/v${VERSION}/$$ff) | $$shm_id \n" ; \
	done

./webconsole/dist:
ifeq (${WEBCONSOLE}, default)
	@echo "Using webconsole version: ${DEFAULT_WEBCONSOLE_VERSION}"
	curl -L https://github.com/codenotary/immudb-webconsole/releases/download/v${DEFAULT_WEBCONSOLE_VERSION}/immudb-webconsole.tar.gz | tar -xvz -C webconsole
else ifeq (${WEBCONSOLE}, latest)
	@echo "Using webconsole version: latest"
	curl -L https://github.com/codenotary/immudb-webconsole/releases/latest/download/immudb-webconsole.tar.gz | tar -xvz -C webconsole
else ifeq (${WEBCONSOLE}, 1)
	@echo "The meaning of the 'WEBCONSOLE' variable has changed, please specify one of:"
	@echo "  default   - to use the default version of the webconsole for this immudb release"
	@echo "  latest    - to use the latest version of the webconsole"
	@echo "  <version> - to use a specific version of the webconsole"
	@exit 1
else
	@echo "Using webconsole version: ${WEBCONSOLE}"
	curl -L https://github.com/codenotary/immudb-webconsole/releases/download/v${WEBCONSOLE}/immudb-webconsole.tar.gz | tar -xvz -C webconsole
endif

########################## releases scripts end ########################################################################
