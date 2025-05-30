# Copyright 2022 Codenotary Inc. All rights reserved. 											\
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

VERSION=1.9.7
DEFAULT_WEBCONSOLE_VERSION=1.0.18
SERVICES=immudb immuadmin immuclient
TARGETS=linux/amd64 windows/amd64 darwin/amd64 linux/s390x linux/arm64 freebsd/amd64 darwin/arm64
SWAGGER?=false
FIPSENABLED?=false
SWAGGERUIVERSION=4.15.5
SWAGGERUILINK="https://github.com/swagger-api/swagger-ui/archive/refs/tags/v${SWAGGERUIVERSION}.tar.gz"

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
V_LDFLAGS_SYMBOL := -s
V_LDFLAGS_BUILD := -X "github.com/codenotary/immudb/cmd/version.Version=${VERSION}" \
					-X "github.com/codenotary/immudb/cmd/version.Commit=${V_COMMIT}" \
					-X "github.com/codenotary/immudb/cmd/version.BuiltBy=${V_BUILT_BY}"\
					-X "github.com/codenotary/immudb/cmd/version.BuiltAt=${V_BUILT_AT}"
V_LDFLAGS_COMMON := ${V_LDFLAGS_SYMBOL} ${V_LDFLAGS_BUILD}
V_LDFLAGS_STATIC := ${V_LDFLAGS_COMMON} \
				  -X github.com/codenotary/immudb/cmd/version.Static=static \
				  -extldflags "-static"
V_LDFLAGS_FIPS_BUILD = ${V_LDFLAGS_BUILD} \
				  -X github.com/codenotary/immudb/cmd/version.FIPSEnabled=true
V_GO_ENV_FLAGS := GOOS=$(GOOS) GOARCH=$(GOARCH)
V_BUILD_NAME ?= ""
V_BUILD_FLAG = -o $(V_BUILD_NAME)

GRPC_GATEWAY_VERSION := $(shell go list -m -versions github.com/grpc-ecosystem/grpc-gateway | awk -F ' ' '{print $$NF}')
SWAGGER_BUILDTAG=
WEBCONSOLE_BUILDTAG=
FIPS_BUILDTAG=
ifdef WEBCONSOLE
WEBCONSOLE_BUILDTAG=webconsole
endif
ifeq ($(SWAGGER),true)
SWAGGER_BUILDTAG=swagger
endif
ifeq ($(FIPSENABLED),true)
FIPS_BUILDTAG=swagger
endif
IMMUDB_BUILD_TAGS=-tags "$(SWAGGER_BUILDTAG) $(WEBCONSOLE_BUILDTAG) $(FIPS_BUILDTAG)"

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
	$(V_GO_ENV_FLAGS) $(GO) build $(V_BUILD_FLAG) -v -ldflags '$(V_LDFLAGS_COMMON)' ./cmd/immuclient

.PHONY: immuadmin
immuadmin:
	$(V_GO_ENV_FLAGS) $(GO) build $(V_BUILD_FLAG) -v -ldflags '$(V_LDFLAGS_COMMON)' ./cmd/immuadmin

.PHONY: immudb
immudb: webconsole swagger
	$(V_GO_ENV_FLAGS) $(GO) build $(V_BUILD_FLAG) $(IMMUDB_BUILD_TAGS) -v -ldflags '$(V_LDFLAGS_COMMON)' ./cmd/immudb

.PHONY: immutest
immutest:
	$(GO) build -v -ldflags '$(V_LDFLAGS_COMMON)' ./cmd/immutest

.PHONY: immuclient-static
immuclient-static:
	$(V_GO_ENV_FLAGS) CGO_ENABLED=0 $(GO) build $(V_BUILD_FLAG) -a -ldflags '$(V_LDFLAGS_STATIC)' ./cmd/immuclient

.PHONY: immuclient-fips
immuclient-fips:
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 $(GO) build -tags=fips -a -o immuclient -ldflags '$(V_LDFLAGS_FIPS_BUILD)' ./cmd/immuclient/fips
	./build/fips/check-fips.sh immuclient

.PHONY: immuadmin-static
immuadmin-static:
	$(V_GO_ENV_FLAGS) CGO_ENABLED=0 $(GO) build $(V_BUILD_FLAG) -a -ldflags '$(V_LDFLAGS_STATIC)' ./cmd/immuadmin

.PHONY: immuadmin-fips
immuadmin-fips:
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 $(GO) build -tags=fips -a -o immuadmin -ldflags '$(V_LDFLAGS_FIPS_BUILD)' ./cmd/immuadmin/fips
	./build/fips/check-fips.sh immuadmin

.PHONY: immudb-static
immudb-static: webconsole
	$(V_GO_ENV_FLAGS) CGO_ENABLED=0 $(GO) build $(V_BUILD_FLAG) $(IMMUDB_BUILD_TAGS) -a -ldflags '$(V_LDFLAGS_STATIC)' ./cmd/immudb

.PHONY: immudb-fips
immudb-fips: webconsole
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 WEBCONSOLE=default $(GO) build -tags=webconsole,fips -a -o immudb -ldflags '$(V_LDFLAGS_FIPS_BUILD)' ./cmd/immudb/fips
	./build/fips/check-fips.sh immudb

.PHONY: immutest-static
immutest-static:
	CGO_ENABLED=0 $(GO) build -a -ldflags '$(V_LDFLAGS_STATIC)' ./cmd/immutest

.PHONY: vendor
vendor:
	$(GO) mod vendor

.PHONY: test
test:
	$(GO) vet ./...
	LOG_LEVEL=error $(GO) test -v -failfast ./... ${GO_TEST_FLAGS}

# build FIPS binary from docker image
.PHONY: test/fips
test/fips:
	$(DOCKER) build -t fips:test-build -f build/fips/Dockerfile.build .
	$(DOCKER) run --rm fips:test-build -c "GO_TEST_FLAGS='-tags fips' make test"

.PHONY: test-client
test-client:
	$(GO) test -v -failfast ./pkg/client ${GO_TEST_FLAGS}

# To view coverage as HTML run: go tool cover -html=coverage.txt
.PHONY: coverage
coverage:
	go-acc ./... --covermode=atomic --ignore=test,immuclient,immuadmin,helper,cmdtest,sservice,version,tools,webconsole,protomodel,schema,swagger
	cat coverage.txt | grep -v "schema" | grep -v "protomodel" | grep -v "swagger" | grep -v "webserver.go" | grep -v "immuclient" | grep -v "immuadmin" | grep -v "helper" | grep -v "cmdtest" | grep -v "sservice" | grep -v "version" > coverage.out
	$(GO) tool cover -func coverage.out

.PHONY: build/codegen
build/codegen:
	$(PWD)/ext-tools/buf format -w

	$(PROTOC) -I pkg/api/schema/ pkg/api/schema/schema.proto \
	  -I$(GOPATH)/pkg/mod \
	  -I$(GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@$(GRPC_GATEWAY_VERSION) \
	  -I$(GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@$(GRPC_GATEWAY_VERSION)/third_party/googleapis \
	  --go_out=paths=source_relative:pkg/api/schema \
	  --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:pkg/api/schema \
      --grpc-gateway_out=logtostderr=true,paths=source_relative:pkg/api/schema \
	  --doc_out=pkg/api/schema --doc_opt=markdown,docs.md \
	  --swagger_out=logtostderr=true:pkg/api/schema \

.PHONY: build/codegenv2
build/codegenv2:
	$(PWD)/ext-tools/buf format -w

	$(PROTOC) -I pkg/api/proto/ pkg/api/proto/authorization.proto pkg/api/proto/documents.proto \
	  -I pkg/api/schema/ \
	  -I$(GOPATH)/pkg/mod \
	  -I$(GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@$(GRPC_GATEWAY_VERSION) \
	  -I$(GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@$(GRPC_GATEWAY_VERSION)/third_party/googleapis \
	  --go_out=paths=source_relative:pkg/api/protomodel \
	  --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:pkg/api/protomodel \
	  --grpc-gateway_out=logtostderr=true,paths=source_relative:pkg/api/protomodel \
	  --doc_out=pkg/api/protomodel --doc_opt=markdown,docs.md \
	  --swagger_out=logtostderr=true,allow_merge=true,simple_operation_ids=true:pkg/api/openapi \

./swagger/dist:
	rm -rf swagger/dist/
	curl -L $(SWAGGERUILINK) | tar -xz -C swagger
	mv swagger/swagger-ui-$(SWAGGERUIVERSION)/dist/ swagger/ && rm -rf swagger/swagger-ui-$(SWAGGERUIVERSION)
	cp pkg/api/openapi/apidocs.swagger.json swagger/dist/apidocs.swagger.json
	cp pkg/api/schema/schema.swagger.json swagger/dist/schema.swagger.json
	cp swagger/swaggeroverrides.js swagger/dist/swagger-initializer.js

.PHONY: swagger
ifeq ($(SWAGGER),true)
swagger: ./swagger/dist
	env -u GOOS -u GOARCH $(GO) generate $(IMMUDB_BUILD_TAGS) ./swagger
else
swagger:
	env -u GOOS -u GOARCH $(GO) generate $(IMMUDB_BUILD_TAGS) ./swagger
endif


.PHONY: clean
clean:
	rm -rf immudb immuclient immuadmin immutest ./webconsole/dist ./swagger/dist

.PHONY: man
man:
	$(GO) run ./cmd/immuclient mangen ./cmd/docs/man/immuclient
	$(GO) run ./cmd/immuadmin mangen ./cmd/docs/man/immuadmin
	$(GO) run ./cmd/immudb mangen ./cmd/docs/man/immudb
	$(GO) run ./cmd/immutest mangen ./cmd/docs/man/immutest

.PHONY: prerequisites
prerequisites:
	$(GO) mod tidy -compat=1.17
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
dist: webconsole dist/binaries dist/fips
	@echo 'Binaries generation complete. Now vcn signature is needed.'

# build FIPS binary from docker image (no arm or non-linux support)
.PHONY: dist/fips
dist/fips: clean
	$(DOCKER) build -t fips:build -f build/fips/Dockerfile.build .
	$(DOCKER) run -v ${PWD}:/src --user root --rm fips:build -c "WEBCONSOLE=default make immudb-fips"
	mv immudb ./dist/immudb-v${VERSION}-linux-amd64-fips
	$(DOCKER) run -v ${PWD}:/src --user root --rm fips:build -c "make immuclient-fips"
	mv immuclient ./dist/immuclient-v${VERSION}-linux-amd64-fips
	$(DOCKER) run -v ${PWD}:/src --user root --rm fips:build -c "make immuadmin-fips"
	mv immuadmin ./dist/immuadmin-v${VERSION}-linux-amd64-fips

.PHONY: dist/binaries
dist/binaries:
		mkdir -p dist; \
		for service in ${SERVICES}; do \
    		for os_arch in ${TARGETS}; do \
    			goos=`echo $$os_arch|sed 's|/.*||'`; \
    			goarch=`echo $$os_arch|sed 's|^.*/||'`; \
				GOOS=$$goos GOARCH=$$goarch V_BUILD_NAME=./dist/$$service-v${VERSION}-$$goos-$$goarch make $$service ; \
    		done; \
			CGO_ENABLED=0 GOOS=linux GOARCH=amd64 V_BUILD_NAME=./dist/$$service-v${VERSION}-linux-amd64-static make $$service-static ; \
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
	@build/gen-downloads-md.sh "${VERSION}"

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
