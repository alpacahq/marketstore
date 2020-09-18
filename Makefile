.PHONY: plugins

GOFLAGS=""
GOPATH0 := $(firstword $(subst :, ,$(GOPATH)))
UTIL_PATH := github.com/alpacahq/marketstore/v4/utils

build:
	GOFLAGS=$(GOFLAGS) go build -ldflags "-s -X $(UTIL_PATH).Tag=$(DOCKER_TAG) -X $(UTIL_PATH).BuildStamp=$(shell date -u +%Y-%m-%d-%H-%M-%S) -X $(UTIL_PATH).GitHash=$(shell git rev-parse HEAD)" .

install:
	GOFLAGS=$(GOFLAGS) go install -ldflags "-s -X $(UTIL_PATH).Tag=$(DOCKER_TAG) -X $(UTIL_PATH).BuildStamp=$(shell date -u +%Y-%m-%d-%H-%M-%S) -X $(UTIL_PATH).GitHash=$(shell git rev-parse HEAD)" .

debug:
	$(MAKE) debug -C contrib/binancefeeder
	$(MAKE) debug -C contrib/bitmexfeeder
	$(MAKE) debug -C contrib/gdaxfeeder
	$(MAKE) debug -C contrib/iex
	$(MAKE) debug -C contrib/ondiskagg
	$(MAKE) debug -C contrib/polygon
	$(MAKE) debug -C contrib/stream
	$(MAKE) debug -C contrib/xignitefeeder
	GOFLAGS=$(GOFLAGS) go install -gcflags="all=-N -l" -ldflags "-X $(UTIL_PATH).Tag=$(DOCKER_TAG) -X $(UTIL_PATH).BuildStamp=$(shell date -u +%Y-%m-%d-%H-%M-%S) -X $(UTIL_PATH).GitHash=$(shell git rev-parse HEAD)" ./...

generate:
	GOFLAGS=$(GOFLAGS) go generate $(shell find . -path ./vendor -prune -o -name \*.go -exec grep -q go:generate {} \; -print | while read file; do echo `dirname $$file`; done | xargs)

generate-sql:
	make -C sqlparser

update:
	GOFLAGS=$(GOFLAGS) go mod tidy

plugins:
	$(MAKE) -C contrib/binancefeeder
	$(MAKE) -C contrib/bitmexfeeder
	$(MAKE) -C contrib/gdaxfeeder
	$(MAKE) -C contrib/iex
	$(MAKE) -C contrib/ondiskagg
	$(MAKE) -C contrib/polygon
	$(MAKE) -C contrib/stream
	$(MAKE) -C contrib/xignitefeeder

fmt:
	GOFLAGS=$(GOFLAGS) go fmt ./...

unit-test:
	# marketstore/contrib/stream/shelf/shelf_test.go fails if "-race" enabled...
	# GOFLAGS=$(GOFLAGS) go test -race -coverprofile=coverage.txt -covermode=atomic ./...
	GOFLAGS=$(GOFLAGS) go test -coverprofile=coverage.txt -covermode=atomic ./...

import-csv-test:
	@tests/integ/bin/runtests.sh

integration-test-jsonrpc:
	$(MAKE) -C tests/integ test-jsonrpc

integration-test-grpc:
	$(MAKE) -C tests/integ test-grpc

replication-test:
	$(MAKE) -C tests/replication test-replication

test: build
	$(MAKE) unit-test
	$(MAKE) import-csv-test
	$(MAKE) integration-test

image:
	docker build . -t marketstore:latest -f $(DOCKER_FILE_PATH)

runimage:
	make -C tests/integ run IMAGE_NAME=alpacamarkets/marketstore.test

stopimage:
	make -C tests/integ clean IMAGE_NAME=alpacamarkets/marketstore.test

push:
	docker build --build-arg tag=$(DOCKER_TAG) -t alpacamarkets/marketstore:$(DOCKER_TAG) -t alpacamarkets/marketstore:latest .
	docker login -u $(DOCKER_USER) -p $(DOCKER_PASS)
	docker push alpacamarkets/marketstore:$(DOCKER_TAG)
	docker push alpacamarkets/marketstore:latest
