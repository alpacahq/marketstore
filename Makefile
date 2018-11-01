.PHONY: plugins

GOPATH0 := $(firstword $(subst :, ,$(GOPATH)))
UTIL_PATH := github.com/alpacahq/marketstore/utils

all:
	go install -ldflags "-s -X $(UTIL_PATH).Tag=$(DOCKER_TAG) -X $(UTIL_PATH).BuildStamp=$(shell date -u +%Y-%m-%d-%H-%M-%S) -X $(UTIL_PATH).GitHash=$(shell git rev-parse HEAD)" ./...

install: all

generate:
	make -C sqlparser
	go generate $(shell find . -path ./vendor -prune -o -name \*.go -exec grep -q go:generate {} \; -print | while read file; do echo `dirname $$file`; done | xargs)

vendor:
	go mod vendor

update:
	go mod tidy

plugins:
	$(MAKE) -C contrib/ondiskagg
	$(MAKE) -C contrib/gdaxfeeder
	$(MAKE) -C contrib/slait
	$(MAKE) -C contrib/stream
	$(MAKE) -C contrib/polygon
	$(MAKE) -C contrib/bitmexfeeder
	$(MAKE) -C contrib/binancefeeder

unittest: all
	go fmt ./...
	go test ./...
	$(MAKE) -C tests/integ test

image:
	docker build -t alpacamarkets/marketstore.test .

runimage:
	make -C tests/integ run IMAGE_NAME=alpacamarkets/marketstore.test

stopimage:
	make -C tests/integ clean IMAGE_NAME=alpacamarkets/marketstore.test

push:
	docker build --build-arg tag=$(DOCKER_TAG) -t alpacamarkets/marketstore:$(DOCKER_TAG) -t alpacamarkets/marketstore:latest .
	docker login -u $(DOCKER_USER) -p $(DOCKER_PASS)
	docker push alpacamarkets/marketstore:$(DOCKER_TAG)
	docker push alpacamarkets/marketstore:latest
