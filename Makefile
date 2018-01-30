.PHONY: plugins

GOPATH0 := $(firstword $(subst :, ,$(GOPATH)))
all:
	go install -ldflags "-s -X utils.Tag=$(DOCKER_TAG) -X utils.BuildStamp=$(shell date -u +%Y-%m-%d-%H-%M-%S) -X utils.GitHash=$(shell git rev-parse HEAD)" ./cmd/marketstore ./cmd/tools/...

install: all

generate:
	make -C SQLParser
	go generate $(shell find . -path ./vendor -prune -o -name \*.go -exec grep -q go:generate {} \; -print | while read file; do echo `dirname $$file`; done | xargs)

configure:
	dep ensure
	make -C contrib/gdaxfeeder $@

update:
	dep ensure -update

plugins:
	$(MAKE) -C contrib/ondiskagg
	$(MAKE) -C contrib/gdaxfeeder

unittest:
	go fmt ./...
	go vet ./...
	go test ./...

push:
	docker build -t alpacamarkets/marketstore:$(DOCKER_TAG)
	docker login -u $(DOCKER_USER) -p $(DOCKER_PASS)
	docker push alpacamarkets/marketstore:$(DOCKER_TAG)