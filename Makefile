.PHONY: plugins

GOPATH0 := $(firstword $(subst :, ,$(GOPATH)))
all:
	go install -ldflags "-s -X utils.Version=$(shell date -u +%Y-%m-%d-%H-%M-%S)" ./cmd/marketstore ./cmd/tools/...

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

unittest-with-docker:
	docker build -t gobuild .
	docker run -v $(CURDIR):/go/src/github.com/alpacahq/marketstore gobuild make unittest
