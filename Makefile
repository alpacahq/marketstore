.PHONY: plugins

GOPATH0 := $(firstword $(subst :, ,$(GOPATH)))
all:
	go install -ldflags "-s -X utils.Version=$(shell date -u +%Y-%m-%d-%H-%M-%S)" ./cmd/marketstore ./cmd/tools/...

install: all

generate:
	make -C SQLParser
	go generate $(shell find . -path ./vendor -prune -o -name \*.go -exec grep -q go:generate {} \; -print | while read file; do echo `dirname $$file`; done | xargs)

configure:
	glide install

update:
	glide update

plugins:
	go build -o $(GOPATH0)/bin/simpleAgg.so -buildmode=plugin ./cmd/plugins/triggers/simpleAgg
	$(MAKE) -C contrib/gdaxfeeder

unittest:
	! gofmt -l $(shell glide novendor -no-subdir) | grep .
	go vet $(shell glide novendor)
	go test -ldflags -s -cover $(shell glide novendor | grep -v cmd)

unittest-with-docker:
	docker build -t gobuild .
	docker run -v $(CURDIR):/go/src/github.com/alpacahq/marketstore gobuild make unittest
