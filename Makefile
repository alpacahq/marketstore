.PHONY: plugins

GOPATH0 := $(firstword $(subst :, ,$(GOPATH)))
UTIL_PATH := github.com/alpacahq/marketstore/utils

all:
	go install -ldflags "-s -X $(UTIL_PATH).Tag=$(DOCKER_TAG) -X $(UTIL_PATH).BuildStamp=$(shell date -u +%Y-%m-%d-%H-%M-%S) -X $(UTIL_PATH).GitHash=$(shell git rev-parse HEAD)" ./...

install: all

generate:
	make -C SQLParser
	go generate $(shell find . -path ./vendor -prune -o -name \*.go -exec grep -q go:generate {} \; -print | while read file; do echo `dirname $$file`; done | xargs)

vendor:
	go mod vendor

update:
	go mod tidy

plugins:
	$(MAKE) -C contrib/ondiskagg
	$(MAKE) -C contrib/slait
	$(MAKE) -C contrib/stream
	$(MAKE) -C contrib/polygon
	$(MAKE) -C contrib/bitmexfeeder
	$(MAKE) -C contrib/testgdax_usd
	$(MAKE) -C contrib/testgdax_btc
	$(MAKE) -C contrib/gdax_usd
	$(MAKE) -C contrib/gdax_btc
	$(MAKE) -C contrib/binance_usdt_1
	$(MAKE) -C contrib/binance_btc_1
	$(MAKE) -C contrib/binance_eth_1
	$(MAKE) -C contrib/binance_bnb_1
	$(MAKE) -C contrib/binance_usdt_2
	$(MAKE) -C contrib/binance_btc_2
	$(MAKE) -C contrib/binance_eth_2
	$(MAKE) -C contrib/binance_bnb_2
	$(MAKE) -C contrib/binance_usdt_3
	$(MAKE) -C contrib/binance_btc_3
	$(MAKE) -C contrib/binance_eth_3
	$(MAKE) -C contrib/binance_bnb_3
	$(MAKE) -C contrib/binance_usdt_4
	$(MAKE) -C contrib/binance_btc_4
	$(MAKE) -C contrib/binance_eth_4
	$(MAKE) -C contrib/binance_bnb_4

unittest:
	go fmt ./...
	go test ./...

push:
	docker build --build-arg tag=$(DOCKER_TAG) -t alpacamarkets/marketstore:$(DOCKER_TAG) -t alpacamarkets/marketstore:latest .
	docker login -u $(DOCKER_USER) -p $(DOCKER_PASS)
	docker push alpacamarkets/marketstore:$(DOCKER_TAG)
	docker push alpacamarkets/marketstore:latest