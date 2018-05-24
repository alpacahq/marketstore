GOPATH0 := $(firstword $(subst :, ,$(GOPATH)))
all:
	go build -o $(GOPATH0)/bin/stream.so -buildmode=plugin .
