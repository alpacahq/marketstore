GOFLAGS="-mod=vendor"
GOPATH0 := $(firstword $(subst :, ,$(GOPATH)))
all:
	GOFLAGS=$(GOFLAGS) go build -o $(GOPATH0)/bin/xignitefeeder.so -buildmode=plugin .
