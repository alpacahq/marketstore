GOPATH0 := $(firstword $(subst :, ,$(GOPATH)))
all:
	GOFLAGS=$(GOFLAGS) go build -o $(GOPATH0)/bin/alpacabkfeeder.so -buildmode=plugin .

debug:
	GOFLAGS=$(GOFLAGS) go build -gcflags="all=-N -l" -o $(GOPATH0)/bin/alpacabkfeeder.so -buildmode=plugin .
