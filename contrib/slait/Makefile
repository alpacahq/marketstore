GOFLAGS="-mod=vendor"
GOPATH0 := $(firstword $(subst :, ,$(GOPATH)))
all:
	GOFLAGS=$(GOFLAGS) go build -o $(GOPATH0)/bin/slait.so -buildmode=plugin .

debug:
	GOFLAGS=$(GOFLAGS) go build -gcflags="all=-N -l" -o $(GOPATH0)/bin/slait.so -buildmode=plugin .
