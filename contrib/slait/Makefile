GOPATH0 := $(firstword $(subst :, ,$(GOPATH)))
all:
	go build -o $(GOPATH0)/bin/slait.so -buildmode=plugin .

debug:
	go build -gcflags="all=-N -l" -o $(GOPATH0)/bin/slait.so -buildmode=plugin .
