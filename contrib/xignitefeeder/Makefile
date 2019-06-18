GOPATH0 := $(firstword $(subst :, ,$(GOPATH)))
all:
	go build -o $(GOPATH0)/bin/xignitefeeder.so -buildmode=plugin .
