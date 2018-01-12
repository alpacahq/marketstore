GOPATH0 := $(firstword $(subst :, ,$(GOPATH)))
all:
	go build -o $(GOPATH0)/bin/gdaxfeeder.so -buildmode=plugin .
	
configure:
	glide install
