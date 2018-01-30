FROM golang:1.9.3-alpine

RUN apk update
RUN apk --no-cache add git make tar curl alpine-sdk
RUN  go get -u github.com/golang/dep/... && mv /go/bin/dep /usr/local/bin/dep
ADD . /go/src/github.com/alpacahq/marketstore
WORKDIR /go/src/github.com/alpacahq/marketstore
RUN make install
RUN make all plugins

CMD make unittest
