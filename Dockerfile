FROM golang:1.8.3-alpine

RUN apk update

RUN apk --no-cache add git make tar curl alpine-sdk

RUN  go get -u github.com/golang/dep/... && mv /go/bin/dep /usr/local/bin/dep

WORKDIR /go/src/github.com/alpacahq/marketstore

CMD make unittest
