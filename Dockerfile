FROM golang:1.8.3-alpine

RUN apk update

RUN apk --no-cache add git make tar curl alpine-sdk

RUN go get github.com/Masterminds/glide && mv /go/bin/glide /usr/local/bin/glide

WORKDIR /go/src/github.com/alpacahq/marketstore

CMD make unittest
