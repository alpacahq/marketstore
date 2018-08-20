# -*- mode: dockerfile -*-
#
# A multi-stage Dockerfile that builds a Linux target then creates a small
# final image for deployment.

#
# STAGE 1
#
# Uses a Go image to build a release binary.
#
FROM golang:1.10.3-alpine AS builder
ARG tag=latest
ENV DOCKER_TAG=$tag

RUN apk --no-cache add git make gcc g++
RUN go get -u github.com/golang/dep/...
WORKDIR /go/src/github.com/alpacahq/marketstore/
ADD ./ ./
RUN dep ensure -vendor-only
RUN make install plugins

#
# STAGE 2
#
# Use a tiny base image (alpine) and copy in the release target. This produces
# a very small output image for deployment.
#
FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /
COPY --from=builder /go/bin/marketstore /bin/
COPY --from=builder /go/bin/*.so /bin/

RUN ["marketstore", "init"]
RUN mv mkts.yml etc/
VOLUME /data
EXPOSE 5993

ENTRYPOINT ["marketstore"]
CMD ["start", "--config", "etc/mkts.yml"]
