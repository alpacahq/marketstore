#
# STAGE 1
#
# Uses a Go image to build a release binary.
#
FROM golang:1.13.0-buster as builder
ARG tag=latest
ENV DOCKER_TAG=$tag
ENV GOPATH=/go

WORKDIR /go/src/github.com/alpacahq/marketstore/
ADD ./ ./
RUN make vendor
RUN make install plugins

#
# STAGE 2
#
# Use a tiny base image (alpine) and copy in the release target. This produces
# a very small output image for deployment.
#
FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata libc6-compat
RUN cp /usr/share/zoneinfo/America/New_York /etc/localtime

WORKDIR /
COPY --from=builder /go/bin/marketstore /bin/
COPY --from=builder /go/bin/*.so /bin/
ENV GOPATH=/

COPY /quanatee/mkts.yml .

VOLUME /data
EXPOSE 5993

ENTRYPOINT sed -i "s|<tiingo_api_key>|${TIINGO_API_KEY}|g" mkts.yml && sed -i "s|<intrinio_api_key>|${INTRINIO_API_KEY}|g" mkts.yml && sed -i "s|<tdameritrade_api_key>|${TDAMERITRADE_API_KEY}|g" mkts.yml && marketstore start --config mkts.yml && /bin/bash