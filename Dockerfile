#
# STAGE 1
#
# Uses a Go image to build a release binary.
#
FROM golang:1.13.0-buster as builder
ARG tag=latest
ARG INCLUDE_PLUGINS=true
ENV DOCKER_TAG=$tag
ENV GOPATH=/go

WORKDIR /go/src/github.com/alpacahq/marketstore/
ADD ./ ./
RUN make vendor
RUN if [ "$INCLUDE_PLUGINS" = "true" ] ; then make build plugins ; else make build ; fi

#
# STAGE 2
#
# Use a tiny base image (alpine) and copy in the release target. This produces
# a very small output image for deployment.
#
FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata libc6-compat
WORKDIR /
COPY --from=builder /go/src/github.com/alpacahq/marketstore/marketstore /bin/
# copy plugins if any
COPY --from=builder /go/bin /bin/
ENV GOPATH=/

RUN ["marketstore", "init"]
RUN mv mkts.yml /etc/
VOLUME /data
EXPOSE 5993

ENTRYPOINT ["marketstore"]
CMD ["start", "--config", "/etc/mkts.yml"]
