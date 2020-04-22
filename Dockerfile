#
# STAGE 1
#
# Uses a Go image to build a release binary.
#
FROM golang:1.14.0-buster as builder
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
# Create final image
#
FROM debian:buster
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
