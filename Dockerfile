#
# STAGE 1
#
# Uses a Go image to build a release binary.
#
FROM golang:1.14.2-buster as builder
ARG tag=latest
ARG INCLUDE_PLUGINS=true
ENV DOCKER_TAG=$tag
ENV GOPATH=/go

WORKDIR /go/src/github.com/alpacahq/marketstore/
ADD ./ ./
RUN if [ "$INCLUDE_PLUGINS" = "true" ] ; then make build plugins ; else make build ; fi

#
# STAGE 2
#
# Create final image
#
FROM debian:10.3
WORKDIR /

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /go/src/github.com/alpacahq/marketstore/marketstore /bin/
COPY --from=builder /go/bin /bin/
COPY --from=builder /go/src/github.com/alpacahq/marketstore/contrib/polygon/polygon-backfill-*.sh /bin/

ENV GOPATH=/

RUN ["marketstore", "init"]
RUN mv mkts.yml /etc/
VOLUME /data
VOLUME /polygon
EXPOSE 5993

ENTRYPOINT ["marketstore"]
CMD ["start", "--config", "/etc/mkts.yml"]
