#
# STAGE 1
#
# Uses a Go image to build a release binary.
#
ARG golang=golang:1.18.1-buster
#ARG golang=arm64v8/golang:1.19.2-bullseye
FROM $golang as builder
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
ARG os=debian:10.3
#ARG os=arm64v8/debian:bullseye
FROM $os
WORKDIR /

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /go/src/github.com/alpacahq/marketstore/marketstore /bin/
COPY --from=builder /go/bin /bin/
COPY --from=builder /go/src/github.com/alpacahq/marketstore/contrib/polygon/polygon-backfill-*.sh /bin/
COPY --from=builder /go/src/github.com/alpacahq/marketstore/contrib/ice/ca-sync-*.sh /bin/

ENV GOPATH=/

RUN ["marketstore", "init"]
RUN mv mkts.yml /etc/
VOLUME /data
EXPOSE 5993

ENTRYPOINT ["marketstore"]
CMD ["start", "--config", "/etc/mkts.yml"]
