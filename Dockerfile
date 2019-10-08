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
WORKDIR /
COPY --from=builder /go/bin/marketstore /bin/
COPY --from=builder /go/bin/*.so /bin/
ENV GOPATH=/

RUN ["marketstore", "init"]
RUN mv mkts.yml /etc/
VOLUME /data
EXPOSE 5993

ENTRYPOINT ["marketstore"]
CMD ["start", "--config", "/etc/mkts.yml"]
