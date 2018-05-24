FROM golang:1.10.2-alpine

ARG tag

ENV DOCKER_TAG=$tag

RUN apk update
RUN apk --no-cache add git make tar curl alpine-sdk su-exec
RUN  go get -u github.com/golang/dep/... && mv /go/bin/dep /usr/local/bin/dep
ADD . /go/src/github.com/alpacahq/marketstore
WORKDIR /go/src/github.com/alpacahq/marketstore
RUN make install
RUN make all plugins

COPY entrypoint.sh /bin/
RUN chmod +x /bin/entrypoint.sh
ENTRYPOINT ["/bin/entrypoint.sh"]

CMD marketstore