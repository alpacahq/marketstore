FROM alpine:latest

RUN apk --no-cache add ca-certificates tar curl tzdata bash

ADD ./bin /marketstore/bin

ENV PATH $PATH:/marketstore/bin

CMD launcher
