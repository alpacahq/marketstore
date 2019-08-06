#
# STAGE 1
#
# Uses a Go image to build a release binary.
#
FROM ubuntu:18.04 as builder
ARG tag=latest
ENV DOCKER_TAG=$tag
ENV GO111MODULE=on
ENV GOPATH=/go

#RUN apk --no-cache add git make gcc g++
RUN apt update
RUN apt install -y golang gcc g++ make wget git
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
FROM ubuntu:18.04
RUN apt update
RUN apt install -y ca-certificates tzdata
# set your timezone
RUN ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime
RUN dpkg-reconfigure --frontend noninteractive tzdata
WORKDIR /
COPY --from=builder /go/bin/marketstore /bin/
COPY --from=builder /go/bin/*.so /bin/
ENV GOPATH=/

# RUN ["marketstore", "init"]
# RUN mv mkts.yml /etc/
COPY /quanatee/mkts.yml .

# ENV POLYGON_API_KEY $POLYGON_API_KEY
RUN sed -i "s|<polygon_api_key>|polygon_api_key: $POLYGON_API_KEY|g" mkts.yml
VOLUME /data
EXPOSE 5993

RUN ls
ENTRYPOINT ["marketstore"]
CMD ["start", "--config", "./mkts.yml"]
