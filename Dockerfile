# This Dockerfile is for building a container image that has all the
# prerequisites for building cbft.  For example...
#
#    docker build -t cbft-builder .
#
FROM golang:1.4.2

MAINTAINER Steve Yen <steve.yen@gmail.com>

RUN go get -u github.com/couchbaselabs/cbft

RUN make --directory=/go/src/github.com/couchbaselabs/cbft prerequisites test build gen-docs
