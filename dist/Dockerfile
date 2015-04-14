# This Dockerfile is for building a container image that has all the
# prerequisites for building cbft.  For example...
#
#    docker build -t cbft-builder --no-cache=true .
#
FROM golang:1.4.2-cross

MAINTAINER Steve Yen <steve.yen@gmail.com>

RUN go get -u github.com/couchbaselabs/cbft

RUN make --directory=/go/src/github.com/couchbaselabs/cbft prereqs-dist

RUN make --directory=/go/src/github.com/couchbaselabs/cbft test coverage

RUN make --directory=/go/src/github.com/couchbaselabs/cbft build

RUN make --directory=/go/src/github.com/couchbaselabs/cbft dist-clean

# Reaching here, we've exercised relevant build/dist steps, and will
# leave a clean, ready-to-use state of the image.
