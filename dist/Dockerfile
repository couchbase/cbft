# This Dockerfile is for building a container image that has all the
# prerequisites for building cbft.  For example...
#
#    docker build -t cbft-builder:latest
#
# To force a fresh image rebuild...
#
#    docker build -t cbft-builder:latest --no-cache=true .
#
FROM golang:1.4.2-cross

MAINTAINER Steve Yen <steve.yen@gmail.com>

RUN apt-get update && apt-get -y install \
    build-essential \
    libicu-dev \
    libstemmer-dev \
    libleveldb-dev \
    python-pip

RUN go get -u -v github.com/couchbaselabs/cbft

RUN make --directory=/go/src/github.com/couchbaselabs/cbft \
    prereqs-dist \
    test \
    coverage \
    build \
    dist-clean

# Reaching here, we've exercised relevant build/dist steps,
# leaving a clean, ready-to-use image state.
