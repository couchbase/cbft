# This Dockerfile is for building a container image that has all the
# prerequisites for building cbft.  For example...
#
#    docker build -t cbft-builder:latest .
#
# To force a fresh image rebuild...
#
#    docker build -t cbft-builder:latest --no-cache=true .
#
FROM golang:1.8.1

MAINTAINER Steve Yen <steve.yen@gmail.com>

RUN apt-get update && apt-get -y install \
    build-essential \
    cmake \
    libicu-dev \
    libleveldb-dev \
    libsnappy-dev \
    libstemmer-dev \
    python-pip

# TODO: Skipping some go get tags, because these are shared libraries
# which means a single downloadable executable doesn't work.
# - cld (link conflicts possibly related to c++ exceptions)
# - icu
# - leveldb

RUN go get -u -v -tags "debug kagome libstemmer" github.com/couchbase/cbft/...

RUN make --directory=/go/src/github.com/couchbase/cbft \
    prereqs-dist \
    test \
    coverage \
    build \
    dist-clean

RUN rm -rf /go/pkg/*

# Reaching here, we've exercised relevant build/dist steps,
# leaving a clean, ready-to-use image state.
