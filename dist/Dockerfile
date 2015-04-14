# This Dockerfile is for building a container image that has all the
# prerequisites for building cbft.  For example...
#
#    docker build -t cbft-builder:latest .
#
# To force a fresh image rebuild...
#
#    docker build -t cbft-builder:latest --no-cache=true .
#
FROM golang:1.4.2-cross

MAINTAINER Steve Yen <steve.yen@gmail.com>

RUN apt-get update && apt-get -y install \
    build-essential \
    cmake \
    libicu-dev \
    libleveldb-dev \
    libsnappy-dev \
    libstemmer-dev \
    python-pip

RUN git clone git://github.com/couchbase/forestdb /tmp/forestdb && \
    cd /tmp/forestdb && \
    mkdir build && \
    cd build && \
    cmake .. && \
    make all install

# TODO: Skipping forestdb and cld, for now, where they
# are not specified as part of the following go get tags.
# - forestdb builds as shared library rather than static,
#   which means a single downloadable executable doesn't work.
# - cld2 has linking issues with C++ exceptions.

RUN go get -u -v -tags "debug icu libstemmer kagome leveldb" github.com/couchbaselabs/cbft/...

RUN make --directory=/go/src/github.com/couchbaselabs/cbft \
    prereqs-dist \
    test \
    coverage \
    build \
    dist-clean

# Reaching here, we've exercised relevant build/dist steps,
# leaving a clean, ready-to-use image state.
