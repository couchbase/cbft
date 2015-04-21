# Overview

cbft is a full-text indexing server for documents that you've created
and stored into a Couchbase bucket.  The full-text indexes that cbft
manages can be automatically distributed across multiple, clustered
cbft server processes to allow for larger indexes and for higher
performance.

# Getting started

## Getting cbft

Download a pre-built cbft from the [releases](https://github.com/couchbaselabs/cbft/releases) page.  For example, for OSX...

    wget https://github.com/couchbaselabs/cbft/releases/download/vX.Y.Z/vX.Y.Z-AAA_cbft.darwin.amd64.tar.gz
    tar -xzvf vX.Y.Z-AAA_cbft.darwin.amd64.tar.gz
    ./cbft.darwin.amd64 --help

Note: ```cbft-full``` builds are currently compiled with more advanced
features (text stemmers, etc) than ```cbft``` basic builds.  For the
purposes of these getting start steps, though, downloading either one
is fine.

## First time setup

Prerequisites: you should have a Couchbase Server (3.0+) already
installed and running somewhere.

Create a directory where cbft will store its config and data files...

    mkdir -p data

## Running cbft

Start cbft, pointing it to your Couchbase Server as its datasource
server...

    ./cbft -server http://localhost:8091

Next, you can use a web browser on cbft's web admin UI...

    http://localhost:8095

# Where to go next

Please see
the [Developer's Guide](dev-guide/overview.md),
the [API Reference](api-ref.md) and
the [Administrator's Guide](admin-guide/overview.md).

---

Copyright (c) 2015 Couchbase, Inc.
