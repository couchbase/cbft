cbft
====

Couchbase Full Text server

This project integrates the bleve full-text search engine and Couchbase Server.

[![Build Status](https://drone.io/github.com/couchbaselabs/cbft/status.png)](https://drone.io/github.com/couchbaselabs/cbft/latest) [![Coverage Status](https://coveralls.io/repos/couchbaselabs/cbft/badge.png?branch=master)](https://coveralls.io/r/couchbaselabs/cbft?branch=master) [![GoDoc](https://godoc.org/github.com/couchbaselabs/cbft?status.svg)](https://godoc.org/github.com/couchbaselabs/cbft)

LICENSE: Apache 2.0

A cbft process creates and maintains full-text indexes using the
[bleve full-text indexing engine](http://www.blevesearch.com/).  Data
sources for indexing include Couchbase 3.0 buckets.  Full-text indexes
can be optionally partitioned across multiple cbft processes.  Queries
on an index will be scatter/gather'ed across the relevant, distributed
cbft processes.

# Getting started

## Getting cbft

Please see the
[releases](https://github.com/couchbaselabs/cbft/releases) page for
released cbft downloadables.

For example, for OSX...

    wget https://github.com/couchbaselabs/cbft/releases/download/vX.Y.Z/vX.Y.Z-AAA_cbft.darwin.amd64.tar.gz
    tar -xzvf vX.Y.Z-AAA_cbft.darwin.amd64.tar.gz
    ./cbft.darwin.amd64 --help

Note: ```cbft-full``` builds are currently compiled with more advanced
features (text stemmers, etc) than ```cbft``` basic builds.

For the purposes of these getting start steps, though, downloading a
```cbft``` build is fine.

### Building cbft from source

Or, to build cbft from source (requires golang 1.4)...

    go get -u github.com/couchbaselabs/cbft/...
    $GOPATH/bin/cbft --help

## Prerequisites

You should have a Couchbase Server (3.0+) already installed and
running somewhere.

## Running cbft

Start cbft, pointing it to your Couchbase Server as its datasource
server...

    ./cbft -server http://localhost:8091

Next, you can use a web browser on cbft's web admin UI...

    http://localhost:8095

Create a new full-text index, which will be powered by the
[bleve](http://blevesearch.com) full-text engine; the index will be
called "default" and will have the "default" bucket from Couchbase as
its datasource...

    curl -XPUT 'http://localhost:8095/api/index/default?indexType=bleve&sourceType=couchbase'

Check how many documents are indexed...

    curl http://localhost:8095/api/index/default/count

Query the index...

    curl -XPOST --header Content-Type:text/json \
         -d '{"query":{"size":10,"query":{"query":"your-search-term"}}}' \
         http://localhost:8095/api/index/default/query

Delete the index...

    curl -XDELETE http://localhost:8095/api/index/default

# More links

* [cbft documentation](http://labs.couchbase.com/cbft)
