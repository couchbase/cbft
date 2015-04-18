cbft
====

Couchbase Full Text server

This project integrates the bleve full-text search engine and Couchbase Server.

[![Build Status](https://drone.io/github.com/couchbaselabs/cbft/status.png)](https://drone.io/github.com/couchbaselabs/cbft/latest) [![Coverage Status](https://coveralls.io/repos/couchbaselabs/cbft/badge.png?branch=master)](https://coveralls.io/r/couchbaselabs/cbft?branch=master) [![GoDoc](https://godoc.org/github.com/couchbaselabs/cbft?status.svg)](https://godoc.org/github.com/couchbaselabs/cbft)

LICENSE: Apache 2.0

A cbft process creates and maintains connections to a Couchbase Server
cluster and indexes any incoming streamed data (coming from the
Couchbase's DCP protocol) using the bleve full-text search engine.
Indexes can be partitioned amongst multiple cbft processes, and
queries on the index will be scatter/gather'ed across the distributed
index partitions.

# Getting started

## Getting cbft

Download a pre-built cbft from the [releases](https://github.com/couchbaselabs/cbft/releases) page.  For example, for OSX...

    wget https://github.com/couchbaselabs/cbft/releases/download/vX.Y.Z/vX.Y.Z-AAA_cbft.darwin.amd64.tar.gz
    tar -xzvf vX.Y.Z-AAA_cbft.darwin.amd64.tar.gz
    ./cbft.darwin.amd64 --help

Or, to build cbft from source (requires golang 1.4)...

    go get -u github.com/couchbaselabs/cbft/...
    $GOPATH/bin/cbft --help

## First time setup

Prerequisites: you should have a Couchbase Server (3.0+) already
installed and running somewhere.

Create a directory where cbft will store its config and data files...

    mkdir -p data

## Running cbft

Start cbft, pointing it to the Couchbase Server as its datasource
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
