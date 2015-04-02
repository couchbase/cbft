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

### Usage

Getting

```go get -u github.com/couchbaselabs/cbft/...```

Create a directory for index data

```mkdir data```

Running against local Couchbase Server

```./cbft -server http://localhost:8091```

More complex example

```./cbft -addr=localhost:9090 -cfgConnect=couchbase:http://cfg@localhost:8091 -server=http://localhost:8091```

Create a new index (for the default bucket)

```curl -XPUT http://localhost:8095/api/index/default```

Check how many documents are indexed

```curl http://localhost:8095/api/index/default/count```

Submit search query

```curl -XPOST -d '{"query":{"size":10,"query":{"query":"your-search-term"}}}' --header Content-Type:text/json http://localhost:9090/api/index/default/query```

Delete index

```curl -XDELETE http://localhost:8095/api/index/default```

### For cbft developers

Getting

    mkdir -p $GOPATH/src/github.com/couchbaselabs
    cd $GOPATH/src/github.com/couchbaselabs
    git clone git://github.com/couchbaselabs/cbft.git

Building

    make

Unit tests

    make test

To get local coverage reports with heatmaps...

    make coverage

To get more coverage reports that include dependencies like the bleve library...

    go test -coverpkg github.com/couchbaselabs/cbft,github.com/blevesearch/bleve,github.com/blevesearch/bleve/index -coverprofile=coverage.out -covermode=count && go tool cover -html=coverage.out

Error messages

In the cbft project, fmt.Errorf() message strings follow a rough
convention, like...

    source_file_base_name: short msg, arg0: val0, arg1: val1

Generating documentation

We use the [MkDocs](http://mkdocs.org) tool to help generate docs.

To generate the REST API markdown documentation...

    make gen-docs

For a local development testing web server that automatically
generates on changes, run...

    mkdocs serve

Then browse to http://127.0.0.1:8000

To deploy to github's gh-pages, run...

    mkdocs gh-deploy
